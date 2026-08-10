"""
Fase 1 — mide si el carry de funding, NETO DE COSTOS, supera el piso de stablecoins.

Regla de parada (pre-registrada en HANDOFF_BASIS.md seccion 4, NO se renegocia):
  se sigue solo si el retorno neto supera el rendimiento de stablecoins por un margen
  claro, CON MEDIANA POSITIVA por simbolo y por semana, Y sobrevive sacar los 3
  simbolos que mas aportan.

Marco de medicion heredado del swing (seccion 7 del handoff) — sin esto ya se dieron
cinco hallazgos falsos por buenos en este repo:
  mediana ademas de media | chequeo de concentracion top-1/3/5 | corte por semana |
  costos desde el dia uno | ventana FIJA con fechas explicitas | regla escrita antes.

Posicion modelada: comprar spot y shortear el perp por el mismo notional. El short
cobra funding cuando la tasa es positiva -> retorno bruto = suma de tasas settled
mientras la posicion esta abierta. Neutral a precio.

Alcance: mide SOLO funding. No incluye basis de entrada/salida, slippage ni el
rendimiento del colateral. Ver "Que NO mide" al final del reporte.

Uso:  py -3.13 phase1.py [--out fase1.json]
"""
import json
import os
import sys

import numpy as np
import pandas as pd

from fetch_funding import load_config, build_universe, to_ms

HERE = os.path.dirname(os.path.abspath(__file__))
MS_DAY = 86400000


# ---------------------------------------------------------------- costos

def round_trip_cost_pct(cfg):
    """4 patas: comprar spot, shortear perp, vender spot, cerrar perp."""
    c = cfg["costs"]
    mode = c.get("MODE", "taker")
    return (c[f"spot_{mode}_pct"] * 2) + (c[f"perp_{mode}_pct"] * 2)


def capital_factor(cfg):
    """Capital inmovilizado por unidad de notional: spot 1.0 + margen del perp."""
    return 1.0 + cfg["capital"].get("PERP_MARGIN_FRACTION", 0.35)


# ---------------------------------------------------------------- panel

def build_panel(data, cfg):
    """{symbol: (times_ms, csum_rates)} — csum[i] = funding acumulado ANTES del i-esimo
    settlement, para poder restar por rangos con searchsorted."""
    panel = {}
    for sym, df in data.items():
        t = df["funding_time"].to_numpy(dtype=np.int64)
        r = df["funding_rate"].to_numpy(dtype=float)
        order = np.argsort(t)
        t, r = t[order], r[order]
        panel[sym] = (t, np.concatenate([[0.0], np.cumsum(r)]))
    return panel


def funding_between(t_arr, csum, t0, t1):
    """Suma de tasas settled en (t0, t1]. Vectorizado sobre arrays de t0/t1."""
    i0 = np.searchsorted(t_arr, t0, side="right")
    i1 = np.searchsorted(t_arr, t1, side="right")
    return csum[i1] - csum[i0]


def entry_grid(t_arr, start_ms, end_ms, hold_days):
    """Entradas candidatas: una por dia, siempre que quepa la tenencia completa
    dentro de la ventana Y dentro de la historia del simbolo."""
    lo = max(start_ms, int(t_arr[0]))
    hi = min(end_ms, int(t_arr[-1])) - hold_days * MS_DAY
    if hi <= lo:
        return np.array([], dtype=np.int64)
    return np.arange(lo, hi, MS_DAY, dtype=np.int64)


# ---------------------------------------------------------------- observaciones

def observations(panel, cfg, hold_days):
    """Una fila por (simbolo, entrada): retorno neto anualizado sobre capital."""
    w = cfg["window"]
    start_ms, end_ms = to_ms(w["start_utc"]), to_ms(w["end_utc"])
    cost = round_trip_cost_pct(cfg)
    capf = capital_factor(cfg)

    rows = []
    for sym, (t_arr, csum) in panel.items():
        t0 = entry_grid(t_arr, start_ms, end_ms, hold_days)
        if t0.size == 0:
            continue
        t1 = t0 + hold_days * MS_DAY
        gross = funding_between(t_arr, csum, t0, t1) * 100.0     # % sobre notional
        net = gross - cost                                       # % neto de costos
        ann = (net / capf) * (365.0 / hold_days)                 # % anual s/ capital
        rows.append(pd.DataFrame({
            "symbol": sym,
            "entry_ms": t0,
            "gross_pct": gross,
            "net_pct": net,
            "net_apy": ann,
        }))
    if not rows:
        return pd.DataFrame()
    out = pd.concat(rows, ignore_index=True)
    out["entry"] = pd.to_datetime(out["entry_ms"], unit="ms", utc=True)
    out["week"] = out["entry"].dt.strftime("%G-W%V")
    return out


# ---------------------------------------------------------------- break-even

def days_to_breakeven(panel, cfg, max_days=45):
    """Pregunta central: cuantos dias tarda una posicion en cubrir el round-trip?

    Para cada entrada diaria camina hacia adelante y devuelve el primer dia en que
    el funding acumulado >= costo. None (censurado) si no ocurre en max_days.
    Separa entradas 'funding alto' (trailing APY > umbral) del resto: si el funding
    alto no sobrevive al break-even, el proyecto muere aca.
    """
    w = cfg["window"]
    start_ms, end_ms = to_ms(w["start_utc"]), to_ms(w["end_utc"])
    cost = round_trip_cost_pct(cfg)
    look = cfg["persistence"]["LOOKBACK_DAYS"]
    thr = cfg["persistence"]["HIGH_APY_THRESHOLD_PCT"]
    capf = capital_factor(cfg)

    rows = []
    horizons = np.arange(1, max_days + 1, dtype=np.int64)
    for sym, (t_arr, csum) in panel.items():
        t0 = entry_grid(t_arr, start_ms, end_ms, max_days)
        # la senal necesita `look` dias de historia ANTES de entrar: sin eso el
        # trailing sale subestimado y la entrada nunca se marca como 'alta'.
        t0 = t0[t0 >= int(t_arr[0]) + look * MS_DAY]
        if t0.size == 0:
            continue
        # senal en el momento de entrar: funding de los ultimos `look` dias, anualizado
        trail = funding_between(t_arr, csum, t0 - look * MS_DAY, t0) * 100.0
        trail_apy = (trail / capf) * (365.0 / look)

        # matriz (entradas x horizontes) de funding acumulado
        cum = np.stack([funding_between(t_arr, csum, t0, t0 + h * MS_DAY) * 100.0
                        for h in horizons], axis=1)
        covered = cum >= cost
        any_cov = covered.any(axis=1)
        first = np.where(any_cov, covered.argmax(axis=1) + 1, -1)

        rows.append(pd.DataFrame({
            "symbol": sym,
            "entry_ms": t0,
            "trail_apy": trail_apy,
            "be_days": first,                 # -1 = censurado (no cubrio en max_days)
            "cum_at_max": cum[:, -1],
            "is_high": trail_apy > thr,
        }))
    return pd.concat(rows, ignore_index=True) if rows else pd.DataFrame()


# ---------------------------------------------------------------- reporte

def _q(s):
    return dict(n=int(len(s)), mean=float(s.mean()), median=float(s.median()),
                p25=float(s.quantile(.25)), p75=float(s.quantile(.75)))


def concentration_check(obs, floor, k_list=(1, 3, 5)):
    """Recalcula sacando los k simbolos que mas aportan. BANKUSDT dio vuelta cinco
    resultados en el swing: todo promedio positivo se rechequea sin su top."""
    contrib = obs.groupby("symbol")["net_pct"].sum().sort_values(ascending=False)
    out = {"top_contributors": [(s, round(float(v), 2)) for s, v in contrib.head(5).items()]}
    for k in k_list:
        drop = set(contrib.head(k).index)
        sub = obs[~obs["symbol"].isin(drop)]
        out[f"drop_top{k}"] = {
            "mean_apy": round(float(sub["net_apy"].mean()), 2),
            "median_apy": round(float(sub["net_apy"].median()), 2),
            "pct_beat_floor": round(float((sub["net_apy"] > floor).mean() * 100), 1),
        }
    return out


def report(cfg, panel, out_path=None):
    cost = round_trip_cost_pct(cfg)
    capf = capital_factor(cfg)
    floor = cfg["floor"]["STABLECOIN_APY_PCT"]
    w = cfg["window"]

    print("=" * 74)
    print("FASE 1 — carry de funding delta-neutral, neto de costos")
    print("=" * 74)
    print(f"Ventana FIJA   : {w['start_utc']} -> {w['end_utc']}")
    print(f"Simbolos       : {len(panel)} perps USDT con pata spot y >= "
          f"{cfg['universe']['MIN_HISTORY_DAYS']}d de historia")
    print(f"Round-trip     : {cost:.2f}% del notional ({cfg['costs']['MODE']}, 4 patas)")
    print(f"Capital        : {capf:.2f}x el notional (spot 1.0 + margen perp "
          f"{cfg['capital']['PERP_MARGIN_FRACTION']})")
    print(f"PISO a superar : {floor:.1f}% anual (stablecoins, sin riesgo de liquidacion)")

    results = {"config": {"window": w, "round_trip_pct": cost,
                          "capital_factor": capf, "floor_apy": floor,
                          "n_symbols": len(panel)}, "windows": {}}

    # ---- A. carry crudo: cuanto paga el funding antes de costos
    print("\n" + "-" * 74)
    print("A. FUNDING BRUTO — cuanto paga el mercado, sin costos")
    print("-" * 74)
    daily = []
    for sym, (t_arr, csum) in panel.items():
        span = (t_arr[-1] - t_arr[0]) / MS_DAY
        if span < 30:
            continue
        daily.append({"symbol": sym, "apy": float(csum[-1]) * 100 / span * 365})
    dfd = pd.DataFrame(daily)
    print(f"  APY bruto por simbolo (buy&hold toda la ventana, sobre NOTIONAL):")
    print(f"    mediana {dfd['apy'].median():6.2f}%   media {dfd['apy'].mean():6.2f}%"
          f"   p25 {dfd['apy'].quantile(.25):6.2f}%   p75 {dfd['apy'].quantile(.75):6.2f}%")
    print(f"    simbolos con funding bruto negativo: "
          f"{(dfd['apy'] < 0).mean()*100:.0f}%")
    results["gross_buyhold_apy"] = _q(dfd["apy"])

    # buy & hold neto: un solo round-trip en toda la ventana
    span_days = (to_ms(w["end_utc"]) - to_ms(w["start_utc"])) / MS_DAY
    bh_net = (dfd["apy"] * span_days / 365 - cost) / capf * (365 / span_days)
    print(f"  APY NETO sobre capital, buy&hold {span_days:.0f}d (1 solo round-trip):")
    print(f"    mediana {bh_net.median():6.2f}%   media {bh_net.mean():6.2f}%"
          f"   | supera el piso: {(bh_net > floor).mean()*100:.0f}% de los simbolos")
    results["buyhold_net_apy"] = _q(bh_net)
    results["buyhold_pct_beat_floor"] = float((bh_net > floor).mean() * 100)

    # ---- B. ventanas rotativas
    for N in cfg["windows_days"]:
        obs = observations(panel, cfg, N)
        if obs.empty:
            continue
        print("\n" + "-" * 74)
        print(f"B. TENENCIA DE {N} DIAS — {len(obs):,} entradas (diarias, {obs['symbol'].nunique()} simbolos)")
        print("-" * 74)
        print(f"  neto anualizado s/capital:  mediana {obs['net_apy'].median():7.2f}%"
              f"   media {obs['net_apy'].mean():7.2f}%")
        print(f"  supera el piso de {floor:.0f}%:      "
              f"{(obs['net_apy'] > floor).mean()*100:5.1f}% de las entradas")
        print(f"  neto positivo (>0):         "
              f"{(obs['net_apy'] > 0).mean()*100:5.1f}% de las entradas")

        by_sym = obs.groupby("symbol")["net_apy"].median()
        by_week = obs.groupby("week")["net_apy"].median()
        print(f"  MEDIANA POR SIMBOLO:  {by_sym.median():7.2f}%  "
              f"| simbolos con mediana > piso: {(by_sym > floor).mean()*100:.0f}%")
        print(f"  MEDIANA POR SEMANA :  {by_week.median():7.2f}%  "
              f"| semanas con mediana > piso: {(by_week > floor).mean()*100:.0f}%"
              f"  ({len(by_week)} semanas)")

        conc = concentration_check(obs, floor)
        print(f"  CONCENTRACION — top aportantes: "
              f"{', '.join(f'{s}' for s, _ in conc['top_contributors'][:3])}")
        for k in (1, 3, 5):
            d = conc[f"drop_top{k}"]
            print(f"    sin top-{k}: mediana {d['median_apy']:7.2f}%  "
                  f"media {d['mean_apy']:7.2f}%  supera piso {d['pct_beat_floor']:4.1f}%")

        results["windows"][str(N)] = {
            "n_obs": int(len(obs)),
            "net_apy": _q(obs["net_apy"]),
            "pct_beat_floor": float((obs["net_apy"] > floor).mean() * 100),
            "pct_positive": float((obs["net_apy"] > 0).mean() * 100),
            "median_of_symbol_medians": float(by_sym.median()),
            "pct_symbols_beat_floor": float((by_sym > floor).mean() * 100),
            "median_of_week_medians": float(by_week.median()),
            "pct_weeks_beat_floor": float((by_week > floor).mean() * 100),
            "concentration": conc,
        }

    # ---- C. la pregunta central
    print("\n" + "=" * 74)
    print("C. PREGUNTA CENTRAL — el funding alto dura mas que el break-even?")
    print("=" * 74)
    be = days_to_breakeven(panel, cfg)
    thr = cfg["persistence"]["HIGH_APY_THRESHOLD_PCT"]
    look = cfg["persistence"]["LOOKBACK_DAYS"]
    if not be.empty:
        for label, sub in (("TODAS las entradas", be),
                           (f"entradas con funding alto (trailing {look}d > {thr:.0f}% APY)",
                            be[be["is_high"]])):
            if sub.empty:
                continue
            cov = sub["be_days"] > 0
            print(f"\n  {label}  (n={len(sub):,})")
            print(f"    cubren el round-trip en <=45d : {cov.mean()*100:5.1f}%")
            if cov.any():
                d = sub.loc[cov, "be_days"]
                print(f"    dias hasta break-even         : mediana {d.median():.0f}d"
                      f"   p25 {d.quantile(.25):.0f}d   p75 {d.quantile(.75):.0f}d")
            print(f"    cubren en <=10d               : "
                  f"{((sub['be_days'] > 0) & (sub['be_days'] <= 10)).mean()*100:5.1f}%")
            print(f"    funding acumulado a 45d       : mediana "
                  f"{sub['cum_at_max'].median():.3f}%  (costo {cost:.2f}%)")
        results["breakeven"] = {
            "all": {"pct_cover_45d": float((be["be_days"] > 0).mean() * 100),
                    "median_be_days": float(be.loc[be["be_days"] > 0, "be_days"].median())
                    if (be["be_days"] > 0).any() else None,
                    "pct_cover_10d": float(((be["be_days"] > 0) & (be["be_days"] <= 10)).mean() * 100)},
        }
        hi = be[be["is_high"]]
        if not hi.empty:
            results["breakeven"]["high_funding"] = {
                "n": int(len(hi)),
                "pct_cover_45d": float((hi["be_days"] > 0).mean() * 100),
                "median_be_days": float(hi.loc[hi["be_days"] > 0, "be_days"].median())
                if (hi["be_days"] > 0).any() else None,
                "pct_cover_10d": float(((hi["be_days"] > 0) & (hi["be_days"] <= 10)).mean() * 100),
            }

    # ---- D. veredicto contra la regla de parada
    print("\n" + "=" * 74)
    print("D. REGLA DE PARADA (pre-registrada, no se renegocia)")
    print("=" * 74)
    verdict = {}
    for N in cfg["windows_days"]:
        r = results["windows"].get(str(N))
        if not r:
            continue
        c3 = r["concentration"]["drop_top3"]
        checks = {
            "mediana por simbolo > piso": r["median_of_symbol_medians"] > floor,
            "mediana por semana > piso": r["median_of_week_medians"] > floor,
            "sobrevive sacar top-3": c3["median_apy"] > floor,
        }
        ok = all(checks.values())
        verdict[str(N)] = {"pass": ok, "checks": checks}
        print(f"  tenencia {N}d: {'PASA' if ok else 'NO PASA'}")
        for k, v in checks.items():
            print(f"      [{'x' if v else ' '}] {k}")
    bh_ok = results["buyhold_net_apy"]["median"] > floor
    verdict["buyhold"] = {"pass": bool(bh_ok)}
    print(f"  buy&hold {span_days:.0f}d: {'PASA' if bh_ok else 'NO PASA'}"
          f"   (mediana {results['buyhold_net_apy']['median']:.2f}% vs piso {floor:.1f}%)")
    results["verdict"] = verdict

    print("\n" + "-" * 74)
    print("Que NO mide esto (declarado, no descubierto despues):")
    print("  - basis de entrada/salida, slippage y profundidad real del libro")
    print("  - rendimiento del colateral, ni el costo de mantener margen")
    print("  - SESGO DE SUPERVIVENCIA: el universo son perps que HOY siguen listados.")
    print("    Los delisted del periodo no estan. Ver [[project-swing-backtest-sesgo-universo]].")
    print("-" * 74)

    if out_path:
        with open(out_path, "w", encoding="utf-8") as f:
            json.dump(results, f, indent=1, default=str)
        print(f"\nJSON -> {out_path}")
    return results


if __name__ == "__main__":
    cfg = load_config()
    out = None
    if "--out" in sys.argv:
        out = sys.argv[sys.argv.index("--out") + 1]
    data, _meta = build_universe(cfg)
    panel = build_panel(data, cfg)
    report(cfg, panel, out)
