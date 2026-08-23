"""
DATOS FUERA DEL PRECIO — posicionamiento de futuros sobre las alertas del swing.

Las ~450 hipótesis muertas del repo son TODAS features de precio/volumen sobre la
misma moneda. Esta familia es la única categoría entera sin tocar en el swing: su
config no tiene sección `derivatives` a propósito.

Fuente: dumps diarios públicos de Binance Futures (data.binance.vision), que dan
resolución de 5 min desde 2020 — sin el muro de 30 días de la API REST. Se
agregan a HORA usando el ÚLTIMO valor de cada hora, que es lo que se sabe cuando
la vela cierra (nada forward-looking).

Variables (ninguna es precio):
  oi, oi_usd   posición abierta agregada
  ls_cuentas   ratio long/short de CUENTAS (la multitud)
  tt_cuentas   ratio long/short de cuentas de TOP TRADERS
  tt_pos       ratio long/short por POSICIÓN de top traders (el dinero grande)
  taker        ratio de volumen taker comprador/vendedor

Y una gratis, que sale de que el símbolo tenga o no perp:
  perp         proxy de "tier" de la moneda; el swing nunca lo usó

FASE A (barata, este script): ¿tener perp predice? Sonda de 1 día por símbolo.
FASE B: descarga completa para el subconjunto con perp y prueba las 6 variables.

Todo contra el DARDO PAREADO, con Benjamini-Hochberg, concentración en los dos
ejes y partición temporal. Nada se llama hallazgo sin los cuatro.

    py -3.13 posicionamiento.py --fase A
    py -3.13 posicionamiento.py --fase B
"""
import argparse
import io
import json
import os
import random
import zipfile
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import numpy as np
import pandas as pd
import requests

import fase0_plan as F
import dip_previo as D

BASE = "https://data.binance.vision/data/futures/um/daily/metrics"
MCACHE = F.HERE / ".metrics_cache"
MCACHE.mkdir(exist_ok=True)
MS_H = 3_600_000

COLS = {
    "sum_open_interest": "oi",
    "sum_open_interest_value": "oi_usd",
    "count_toptrader_long_short_ratio": "tt_cuentas",
    "sum_toptrader_long_short_ratio": "tt_pos",
    "count_long_short_ratio": "ls_cuentas",
    "sum_taker_long_short_vol_ratio": "taker",
}
VARS = list(COLS.values())


def _perp_variantes(spot):
    """Binance re-escala los perps de precio chico (1000PEPEUSDT, etc.)."""
    return [spot, f"1000{spot}", f"1000000{spot}"]


def _dia(perp, fecha):
    url = f"{BASE}/{perp}/{perp}-metrics-{fecha}.zip"
    try:
        r = requests.get(url, timeout=30)
        if r.status_code != 200:
            return None
        with zipfile.ZipFile(io.BytesIO(r.content)) as z:
            with z.open(z.namelist()[0]) as f:
                return pd.read_csv(f)
    except Exception:
        return None


def resolver_perp(spot, fecha_muestra):
    """Qué variante de perp existe (o None). Cacheado: es la sonda de la Fase A."""
    p = MCACHE / f"perp_{spot}.json"
    if p.exists():
        try:
            return json.loads(p.read_text()).get("perp")
        except Exception:
            pass
    perp = None
    for cand in _perp_variantes(spot):
        if _dia(cand, fecha_muestra) is not None:
            perp = cand
            break
    p.write_text(json.dumps({"perp": perp}))
    return perp


def frame_simbolo(spot, fechas, workers=16):
    """DataFrame horario [t, oi, ...] o None si no hay perp. Cacheado en disco."""
    tag = f"{spot}_{fechas[0]}_{fechas[-1]}"
    p = MCACHE / f"{tag}.pkl"
    if p.exists():
        try:
            d = pd.read_pickle(p)
            return None if d.empty else d
        except Exception:
            pass
    perp = resolver_perp(spot, fechas[len(fechas) // 2])
    if perp is None:
        pd.DataFrame(columns=["t"]).to_pickle(p)
        return None
    with ThreadPoolExecutor(workers) as ex:
        dias = [d for d in ex.map(lambda f: _dia(perp, f), fechas)
                if d is not None and not d.empty]
    if not dias:
        pd.DataFrame(columns=["t"]).to_pickle(p)
        return None
    d = pd.concat(dias, ignore_index=True).rename(columns=COLS)
    ts = pd.to_datetime(d["create_time"], utc=True, format="mixed").astype("int64") // 10**6
    d["t"] = (ts // MS_H) * MS_H
    out = (d.groupby("t", as_index=False)[VARS].last()
           .sort_values("t").reset_index(drop=True))
    out.to_pickle(p)
    return out


# ── carga de alertas + retorno forward + dardo (compartido por A y B) ────────
def cargar(csv, bucket, H, darts, seed):
    df = pd.read_csv(csv)
    df = df[df.bucket == bucket].copy()
    df["ts"] = pd.to_datetime(df.alerted_at, utc=True)
    df["ts_ms"] = df.ts.astype("int64") // 10**6
    df["week"] = df.ts.dt.strftime("%G-W%V")
    K = F.bulk_load(sorted(df.symbol.unique()), "1h",
                    int(df.ts_ms.min() - 40 * D.DAY_MS),
                    int(df.ts_ms.max() + 9 * D.DAY_MS))
    de = max(int(d.open_time.iloc[-1]) for d in K.values())
    df = df[df.ts_ms <= de - H * D.HOUR_MS]
    rng = random.Random(seed)
    R = []
    for _, a in df.iterrows():
        d = K.get(a.symbol)
        if d is None:
            continue
        i = F.engine_bar_index(d, int(a.ts_ms))
        r = D.fwd(d, i, H)
        if r is None:
            continue
        got, tries = [], 0
        while len(got) < darts and tries < darts * 8:
            tries += 1
            t = rng.randrange(int(a.ts_ms) - 21 * D.DAY_MS, int(a.ts_ms) + 21 * D.DAY_MS)
            if abs(t - int(a.ts_ms)) < 7 * D.DAY_MS:
                continue
            rr = D.fwd(d, F.bar_index(d, t), H)
            if rr is not None:
                got.append(rr)
        if len(got) < 4:
            continue
        R.append({"symbol": a.symbol, "signal": a.signal_type, "week": a.week,
                  "ts": a.ts, "ts_ms": int(a.ts_ms), "i": i, "r": r,
                  "dardo": float(np.mean(got))})
    return pd.DataFrame(R), K


def resumen(nom, g, col="r"):
    v = g[col]
    lo, hi = F.boot_ci(v.tolist(), g.week.tolist())
    d2, _, _ = F.drop_top(v.tolist(), g.week.tolist(), g.symbol.tolist())
    d3, _, _ = F.drop_top(v.tolist(), g.week.tolist(), g.week.tolist())
    m = (g.r - g.dardo)
    lo2, hi2 = F.boot_ci(m.tolist(), g.week.tolist())
    print(f"  {nom:<26} n={len(g):>4} ret {v.mean() * 100:>+7.2f}% "
          f"(med {v.median() * 100:>+6.2f}%) IC95 [{lo * 100:+6.2f},{hi * 100:+6.2f}]  "
          f"sin3sim {np.mean(d2) * 100:>+6.2f}%  sin3sem {np.mean(d3) * 100:>+6.2f}%  "
          f"vs dardo {m.mean() * 100:>+6.2f}pp [{lo2 * 100:+6.2f},{hi2 * 100:+6.2f}]"
          f"{' CRUZA' if lo2 > 0 else ''}")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--csv", default=str(F.CSV_DEFAULT))
    ap.add_argument("--bucket", default="BEST")
    ap.add_argument("--fase", default="A", choices=["A", "B"])
    ap.add_argument("--horizonte-h", type=int, default=168)
    ap.add_argument("--darts", type=int, default=10)
    ap.add_argument("--lookback-d", type=int, default=14)
    ap.add_argument("--out", default=None)
    ap.add_argument("--seed", type=int, default=41)
    args = ap.parse_args()
    H = args.horizonte_h
    out = args.out or f"posic_fase{args.fase}.json"

    T, K = cargar(args.csv, args.bucket, H, args.darts, args.seed)
    print(f"Alertas {args.bucket} con forward {H}h: {len(T)} · "
          f"{T.symbol.nunique()} símbolos\n")

    syms = sorted(T.symbol.unique())
    muestra = T.ts.median().strftime("%Y-%m-%d")

    # ── FASE A: ¿tener perp predice? ────────────────────────────────────────
    print(f"Sondeando perp de {len(syms)} símbolos (día de muestra {muestra})...")
    with ThreadPoolExecutor(24) as ex:
        perps = dict(zip(syms, ex.map(lambda s: resolver_perp(s, muestra), syms)))
    T["perp"] = T.symbol.map(lambda s: perps.get(s) is not None)
    n_con = sum(1 for v in perps.values() if v)
    print(f"  con perp: {n_con}/{len(syms)} símbolos "
          f"({T.perp.mean() * 100:.1f}% de las alertas)\n")

    print("=" * 118)
    print("FASE A — ¿tener perp (proxy de tier de la moneda) predice?")
    print("=" * 118)
    resumen("CON perp", T[T.perp])
    resumen("SIN perp", T[~T.perp])
    dif = T[T.perp].r.mean() - T[~T.perp].r.mean()
    print(f"\n  diferencia con−sin: {dif * 100:+.2f}pp")
    mid = T.ts.quantile(0.5)
    for nom, S in (("1a mitad", T[T.ts <= mid]), ("2a mitad (OOS)", T[T.ts > mid])):
        if S.perp.nunique() < 2:
            continue
        print(f"  {nom:<16} con perp {S[S.perp].r.mean() * 100:>+7.2f}% "
              f"(n={S.perp.sum()})   sin perp {S[~S.perp].r.mean() * 100:>+7.2f}% "
              f"(n={(~S.perp).sum()})")

    if args.fase == "A":
        Path(out).write_text(json.dumps(
            {"n": len(T), "perp_pct": float(T.perp.mean()),
             "con_perp": float(T[T.perp].r.mean()),
             "sin_perp": float(T[~T.perp].r.mean())},
            indent=2, ensure_ascii=False), encoding="utf-8")
        print(f"\nGuardado: {out}")
        return

    # ── FASE B: las 6 variables de posicionamiento ──────────────────────────
    S = T[T.perp].copy()
    ini = (S.ts.min() - pd.Timedelta(days=args.lookback_d + 2)).strftime("%Y-%m-%d")
    fin = (S.ts.max() + pd.Timedelta(days=1)).strftime("%Y-%m-%d")
    fechas = [d.strftime("%Y-%m-%d")
              for d in pd.date_range(ini, fin, freq="D", inclusive="left")]
    csyms = sorted(S.symbol.unique())
    print(f"\nFASE B — bajando métricas de {len(csyms)} símbolos × {len(fechas)} días...")
    M = {}
    for n, s in enumerate(csyms, 1):
        f = frame_simbolo(s, fechas)
        if f is not None and len(f) > 24 * args.lookback_d:
            M[s] = f
        if n % 25 == 0:
            print(f"    {n}/{len(csyms)}  ({len(M)} con datos)", flush=True)
    print(f"  {len(M)} símbolos con serie de métricas\n")

    LB = 24 * args.lookback_d
    filas = []
    for _, a in S.iterrows():
        m = M.get(a.symbol)
        if m is None:
            continue
        t = m["t"].values
        # El valor horario es el ULTIMO dato de esa hora, o sea que la hora que
        # CONTIENE la alerta incluye observaciones POSTERIORES a ella: usarla es
        # lookahead de hasta 59 min. Se toma la ultima hora COMPLETAMENTE cerrada
        # antes de la alerta.
        j = int(np.searchsorted(t, a.ts_ms - MS_H, side="right")) - 1
        if j < LB:
            continue
        row = {"symbol": a.symbol, "week": a.week, "ts": a.ts,
               "r": a.r, "dardo": a.dardo}
        ok = True
        for v in VARS:
            x = m[v].values[j - LB:j + 1].astype(float)
            if np.isnan(x).any() or x.std() == 0:
                ok = False
                break
            row[v] = float(x[-1])
            row[f"z_{v}"] = float((x[-1] - x.mean()) / x.std())   # z contra su propia historia
        if ok:
            filas.append(row)
    B = pd.DataFrame(filas)
    print(f"Alertas con posicionamiento: {len(B)}\n")
    if len(B) < 100:
        print("Muy pocas. Fin.")
        return

    campos = [f"z_{v}" for v in VARS] + ["ls_cuentas", "tt_pos", "tt_cuentas", "taker"]
    print("=" * 118)
    print(f"FASE B — {len(campos)} variables de posicionamiento (nivel y z de "
          f"{args.lookback_d}d), n={len(B)}")
    print("=" * 118)
    print(f"  {'variable':<14} {'rho(var,ret)':>13} {'p':>7} | quintiles del retorno (q1→q5)")
    import pileta as P
    tests = []
    for c in campos:
        sub = B[B[c].notna()]
        if len(sub) < 80:
            continue
        p, rho = P.p_boot_corr(sub[c].tolist(), sub.r.tolist(), sub.week.tolist())
        try:
            q = pd.qcut(sub[c], 5, labels=False, duplicates="drop")
        except ValueError:
            continue
        med = [sub.r[q == k].mean() * 100 for k in sorted(set(q.dropna()))]
        print(f"  {c:<14} {rho:>+13.3f} {p:>7.3f} | " +
              " ".join(f"{x:>+7.2f}%" for x in med))
        tests.append({"var": c, "rho": rho, "p": p, "n": len(sub), "quintiles": med})

    if tests:
        corte = D.bh([t["p"] for t in tests], q=0.10)
        vivos = [t for t in tests if t["p"] <= corte]
        print(f"\n  Benjamini-Hochberg FDR 10% sobre {len(tests)} variables → "
              f"p <= {corte:.4f}  ({len(vivos)} vivos)")
        mid = B.ts.quantile(0.5)
        for t in vivos:
            c = t["var"]
            sub = B[B[c].notna()].copy()
            sub["q"] = pd.qcut(sub[c], 5, labels=False, duplicates="drop")
            print(f"\n  ── {c} (rho {t['rho']:+.3f}, p {t['p']:.4f}) ──")
            for k, nom in ((sub.q.max(), "q5 (alto)"), (sub.q.min(), "q1 (bajo)")):
                g = sub[sub.q == k]
                resumen(nom, g)
                a1, a2 = g[g.ts <= mid], g[g.ts > mid]
                if len(a1) > 10 and len(a2) > 10:
                    print(f"  {'':<26} 1a mitad {a1.r.mean() * 100:>+7.2f}% (n={len(a1)})"
                          f"   2a mitad {a2.r.mean() * 100:>+7.2f}% (n={len(a2)})")

    Path(out).write_text(json.dumps(
        {"n": len(B), "lookback_d": args.lookback_d, "tests": tests},
        indent=2, ensure_ascii=False), encoding="utf-8")
    print(f"\nGuardado: {out}")


if __name__ == "__main__":
    main()
