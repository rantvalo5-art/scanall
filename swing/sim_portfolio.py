"""
NUEVA DIRECCIÓN — Cartera + sizing (no re-slicing de la señal de entrada).

Todo el análisis previo fue PER-ALERTA, equal-weight, capital infinito. Nunca se simuló
una CUENTA real con capital limitado, concurrencia y sizing. El meta-patrón del proyecto
dice que lo que mueve EV es gestión de posición (exit ✓) y datos ortogonales (funding ✓),
NO re-cortar la entrada. El sizing/selección a nivel CARTERA es gestión de posición pura y
está sin tocar.

Hipótesis: aunque la SELECCIÓN per-alerta no tenga edge (score Spearman≈0), `atr_pct_1d`
predice mover-probability (AUC per-símbolo 0.85, banda en U ~3-12). Ponderar TAMAÑO por banda
de atr y/o limitar concurrencia puede subir el retorno y el risk-adjusted de la CUENTA, que
es una métrica distinta a la media per-alerta.

PASO 1: por alerta BEST+STRONG, correr el exit trailing shipeado (ARM/TRAIL/STOP de config)
        → (entry_ms, exit_ms, realized). Cacheado a disco (caro: 1h de ~2700 alertas).
PASO 2: motor de cartera event-driven con capital limitado (sin leverage, skip si no hay
        capital) comparando políticas. Métricas: retorno total, Sharpe (equity diario),
        max drawdown, #trades, exposición media.

Corre con `py -3.13`. NO toca producción.
"""
import json, sys
from datetime import datetime, timezone, timedelta
from pathlib import Path
import numpy as np

from sim_exit import load_1h, CACHE_DIR

EXITS_CACHE = Path("sim_portfolio_exits.json")
DIAG = "diag_16w.json"
DAY_MS = 86_400_000
HOUR_MS = 3_600_000


def parse_ms(s):
    return int(datetime.fromisoformat(s.replace("Z", "+00:00")).timestamp() * 1000)


def exit_with_time(entry, t0_ms, df, arm, trail, stop, window_days):
    """Exit trailing → (realized, exit_ms). Igual a sim_exit.simulate pero devuelve el ts de salida."""
    t1_ms = t0_ms + window_days * DAY_MS
    fwd = df[(df.open_time > t0_ms) & (df.open_time <= t1_ms)]
    if len(fwd) < 3:
        return None
    ot = fwd.open_time.values; hi = fwd.high.values; lo = fwd.low.values; cl = fwd.close.values
    run_max = entry; armed = False
    for k in range(len(fwd)):
        if stop and not armed and lo[k] <= entry * (1 - stop):
            return (-stop, int(ot[k]))
        run_max = max(run_max, hi[k])
        if not armed and run_max / entry - 1 >= arm:
            armed = True
        if armed and cl[k] <= run_max * (1 - trail):
            return (cl[k] / entry - 1, int(ot[k]))
    return (float(cl[-1]) / entry - 1, int(ot[-1]))


def build_exit_cache(cfg_exit):
    """Paso 1 — calcula (entry_ms, exit_ms, realized) por alerta BEST+STRONG. Cacheado."""
    if EXITS_CACHE.exists():
        rows = json.load(open(EXITS_CACHE))
        print(f"Exits desde cache: {len(rows)} posiciones")
        return rows
    arm = cfg_exit["ARM_PCT"]; trail = cfg_exit["TRAIL_PCT"]; stop = cfg_exit["STOP_PCT"]
    wd = cfg_exit.get("WINDOW_DAYS", 7)
    buckets = set(cfg_exit.get("BUCKETS", ["BEST", "STRONG"]))
    alerts = [a for a in json.load(open(DIAG))["main"]
              if a.get("bucket") in buckets and a.get("entry_price")]
    print(f"Paso 1: {len(alerts)} alertas {buckets} → corriendo exit trailing (carga 1h)...")
    by_sym = {}
    for a in alerts:
        by_sym.setdefault(a["symbol"], []).append(a)
    rows = []
    for k, (sym, lst) in enumerate(sorted(by_sym.items())):
        if k % 40 == 0:
            print(f"  {k}/{len(by_sym)} símbolos...", file=sys.stderr)
        df = load_1h(sym)
        if df is None:
            continue
        for a in lst:
            e = a["entry_price"]; t0 = parse_ms(a["alerted_at"])
            r = exit_with_time(e, t0, df, arm, trail, stop, wd)
            if r is None:
                continue
            realized, exit_ms = r
            rows.append({"symbol": sym, "entry_ms": t0, "exit_ms": exit_ms,
                         "realized": realized, "atr": a.get("atr_pct_1d"),
                         "bucket": a.get("bucket"), "signal": a.get("signal_type")})
    rows.sort(key=lambda x: x["entry_ms"])
    EXITS_CACHE.write_text(json.dumps(rows))
    print(f"  → {len(rows)} posiciones cacheadas en {EXITS_CACHE}")
    return rows


# ---- sizing por banda de atr (U invertida: junk <3 y >15 penalizados) ----
def band_mult(atr, scheme):
    if scheme == "equal" or atr is None:
        return 1.0
    if scheme == "atr_band":
        if atr < 3:      return 0.5
        if atr < 6:      return 1.0
        if atr < 12:     return 1.5
        if atr < 15:     return 1.0
        return 0.5
    if scheme == "atr_soft":     # tilt gentil (menos concentración en el mid-band)
        if atr < 3:      return 0.7
        if atr < 6:      return 1.0
        if atr < 12:     return 1.3
        if atr < 15:     return 1.0
        return 0.7
    return 1.0


def run_portfolio(rows, cap_slots, sizing, atr_lo=None, atr_hi=None, base_w=None, tx_cost=0.0):
    """Motor event-driven. cap_slots = nº máximo de posiciones concurrentes. base_w =
    fracción de equity por posición (default 1/cap_slots). Sin leverage: skip si no hay
    capital libre. Devuelve dict de métricas + equity diario."""
    if base_w is None:
        base_w = 1.0 / cap_slots
    # eventos: (ts, tipo, idx) — abrir en entry_ms, cerrar en exit_ms
    open_ev = [(r["entry_ms"], "open", i) for i, r in enumerate(rows)]
    close_ev = []
    equity = 1.0
    free = 1.0                  # capital libre (fracción)
    n_open = 0
    positions = {}              # idx -> notional
    booked = []                 # (ts, pnl) para curva diaria
    n_trades = 0; n_skip_cap = 0; n_skip_filter = 0
    exposure_samples = []

    # merge cronológico de aperturas y cierres
    events = []
    for ts, _, i in open_ev:
        events.append((ts, 0, i))     # 0 = open prioridad menor (cerrar antes de abrir en mismo ts)
    # los cierres se agregan dinámicamente; para simplicidad pre-resolvemos cierres por idx
    # haciendo un timeline ordenado de (ts, kind, idx): kind -1 cierre, +1 apertura
    timeline = []
    for i, r in enumerate(rows):
        timeline.append((r["entry_ms"], 1, i))
        timeline.append((r["exit_ms"], -1, i))
    timeline.sort(key=lambda x: (x[0], x[1]))   # en mismo ts, cierre(-1) antes que apertura(1)

    for ts, kind, i in timeline:
        if kind == -1:
            if i in positions:
                notional = positions.pop(i)
                pnl = notional * (rows[i]["realized"] - tx_cost)   # tx_cost = round-trip (fees+slippage)
                equity += pnl
                free += notional
                n_open -= 1
                booked.append((ts, equity))
        else:
            r = rows[i]
            atr = r["atr"]
            if (atr_lo is not None and (atr is None or atr < atr_lo)) or \
               (atr_hi is not None and (atr is not None and atr > atr_hi)):
                n_skip_filter += 1
                continue
            if n_open >= cap_slots:
                n_skip_cap += 1
                continue
            w = base_w * band_mult(atr, sizing)
            notional = min(w * equity, free)
            if notional <= 1e-6:
                n_skip_cap += 1
                continue
            positions[i] = notional
            free -= notional
            n_open += 1
            n_trades += 1
        exposure_samples.append(1.0 - free)

    # cerrar lo que quede al final (mark a realized ya está booked en su exit_ms; todos cierran)
    # curva de equity diaria a partir de los booked
    if not booked:
        return {"final": equity, "total_ret": 0.0, "sharpe": float("nan"),
                "maxdd": 0.0, "trades": 0, "skip_cap": n_skip_cap, "skip_filter": n_skip_filter,
                "avg_exposure": 0.0}
    booked.sort()
    t0 = booked[0][0]; t1 = booked[-1][0]
    days = np.arange(t0, t1 + DAY_MS, DAY_MS)
    eq = np.empty(len(days)); j = 0; cur = 1.0
    bk = booked
    bi = 0
    for di, d in enumerate(days):
        while bi < len(bk) and bk[bi][0] <= d:
            cur = bk[bi][1]; bi += 1
        eq[di] = cur
    rets = np.diff(eq) / eq[:-1]
    sharpe = (rets.mean() / (rets.std() + 1e-12)) * np.sqrt(365) if len(rets) > 1 else float("nan")
    peak = np.maximum.accumulate(eq)
    maxdd = float(((eq - peak) / peak).min())
    return {"final": equity, "total_ret": equity - 1.0, "sharpe": float(sharpe),
            "maxdd": maxdd, "trades": n_trades, "skip_cap": n_skip_cap,
            "skip_filter": n_skip_filter, "avg_exposure": float(np.mean(exposure_samples))}


def fmt(label, m):
    print(f"  {label:<34} ret {m['total_ret']*100:>+7.1f}%  sharpe {m['sharpe']:>5.2f}  "
          f"maxDD {m['maxdd']*100:>6.1f}%  trades {m['trades']:>4}  "
          f"expo {m['avg_exposure']*100:>4.0f}%  skipCap {m['skip_cap']:>4}")


def main():
    cfg_exit = json.load(open("config.json")).get("exit_mgmt", {})
    rows = build_exit_cache(cfg_exit)
    n = len(rows)
    base_real = np.array([r["realized"] for r in rows])
    print(f"\nPosiciones: {n} | realized medio {base_real.mean()*100:+.2f}% | "
          f"mediana {np.median(base_real)*100:+.2f}% | win {np.mean(base_real>=0)*100:.0f}%\n")

    print("=== BASELINE: equal-weight, capital infinito (≈ visión per-alerta actual) ===")
    # cada alerta toma misma fracción chica, sin restricción de capital
    inf = run_portfolio(rows, cap_slots=100_000, sizing="equal", base_w=0.01)
    fmt("all, w=1%, sin límite", inf)

    print("\n=== CARTERA con capital limitado (cuenta real, sin leverage) ===")
    for cap in (5, 10, 20):
        m = run_portfolio(rows, cap_slots=cap, sizing="equal")
        fmt(f"cap={cap} equal", m)

    print("\n=== + sizing por banda de atr (U: <3 y >15 a 0.5x, 6-12 a 1.5x) ===")
    for cap in (5, 10, 20):
        m = run_portfolio(rows, cap_slots=cap, sizing="atr_band")
        fmt(f"cap={cap} atr_band", m)

    print("\n=== + filtro de atr (descartar <3 y >15) ===")
    for cap in (5, 10, 20):
        m = run_portfolio(rows, cap_slots=cap, sizing="atr_band", atr_lo=3, atr_hi=15)
        fmt(f"cap={cap} atr_band+filtro", m)

    print("\n  LECTURA: comparar retorno total Y Sharpe/maxDD entre políticas. Si sizing/filtro "
          "\n  por atr o el cap de concurrencia mejoran el risk-adjusted → lever de CARTERA real.")

    # esquema ganador (atr_soft) — robustez a concurrencia y a costos de transacción
    for tx in (0.0, 0.0015, 0.0030):    # round-trip: 0 / 15bps / 30bps (fees+slippage)
        for cap in (5, 10, 15, 20):
            walk_forward(rows, cap=cap, n_folds=4, scheme="atr_soft", tx_cost=tx)

    # robustez al nº de folds (cap=10 fijo, tx=15bps) — descartar artefacto de partición
    print(f"\n{'='*60}\n  ROBUSTEZ A Nº DE FOLDS (cap=10, tx=0.15%)\n{'='*60}")
    for nf in (3, 5, 6):
        walk_forward(rows, cap=10, n_folds=nf, scheme="atr_soft", tx_cost=0.0015)


def walk_forward(rows, cap, n_folds, scheme, tx_cost):
    """Partición temporal en folds independientes (cuenta fresca por fold). Por fold:
    equal vs `scheme` al mismo cap, neto de tx_cost. PASA si gana en (casi) todos los folds,
    incl. el bajista. Esquema FIJO (no fiteado) → sin leakage de calibración."""
    t0 = rows[0]["entry_ms"]; t1 = rows[-1]["entry_ms"]
    span = (t1 - t0) // n_folds
    print(f"\n  cap={cap:<2} tx={tx_cost*100:.2f}% | {scheme} vs equal  "
          f"({n_folds} folds ~{span//DAY_MS}d)")
    print(f"  {'fold':>4} {'n':>4} | {'EQ ret':>8} {'sh':>5} | {scheme[:8]+' ret':>13} {'sh':>5} | {'Δret':>6} {'Δsh':>6}")
    wins_ret = wins_sh = 0; folds = 0
    for f in range(n_folds):
        lo = t0 + f * span
        hi = t1 + 1 if f == n_folds - 1 else t0 + (f + 1) * span
        sub = [r for r in rows if lo <= r["entry_ms"] < hi]
        if len(sub) < 20:
            continue
        folds += 1
        me = run_portfolio(sub, cap_slots=cap, sizing="equal", tx_cost=tx_cost)
        ma = run_portfolio(sub, cap_slots=cap, sizing=scheme, tx_cost=tx_cost)
        dret = (ma["total_ret"] - me["total_ret"]) * 100
        dsh = ma["sharpe"] - me["sharpe"]
        wins_ret += dret > 0; wins_sh += dsh > 0
        print(f"  {f+1:>4} {len(sub):>4} | {me['total_ret']*100:>+7.1f}% {me['sharpe']:>5.2f} | "
              f"{ma['total_ret']*100:>+12.1f}% {ma['sharpe']:>5.2f} | {dret:>+5.1f} {dsh:>+6.2f}")
    print(f"   → {scheme} gana: ret {wins_ret}/{folds} | Sharpe {wins_sh}/{folds}")


if __name__ == "__main__":
    main()
