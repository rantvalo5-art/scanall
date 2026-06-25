"""
Prototipo NUEVO — FUNDING como TRIGGER DE SALIDA (no de entrada).

Tesis: a diferencia de trail/TF/régimen (perillas de TIMING que solo reordenan la misma
distribución = shape-not-expectation), el funding es una SEÑAL exógena de información:
funding alto+positivo post-alerta = long crowded = giveback inminente. Salir ahí PODRÍA
mover expectativa (cortar perdedores crowded), no solo forma.

Mecanismo: exit en el PRIMERO de {trailing global ARM12/TRAIL8, spike de funding}. El spike
= primer funding-time forward con rate>0 y pct-vs-historia-propia >= umbral (sin leakage).
Compara trailing-solo vs trailing+funding para varios umbrales. Mide avg/med/win/p10/giveback
y cuántas alertas dispara el funding + su outcome.

Reusa sim_exit + sim_funding. NO toca producción. py -3.13. Diag por argv (default diag_16w.json).
"""
import json, sys, statistics as st
from datetime import datetime, timezone, timedelta
import numpy as np

from sim_exit import load_1h, BUCKETS, WINDOW_DAYS, MATURE_BEFORE
from sim_funding import load_funding, funding_at, get_perp_symbols, diag_span

GLOBAL_ARM, GLOBAL_TRAIL, GLOBAL_STOP = 0.12, 0.08, 0.0
# trigger por rate ABSOLUTO (crowding extremo y RARO) en vez de pct-vs-historia (no selectivo).
# umbrales en frac/8h: 0.0005=0.05%, 0.001=0.1%, 0.002=0.2% (muy crowded).
ABS_THRS = (None, 0.0005, 0.001, 0.002)


def funding_exit_time(fund_df, t0_ms, t1_ms, abs_thr):
    """Primer funding-time en (t0,t1] con rate >= abs_thr (crowding extremo). None si no hay."""
    if fund_df is None or abs_thr is None:
        return None
    ft = fund_df["funding_time"].to_numpy()
    fr = fund_df["funding_rate"].to_numpy()
    m = (ft > t0_ms) & (ft <= t1_ms) & (fr >= abs_thr)
    hit = ft[m]
    return int(hit[0]) if len(hit) else None


def simulate_combined(alert, df1h, fund_df, arm, trail, stop, pct_thr):
    """Exit en el PRIMERO de {trailing, funding spike}. Devuelve (realized, mfe, reason)."""
    e = alert.get("entry_price")
    if not e:
        return None
    t0 = datetime.fromisoformat(alert["alerted_at"].replace("Z", "+00:00"))
    t0_ms = int(t0.timestamp() * 1000)
    t1_ms = int((t0 + timedelta(days=WINDOW_DAYS)).timestamp() * 1000)
    t_fund = funding_exit_time(fund_df, t0_ms, t1_ms, pct_thr)
    end_ms = t1_ms if t_fund is None else min(t1_ms, t_fund)
    fwd = df1h[(df1h.open_time > t0_ms) & (df1h.open_time <= end_ms)]
    if len(fwd) < 3:
        return None
    run_max, armed = e, False
    for k in fwd.itertuples():
        hi, lo, cl = k.high, k.low, k.close
        if stop and not armed and lo <= e * (1 - stop):
            return (-stop, run_max / e - 1, "stop")
        run_max = max(run_max, hi)
        if not armed and run_max / e - 1 >= arm:
            armed = True
        if armed and cl <= run_max * (1 - trail):
            return (cl / e - 1, run_max / e - 1, "trail")
    # no disparó el trailing: si fue por funding, salimos en el close final; si no, hold a 7d
    last = float(fwd.close.iloc[-1])
    return (last / e - 1, run_max / e - 1, "funding" if t_fund is not None else "hold")


def show(tag, res):
    real = [r[0] for r in res]
    gb = [r[1] - r[0] for r in res]
    a = np.array(real)
    print(f"  {tag:26} n={len(real):4}  avg {a.mean()*100:+.2f}%  med {np.median(a)*100:+.2f}%  "
          f"win {(a>0).mean()*100:.0f}%  p10 {np.quantile(a,.1)*100:+.1f}%  giveback_med {np.median(gb)*100:+.1f}pp")


def main():
    diag = sys.argv[1] if len(sys.argv) > 1 else "diag_16w.json"
    d = json.load(open(diag))["main"]
    alerts = [a for a in d if a.get("bucket") in BUCKETS and a.get("entry_price")
              and datetime.fromisoformat(a["alerted_at"].replace("Z", "+00:00")) <= MATURE_BEFORE]
    syms = sorted(set(a["symbol"] for a in alerts))
    print(f"Diag: {diag} | {len(alerts)} alertas BEST+STRONG maduras | {len(syms)} símbolos")
    cache = {s: load_1h(s) for s in syms}
    alerts = [a for a in alerts if cache.get(a["symbol"]) is not None]

    start_ms, end_ms = diag_span(alerts)
    print("Obteniendo perps + funding (cache-first)...")
    perp_set = get_perp_symbols()
    fund = {}
    for s in syms:
        fund[s] = load_funding(s, perp_set, start_ms, end_ms)
    n_fund = sum(1 for s in syms if fund.get(s) is not None)
    print(f"Funding disponible: {n_fund}/{len(syms)} símbolos\n")

    # solo alertas con funding (comparación justa entre umbrales)
    A = [a for a in alerts if fund.get(a["symbol"]) is not None]
    print(f"Alertas con funding (set de comparación): {len(A)}\n")
    print(f"{'='*92}")
    print(f" FUNDING-COMO-SALIDA — exit en primero de {{trailing ARM12/TRAIL8, funding spike pct>=thr}}")
    print(f"{'='*92}")

    for thr in ABS_THRS:
        res = [simulate_combined(a, cache[a["symbol"]], fund[a["symbol"]],
                                 GLOBAL_ARM, GLOBAL_TRAIL, GLOBAL_STOP, thr) for a in A]
        res = [r for r in res if r is not None]
        tag = "trailing solo (baseline)" if thr is None else f"trailing + funding rate>={thr}"
        show(tag, res)
        if thr is not None:
            from collections import Counter
            reasons = Counter(r[2] for r in res)
            fired = [r for r in res if r[2] == "funding"]
            if fired:
                fa = np.array([r[0] for r in fired])
                print(f"      └─ exits por funding: {len(fired)}  | su outcome: avg {fa.mean()*100:+.2f}%  "
                      f"med {np.median(fa)*100:+.2f}%  win {(fa>0).mean()*100:.0f}%   (reasons={dict(reasons)})")
    print("\nLectura: el funding-exit gana solo si SUBE avg (no solo med/giveback). Si el outcome de")
    print("los exits-por-funding es peor que el resto, está cortando perdedores = mueve expectativa.")


if __name__ == "__main__":
    main()
