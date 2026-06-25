"""
Prototipo — capa de salida (trailing-stop / take-profit) para swing.

Simula sobre las alertas del backtest (diag_8w.json) cuánto del MFE 7d se recupera
aplicando un trailing-stop, vs el comportamiento actual (hold ciego a 7d). NO toca
producción ni envía nada; solo cuantifica el lever y tunea ARM/TRAIL[/STOP].

Regla:
  - entry = entry_price; run_max arranca en entry.
  - por cada vela 1h forward (hasta +7d): run_max = max(run_max, high).
  - armado cuando run_max/entry-1 >= ARM_PCT.
  - STOP duro opcional: si low/entry-1 <= -STOP_PCT antes de armar -> salida en -STOP_PCT.
  - una vez armado, si close <= run_max*(1-TRAIL_PCT) -> salida a ese close.
  - si nunca dispara: realized = close de la última vela de la ventana (~price_7d).
"""
import json, glob, os, sys, statistics as st
from datetime import datetime, timezone, timedelta
from pathlib import Path
import pandas as pd
import requests

WINDOW_DAYS = 7
NOW = datetime.now(timezone.utc)
MATURE_BEFORE = NOW - timedelta(days=WINDOW_DAYS)

CACHE_DIR = Path(".backtest_cache")
BINANCE_KLINES = "https://data-api.binance.vision/api/v3/klines"


def _download_klines(symbol, tf, start_ms, end_ms):
    """Baja klines de cualquier TF desde Binance (paginado 1000) -> DataFrame
    open_time/high/low/close. Mismo patrón que exit_tracker.get_klines_1h."""
    rows, cursor = [], start_ms
    while cursor < end_ms:
        try:
            r = requests.get(BINANCE_KLINES, params={
                "symbol": symbol, "interval": tf,
                "startTime": cursor, "endTime": end_ms, "limit": 1000,
            }, timeout=15)
            r.raise_for_status()
            data = r.json()
        except Exception as e:
            print(f"  download {symbol} {tf} error: {e}")
            break
        if not data:
            break
        rows.extend(data)
        cursor = int(data[-1][6]) + 1
        if len(data) < 1000:
            break
    if not rows:
        return None
    return pd.DataFrame({
        "open_time": [int(k[0]) for k in rows],
        "high":      [float(k[2]) for k in rows],
        "low":       [float(k[3]) for k in rows],
        "close":     [float(k[4]) for k in rows],
    })


def load_tf(symbol, tf, span=None):
    """Carga klines del TF desde cache; si no está y `span=(start_ms,end_ms)`, baja y cachea.
    Para 1h/4h/1d/1w usa el cache del backtest; 5m/15m se bajan a demanda."""
    fs = glob.glob(str(CACHE_DIR / f"klines_{symbol}_{tf}_*.pkl"))
    if fs:
        f = min(fs, key=lambda p: int(p.split(f"_{tf}_")[1].split("_")[0]))
        df = pd.read_pickle(f)
        df = df[["open_time", "high", "low", "close"]]
        if span is None or (df.open_time.min() <= span[0] and df.open_time.max() >= span[1]):
            return df.sort_values("open_time").reset_index(drop=True)
    if span is None:
        return None
    start_ms, end_ms = span
    df = _download_klines(symbol, tf, start_ms, end_ms)
    if df is None or df.empty:
        return None
    CACHE_DIR.mkdir(exist_ok=True)
    df.to_pickle(CACHE_DIR / f"klines_{symbol}_{tf}_{start_ms}_{end_ms}.pkl")
    return df.sort_values("open_time").reset_index(drop=True)


def load_1h(symbol):
    return load_tf(symbol, "1h")


def simulate(alert, df, arm, trail, stop):
    """Devuelve (realized, mfe) en fracción desde entry, o None. mfe = pico (run_max) visto."""
    e = alert.get("entry_price")
    if not e:
        return None
    t0 = datetime.fromisoformat(alert["alerted_at"].replace("Z", "+00:00"))
    t0_ms = int(t0.timestamp() * 1000)
    t1_ms = int((t0 + timedelta(days=WINDOW_DAYS)).timestamp() * 1000)
    fwd = df[(df.open_time > t0_ms) & (df.open_time <= t1_ms)]
    if len(fwd) < 3:
        return None
    run_max = e
    armed = False
    for _, k in fwd.iterrows():
        hi, lo, cl = k.high, k.low, k.close
        # stop duro pre-armado
        if stop and not armed and lo <= e * (1 - stop):
            return (-stop, run_max / e - 1)
        run_max = max(run_max, hi)
        if not armed and run_max / e - 1 >= arm:
            armed = True
        if armed and cl <= run_max * (1 - trail):
            return (cl / e - 1, run_max / e - 1)
    return (float(fwd.close.iloc[-1]) / e - 1, run_max / e - 1)  # hold a fin de ventana

BUCKETS = ("BEST", "STRONG")  # gestionar también STRONG: el score no discrimina outcome


def main():
    d = json.load(open("diag_8w.json"))["main"]
    # BEST+STRONG maduras (forward 7d real) con entry
    best = [a for a in d if a.get("bucket") in BUCKETS and a.get("entry_price")
            and datetime.fromisoformat(a["alerted_at"].replace("Z", "+00:00")) <= MATURE_BEFORE]
    syms = sorted(set(a["symbol"] for a in best))
    cache = {s: load_1h(s) for s in syms}
    best = [a for a in best if cache.get(a["symbol"]) is not None]
    from collections import Counter
    by_bucket = Counter(a["bucket"] for a in best)
    print(f"BEST+STRONG maduras simulables: {len(best)} ({len(syms)} símbolos) "
          f"| {dict(by_bucket)}")

    def baseline(a):
        return a["price_7d"] / a["entry_price"] - 1 if a.get("price_7d") else None
    base = [baseline(a) for a in best]; base = [x for x in base if x is not None]
    print(f"\nBASELINE (hold ciego a 7d): n={len(base)} "
          f"avg {st.mean(base)*100:+.2f}% med {st.median(base)*100:+.2f}% "
          f"win {sum(1 for x in base if x>0)/len(base)*100:.0f}%")
    # baseline por bucket: ¿STRONG se comporta como BEST? (esperado: sí, score no discrimina)
    for b in BUCKETS:
        bb = [baseline(a) for a in best if a["bucket"] == b]
        bb = [x for x in bb if x is not None]
        if bb:
            print(f"  {b:7} n={len(bb):3} avg {st.mean(bb)*100:+.2f}% med {st.median(bb)*100:+.2f}% "
                  f"win {sum(1 for x in bb if x>0)/len(bb)*100:.0f}%")
    print()
    print(f"{'ARM':>4} {'TRAIL':>5} {'STOP':>4} | {'avg':>7} {'med':>7} {'win':>5} {'p10':>7} {'p90':>7}")
    grid = []
    for arm in (0.05, 0.06, 0.08, 0.12):
        for trail in (0.08, 0.12):
            for stop in (0.0, 0.05, 0.06, 0.07, 0.08):
                rs = [simulate(a, cache[a["symbol"]], arm, trail, stop) for a in best]
                rs = [x[0] for x in rs if x is not None]
                if not rs:
                    continue
                avg = st.mean(rs); med = st.median(rs)
                win = sum(1 for x in rs if x > 0) / len(rs) * 100
                p10 = st.quantiles(rs, n=10)[0]; p90 = st.quantiles(rs, n=10)[-1]
                grid.append((avg, arm, trail, stop, med, win, p10, p90))
                print(f"{arm:>4.2f} {trail:>5.2f} {stop:>4.2f} | {avg*100:>+6.2f}% {med*100:>+6.2f}% "
                      f"{win:>4.0f}% {p10*100:>+6.1f}% {p90*100:>+6.1f}%")
    print(f"\n(baseline avg {st.mean(base)*100:+.2f}% med {st.median(base)*100:+.2f}% "
          f"win {sum(1 for x in base if x>0)/len(base)*100:.0f}%)")
    grid.sort(reverse=True)
    print("\nTop 5 por avg realizado:")
    for avg, arm, trail, stop, med, win, p10, p90 in grid[:5]:
        print(f"  ARM {arm:.2f} TRAIL {trail:.2f} STOP {stop:.2f} -> avg {avg*100:+.2f}% "
              f"med {med*100:+.2f}% win {win:.0f}% p10 {p10*100:+.1f}%")
    # top-5 por p10: el stop protege la cola izquierda, no la media
    grid.sort(key=lambda g: g[6], reverse=True)
    print("\nTop 5 por p10 (cola izquierda — qué config corta mejor las pérdidas):")
    for avg, arm, trail, stop, med, win, p10, p90 in grid[:5]:
        print(f"  ARM {arm:.2f} TRAIL {trail:.2f} STOP {stop:.2f} -> p10 {p10*100:+.1f}% "
              f"avg {avg*100:+.2f}% med {med*100:+.2f}% win {win:.0f}%")

    # ── Comparación por TF (¿exit en 5m/15m cosecha más que en 1h?) ──────────
    tfs = sys.argv[1:] or ["1h", "15m"]
    tf_comparison(best, tfs)


def tf_comparison(best, tfs, arm=0.12, stop=0.0, trails=(0.05, 0.06, 0.08, 0.10)):
    """Mismo trailing, distinto TF de evaluación. Mide retorno realizado + giveback
    (mfe-realizado) sobre las MISMAS alertas. TF rápido gana solo si sube realizado Y
    baja giveback vs 1h. Baja 5m/15m a demanda (no están en el cache del backtest)."""
    from collections import defaultdict
    sym_alerts = defaultdict(list)
    for a in best:
        sym_alerts[a["symbol"]].append(a)
    spans = {}
    for s, alerts in sym_alerts.items():
        t0s = [datetime.fromisoformat(a["alerted_at"].replace("Z", "+00:00")) for a in alerts]
        spans[s] = (int(min(t0s).timestamp() * 1000),
                    int((max(t0s) + timedelta(days=WINDOW_DAYS)).timestamp() * 1000))

    print(f"\n{'='*70}\n COMPARACIÓN POR TF — exit en {'/'.join(tfs)} (ARM {arm:.0%} STOP {stop:.0%})\n{'='*70}")
    data = {}
    for tf in tfs:
        ok = 0
        for i, s in enumerate(sym_alerts, 1):
            df = load_tf(s, tf, span=spans[s])
            if df is not None:
                data[(tf, s)] = df; ok += 1
            if tf != "1h" and i % 25 == 0:
                print(f"  [{tf}] {i}/{len(sym_alerts)} símbolos...")
        print(f"  {tf}: {ok}/{len(sym_alerts)} símbolos con data")

    avail = [a for a in best if all((tf, a["symbol"]) in data for tf in tfs)]
    print(f"  alertas comparables en los {len(tfs)} TFs: {len(avail)}\n")
    print(f"  {'TF':>4} {'TRAIL':>5} | {'avg':>7} {'med':>7} {'win':>5} {'p10':>7} | {'giveback_med':>12}")
    print("  " + "-" * 58)
    for tf in tfs:
        for trail in trails:
            res = [simulate(a, data[(tf, a["symbol"])], arm, trail, stop) for a in avail]
            res = [r for r in res if r is not None]
            real = [r[0] for r in res]
            gb = [r[1] - r[0] for r in res]   # giveback = pico - realizado
            avg = st.mean(real); med = st.median(real)
            win = sum(1 for x in real if x > 0) / len(real) * 100
            p10 = st.quantiles(real, n=10)[0]; gbmed = st.median(gb)
            print(f"  {tf:>4} {trail:>5.2f} | {avg*100:>+6.2f}% {med*100:>+6.2f}% {win:>4.0f}% "
                  f"{p10*100:>+6.1f}% | {gbmed*100:>+9.1f}pp")
        print()


if __name__ == "__main__":
    main()
