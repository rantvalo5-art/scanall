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
import json, glob, statistics as st
from datetime import datetime, timezone, timedelta
import pandas as pd

WINDOW_DAYS = 7
NOW = datetime.now(timezone.utc)
MATURE_BEFORE = NOW - timedelta(days=WINDOW_DAYS)

def load_1h(symbol):
    fs = glob.glob(f".backtest_cache/klines_{symbol}_1h_*.pkl")
    if not fs:
        return None
    # el de mayor span (menor start_ms)
    f = min(fs, key=lambda p: int(p.split("_1h_")[1].split("_")[0]))
    df = pd.read_pickle(f)
    return df[["open_time", "high", "low", "close"]].sort_values("open_time").reset_index(drop=True)

def simulate(alert, df1h, arm, trail, stop):
    e = alert.get("entry_price")
    if not e:
        return None
    t0 = datetime.fromisoformat(alert["alerted_at"].replace("Z", "+00:00"))
    t0_ms = int(t0.timestamp() * 1000)
    t1_ms = int((t0 + timedelta(days=WINDOW_DAYS)).timestamp() * 1000)
    fwd = df1h[(df1h.open_time > t0_ms) & (df1h.open_time <= t1_ms)]
    if len(fwd) < 3:
        return None
    run_max = e
    armed = False
    for _, k in fwd.iterrows():
        hi, lo, cl = k.high, k.low, k.close
        # stop duro pre-armado
        if stop and not armed and lo <= e * (1 - stop):
            return -stop
        run_max = max(run_max, hi)
        if not armed and run_max / e - 1 >= arm:
            armed = True
        if armed and cl <= run_max * (1 - trail):
            return cl / e - 1
    return float(fwd.close.iloc[-1]) / e - 1  # hold a fin de ventana

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
                rs = [x for x in rs if x is not None]
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

if __name__ == "__main__":
    main()
