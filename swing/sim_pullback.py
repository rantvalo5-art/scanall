"""
Prototipo idea #5 (Tier 2, lateness) — RE-ENTRADA EN PULLBACK para señales tardías.

Las tardías (BREAKOUT/HOLD) persiguen el techo → giveback. Tesis: en vez de entrar en el
breakout (precio de chase), esperar un pullback de P% y entrar ahí (mejor RR). Riesgo medido:
los runners que NO retroceden se pierden (y suelen ser los mejores movers) → mido EV
INCONDICIONAL (0 cuando no llena), no solo la calidad de los fills.

Compara, sobre las MISMAS alertas tardías, el exit shipeado (ARM12/TRAIL8/STOP0):
  - baseline: entrar en señal (siempre llena).
  - pullback(P,W): orden límite a entry*(1-P); si la toca dentro de W horas, entra ahí y corre
    el exit hasta 7d-desde-señal (el pullback come ventana, costo realista); si no, MISS (=0).

NO toca producción. py -3.13. Diag por argv (default diag_16w.json).
"""
import json, sys, statistics as st
from datetime import datetime, timedelta
import numpy as np

from sim_exit import load_1h, WINDOW_DAYS, MATURE_BEFORE, BUCKETS

GLOBAL = (0.12, 0.08, 0.0)
LATE = {"BREAKOUT", "HOLD"}
PCTS = (0.02, 0.04, 0.06)
WINDOWS_H = (12, 24, 48)


def sim_from(df, entry_price, start_ms, end_ms, arm, trail, stop):
    """Exit (ARM/TRAIL/STOP) entrando a entry_price desde start_ms hasta end_ms. Frac realizada."""
    fwd = df[(df.open_time > start_ms) & (df.open_time <= end_ms)]
    if len(fwd) < 2:
        return None
    run_max = entry_price
    armed = False
    for k in fwd.itertuples():
        if stop and not armed and k.low <= entry_price * (1 - stop):
            return -stop
        run_max = max(run_max, k.high)
        if not armed and run_max / entry_price - 1 >= arm:
            armed = True
        if armed and k.close <= run_max * (1 - trail):
            return k.close / entry_price - 1
    return float(fwd.close.iloc[-1]) / entry_price - 1


def find_pullback(df, entry_price, sig_ms, window_h, pct):
    """Primera barra 1h dentro de la ventana cuyo low toca entry*(1-pct). Devuelve (fill_price, fill_ms) o None."""
    win_end = sig_ms + window_h * 3600 * 1000
    seg = df[(df.open_time > sig_ms) & (df.open_time <= win_end)]
    target = entry_price * (1 - pct)
    for k in seg.itertuples():
        if k.low <= target:
            return target, int(k.open_time)
    return None


def main():
    diag = sys.argv[1] if len(sys.argv) > 1 else "diag_16w.json"
    d = json.load(open(diag))["main"]
    alerts = [a for a in d if a.get("bucket") in BUCKETS and a.get("entry_price")
              and (a.get("signal_type") or a.get("label")) in LATE
              and datetime.fromisoformat(a["alerted_at"].replace("Z", "+00:00")) <= MATURE_BEFORE]
    syms = sorted(set(a["symbol"] for a in alerts))
    print(f"Diag: {diag} | señales TARDÍAS (BREAKOUT/HOLD) maduras: {len(alerts)} ({len(syms)} símbolos)")
    cache = {s: load_1h(s) for s in syms}
    alerts = [a for a in alerts if cache.get(a["symbol"]) is not None]

    # baseline: entrar en señal
    base = []
    for a in alerts:
        df = cache[a["symbol"]]
        sig_ms = int(datetime.fromisoformat(a["alerted_at"].replace("Z", "+00:00")).timestamp() * 1000)
        end_ms = sig_ms + WINDOW_DAYS * 86400 * 1000
        r = sim_from(df, a["entry_price"], sig_ms, end_ms, *GLOBAL)
        if r is not None:
            base.append(r)
    print(f"\nBASELINE entrar-en-señal: n={len(base)}  avg {st.mean(base)*100:+.2f}%  "
          f"med {st.median(base)*100:+.2f}%  win {np.mean(np.array(base)>0)*100:.0f}%  "
          f"p10 {st.quantiles(base,n=10)[0]*100:+.1f}%")

    print(f"\n{'='*74}\n PULLBACK re-entry — EV incondicional (MISS = 0) vs baseline\n{'='*74}")
    print(f"  {'P':>4} {'W(h)':>5} | {'fill%':>5} | {'EV uncond':>9} | "
          f"{'fill: avg':>9} {'med':>7} {'win':>5} | {'giveback↓?':>10}")
    for pct in PCTS:
        for wh in WINDOWS_H:
            fills, missed = [], 0
            for a in alerts:
                df = cache[a["symbol"]]
                sig_ms = int(datetime.fromisoformat(a["alerted_at"].replace("Z", "+00:00")).timestamp() * 1000)
                end_ms = sig_ms + WINDOW_DAYS * 86400 * 1000
                pb = find_pullback(df, a["entry_price"], sig_ms, wh, pct)
                if pb is None:
                    missed += 1
                    continue
                fill_price, fill_ms = pb
                r = sim_from(df, fill_price, fill_ms, end_ms, *GLOBAL)
                if r is not None:
                    fills.append(r)
            n = len(fills) + missed
            fill_rate = len(fills) / n * 100 if n else 0
            ev = (sum(fills) + 0 * missed) / n if n else 0   # MISS cuenta como 0
            if fills:
                fa, fm = st.mean(fills), st.median(fills)
                fw = np.mean(np.array(fills) > 0) * 100
            else:
                fa = fm = fw = 0
            print(f"  {pct*100:>3.0f}% {wh:>5} | {fill_rate:>4.0f}% | {ev*100:>+8.2f}% | "
                  f"{fa*100:>+8.2f}% {fm*100:>+6.2f}% {fw:>4.0f}% |")
    print(f"\n  (baseline EV = avg {st.mean(base)*100:+.2f}% con fill 100%)")


if __name__ == "__main__":
    main()
