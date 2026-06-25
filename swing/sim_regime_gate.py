"""
Prototipo — GATE DE ENTRADA POR RÉGIMEN BTC (el lever con evidencia de mover EV).

Tesis (validada parcial en meta-labeling + cierre de #1): operar swing longs en BTC down-regime
es bajo-EV (avg +0.48% vs +2.24% en up). Gate = suprimir/throttlear entradas en down-regime.

Valida lo que quedó abierto:
  (a) ¿régimen→EV es robusto y MONÓTONO? binario (BTC vs EMA168) Y continuo (dist BTC/EMA) → decide
      gate duro vs throttle proporcional.
  (b) costo de CATCH: ¿cuántos movers únicos se pierden al suprimir down-regime? (per-símbolo, honesto).
  (c) gate OOS: split cronológico, baseline-all vs up-only — EV/win en TEST.

Exit shipeado ARM12/TRAIL8/STOP0. NO toca producción. py -3.13. Diag por argv (default diag_16w.json).
"""
import json, sys, statistics as st
from collections import defaultdict
from datetime import datetime
import numpy as np

from sim_exit import load_1h, simulate, MATURE_BEFORE, BUCKETS
from meta_label import EMA_BARS


def btc_regime_series(btc):
    c = btc["close"].astype(float)
    ema = c.ewm(span=EMA_BARS, adjust=False).mean()
    return btc["open_time"].to_numpy(), c.to_numpy(), ema.to_numpy()


def reg_at(ot, close, ema, t_ms):
    """dist = close/ema - 1 en la última barra <= t (continuo, signo = up/down). None si no hay."""
    idx = np.searchsorted(ot, t_ms, side="right") - 1
    if idx < 0:
        return None
    return close[idx] / ema[idx] - 1.0


def show(tag, arr):
    if not arr:
        print(f"  {tag:34} (vacío)"); return
    a = np.array(arr)
    print(f"  {tag:34} n={len(a):4}  avg {a.mean()*100:+.2f}%  med {np.median(a)*100:+.2f}%  "
          f"win {(a>0).mean()*100:.0f}%  p10 {np.quantile(a,0.1)*100:+.1f}%")


def main():
    diag = sys.argv[1] if len(sys.argv) > 1 else "diag_16w.json"
    d = json.load(open(diag))["main"]
    alerts = [a for a in d if a.get("bucket") in BUCKETS and a.get("entry_price")
              and datetime.fromisoformat(a["alerted_at"].replace("Z", "+00:00")) <= MATURE_BEFORE]
    alerts.sort(key=lambda a: a["alerted_at"])
    syms = sorted(set(a["symbol"] for a in alerts))
    print(f"Diag: {diag} | cargando klines de {len(syms)} símbolos...")
    cache = {s: load_1h(s) for s in syms}
    alerts = [a for a in alerts if cache.get(a["symbol"]) is not None]

    btc = load_1h("BTCUSDT")
    ot, close, ema = btc_regime_series(btc)
    for a in alerts:
        t = int(datetime.fromisoformat(a["alerted_at"].replace("Z", "+00:00")).timestamp() * 1000)
        a["_dist"] = reg_at(ot, close, ema, t)
        r = simulate(a, cache[a["symbol"]], 0.12, 0.08, 0.0)
        a["_rz"] = r[0] if r is not None else None
        a["_move"] = (a["max_high_7d"] / a["entry_price"] - 1) if a.get("max_high_7d") else None
    A = [a for a in alerts if a["_rz"] is not None and a["_dist"] is not None]
    dist = np.array([a["_dist"] for a in A])
    rz = np.array([a["_rz"] for a in A])
    print(f"Alertas simulables: {len(A)}  | BTC up (dist>0): {(dist>0).mean()*100:.0f}%\n")

    # ── (a) ¿monótono? binario + quintiles de dist continua ──────────────────
    print("=" * 64)
    print(" (a) P&L realizado por RÉGIMEN — ¿binario y continuo son monótonos?")
    print("=" * 64)
    show("BTC up   (dist>0)", rz[dist > 0].tolist())
    show("BTC down (dist<0)", rz[dist <= 0].tolist())
    print("\n  por quintil de dist (BTC/EMA-1, continuo, asc):")
    qs = np.quantile(dist, [0, .2, .4, .6, .8, 1.0])
    for i in range(5):
        m = (dist >= qs[i]) & (dist <= qs[i+1] if i == 4 else dist < qs[i+1])
        show(f"  Q{i+1} dist[{qs[i]*100:+.1f}%,{qs[i+1]*100:+.1f}%]", rz[m].tolist())

    # ── (b) costo de catch: movers únicos del down-regime (per-símbolo) ──────
    print(f"\n{'='*64}\n (b) costo de CATCH — movers que SOLO aparecen en down-regime\n{'='*64}")
    sym_up = defaultdict(lambda: False); sym_dn_move = defaultdict(float); sym_up_move = defaultdict(float)
    for a in A:
        s = a["symbol"]; mv = a["_move"] or 0
        if a["_dist"] > 0:
            sym_up_move[s] = max(sym_up_move[s], mv)
        else:
            sym_dn_move[s] = max(sym_dn_move[s], mv)
    allsy = set(sym_up_move) | set(sym_dn_move)
    movers = [s for s in allsy if max(sym_up_move.get(s,0), sym_dn_move.get(s,0)) >= 0.20]
    only_dn = [s for s in movers if sym_up_move.get(s,0) < 0.20 and sym_dn_move.get(s,0) >= 0.20]
    print(f"  símbolos movers>=20%: {len(movers)}  |  que SOLO califican en down-regime: {len(only_dn)}")
    print(f"  → suprimir down-regime perdería {len(only_dn)}/{len(movers)} movers ({len(only_dn)/len(movers)*100:.0f}%)")

    # ── (c) gate OOS: split cronológico, all vs up-only ──────────────────────
    cut = int(len(A) * 0.70)
    test = A[cut:]
    print(f"\n{'='*64}\n (c) gate OOS — split cronológico, TEST n={len(test)}\n{'='*64}")
    show("TODAS (sin gate)", [a["_rz"] for a in test])
    show("solo up-regime (gate duro)", [a["_rz"] for a in test if a["_dist"] > 0])
    # throttle continuo: solo dist por encima de la mediana de TRAIN
    thr = np.median([a["_dist"] for a in A[:cut]])
    show(f"solo dist>mediana train ({thr*100:+.1f}%)", [a["_rz"] for a in test if a["_dist"] > thr])
    # BANDA derivada de TRAIN [q40,q80] (excluye deep-down Y sobre-extendido), aplicada a TEST
    train_dist = [a["_dist"] for a in A[:cut]]
    lo_t, hi_t = np.quantile(train_dist, [0.40, 0.80])
    show(f"BANDA dist[{lo_t*100:+.1f}%,{hi_t*100:+.1f}%] (train Q3-Q4)",
         [a["_rz"] for a in test if lo_t <= a["_dist"] <= hi_t])
    # contraste: ¿el sobre-extendido (>q80 train) es malo también en TEST?
    show(f"  sobre-extendido dist>{hi_t*100:+.1f}% (test)", [a["_rz"] for a in test if a["_dist"] > hi_t])

    # ── (d) walk-forward: ¿el gate aguanta en varios períodos? ───────────────
    print(f"\n{'='*64}\n (d) WALK-FORWARD (5 folds) — avg TEST: all vs up vs banda\n{'='*64}")
    n = len(A); start = int(n * 0.40)
    bounds = np.linspace(start, n, 6, dtype=int)
    print(f"  {'fold':>4} | {'n_te':>4} | {'all':>7} {'up-regime':>9} {'banda Q3-Q4':>12}")
    wins = {"up": 0, "band": 0}
    for i in range(5):
        a0, a1 = bounds[i], bounds[i+1]
        tr = A[:a0]; te = A[a0:a1]
        if len(te) < 20:
            continue
        td = [a["_dist"] for a in tr]
        lo, hi = np.quantile(td, [0.40, 0.80])
        all_avg = np.mean([a["_rz"] for a in te]) * 100
        up = [a["_rz"] for a in te if a["_dist"] > 0]
        bd = [a["_rz"] for a in te if lo <= a["_dist"] <= hi]
        up_avg = np.mean(up) * 100 if up else float("nan")
        bd_avg = np.mean(bd) * 100 if bd else float("nan")
        wins["up"] += up_avg > all_avg
        wins["band"] += bd_avg > all_avg
        print(f"  {i+1:>4} | {len(te):>4} | {all_avg:>+6.2f}% {up_avg:>+8.2f}% {bd_avg:>+11.2f}%")
    print(f"  → folds donde el gate bate a 'all':  up-regime {wins['up']}/5   banda {wins['band']}/5")


if __name__ == "__main__":
    main()
