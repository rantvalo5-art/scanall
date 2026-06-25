"""
NUEVA DIRECCIÓN (datos ortogonales) — Relative-strength cross-sectional como predictor de NET.

El score no predice outcome (Spearman≈0) y re-cortar la entrada falló siempre. Pero RS usa
información que el score NO contiene: el rank del símbolo vs el MERCADO. Es el factor más robusto
de la literatura cripto y es ortogonal a atr (volatilidad) y al score. Análogo al funding (dato
ortogonal que sí movió EV como filtro).

Pregunta: ¿la alta fuerza relativa al ENTRAR predice mejor NET realizado (exit trailing shipeado)?
RS(sym,t) = retorno trailing W barras del símbolo − retorno trailing W del mercado (mediana
transversal), todo as-of t (sin leakage). Outcome = `realized` de sim_portfolio_exits.json.

Barra: quintiles + AUC + walk-forward (¿un gate pro-RS bate a 'all' en los folds, incl. bajista?).
Reusa build_return_matrix de sim_leadlag. Corre con `py -3.13`.
"""
import json, sys, statistics as st
import numpy as np

from sim_leadlag import build_return_matrix, HOUR_MS, parse_dt

EXITS = "sim_portfolio_exits.json"
WINS = (24 * 7, 24 * 14)     # ventanas trailing en barras 1h: 7d y 14d


def auc(pos, neg):
    """Mann-Whitney AUC de que un score separa pos de neg."""
    if not pos or not neg:
        return float("nan")
    allv = sorted([(v, 1) for v in pos] + [(v, 0) for v in neg])
    r = 0.0; i = 0; n = len(allv)
    while i < n:
        j = i
        while j < n and allv[j][0] == allv[i][0]:
            j += 1
        avg_rank = (i + j - 1) / 2 + 1
        for k in range(i, j):
            if allv[k][1] == 1:
                r += avg_rank
        i = j
    n1 = len(pos)
    return (r - n1 * (n1 + 1) / 2) / (n1 * len(neg))


def main():
    rows = json.load(open(EXITS))
    t0 = min(r["entry_ms"] for r in rows)
    t1 = max(r["entry_ms"] for r in rows)
    grid, syms, R, C = build_return_matrix(t0, t1)
    col = {s: j for j, s in enumerate(syms)}
    T = len(grid)
    gt0 = grid[0]

    # retorno trailing por ventana, matriz [T,N], y mercado (mediana transversal) por barra
    trail = {}
    mkt = {}
    for W in WINS:
        tr = np.full((T, len(syms)), np.nan)
        tr[W:] = C[W:] / C[:-W] - 1.0
        trail[W] = tr
        mkt[W] = np.nanmedian(tr, axis=1)

    # join: cada alerta → RS as-of entry
    data = []  # (rs7, rs14, realized)
    miss = 0
    for r in rows:
        s = r["symbol"]
        if s not in col:
            miss += 1; continue
        j = col[s]
        gi = int(round((r["entry_ms"] - gt0) / HOUR_MS))
        if gi < max(WINS) or gi >= T:
            continue
        rs = {}
        ok = True
        for W in WINS:
            sym_tr = trail[W][gi, j]
            if not np.isfinite(sym_tr) or not np.isfinite(mkt[W][gi]):
                ok = False; break
            rs[W] = sym_tr - mkt[W][gi]
        if not ok:
            continue
        data.append((rs[WINS[0]], rs[WINS[1]], r["realized"], r["entry_ms"]))
    print(f"Alertas con RS computable: {len(data)} (sin cobertura: {miss})\n")

    real = np.array([d[2] for d in data])
    print(f"NET realizado global: media {real.mean()*100:+.2f}% | mediana {np.median(real)*100:+.2f}% | "
          f"win {np.mean(real>=0)*100:.0f}%\n")

    for wi, W in enumerate(WINS):
        rs = np.array([d[wi] for d in data])
        print(f"=== RS trailing {W//24}d (símbolo − mercado) ===")
        # quintiles de RS → NET
        qs = np.quantile(rs, [0.2, 0.4, 0.6, 0.8])
        print(f"  {'quintil':>8} {'n':>5} {'RS medio':>9} {'NET med':>8} {'NET mean':>9} {'win%':>6}")
        for q in range(5):
            lo = -np.inf if q == 0 else qs[q-1]
            hi = np.inf if q == 4 else qs[q]
            m = (rs >= lo) & (rs < hi) if q < 4 else (rs >= lo)
            rr = real[m]
            if len(rr):
                print(f"  Q{q+1:>6} {len(rr):>5} {rs[m].mean()*100:>+8.1f}% "
                      f"{np.median(rr)*100:>+7.2f}% {rr.mean()*100:>+8.2f}% {np.mean(rr>=0)*100:>5.0f}%")
        # AUC: ¿RS separa ganadores (NET>=0) de perdedores?
        pos = list(rs[real >= 0]); neg = list(rs[real < 0])
        print(f"  AUC(RS → NET>=0) = {auc(pos, neg):.3f}   "
              f"corr(RS, NET) = {np.corrcoef(rs, real)[0,1]:+.3f}\n")

    # walk-forward: gates pro-RS por percentil de TRAIN (expanding, sin leakage), RS 14d
    data.sort(key=lambda d: d[3])
    rs14 = np.array([d[1] for d in data]); rl = np.array([d[2] for d in data])
    ts = np.array([d[3] for d in data])
    ta, tb = ts.min(), ts.max(); span = (tb - ta) // 4
    for pct, lbl in [(50, "mediana"), (80, "top-quintil q80")]:
        print(f"\n{'='*64}\n  WALK-FORWARD gate pro-RS (keep RS >= {lbl}-train), RS 14d, 4 folds\n{'='*64}")
        print(f"  {'fold':>4} {'n':>5} | {'ALL net':>8} {'win%':>5} | {'gate net':>9} {'win%':>5} {'kept':>5} | {'Δmean':>7}")
        wins = 0; ev = 0
        for f in range(4):
            lo = ta + f*span; hi = tb+1 if f == 3 else ta+(f+1)*span
            train_m = ts < lo
            test_m = (ts >= lo) & (ts < hi)
            if test_m.sum() < 20 or train_m.sum() < 50:
                print(f"  {f+1:>4} {test_m.sum():>5} | (train/test insuficiente)")
                continue
            ev += 1
            thr = np.percentile(rs14[train_m], pct)
            all_net = rl[test_m].mean()
            keep = test_m & (rs14 >= thr)
            kept_net = rl[keep].mean() if keep.sum() else float("nan")
            d = (kept_net - all_net) * 100
            wins += d > 0
            print(f"  {f+1:>4} {test_m.sum():>5} | {all_net*100:>+7.2f}% {np.mean(rl[test_m]>=0)*100:>4.0f}% | "
                  f"{kept_net*100:>+8.2f}% {np.mean(rl[keep]>=0)*100:>4.0f}% {keep.sum():>5} | {d:>+6.2f}")
        print(f"   → gate {lbl} bate a 'all': {wins}/{ev}")


if __name__ == "__main__":
    main()
