"""
Prototipo idea #2 — ¿rankear por atr_pct_1d bate al score?

Sabemos: score anti-predictivo (AUC ~0.45), atr_pct_1d único separador robusto de movers
(AUC ~0.66). Acá lo aterrizo a la decisión real del screener: a presupuesto FIJO de alertas
(top-K), ¿qué ranking agarra más movers? Mido AUC + precision@K + recall, per-ALERTA y
per-SÍMBOLO (la métrica honesta, lección de la sesión). NO toca producción. py -3.13.
"""
import json, sys, statistics as st
from datetime import datetime
import numpy as np
from meta_label import auc
from sim_exit import simulate, load_1h, MATURE_BEFORE, BUCKETS

THRS = (0.10, 0.20, 0.30)   # umbral de "mover" sobre max_high_7d / entry


def precision_recall_at_k(y, rank_key, Ks):
    order = np.argsort(-rank_key)
    yo = y[order]
    tot = y.sum()
    out = []
    for K in Ks:
        topk = yo[:K]
        out.append((K, topk.sum() / K, topk.sum() / tot))  # K, precision, recall
    return out


def main():
    diag = sys.argv[1] if len(sys.argv) > 1 else "diag_16w.json"
    d = json.load(open(diag))["main"]
    A = [a for a in d if a.get("entry_price") and a.get("atr_pct_1d") is not None
         and a.get("max_high_7d")]
    print(f"Diag: {diag} | alertas con forward+atr: {len(A)}")

    move = np.array([a["max_high_7d"] / a["entry_price"] - 1 for a in A])
    score = np.array([a["score"] for a in A], float)
    atr = np.array([a["atr_pct_1d"] for a in A], float)
    bucket = np.array([a.get("bucket") for a in A])

    print(f"\n{'='*58}\n AUC para predecir 'mover' (per-ALERTA, n={len(A)})\n{'='*58}")
    print(f"  {'umbral':>8} {'base+':>6} | {'score':>7} {'atr_pct_1d':>11}")
    for thr in THRS:
        y = (move >= thr).astype(float)
        print(f"  >={thr*100:>4.0f}%  {y.mean()*100:>5.1f}% | {auc(y,score):>7.3f} {auc(y,atr):>11.3f}")

    # ¿atr predice el NETO (price_7d) o solo el MFE (max_high)?
    net = np.array([(a["price_7d"]/a["entry_price"]-1) if a.get("price_7d") else np.nan for a in A])
    nm = ~np.isnan(net)
    print(f"  -- mismo, sobre NETO (price_7d) en vez de MFE (n={nm.sum()}):")
    for thr in (0.0, 0.10):
        y = (net[nm] >= thr).astype(float)
        print(f"  net>={thr*100:>4.0f}%  {y.mean()*100:>5.1f}% | {auc(y,score[nm]):>7.3f} {auc(y,atr[nm]):>11.3f}")

    # precision@K: a presupuesto fijo, score vs atr (per-ALERTA)
    n_best = int((bucket == "BEST").sum())
    n_bs = int(np.isin(bucket, ["BEST", "STRONG"]).sum())
    Ks = sorted(set([n_best, n_bs, 200, 400]))
    print(f"\n{'='*58}\n precision@K / recall@K (per-ALERTA, mover>=20%)\n{'='*58}")
    print(f"  (K=BEST actual={n_best}, K=BEST+STRONG={n_bs})")
    y20 = (move >= 0.20).astype(float)
    print(f"  {'K':>5} | {'prec score':>11} {'rec score':>10} | {'prec atr':>9} {'rec atr':>8}")
    psr = {k: (p, r) for k, p, r in precision_recall_at_k(y20, score, Ks)}
    par = {k: (p, r) for k, p, r in precision_recall_at_k(y20, atr, Ks)}
    for K in Ks:
        print(f"  {K:>5} | {psr[K][0]*100:>10.1f}% {psr[K][1]*100:>9.1f}% | "
              f"{par[K][0]*100:>8.1f}% {par[K][1]*100:>7.1f}%")

    # baseline: el bucket BEST actual (score+gates) — ¿qué precision/recall da?
    bm = (bucket == "BEST")
    print(f"\n  baseline bucket BEST actual: n={bm.sum()} "
          f"prec {y20[bm].mean()*100:.1f}% rec {y20[bm].sum()/y20.sum()*100:.1f}%")

    # ── per-SÍMBOLO (métrica honesta): dedup al mejor atr y mejor score por símbolo ──
    print(f"\n{'='*58}\n per-SÍMBOLO (dedup; mover = best move del símbolo >=20%)\n{'='*58}")
    from collections import defaultdict
    sym_move = defaultdict(float); sym_atr = {}; sym_score = defaultdict(float)
    for a in A:
        s = a["symbol"]; m = a["max_high_7d"]/a["entry_price"]-1
        sym_move[s] = max(sym_move[s], m)
        sym_score[s] = max(sym_score[s], a["score"])
        # atr representativo: el del alerta de mayor atr (proxy de convicción del símbolo)
        if s not in sym_atr or a["atr_pct_1d"] > sym_atr[s]:
            sym_atr[s] = a["atr_pct_1d"]
    syms = list(sym_move)
    ym = np.array([1.0 if sym_move[s] >= 0.20 else 0.0 for s in syms])
    sc = np.array([sym_score[s] for s in syms]); at = np.array([sym_atr[s] for s in syms])
    print(f"  símbolos: {len(syms)} | movers>=20%: {int(ym.sum())} ({ym.mean()*100:.0f}%)")
    print(f"  AUC score {auc(ym,sc):.3f} | AUC atr_pct_1d {auc(ym,at):.3f}")
    for K in sorted(set([30, 50, 80, int(ym.sum())])):
        prs = {k:(p,r) for k,p,r in precision_recall_at_k(ym, sc, [K])}[K]
        pra = {k:(p,r) for k,p,r in precision_recall_at_k(ym, at, [K])}[K]
        print(f"  top-{K:>3}: score prec {prs[0]*100:>4.0f}% rec {prs[1]*100:>4.0f}%  | "
              f"atr prec {pra[0]*100:>4.0f}% rec {pra[1]*100:>4.0f}%")

    realized_topk(A)


def realized_topk(A):
    """LA validación de #2: a presupuesto fijo de alertas (top-K), ¿rankear por atr da mejor
    P&L REALIZADO (exit shipeado ARM12/TRAIL8/STOP0) que por score? Cierra el loop MFE+exit."""
    mat = [a for a in A if a.get("bucket") in BUCKETS
           and datetime.fromisoformat(a["alerted_at"].replace("Z", "+00:00")) <= MATURE_BEFORE]
    syms = sorted(set(a["symbol"] for a in mat))
    print(f"\n{'='*58}\n P&L REALIZADO top-K (exit ARM12/TRAIL8/STOP0) — atr vs score\n{'='*58}")
    print(f" cargando klines de {len(syms)} símbolos...")
    cache = {s: load_1h(s) for s in syms}
    rows = []
    for a in mat:
        df = cache.get(a["symbol"])
        if df is None:
            continue
        r = simulate(a, df, 0.12, 0.08, 0.0)
        if r is not None:
            rows.append((a["score"], a["atr_pct_1d"], r[0]))
    if not rows:
        print("  sin datos"); return
    sc = np.array([x[0] for x in rows]); at = np.array([x[1] for x in rows])
    rz = np.array([x[2] for x in rows])
    print(f" alertas maduras simulables: {len(rows)}")
    def topk_pnl(key, K):
        idx = np.argsort(-key)[:K]; sub = rz[idx]
        return st.mean(sub)*100, st.median(sub)*100, np.mean(sub>0)*100
    print(f"\n  {'K':>5} | {'score: avg':>10} {'med':>7} {'win':>5} | {'atr: avg':>9} {'med':>7} {'win':>5} | {'rnd avg':>7}")
    rng = np.random.default_rng(0)
    for K in (100, 200, 400, 800):
        if K > len(rows): continue
        s_a,s_m,s_w = topk_pnl(sc, K); a_a,a_m,a_w = topk_pnl(at, K)
        rnd = st.mean([st.mean(rz[rng.choice(len(rz),K,replace=False)]) for _ in range(20)])*100
        print(f"  {K:>5} | {s_a:>+9.2f}% {s_m:>+6.2f}% {s_w:>4.0f}% | "
              f"{a_a:>+8.2f}% {a_m:>+6.2f}% {a_w:>4.0f}% | {rnd:>+6.2f}%")

    # ── curva por BANDA de atr_pct_1d: ¿dónde está el sweet spot? ¿5.0 es óptimo? ──
    print(f"\n  P&L realizado por BANDA de atr_pct_1d (umbral gate actual = 5.0):")
    print(f"  {'banda':>10} | {'n':>4} {'avg':>7} {'med':>7} {'win':>5} {'mover>=20%':>10}")
    bands = [(0,3),(3,5),(5,7),(7,10),(10,15),(15,999)]
    for lo,hi in bands:
        m = (at>=lo)&(at<hi)
        if m.sum()<10: continue
        sub=rz[m]
        mov = np.mean([1 if x>0 else 0 for x in sub])  # placeholder
        print(f"  {lo:>4}-{hi if hi<999 else '+':<4} | {int(m.sum()):>4} {st.mean(sub)*100:>+6.2f}% "
              f"{st.median(sub)*100:>+6.2f}% {np.mean(sub>0)*100:>4.0f}%")


if __name__ == "__main__":
    main()
