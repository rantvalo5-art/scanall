"""
Prototipo — Triple-Barrier + Meta-Labeling (López de Prado) sobre las alertas del swing.

Pregunta que responde: ¿las features de ENTRADA separan ganadores de perdedores mejor
que el `score`? (sabemos que el score tiene Spearman~0 con el outcome). Si un meta-modelo
da AUC > 0.5 out-of-sample, podemos filtrar alertas predichas-perdedoras y subir avg/win
SIN tocar el scoring.

Pipeline:
  1. Etiquetado triple-barrier por alerta: con klines 1h forward (<=7d), barreras
     TP=+k*ATR y SL=-k*ATR (ATR = atr_pct_1d). Gana la que toca primero; si ninguna,
     label por signo del close final. -> y in {0,1}.
  2. Features SOLO de entrada (sin leakage de forward): atr_pct_1d, bb_width,
     breakout_distance, cvd_ratio, dist_to_res, obv_slope, vol_ratio, score, htf_1d_up,
     + one-hot de signal_type. (htf_1w_up se excluye: inerte.)
  3. Split CRONOLÓGICO 70/30 (no aleatorio: evita leakage temporal).
  4. Logistic regression regularizada (hand-rolled, numpy) -> AUC OOS del meta-modelo
     vs AUC del score solo.
  5. Test práctico: bajo el exit de producción (ARM12/TRAIL8/STOP0), ¿filtrar por la
     prob del meta-modelo (umbral elegido en train) mejora avg/med/win OOS vs tomar todo?

NO toca producción ni envía nada. Corre con `py -3.13` (numpy/pandas; sin sklearn).
"""
import json, sys, statistics as st
from datetime import datetime, timezone, timedelta
import numpy as np

from sim_exit import load_1h, simulate, WINDOW_DAYS, MATURE_BEFORE, BUCKETS

# exit de producción para el test práctico de P&L
ARM, TRAIL, STOP = 0.12, 0.08, 0.0
TB_MULT = 2.0           # múltiplos de ATR para las barreras TP/SL

# features de entrada (numéricas) + booleanas; one-hot de signal_type aparte
NUM_FEATS = ["atr_pct_1d", "bb_width", "breakout_distance", "cvd_ratio",
             "dist_to_res", "obv_slope", "vol_ratio", "score"]
BOOL_FEATS = ["htf_1d_up"]
SIGNAL_TYPES = ["BREAKOUT", "HOLD", "RIDING", "PREBREAK", "COILING"]


def triple_barrier(alert, df):
    """Etiqueta 1/0 con triple-barrier sobre klines 1h forward <=7d.
    Barreras simetricas en multiplos de ATR (atr_pct_1d, en %). Devuelve None si no hay data."""
    e = alert.get("entry_price")
    atr = alert.get("atr_pct_1d")
    if not e or not atr:
        return None
    atr_f = atr / 100.0
    tp = e * (1 + TB_MULT * atr_f)
    sl = e * (1 - TB_MULT * atr_f)
    t0 = datetime.fromisoformat(alert["alerted_at"].replace("Z", "+00:00"))
    t0_ms = int(t0.timestamp() * 1000)
    t1_ms = int((t0 + timedelta(days=WINDOW_DAYS)).timestamp() * 1000)
    fwd = df[(df.open_time > t0_ms) & (df.open_time <= t1_ms)]
    if len(fwd) < 3:
        return None
    for _, k in fwd.iterrows():
        if k.high >= tp:
            return 1
        if k.low <= sl:
            return 0
    return 1 if fwd.close.iloc[-1] > e else 0


def featurize(alert):
    """Vector de features de entrada (sin forward). NaN -> 0 (imputación simple)."""
    row = []
    for f in NUM_FEATS:
        v = alert.get(f)
        row.append(float(v) if v is not None else 0.0)
    for f in BOOL_FEATS:
        row.append(1.0 if alert.get(f) else 0.0)
    st_ = alert.get("signal_type") or alert.get("label")
    for s in SIGNAL_TYPES:
        row.append(1.0 if st_ == s else 0.0)
    return row


FEAT_NAMES = NUM_FEATS + BOOL_FEATS + [f"sig_{s}" for s in SIGNAL_TYPES]


def standardize(X, mu=None, sd=None):
    if mu is None:
        mu = X.mean(axis=0)
        sd = X.std(axis=0)
        sd[sd == 0] = 1.0
    return (X - mu) / sd, mu, sd


def fit_logreg(X, y, l2=1.0, lr=0.1, iters=4000):
    """Regresión logística con descenso de gradiente + L2. X ya estandarizado, con bias agregado fuera."""
    n, d = X.shape
    w = np.zeros(d)
    for _ in range(iters):
        z = X @ w
        p = 1.0 / (1.0 + np.exp(-z))
        grad = X.T @ (p - y) / n
        grad[1:] += l2 * w[1:] / n      # no regularizar el bias (col 0)
        w -= lr * grad
    return w


def predict(X, w):
    return 1.0 / (1.0 + np.exp(-(X @ w)))


def auc(y, p):
    """ROC-AUC via Mann-Whitney (rank-sum). Robusto, sin sklearn."""
    y = np.asarray(y); p = np.asarray(p)
    pos = p[y == 1]; neg = p[y == 0]
    if len(pos) == 0 or len(neg) == 0:
        return float("nan")
    order = np.argsort(p)
    ranks = np.empty_like(order, dtype=float)
    ranks[order] = np.arange(1, len(p) + 1)
    # promediar ranks de empates
    _, inv, cnt = np.unique(p, return_inverse=True, return_counts=True)
    csum = np.cumsum(cnt)
    avg_rank = {}
    start = 0
    for i, c in enumerate(cnt):
        avg_rank[i] = (start + 1 + start + c) / 2.0
        start += c
    ranks = np.array([avg_rank[i] for i in inv])
    r_pos = ranks[y == 1].sum()
    n_pos, n_neg = len(pos), len(neg)
    return (r_pos - n_pos * (n_pos + 1) / 2.0) / (n_pos * n_neg)


EMA_BARS = 168          # ~7d en 1h: EMA de régimen swing


def ema_series(df):
    """Devuelve (open_time[np], above[np bool]) — close > EMA(EMA_BARS) en cada barra 1h."""
    c = df["close"].astype(float)
    ema = c.ewm(span=EMA_BARS, adjust=False).mean()
    return df["open_time"].to_numpy(), (c.to_numpy() > ema.to_numpy())


def val_at(ot, arr, t_ms):
    """Último valor de arr cuya open_time <= t_ms (sin leakage). None si no hay barra previa."""
    idx = np.searchsorted(ot, t_ms, side="right") - 1
    return arr[idx] if idx >= 0 else None


def regime_experiment(rows, cache, X, y, scores):
    """¿Un gate de régimen de MERCADO (BTC vs EMA + breadth) predice el base rate / mejora P&L?
    Todo computado con data <= tiempo de alerta. Cierra la línea meta-labeling."""
    print(f"\n{'#'*56}\n REGIMEN DE MERCADO (BTC EMA{EMA_BARS}h + breadth)\n{'#'*56}")
    # series above-EMA por símbolo (reusa el cache 1h ya cargado) + BTC
    above = {s: ema_series(df) for s, df in cache.items() if df is not None and len(df) > EMA_BARS}
    btc = load_1h("BTCUSDT")
    btc_ot, btc_above = ema_series(btc)

    # features de régimen por alerta
    reg = []
    for a, _ in rows:
        t = int(datetime.fromisoformat(a["alerted_at"].replace("Z", "+00:00")).timestamp() * 1000)
        b = val_at(btc_ot, btc_above, t)
        vals = [val_at(ot, ab, t) for ot, ab in above.values()]
        vals = [v for v in vals if v is not None]
        breadth = float(np.mean(vals)) if vals else np.nan
        reg.append((1.0 if b else 0.0, breadth))
    btc_up = np.array([r[0] for r in reg])
    breadth = np.array([r[1] for r in reg])
    breadth = np.where(np.isnan(breadth), np.nanmedian(breadth), breadth)

    # 1) base rate por régimen
    def br(mask, tag):
        if mask.sum() == 0:
            print(f"  {tag:28} (vacío)"); return
        print(f"  {tag:28} n={int(mask.sum()):4}  base+ {y[mask].mean()*100:4.1f}%")
    print("\n base rate (TP-first) por régimen:")
    br(btc_up == 1, "BTC > EMA (up)")
    br(btc_up == 0, "BTC < EMA (down)")
    bmed = np.median(breadth)
    br(breadth >= bmed, f"breadth >= {bmed:.2f} (top)")
    br(breadth < bmed, f"breadth <  {bmed:.2f} (bottom)")

    # 2) walk-forward con features de régimen AGREGADAS
    Xr = np.hstack([X, btc_up.reshape(-1, 1), breadth.reshape(-1, 1)])
    print("\n walk-forward (5 folds) con régimen agregado:")
    n = len(rows); start = int(n * 0.40)
    bounds = np.linspace(start, n, 6, dtype=int)
    am_b, am_r = [], []
    for i in range(5):
        a0, a1 = bounds[i], bounds[i + 1]
        if a1 - a0 < 10 or a0 < 30:
            continue
        for Xsrc, store in ((X, am_b), (Xr, am_r)):
            Xa, mu_, sd_ = standardize(Xsrc[:a0])
            Xb, _, _ = standardize(Xsrc[a0:a1], mu_, sd_)
            Xa = np.hstack([np.ones((len(Xa), 1)), Xa]); Xb = np.hstack([np.ones((len(Xb), 1)), Xb])
            w_ = fit_logreg(Xa, y[:a0], l2=2.0)
            store.append(auc(y[a0:a1], predict(Xb, w_)))
    print(f"  AUC media:  base-feats {np.nanmean(am_b):.3f}  +régimen {np.nanmean(am_r):.3f}")

    # 3) gate práctico: tomar solo alertas en up-regime, P&L realizado (todo el set)
    realized = []
    for a, _ in rows:
        r = simulate(a, cache[a["symbol"]], ARM, TRAIL, STOP)
        realized.append(r[0] if r else None)
    rmask = np.array([r is not None for r in realized])
    rz = np.array([r for r in realized if r is not None])

    def pnl(sub_mask, tag):
        m = sub_mask[rmask]
        arr = rz[m]
        if len(arr) == 0:
            print(f"  {tag:34} (vacío)"); return
        print(f"  {tag:34} n={len(arr):4}  avg {st.mean(arr)*100:+.2f}%  "
              f"med {st.median(arr)*100:+.2f}%  win {sum(1 for x in arr if x>0)/len(arr)*100:.0f}%")
    print("\n gate práctico — exit prod ARM12/TRAIL8/STOP0, P&L realizado:")
    pnl(np.ones(len(rows), bool), "TODAS")
    pnl(btc_up == 1, "solo BTC up-regime")
    pnl(breadth >= bmed, "solo breadth top-50%")
    pnl((btc_up == 1) & (breadth >= bmed), "BTC up Y breadth top")

    # validación OOS: el gate aplicado SOLO al slice test (últimas 30%, el régimen malo)
    cut = int(len(rows) * 0.70)
    oos = np.zeros(len(rows), bool); oos[cut:] = True
    print("\n  -- validación OOS (solo slice test, últimas 30%):")
    pnl(oos, "  test: TODAS")
    pnl(oos & (btc_up == 1), "  test: BTC up-regime")


# ventana 1h del Hurst. Override por env para re-validar con la ventana de producción
# (el screener live solo baja LIMIT=200 barras 1h -> WIN debe ser <=~192).
import os
HURST_WIN = int(os.environ.get("HURST_WIN", 480))
HURST_MAXLAG = int(os.environ.get("HURST_MAXLAG", 40))
LATE_SIGS = {"BREAKOUT", "HOLD", "RIDING"}    # entran en el techo -> Hurst debería discriminar
EARLY_SIGS = {"PREBREAK", "COILING"}


def hurst_exp(closes):
    """Exponente de Hurst por el método lag-variance sobre log-precio. H>0.5 trending,
    <0.5 mean-reverting, ~0.5 random walk. None si no hay data suficiente."""
    if len(closes) < HURST_WIN // 2:
        return None
    ts = np.log(np.asarray(closes, dtype=float))
    lags = np.arange(2, HURST_MAXLAG)
    tau = np.array([np.std(ts[l:] - ts[:-l]) for l in lags])
    if np.any(tau <= 0):
        return None
    return float(np.polyfit(np.log(lags), np.log(tau), 1)[0])


def hurst_at(df, t_ms):
    """Hurst sobre las últimas HURST_WIN barras con open_time <= t_ms (sin leakage)."""
    past = df[df.open_time <= t_ms]
    if len(past) < HURST_WIN // 2:
        return None
    return hurst_exp(past["close"].to_numpy()[-HURST_WIN:])


def hurst_experiment(rows, cache, X, y, scores):
    """¿H>0.5 (trending) separa ganadores de perdedores, sobre todo en señales TARDÍAS
    (BREAKOUT/HOLD/RIDING) que entran en el techo? Todo computado con precio <= tiempo alerta."""
    print(f"\n{'#'*56}\n HURST por símbolo (win {HURST_WIN}h, lag-variance log-precio)\n{'#'*56}")
    H = []
    for a, _ in rows:
        t = int(datetime.fromisoformat(a["alerted_at"].replace("Z", "+00:00")).timestamp() * 1000)
        H.append(hurst_at(cache[a["symbol"]], t))
    H = np.array([h if h is not None else np.nan for h in H])
    sig = np.array([(a.get("signal_type") or a.get("label")) for a, _ in rows])
    valid = ~np.isnan(H)
    print(f"\n  Hurst válido en {valid.sum()}/{len(H)} alertas | "
          f"mediana {np.nanmedian(H):.3f}  p25 {np.nanpercentile(H,25):.3f}  p75 {np.nanpercentile(H,75):.3f}")
    print(f"  %trending (H>0.5): {np.mean(H[valid]>0.5)*100:.0f}%")

    # realized P&L (exit prod) para todas las alertas
    realized = []
    for a, _ in rows:
        r = simulate(a, cache[a["symbol"]], ARM, TRAIL, STOP)
        realized.append(r[0] if r else None)
    realized = np.array([r if r is not None else np.nan for r in realized])

    def report(mask, tag):
        m = mask & valid
        if m.sum() < 15:
            print(f"  {tag:34} n={int(m.sum()):4} (muestra chica)"); return
        yb = y[m]; rz = realized[m & ~np.isnan(realized)]
        print(f"  {tag:34} n={int(m.sum()):4}  base+ {yb.mean()*100:4.1f}%  "
              f"P&L avg {np.mean(rz)*100:+.2f}%  med {np.median(rz)*100:+.2f}%  "
              f"win {np.mean(rz>0)*100:.0f}%")

    is_late = np.isin(sig, list(LATE_SIGS))
    is_early = np.isin(sig, list(EARLY_SIGS))
    hi = H > 0.5
    print("\n  base rate + P&L por Hurst (la hipótesis: discrimina sobre todo en TARDÍAS):")
    report(is_late & hi,        "TARDÍAS  H>0.5 (trending)")
    report(is_late & ~hi,       "TARDÍAS  H<0.5 (mean-rev)")
    report(is_early & hi,       "TEMPRANAS H>0.5")
    report(is_early & ~hi,      "TEMPRANAS H<0.5")
    print()
    report(hi,                  "TODAS    H>0.5")
    report(~hi,                 "TODAS    H<0.5")

    # walk-forward AUC con Hurst agregado (imputar nan a 0.5 = random walk)
    Hf = np.where(np.isnan(H), 0.5, H).reshape(-1, 1)
    Xh = np.hstack([X, Hf])
    n = len(rows); start = int(n * 0.40)
    bounds = np.linspace(start, n, 6, dtype=int)
    ab, ah = [], []
    for i in range(5):
        a0, a1 = bounds[i], bounds[i + 1]
        if a1 - a0 < 10 or a0 < 30:
            continue
        for Xsrc, store in ((X, ab), (Xh, ah)):
            Xa, mu_, sd_ = standardize(Xsrc[:a0]); Xb, _, _ = standardize(Xsrc[a0:a1], mu_, sd_)
            Xa = np.hstack([np.ones((len(Xa),1)), Xa]); Xb = np.hstack([np.ones((len(Xb),1)), Xb])
            w_ = fit_logreg(Xa, y[:a0], l2=2.0)
            store.append(auc(y[a0:a1], predict(Xb, w_)))
    print(f"\n  walk-forward AUC:  base-feats {np.nanmean(ab):.3f}  +Hurst {np.nanmean(ah):.3f}")

    # gate OOS: descartar TARDÍAS con H<0.5 en el slice test
    cut = int(n * 0.70)
    oos = np.zeros(n, bool); oos[cut:] = True
    drop = is_late & ~hi          # lo que el gate eliminaría
    print("\n  gate OOS (slice test) — ¿el Hurst salva las tardías buenas o solo mata tardías?:")
    report(oos & ~np.isnan(realized),                 "  test: TODAS (sin gate)")
    report(oos & ~is_late & ~np.isnan(realized),      "  test: drop TODAS las tardías (baseline)")
    report(oos & ~drop & ~np.isnan(realized),         "  test: drop tardías H<0.5 (gate Hurst)")
    # marginal: ¿las tardías H>0.5 que el gate CONSERVA valen la pena?
    report(oos & is_late & hi & ~np.isnan(realized),  "  test: SOLO tardías H>0.5 (las salvadas)")


def main():
    diag = sys.argv[1] if len(sys.argv) > 1 else "diag_8w.json"
    print(f"Diag: {diag}")
    d = json.load(open(diag))["main"]
    alerts = [a for a in d if a.get("bucket") in BUCKETS and a.get("entry_price")
              and a.get("atr_pct_1d")
              and datetime.fromisoformat(a["alerted_at"].replace("Z", "+00:00")) <= MATURE_BEFORE]
    alerts.sort(key=lambda a: a["alerted_at"])
    syms = sorted(set(a["symbol"] for a in alerts))
    print(f"Cargando klines 1h de {len(syms)} símbolos...")
    cache = {s: load_1h(s) for s in syms}
    alerts = [a for a in alerts if cache.get(a["symbol"]) is not None]

    # etiquetado triple-barrier
    rows = []
    for a in alerts:
        y = triple_barrier(a, cache[a["symbol"]])
        if y is None:
            continue
        rows.append((a, y))
    print(f"Alertas etiquetadas (triple-barrier, ±{TB_MULT}*ATR, 7d): {len(rows)}")
    ys = [r[1] for r in rows]
    print(f"  base rate TP-first/positivo: {sum(ys)/len(ys)*100:.1f}%  "
          f"(n+={sum(ys)} n-={len(ys)-sum(ys)})")

    X = np.array([featurize(a) for a, _ in rows])
    y = np.array(ys, dtype=float)
    scores = np.array([a["score"] for a, _ in rows], dtype=float)

    # split cronológico 70/30 (rows ya viene ordenado por alerted_at)
    cut = int(len(rows) * 0.70)
    Xtr_raw, Xte_raw = X[:cut], X[cut:]
    ytr, yte = y[:cut], y[cut:]
    print(f"\nSplit cronológico: train={cut}  test={len(rows)-cut}")
    print(f"  base rate train {ytr.mean()*100:.1f}%  test {yte.mean()*100:.1f}%")

    # estandarizar con stats de TRAIN, agregar bias
    Xtr_s, mu, sd = standardize(Xtr_raw)
    Xte_s, _, _ = standardize(Xte_raw, mu, sd)
    Xtr_b = np.hstack([np.ones((len(Xtr_s), 1)), Xtr_s])
    Xte_b = np.hstack([np.ones((len(Xte_s), 1)), Xte_s])

    w = fit_logreg(Xtr_b, ytr, l2=2.0)
    ptr = predict(Xtr_b, w)
    pte = predict(Xte_b, w)

    print(f"\n{'='*56}\n AUC out-of-sample (lo único que importa)\n{'='*56}")
    print(f"  meta-modelo (features):  train {auc(ytr, ptr):.3f}   TEST {auc(yte, pte):.3f}")
    print(f"  score solo:              train {auc(ytr, scores[:cut]):.3f}   "
          f"TEST {auc(yte, scores[cut:]):.3f}")

    # pesos (estandarizados -> comparables): qué features cargan señal
    print("\n  Pesos LR (estandarizados, |·| desc):")
    coefs = sorted(zip(FEAT_NAMES, w[1:]), key=lambda t: -abs(t[1]))
    for name, c in coefs[:8]:
        print(f"    {name:18} {c:+.3f}")

    # ── walk-forward expandido: AUC justo a través de varios regímenes ───────
    print(f"\n{'='*56}\n Walk-forward (expanding, 5 folds) — AUC OOS por fold\n{'='*56}")
    n = len(rows)
    folds = 5
    start = int(n * 0.40)            # primer train mínimo 40%
    bounds = np.linspace(start, n, folds + 1, dtype=int)
    wf_meta, wf_score = [], []
    for i in range(folds):
        tr_end = bounds[i]
        te_end = bounds[i + 1]
        if te_end - tr_end < 10 or tr_end < 30:
            continue
        Xa, mu_, sd_ = standardize(X[:tr_end])
        Xb, _, _ = standardize(X[tr_end:te_end], mu_, sd_)
        Xa = np.hstack([np.ones((len(Xa), 1)), Xa])
        Xb = np.hstack([np.ones((len(Xb), 1)), Xb])
        wf = fit_logreg(Xa, y[:tr_end], l2=2.0)
        am = auc(y[tr_end:te_end], predict(Xb, wf))
        asc = auc(y[tr_end:te_end], scores[tr_end:te_end])
        wf_meta.append(am); wf_score.append(asc)
        print(f"  fold {i+1}: train[:{tr_end}] test[{tr_end}:{te_end}]  "
              f"base+ {y[tr_end:te_end].mean()*100:4.1f}%  "
              f"meta AUC {am:.3f}  score AUC {asc:.3f}")
    if wf_meta:
        print(f"  --- media folds:  meta {np.nanmean(wf_meta):.3f}   "
              f"score {np.nanmean(wf_score):.3f}  (0.50 = azar)")

    # ── test práctico: filtrar por prob mejora el P&L realizado OOS? ──────────
    test_alerts = [rows[cut + i][0] for i in range(len(rows) - cut)]
    realized = []
    for a in test_alerts:
        r = simulate(a, cache[a["symbol"]], ARM, TRAIL, STOP)
        realized.append(r[0] if r else None)
    mask = np.array([r is not None for r in realized])
    rz = np.array([r for r in realized if r is not None])
    pkeep = pte[mask]

    def stats(arr, tag):
        if len(arr) == 0:
            print(f"  {tag}: (vacío)"); return
        print(f"  {tag:34} n={len(arr):3}  avg {st.mean(arr)*100:+.2f}%  "
              f"med {st.median(arr)*100:+.2f}%  win {sum(1 for x in arr if x>0)/len(arr)*100:.0f}%")

    print(f"\n{'='*56}\n Test práctico — exit prod ARM12/TRAIL8/STOP0, P&L realizado OOS\n{'='*56}")
    stats(rz, "TODAS (sin filtro)")
    # umbral = mediana de prob en TRAIN (filtra ~mitad inferior); también probamos terciles
    for q, lbl in ((0.50, "top-50% prob"), (0.33, "top-67% prob"), (0.67, "top-33% prob")):
        thr = np.quantile(ptr, q)
        keep = pkeep >= thr
        stats(rz[keep], f"meta-filtro {lbl} (thr={thr:.2f})")

    regime_experiment(rows, cache, X, y, scores)
    hurst_experiment(rows, cache, X, y, scores)


if __name__ == "__main__":
    main()
