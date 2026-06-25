"""
Prototipo — P1 DERIVADOS (funding rate). Paso 1: BACKFILL + cache.

Gate de feasibility (2026-06-18): OI history muere a 30d → solo forward-test live.
Funding rate SÍ tiene historia completa (cubre 100% de diag_16w) → backtesteable limpio.
Ver memoria project-swing-p1-deriv-gate-result.

Este módulo baja el funding rate (cada 8h) de todos los símbolos de un diag, lo cachea
a disco y reporta cobertura. El join a alertas + AUC (funding extremo → mover/NET) +
walk-forward EV vienen en pasos siguientes sobre estas mismas funciones.

NO toca producción. py -3.13. Diag por argv (default diag_16w.json).
Cache: .backtest_cache/funding_<symbol>_<start>_<end>.pkl
"""
import json, glob, sys
from datetime import datetime, timezone, timedelta
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
import numpy as np
import pandas as pd
import requests

from sim_exit import load_1h, simulate, MATURE_BEFORE, BUCKETS
from meta_label import auc

EXIT = (0.12, 0.08, 0.0)   # ARM12/TRAIL8/STOP0 — exit shipeado a producción
MOVE_THR = 0.20            # "mover" = MFE 7d >= 20%

CACHE_DIR = Path(".backtest_cache")
FAPI = "https://fapi.binance.com"
MAX_WORKERS = 8


def _fapi_get(path, params, retries=3):
    import time
    for attempt in range(retries):
        try:
            r = requests.get(f"{FAPI}{path}", params=params, timeout=20)
            if r.status_code == 200:
                return r.json()
            if r.status_code == 429:
                time.sleep(2 ** attempt)
                continue
            return None
        except requests.exceptions.RequestException:
            time.sleep(1)
    return None


def get_perp_symbols():
    """Set de pares USDT-M perp activos en Binance Futures."""
    data = _fapi_get("/fapi/v1/exchangeInfo", {})
    if not data:
        return set()
    return {s["symbol"] for s in data.get("symbols", [])
            if s.get("contractType") == "PERPETUAL"
            and s.get("quoteAsset") == "USDT" and s.get("status") == "TRADING"}


def perp_name(spot_sym, perp_set):
    """Mapea un símbolo spot a su perp. Maneja el prefijo 1000x (PEPEUSDT→1000PEPEUSDT)."""
    if spot_sym in perp_set:
        return spot_sym
    base = spot_sym[:-4]  # quita USDT
    for pref in ("1000", "1000000"):
        cand = f"{pref}{base}USDT"
        if cand in perp_set:
            return cand
    return None


def get_funding_history(perp_sym, start_ms, end_ms):
    """Funding (cada 8h). Paginado (max 1000). DataFrame [funding_time, funding_rate] o None."""
    rows, cursor = [], start_ms
    while cursor < end_ms:
        data = _fapi_get("/fapi/v1/fundingRate",
                         {"symbol": perp_sym, "startTime": cursor, "endTime": end_ms, "limit": 1000})
        if not data:
            break
        rows.extend(data)
        last = int(data[-1]["fundingTime"])
        if len(data) < 1000 or last <= cursor:
            break
        cursor = last + 1
    if not rows:
        return None
    df = pd.DataFrame([{"funding_time": int(r["fundingTime"]),
                        "funding_rate": float(r["fundingRate"])} for r in rows])
    return df.drop_duplicates("funding_time").sort_values("funding_time").reset_index(drop=True)


def _cache_file(spot_sym, start_ms, end_ms):
    return CACHE_DIR / f"funding_{spot_sym}_{start_ms}_{end_ms}.pkl"


def load_funding(spot_sym, perp_set, start_ms, end_ms):
    """Carga funding de cache (cualquier rango que cubra) o baja+cachea. DataFrame o None."""
    # cache hit: cualquier archivo cuyo rango cubra [start,end]
    for f in glob.glob(str(CACHE_DIR / f"funding_{spot_sym}_*.pkl")):
        try:
            parts = Path(f).stem.split("_")
            cs, ce = int(parts[-2]), int(parts[-1])
            if cs <= start_ms and ce >= end_ms:
                return pd.read_pickle(f)
        except (ValueError, IndexError):
            continue
    perp = perp_name(spot_sym, perp_set)
    if perp is None:
        return None
    df = get_funding_history(perp, start_ms, end_ms)
    if df is None or df.empty:
        return None
    CACHE_DIR.mkdir(exist_ok=True)
    df.to_pickle(_cache_file(spot_sym, start_ms, end_ms))
    return df


def diag_span(alerts):
    """Rango [start,end] en ms que cubre todas las alertas + buffer de 1d a cada lado."""
    ts = [datetime.fromisoformat(a["alerted_at"].replace("Z", "+00:00")) for a in alerts]
    start = (min(ts) - timedelta(days=1)).timestamp() * 1000
    end = (max(ts) + timedelta(days=1)).timestamp() * 1000
    return int(start), int(end)


def funding_at(df, t_ms):
    """Funding vigente (último funding_time <= t) + percentil dentro del histórico-hasta-ahora
    del propio símbolo (sin leakage). Devuelve (rate, pct) o (None, None)."""
    ft = df["funding_time"].to_numpy()
    fr = df["funding_rate"].to_numpy()
    idx = np.searchsorted(ft, t_ms, side="right") - 1
    if idx < 0:
        return None, None
    rate = fr[idx]
    past = fr[:idx + 1]
    pct = float((past <= rate).mean())   # 0..1, qué tan alto está vs su propia historia
    return float(rate), pct


def show(tag, arr):
    if len(arr) == 0:
        print(f"  {tag:32} (vacío)"); return
    a = np.asarray(arr)
    print(f"  {tag:32} n={len(a):4}  avg {a.mean()*100:+.2f}%  med {np.median(a)*100:+.2f}%  "
          f"win {(a>0).mean()*100:.0f}%  p10 {np.quantile(a,0.1)*100:+.1f}%")


def analyze(alerts, funding_map):
    """Join funding→alertas + AUC (mover/NET) + EV por quintil de funding."""
    mature = [a for a in alerts if a.get("bucket") in BUCKETS and a.get("entry_price")
              and datetime.fromisoformat(a["alerted_at"].replace("Z", "+00:00")) <= MATURE_BEFORE
              and a["symbol"] in funding_map]
    syms = sorted(set(a["symbol"] for a in mature))
    print(f"\n{'='*60}\n JOIN + AUC (funding → outcome)\n{'='*60}")
    print(f"Cargando klines 1h de {len(syms)} símbolos para NET del exit shipeado...")
    kl = {s: load_1h(s) for s in syms}

    A = []
    for a in mature:
        df = funding_map[a["symbol"]]
        t_ms = int(datetime.fromisoformat(a["alerted_at"].replace("Z", "+00:00")).timestamp() * 1000)
        rate, pct = funding_at(df, t_ms)
        if rate is None:
            continue
        kdf = kl.get(a["symbol"])
        net = simulate(a, kdf, *EXIT) if kdf is not None else None
        if net is None:
            continue
        a["_fr"] = rate
        a["_fr_abs"] = abs(rate)
        a["_fr_pct"] = pct
        a["_atr"] = a.get("atr_pct_1d")
        a["_net"] = net[0]
        a["_move"] = (a["max_high_7d"] / a["entry_price"] - 1) if a.get("max_high_7d") else None
        if a["_move"] is None:
            continue
        A.append(a)
    A.sort(key=lambda a: a["alerted_at"])
    print(f"Alertas con funding + NET + move: {len(A)}\n")

    fr = np.array([a["_fr"] for a in A])
    print(f"funding vigente: med {np.median(fr)*100:+.4f}%  "
          f"p10 {np.quantile(fr,.1)*100:+.4f}%  p90 {np.quantile(fr,.9)*100:+.4f}%  "
          f"(8h; >0 = longs pagan = sobrecalentado)\n")

    # ── AUC: ¿el funding separa movers y NET-ganadores? ───────────────────────
    y_move = np.array([1 if a["_move"] >= MOVE_THR else 0 for a in A])
    y_win = np.array([1 if a["_net"] > 0 else 0 for a in A])
    print("  AUC (>0.5 ⇒ funding ALTO predice; <0.5 ⇒ funding BAJO/negativo predice):")
    for name, feat in [("funding raw", "_fr"), ("|funding|", "_fr_abs"), ("funding pct-símbolo", "_fr_pct")]:
        x = np.array([a[feat] for a in A])
        print(f"    {name:22}  AUC mover={auc(y_move, x):.3f}   AUC NET-win={auc(y_win, x):.3f}")

    # ── EV por quintil de funding (lo que decide si mueve EV) ─────────────────
    print("\n  NET realizado por QUINTIL de funding vigente (asc):")
    qs = np.quantile(fr, [0, .2, .4, .6, .8, 1.0])
    for i in range(5):
        m = (fr >= qs[i]) & (fr <= qs[i+1] if i == 4 else fr < qs[i+1])
        show(f"  Q{i+1} fr[{qs[i]*100:+.3f}%,{qs[i+1]*100:+.3f}%]", [a["_net"] for a, mm in zip(A, m) if mm])

    print("\n  NET realizado por QUINTIL de funding pct-símbolo (asc):")
    pc = np.array([a["_fr_pct"] for a in A])
    for i in range(5):
        lo, hi = i*0.2, (i+1)*0.2
        m = (pc >= lo) & (pc <= hi if i == 4 else pc < hi)
        show(f"  Q{i+1} pct[{lo:.1f},{hi:.1f}]", [a["_net"] for a, mm in zip(A, m) if mm])

    walkforward(A)
    orthogonality(A)
    recall_cost(A)
    return A


def recall_cost(A):
    """Costo de CATCH del gate drop-hi-fr (per-símbolo, honesto): movers perdidos + qué se descarta."""
    from collections import defaultdict
    print(f"\n{'='*60}\n COSTO DE RECALL — gate drop fr>q80 (descarta funding alto)\n{'='*60}")
    g80 = np.quantile([a["_fr"] for a in A], 0.80)
    kept = [a for a in A if a["_fr"] <= g80]
    drop = [a for a in A if a["_fr"] > g80]
    print(f"  alertas: {len(A)} → quedan {len(kept)} ({100*len(kept)/len(A):.0f}%), "
          f"descarta {len(drop)} ({100*len(drop)/len(A):.0f}%)")

    # qué se descarta: mover-rate y NET de lo descartado vs lo que queda
    def mr(xs): return np.mean([1 if a["_move"] >= MOVE_THR else 0 for a in xs]) * 100 if xs else 0
    def net(xs): return np.mean([a["_net"] for a in xs]) * 100 if xs else 0
    print(f"  DESCARTADO: mover-rate {mr(drop):.0f}%  NET avg {net(drop):+.2f}%  win {np.mean([a['_net']>0 for a in drop])*100:.0f}%")
    print(f"  QUEDA:      mover-rate {mr(kept):.0f}%  NET avg {net(kept):+.2f}%  win {np.mean([a['_net']>0 for a in kept])*100:.0f}%")

    # per-símbolo: movers perdidos (ningún alert kept los captura)
    best_all = defaultdict(float); best_kept = defaultdict(float)
    for a in A:
        s = a["symbol"]; mv = a["_move"] or 0
        best_all[s] = max(best_all[s], mv)
        if a["_fr"] <= g80:
            best_kept[s] = max(best_kept[s], mv)
    movers = [s for s in best_all if best_all[s] >= MOVE_THR]
    lost = [s for s in movers if best_kept.get(s, 0) < MOVE_THR]
    print(f"\n  símbolos mover (MFE>=20% en alguna alerta): {len(movers)}")
    print(f"  movers PERDIDOS (todas sus alertas-mover descartadas): {len(lost)} "
          f"({100*len(lost)/len(movers):.0f}%)  {lost[:10]}{'...' if len(lost)>10 else ''}")


def _spearman(x, y):
    """Spearman rho sin scipy: Pearson sobre ranks."""
    x = np.asarray(x, float); y = np.asarray(y, float)
    rx = np.argsort(np.argsort(x)); ry = np.argsort(np.argsort(y))
    return float(np.corrcoef(rx, ry)[0, 1])


def orthogonality(A):
    """¿El gate de funding aporta SOBRE atr_pct_1d y DENTRO del régimen BTC? (lo decisivo)."""
    from meta_label import ema_series, val_at
    print(f"\n{'='*60}\n ORTOGONALIDAD — ¿funding aporta sobre atr / régimen?\n{'='*60}")

    Aa = [a for a in A if a.get("_atr") is not None]
    fr = np.array([a["_fr"] for a in Aa]); at = np.array([a["_atr"] for a in Aa])
    print(f"  (A) corr funding↔atr_pct_1d:  Spearman {_spearman(fr, at):+.3f}  "
          f"(|rho| alto ⇒ proxies; ~0 ⇒ ortogonales)   n={len(Aa)}")

    # régimen BTC (EMA168 1h, sin leakage)
    btc = load_1h("BTCUSDT")
    ot, above = ema_series(btc)
    for a in A:
        t = int(datetime.fromisoformat(a["alerted_at"].replace("Z", "+00:00")).timestamp() * 1000)
        a["_up"] = val_at(ot, above, t)

    # (B) gate de funding DENTRO de cada tercil de atr (in-sample, ¿dirección aguanta?)
    print("\n  (B) drop hi-fr (q80 global) DENTRO de cada tercil de atr_pct_1d:")
    q = np.quantile(at, [0, 1/3, 2/3, 1.0])
    g80 = np.quantile(fr, 0.80)
    for i in range(3):
        hi_ok = (lambda v: v <= q[i+1]) if i == 2 else (lambda v: v < q[i+1])
        band = [a for a in Aa if q[i] <= a["_atr"] and hi_ok(a["_atr"])]
        alln = [a["_net"] for a in band]
        kept = [a["_net"] for a in band if a["_fr"] <= g80]
        da = (np.mean(kept) - np.mean(alln)) * 100 if alln and kept else float("nan")
        print(f"    atr[{q[i]:.1f},{q[i+1]:.1f}]  all {np.mean(alln)*100:+.2f}% (n={len(alln)})  "
              f"→ drop-hi-fr {np.mean(kept)*100:+.2f}% (n={len(kept)})  Δ {da:+.2f}pp")

    # (C) gate de funding DENTRO del régimen
    print("\n  (C) drop hi-fr DENTRO del régimen BTC:")
    for tag, sub in [("BTC up", [a for a in A if a.get("_up")]),
                     ("BTC down", [a for a in A if a.get("_up") is False])]:
        alln = [a["_net"] for a in sub]
        kept = [a["_net"] for a in sub if a["_fr"] <= g80]
        da = (np.mean(kept) - np.mean(alln)) * 100 if alln and kept else float("nan")
        print(f"    {tag:9} all {np.mean(alln)*100:+.2f}% (n={len(alln)})  "
              f"→ drop-hi-fr {np.mean(kept)*100:+.2f}% (n={len(kept)})  Δ {da:+.2f}pp")

    # (D) walk-forward: all vs atr-band vs funding vs COMBINADO (¿aditivo?)
    print("\n  (D) WALK-FORWARD 5 folds — ¿funding suma SOBRE un gate de atr?")
    print("      atr-band = keep 3<=atr<=12 (banda de atr-ranking) | fr = drop fr>q80(train)")
    n = len(Aa); start = int(n * 0.40)
    bounds = np.linspace(start, n, 6, dtype=int)
    Aa.sort(key=lambda a: a["alerted_at"])
    print(f"  {'fold':>4} | {'all':>7} | {'atr-band':>8} | {'fr-gate':>8} | {'atr+fr':>8}")
    wins = {"atr": 0, "fr": 0, "both": 0, "both_vs_atr": 0}
    for i in range(5):
        a0, a1 = bounds[i], bounds[i+1]
        tr, te = Aa[:a0], Aa[a0:a1]
        if len(te) < 20:
            continue
        q80 = np.quantile([a["_fr"] for a in tr], 0.80)
        f_all = np.mean([a["_net"] for a in te]) * 100
        f_atr = np.mean([a["_net"] for a in te if 3 <= a["_atr"] <= 12]) * 100
        f_fr = np.mean([a["_net"] for a in te if a["_fr"] <= q80]) * 100
        comb = [a["_net"] for a in te if 3 <= a["_atr"] <= 12 and a["_fr"] <= q80]
        f_both = np.mean(comb) * 100 if comb else float("nan")
        wins["atr"] += f_atr > f_all; wins["fr"] += f_fr > f_all
        wins["both"] += f_both > f_all; wins["both_vs_atr"] += f_both > f_atr
        print(f"  {i+1:>4} | {f_all:>+6.2f}% | {f_atr:>+7.2f}% | {f_fr:>+7.2f}% | {f_both:>+7.2f}%")
    print(f"  → bate a 'all':  atr {wins['atr']}/5   fr {wins['fr']}/5   atr+fr {wins['both']}/5")
    print(f"  → atr+fr bate a SOLO-atr (funding aporta sobre atr): {wins['both_vs_atr']}/5")


def walkforward(A):
    """5 folds, train expansivo. Gate = DROP funding alto (umbral de TRAIN). Compara NET avg
    del subset filtrado vs 'all' en cada TEST. El test que decide (espeja sim_regime_gate sec d)."""
    print(f"\n{'='*60}\n WALK-FORWARD (5 folds) — gate de funding vs all (NET avg TEST)\n{'='*60}")
    n = len(A); start = int(n * 0.40)
    bounds = np.linspace(start, n, 6, dtype=int)
    print(f"  gates: drop fr>q80(train) | only fr<=med(train) | drop pct>0.8")
    print(f"  {'fold':>4} | {'n_te':>4} | {'all':>7} | {'drop hi-fr':>11} | {'lo-fr only':>11} | {'drop hi-pct':>12}")
    wins = {"drophi": 0, "loonly": 0, "droppct": 0}
    for i in range(5):
        a0, a1 = bounds[i], bounds[i+1]
        tr, te = A[:a0], A[a0:a1]
        if len(te) < 20:
            continue
        trfr = np.array([a["_fr"] for a in tr])
        q80 = np.quantile(trfr, 0.80)
        med = np.median(trfr)
        all_net = [a["_net"] for a in te]
        drophi = [a["_net"] for a in te if a["_fr"] <= q80]
        loonly = [a["_net"] for a in te if a["_fr"] <= med]
        droppct = [a["_net"] for a in te if a["_fr_pct"] <= 0.80]
        a_avg = np.mean(all_net) * 100
        dh = np.mean(drophi) * 100 if drophi else float("nan")
        lo = np.mean(loonly) * 100 if loonly else float("nan")
        dp = np.mean(droppct) * 100 if droppct else float("nan")
        wins["drophi"] += dh > a_avg
        wins["loonly"] += lo > a_avg
        wins["droppct"] += dp > a_avg
        print(f"  {i+1:>4} | {len(te):>4} | {a_avg:>+6.2f}% | {dh:>+10.2f}% | {lo:>+10.2f}% | {dp:>+11.2f}%")
    print(f"  → folds donde bate a 'all':  drop hi-fr {wins['drophi']}/5   "
          f"lo-fr only {wins['loonly']}/5   drop hi-pct {wins['droppct']}/5")
    print("  (recall: 'lo-fr only' descarta ~50% de entradas; ojo costo de oportunidad)")


def main():
    diag = sys.argv[1] if len(sys.argv) > 1 else "diag_16w.json"
    alerts = json.load(open(diag))["main"]
    syms = sorted(set(a["symbol"] for a in alerts))
    start_ms, end_ms = diag_span(alerts)
    d = lambda ms: datetime.fromtimestamp(ms / 1000, timezone.utc).strftime("%Y-%m-%d")
    print(f"Diag: {diag} | {len(alerts)} alertas | {len(syms)} símbolos")
    print(f"Rango funding a backfillear: {d(start_ms)} → {d(end_ms)}\n")

    print("Obteniendo lista de perps USDT-M...")
    perp_set = get_perp_symbols()
    print(f"  {len(perp_set)} perps activos\n")

    print(f"Backfill funding ({MAX_WORKERS} workers)...")
    results = {}
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
        futs = {ex.submit(load_funding, s, perp_set, start_ms, end_ms): s for s in syms}
        done = 0
        for fut in as_completed(futs):
            s = futs[fut]
            try:
                results[s] = fut.result()
            except Exception as e:
                print(f"  {s}: error {e}")
                results[s] = None
            done += 1
            if done % 25 == 0:
                print(f"  {done}/{len(syms)}...")

    # ── reporte de cobertura ────────────────────────────────────────────────
    ok = {s: df for s, df in results.items() if df is not None and not df.empty}
    no_perp = [s for s in syms if perp_name(s, perp_set) is None]
    empty = [s for s in syms if s not in ok and perp_name(s, perp_set) is not None]
    print(f"\n{'='*60}\n COBERTURA FUNDING\n{'='*60}")
    print(f"  con funding:   {len(ok)}/{len(syms)} ({100*len(ok)/len(syms):.0f}%)")
    print(f"  sin perp:      {len(no_perp)}  {no_perp[:8]}{'...' if len(no_perp)>8 else ''}")
    print(f"  perp sin data: {len(empty)}  {empty[:8]}{'...' if len(empty)>8 else ''}")
    if ok:
        npts = [len(df) for df in ok.values()]
        print(f"  puntos/símbolo: min {min(npts)}  med {sorted(npts)[len(npts)//2]}  max {max(npts)}")
        # ¿cubren el período completo? (funding cada 8h → ~3/día)
        cov_start = max(df.funding_time.min() for df in ok.values())
        cov_end = min(df.funding_time.max() for df in ok.values())
        print(f"  cobertura común (todos los símbolos): {d(cov_start)} → {d(cov_end)}")

    # fracción de ALERTAS con funding disponible
    alert_ok = sum(1 for a in alerts if a["symbol"] in ok)
    print(f"\n  alertas con funding disponible: {alert_ok}/{len(alerts)} "
          f"({100*alert_ok/len(alerts):.0f}%)")
    print(f"\nCache en {CACHE_DIR}/funding_*.pkl")

    analyze(alerts, ok)


if __name__ == "__main__":
    main()
