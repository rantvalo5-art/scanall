#!/usr/bin/env python3
"""Análisis de outcomes del day trader sobre un --out JSON de backtest.py (sin pandas).

Modos:
  py -3.13 dt_analyze.py horizons <out.json>          buckets a 1h/4h/8h/24h (MFE/MAE/win)
  py -3.13 dt_analyze.py score    <out.json>          por score / por signal / bucket×signal / top-movers (4h)
  py -3.13 dt_analyze.py wf        <out.json>          tabla de buckets semana a semana (walk-forward)
  py -3.13 dt_analyze.py compare   <A.json> <B.json>   bucket-quality A vs B (4h) + recall + top-movers
  py -3.13 dt_analyze.py validate  <A.json> <B.json>   chequea que las alertas sean idénticas campo a campo

MFE = excursión favorable máxima en la ventana (mejor salida disponible).
MAE = excursión adversa máxima (riesgo de drawdown/stop). Para un day trader medir a 4h, no 24h.
Requiere que el backtest haya calculado max_high_{1h,4h,8h,24h}/min_low_* (calculate_outcomes).
"""
import json, sys
from collections import Counter
from datetime import datetime, timedelta


def load(path):
    return json.load(open(path, encoding="utf-8"))["main"]

def pct(a, b):
    if a is None or b is None or b == 0:
        return None
    return (a / b - 1) * 100.0

def mfe(a, h): return pct(a.get(f"max_high_{h}"), a.get("entry_price"))
def mae(a, h): return pct(a.get(f"min_low_{h}"),  a.get("entry_price"))

def avg(xs):
    xs = [x for x in xs if x is not None]
    return sum(xs) / len(xs) if xs else None

def winr(xs, thr=2.0):
    xs = [x for x in xs if x is not None]
    return (sum(1 for x in xs if x >= thr) / len(xs) * 100) if xs else None

def f(x, s="%"):
    return f"{x:+.2f}{s}" if x is not None else "  -  "


def cmd_horizons(rows):
    print(f"{len(rows)} alertas\n")
    for h in ("1h", "4h", "8h", "24h"):
        print(f"== Horizonte {h} ==")
        print(f"  {'Bucket':<7} {'n':>4} {'MFE':>8} {'win>2%':>7} {'MAE':>8}")
        rank = {}
        for b in ("BEST", "STRONG", "WATCH"):
            al = [a for a in rows if a.get("bucket") == b]
            m = avg([mfe(a, h) for a in al]); ad = avg([mae(a, h) for a in al]); w = winr([mfe(a, h) for a in al])
            rank[b] = m
            ws = (str(round(w)) + "%") if w is not None else "-"
            print(f"  {b:<7} {len(al):>4} {f(m):>8} {ws:>7} {f(ad):>8}")
        top = max(rank, key=lambda k: (rank[k] if rank[k] is not None else -1e9))
        order = " > ".join(f"{k}{f(rank[k])}" for k in sorted(rank, key=lambda k: -(rank[k] or -1e9)))
        print(f"  -> {order}   [BEST #1? {'SI' if top=='BEST' else 'NO('+top+')'}]\n")


def cmd_score(rows):
    print("== Por SCORE (MFE 4h) ==")
    print(f"{'score':>5} {'bucket':>8} {'n':>4} {'MFE4h':>8} {'win>2%':>7} {'MAE4h':>8}")
    for sc in sorted({a['score'] for a in rows}):
        al = [a for a in rows if a['score'] == sc]
        bk = "/".join(sorted({a.get('bucket') for a in al}))
        m = avg([mfe(a, "4h") for a in al]); w = winr([mfe(a, "4h") for a in al]); ad = avg([mae(a, "4h") for a in al])
        ws = (str(round(w)) + "%") if w is not None else "-"
        print(f"{sc:>5} {bk:>8} {len(al):>4} {f(m):>8} {ws:>7} {f(ad):>8}")

    print("\n== Por SIGNAL_TYPE (MFE 4h) ==")
    print(f"{'signal':<10} {'n':>4} {'MFE4h':>8} {'win>2%':>7} {'scoreMedio':>10}")
    for st in sorted({a['signal_type'] for a in rows}):
        al = [a for a in rows if a['signal_type'] == st]
        m = avg([mfe(a, "4h") for a in al]); w = winr([mfe(a, "4h") for a in al]); sm = avg([a['score'] for a in al])
        ws = (str(round(w)) + "%") if w is not None else "-"
        print(f"{st:<10} {len(al):>4} {f(m):>8} {ws:>7} {sm:>10.1f}")

    print("\n== Composicion BUCKET x SIGNAL (n | MFE4h) ==")
    sigs = sorted({a['signal_type'] for a in rows})
    print(f"{'bucket':<7} " + " ".join(f"{s:>13}" for s in sigs))
    for b in ("BEST", "STRONG", "WATCH"):
        cells = []
        for s in sigs:
            al = [a for a in rows if a.get('bucket') == b and a['signal_type'] == s]
            cells.append(f"{len(al)}|{f(avg([mfe(a, '4h') for a in al]))}" if al else "-")
        print(f"{b:<7} " + " ".join(f"{c:>13}" for c in cells))

    print("\n== TOP-30 movers (MFE 4h) ==")
    top = sorted([a for a in rows if mfe(a, "4h") is not None], key=lambda a: mfe(a, "4h"), reverse=True)[:30]
    print("  bucket:", dict(Counter(a.get('bucket') for a in top)))
    print("  signal:", dict(Counter(a['signal_type'] for a in top)))
    print("  score :", dict(sorted(Counter(a['score'] for a in top).items())))
    print(f"  MFE4h medio: {avg([mfe(a,'4h') for a in top]):+.1f}%  score medio: {avg([a['score'] for a in top]):.1f}")


def cmd_wf(rows):
    R = [dict(a, dt=datetime.fromisoformat(a["alerted_at"])) for a in rows if a.get("max_high_24h") is not None]
    if not R:
        print("sin alertas completas"); return
    start = min(r["dt"] for r in R)
    wk = lambda dt: int((dt - start).total_seconds() // (7 * 86400))
    print(f"{'Semana':<14} {'Bucket':<7} {'n':>4} {'MFE4h':>8} {'win>2%':>7}")
    for w in range(wk(max(r["dt"] for r in R)) + 1):
        wkrows = [r for r in R if wk(r["dt"]) == w]
        tag = str((start + timedelta(days=7 * w)).date())
        for b in ("BEST", "STRONG", "WATCH"):
            al = [r for r in wkrows if r.get("bucket") == b]
            if al:
                m = avg([mfe(a, "4h") for a in al]); win = winr([mfe(a, "4h") for a in al])
                print(f"{tag:<14} {b:<7} {len(al):>4} {f(m):>8} {round(win):>6}%")
        print()


def _bstats(rows, b):
    al = [a for a in rows if a.get("bucket") == b]
    n = len(al)
    if not n: return (0, None, None)
    return (n, avg([mfe(a, "4h") for a in al]), winr([mfe(a, "4h") for a in al]))

def cmd_compare(A, B):
    print(f"A ({len(A)})  vs  B ({len(B)})   [recall: {len(A)} -> {len(B)}]")
    print(f"{'Bucket':<7} {'A: n MFE4h win':<22} {'B: n MFE4h win':<22}")
    for b in ("BEST", "STRONG", "WATCH"):
        na, ma, wa = _bstats(A, b); nb, mb, wb = _bstats(B, b)
        print(f"{b:<7} {na:>4} {f(ma)} {round(wa) if wa else 0:>3}%   {nb:>4} {f(mb)} {round(wb) if wb else 0:>3}%")
    tb = lambda R: dict(Counter(a.get('bucket') for a in sorted([x for x in R if mfe(x,'4h') is not None], key=lambda x: mfe(x,'4h'), reverse=True)[:30]))
    print(f"\nTop-30 movers -> bucket:\n  A: {tb(A)}\n  B: {tb(B)}")


def cmd_validate(A, B):
    def norm(a):
        return {k: (round(v, 8) if isinstance(v, float) else v) for k, v in a.items()}
    key = lambda d: (d.get("alerted_at"), d.get("symbol"), d.get("label"), d.get("score"), d.get("timeframe"))
    a = sorted((norm(x) for x in A), key=key); b = sorted((norm(x) for x in B), key=key)
    print(f"A={len(a)} B={len(b)}")
    if len(a) != len(b):
        print("DIFIERE conteo"); sys.exit(1)
    diffs = sum(1 for x, y in zip(a, b) if x != y)
    print("IDENTICO" if diffs == 0 else f"{diffs} con diferencias")
    sys.exit(0 if diffs == 0 else 1)


def main():
    if len(sys.argv) < 3:
        print(__doc__); sys.exit(1)
    mode = sys.argv[1]
    if mode in ("compare", "validate"):
        A, B = load(sys.argv[2]), load(sys.argv[3])
        (cmd_compare if mode == "compare" else cmd_validate)(A, B)
    else:
        rows = load(sys.argv[2])
        {"horizons": cmd_horizons, "score": cmd_score, "wf": cmd_wf}[mode](rows)


if __name__ == "__main__":
    main()
