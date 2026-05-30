import argparse
import json
from datetime import datetime, timezone
from collections import defaultdict

parser = argparse.ArgumentParser()
parser.add_argument("--in",   dest="infile", default="outcomes_5d.json", help="JSON de entrada")
parser.add_argument("--days", type=int,      default=5,                  help="Ventana (solo para el título)")
args = parser.parse_args()

with open(args.infile, encoding="utf-8") as f:
    rows = json.load(f)

def pct(new, base):
    if new is None or base is None or base == 0:
        return None
    return round((new - base) / base * 100, 2)

def avg(lst):
    lst = [x for x in lst if x is not None]
    return round(sum(lst) / len(lst), 2) if lst else None

def pct_pos(lst, thr=0):
    lst = [x for x in lst if x is not None]
    return round(100 * sum(1 for x in lst if x > thr) / len(lst), 0) if lst else None

for r in rows:
    ep = r.get("entry_price")
    r["_p15m"]    = pct(r.get("price_15m"),    ep)
    r["_p1h"]     = pct(r.get("price_1h"),     ep)
    r["_p4h"]     = pct(r.get("price_4h"),     ep)
    r["_p24h"]    = pct(r.get("price_24h"),    ep)
    r["_max4h"]   = pct(r.get("max_high_4h"),  ep)
    r["_min4h"]   = pct(r.get("min_low_4h"),   ep)
    r["_max24h"]  = pct(r.get("max_high_24h"), ep)
    r["_min24h"]  = pct(r.get("min_low_24h"),  ep)

SEP = "=" * 64
SEP2 = "-" * 64

print(f"\n{SEP}")
print(f"DIAGNOSTICO SCREENER — ultimos {args.days} dias  ({len(rows)} alertas)")
print(f"{SEP}")

# ── SALUD OPERATIVA ────────────────────────────────────────────────────────────
complete = [r for r in rows if r.get("outcomes_complete")]
partial  = [r for r in rows if r.get("price_1h") is not None and not r.get("outcomes_complete")]
pending  = [r for r in rows if r.get("price_1h") is None]

print(f"\n{SEP2}")
print("SALUD OPERATIVA")
print(f"{SEP2}")
print(f"  Completos: {len(complete)}  |  Parciales: {len(partial)}  |  Pendientes: {len(pending)}")
pct_done = round(100 * len(complete) / len(rows), 0) if rows else 0
print(f"  Tasa de completado: {pct_done}%")

# Repeats por símbolo
by_symbol = defaultdict(list)
for r in rows:
    by_symbol[r["symbol"]].append(r)
repeats = {s: items for s, items in by_symbol.items() if len(items) > 1}

print(f"\n  Simbolos con >1 alerta en ventana: {len(repeats)}")
if repeats:
    by_sig_repeats = defaultdict(list)
    for s, items in repeats.items():
        for r in items:
            by_sig_repeats[r.get("signal_type", "?")].append(s)
    for sig, syms in sorted(by_sig_repeats.items(), key=lambda x: -len(x[1])):
        unique = len(set(syms))
        print(f"    {sig}: {unique} simbolos ({len(syms)} alertas en repeat)")
    top_repeats = sorted(repeats.items(), key=lambda x: -len(x[1]))[:8]
    print(f"  Top repeats: {', '.join(f'{s}({len(i)})' for s, i in top_repeats)}")

# Distribución horaria (UTC)
print(f"\n  Distribucion horaria (UTC):")
by_hour = defaultdict(int)
for r in rows:
    ts = (r.get("alerted_at") or "")
    if len(ts) >= 13:
        h = int(ts[11:13])
        by_hour[h] += 1
peak_h = max(by_hour, key=by_hour.get) if by_hour else None
for h in sorted(by_hour):
    bar = "█" * min(by_hour[h], 40)
    print(f"    {h:02d}h: {bar} ({by_hour[h]})")

# Gaps máximos entre alertas consecutivas
sorted_ts = sorted(
    (r.get("alerted_at") or "") for r in rows if r.get("alerted_at")
)
max_gap_min = 0
max_gap_ts  = ""
for i in range(1, len(sorted_ts)):
    try:
        t0 = datetime.fromisoformat(sorted_ts[i-1].replace("Z", "+00:00"))
        t1 = datetime.fromisoformat(sorted_ts[i].replace("Z", "+00:00"))
        gap = (t1 - t0).total_seconds() / 60
        if gap > max_gap_min:
            max_gap_min = gap
            max_gap_ts  = sorted_ts[i-1][:16]
    except Exception:
        pass
if max_gap_min:
    flag = "  *** GAP SOSPECHOSO (>30min) ***" if max_gap_min > 30 else ""
    print(f"\n  Max gap entre alertas consecutivas: {max_gap_min:.0f} min  (desde {max_gap_ts}){flag}")

# ── POR BUCKET ────────────────────────────────────────────────────────────────
print(f"\n{SEP2}")
print("POR BUCKET")
print(f"{SEP2}")
by_bucket = defaultdict(list)
for r in rows:
    by_bucket[r.get("bucket", "?")].append(r)

for bucket, items in sorted(by_bucket.items()):
    scores = [i.get("score") for i in items if i.get("score") is not None]
    p1h  = [i["_p1h"]  for i in items if i["_p1h"]  is not None]
    p4h  = [i["_p4h"]  for i in items if i["_p4h"]  is not None]
    p24  = [i["_p24h"] for i in items if i["_p24h"] is not None]
    mx24 = [i["_max24h"]for i in items if i["_max24h"]is not None]
    print(f"\n  {bucket}  (n={len(items)}  score avg={avg(scores)}  min={min(scores) if scores else '?'})")
    if p1h:  print(f"    Delta1h : avg {avg(p1h):+.2f}%  >0: {pct_pos(p1h)}%  (n={len(p1h)})")
    if p4h:  print(f"    Delta4h : avg {avg(p4h):+.2f}%  >0: {pct_pos(p4h)}%  (n={len(p4h)})")
    if p24:  print(f"    Delta24h: avg {avg(p24):+.2f}%  >0: {pct_pos(p24)}%  (n={len(p24)})")
    if mx24: print(f"    max24h  : avg {avg(mx24):+.2f}%")

# ── COMPOSICIÓN DE BUCKETS ────────────────────────────────────────────────────
print(f"\n{SEP2}")
print("COMPOSICION DE BUCKETS (% signal_type dentro de cada bucket)")
print(f"{SEP2}")
all_buckets_seen = sorted(set(r.get("bucket", "?") for r in rows))
all_signals_seen = sorted(set(r.get("signal_type", "?") for r in rows))
by_bucket_full = defaultdict(list)
for r in rows:
    by_bucket_full[r.get("bucket", "?")].append(r)
print(f"  {'SEÑAL':<14}", end="")
for b in all_buckets_seen:
    print(f"  {b:>10}", end="")
print()
for sig in all_signals_seen:
    print(f"  {sig:<14}", end="")
    for b in all_buckets_seen:
        items_b = by_bucket_full[b]
        n_sig = sum(1 for i in items_b if i.get("signal_type") == sig)
        if items_b:
            pct_s = round(100 * n_sig / len(items_b), 0)
            print(f"  {n_sig:>3}({pct_s:>2.0f}%)", end="")
        else:
            print(f"  {'—':>7}", end="")
    print()

# ── SIGNAL × BUCKET — PERFORMANCE ─────────────────────────────────────────────
print(f"\n{SEP2}")
print("SIGNAL x BUCKET — PERFORMANCE (confirma si score discrimina dentro de cada señal)")
print(f"{SEP2}")
sig_bkt_groups = defaultdict(list)
for r in rows:
    sig_bkt_groups[(r.get("signal_type", "?"), r.get("bucket", "?"))].append(r)

signals_order = sorted(set(k[0] for k in sig_bkt_groups))
for sig in signals_order:
    print(f"\n  {sig}")
    for bkt in all_buckets_seen:
        items = sig_bkt_groups.get((sig, bkt), [])
        if not items:
            continue
        p1h  = [i["_p1h"]  for i in items if i["_p1h"]  is not None]
        p24  = [i["_p24h"] for i in items if i["_p24h"] is not None]
        mx24 = [i["_max24h"]for i in items if i["_max24h"]is not None]
        scores = [i.get("score") for i in items if i.get("score") is not None]
        line = f"    {bkt:<8} n={len(items):>2}  sc_avg={avg(scores):>5}"
        if p1h:  line += f"  Δ1h={avg(p1h):+.2f}%(pos:{pct_pos(p1h):>2.0f}%)"
        if p24:  line += f"  Δ24h={avg(p24):+.2f}%(pos:{pct_pos(p24):>2.0f}%)"
        if mx24: line += f"  max24h={avg(mx24):+.2f}%"
        print(line)

# ── SCORE → OUTCOME POR SEÑAL ──────────────────────────────────────────────────
print(f"\n{SEP2}")
print("SCORE → OUTCOME POR SEÑAL (correlacion score-performance dentro de cada señal)")
print(f"{SEP2}")
for sig in signals_order:
    sig_rows = [r for r in rows if r.get("signal_type") == sig]
    if not sig_rows:
        continue
    by_score = defaultdict(list)
    for r in sig_rows:
        s = int(r.get("score") or 0)
        by_score[s].append(r)
    print(f"\n  {sig}  (n={len(sig_rows)})")
    print(f"  {'sc':>3}  {'n':>3}  {'Δ1h':>8}  {'pos1h':>6}  {'Δ24h':>8}  {'pos24h':>6}  {'max24h':>8}")
    for sc in sorted(by_score):
        items = by_score[sc]
        p1h  = [i["_p1h"]  for i in items if i["_p1h"]  is not None]
        p24  = [i["_p24h"] for i in items if i["_p24h"] is not None]
        mx24 = [i["_max24h"]for i in items if i["_max24h"]is not None]
        p1h_s  = f"{avg(p1h):+.2f}%" if p1h  else "  —  "
        p24_s  = f"{avg(p24):+.2f}%" if p24  else "  —  "
        mx24_s = f"{avg(mx24):+.2f}%" if mx24 else "  —  "
        pos1h  = f"{pct_pos(p1h):>2.0f}%" if p1h  else " —"
        pos24  = f"{pct_pos(p24):>2.0f}%" if p24  else " —"
        print(f"  {sc:>3}  {len(items):>3}  {p1h_s:>8}  {pos1h:>6}  {p24_s:>8}  {pos24:>6}  {mx24_s:>8}")

# ── POR TIPO DE SEÑAL ──────────────────────────────────────────────────────────
print(f"\n{SEP2}")
print("POR TIPO DE SENAL")
print(f"{SEP2}")
by_signal = defaultdict(list)
for r in rows:
    by_signal[r.get("signal_type", "?")].append(r)

for sig, items in sorted(by_signal.items()):
    scores = [i.get("score") for i in items if i.get("score") is not None]
    p15  = [i["_p15m"]  for i in items if i["_p15m"]  is not None]
    p1h  = [i["_p1h"]   for i in items if i["_p1h"]   is not None]
    p4h  = [i["_p4h"]   for i in items if i["_p4h"]   is not None]
    p24  = [i["_p24h"]  for i in items if i["_p24h"]  is not None]
    mx4  = [i["_max4h"] for i in items if i["_max4h"] is not None]
    mn4  = [i["_min4h"] for i in items if i["_min4h"] is not None]
    mx24 = [i["_max24h"]for i in items if i["_max24h"]is not None]

    # Split BEST vs STRONG dentro de la señal
    b_split = defaultdict(int)
    for i in items:
        b_split[i.get("bucket", "?")] += 1
    split_str = "  ".join(f"{k}:{v}" for k, v in sorted(b_split.items()))

    print(f"\n  {sig}  (n={len(items)}  score avg={avg(scores)}  [{split_str}])")
    if p15:  print(f"    Delta15m: avg {avg(p15):+.2f}%  >0: {pct_pos(p15)}%  (n={len(p15)})")
    if p1h:  print(f"    Delta1h : avg {avg(p1h):+.2f}%  >0: {pct_pos(p1h)}%  (n={len(p1h)})")
    if p4h:  print(f"    Delta4h : avg {avg(p4h):+.2f}%  >0: {pct_pos(p4h)}%  (n={len(p4h)})")
    if p24:  print(f"    Delta24h: avg {avg(p24):+.2f}%  >0: {pct_pos(p24)}%  (n={len(p24)})")
    if mx4:  print(f"    max4h   : avg {avg(mx4):+.2f}%")
    if mn4:  print(f"    min4h   : avg {avg(mn4):+.2f}%  (drawdown)")
    if mx24: print(f"    max24h  : avg {avg(mx24):+.2f}%")

# ── TOP 10 GANADORES ──────────────────────────────────────────────────────────
print(f"\n{SEP2}")
print("TOP 10 — por pico maximo en 24h")
print(f"{SEP2}")
with_mx = [r for r in rows if r["_max24h"] is not None]
for r in sorted(with_mx, key=lambda x: x["_max24h"], reverse=True)[:10]:
    c = "*" if r.get("outcomes_complete") else " "
    bkt = (r.get("bucket") or "")[:1]
    print(f"  {c}{r['symbol']:12s} {r['signal_type']:10s} [{bkt}] sc={r.get('score','?'):4.0f}  "
          f"max24h={r['_max24h']:+6.2f}%  1h={str(r['_p1h'] or '---'):>7}  4h={str(r['_p4h'] or '---'):>7}  "
          f"{(r.get('alerted_at') or '')[:16]}")

# ── BOTTOM 10 FAKEOUTS ────────────────────────────────────────────────────────
print(f"\n{SEP2}")
print("BOTTOM 10 — peor drawdown en 4h (fakeouts / entradas costosas)")
print(f"{SEP2}")
with_mn = [r for r in rows if r["_min4h"] is not None]
for r in sorted(with_mn, key=lambda x: x["_min4h"])[:10]:
    c = "*" if r.get("outcomes_complete") else " "
    bkt = (r.get("bucket") or "")[:1]
    print(f"  {c}{r['symbol']:12s} {r['signal_type']:10s} [{bkt}] sc={r.get('score','?'):4.0f}  "
          f"min4h={r['_min4h']:+6.2f}%  1h={str(r['_p1h'] or '---'):>7}  24h={str(r['_p24h'] or '---'):>8}  "
          f"{(r.get('alerted_at') or '')[:16]}")

# ── DELTA 1H NEGATIVOS ────────────────────────────────────────────────────────
print(f"\n{SEP2}")
print("ALERTAS CON Delta1h NEGATIVO (loss en primera hora)")
print(f"{SEP2}")
neg1h = [r for r in rows if r["_p1h"] is not None and r["_p1h"] < 0]
if neg1h:
    for r in sorted(neg1h, key=lambda x: x["_p1h"]):
        bkt = (r.get("bucket") or "")[:1]
        print(f"  {r['symbol']:12s} {r['signal_type']:10s} [{bkt}] sc={r.get('score','?'):4.0f}  "
              f"1h={r['_p1h']:+.2f}%  max4h={r['_max4h'] or '---'}")
else:
    print("  (ninguna con Delta1h negativo — o aun sin datos)")

# ── POR DIA ───────────────────────────────────────────────────────────────────
print(f"\n{SEP2}")
print("DISTRIBUCION POR DIA")
print(f"{SEP2}")
by_day = defaultdict(lambda: defaultdict(int))
for r in rows:
    d = (r.get("alerted_at") or "")[:10]
    by_day[d][r.get("signal_type", "?")] += 1
for d in sorted(by_day):
    breakdown = "  ".join(f"{k}:{v}" for k, v in sorted(by_day[d].items()))
    total_d = sum(by_day[d].values())
    print(f"  {d}: {total_d:3d} alertas  [{breakdown}]")

# ── SCORES ────────────────────────────────────────────────────────────────────
print(f"\n{SEP2}")
print("DISTRIBUCION DE SCORES")
print(f"{SEP2}")
buckets = defaultdict(int)
for r in rows:
    s = r.get("score") or 0
    buckets[int(s)] += 1
for b in sorted(buckets):
    bar = "█" * buckets[b]
    print(f"  {b:2d}: {bar} ({buckets[b]})")

print()
