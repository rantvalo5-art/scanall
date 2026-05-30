"""
Fase 0 — diagnóstico HTF: ¿los ganadores swing (24h) son la misma población
que los ganadores intradía (4h)? ¿Están capturados por alineación HTF?

Uso:
    python backtest.py --weeks 3 --max-pairs 300 --out diag_htf_run.json
    python diag_htf.py --in diag_htf_run.json
"""
import argparse
import json
from collections import defaultdict

parser = argparse.ArgumentParser()
parser.add_argument("--in", dest="infile", default="diag_htf_run.json")
parser.add_argument("--decile", type=float, default=0.10, help="Fracción 'top' (default 0.10 = top 10%%)")
args = parser.parse_args()

with open(args.infile, encoding="utf-8") as f:
    raw = json.load(f)

# Aceptar formato run simple {"main": [...]} o compare {"old"/"new": [...]}
if isinstance(raw, list):
    rows = raw
elif "main" in raw:
    rows = raw["main"]
elif "old" in raw:
    rows = raw["old"]
else:
    rows = next(iter(raw.values()))

# Filtrar solo filas con outcome completo (max_high_24h disponible)
rows = [r for r in rows if r.get("max_high_24h") is not None and r.get("entry_price")]

SEP  = "=" * 68
SEP2 = "-" * 68

def pct(new, base):
    if new is None or base is None or base == 0:
        return None
    return (new - base) / base * 100

def avg(lst):
    lst = [x for x in lst if x is not None]
    return round(sum(lst) / len(lst), 2) if lst else None

def rate(lst):
    lst = [x for x in lst if x is not None]
    return round(100 * sum(1 for x in lst if x) / len(lst), 1) if lst else None

for r in rows:
    ep = r["entry_price"]
    r["_max4h"]  = pct(r.get("max_high_4h"),  ep)
    r["_max24h"] = pct(r.get("max_high_24h"), ep)
    r["_p15m"]   = pct(r.get("price_15m"),    ep)
    r["_p1h"]    = pct(r.get("price_1h"),     ep)
    r["_min4h"]  = pct(r.get("min_low_4h"),   ep)

n_total = len(rows)
cutoff  = max(1, int(n_total * args.decile))

print(f"\n{SEP}")
print(f"DIAGNÓSTICO HTF — Fase 0  ({n_total} alertas con outcome completo)")
print(f"Top decil = {args.decile*100:.0f}%  →  top {cutoff} alertas")
print(f"{SEP}")

# ── 1. OVERLAP de poblaciones ──────────────────────────────────────────────────
print(f"\n{SEP2}")
print("1. OVERLAP: ¿top-24h y top-4h son la misma población?")
print(f"{SEP2}")

top24_idx = {i for i, r in enumerate(sorted(rows, key=lambda x: x["_max24h"] or 0, reverse=True)[:cutoff])}
top4_idx  = {i for i, r in enumerate(sorted(rows, key=lambda x: x["_max4h"]  or 0, reverse=True)[:cutoff])}

# Usar sets de símbolos+timestamp para comparar
def top_set(field, n):
    ranked = sorted(rows, key=lambda x: x.get(field) or 0, reverse=True)[:n]
    return {(r["symbol"], r.get("alerted_at", "")[:16]) for r in ranked}

top24_set = top_set("_max24h", cutoff)
top4_set  = top_set("_max4h",  cutoff)
overlap   = top24_set & top4_set

pct_overlap = round(100 * len(overlap) / cutoff, 1)
print(f"  Top {cutoff} por max24h: {len(top24_set)} registros únicos")
print(f"  Top {cutoff} por max4h : {len(top4_set)} registros únicos")
print(f"  Overlap (en ambos)   : {len(overlap)} ({pct_overlap}%)")
print()
if pct_overlap >= 60:
    print("  → ALTA superposición: los ganadores 4h y 24h son la MISMA población.")
    print("    Reforzar filtros HTF debería capturar ambos horizontes (Rama A).")
elif pct_overlap >= 35:
    print("  → SUPERPOSICIÓN MEDIA: hay overlap parcial.")
    print("    El filtro HTF captura parte del swing; puede haber ganancia incremental.")
else:
    print("  → BAJA superposición: 24h y 4h son POBLACIONES DISTINTAS.")
    print("    Los mejores swings no se capturan mejorando filtros intradía (→ Rama B).")

# ── 2. ALINEACIÓN HTF entre top ganadores 24h ──────────────────────────────────
print(f"\n{SEP2}")
print("2. HTF ALIGNMENT: ¿los ganadores 24h ya estaban alineados en 1h/4h?")
print(f"{SEP2}")

has_htf = [r for r in rows if "htf_1h_up" in r and "htf_4h_up" in r]
if not has_htf:
    print("  ⚠ Columnas htf_1h_up / htf_4h_up no encontradas en los datos.")
    print("    Asegurate de correr el backtest con la versión instrumentada.")
else:
    top24_rows = sorted(has_htf, key=lambda x: x["_max24h"] or 0, reverse=True)[:cutoff]
    all_htf_rows = has_htf

    def htf_rates(subset):
        n = len(subset)
        if n == 0:
            return None, None, None
        r1h = rate([r.get("htf_1h_up") for r in subset])
        r4h = rate([r.get("htf_4h_up") for r in subset])
        both= rate([r.get("htf_1h_up") and r.get("htf_4h_up") for r in subset])
        return r1h, r4h, both

    r1h_top, r4h_top, both_top = htf_rates(top24_rows)
    r1h_all, r4h_all, both_all = htf_rates(all_htf_rows)

    print(f"  {'':28s}  {'top 24h':>12}  {'todos':>10}")
    print(f"  {'htf_1h_up (EMA 1h alcista)':28s}  {str(r1h_top)+'%' if r1h_top is not None else '—':>12}  {str(r1h_all)+'%' if r1h_all is not None else '—':>10}")
    print(f"  {'htf_4h_up (EMA 4h alcista)':28s}  {str(r4h_top)+'%' if r4h_top is not None else '—':>12}  {str(r4h_all)+'%' if r4h_all is not None else '—':>10}")
    print(f"  {'ambos alineados':28s}  {str(both_top)+'%' if both_top is not None else '—':>12}  {str(both_all)+'%' if both_all is not None else '—':>10}")
    print()
    if both_top is not None and both_all is not None:
        lift = round(both_top - both_all, 1)
        if lift >= 15:
            print(f"  → LIFT HTF significativo (+{lift}pp): los ganadores 24h son")
            print(f"    desproporcionadamente HTF-alineados. Filtrar/bonificar por HTF")
            print(f"    debería mejorar la calidad de BEST sin perder los swings (Rama A).")
        elif lift >= 5:
            print(f"  → Lift HTF moderado (+{lift}pp): alineación HTF correlaciona algo.")
        else:
            print(f"  → Sin lift HTF significativo ({lift:+.1f}pp): los ganadores 24h no")
            print(f"    son más HTF-alineados que el promedio. Reforzar filtros no ayuda (→ Rama B).")

# ── 3. LATENESS de los ganadores 24h ──────────────────────────────────────────
print(f"\n{SEP2}")
print("3. LATENESS: ¿los grandes swings entraron temprano o ya era tarde?")
print(f"{SEP2}")
top24_rows_all = sorted(rows, key=lambda x: x["_max24h"] or 0, reverse=True)[:cutoff]
bot24_rows_all = sorted(rows, key=lambda x: x["_max24h"] or 0)[:cutoff]

p15_top = avg([r["_p15m"] for r in top24_rows_all])
p15_all = avg([r["_p15m"] for r in rows])
p1h_top = avg([r["_p1h"]  for r in top24_rows_all])
p1h_all = avg([r["_p1h"]  for r in rows])

print(f"  {'':30s}  {'top 24h':>10}  {'todos':>10}")
if p15_top is not None:
    print(f"  {'Δ15m post-entry (proxy lateness)':30s}  {p15_top:>+9.2f}%  {(p15_all or 0):>+9.2f}%")
if p1h_top is not None:
    print(f"  {'Δ1h post-entry':30s}  {p1h_top:>+9.2f}%  {(p1h_all or 0):>+9.2f}%")
drawdown_top = avg([r["_min4h"] for r in top24_rows_all])
drawdown_all = avg([r["_min4h"] for r in rows])
if drawdown_top is not None:
    print(f"  {'drawdown max 4h':30s}  {drawdown_top:>+9.2f}%  {(drawdown_all or 0):>+9.2f}%")
print()
note = ""
if p15_top is not None and p15_all is not None:
    if p15_top > p15_all + 0.5:
        note = "los ganadores 24h suelen continuar bien en los primeros 15m — la latencia de TF grande no sacrifica entrada."
    elif p15_top < p15_all - 0.5:
        note = "los ganadores 24h tienden a retroceder antes de subir — la latencia de TF grande es un problema real."
    else:
        note = "comportamiento 15m similar entre ganadores 24h y el universo completo."
if note:
    print(f"  → {note}")

# ── 4. COMPOSICIÓN por señal y bucket en top 24h ──────────────────────────────
print(f"\n{SEP2}")
print("4. COMPOSICIÓN de top ganadores 24h (por señal y bucket)")
print(f"{SEP2}")
by_sig = defaultdict(int)
by_bkt = defaultdict(int)
for r in top24_rows_all:
    by_sig[r.get("signal_type", r.get("label", "?"))] += 1
    by_bkt[r.get("bucket", "?")] += 1

print(f"  Por señal:")
for sig, n in sorted(by_sig.items(), key=lambda x: -x[1]):
    bar = "█" * n
    print(f"    {sig:14s}: {bar} ({n})")
print(f"  Por bucket:")
for bkt, n in sorted(by_bkt.items(), key=lambda x: -x[1]):
    bar = "█" * n
    print(f"    {bkt:8s}: {bar} ({n})")

# ── 5. TOP 15 ganadores 24h ────────────────────────────────────────────────────
print(f"\n{SEP2}")
print(f"5. TOP 15 ganadores 24h (con flag HTF si disponible)")
print(f"{SEP2}")
for r in top24_rows_all[:15]:
    htf = ""
    if "htf_1h_up" in r:
        htf = f"  1h={'↑' if r['htf_1h_up'] else '↓'}  4h={'↑' if r['htf_4h_up'] else '↓'}"
    sig   = r.get("signal_type", r.get("label", "?"))
    bkt   = (r.get("bucket") or "")[:1]
    sc    = r.get("score", "?")
    mx24  = r["_max24h"]
    mx4   = r.get("_max4h")
    ts    = (r.get("alerted_at") or "")[:16]
    print(f"  {r['symbol']:14s} {sig:10s} [{bkt}] sc={sc:<3}  max24h={mx24:+6.2f}%  max4h={mx4 or 0:+6.2f}%{htf}  {ts}")

print()
