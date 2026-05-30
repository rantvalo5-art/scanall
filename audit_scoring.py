"""
Diagnóstico de scoring: correlación entre componentes y outcomes reales.

Uso:
    python audit_scoring.py --in audit_run.json [--top N] [--min-n N]

Input: JSON de backtest corrido con --audit-scoring (contiene campo "breakdown" por alerta).
Output: reporte de correlación componente → max_24h, post-mortem de top movers.
"""
import sys
sys.stdout.reconfigure(encoding="utf-8")
import argparse
import json
from collections import defaultdict

parser = argparse.ArgumentParser()
parser.add_argument("--in",     dest="infile", default="audit_run.json")
parser.add_argument("--top",    type=int,      default=30,  help="N top movers para post-mortem")
parser.add_argument("--min-n",  type=int,      default=10,  help="Mínimo de muestras para reportar componente")
args = parser.parse_args()

with open(args.infile, encoding="utf-8") as f:
    raw = json.load(f)

# El JSON puede ser {config_name: [alerts]} o directamente [alerts]
if isinstance(raw, dict):
    alerts = list(raw.values())[0]
else:
    alerts = raw

# Solo alertas con breakdown y con outcome completo
alerts = [a for a in alerts if a.get("breakdown") and a.get("max_high_24h") is not None]

def gain(a):
    ep = a.get("entry_price")
    mx = a.get("max_high_24h")
    if not ep or not mx or ep == 0:
        return None
    return round((mx - ep) / ep * 100, 2)

for a in alerts:
    a["_gain"] = gain(a)

alerts = [a for a in alerts if a["_gain"] is not None]

SEP  = "=" * 64
SEP2 = "-" * 64

print(f"\n{SEP}")
print(f"AUDIT SCORING — {args.infile} ({len(alerts)} alertas con breakdown + outcome)")
print(SEP)

# ── 1. CORRELACIÓN COMPONENTE → OUTCOME ──────────────────────────────────────
# Para cada componente: avg gain cuando presente vs ausente, y diff.

print(f"\n{SEP2}")
print("CORRELACIÓN COMPONENTE → MAX_24H GAIN")
print(f"{'Componente':28s}  {'Señal':10s}  {'n_con':6s}  {'avg_con':8s}  {'avg_sin':8s}  {'diff':8s}  flag")
print(SEP2)

# Recolectar por (signal_type, component)
# Para cada alerta: cuáles componentes tiene, y el gain
by_sig_comp = defaultdict(lambda: {"present": [], "absent": []})

all_signals = sorted(set(a["signal_type"] for a in alerts))
all_comps = sorted(set(k for a in alerts for k in (a["breakdown"] or {}).keys()))

for sig in all_signals:
    sig_alerts = [a for a in alerts if a["signal_type"] == sig]
    if not sig_alerts:
        continue
    for comp in all_comps:
        present = [a["_gain"] for a in sig_alerts if comp in a.get("breakdown", {})]
        absent  = [a["_gain"] for a in sig_alerts if comp not in a.get("breakdown", {})]
        if len(present) < args.min_n:
            continue
        avg_p = round(sum(present) / len(present), 1) if present else None
        avg_a = round(sum(absent) / len(absent), 1) if absent else None
        if avg_p is None or avg_a is None:
            continue
        diff = round(avg_p - avg_a, 1)
        # Flags: penalty presente pero outcome mejor = penalty anti-útil (⚠)
        #        bonus presente pero outcome peor = bonus anti-útil (⚠)
        bd_vals = [a["breakdown"].get(comp, 0) for a in sig_alerts if comp in a["breakdown"]]
        avg_delta = sum(bd_vals) / len(bd_vals) if bd_vals else 0
        is_penalty = avg_delta < 0
        is_bonus   = avg_delta > 0
        flag = ""
        if is_penalty and diff > 2:
            flag = "⚠ PENALTY INNECESARIA"
        elif is_bonus and diff < -2:
            flag = "⚠ BONUS CONTRAPRODUCENTE"
        by_sig_comp[(sig, comp)] = {"n": len(present), "avg_p": avg_p, "avg_a": avg_a, "diff": diff, "flag": flag, "avg_delta": avg_delta}

# Ordenar por |diff| descendente
ranked = sorted(by_sig_comp.items(), key=lambda x: abs(x[1]["diff"]), reverse=True)

prev_sig = None
for (sig, comp), v in ranked:
    if sig != prev_sig:
        print(f"\n  [{sig}]")
        prev_sig = sig
    sign = "+" if v["avg_delta"] >= 0 else ""
    delta_str = f"({sign}{v['avg_delta']:.1f})"
    print(f"  {comp:28s}  n={v['n']:4d}  con={v['avg_p']:+6.1f}%  sin={v['avg_a']:+6.1f}%  diff={v['diff']:+5.1f}%  {v['flag']}")

# ── 2. RESUMEN GLOBAL POR COMPONENTE (todas las señales) ────────────────────
print(f"\n{SEP2}")
print("RANKING GLOBAL DE COMPONENTES (todas las señales, |diff| descendente)")
print(SEP2)

global_comp = defaultdict(lambda: {"present": [], "absent": []})
for comp in all_comps:
    present = [a["_gain"] for a in alerts if comp in a.get("breakdown", {})]
    absent  = [a["_gain"] for a in alerts if comp not in a.get("breakdown", {})]
    if len(present) < args.min_n:
        continue
    avg_p = round(sum(present) / len(present), 1)
    avg_a = round(sum(absent) / len(absent), 1) if absent else None
    if avg_a is None:
        continue
    diff = round(avg_p - avg_a, 1)
    all_deltas = [a["breakdown"][comp] for a in alerts if comp in a.get("breakdown", {})]
    avg_delta = sum(all_deltas) / len(all_deltas) if all_deltas else 0
    is_penalty = avg_delta < 0
    is_bonus   = avg_delta > 0
    flag = ""
    if is_penalty and diff > 2:
        flag = "⚠ PENALTY INNECESARIA"
    elif is_bonus and diff < -2:
        flag = "⚠ BONUS CONTRAPRODUCENTE"
    global_comp[comp] = {"n": len(present), "avg_p": avg_p, "avg_a": avg_a, "diff": diff, "flag": flag}

for comp, v in sorted(global_comp.items(), key=lambda x: abs(x[1]["diff"]), reverse=True):
    print(f"  {comp:28s}  n={v['n']:4d}  con={v['avg_p']:+6.1f}%  sin={v['avg_a']:+6.1f}%  diff={v['diff']:+5.1f}%  {v['flag']}")

# ── 3. POST-MORTEM TOP-N MOVERS ───────────────────────────────────────────────
print(f"\n{SEP2}")
print(f"POST-MORTEM — TOP {args.top} MOVERS (breakdown de cada alerta)")
print(SEP2)

top = sorted(alerts, key=lambda a: a["_gain"] or 0, reverse=True)[:args.top]
for i, a in enumerate(top, 1):
    bd = a.get("breakdown", {})
    bd_str = "  ".join(f"{k}:{v:+d}" for k, v in sorted(bd.items()) if v != 0)
    print(f"  #{i:2d} {a['symbol']:14s} {a['signal_type']:9s} sc={a['score']:2d} {a['bucket']:6s} "
          f"gain={a['_gain']:+6.1f}%  | {bd_str}")

# ── 4. DISTRIBUCIÓN DE SCORES POR BUCKET (sanity check) ─────────────────────
print(f"\n{SEP2}")
print("DISTRIBUCIÓN GAIN POR BUCKET (sanity check)")
print(SEP2)
for bucket in ["BEST", "STRONG", "WATCH"]:
    g = [a["_gain"] for a in alerts if a.get("bucket") == bucket]
    if not g:
        continue
    avg = round(sum(g) / len(g), 1)
    median = sorted(g)[len(g)//2]
    print(f"  {bucket:6s}  n={len(g):4d}  avg={avg:+6.1f}%  median={median:+6.1f}%")

# ── 5. BUCKET DISTRIBUTION POR SIGNAL_TYPE ──────────────────────────────────
print(f"\n{SEP2}")
print("BUCKET DISTRIBUTION POR SIGNAL_TYPE (avg / median / win-rate >5%)")
print(SEP2)

def stats(gains):
    if not gains:
        return None
    n = len(gains)
    avg = round(sum(gains) / n, 1)
    med = round(sorted(gains)[n // 2], 1)
    wr  = round(100 * sum(1 for g in gains if g > 5) / n, 0)
    return n, avg, med, int(wr)

for sig in all_signals:
    sig_alerts = [a for a in alerts if a["signal_type"] == sig]
    if not sig_alerts:
        continue
    print(f"\n  [{sig}]")
    print(f"  {'Bucket':7s}  {'n':>5s}  {'avg':>7s}  {'median':>7s}  {'wr>5%':>6s}")
    for bucket in ["BEST", "STRONG", "WATCH"]:
        g = [a["_gain"] for a in sig_alerts if a.get("bucket") == bucket]
        s = stats(g)
        if s:
            n, avg, med, wr = s
            marker = "  <<< WATCH > BEST" if bucket == "WATCH" else ""
            print(f"  {bucket:7s}  {n:5d}  {avg:+6.1f}%  {med:+6.1f}%  {wr:5d}%{marker}")

# ── 6. SCORE → GAIN SCATTER POR SIGNAL_TYPE ─────────────────────────────────
print(f"\n{SEP2}")
print("SCORE → GAIN POR SIGNAL_TYPE (avg gain por bin de score)")
print(SEP2)

for sig in all_signals:
    sig_alerts = [a for a in alerts if a["signal_type"] == sig]
    if not sig_alerts:
        continue
    by_score = defaultdict(list)
    for a in sig_alerts:
        by_score[a["score"]].append(a["_gain"])
    scores = sorted(by_score.keys())
    print(f"\n  [{sig}]")
    print(f"  {'sc':>4s}  {'n':>5s}  {'avg':>7s}  {'median':>7s}  {'wr>5%':>6s}  bucket_ref")
    for sc in scores:
        g = by_score[sc]
        s = stats(g)
        if not s:
            continue
        n, avg, med, wr = s
        # referencia de bucket para ese score
        from_config = "BEST" if sc >= 13 else ("STRONG" if sc >= 11 else "WATCH")
        print(f"  {sc:4d}  {n:5d}  {avg:+6.1f}%  {med:+6.1f}%  {wr:5d}%  {from_config}")

# ── 7. LEVERAGE DE COMPONENTES (score loss agregado = Σ|delta|) ──────────────
print(f"\n{SEP2}")
print("LEVERAGE DE COMPONENTES (Σ|delta| total, n alertas afectadas)")
print(f"{'Componente':28s}  {'n_afect':7s}  {'Σ|delta|':10s}  {'avg|delta|':10s}  {'avg_outcome_diff':16s}")
print(SEP2)

leverage = {}
for comp in all_comps:
    deltas = [abs(a["breakdown"][comp]) for a in alerts if comp in a.get("breakdown", {})]
    if len(deltas) < 5:
        continue
    total = sum(deltas)
    avg_d = round(total / len(deltas), 2)
    outcome_diff = global_comp.get(comp, {}).get("diff")
    leverage[comp] = {"n": len(deltas), "total": round(total, 1), "avg_d": avg_d, "diff": outcome_diff}

for comp, v in sorted(leverage.items(), key=lambda x: x[1]["total"], reverse=True):
    diff_str = f"{v['diff']:+.1f}%" if v["diff"] is not None else "  n/a"
    print(f"  {comp:28s}  {v['n']:7d}  {v['total']:10.1f}  {v['avg_d']:10.2f}  {diff_str}")

print(f"\n{SEP}\nFIN\n{SEP}\n")
