"""
Reporte de calibración del CDF por signal_type.

Fuente: outcomes_5d.json (BEST+STRONG, score >= STRONG_MIN_SCORE).
Limitación: datos truncados — WATCH no se guarda en Supabase (TRACK_ALL_CANDIDATES=false).
El ECDF calculado aquí es condicional a score >= 10, no el incondicional.
Sirve como referencia para el rango observable, no para recalibrar la cola izquierda.
"""
import argparse
import json
from collections import Counter

parser = argparse.ArgumentParser()
parser.add_argument("--in",   dest="infile", default="outcomes_5d.json")
parser.add_argument("--config", default="config.json")
args = parser.parse_args()

with open(args.infile, encoding="utf-8") as f:
    rows = json.load(f)

with open(args.config, encoding="utf-8") as f:
    cfg = json.load(f)

config_cdf = cfg.get("scoring_calibration", {}).get("cdf", {})
MIN_SAMPLES = cfg.get("scoring_calibration", {}).get("MIN_SAMPLES_PER_SIGNAL", 30)

# Agrupar scores por signal_type
from collections import defaultdict
by_sig = defaultdict(list)
for r in rows:
    sig = r.get("signal_type")
    sc  = r.get("score")
    if sig and sc is not None:
        by_sig[sig].append(sc)

SEP  = "=" * 64
SEP2 = "-" * 64

print(f"\n{SEP}")
print(f"CALIBRACIÓN CDF — {args.infile} ({len(rows)} filas)")
print(f"NOTA: datos truncados (solo BEST+STRONG, score >= STRONG_MIN_SCORE)")
print(SEP)

for sig in sorted(by_sig):
    scores = sorted(by_sig[sig])
    n = len(scores)
    dist = sorted(Counter(scores).items())

    print(f"\n{SEP2}")
    print(f"{sig}  n={n}{'  ⚠ insuficiente (<' + str(MIN_SAMPLES) + ')' if n < MIN_SAMPLES else ''}")
    print(f"  Distribución: {dict(dist)}")

    if n < MIN_SAMPLES:
        print(f"  → mantener CDF manual del config")
        continue

    # ECDF empírico (condicional a datos disponibles)
    ecdf = {}
    for i, (sc, _) in enumerate(dist):
        cumsum = sum(cnt for s, cnt in dist if s <= sc)
        ecdf[sc] = round(cumsum / n, 3)

    print(f"  ECDF empírico (truncado): {ecdf}")

    # Comparar vs config actual
    cfg_pts = {float(k): float(v) for k, v in config_cdf.get(sig, {}).items()}
    if not cfg_pts:
        print(f"  Config: (sin CDF para esta señal)")
        continue

    print(f"  Config actual: { {int(k): v for k, v in sorted(cfg_pts.items())} }")
    print(f"  Drift por punto observable:")
    for sc, emp in sorted(ecdf.items()):
        # interpolar en config_cdf
        pts = sorted(cfg_pts.items())
        if sc <= pts[0][0]:
            cfg_val = pts[0][1]
        elif sc >= pts[-1][0]:
            cfg_val = pts[-1][1]
        else:
            cfg_val = None
            for (s1, p1), (s2, p2) in zip(pts, pts[1:]):
                if s1 <= sc <= s2:
                    cfg_val = p1 + (p2 - p1) * (sc - s1) / (s2 - s1) if s2 > s1 else p1
                    break
        if cfg_val is not None:
            drift = emp - cfg_val
            flag = " ⚠" if abs(drift) > 0.10 else ""
            print(f"    sc={sc:4.0f}  empírico={emp:.3f}  config={cfg_val:.3f}  drift={drift:+.3f}{flag}")

print(f"\n{SEP}")
print("FIN — para usar en backtest comparativo, mantener CDF manual o actualizar config.")
print("Antes de deploy real: re-recalibrar con datos post-EX11 (5-7 días desde 2026-05-23).")
print(SEP)
