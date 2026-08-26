"""
TEST del PREREGISTRO_OI, ahora CON funding.

El preregistro conto solo 0,20% de comision. Un short en perpetuo tambien paga (o cobra)
funding cada 8h, y la regla aguanta hasta 7 dias = hasta 21 pagos. Esto lo mide sobre los
mismos trades del test aprobado y vuelve a pasar los siete criterios con el costo real.

No se cambia NADA de la regla: mismo universo, misma senal, mismo regimen, misma salida.
Lo unico que cambia es que el costo deja de ser una suposicion.
"""
import numpy as np
import pandas as pd

from funding import acumulado, bajar
from klines import klines, load_panel, to_ms
from lote import features, lote
from metricas import feat_metricas, load_metrics
from primer_toque import COSTO_PCT, tabla, winrate_necesario

INICIO, FIN = "2021-08-01", "2026-08-01"
INI_MS, FIN_MS = to_ms(INICIO), to_ms(FIN)

panel = load_panel(INICIO, FIN, n=60, pin="oos_oi", verbose=False)
T = tabla(panel, target=8, stop=8, horizonte_d=7, paso_h=4, verbose=False)
F = features(panel, T, verbose=False)
M = load_metrics(list(panel.keys()), INICIO, FIN, verbose=False)
G = feat_metricas(M, T, verbose=False)

btc = klines("BTCUSDT", INI_MS, FIN_MS, "1h")[["t", "c"]].copy()
btc["ema"] = btc["c"].ewm(span=168, adjust=False).mean()
btc["bajista"] = btc["c"] < btc["ema"]
reg = T[["t"]].merge(btc[["t", "bajista"]], on="t", how="left")["bajista"]
reg.index = T.index
mask = (G.oi_z < -2).fillna(False) & reg.fillna(False)

S = T[mask]
print(f"trades de la senal: {len(S):,}  |  simbolos: {S.sym.nunique()}")

print("\nbajando funding...")
FUND = {}
for i, s in enumerate(sorted(S.sym.unique()), 1):
    FUND[s] = bajar(s, INI_MS, FIN_MS)
    if i % 15 == 0:
        print(f"  {i}/{S.sym.nunique()}...", flush=True)

# funding acumulado por trade. Signo directo: positivo = el SHORT cobra.
f = np.array([acumulado(FUND.get(r.sym), int(r.t), int(r.velas))
              for r in S.itertuples()])
sin_datos = sum(1 for s in S.sym.unique() if FUND.get(s) is None)

print(f"\n--- funding sobre {len(f):,} trades ---")
print(f"  simbolos sin serie de funding : {sin_datos}")
print(f"  horas en posicion (mediana)   : {S.velas.median():.0f}h")
print(f"  funding medio por trade       : {100*f.mean():+.4f}%  "
      f"({'el short COBRA' if f.mean() > 0 else 'el short PAGA'})")
print(f"  mediana                       : {100*np.median(f):+.4f}%")
print(f"  p10 / p90                     : {100*np.percentile(f,10):+.4f}% / "
      f"{100*np.percentile(f,90):+.4f}%")
print(f"  trades donde el short paga    : {100*(f < 0).mean():.1f}%")

# El funding es (casi) independiente de que barrera se toca, asi que entra en la
# expectativa como un corrimiento del costo.
costo_real = COSTO_PCT - 100 * f.mean()
print(f"\n  costo del preregistro : {COSTO_PCT:.4f}%")
print(f"  costo REAL con funding: {costo_real:.4f}%")
print(f"  umbral {winrate_necesario(8, 8, COSTO_PCT):.2f}% -> "
      f"{winrate_necesario(8, 8, costo_real):.2f}%")

Tc = T.copy()
Tc["res"] = -Tc["res"]
Tc.attrs.update(T.attrs)

print("\n" + "=" * 100)
print("LOS SIETE CRITERIOS, CON EL COSTO REAL")
print("=" * 100)
D = lote(Tc, {"OI -2z + BTC bajista": mask}, costo=costo_real)
r = D.iloc[0]
crit = [
    ("1. n >= 200",           r.n >= 200,            f"n={r.n:,}"),
    ("2. win rate > umbral",  r.margen > 0,          f"{r.margen:+.2f} pp"),
    ("3. p bloques <= 0,10",  r.p <= 0.10,           f"p={r.p:.4f}"),
    ("4. le gana al pareado", r.vs_pareado > 0,      f"{r.vs_pareado:+.2f} pp"),
    ("5. sin top-3 > 0",      r.margen_sin_top3 > 0, f"{r.margen_sin_top3:+.2f} pp"),
    ("6. sin el mejor > 0",   r.margen_sin_top1 > 0, f"{r.margen_sin_top1:+.2f} pp"),
    ("7. >= 60% de semanas",  r.sem_ok >= 0.60,      f"{100*r.sem_ok:.0f}%"),
]
for nombre, ok, val in crit:
    print(f"  {nombre:28s} {val:>14s}   {'OK' if ok else 'FALLA'}")
print("-" * 100)
print("  APRUEBA CON COSTO REAL" if all(c[1] for c in crit) else "  FALLA con el costo real")
print("-" * 100)
