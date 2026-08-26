"""
TEST del PREREGISTRO_OI — una sola hipotesis, datos no usados, siete criterios.

La regla (copiada del preregistro, sin cambios):
  regimen : close_1h(BTC) < EMA168(close_1h(BTC))
  senal   : oi_z < -2
  accion  : SHORT al cierre
  salida  : triple barrera +-8%, 7 dias, costo 0,20%

Universo de prueba: `universo_oos_oi.json` = simbolos 41-100 por volumen, ninguno de
los cuales participo del descubrimiento.

Se usa `lote()` con UNA hipotesis a proposito: es el mismo codigo auditado que aplica
las compuertas, y con n=1 la correccion de Benjamini-Hochberg exige p <= 0,10, que es
el criterio 3 del preregistro.
"""
import pandas as pd

from klines import klines, load_panel, to_ms
from lote import features, lote
from metricas import feat_metricas, load_metrics
from primer_toque import tabla

INICIO, FIN = "2021-08-01", "2026-08-01"

print("panel OOS (simbolos 41-100, no usados en el descubrimiento)")
panel = load_panel(INICIO, FIN, n=60, pin="oos_oi")
T = tabla(panel, target=8, stop=8, horizonte_d=7, paso_h=4)
F = features(panel, T)
M = load_metrics(list(panel.keys()), INICIO, FIN, verbose=False)
G = feat_metricas(M, T, verbose=False)
print(f"  cobertura OI: {100*G.oi_z.notna().mean():.1f}% de las entradas")

# --- regimen de BTC (identico al descubrimiento) -----------------------------
btc = klines("BTCUSDT", to_ms(INICIO), to_ms(FIN), "1h")
btc = btc[["t", "c"]].copy()
btc["ema"] = btc["c"].ewm(span=168, adjust=False).mean()
btc["bajista"] = btc["c"] < btc["ema"]
reg = T[["t"]].merge(btc[["t", "bajista"]], on="t", how="left")["bajista"]
reg.index = T.index
reg = reg.fillna(False)

mask = (G.oi_z < -2).fillna(False) & reg

Tc = T.copy()
Tc["res"] = -Tc["res"]          # SHORT: barreras simetricas, se da vuelta el signo
Tc.attrs.update(T.attrs)

print(f"\nentradas totales {len(T):,} | BTC bajista {100*reg.mean():.1f}% | "
      f"senal {int(mask.sum()):,}")
print("\n" + "=" * 100)
print("TEST — una hipotesis preespecificada sobre datos no usados")
print("=" * 100)
D = lote(Tc, {"OI -2z + BTC bajista": mask})

r = D[D.hipotesis == "OI -2z + BTC bajista"].iloc[0]
print("\n" + "=" * 100)
print("LOS SIETE CRITERIOS DEL PREREGISTRO")
print("=" * 100)
crit = [
    ("1. n >= 200",                     r.n >= 200,            f"n={r.n:,}"),
    ("2. win rate > umbral",            r.margen > 0,          f"{r.margen:+.2f} pp"),
    ("3. p bloques <= 0,10",            r.p <= 0.10,           f"p={r.p:.4f}"),
    ("4. le gana al pareado",           r.vs_pareado > 0,      f"{r.vs_pareado:+.2f} pp"),
    ("5. sin top-3 > 0",                r.margen_sin_top3 > 0, f"{r.margen_sin_top3:+.2f} pp"),
    ("6. sin el mejor > 0",             r.margen_sin_top1 > 0, f"{r.margen_sin_top1:+.2f} pp"),
    ("7. >= 60% de semanas",            r.sem_ok >= 0.60,      f"{100*r.sem_ok:.0f}%"),
]
for nombre, ok, val in crit:
    print(f"  {nombre:28s} {val:>14s}   {'OK' if ok else 'FALLA'}")
print("-" * 100)
print("  APRUEBA" if all(c[1] for c in crit)
      else "  FALLA -> se cierra. Sin re-correr con otro umbral, ventana ni horizonte.")
print("-" * 100)
