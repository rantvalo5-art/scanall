"""
TEST del PREREGISTRO_CASCADA — una sola hipotesis, una sola corrida, siete criterios.

La regla (copiada del preregistro, sin cambios):
  regimen : close_1h(BTC) < EMA168(close_1h(BTC))
  senal   : oi_z < -2
  cascada : k >= N*p0 + 2*sqrt(N*p0*(1-p0))   con p0 = phi(-2) = 0,02275
  accion  : SHORT al cierre
  salida  : triple barrera +-8%, 7 dias, costo 0,20%

Universo de prueba: `metricas40`. Produjo la regla ANCHA, pero nunca se lo miro por
cascada — esa particion se observo entera sobre el OOS-54, que por eso quedo gastado.

Los criterios 3 y 7 van con `sem_n_min=1` (TODAS las semanas). Ese es el arreglo: el
filtro de 20 del harness es exactamente lo que la auditoria del 2026-08-22 encontro roto.

NOTA de implementacion: el preregistro dice "la hora de la senal". La tabla de entradas
vive en una grilla de paso_h=4, asi que "hora" se implementa como LA BARRA DE ENTRADA —
que es el punto de decision real y lo unico computable en vivo. La derivacion del umbral
no depende de la grilla: p0 es la probabilidad marginal de oi_z < -2 en una observacion
cualquiera, sea la grilla horaria o de 4h.
"""
import numpy as np
import pandas as pd

from klines import klines, load_panel, to_ms
from lote import lote
from metricas import _feat, feat_metricas, load_metrics
from primer_toque import tabla

INICIO, FIN = "2021-08-01", "2026-08-01"
P0 = 0.02275                      # phi(-2)

print("panel metricas40 (el universo del descubrimiento, nunca mirado por cascada)")
panel = load_panel(INICIO, FIN, n=40, pin="metricas40")
T = tabla(panel, target=8, stop=8, horizonte_d=7, paso_h=4)
M = load_metrics(list(panel.keys()), INICIO, FIN, verbose=False)
G = feat_metricas(M, T, verbose=False)
print(f"  cobertura OI: {100*G.oi_z.notna().mean():.1f}% de las entradas")

# --- regimen de BTC (identico al preregistro anterior) ------------------------
btc = klines("BTCUSDT", to_ms(INICIO), to_ms(FIN), "1h")[["t", "c"]].copy()
btc["ema"] = btc["c"].ewm(span=168, adjust=False).mean()
btc["bajista"] = btc["c"] < btc["ema"]
reg = T[["t"]].merge(btc[["t", "bajista"]], on="t", how="left")["bajista"]
reg.index = T.index
reg = reg.fillna(False)

# --- cascada: cuantos del universo disparan en la MISMA HORA ------------------
# OJO: se calcula sobre la grilla COMPLETA simbolo x hora (`FULL`), no sobre la tabla
# de entradas. Las entradas estan escalonadas por simbolo — agrupar por `t` ahi da
# N mediano 2 en vez de 21, el umbral colapsa a k>=1 y la condicion queda inerte.
# Esa fue la corrida NULA del 2026-08-22; ver el anexo del preregistro.
senal = (G.oi_z < -2).fillna(False)
FULL = pd.concat([pd.DataFrame({"t": d["t"].to_numpy(), "oi_z": _feat(d)["oi_z"]})
                  for d in M.values()], ignore_index=True)
FULL["fire"] = FULL.oi_z < -2
hora = FULL.groupby("t").agg(k=("fire", "sum"), N=("oi_z", "count"))
hora["umbral"] = hora.N * P0 + 2 * np.sqrt(hora.N * P0 * (1 - P0))
hora["cascada"] = hora.k >= hora.umbral
casc = pd.Series(T["t"].map(hora["cascada"]).to_numpy(), index=T.index).fillna(False)

mask = senal & reg & casc

print(f"\nhoras distintas {len(hora):,} | N mediano {hora.N.median():.0f} "
      f"-> umbral mediano k>={np.ceil(hora.umbral.median()):.0f}")
print(f"horas de cascada {int(hora.cascada.sum()):,} ({100*hora.cascada.mean():.1f}%) "
      f"| bajo independencia se esperaria ~8,2%")
print(f"senal sola {int(senal.sum()):,} | + bajista {int((senal & reg).sum()):,} "
      f"| + cascada {int(mask.sum()):,}")

Tc = T.copy()
Tc["res"] = -Tc["res"]            # SHORT: barreras simetricas, se da vuelta el signo
Tc.attrs.update(T.attrs)

print("\n" + "=" * 100)
print("TEST — una hipotesis preespecificada, un solo tiro")
print("=" * 100)
D = lote(Tc, {"OI -2z + bajista + CASCADA": mask}, sem_n_min=1)
r = D.iloc[0]

print("\n" + "=" * 100)
print("LOS SIETE CRITERIOS DEL PREREGISTRO_CASCADA")
print("=" * 100)
crit = [
    ("1. n >= 200",              r.n >= 200,                 f"n={r.n:,}"),
    ("2. win rate > umbral",     r.margen > 0,               f"{r.margen:+.2f} pp"),
    ("3. p bloques <= 0,10",     r.p <= 0.10,                f"p={r.p:.4f}"),
    ("4. le gana al pareado",    r.vs_pareado > 0,           f"{r.vs_pareado:+.2f} pp"),
    ("5. sin top-3 > 0",         r.margen_sin_top3 > 0,      f"{r.margen_sin_top3:+.2f} pp"),
    ("6. sin el mejor > 0",      r.margen_sin_top1 > 0,      f"{r.margen_sin_top1:+.2f} pp"),
    ("7. >= 60% de semanas",     r.sem_ok >= 0.60,           f"{100*r.sem_ok:.0f}%"),
]
for nombre, ok, val in crit:
    print(f"  {nombre:28s} {val:>14s}   {'OK' if ok else 'FALLA'}")
print("-" * 100)
print("  APRUEBA" if all(c[1] for c in crit)
      else "  NO APRUEBA — se cierra. Sin re-operacionalizar, sin otro universo.")
print("-" * 100)

# --- diagnostico declarado (NO es compuerta) ---------------------------------
S = Tc[Tc.resuelto][mask.reindex(Tc[Tc.resuelto].index, fill_value=False)]
psem = S.groupby("semana").size()
print(f"\nDIAGNOSTICO (declarado, no decide): {len(psem)} semanas con senal | "
      f"mediana {psem.median():.0f} senales/sem | "
      f"semanas de 1-2 senales: {100*(psem <= 2).mean():.0f}%")
print("Si quedan muchas semanas flacas, la definicion horaria no captura el mecanismo.")
