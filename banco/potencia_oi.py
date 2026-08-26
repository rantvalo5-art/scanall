"""
POTENCIA del forward test del OI shock — cuantas semanas bajistas hacen falta.

NO es un test ni mira datos nuevos. Usa la distribucion semanal YA GASTADA (el OOS
del preregistro) para calcular la caracteristica operativa del forward: con k semanas,
que probabilidad hay de que la regla —siendo verdadera— cruce sus propias compuertas.

Existe porque `SEMANAS_MIN = 8` en forward_oi.py fue puesto a ojo, y `_p_bloques`
descarta semanas con < 20 senales y devuelve p=1 con menos de 8 semanas utiles.
Si 8 no alcanza, hay que saberlo AHORA — no cuando el forward de p=0,14 y de ganas
de "esperar un poco mas", que es multiplicidad encubierta.
"""
import numpy as np
import pandas as pd

from klines import klines, load_panel, to_ms
from lote import features, _p_bloques
from metricas import feat_metricas, load_metrics
from primer_toque import tabla, winrate_necesario

INICIO, FIN = "2021-08-01", "2026-08-01"
NEC = winrate_necesario(8.0, 8.0, 0.181)   # con el funding cobrado ya medido
print(f"umbral de win rate necesario: {NEC:.2f}%\n")

panel = load_panel(INICIO, FIN, n=60, pin="oos_oi")
T = tabla(panel, target=8, stop=8, horizonte_d=7, paso_h=4, verbose=False)
M = load_metrics(list(panel.keys()), INICIO, FIN, verbose=False)
G = feat_metricas(M, T, verbose=False)

btc = klines("BTCUSDT", to_ms(INICIO), to_ms(FIN), "1h")[["t", "c"]].copy()
btc["ema"] = btc["c"].ewm(span=168, adjust=False).mean()
btc["bajista"] = btc["c"] < btc["ema"]
reg = T[["t"]].merge(btc[["t", "bajista"]], on="t", how="left")["bajista"]
reg.index = T.index
S = T[(G.oi_z < -2).fillna(False) & reg.fillna(False) & T.resuelto].copy()
S["gana"] = S.res < 0          # short gana cuando el largo pierde

sem = S.groupby("semana").gana.agg(wr=lambda s: 100*s.mean(), n="size").sort_index()
utiles = sem[sem.n >= 20]
print(f"senales {len(S):,} | semanas {len(sem)} | semanas UTILES (n>=20) {len(utiles)} "
      f"({100*len(utiles)/len(sem):.0f}%)")
print(f"senales por semana: mediana {sem.n.median():.0f}  p25 {sem.n.quantile(.25):.0f}  "
      f"p75 {sem.n.quantile(.75):.0f}")
print(f"win rate semanal (utiles): media {utiles.wr.mean():.2f}%  DE {utiles.wr.std():.2f}pp  "
      f"| semanas arriba del umbral {100*(utiles.wr > NEC).mean():.0f}%")

# ---- bootstrap anidado: k semanas -> pasa las compuertas? ----
wr_pool = utiles.wr.to_numpy()
n_pool  = utiles.n.to_numpy()
rng = np.random.default_rng(7)
OUT, REPS = 3000, 1500

print(f"\n{'k semanas':>10} {'P(p<=0,10)':>11} {'P(sem>=60%)':>12} {'P(n>=200)':>10} "
      f"{'P(TODAS)':>10} {'p mediano':>10}")
for k in (8, 10, 12, 16, 20, 26, 39, 52):
    idx = rng.integers(0, len(wr_pool), size=(OUT, k))
    muestras, cuentas = wr_pool[idx], n_pool[idx]
    ps = np.empty(OUT)
    for i in range(OUT):                       # bootstrap interno = el de _p_bloques
        m = muestras[i][rng.integers(0, k, size=(REPS, k))].mean(axis=1)
        ps[i] = (m <= NEC).mean()
    g3 = ps <= 0.10
    g7 = (muestras > NEC).mean(axis=1) >= 0.60
    g1 = cuentas.sum(axis=1) >= 200
    gw = muestras.mean(axis=1) > NEC
    print(f"{k:>10} {g3.mean():>10.0%} {g7.mean():>11.0%} {g1.mean():>9.0%} "
          f"{(g3&g7&g1&gw).mean():>9.0%} {np.median(ps):>10.3f}")
