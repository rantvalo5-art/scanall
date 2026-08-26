"""
Etapa de DESCARGA de metricas, separada del lote.

5 anios x 36 pares son ~66.000 archivos y no entran en un solo proceso. Como
`frame_simbolo` cachea por simbolo, esto es REANUDABLE: si lo matan, se vuelve a
correr y sigue donde iba.

    py -3.13 bajar_metricas.py --inicio 2021-08-01 --fin 2026-08-01
"""
import argparse
import json
import os
import time

import pandas as pd

from metricas import CACHE, frame_simbolo

HERE = os.path.dirname(os.path.abspath(__file__))

ap = argparse.ArgumentParser()
ap.add_argument("--inicio", default="2021-08-01")
ap.add_argument("--fin", default="2026-08-01")
ap.add_argument("--pin", default="metricas40")
ap.add_argument("--lista", default=None,
                help="json con la lista de simbolos (pisa a --pin)")
ap.add_argument("--workers", type=int, default=40)
ap.add_argument("--presupuesto-s", type=int, default=480,
                help="corta y sale limpio antes de que lo mate el harness")
a = ap.parse_args()

ruta = a.lista or os.path.join(HERE, ".kline_cache", f"universo_{a.pin}.json")
with open(ruta, encoding="utf-8") as f:
    syms = json.load(f)

fechas = [d.strftime("%Y-%m-%d")
          for d in pd.date_range(a.inicio, a.fin, freq="D", inclusive="left")]
tag_ini, tag_fin = fechas[0], fechas[-1]

pendientes = [s for s in syms
              if not os.path.exists(os.path.join(CACHE, f"{s}_{tag_ini}_{tag_fin}.pkl"))]
print(f"{len(syms)} pares | {len(fechas)} dias | pendientes: {len(pendientes)}")

t0 = time.time()
hechos = 0
for s in pendientes:
    if time.time() - t0 > a.presupuesto_s:
        print(f"\npresupuesto agotado. faltan {len(pendientes) - hechos}. "
              f"volver a correr para seguir.")
        break
    f = frame_simbolo(s, fechas, workers=a.workers)
    hechos += 1
    n = 0 if f is None else len(f)
    print(f"  [{hechos}/{len(pendientes)}] {s:14s} {n:6,d} horas  "
          f"({time.time()-t0:.0f}s)", flush=True)
else:
    print(f"\nCOMPLETO: los {len(syms)} pares estan en cache.")
