"""
CORRIDA 11 — compuerta de potencia para PATRONES DE GRAFICO, ANTES de construir el detector.

Es la advertencia que las corridas 8 y 9 dejaron pagada, escrita en el §2.1 del handoff:

    Antes de construir el detector, correr la compuerta de potencia para medir la sigma
    por evento y ver cuantas semanas harian falta. Un patron de grafico es un evento
    esparcido sobre simbolos volatiles, o sea exactamente la forma que hizo fracasar a la
    corrida 9. Si la sigma sale del orden de la de listados, se cierra ahi y no se
    escriben las 300 lineas.

LA IDEA. El MDE de este estimador no depende de QUE patron sea: depende de la TASA DE
DISPARO. Un patron que dispara poco tiene pocas barras por semana, la media semanal es
mas ruidosa y el MDE se abre. Asi que se puede medir la curva MDE(tasa) con mascaras AL
AZAR —la nula real— sin escribir un solo detector, y despues preguntar si los patrones de
grafico disparan en la zona medible o afuera.

    py -3.13 -u potencia_graficos.py --tf 1d

Referencias medidas, de la corrida 7 (mismo estimador, mismo universo, misma ventana):
    1d: 253 semanas, MDE +-0,0386 ATR
    1h: 257 semanas, MDE +-0,0221 ATR
"""
import argparse
import json
import os
import sys
import time

import numpy as np
import pandas as pd

from correr_velas import COSTOS, FUERA, INICIO, FIN, PARAMS, _exceso, tablero_eventos
from klines import CACHE, load_panel

# El umbral: un patron es medible si su MDE detecta un efecto que valdria la pena.
# Con un ATR base tipico de ~4%/dia, un costo de 0,20% son ~0,05 ATR. Un efecto que
# sobreviva a dos costos tiene que estar bastante arriba de eso: 0,10 ATR es la vara.
MDE_MAX = 0.10
Z = 2.80                      # 1,96 + 0,84
REPS = 7                      # mascaras al azar por cada tasa
TASAS = (0.05, 0.03, 0.02, 0.01, 0.005, 0.002, 0.001, 0.0005, 0.0002)


def mde_de_tasa(TB, tasa, reps=REPS, semilla=0, costo=COSTOS[0]):
    """MDE del azar a una tasa de disparo dada. La nula real, no una supuesta."""
    rng = np.random.default_rng(semilla)
    sds, sems, disp = [], [], []
    for _ in range(reps):
        m = pd.Series(rng.random(len(TB)) < tasa, index=TB.index)
        sem, _, _, n = _exceso(TB, m, "largo", costo)
        if sem is None or len(sem) < 2:
            continue
        sds.append(sem.std(ddof=1))
        sems.append(len(sem))
        disp.append(n)
    if not sds:
        return None
    sd, ns = float(np.median(sds)), float(np.median(sems))
    return {"tasa": tasa, "disparos": float(np.median(disp)), "semanas": ns,
            "sd_sem": sd, "mde": Z * sd / np.sqrt(ns)}


def main():
    ap = argparse.ArgumentParser(description="Banco — corrida 11: potencia de patrones de grafico")
    ap.add_argument("--tf", default="1d", choices=["1d", "1h"])
    ap.add_argument("--workers", type=int, default=12)
    a = ap.parse_args()

    with open(os.path.join(CACHE, "universo_base200.json"), encoding="utf-8") as f:
        syms = [s for s in json.load(f) if s not in FUERA]
    mb = 400 if a.tf == "1d" else 8000
    panel = load_panel(INICIO, FIN, tf=a.tf, full=True, workers=a.workers,
                       syms=syms, min_bars=mb)
    if not panel:
        print("FATAL: panel vacio")
        sys.exit(1)

    HS = PARAMS[a.tf]["horizontes"]

    print("=" * 88)
    print(f"CORRIDA 11 — POTENCIA DE PATRONES DE GRAFICO ({a.tf}, H={HS})")
    print("=" * 88)
    print("La compuerta va ANTES del detector: el MDE de este estimador no depende de QUE")
    print("patron sea, sino de cada cuanto dispara. Se mide con mascaras al azar.")
    print(f"\numbral: MDE <= {MDE_MAX} ATR (a ~4%/dia de ATR base, un costo son ~0,05 ATR;")
    print("        un efecto que sobreviva a dos costos tiene que estar arriba de eso)")
    print(f"referencia corrida 7 a {a.tf}: "
          f"MDE +-{'0,0386' if a.tf == '1d' else '0,0221'} ATR\n")

    t0 = time.time()
    filas = []
    # OJO: el MDE crece con el HORIZONTE, no solo con la tasa. Medir un solo horizonte
    # —el mas corto— reporta el mejor caso y no la compuerta.
    for H in HS:
        TB = tablero_eventos(panel, a.tf, H)
        print(f"\n  H={H}")
        print(f"  {'tasa':>9}{'disparos':>11}{'barras/sem':>12}{'semanas':>9}"
              f"{'sd sem':>10}{'MDE (ATR)':>12}{'':>4}")
        for p in TASAS:
            r = mde_de_tasa(TB, p, semilla=int(p * 1e6))
            if r is None:
                print(f"  {p:>8.2%}{'sin disparos suficientes':>46}")
                continue
            r["tf"], r["H"] = a.tf, H
            filas.append(r)
            marca = "  ok" if r["mde"] <= MDE_MAX else "  NO"
            print(f"  {p:>8.2%}{r['disparos']:>11,.0f}"
                  f"{r['disparos']/max(r['semanas'],1):>12.1f}"
                  f"{r['semanas']:>9.0f}{r['sd_sem']:>10.4f}{r['mde']:>12.4f}{marca}")
    R = pd.DataFrame(filas)
    print(f"  ({time.time()-t0:.0f}s)")

    if R.empty:
        print("\nno se pudo calcular la curva")
        return 1

    print(f"\n{'='*88}\nDONDE ESTA LA FRONTERA, HORIZONTE POR HORIZONTE\n{'='*88}")
    print(f"  {'H':>4}{'tasa minima medible':>22}{'MDE ahi':>10}")
    for H in HS:
        g = R[(R.H == H) & (R.mde <= MDE_MAX)]
        if len(g):
            pmin = g.tasa.min()
            print(f"  {H:>4}{pmin:>21.3%}{g[g.tasa==pmin].mde.iloc[0]:>10.4f}")
        else:
            print(f"  {H:>4}{'ninguna de la grilla':>22}{'-':>10}")
    print("\n  LA PREGUNTA QUE DECIDE 2.1: un patron de grafico dispara mas o menos")
    print("  que esa tasa? Se contesta con un detector minimo y sin estimar un efecto.")
    print("=" * 88)
    R.to_csv(f"potencia_graficos_{a.tf}.csv", index=False)
    return 0


if __name__ == "__main__":
    sys.exit(main())
