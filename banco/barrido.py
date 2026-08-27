"""BARRIDO de horizonte y k — el ultimo lugar barato donde puede estar la direccion.

Las corridas 1-3 midieron siempre lo mismo: comprar y mirar 24h despues, eligiendo 8 o
20 monedas. Si la direccion vive en otro horizonte —horas, o una semana— o requiere ser
mucho mas selectivo, no la habrian visto.

La regla de parada esta en `PREREGISTRO_TRANSVERSAL.md`, corrida 4, escrita antes.

Lo que hace honesto a un barrido es la multiplicidad: mirar 15 configuraciones y quedarse
con la mejor es una maquina de fabricar falsos positivos. Aca **Benjamini-Hochberg corre
sobre los 4.230 brazos juntos**, no por configuracion.

    py -3.13 -u barrido.py                 # el barrido entero
    py -3.13 -u barrido.py --costo 0.50    # la compuerta de costo
"""
import argparse
import sys
import time

import numpy as np
import pandas as pd

import ranking as R
from klines import load_panel
from metricas import feat_metricas, load_metrics
from primer_toque import COSTO_PCT

HORIZONTES = [4, 8, 24, 72, 168]
KS = [3, 8, 16]
OBJETIVOS = ["largo", "corto"]


def una_config(TB, S, h, k, costo, verbose=True):
    """Todos los rankings x los dos objetivos direccionales, para un (horizonte, k)."""
    filas = []
    for o in OBJETIVOS:
        y = f"y_{o}"
        for nombre, s in S.items():
            sem, aporte, sem_crudo, ratios = R._spread_semanal(TB, s, k, y, costo)
            if sem is None or len(sem) < R.SEM_N_MIN:
                continue
            m = float(sem.mean())
            filas.append(dict(
                h=h, k=k, objetivo=o, ranking=nombre,
                semanas=int(len(sem)), spread=m,
                spread_crudo=float(sem_crudo.mean()),
                atr_ratio=ratios[0], atr_propio=ratios[1],
                sem_ok=float((sem > 0).mean()),
                # el bootstrap solo tiene sentido si el spread es positivo; para el
                # resto la nula ya esta rechazada por el signo
                p=R._p_bloques(sem) if m > 0 else 1.0))
    if verbose:
        pos = sum(1 for f in filas if f["spread"] > 0)
        print(f"    h={h:3d}h k={k:2d} -> {len(filas):4d} brazos, "
              f"{pos:3d} con spread > 0", flush=True)
    return filas


def main():
    ap = argparse.ArgumentParser(description="Banco — barrido de horizonte y k")
    ap.add_argument("--costo", type=float, default=COSTO_PCT)
    ap.add_argument("--q", type=float, default=R.Q_FDR)
    ap.add_argument("--out", default="barrido.csv")
    a = ap.parse_args()

    t0 = time.time()
    panel = load_panel("2021-08-01", "2026-08-01", n=46, pin="deriv46", full=True)
    M = load_metrics(list(panel), "2021-08-01", "2026-08-01", verbose=False)

    todo = []
    for h in HORIZONTES:
        print(f"\n=== horizonte {h}h (paso {h}h, sin solape) ===", flush=True)
        TB = R.tablero(panel, paso=h, horizonte=h, verbose=False)
        TB = pd.concat([TB, feat_metricas(M, TB[["sym", "t"]], verbose=False)], axis=1)
        S = {**R.scores(TB), **R.controles(TB)}
        print(f"  {len(TB):,} filas | {TB['t'].nunique():,} barras | "
              f"{TB['semana'].nunique()} semanas | {len(S)} rankings", flush=True)
        for k in KS:
            todo += una_config(TB, S, h, k, a.costo)

    D = pd.DataFrame(todo)
    if D.empty:
        print("FATAL: el barrido no produjo brazos"); sys.exit(1)

    # ---- multiplicidad sobre el BARRIDO ENTERO, no por configuracion
    reales = ~D["ranking"].str.startswith("CONTROL")
    D["fdr_ok"] = False
    D.loc[reales, "fdr_ok"] = R._bh(D.loc[reales, "p"].to_numpy(), a.q)

    # ---- MDE por configuracion, de los controles al azar de esa misma configuracion
    ctrl = (D[~reales].groupby(["h", "k", "objetivo"])
            .agg(ctrl_spread=("spread", "median"), ctrl_sd=("spread", "std"))
            .reset_index())
    D = D.merge(ctrl, on=["h", "k", "objetivo"], how="left")

    cand = D[reales & (D.spread > 0) & (D.spread_crudo > 0) & D.fdr_ok].copy()

    print("\n" + "=" * 92)
    print(f"BARRIDO — {len(D[reales]):,} brazos direccionales | "
          f"{len(HORIZONTES)}x{len(KS)} configuraciones | costo {a.costo:.2f}% | "
          f"FDR q={a.q} sobre TODO")
    print("=" * 92)

    print(f"\n{'':6s}" + "".join(f"{'k=' + str(k):>22s}" for k in KS))
    print(f"{'horiz':6s}" + "".join(f"{'mejor spread / n>0':>22s}" for _ in KS))
    print("-" * 92)
    for h in HORIZONTES:
        fila = f"{h:>4d}h "
        for k in KS:
            sub = D[reales & (D.h == h) & (D.k == k)]
            if sub.empty:
                fila += f"{'--':>22s}"
                continue
            mejor = sub["spread"].max()
            npos = int((sub["spread"] > 0).sum())
            fila += f"{mejor:>+13.4f} /{npos:>4d}   "
        print(fila)

    print(f"\ncandidatos (spread>0, crudo>0, pasan FDR): {len(cand)}")
    if cand.empty:
        print("\nNINGUN brazo direccional pasa en NINGUNA de las 15 configuraciones.")
        print("Por la regla 1 del preregistro (corrida 4) la familia queda cerrada:")
        print("ranking transversal top-k sobre precio + flujo + posicionamiento.")
        print("Lo que quedaria vivo es OTRA COSA: otra resolucion (5m), otra fuente")
        print("(libro, on-chain, listados) u otra forma (banda, delta-rank, multi-feature).")
    else:
        print("\nOJO regla 2: un pico solitario rodeado de ceros es ruido de barrido.")
        print("Un efecto real es CONTINUO en horizonte y k.\n")
        c = cand.sort_values("spread", ascending=False)
        print(c.head(25)[["h", "k", "objetivo", "ranking", "semanas", "spread",
                          "spread_crudo", "sem_ok", "p"]].to_string(index=False))
        # continuidad: cuantas configuraciones distintas toca cada ranking
        print("\ncontinuidad por ranking (cuantas de las 15 configuraciones toca):")
        print(c.groupby(["ranking", "objetivo"]).size()
              .sort_values(ascending=False).head(15).to_string())

    D.to_csv(a.out, index=False)
    print(f"\ntabla -> {a.out}   ({time.time() - t0:.0f}s)")


if __name__ == "__main__":
    main()
