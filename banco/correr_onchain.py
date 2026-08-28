"""
CORRIDA 6 — ON-CHAIN: la unica informacion que no sale del precio ni del libro.

Direccion 4.3 de `HANDOFF_SIGUIENTE.md`. Preregistro con la regla de parada:
`PREREGISTRO_ONCHAIN.md`.

Diseno: **espejo de la corrida 3** (la de derivados), porque es el unico comparador
honesto — misma ventana de 5 anios, mismo `k=8`, mismo `paso = horizonte = 24h` sin
solape, mismas seis compuertas, mismo FDR sobre el lote entero. Lo unico que cambia es
la FUENTE: en vez de OI y posicionamiento de futuros, actividad de cadena.

    py -3.13 -u correr_onchain.py --nula     # SOLO el n post-join y el MDE (paso 1)
    py -3.13 -u correr_onchain.py            # el lote entero

El `--nula` primero no es opcional: la regla del handoff es **contar el n post-join y
calcular el MDE con la nula real ANTES de estimar nada**. Es lo que convirtio un "no se
pudo medir" en un "no esta" en unlocks y en la cola iliquida.
"""
import argparse
import json
import os
import sys
import time

import numpy as np
import pandas as pd

import onchain
from klines import load_panel, universe
from ranking import (MIN_SYMS, controles, lote_rankings, mde_del_azar, scores,
                     tablero)
from primer_toque import COSTO_PCT

INICIO, FIN = "2021-08-01", "2026-08-01"
K, PASO, HORIZONTE = 8, 24, 24
COSTO_DURO = 0.50

# NO son cripto y no compiten en el mismo ranking: stablecoins, tokens de oro, y el
# BTC envuelto (que es el mismo activo que BTC y duplicaria la seccion cruzada).
# Se sacan ANTES de correr, no despues de ver un resultado. Es la regla de metodo que
# dejo la corrida 5: el universo se filtra por CLASE DE ACTIVO, no solo por volumen.
FUERA = {"USDCUSDT", "TUSDUSDT", "PAXGUSDT", "XAUTUSDT", "WBTCUSDT"}


def preparar(workers=12, verbose=True):
    """Panel de precios + on-chain alineado sin lookahead. Devuelve (TB, mapa)."""
    mapa = onchain.activos_binance(universe(3000))
    mapa = {a: s for a, s in mapa.items() if s not in FUERA}
    syms = sorted(mapa.values())
    if verbose:
        print(f"universo on-chain: {len(syms)} activos "
              f"(de {len(syms) + len(FUERA)}, sacando {len(FUERA)} que no son cripto)")

    panel = load_panel(INICIO, FIN, full=True, workers=workers, syms=syms,
                       min_bars=2000)
    if not panel:
        return None, None
    TB = tablero(panel, paso=PASO, horizonte=HORIZONTE)

    M = onchain.bajar(sorted(mapa), INICIO, FIN)
    if M is None:
        return None, None
    F = onchain.alinear(TB[["sym", "t"]], M, mapa)
    TB = pd.concat([TB, F], axis=1)

    cob = F.notna().any(axis=1).mean()
    print(f"on-chain: {M['asset'].nunique()} activos | {len(M):,} filas diarias | "
          f"cobertura de filas del tablero {cob:.1%}")
    return TB, mapa


def n_post_join(TB):
    """El conteo que el handoff exige ANTES de estimar nada."""
    cols = [c for c in TB.columns if any(c.startswith(m) for m in
                                         onchain.METRICAS + onchain.HIBRIDAS)]
    v = TB[cols].notna().any(axis=1)
    D = TB[v]
    print("\n" + "=" * 72)
    print("n POST-JOIN — se cuenta ANTES de estimar (regla del handoff)")
    print("=" * 72)
    print(f"  filas del tablero            {len(TB):,}")
    print(f"  filas CON on-chain           {len(D):,}  ({len(D)/len(TB):.1%})")
    print(f"  barras                       {D['t'].nunique():,}")
    print(f"  activos                      {D['sym'].nunique()}")
    print(f"  SEMANAS (el n independiente) {D['semana'].nunique()}")
    barras = D.groupby("t")["sym"].nunique()
    print(f"  activos por barra: mediana {barras.median():.0f} | "
          f"p5 {barras.quantile(.05):.0f} | barras con >= {MIN_SYMS}: "
          f"{(barras >= MIN_SYMS).mean():.1%}")
    print("\n  comparador — unlocks murio con 1.040 eventos y MDE 6,6 pp/decada;")
    print("  la corrida 3 (derivados) tenia 46 pares y 251 semanas, MDE +-0,062 ATR.")
    return D


def main():
    ap = argparse.ArgumentParser(description="Banco — corrida 6: on-chain")
    ap.add_argument("--k", type=int, default=K)
    ap.add_argument("--workers", type=int, default=12)
    ap.add_argument("--nula", action="store_true",
                    help="SOLO el n post-join y el MDE del azar, y sale")
    ap.add_argument("--out", default="rank_onchain.csv")
    a = ap.parse_args()

    t0 = time.time()
    TB, mapa = preparar(workers=a.workers)
    if TB is None:
        print("FATAL: no se pudo preparar el panel")
        sys.exit(1)
    n_post_join(TB)

    if a.nula:
        mde_del_azar(TB, k=a.k, costo=COSTO_PCT)
        print("\nCon esto se decide si vale la pena correr el lote. Recien despues,")
        print("y con el preregistro escrito, correr sin --nula.")
        return

    mde = mde_del_azar(TB, k=a.k, costo=COSTO_PCT)
    R = {**scores(TB), **controles(TB)}
    print(f"\n{len(R)} rankings x 3 objetivos = {len(R)*3} brazos")

    partes = []
    for c in (COSTO_PCT, COSTO_DURO):
        print(f"\n{'#' * 104}\n# ON-CHAIN — costo {c:.2f}%\n{'#' * 104}")
        D = lote_rankings(TB, R, k=a.k, costo=c,
                          mde=float(np.median(list(mde.values()))))
        D.insert(0, "costo", c)
        partes.append(D)
    D = pd.concat(partes, ignore_index=True)
    D.to_csv(a.out, index=False)
    print(f"\ntabla -> {a.out} | {time.time() - t0:.0f}s")


if __name__ == "__main__":
    main()
