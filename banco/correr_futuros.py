"""
CORRIDA 5 — el INSTRUMENTO: perpetuo en vez de spot.

Preregistro con la regla de parada: `PREREGISTRO_FUTUROS.md`, escrito ANTES de bajar la
primera vela de futuros.

Tres paneles, todo lo demas identico a la corrida 2 (ventana 2025-08 -> 2026-08, 1h,
paso = horizonte = 24h sin solape, k=20, panel ANCHO, las dos direcciones, 3 objetivos):

    S     spot, universo base200                       costo 0,20%   sin carry
    FS    PERP de los mismos simbolos de base200       costo 0,10%   con funding
    F200  PERP, top-200 por volumen de perp            costo 0,10%   con funding

S vs FS aisla el INSTRUMENTO (precio del perp + costo + funding) con el universo clavado.
FS vs F200 aisla el UNIVERSO. Sin esa particion, un sobreviviente no se puede atribuir.

    py -3.13 -u correr_futuros.py                 # los tres paneles, dos costos
    py -3.13 -u correr_futuros.py --paneles FS    # uno solo
"""
import argparse
import json
import os
import sys
import time

import pandas as pd

import futuros
from klines import CACHE, load_panel, universe
from ranking import controles, lote_rankings, mde_del_azar, scores, tablero

COSTO_SPOT = 0.20      # taker spot 0,10%/lado, ida y vuelta
COSTO_FUT = 0.10       # taker futuros 0,05%/lado, ida y vuelta
COSTO_DURO = 0.50      # compuerta 7 de siempre: un sobreviviente solo-barato no cuenta

INICIO, FIN = "2025-08-01", "2026-08-01"
K, PASO, HORIZONTE = 20, 24, 24


def _perp_de(spot_syms):
    """Mapea cada simbolo de spot a su perpetuo, si existe.

    En perp una moneda barata cotiza como `1000PEPEUSDT`: mismo activo, precio x1000,
    retornos identicos. El mapeo es por variantes, igual que en `funding.py`.
    """
    # el mapeo se CONGELA en disco: `universe` consulta el ranking en vivo y dos
    # corridas separadas por horas no tienen por que resolver los mismos nombres.
    path = os.path.join(CACHE, "mapa_perp_base200.json")
    if os.path.exists(path):
        with open(path, encoding="utf-8") as f:
            return json.load(f)
    vivos = set(universe(2000, mercado="fut"))
    par = {}
    for s in spot_syms:
        for cand in (s, f"1000{s}", f"1000000{s}"):
            if cand in vivos:
                par[s] = cand
                break
    with open(path, "w", encoding="utf-8") as f:
        json.dump(par, f)
    return par


def _universo_spot():
    with open(os.path.join(CACHE, "universo_base200.json"), encoding="utf-8") as f:
        return json.load(f)


def panel_de(nombre, workers):
    """Devuelve (panel, costo, es_perp) del panel pedido."""
    if nombre == "S":
        return load_panel(INICIO, FIN, n=200, pin="base200", full=True,
                          workers=workers), COSTO_SPOT, False
    if nombre == "FS":
        par = _perp_de(_universo_spot())
        print(f"base200 -> perp: {len(par)} de 200 tienen perpetuo vivo")
        raros = [f"{k}->{v}" for k, v in par.items() if k != v]
        if raros:
            print(f"  renombrados: {', '.join(raros)}")
        return load_panel(INICIO, FIN, full=True, workers=workers, mercado="fut",
                          syms=sorted(set(par.values()))), COSTO_FUT, True
    if nombre == "SF":
        # spot, pero SOLO los simbolos que efectivamente quedaron en el panel FS.
        # Sin esto la comparacion S vs FS mezcla dos cosas: el instrumento y los 26
        # nombres de base200 que no tienen perp y por eso desaparecen del panel de
        # futuros. Este panel clava el universo de verdad, simbolo por simbolo.
        par = _perp_de(_universo_spot())
        fs, _, _ = panel_de("FS", workers)
        atras = {v: k for k, v in par.items()}
        syms = sorted(atras[s] for s in fs if s in atras)
        print(f"panel SF: {len(syms)} simbolos de spot = los mismos que entraron a FS")
        return load_panel(INICIO, FIN, full=True, workers=workers,
                          syms=syms), COSTO_SPOT, False
    if nombre == "F200":
        return load_panel(INICIO, FIN, n=200, pin="fut200", full=True,
                          workers=workers, mercado="fut"), COSTO_FUT, True
    raise ValueError(nombre)


# Que costos corre cada panel. SF corre TRES a proposito: 0,20% es su costo real
# (spot), 0,10% es el de futuros puesto sobre la serie de spot —o sea, el efecto del
# ABARATAMIENTO SOLO, sin cambiar de serie ni pagar funding— y 0,50% es la compuerta
# dura de siempre. La diferencia SF@0,20 -> SF@0,10 es costo; SF@0,10 -> FS@0,10 es
# la serie del perp mas el funding. Sin esa particion un sobreviviente no se atribuye.
COSTOS = {"S": (COSTO_SPOT, COSTO_DURO),
          "SF": (COSTO_SPOT, COSTO_FUT, COSTO_DURO),
          "FS": (COSTO_FUT, COSTO_DURO),
          "F200": (COSTO_FUT, COSTO_DURO)}


def correr(nombre, workers=12, k=K):
    panel, costo, es_perp = panel_de(nombre, workers)
    if not panel:
        print(f"FATAL: panel {nombre} vacio")
        return None

    TB = tablero(panel, paso=PASO, horizonte=HORIZONTE)
    if es_perp:
        C = futuros.carry(TB[["sym", "t"]], INICIO, FIN, horizonte=HORIZONTE)
        TB = futuros.aplicar(TB, C)
        vivas = TB["ret"].notna().mean()
        print(f"panel {nombre}: filas con retorno neto de funding {vivas:.1%} | "
              f"carry mediano a 24h {100*TB['carry'].median():+.4f}%")

    R = {**scores(TB), **controles(TB)}
    out = []
    for c in COSTOS[nombre]:
        print(f"\n{'#' * 104}\n# PANEL {nombre} — costo {c:.2f}%\n{'#' * 104}")
        mde = mde_del_azar(TB, k=k, costo=c)
        D = lote_rankings(TB, R, k=k, costo=c,
                          mde=float(pd.Series(mde).median()))
        D.insert(0, "panel", nombre)
        D.insert(1, "costo", c)
        out.append(D)
    return pd.concat(out, ignore_index=True)


def resumen(D):
    """La tabla que contesta la regla de parada 1: sobrevivientes DIRECCIONALES."""
    dire = D[D["objetivo"].isin(["largo", "corto"])]
    print("\n" + "=" * 78)
    print("REGLA DE PARADA 1 — sobrevivientes DIRECCIONALES por panel y costo")
    print("=" * 78)
    print(f"{'panel':6s} {'costo':>6s} {'brazos':>7s} {'spread>0':>9s} "
          f"{'SOBREVIVEN':>11s} {'mejor spread':>13s}")
    print("-" * 78)
    for (p, c), g in dire.groupby(["panel", "costo"], sort=False):
        g = g[g["veredicto"] != "control"]
        print(f"{p:6s} {c:6.2f} {len(g):7d} {(g['spread'] > 0).sum():9d} "
              f"{(g['veredicto'] == 'SOBREVIVE').sum():11d} "
              f"{g['spread'].max():+13.4f}")
    print("-" * 78)
    mag = D[(D["objetivo"] == "magnitud") & (D["veredicto"] != "control")]
    print("\nmagnitud (replica, no hallazgo nuevo — ver §7 del preregistro):")
    for (p, c), g in mag.groupby(["panel", "costo"], sort=False):
        print(f"  {p:6s} costo {c:.2f}: {(g['veredicto'] == 'SOBREVIVE').sum():3d} "
              f"sobreviven de {len(g)} | mejor {g['spread'].max():+.3f}")


def main():
    ap = argparse.ArgumentParser(description="Banco — corrida 5: futuros vs spot")
    ap.add_argument("--paneles", nargs="+", default=["S", "FS", "F200"])
    ap.add_argument("--workers", type=int, default=12)
    ap.add_argument("--k", type=int, default=K)
    ap.add_argument("--out", default="rank_futuros.csv")
    a = ap.parse_args()

    t0 = time.time()
    partes = []
    for nombre in a.paneles:
        D = correr(nombre, workers=a.workers, k=a.k)
        if D is not None:
            partes.append(D)
            D.to_csv(f"rank_fut_{nombre}.csv", index=False)
    if not partes:
        sys.exit(1)
    D = pd.concat(partes, ignore_index=True)
    D.to_csv(a.out, index=False)
    resumen(D)
    print(f"\ntabla -> {a.out} | {time.time() - t0:.0f}s")


if __name__ == "__main__":
    main()
