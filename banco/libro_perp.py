"""
LIBRO DEL PERPETUO — la pieza que la corrida 5 dejo abierta.

La corrida 5 cerro 4.2, pero con un pendiente concreto: sus cuatro candidatos cruzan cero
entre **31 y 46 bps** ida y vuelta, y el unico costo real que el banco tenia medido era el
de **spot** (`libro.py` caminaba `api.binance.com`). El fee de futuros se conoce —0,05%
por lado, o sea 10 bps ida y vuelta— pero el otro sumando, **spread + slippage del libro
del perp**, nunca se midio. Sin ese numero los cuatro candidatos no estan ni vivos ni
muertos.

    costo_roundtrip = 2*fee + slippage(ida) + slippage(vuelta)

## Por que pareado y en el mismo instante

Medir el perp hoy y compararlo contra la tabla de spot del handoff §2.3 —medida otro dia—
mezcla **mercado** con **momento**. Es exactamente el error que la corrida 5 casi comete
con los paneles (un salto de +0,41 ATR que parecia el instrumento y era el universo), y
que quedo como regla de metodo. Aca los dos libros se piden en la **misma vuelta**, para
los **mismos activos**, y se reporta la diferencia **apareada por simbolo**.

## El mapeo de nombres

En perp una moneda barata cotiza como `1000PEPEUSDT`. Es el mismo activo con el precio
x1000, asi que el spread PORCENTUAL es comparable directamente; lo que no es comparable es
el string. Se resuelve sacando el prefijo cuando el simbolo de spot existe.

    py -3.13 -u libro_perp.py --top 200 --orden 1000 10000 --reps 3
"""
import argparse
import time

import numpy as np
import pandas as pd

from klines import universe
from libro import FEE, snapshot, universo_por_volumen

# el rango que la corrida 5 dejo escrito: los cuatro candidatos cruzan cero ahi
EQUILIBRIO = (0.31, 0.46)


def mapear(perps):
    """{simbolo_perp: simbolo_spot} para los que existen en los dos mercados."""
    spot = set(universe(3000))
    out = {}
    for p in perps:
        for cand in (p, p[4:] if p.startswith("1000") else None,
                     p[7:] if p.startswith("1000000") else None):
            if cand and cand in spot:
                out[p] = cand
                break
    return out


def correr(top=200, orden=1000.0, reps=3, espera=15.0):
    U = universo_por_volumen(top, mercado="fut")
    mapa = mapear(U.sym.tolist())
    print(f"perps por volumen: {len(U)} | con par de spot vivo: {len(mapa)}")

    filas = []
    for r in range(reps):
        print(f"  snapshot {r+1}/{reps} (orden ${orden:,.0f})...", flush=True)
        # LOS DOS LIBROS EN LA MISMA VUELTA. Ese es el punto del archivo.
        F = snapshot(list(mapa), orden, mercado="fut")
        S = snapshot(list(set(mapa.values())), orden, mercado="spot")
        for p, sp in mapa.items():
            if p in F.index and sp in S.index:
                filas.append({"perp": p, "spot": sp, "rep": r,
                              "costo_fut": F.loc[p, "costo"],
                              "costo_spot": S.loc[sp, "costo"],
                              "spread_fut": F.loc[p, "spread"],
                              "spread_spot": S.loc[sp, "spread"],
                              "slip_fut": F.loc[p, "slip"],
                              "slip_spot": S.loc[sp, "slip"]})
        if r < reps - 1:
            time.sleep(espera)

    A = pd.DataFrame(filas)
    G = A.groupby(["perp", "spot"]).median(numeric_only=True).drop(columns="rep")
    G = G.join(U.set_index("sym")["rank"], on="perp").sort_values("rank")
    G["dif"] = G["costo_fut"] - G["costo_spot"]
    return G


def informe(G, orden):
    banda = pd.cut(G["rank"], [0, 50, 200, 400, 10_000],
                   labels=["1-50", "51-200", "201-400", "400+"])
    t = G.groupby(banda, observed=True).agg(
        pares=("costo_fut", "size"),
        spread_fut=("spread_fut", "median"), spread_spot=("spread_spot", "median"),
        slip_fut=("slip_fut", "median"),
        costo_fut=("costo_fut", "median"), costo_spot=("costo_spot", "median"),
        dif=("dif", "median"),
        no_llena=("slip_fut", lambda s: float(s.isna().mean() * 100)))
    print("\n" + "=" * 92)
    print(f"COSTO ROUND-TRIP TAKER, orden de ${orden:,.0f} — PERP contra SPOT, "
          f"mismo instante")
    print(f"  fee: {2*FEE['fut']:.2f}% ida y vuelta en perp, {2*FEE['spot']:.2f}% en spot")
    print("=" * 92)
    print(t.to_string(float_format=lambda x: f"{x:9.3f}"))
    print("\n  'no_llena' = % de perps donde el libro no alcanza para la orden.")

    lo, hi = EQUILIBRIO
    print("\n" + "-" * 92)
    print(f"LA DECISION — los 4 candidatos de la corrida 5 cruzan cero entre "
          f"{100*lo:.0f} y {100*hi:.0f} bps")
    print("-" * 92)
    for nom, sub in (("top-200 perp", G[G["rank"] <= 200]),
                     ("top-50 perp", G[G["rank"] <= 50])):
        if not len(sub):
            continue
        c = sub["costo_fut"].median()
        vivos = float((sub["costo_fut"] < lo).mean() * 100)
        print(f"  {nom:14s} costo mediano {100*c:5.1f} bps | "
              f"{vivos:5.1f}% de los pares por debajo de {100*lo:.0f} bps | "
              f"{'DEBAJO' if c < lo else 'ADENTRO' if c <= hi else 'ARRIBA'} de la banda")
    return t


def main():
    ap = argparse.ArgumentParser(description="Banco — libro del perpetuo vs spot")
    ap.add_argument("--top", type=int, default=200)
    ap.add_argument("--orden", type=float, nargs="+", default=[1000.0, 10000.0])
    ap.add_argument("--reps", type=int, default=3)
    ap.add_argument("--csv", default="libro_perp.csv")
    a = ap.parse_args()

    partes = []
    for usd in a.orden:
        print(f"\n{'#'*92}\n# ORDEN ${usd:,.0f}\n{'#'*92}")
        G = correr(a.top, usd, a.reps)
        informe(G, usd)
        G = G.reset_index()
        G["orden"] = usd
        partes.append(G)
    D = pd.concat(partes, ignore_index=True)
    D.to_csv(a.csv, index=False)
    print(f"\n-> {a.csv}")


if __name__ == "__main__":
    main()
