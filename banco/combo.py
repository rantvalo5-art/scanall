"""La combinacion, medida — porque combinar es una HIPOTESIS NUEVA, no una consecuencia.

La corrida 3 midio `oi_rel_168`, `n_surge` y `turnover` POR SEPARADO. Un screener que las
sume esta apostando a que aportan cosas distintas, y eso no esta medido: si las tres
miden lo mismo (actividad), sumarlas no agrega nada y solo agrega perillas.

Se mide contra las mismas compuertas de `ranking.py` y contra el mejor individual, que es
la linea base honesta: una combinacion tiene que GANARLE al mejor de sus partes, no al
azar.

    py -3.13 -u combo.py
"""
import numpy as np
import pandas as pd

import ranking as R
from klines import load_panel
from metricas import feat_metricas, load_metrics

PARTES = ["oi_rel_168", "n_surge", "turnover"]


def z_barra(TB, col):
    return TB.groupby("t")[col].transform(R._z)


def main():
    panel = load_panel("2021-08-01", "2026-08-01", n=46, pin="deriv46", full=True)
    M = load_metrics(list(panel), "2021-08-01", "2026-08-01", verbose=False)
    TB = R.tablero(panel, paso=24, horizonte=24)
    TB = pd.concat([TB, feat_metricas(M, TB[["sym", "t"]], verbose=False)], axis=1)

    # correlacion entre las partes DENTRO de la barra: si es alta, sumar no aporta
    Z = pd.DataFrame({c: z_barra(TB, c) for c in PARTES})
    print("\ncorrelacion transversal entre las partes (dentro de cada barra):")
    print(Z.corr().round(3).to_string())

    cands = {c: TB[c] for c in PARTES}
    cands["COMBO oi+n+turn"] = Z.mean(axis=1)
    cands["COMBO oi+turn"] = Z[["oi_rel_168", "turnover"]].mean(axis=1)
    cands["COMBO n+turn"] = Z[["n_surge", "turnover"]].mean(axis=1)
    cands.update(R.controles(TB))

    D = R.lote_rankings(TB, cands, k=8, objetivos=("magnitud",), mostrar=True)

    mejor_parte = D[D.ranking.isin(PARTES)].spread.max()
    print(f"\n{'-'*70}")
    print(f"linea base honesta = mejor parte individual: {mejor_parte:+.4f}")
    for _, r in D[D.ranking.str.startswith("COMBO")].iterrows():
        d = r.spread - mejor_parte
        veredicto = "GANA" if d > 0 else "NO le gana al mejor individual"
        print(f"  {r.ranking:20s} {r.spread:+.4f}  ({d:+.4f} vs mejor parte)  {veredicto}")
    print("\nSi ninguna combinacion le gana al mejor individual, el screener usa UNA")
    print("feature y no tres. Menos perillas, menos sobreajuste, mismo resultado.")


if __name__ == "__main__":
    main()
