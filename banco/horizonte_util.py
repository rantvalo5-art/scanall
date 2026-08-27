"""A que horizonte sirve el radar, con la metrica que SI se puede comparar.

`barrido_magnitud.py` mostro que el spread crece con el horizonte y la consistencia baja.
Ninguna de las dos es comparable entre horizontes:

  - el spread crece porque el CAMINO crece con el tiempo (y = camino / atr_base),
  - la consistencia semanal sube a horizontes cortos porque hay mas barras por semana
    (a 4h son 42 barras por semana y a 168h es 1), asi que la media semanal es mas
    precisa por construccion, no porque la senal sea mejor.

Lo comparable es sin escala:

  MULTIPLO  camino del top-k / camino del universo. "Se mueve X veces mas que la tipica."
  TASA      cuantas veces la elegida supera la mediana de su propia barra.
  t         spread / error estandar de la media semanal. Cuanta senal por unidad de ruido.

    py -3.13 -u horizonte_util.py
"""
import time

import numpy as np
import pandas as pd

import ranking as R
from klines import load_panel

HORIZONTES = [4, 8, 24, 72, 168]
K = 8
COL = "n_surge"


def main():
    t0 = time.time()
    panel = load_panel("2021-08-01", "2026-08-01", n=46, pin="deriv46", full=True)

    filas = []
    for h in HORIZONTES:
        TB = R.tablero(panel, paso=h, horizonte=h, verbose=False)
        TB["camino"] = TB["runup"] - TB["caida"]

        D = TB[["t", "sym", "semana", "camino", "atr_base"]].copy()
        D["s"] = TB[COL].to_numpy()
        D = D[D.s.notna() & D.camino.notna() & D.atr_base.gt(0)]
        D = D.sort_values(["t", "s"], ascending=[True, False], kind="mergesort")
        sel = D.groupby("t").cumcount() < K
        top = D[sel]

        med = D.groupby("t")["camino"].median()
        j = top.join(med.rename("m"), on="t")
        jb = D.join(med.rename("m"), on="t")

        # t de la media semanal del spread, que es la senal por unidad de ruido
        D["y"] = D["camino"] / D["atr_base"]
        g = D.groupby("t")
        sp = (g.apply(lambda x: x.loc[x.index.isin(top.index), "y"].mean()
                      - x["y"].mean(), include_groups=False).dropna())
        sem = sp.groupby(D.groupby("t")["semana"].first().reindex(sp.index)).mean()
        t_stat = sem.mean() / (sem.std(ddof=1) / np.sqrt(len(sem)))

        filas.append(dict(
            h=h, barras=D["t"].nunique(), semanas=len(sem),
            camino_top=top["camino"].median(), camino_uni=D["camino"].median(),
            multiplo=top["camino"].median() / D["camino"].median(),
            tasa=(j["camino"] > j["m"]).mean(), base=(jb["camino"] > jb["m"]).mean(),
            t=t_stat))
        print(f"h={h:3d}h listo ({time.time()-t0:.0f}s)", flush=True)

    F = pd.DataFrame(filas)
    print("\n" + "=" * 78)
    print(f"{COL}, top-{K} — metricas SIN ESCALA, comparables entre horizontes")
    print("=" * 78)
    print(f"{'horizonte':>10s}{'camino top':>12s}{'camino uni':>12s}"
          f"{'MULTIPLO':>10s}{'TASA':>8s}{'base':>7s}{'t':>7s}")
    print("-" * 78)
    for _, r in F.iterrows():
        print(f"{str(int(r.h)) + 'h':>10s}{100*r.camino_top:>11.2f}%"
              f"{100*r.camino_uni:>11.2f}%{r.multiplo:>9.2f}x"
              f"{100*r.tasa:>7.1f}%{100*r.base:>6.1f}%{r.t:>7.1f}")
    print("\nMULTIPLO = cuanto mas recorre la elegida que la moneda tipica.")
    print("TASA     = veces que la elegida supera la mediana de su barra (base ~50%).")
    print("t        = spread / error estandar semanal. Comparable entre horizontes.")
    F.to_csv("horizonte_util.csv", index=False)


if __name__ == "__main__":
    main()
