"""Compara PREBREAK entre configs sobre LA MISMA ventana: volumen y contaminacion.

La metrica de contaminacion es la SUBIDA PREVIA: mediana del camino de -24h a 0 en
unidades de ATR. PREBREAK base da +0,89 (el precio estaba MAS ALTO antes = no compra
techo). Las demas senales dan -3 a -4. Si al aflojar la subida previa se vuelve
negativa, la variante se contamino con el defecto que se queria evitar.
"""
import sys, glob
import numpy as np
import pandas as pd
import alertas as al

OFFS = [-24, -12, 0, 12, 24, 48]

def analiza(paths, etiqueta):
    A = al.cargar(paths)
    panel = al.bajar(A, 48 + 24, verbose=False)
    T = al.tabla_alertas(A, panel, 8, 8, 48, verbose=False)
    a = A.loc[T["aid"].to_numpy()].reset_index(drop=True); a.index = T.index
    atr = {}
    for s, df in panel.items():
        h, l, c = df["h"].to_numpy(), df["l"].to_numpy(), df["c"].to_numpy()
        atr[s] = (pd.Series(h - l).rolling(24).mean().to_numpy() / c) * 100

    print(f"\n=== {etiqueta} ===")
    print(f"  alertas totales {len(T):,}  |  " +
          "  ".join(f"{k} {v}" for k, v in a['signal_type'].value_counts().items()))
    for tp in ["PREBREAK"] + [x for x in sorted(a.signal_type.unique()) if x != "PREBREAK"]:
        m = a["signal_type"] == tp
        filas = []
        for r in T[m].itertuples():
            df = panel.get(r.sym)
            if df is None: continue
            t = df["t"].to_numpy(); c = df["c"].to_numpy()
            j0 = int(np.searchsorted(t, r.t, side="left"))
            if j0 < 24 or j0 + 48 >= len(c): continue
            s = atr[r.sym][j0]
            if not np.isfinite(s) or s <= 0: continue
            filas.append([(c[j0+o]/c[j0]-1)*100/s for o in OFFS])
        if len(filas) < 20:
            print(f"    {tp:10s} n={len(filas):4d}  (pocos)"); continue
        v = np.nanmedian(np.array(filas), axis=0)
        subida = v[OFFS.index(0)] - v[0]
        print(f"    {tp:10s} n={len(filas):4d}  " +
              " ".join(f"{o:+d}h {x:+5.2f}" for o, x in zip(OFFS, v)) +
              f"   subida previa {subida:+.2f}")

analiza(["../bt_rk_2026-08-01.json"], "BASE (config.json)")
analiza(["../bt_P5_2026-08-01.json"], "P5 = todo aflojado")
