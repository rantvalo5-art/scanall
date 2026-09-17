"""Prueba A — gatear la MEDIANA. Ver PREREGISTRO_RANKING.md seccion 8."""
import glob
import pandas as pd
import alertas as al
from gate_mediana import lote_mediana, barrido_nmin

A = al.cargar(sorted(glob.glob("../bt_rk_*.json")))
panel = al.bajar(A, 48, verbose=False)
T = al.tabla_alertas(A, panel, 8, 8, 48, verbose=False)
F = al.features_alertas(A, T, panel, verbose=False)
H = al.hipotesis(F, A, T, cruces=True)

a = A.loc[T["aid"].to_numpy()].reset_index(drop=True)
a.index = T.index
for costo in (0.20, 0.50):
    ret = (a["price_24h"].astype(float) / a["entry_price"].astype(float) - 1) * 100 - costo
    D = lote_mediana(A, T, H, ret, costo, titulo=f"(costo {costo:.2f}%)")
    D.to_csv(f"../mediana_gate_c{int(costo*100):03d}.csv", index=False)
    viv = D[D.veredicto == "SOBREVIVE"]["hipotesis"].tolist()
    if viv:
        print(f"\n  === BARRIDO n_min de los {len(viv)} sobrevivientes ===")
        for nombre in viv[:8]:
            barrido_nmin(A, T, H, ret, nombre)
