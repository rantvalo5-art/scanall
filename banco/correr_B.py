"""Prueba B — ¿el efecto de A es ventaja, o solo movimientos mas chicos?

Si las alertas quietas rinden mejor SOLO porque se mueven menos alrededor de una
deriva negativa, entonces al medir el retorno EN UNIDADES DE SU PROPIA VOLATILIDAD
el efecto tiene que desaparecer. Si sobrevive normalizado, es direccion de verdad.
"""
import glob
import numpy as np
import pandas as pd
import alertas as al
from gate_mediana import lote_mediana

A = al.cargar(sorted(glob.glob("../bt_rk_*.json")))
panel = al.bajar(A, 48, verbose=False)
T = al.tabla_alertas(A, panel, 8, 8, 48, verbose=False)
F = al.features_alertas(A, T, panel, verbose=False)
H = al.hipotesis(F, A, T, cruces=True)

a = A.loc[T["aid"].to_numpy()].reset_index(drop=True)
a.index = T.index
ret = (a["price_24h"].astype(float) / a["entry_price"].astype(float) - 1) * 100 - 0.20

# --- diagnostico: los brazos que sobrevivieron, ¿se mueven menos? ---
D = pd.read_csv("../mediana_gate_c020.csv")
viv = D[D.veredicto == "SOBREVIVE"]["hipotesis"].tolist()
print("=" * 90)
print("DISPERSION: los 83 sobrevivientes, ¿tienen ventaja o solo se mueven menos?")
print("=" * 90)
print(f"{'brazo':30s} {'n':>6s} {'mediana':>8s} {'MAD':>8s} {'|ret| med':>10s} {'med/MAD':>8s}")
base = ret.dropna()
bmad = (base - base.median()).abs().median()
print(f"{'LINEA BASE':30s} {len(base):6,d} {base.median():+8.2f} {bmad:8.2f} "
      f"{base.abs().median():10.2f} {base.median()/bmad:8.3f}")
for nombre in viv[:10]:
    m = H[nombre].reindex(T.index, fill_value=False).fillna(False).astype(bool) & ret.notna()
    r = ret[m]
    mad = (r - r.median()).abs().median()
    print(f"{nombre[:30]:30s} {len(r):6,d} {r.median():+8.2f} {mad:8.2f} "
          f"{r.abs().median():10.2f} {r.median()/mad:8.3f}")

# --- el test: retorno en unidades de la volatilidad del propio simbolo ---
esc = (F["atr_24"] * 100).replace(0, np.nan)
ret_n = ret / esc
print(f"\nescala atr_24: mediana {esc.median():.2f}%  (retorno normalizado = ret / atr_24)")
Dn = lote_mediana(A, T, H, ret_n, 0.20, titulo="(NORMALIZADO por atr_24)")
Dn.to_csv("../mediana_gate_normalizado.csv", index=False)
