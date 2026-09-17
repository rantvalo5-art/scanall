"""Prueba D — persistencia. El `repeat penalty` esta en config y nunca se midio."""
import glob
import numpy as np
import pandas as pd
import alertas as al
from gate_mediana import lote_mediana

A = al.cargar(sorted(glob.glob("../bt_rk_*.json")))
panel = al.bajar(A, 48, verbose=False)
T = al.tabla_alertas(A, panel, 8, 8, 48, verbose=False)
F = al.features_alertas(A, T, panel, verbose=False)
a = A.loc[T["aid"].to_numpy()].reset_index(drop=True)
a.index = T.index
esc = (F["atr_24"] * 100).replace(0, np.nan)
ret = ((a["price_24h"].astype(float) / a["entry_price"].astype(float) - 1) * 100 - 0.20) / esc

# cuantas alertas previas tuvo ESE simbolo en las ultimas k horas (solo pasado)
tms = T["t"].to_numpy()
sym = T["sym"].to_numpy()
for k in (24, 72):
    prev = np.zeros(len(T), int)
    for i in range(len(T)):
        lo = tms[i] - k * al.HORA
        prev[i] = int(((sym == sym[i]) & (tms >= lo) & (tms < tms[i])).sum())
    F[f"prev_{k}h"] = prev

print("=" * 84)
print("PERSISTENCIA — mediana normalizada segun cuantas alertas previas tuvo el simbolo")
print("=" * 84)
for k in (24, 72):
    print(f"\n  ventana {k}h:")
    print(f"    {'previas':>9s} {'n':>7s} {'mediana':>9s} {'vs 0':>8s}")
    col = F[f"prev_{k}h"]
    base = None
    for lab, m in [("0", col == 0), ("1", col == 1), ("2", col == 2),
                   ("3-4", col.isin([3, 4])), (">=5", col >= 5)]:
        v = ret[m].dropna()
        if len(v) < 60:
            continue
        med = float(v.median())
        if base is None:
            base = med
        print(f"    {lab:>9s} {len(v):7,d} {med:+9.3f} {med-base:+8.3f}")

H = {}
for k in (24, 72):
    c = F[f"prev_{k}h"]
    H[f"prev_{k}h = 0 (primera)"] = c == 0
    H[f"prev_{k}h >= 2 (repetida)"] = c >= 2
    H[f"prev_{k}h >= 5 (insistente)"] = c >= 5
D = lote_mediana(A, T, H, ret, 0.20, titulo="(persistencia, normalizado)")
