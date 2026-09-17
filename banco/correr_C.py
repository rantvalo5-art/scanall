"""Prueba C — demorar la entrada. Ataca el mismo defecto que `ext B` desde el lado del
TIEMPO en vez del lado de la seleccion.

Si el problema es que el bot compra el techo de la vela de extension, entonces esperar
k horas antes de entrar deberia mejorar. Salida siempre 24h DESPUES DE LA ENTRADA, asi
que todos los brazos tienen el mismo tiempo expuesto (si no, se compara 24h contra 16h).

Todo en unidades de atr_24: la prueba B mostro que sin normalizar se mide escala.
"""
import glob
import numpy as np
import pandas as pd
import alertas as al
from gate_mediana import _difs_semanales, _p_bloques_mediana

HORA = al.HORA
DEMORAS = (0, 1, 2, 4, 8, 12)

A = al.cargar(sorted(glob.glob("../bt_rk_*.json")))
panel = al.bajar(A, 48 + max(DEMORAS) + 24, verbose=False)
T = al.tabla_alertas(A, panel, 8, 8, 48, verbose=False)
F = al.features_alertas(A, T, panel, verbose=False)
a = A.loc[T["aid"].to_numpy()].reset_index(drop=True)
a.index = T.index
esc = (F["atr_24"] * 100).replace(0, np.nan)

# retorno con entrada demorada k horas y salida 24h despues de ENTRAR
def ret_demorado(k, costo=0.20):
    out = np.full(len(T), np.nan)
    for i, r in enumerate(T.itertuples()):
        df = panel.get(r.sym)
        if df is None:
            continue
        t = df["t"].to_numpy()
        j0 = int(np.searchsorted(t, r.t, side="left")) + k
        j1 = j0 + 24
        if j1 >= len(t) or j0 >= len(t):
            continue
        c = df["c"].to_numpy()
        out[i] = (c[j1] / c[j0] - 1) * 100 - costo
    return pd.Series(out, index=T.index)

print("=" * 96)
print("DEMORA DE ENTRADA — mediana del retorno a 24h POST-ENTRADA, en unidades de atr_24")
print("=" * 96)
print(f"{'demora':>7s} {'n':>7s} {'mediana':>9s} {'vs k=0':>8s} {'sem>0':>7s} {'sems':>5s} {'p':>8s}")

base = None
for k in DEMORAS:
    rn = ret_demorado(k) / esc
    ok = rn.notna()
    med = float(rn[ok].median())
    if base is None:
        base = med
    difs = _difs_semanales(T[ok], rn, T["semana"], 5)
    p, ns = _p_bloques_mediana(difs)
    print(f"{k:6d}h {int(ok.sum()):7,d} {med:+9.3f} {med-base:+8.3f} "
          f"{100*(difs>0).mean() if len(difs) else float('nan'):6.0f}% {ns:5d} {p:8.4f}")

# por tipo de senal: donde vive el defecto
print("\n" + "=" * 96)
print("POR TIPO DE SENAL — mediana normalizada, por demora")
print("=" * 96)
tipos = sorted(a["signal_type"].dropna().unique())
print(f"{'tipo':12s} {'n':>6s} " + " ".join(f"{k:>7d}h" for k in DEMORAS) + "   mejor")
rr = {k: ret_demorado(k) / esc for k in DEMORAS}
for tp in tipos:
    m = (a["signal_type"] == tp)
    fila, n = [], int(m.sum())
    for k in DEMORAS:
        v = rr[k][m].dropna()
        fila.append(float(v.median()) if len(v) > 30 else np.nan)
    mejor = DEMORAS[int(np.nanargmax(fila))] if not all(np.isnan(fila)) else -1
    print(f"{tp:12s} {n:6,d} " + " ".join(f"{v:+8.3f}" for v in fila) + f"   {mejor}h")
