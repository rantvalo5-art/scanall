"""Prueba C (corregida) — demora de entrada, con estadistico PAREADO.

Correccion: la version anterior comparaba el brazo contra "todas las alertas" cuando el
brazo ERA todas las alertas -> diferencia cero por construccion y p=1 sin sentido.

Diseno correcto: la demora es un tratamiento sobre LA MISMA alerta, asi que el par
natural es (ret con demora k) - (ret sin demora) en la misma alerta. Se bootstrapea por
semana sobre esa diferencia.

Control de hora-del-dia: k=24h entra a la MISMA hora del dia que k=0, un dia despues. Si
la mejora tambien aparece en k=24, entonces no es "dejar que se descargue la extension",
es deriva o estacionalidad horaria.
"""
import glob
import numpy as np
import pandas as pd
import alertas as al

DEMORAS = (1, 2, 4, 8, 12, 24)

A = al.cargar(sorted(glob.glob("../bt_rk_*.json")))
panel = al.bajar(A, 48 + 24 + 24, verbose=False)
T = al.tabla_alertas(A, panel, 8, 8, 48, verbose=False)
F = al.features_alertas(A, T, panel, verbose=False)
a = A.loc[T["aid"].to_numpy()].reset_index(drop=True)
a.index = T.index
esc = (F["atr_24"] * 100).replace(0, np.nan)


def ret_demorado(k, costo=0.20):
    out = np.full(len(T), np.nan)
    for i, r in enumerate(T.itertuples()):
        df = panel.get(r.sym)
        if df is None:
            continue
        t = df["t"].to_numpy(); c = df["c"].to_numpy()
        j0 = int(np.searchsorted(t, r.t, side="left")) + k
        j1 = j0 + 24
        if j1 >= len(t):
            continue
        out[i] = (c[j1] / c[j0] - 1) * 100 - costo
    return pd.Series(out, index=T.index)


def p_pareado(dif, semana, reps=2000, seed=0, n_min=5):
    """Bootstrap por semana de la diferencia pareada. H0: mediana de la dif <= 0."""
    wk = np.array([g.median() for _, g in dif.dropna().groupby(semana) if len(g) >= n_min])
    k = len(wk)
    if k < 8:
        return 1.0, k, np.nan
    rng = np.random.default_rng(seed)
    m = np.array([rng.choice(wk, k, replace=True).mean() for _ in range(reps)])
    return float((m <= 0).mean()), k, float((wk > 0).mean())


r0 = ret_demorado(0) / esc
print("=" * 100)
print("DEMORA DE ENTRADA — diferencia PAREADA contra entrar ya (unidades de atr_24)")
print(f"linea base k=0: mediana {r0.median():+.3f}")
print("=" * 100)
print(f"{'demora':>7s} {'n par':>7s} {'med k':>8s} {'dif med':>9s} {'sem>0':>7s} {'sems':>5s} {'p':>8s}  ")
rr = {}
for k in DEMORAS:
    rk = ret_demorado(k) / esc
    rr[k] = rk
    dif = (rk - r0).dropna()
    p, ns, so = p_pareado(dif, T["semana"].loc[dif.index])
    tag = "  <-- CONTROL misma hora del dia" if k == 24 else ""
    print(f"{k:6d}h {len(dif):7,d} {rk.median():+8.3f} {dif.median():+9.3f} "
          f"{100*so:6.0f}% {ns:5d} {p:8.4f}{tag}")

print("\n" + "=" * 100)
print("POR TIPO — diferencia pareada contra k=0")
print("=" * 100)
print(f"{'tipo':12s} {'n':>6s} " + " ".join(f"{k:>7d}h" for k in DEMORAS))
for tp in sorted(a["signal_type"].dropna().unique()):
    m = a["signal_type"] == tp
    fila = []
    for k in DEMORAS:
        d = (rr[k] - r0)[m].dropna()
        fila.append(d.median() if len(d) > 30 else np.nan)
    print(f"{tp:12s} {int(m.sum()):6,d} " + " ".join(f"{v:+8.3f}" for v in fila))
