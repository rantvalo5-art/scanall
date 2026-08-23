"""
Chequeos de `panel_tt.py`. Sin red: arma klines y metrics sinteticas.

Lo que se verifica es exactamente donde el repo ya se autoengano antes:
  - el retorno forward sale del precio correcto y descuenta costos una sola vez
  - la observacion de tt_pos es ANTERIOR al instante de entrada (trampa 2 del
    handoff: el valor horario es el ULTIMO dato de la hora, asi que la hora que
    CONTIENE la entrada trae observaciones posteriores a ella)
  - un dato viejo no se arrastra: si falta la hora previa, la fila se descarta
  - el dardo pareado sale del MISMO simbolo, dentro de la ventana y saltando la
    banda de exclusion

    py -3.13 test_panel.py
"""
import sys
import types

import numpy as np
import pandas as pd

import panel_tt as PT

DAY = PT.DAY_MS
H = PT.MS_H
T0 = int(pd.Timestamp("2024-01-01", tz="UTC").value // 10**6)

fallos = []


def ok(cond, nombre, detalle=""):
    print(f"  {'PASA' if cond else 'FALLA'}  {nombre}" + (f"   {detalle}" if detalle else ""))
    if not cond:
        fallos.append(nombre)


def args_falsos(**kw):
    a = types.SimpleNamespace(
        stride=1, horizonte_d=7, desde="2024-01-01", hasta="2024-12-31",
        darts=10, dart_window=30, dart_excl=7, frescura_h=2, seed=7,
        rehacer=True, umbral=1.28, limpio_desde="2023-01-01", out="_t.json")
    for k, v in kw.items():
        setattr(a, k, v)
    return a


def klines(n, precio):
    """n velas diarias desde T0. `precio(i)` da el open."""
    t = np.arange(n, dtype="int64") * DAY + T0
    o = np.array([float(precio(i)) for i in range(n)])
    return pd.DataFrame({"open_time": t, "open": o, "high": o, "low": o, "close": o})


def metrics(n, valor, horas=range(24)):
    """n dias de metrics horarias; `valor(t_ms)` da tt_pos."""
    ts = [T0 + d * DAY + h * H for d in range(n) for h in horas]
    d = {"t": np.array(ts, dtype="int64")}
    for v in PT.VARS:
        d[v] = np.array([1.0] * len(ts), dtype="float32")
    d["tt_pos"] = np.array([float(valor(t)) for t in ts], dtype="float32")
    return pd.DataFrame(d)


def corre(m, k, **kw):
    """construir_panel() con un solo simbolo inyectado, sin tocar disco."""
    a = args_falsos(**kw)
    guardar_carga, guardar_uni, guardar_pk = PT._carga_simbolo, PT.UNIVERSO, PT.PCACHE
    PT._carga_simbolo = lambda sym: (m, k)
    import json as _j
    from pathlib import Path as _P

    class _U:
        @staticmethod
        def read_text(encoding=None):
            return _j.dumps({"simbolos": {"XXXUSDT": ["2024-01-01"]}})
    PT.UNIVERSO = _U

    tmp = _P("_tpanel")
    tmp.mkdir(exist_ok=True)
    PT.PCACHE = tmp
    try:
        for f in tmp.glob("panel_*.pkl"):
            f.unlink()
        return PT.construir_panel(a)
    finally:
        PT._carga_simbolo, PT.UNIVERSO, PT.PCACHE = guardar_carga, guardar_uni, guardar_pk


print("\n── retorno forward ──")
# precio que sube 1% por dia: el forward de 7d es 1.01^7 - 1 - COSTS
k = klines(60, lambda i: 100 * (1.01 ** i))
m = metrics(60, lambda t: 1.0)
P = corre(m, k)
esperado = 1.01 ** 7 - 1 - PT.COSTS
ok(len(P) > 30, "hay filas", f"n={len(P)}")
ok(np.allclose(P.r.values, esperado, atol=1e-9),
   "r = open(T+7d)/open(T) - 1 - COSTS", f"{P.r.iloc[0]:.6f} vs {esperado:.6f}")
ok(abs(P.r.iloc[0] - (1.01 ** 7 - 1)) > PT.COSTS / 2,
   "los costos se descuentan (y una sola vez)")

# precio plano: retorno = -COSTS exacto
P2 = corre(metrics(60, lambda t: 1.0), klines(60, lambda i: 50.0))
ok(np.allclose(P2.r.values, -PT.COSTS), "precio plano -> r = -COSTS",
   f"{P2.r.iloc[0]:.6f}")

# la entrada es la APERTURA del dia de grilla, no el cierre del anterior.
# Ojo: la grilla NO puede arrancar el primer dia de la serie de metrics, porque
# ahi no existe hora previa que mirar; por eso se chequea contra el propio t.
esperados = k.set_index("open_time").open
ok(np.allclose(P.px.values, esperados.loc[P.t.values].values),
   "px de entrada = open del dia T", f"primera fila {P.fecha.iloc[0]}")
ok(P.t.min() > T0, "la grilla saltea el primer dia (sin hora previa que mirar)")

print("\n── alineacion anti-lookahead (trampa 2 del handoff) ──")
# tt_pos vale 9 SOLO en la hora que contiene la entrada (00:00 del dia 10) y 1 antes.
# Si el panel toma esa hora, esta mirando el futuro.
t_ent = T0 + 10 * DAY
m3 = metrics(60, lambda t: 9.0 if t == t_ent else 1.0)
P3 = corre(m3, klines(60, lambda i: 100.0))
f = P3[P3.t == t_ent]
ok(len(f) == 1 and float(f.tt_pos.iloc[0]) == 1.0,
   "usa la hora ANTERIOR, no la que contiene la entrada",
   f"tt_pos={float(f.tt_pos.iloc[0]) if len(f) else 'sin fila'}")
# y la hora 23:00 del dia previo SI se usa (es lo ultimo conocido)
m4 = metrics(60, lambda t: 7.0 if t == t_ent - H else 1.0)
P4 = corre(m4, klines(60, lambda i: 100.0))
f4 = P4[P4.t == t_ent]
ok(len(f4) == 1 and float(f4.tt_pos.iloc[0]) == 7.0,
   "si usa la hora 23:00 del dia previo", f"tt_pos={float(f4.tt_pos.iloc[0]) if len(f4) else 'sin fila'}")

print("\n── dato viejo: no se arrastra ──")
# metrics solo hasta la hora 12 de cada dia -> a las 00:00 el ultimo dato tiene 12h
m5 = metrics(60, lambda t: 1.0, horas=range(0, 13))
P5 = corre(m5, klines(60, lambda i: 100.0))
ok(len(P5) == 0, "descarta si la ultima hora esta a >2h de la entrada", f"n={len(P5)}")
# con frescura_h=24 la misma serie si entra
P6 = corre(m5, klines(60, lambda i: 100.0), frescura_h=24)
ok(len(P6) > 20, "y con --frescura-h 24 entra", f"n={len(P6)}")

print("\n── dardo pareado ──")
rng = np.random.default_rng(3)
n = 200
fechas = np.arange(n, dtype="int64") * DAY + T0
rets = np.arange(n, dtype="float64")          # r = indice, para poder rastrear
d = PT._dardos(fechas, rets, 5000, rng, 30 * DAY, 7 * DAY)
i = 100
# con 5000 dardos la media debe estar cerca del promedio de los indices elegibles
elig = [j for j in range(n) if 7 <= abs(j - i) <= 30]
ok(abs(d[i] - np.mean(elig)) < 1.0,
   "muestrea uniforme dentro de +-30d saltando +-7d",
   f"{d[i]:.1f} vs esperado {np.mean(elig):.1f}")
# ningun dardo cae dentro de la banda de exclusion
peor = max(abs(PT._dardos(fechas, (np.abs(np.arange(n) - i) < 7).astype("float64"),
                          5000, rng, 30 * DAY, 7 * DAY)[i]) for _ in range(3))
ok(peor == 0.0, "nunca cae dentro de la banda de exclusion", f"peso={peor}")
# los bordes de la serie no explotan
ok(not np.isnan(d[0]) and not np.isnan(d[-1]), "los extremos de la serie tienen dardo")
# una serie mas corta que el minimo queda sin dardo
d2 = PT._dardos(fechas[:6], rets[:6], 10, rng, 30 * DAY, 7 * DAY)
ok(np.isnan(d2).all(), "menos de 4 candidatos -> NaN (la fila se cae despues)")

print("\n── huecos en las klines ──")
# si falta la vela de salida exacta (T+7d), la fila no se inventa
kh = klines(60, lambda i: 100.0)
kh = kh[kh.open_time != T0 + 17 * DAY].reset_index(drop=True)
P7 = corre(metrics(60, lambda t: 1.0), kh)
ok((P7.t == T0 + 10 * DAY).sum() == 0,
   "sin vela exacta de salida, no hay fila (no interpola)")

import shutil
shutil.rmtree("_tpanel", ignore_errors=True)

print(f"\n{'TODO PASA' if not fallos else str(len(fallos)) + ' FALLAS: ' + ', '.join(fallos)}")
sys.exit(1 if fallos else 0)
