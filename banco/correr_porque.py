"""POR QUE pierde una alerta — descomposicion, no veredicto.

Dos cosas:
  1) En que componente se va la plata: mercado / eleccion de moneda / momento / costo.
  2) Que hace el precio ALREDEDOR de la alerta (estudio de evento de -24h a +48h).

Todo en unidades de atr_24 del propio simbolo, porque medir en % mide volatilidad.
"""
import glob
import numpy as np
import pandas as pd
import alertas as al

A = al.cargar(sorted(glob.glob("../bt_rk_*.json")))
panel = al.bajar(A, 48 + 24, verbose=False)
T = al.tabla_alertas(A, panel, 8, 8, 48, verbose=False)
F = al.features_alertas(A, T, panel, verbose=False)
a = A.loc[T["aid"].to_numpy()].reset_index(drop=True)
a.index = T.index
esc = (F["atr_24"] * 100).replace(0, np.nan)

# ---------- 1. dardos: mismo simbolo, momento al azar dentro de la ventana ----------
rng = np.random.default_rng(0)
t_min, t_max = int(T["t"].min()), int(T["t"].max())
syms_alerta = set(T["sym"].unique())

def ret_24h(sym, j0, df):
    c = df["c"].to_numpy()
    return (c[j0 + 24] / c[j0] - 1) * 100 if j0 + 24 < len(c) else np.nan

# atr por simbolo/hora para normalizar los dardos igual que las alertas
atr_por_sym = {}
for sym, df in panel.items():
    h, l, c = df["h"].to_numpy(), df["l"].to_numpy(), df["c"].to_numpy()
    atr_por_sym[sym] = (pd.Series(h - l).rolling(24).mean().to_numpy() / c) * 100

def dardos(symbols, n=40000):
    out = []
    ss = list(symbols)
    for _ in range(n):
        sym = ss[rng.integers(len(ss))]
        df = panel.get(sym)
        if df is None:
            continue
        t = df["t"].to_numpy()
        lo = int(np.searchsorted(t, t_min)); hi = int(np.searchsorted(t, t_max))
        if hi - lo < 50:
            continue
        j0 = int(rng.integers(lo, hi))
        r = ret_24h(sym, j0, df)
        s = atr_por_sym[sym][j0]
        if np.isfinite(r) and np.isfinite(s) and s > 0:
            out.append(r / s)
    return pd.Series(out)

d_pareado = dardos(syms_alerta)                     # solo las monedas que alertaron
d_universo = dardos(panel.keys())                   # todo el universo

ret_alerta = (a["price_24h"].astype(float) / a["entry_price"].astype(float) - 1) * 100 / esc

m_uni = float(d_universo.median())
m_par = float(d_pareado.median())
m_ale = float(ret_alerta.median())

print("=" * 84)
print("DONDE SE VA LA PLATA — mediana del retorno a 24h en unidades de ATR")
print("=" * 84)
print(f"  universo entero, momento al azar      {m_uni:+7.3f}   <- beta/deriva del mercado")
print(f"  solo monedas que alertaron, al azar   {m_par:+7.3f}")
print(f"  en el momento de la alerta            {m_ale:+7.3f}")
print()
print(f"  [1] mercado (deriva de fondo)         {m_uni:+7.3f}")
print(f"  [2] eleccion de moneda                {m_par - m_uni:+7.3f}")
print(f"  [3] eleccion del MOMENTO              {m_ale - m_par:+7.3f}")
for costo, lab in ((0.20, "supuesto"), (0.50, "medido")):
    c_atr = costo / float(esc.median())
    print(f"  [4] costo {costo:.2f}% ({lab})            {-c_atr:+7.3f}")
print(f"  {'-'*54}")
print(f"  total con costo 0,20%                 {m_ale - 0.20/float(esc.median()):+7.3f}")

# ---------- 2. estudio de evento ----------
print("\n" + "=" * 84)
print("QUE HACE EL PRECIO ALREDEDOR DE LA ALERTA (mediana, ATR, 0 = momento de alerta)")
print("=" * 84)
offs = list(range(-24, 49, 4))
def camino(mask):
    filas = []
    for r in T[mask].itertuples():
        df = panel.get(r.sym)
        if df is None: continue
        t = df["t"].to_numpy(); c = df["c"].to_numpy()
        j0 = int(np.searchsorted(t, r.t, side="left"))
        if j0 < 24 or j0 + 48 >= len(c): continue
        s = atr_por_sym[r.sym][j0]
        if not np.isfinite(s) or s <= 0: continue
        filas.append([(c[j0 + o] / c[j0] - 1) * 100 / s for o in offs])
    return np.nanmedian(np.array(filas), axis=0) if filas else None

print(f"{'tipo':11s} " + " ".join(f"{o:>6d}" for o in offs))
todo = camino(pd.Series(True, index=T.index))
print(f"{'TODAS':11s} " + " ".join(f"{v:+6.2f}" for v in todo))
for tp in sorted(a["signal_type"].dropna().unique()):
    v = camino(a["signal_type"] == tp)
    if v is not None:
        print(f"{tp:11s} " + " ".join(f"{x:+6.2f}" for x in v))

print("\n  (columna -24 = 24h ANTES de la alerta; 0 = entrada; +48 = dos dias despues)")
i0 = offs.index(0)
print(f"\n  subida previa mediana (de -24h a 0):  {todo[i0]-todo[0]:+.2f} ATR")
print(f"  recorrido posterior (de 0 a +24h):    {todo[offs.index(24)]-todo[i0]:+.2f} ATR")
print(f"  recorrido posterior (de 0 a +48h):    {todo[-1]-todo[i0]:+.2f} ATR")
