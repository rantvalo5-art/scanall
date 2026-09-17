"""Adjudicacion de P5 contra `PREREGISTRO_PREBREAK.md`. 28 semanas."""
import glob
import numpy as np
import pandas as pd
import alertas as al
from gate_mediana import lote_mediana, barrido_nmin

OFFS = [-24, -12, -4, 0, 4, 12, 24, 48]

A = al.cargar(sorted(glob.glob("../bt_P5_*.json")))
panel = al.bajar(A, 48 + 24, verbose=False)
T = al.tabla_alertas(A, panel, 8, 8, 48, verbose=False)
F = al.features_alertas(A, T, panel, verbose=False)
a = A.loc[T["aid"].to_numpy()].reset_index(drop=True); a.index = T.index
esc = (F["atr_24"] * 100).replace(0, np.nan)

atr = {}
for s, df in panel.items():
    h, l, c = df["h"].to_numpy(), df["l"].to_numpy(), df["c"].to_numpy()
    atr[s] = (pd.Series(h - l).rolling(24).mean().to_numpy() / c) * 100

print("=" * 96)
print(f"P5 — 28 semanas | {len(T):,} alertas | " +
      "  ".join(f"{k} {v}" for k, v in a["signal_type"].value_counts().items()))
print("=" * 96)

# ---- compuerta 1 del preregistro: subida previa ----
print("\nCAMINO (mediana, ATR, 0 = entrada) — compuerta: subida previa <= 0 para PREBREAK")
print(f"{'tipo':11s} " + " ".join(f"{o:>7d}h" for o in OFFS) + "   subida previa")
for tp in sorted(a["signal_type"].dropna().unique()):
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
    if len(filas) < 30: continue
    v = np.nanmedian(np.array(filas), axis=0)
    sub = v[OFFS.index(0)] - v[0]
    flag = ""
    if tp == "PREBREAK":
        flag = "  <-- " + ("CONTAMINADA" if sub > 0 else "limpia")
    print(f"{tp:11s} " + " ".join(f"{x:+8.2f}" for x in v) + f"   {sub:+.2f}{flag}")

# ---- compuertas 2 y 3: gates sobre la mediana normalizada ----
H = {f"PREBREAK (P5) n={int((a.signal_type=='PREBREAK').sum())}": a["signal_type"] == "PREBREAK"}
for tp in sorted(a["signal_type"].dropna().unique()):
    if tp != "PREBREAK":
        H[f"{tp} (referencia)"] = a["signal_type"] == tp
for costo in (0.20, 0.50):
    ret = ((a["price_24h"].astype(float) / a["entry_price"].astype(float) - 1) * 100 - costo) / esc
    D = lote_mediana(A, T, H, ret, costo, titulo=f"(P5, costo {costo:.2f}%, normalizado)")
    D.to_csv(f"../P5_gate_c{int(costo*100):03d}.csv", index=False)

# ---- compuerta 5: barrido de n_min ----
ret = ((a["price_24h"].astype(float) / a["entry_price"].astype(float) - 1) * 100 - 0.20) / esc
barrido_nmin(A, T, H, ret, list(H)[0])

# ---- ¿alertas nuevas, o las mismas detectadas antes? ----
B = al.cargar(sorted(glob.glob("../bt_rk_*.json")))
print("\n" + "=" * 96)
print("¿LAS PREBREAK NUEVAS SON OPORTUNIDADES NUEVAS, O LAS MISMAS DETECTADAS ANTES?")
print("=" * 96)
pb = T[(a["signal_type"] == "PREBREAK").to_numpy()]
nuevas = same = 0
adelanto = []
for r in pb.itertuples():
    c = B[(B["symbol"] == r.sym) & (B["t"] - r.t).abs().le(24 * al.HORA)]
    if len(c):
        same += 1
        # la contraparte MAS CERCANA, no la mas temprana: usar .min() sobre el delta
        # con signo elige siempre la de -24h y fabrica un adelanto que no existe.
        d = (c["t"].to_numpy() - r.t) / al.HORA
        adelanto.append(float(d[np.abs(d).argmin()]))
    else:
        nuevas += 1
print(f"  PREBREAK de P5: {len(pb):,}")
print(f"    con una alerta BASE del mismo simbolo en +-24h : {same:,} ({100*same/len(pb):.0f}%)")
print(f"    sin contraparte en base (oportunidad nueva)     : {nuevas:,} ({100*nuevas/len(pb):.0f}%)")
if adelanto:
    print(f"    adelanto mediano sobre la alerta base: {np.median(adelanto):+.1f}h")
