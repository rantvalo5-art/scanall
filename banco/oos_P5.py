"""Reserva OOS 2026-08-01 -> 2026-08-26. Regla 4 de PREREGISTRO_PREBREAK.md."""
import numpy as np
import pandas as pd
import alertas as al

OFFS = [-24, -12, 0, 12, 24, 48]
CORTE = pd.Timestamp("2026-08-01", tz="UTC")

def analiza(path, etiqueta):
    A = al.cargar([path])
    A = A[A["dt"] >= CORTE].reset_index(drop=True)
    panel = al.bajar(A, 48 + 24, verbose=False)
    T = al.tabla_alertas(A, panel, 8, 8, 48, verbose=False)
    F = al.features_alertas(A, T, panel, verbose=False)
    a = A.loc[T["aid"].to_numpy()].reset_index(drop=True); a.index = T.index
    esc = (F["atr_24"] * 100).replace(0, np.nan)
    atr = {}
    for s, df in panel.items():
        h, l, c = df["h"].to_numpy(), df["l"].to_numpy(), df["c"].to_numpy()
        atr[s] = (pd.Series(h - l).rolling(24).mean().to_numpy() / c) * 100

    print(f"\n{'='*94}\n{etiqueta}  |  {len(T):,} alertas con horizonte completo  |  "
          f"semanas {T['semana'].nunique()}\n{'='*94}")
    for costo in (0.20, 0.50):
        ret = ((a["price_24h"].astype(float) / a["entry_price"].astype(float) - 1) * 100 - costo) / esc
        base_med = float(ret.dropna().median())
        print(f"\n  costo {costo:.2f}%  — linea base (todas las alertas) mediana {base_med:+.3f} ATR")
        print(f"    {'tipo':11s} {'n':>5s} {'mediana':>9s} {'vs base':>9s} {'vs dardo':>9s} {'sem>0':>7s}")
        for tp in sorted(a["signal_type"].dropna().unique()):
            m = (a["signal_type"] == tp) & ret.notna()
            if m.sum() < 30:
                continue
            r = ret[m]
            peso = a.loc[m, "symbol"].value_counts()
            med_sym = ret.dropna().groupby(a.loc[ret.notna(), "symbol"]).median()
            com = peso.index.intersection(med_sym.index)
            par = (med_sym[com] * peso[com]).sum() / peso[com].sum() if len(com) else np.nan
            wk = [(ret[m & (T["semana"] == w)].median() - ret[(T["semana"] == w)].median())
                  for w in sorted(T["semana"].unique())]
            wk = [x for x in wk if np.isfinite(x)]
            flag = "  <--" if tp == "PREBREAK" else ""
            print(f"    {tp:11s} {int(m.sum()):5d} {r.median():+9.3f} "
                  f"{r.median()-base_med:+9.3f} {r.median()-par:+9.3f} "
                  f"{100*np.mean([x>0 for x in wk]):6.0f}%{flag}")

    # contaminacion
    print(f"\n  CAMINO (mediana ATR, 0 = entrada) — compuerta: subida previa <= 0")
    print(f"    {'tipo':11s} " + " ".join(f"{o:>7d}h" for o in OFFS) + "   subida previa")
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
        flag = ("  <-- " + ("CONTAMINADA" if sub > 0 else "limpia")) if tp == "PREBREAK" else ""
        print(f"    {tp:11s} " + " ".join(f"{x:+8.2f}" for x in v) + f"   {sub:+.2f}{flag}")

analiza("../bt_P5_OOS.json", "P5 — RESERVA OOS 2026-08-01 -> 2026-08-26")
analiza("../bt_BASE_OOS.json", "BASE — RESERVA OOS (referencia)")
