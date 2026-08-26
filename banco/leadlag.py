"""
LEAD-LAG ENTRE ALTS - item 4.1 del HANDOFF_PENDIENTE.

La pregunta: unas monedas se mueven sistematicamente ANTES que otras?

Por que es nueva. Todo lo que probo este repo es serie de tiempo POR SIMBOLO
(roc, dd, atr, obv...) o transversal CONTEMPORANEO (rank de volumen hoy). El
DESFASE nunca se miro. El candidato obvio -BTC lidera a las alts- ya murio
indirectamente (beta_btc e idio_168 en lote_ancho.py), asi que lo que queda sin
mirar es lead-lag ENTRE alts.

Como se esquiva la matriz de 200x200. Probar cada par (i,j) x cada lag es
look-elsewhere puro: 40.000 pares x 5 lags = 200.000 hipotesis, y encima ninguna
seria operable (un par que lidera a otro en la muestra no lo lidera fuera). Aca
el lead-lag se mide por GRUPO: se ordena el universo por una caracteristica
(liquidez, volatilidad, iliquidez de Amihud, flujo taker), se parte en tercios y
se pregunta si el retorno reciente de un tercio predice el resultado de una
moneda DISTINTA. Es la formulacion clasica de Lo-MacKinlay y deja el lote en
~200 hipotesis, no 200.000.

SIN LOOKAHEAD. La entrada de primer_toque._entradas es e = c[i] (cierre de la
vela i) y el resultado se mide desde i+1. O sea que al entrar se conoce TODO
hasta el cierre de i inclusive. Todas las features de aca usan rolling/shift que
terminan en i inclusive - nunca i+1.

    py -3.13 -u leadlag.py --nula 5     # calibrar look-elsewhere y salir
    py -3.13 -u leadlag.py              # la corrida real
"""
import argparse

import numpy as np
import pandas as pd

from klines import load_panel
from lote import lote, Q_FDR
from primer_toque import tabla

VENT = 168          # ventana de la caracteristica (1 semana de horas)
LAGS = (1, 6, 12, 24)
CHARS = ("qv", "vol", "amihud", "tk")


def _matrices(panel):
    """Panel {sym: df} -> matrices (t x sym) alineadas por timestamp."""
    def M(col):
        return pd.DataFrame({s: df.set_index("t")[col] for s, df in panel.items()}).sort_index()
    C, QV, V, VB = M("c"), M("qv"), M("v"), M("vb")
    LR = np.log(C).diff()
    return C, QV, V, VB, LR


def _caracteristicas(C, QV, V, VB, LR):
    """Caracteristica por (t, sym) sobre las VENT horas que TERMINAN en t.

    Todas terminan en t inclusive, que es lo que se conoce al entrar (cierre de
    la vela t). Ninguna mira adelante.
    """
    ch = {}
    ch["qv"] = QV.rolling(VENT, min_periods=VENT // 2).mean()
    ch["vol"] = LR.rolling(VENT, min_periods=VENT // 2).std()
    # Amihud: |retorno| por unidad de volumen en USD. Mide iliquidez.
    ch["amihud"] = (LR.abs() / QV.replace(0, np.nan)).rolling(
        VENT, min_periods=VENT // 2).mean()
    # fraccion del volumen que fue taker COMPRADOR: lo mas parecido a order flow
    ch["tk"] = (VB / V.replace(0, np.nan)).rolling(VENT, min_periods=VENT // 2).mean()
    return ch


def features(panel, T, verbose=True):
    """Features de lead-lag alineadas a las filas de T.

    Para cada caracteristica se parte el universo en TERCIOS en cada hora (ranking
    transversal, sin mirar adelante). Para cada lag k se calcula el retorno
    acumulado de las ultimas k horas y se promedia DENTRO de cada tercio,
    EXCLUYENDO a la propia moneda (leave-one-out). Sin ese leave-one-out la
    feature contendria el retorno reciente de la moneda misma, que ya esta medido
    (roc_*) y contaminaria todo.
    """
    C, QV, V, VB, LR = _matrices(panel)
    ch = _caracteristicas(C, QV, V, VB, LR)
    if verbose:
        print(f"  matrices {C.shape[0]} horas x {C.shape[1]} pares", flush=True)

    # retorno acumulado de las ultimas k horas, terminando en t inclusive
    RK = {k: (np.log(C) - np.log(C.shift(k))) for k in LAGS}

    out = {}
    for cname in CHARS:
        X = ch[cname]
        r = X.rank(axis=1, pct=True)            # ranking transversal por hora
        grupos = {"hi": (r >= 2 / 3), "lo": (r <= 1 / 3)}
        for k in LAGS:
            Rk = RK[k]
            Rv = Rk.to_numpy()
            ok = ~np.isnan(Rv)
            Rv0 = np.where(ok, Rv, 0.0)
            medias = {}
            for gname, mask in grupos.items():
                m = mask.to_numpy() & ok
                S = (Rv0 * m).sum(axis=1)        # suma del grupo por hora
                n = m.sum(axis=1)
                # leave-one-out: si la moneda esta en el grupo, se la saca
                num = S[:, None] - Rv0 * m
                den = n[:, None] - m
                with np.errstate(invalid="ignore", divide="ignore"):
                    g = np.where(den > 0, num / np.where(den == 0, 1, den), np.nan)
                medias[gname] = pd.DataFrame(g, index=Rk.index, columns=Rk.columns)
                out[f"ll_{cname}_{gname}_{k}"] = medias[gname]
            # el spread hi-lo es la senal de lead-lag propiamente dicha
            out[f"ll_{cname}_spread_{k}"] = medias["hi"] - medias["lo"]
        if verbose:
            print(f"  {cname}: listo", flush=True)

    idx = pd.MultiIndex.from_arrays([T["sym"].to_numpy(), T["t"].to_numpy()])
    F = {}
    for name, M in out.items():
        st = M.stack(future_stack=True)
        st.index = st.index.set_names(["t", "sym"])
        st = st.reorder_levels(["sym", "t"])
        F[name] = st.reindex(idx).to_numpy()
    F = pd.DataFrame(F, index=T.index)
    if verbose:
        print(f"  F: {F.shape[1]} features x {len(F)} filas "
              f"({F.notna().all(axis=1).mean()*100:.1f}% completas)", flush=True)
    return F


def atr_control(panel, T):
    """Volatilidad reciente por entrada - el control de la version condicional."""
    a = {}
    for s, df in panel.items():
        d = df.set_index("t")
        a[s] = ((d["h"] - d["l"]) / d["c"]).rolling(24, min_periods=12).mean()
    A = pd.DataFrame(a).sort_index()
    st = A.stack(future_stack=True)
    st.index = st.index.set_names(["t", "sym"])
    st = st.reorder_levels(["sym", "t"])
    idx = pd.MultiIndex.from_arrays([T["sym"].to_numpy(), T["t"].to_numpy()])
    return pd.Series(st.reindex(idx).to_numpy(), index=T.index)


def hipotesis(F, atr, q=0.20, n_min=2000):
    """Colas por quintil + las mismas condicionadas al quintil de volatilidad.

    Misma forma que micro.py: la version condicional tiene la misma mezcla de
    volatilidad que la linea base, asi que no puede ganar solo por ser volatil.
    """
    H = {}
    vq = pd.qcut(atr.rank(method="first"), 5, labels=False, duplicates="drop")
    for c in F.columns:
        s = F[c]
        if s.notna().sum() < n_min or s.nunique() < 5:
            continue
        lo, hi = s.quantile([q, 1 - q])
        if not np.isfinite(lo) or not np.isfinite(hi) or lo == hi:
            continue
        H[f"{c} alto"] = s >= hi
        H[f"{c} bajo"] = s <= lo
        r = s.groupby(vq).rank(pct=True)
        H[f"{c} alto | vol"] = r >= 1 - q
        H[f"{c} bajo | vol"] = r <= q
    return H


def nula(T, F, atr, q, reps=5, seed=0):
    """Look-elsewhere por DESPLAZAMIENTO CIRCULAR (no permutando filas).

    Barajar destruiria la autocorrelacion de features y resultados y haria la nula
    demasiado facil. Aca se desplaza circularmente la matriz de features dentro de
    cada simbolo: cada feature conserva su estructura temporal y cada moneda su
    secuencia de resultados; lo unico que se rompe es la alineacion entre las dos,
    que es exactamente H0.
    """
    rng = np.random.default_rng(seed)
    bloques = [np.flatnonzero((T["sym"] == s).to_numpy()) for s in T["sym"].unique()]
    out = []
    for r in range(reps):
        idx = np.arange(len(T))
        for ii in bloques:
            if len(ii) > 1:
                idx[ii] = np.roll(ii, int(rng.integers(1, len(ii))))
        Fp = F.iloc[idx].set_axis(F.index)
        ap_ = atr.iloc[idx].set_axis(atr.index)
        H = hipotesis(Fp, ap_)
        n1 = int((lote(T, H, q=q, mostrar=False).veredicto == "SOBREVIVE").sum())
        Tc = T.copy()
        Tc["res"] = -Tc["res"]
        Tc.attrs.update(T.attrs)
        n2 = int((lote(Tc, H, q=q, mostrar=False).veredicto == "SOBREVIVE").sum())
        out.append(n1 + n2)
        print(f"  nula {r+1}/{reps}: {n1} largo + {n2} corto = {n1+n2} "
              f"sobrevivientes de {2*len(H)}", flush=True)
    a = np.array(out)
    print(f"\n  NULA: media {a.mean():.2f}  max {a.max()}  "
          f"-> el resultado real tiene que superar {a.max()}")
    return a


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--pares", type=int, default=200)
    ap.add_argument("--q", type=float, default=Q_FDR)
    ap.add_argument("--nula", type=int, default=0,
                    help="calibra el look-elsewhere con N desplazamientos y sale")
    ap.add_argument("--csv", default="leadlag.csv")
    a = ap.parse_args()

    P = load_panel("2025-08-01", "2026-08-01", n=a.pares, tf="1h",
                   pin="base200", full=True)
    T = tabla(P, target=8, stop=8, horizonte_d=30, verbose=False)
    print(f"\nT: {len(T):,} entradas / {int(T.resuelto.sum()):,} resueltas / "
          f"{T.sym.nunique()} simbolos / {T.semana.nunique()} semanas")
    F = features(P, T)
    atr = atr_control(P, T)

    if a.nula:
        print(f"\nCALIBRACION DE LA NULA ({a.nula} desplazamientos circulares)\n")
        nula(T, F, atr, a.q, reps=a.nula)
        return

    H = hipotesis(F, atr)
    print(f"\n{len(H)} hipotesis x 2 direcciones = {2*len(H)} en total\n")
    D1 = lote(T, H, q=a.q)
    D1["dir"] = "largo"
    Tc = T.copy()
    Tc["res"] = -Tc["res"]
    Tc.attrs.update(T.attrs)
    print("\n--- MISMAS HIPOTESIS, DIRECCION CORTA ---\n")
    D2 = lote(Tc, H, q=a.q)
    D2["dir"] = "corto"
    D = pd.concat([D1, D2], ignore_index=True)
    D.to_csv(a.csv, index=False)
    v = (D.veredicto == "SOBREVIVE").sum()
    print(f"\n=== TOTAL: {v} sobrevivientes de {len(D)} -> {a.csv}")


if __name__ == "__main__":
    main()
