"""
GRAFICOS — patrones de GRAFICO (no de vela) como mascaras booleanas.

La decima familia, la unica que el repo nunca midio. Un patron de vela es una conjuncion
sobre 2-3 barras: una funcion local del OHLC. Un patron de grafico es ESTRUCTURA sobre
decenas de barras: hay que detectar pivotes, comparar alturas y decidir tolerancias.

    py -3.13 graficos.py     # self-test: tasas de disparo sobre el panel 1d

ESTADO: este archivo existe para la COMPUERTA DE POTENCIA (corrida 11), donde lo unico
que importa es CADA CUANTO dispara cada patron, no que efecto tiene. Los umbrales de
abajo son un punto de partida razonable, NO estan preregistrados como canonicos, y si
alguna vez se estima un efecto con ellos hay que fijarlos antes en un preregistro y no
tocarlos despues. Ver §2.1 de HANDOFF_TRES.md.

SIN MIRAR EL FUTURO, y es lo unico delicado del archivo. Un pivote centrado en la barra i
recien se CONFIRMA en la barra i+K: hasta entonces no se sabe si el maximo local aguanta.
Por eso todo patron solo usa pivotes con indice <= j-K cuando decide en la barra j, y la
mascara se marca en la barra de RUPTURA, que es donde se podria entrar de verdad.
"""
import numpy as np
import pandas as pd

K = 3            # semiventana del pivote: max/min sobre [i-K, i+K]
TOL = 0.03       # dos techos son "iguales" si difieren <= 3%
PROF = 0.03      # el valle entre los dos techos tiene que ser >= 3% mas abajo
MAX_SEP = 60     # barras maximas entre el primer pivote y la ruptura
MIN_SEP = 5      # barras minimas entre los dos pivotes del par
HOMBRO = 0.05    # los dos hombros difieren <= 5%
CUNA_N = 3       # pivotes por lado para triangulo/cuna


def pivotes(h, l, k=K):
    """Indices de maximos y minimos locales centrados. Confirmados recien en i+k."""
    n = len(h)
    ph, pl = [], []
    for i in range(k, n - k):
        v = h[i]
        if v == np.max(h[i - k:i + k + 1]) and v > np.max(h[i - k:i]) - 1e-12:
            ph.append(i)
        v = l[i]
        if v == np.min(l[i - k:i + k + 1]) and v < np.min(l[i - k:i]) + 1e-12:
            pl.append(i)
    return np.array(ph, dtype=int), np.array(pl, dtype=int)


def _rompe_abajo(c, j, nivel):
    return c[j] < nivel <= c[j - 1]


def _rompe_arriba(c, j, nivel):
    return c[j] > nivel >= c[j - 1]


NOMBRES = ("doble_techo", "doble_piso", "hch", "hch_inv", "triangulo")


def patrones(df, k=K):
    """{nombre: mascara booleana}, marcada en la barra de RUPTURA.

    Cinco patrones clasicos, cada uno con su espejo donde corresponde:
    doble techo / doble piso, hombro-cabeza-hombro y su inverso, y triangulo.
    """
    h = df["h"].to_numpy(float)
    l = df["l"].to_numpy(float)
    c = df["c"].to_numpy(float)
    ph, pl = pivotes(h, l, k)
    return _desde_pivotes(df, h, l, c, ph, pl, k)


def patrones_barajados(df, rng, k=K):
    """EL CONTROL QUE EXIGE EL HANDOFF: los mismos parametros y la misma logica de
    ruptura, pero con los pivotes reemplazados por indices AL AZAR de la misma cantidad.

    Si el patron real no se separa de esta version —con la estructura destruida y todo
    lo demas igual— lo que se detecto es ruido con forma.
    """
    h = df["h"].to_numpy(float)
    l = df["l"].to_numpy(float)
    c = df["c"].to_numpy(float)
    n = len(c)
    ph, pl = pivotes(h, l, k)
    lib = np.arange(k, max(n - k, k + 1))
    def sortear(m):
        if m == 0 or len(lib) == 0:
            return np.array([], dtype=int)
        return np.sort(rng.choice(lib, size=min(m, len(lib)), replace=False))
    return _desde_pivotes(df, h, l, c, sortear(len(ph)), sortear(len(pl)), k)


def _desde_pivotes(df, h, l, c, ph, pl, k):
    n = len(c)
    out = {nom: np.zeros(n, dtype=bool) for nom in NOMBRES}
    if len(ph) < 2 or len(pl) < 2:
        return {kk: pd.Series(v, index=df.index) for kk, v in out.items()}

    # GOTCHA de velocidad: filtrar los pivotes con ph[ph <= tope] en cada barra es
    # O(pivotes) por barra, o sea cuadratico, y a 1h son 35.000 barras por simbolo.
    # searchsorted lo baja a O(log n) y da EXACTAMENTE lo mismo (verificado a 1d).
    for j in range(k + 1, n):
        tope = j - k                      # solo pivotes ya confirmados en j
        iH = int(np.searchsorted(ph, tope, side="right"))
        iL = int(np.searchsorted(pl, tope, side="right"))

        # --- DOBLE TECHO: dos maximos parecidos, valle en el medio, rompe el valle ---
        if iH >= 2 and iL >= 1:
            a, b = ph[iH - 2], ph[iH - 1]
            if MIN_SEP <= b - a and j - a <= MAX_SEP and abs(h[b] - h[a]) / h[a] <= TOL:
                lo = int(np.searchsorted(pl, a, side="right"))
                hi = int(np.searchsorted(pl, b, side="left"))
                if hi > lo:
                    v = l[pl[lo:hi]].min()
                    if (min(h[a], h[b]) - v) / v >= PROF and _rompe_abajo(c, j, v):
                        out["doble_techo"][j] = True

        # --- DOBLE PISO: el espejo ---
        if iL >= 2 and iH >= 1:
            a, b = pl[iL - 2], pl[iL - 1]
            if MIN_SEP <= b - a and j - a <= MAX_SEP and abs(l[b] - l[a]) / l[a] <= TOL:
                lo = int(np.searchsorted(ph, a, side="right"))
                hi = int(np.searchsorted(ph, b, side="left"))
                if hi > lo:
                    v = h[ph[lo:hi]].max()
                    if (v - max(l[a], l[b])) / v >= PROF and _rompe_arriba(c, j, v):
                        out["doble_piso"][j] = True

        # --- HOMBRO-CABEZA-HOMBRO: 3 maximos, el del medio mas alto, rompe el cuello ---
        if iH >= 3 and iL >= 2:
            a, b, d = ph[iH - 3], ph[iH - 2], ph[iH - 1]
            if (j - a <= MAX_SEP and h[b] > h[a] and h[b] > h[d]
                    and abs(h[d] - h[a]) / h[a] <= HOMBRO):
                p1 = int(np.searchsorted(pl, a, side="right"))
                p2 = int(np.searchsorted(pl, b, side="left"))
                p3 = int(np.searchsorted(pl, b, side="right"))
                p4 = int(np.searchsorted(pl, d, side="left"))
                if p2 > p1 and p4 > p3:
                    cuello = min(l[pl[p1:p2]].min(), l[pl[p3:p4]].min())
                    if _rompe_abajo(c, j, cuello):
                        out["hch"][j] = True

        # --- HCH INVERSO: el espejo ---
        if iL >= 3 and iH >= 2:
            a, b, d = pl[iL - 3], pl[iL - 2], pl[iL - 1]
            if (j - a <= MAX_SEP and l[b] < l[a] and l[b] < l[d]
                    and abs(l[d] - l[a]) / l[a] <= HOMBRO):
                p1 = int(np.searchsorted(ph, a, side="right"))
                p2 = int(np.searchsorted(ph, b, side="left"))
                p3 = int(np.searchsorted(ph, b, side="right"))
                p4 = int(np.searchsorted(ph, d, side="left"))
                if p2 > p1 and p4 > p3:
                    cuello = max(h[ph[p1:p2]].max(), h[ph[p3:p4]].max())
                    if _rompe_arriba(c, j, cuello):
                        out["hch_inv"][j] = True

        # --- TRIANGULO: maximos que bajan y minimos que suben, rompe cualquier lado ---
        if iH >= CUNA_N and iL >= CUNA_N:
            hh, ll = ph[iH - CUNA_N:iH], pl[iL - CUNA_N:iL]
            if (j - min(hh[0], ll[0]) <= MAX_SEP
                    and np.all(np.diff(h[hh]) < 0) and np.all(np.diff(l[ll]) > 0)):
                if _rompe_abajo(c, j, l[ll].max()) or _rompe_arriba(c, j, h[hh].min()):
                    out["triangulo"][j] = True

    return {kk: pd.Series(v, index=df.index) for kk, v in out.items()}


# direccion declarada ANTES de medir, como exige el handoff: la que el patron afirma
DIRECCION = {"doble_techo": "corto", "doble_piso": "largo",
             "hch": "corto", "hch_inv": "largo", "triangulo": None}


if __name__ == "__main__":
    import json
    import os
    from correr_velas import FUERA, INICIO, FIN
    from klines import CACHE, load_panel

    with open(os.path.join(CACHE, "universo_base200.json"), encoding="utf-8") as f:
        syms = [s for s in json.load(f) if s not in FUERA]
    panel = load_panel(INICIO, FIN, tf="1d", full=True, workers=12, syms=syms,
                       min_bars=400)
    tot = {}
    barras = 0
    for sym, df in panel.items():
        barras += len(df)
        for kk, m in patrones(df).items():
            tot[kk] = tot.get(kk, 0) + int(m.sum())
    print(f"\n{len(panel)} pares, {barras:,} barras diarias")
    print(f"{'patron':<14}{'disparos':>10}{'tasa':>10}")
    for kk, v in sorted(tot.items(), key=lambda x: -x[1]):
        print(f"{kk:<14}{v:>10,}{v/barras:>10.3%}")
    print(f"{'TODOS':<14}{sum(tot.values()):>10,}{sum(tot.values())/barras:>10.3%}")
