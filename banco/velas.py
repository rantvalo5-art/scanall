"""
VELAS — patrones de velas japonesas clasicos, como MASCARAS booleanas.

Por que existe, y por que no lo cubre `lote_ancho.py`. Aquel midio forma de vela como
features CONTINUAS (`cuerpo = (c-o)/rango`, `mecha_sup`, `mecha_inf`) cortadas por
quintiles: 0 de 86. Un patron clasico es otra forma funcional:

  - es una CONJUNCION de 4-6 desigualdades, no un corte por cuantil de una variable,
  - abarca 2 o 3 velas consecutivas, no una,
  - y se define CON contexto ("envolvente alcista **despues de una baja**").

Un corte por quintil de `cuerpo` no contiene a un envolvente, ni al reves.

Las definiciones estan fijadas en `PREREGISTRO_VELAS.md` §3.3 y **no se tocan**: si un
umbral se ajusta despues de ver un resultado, el experimento no vale. Los numeros de
abajo (2x, 30%, 5%, 90%) son los canonicos, no elegidos midiendo.

No se usa TA-Lib a proposito: no esta instalada en esta maquina, y escribir las 14
definiciones a mano deja el umbral de cada una a la vista y auditable.

    py -3.13 velas.py     # self-test: tasas de disparo sobre BTCUSDT
"""
import numpy as np
import pandas as pd

# umbrales canonicos, declarados en el preregistro ANTES de correr
MECHA_X = 2.0        # la mecha larga tiene que ser >= 2x el cuerpo
CUERPO_CHICO = 0.30  # martillo: cuerpo <= 30% del rango
DOJI_MAX = 0.05      # doji: cuerpo <= 5% del rango
MARUBOZU = 0.90      # marubozu: cuerpo >= 90% del rango
CTX = 3              # barras de tendencia previa para el brazo "con contexto"


def _piezas(df):
    o = df["o"].to_numpy(float)
    h = df["h"].to_numpy(float)
    l = df["l"].to_numpy(float)
    c = df["c"].to_numpy(float)
    rng = np.where(h - l > 0, h - l, np.nan)
    cuerpo = np.abs(c - o)
    alc = c > o
    sup = h - np.maximum(o, c)
    inf = np.minimum(o, c) - l
    return o, h, l, c, rng, cuerpo, alc, sup, inf


def _prev(a, n=1):
    """Desplaza `n` barras hacia adelante: `_prev(x)[i]` es `x[i-n]`. Solo pasado."""
    out = np.full_like(a, np.nan, dtype=float)
    if n < len(a):
        out[n:] = a[:-n]
    return out


def patrones(df):
    """{nombre: mascara booleana} con los 14 patrones del preregistro.

    La mascara es True en la barra donde el patron SE COMPLETA, o sea que la decision se
    toma al cierre de esa barra y la entrada es a ese precio. Todo mira al pasado.
    """
    o, h, l, c, rng, cuerpo, alc, sup, inf = _piezas(df)
    o1, c1 = _prev(o), _prev(c)
    cue1 = _prev(cuerpo)
    alc1 = _prev(alc.astype(float)) > 0.5
    o2, c2 = _prev(o, 2), _prev(c, 2)
    cue2 = _prev(cuerpo, 2)
    alc2 = _prev(alc.astype(float), 2) > 0.5
    rng1 = _prev(rng)

    P = {}
    with np.errstate(invalid="ignore"):
        # --- una vela --------------------------------------------------------
        chico = cuerpo <= CUERPO_CHICO * rng
        P["martillo"] = chico & (inf >= MECHA_X * cuerpo) & (sup <= cuerpo)
        P["estrella_fugaz"] = chico & (sup >= MECHA_X * cuerpo) & (inf <= cuerpo)
        P["doji"] = cuerpo <= DOJI_MAX * rng
        P["marubozu_alc"] = (cuerpo >= MARUBOZU * rng) & alc
        P["marubozu_baj"] = (cuerpo >= MARUBOZU * rng) & ~alc

        # --- dos velas -------------------------------------------------------
        # envolvente: el cuerpo de hoy contiene ENTERO al de ayer, con signo opuesto
        P["envolvente_alc"] = alc & ~alc1 & (c > o1) & (o < c1)
        P["envolvente_baj"] = ~alc & alc1 & (c < o1) & (o > c1)
        # harami: al reves — el cuerpo de AYER contiene al de hoy
        P["harami_alc"] = alc & ~alc1 & (c < o1) & (o > c1) & (cue1 > cuerpo)
        P["harami_baj"] = ~alc & alc1 & (c > o1) & (o < c1) & (cue1 > cuerpo)
        # perforante / nube oscura: cierra pasando el punto medio del cuerpo anterior
        medio1 = (o1 + c1) / 2.0
        P["perforante"] = alc & ~alc1 & (o < c1) & (c > medio1) & (c < o1)
        P["nube_oscura"] = ~alc & alc1 & (o > c1) & (c < medio1) & (c > o1)

        # --- tres velas ------------------------------------------------------
        # estrella: cuerpo grande, cuerpo chico (la estrella), cuerpo grande opuesto
        chico1 = cue1 <= CUERPO_CHICO * rng1
        P["estrella_maniana"] = (~alc2 & chico1 & alc
                                 & (cue2 > cue1) & (cuerpo > cue1)
                                 & (c > (o2 + c2) / 2.0))
        P["estrella_noche"] = (alc2 & chico1 & ~alc
                               & (cue2 > cue1) & (cuerpo > cue1)
                               & (c < (o2 + c2) / 2.0))
        # tres soldados / tres cuervos: 3 cuerpos del mismo signo, cada cierre superando
        P["tres_soldados"] = alc & alc1 & alc2 & (c > c1) & (c1 > c2)
        P["tres_cuervos"] = ~alc & ~alc1 & ~alc2 & (c < c1) & (c1 < c2)

    return {k: np.where(np.isnan(rng), False, v) for k, v in P.items()}


# Que direccion predice cada patron segun el canon. Se declara ANTES de medir: si se
# eligiera despues, cualquier patron "acierta" mirando el signo que le convino.
DIRECCION = {
    "martillo": "largo", "estrella_fugaz": "corto", "doji": None,
    "marubozu_alc": "largo", "marubozu_baj": "corto",
    "envolvente_alc": "largo", "envolvente_baj": "corto",
    "harami_alc": "largo", "harami_baj": "corto",
    "perforante": "largo", "nube_oscura": "corto",
    "estrella_maniana": "largo", "estrella_noche": "corto",
    "tres_soldados": "largo", "tres_cuervos": "corto",
}

# Los de REVERSION piden tendencia previa contraria; los de continuacion, a favor.
REVERSION = {"martillo", "estrella_fugaz", "envolvente_alc", "envolvente_baj",
             "harami_alc", "harami_baj", "perforante", "nube_oscura",
             "estrella_maniana", "estrella_noche"}


def contexto(df, barras=CTX):
    """(bajo, alto): si las `barras` previas vinieron cayendo o subiendo.

    Es el filtro que el canon exige y que `lote_ancho.py` nunca aplico. Mira solo al
    pasado: compara el cierre de la barra anterior contra el de `barras+1` atras, asi que
    no usa la vela del patron.
    """
    c = df["c"].to_numpy(float)
    c1 = _prev(c)
    cN = _prev(c, barras + 1)
    with np.errstate(invalid="ignore"):
        r = c1 / cN - 1.0
    return np.where(np.isnan(r), False, r < 0), np.where(np.isnan(r), False, r > 0)


if __name__ == "__main__":
    from klines import klines, to_ms
    for tf in ("1d", "1h"):
        df = klines("BTCUSDT", to_ms("2021-08-01"), to_ms("2026-08-01"), tf, full=True)
        P = patrones(df)
        bajo, alto = contexto(df)
        print(f"\n--- BTCUSDT {tf} · {len(df):,} velas ---")
        print(f"{'patron':20s} {'disparos':>9s} {'tasa':>7s} {'con contexto':>13s}")
        for k, m in P.items():
            ctx = bajo if DIRECCION.get(k) == "largo" else alto
            cc = int((m & ctx).sum()) if k in REVERSION else int(m.sum())
            print(f"{k:20s} {int(m.sum()):9,d} {m.mean():6.2%} {cc:13,d}")
