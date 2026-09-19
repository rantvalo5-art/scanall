"""
Cadena de opciones VIVA de Bybit y OKX, normalizada, con delta servido por el venue.

POR QUE ESTO EXISTE SEPARADO. Lo usan dos colectores con cadencias distintas
(`juntar_skew.py` una vez por dia, `prima_radar.py` cada 2h) y la unica forma de que
midan lo mismo es que bajen y normalicen la cadena con el mismo codigo.

EL PUNTO QUE ABARATA TODO: Bybit (`markIv`, `delta`) y OKX (`markVol`, `delta`) sirven
el delta CALCULADO. O sea que el risk reversal a 25 delta sale eligiendo el instrumento
cuyo delta esta mas cerca del objetivo, SIN interpolar el smile — que es la parte que
normalmente ensucia esta medicion y la que hace que dos implementaciones no coincidan.

Deribit queda afuera a proposito: `get_book_summary_by_currency` trae `mark_iv` pero NO
trae delta, y pedir greeks instrumento por instrumento son ~1000 requests por corrida.
Su DVOL —que es un indice, no una cadena— ya lo junta `juntar_iv.py`.

COBERTURA, medida el 2026-09-19 (no de memoria):

    bybit  BTC 802  ETH 725  SOL 396  XRP 316  DOGE 296  HYPE 400   instrumentos
    okx    BTC 1484 ETH 1292 SOL 194                                 filas
    bybit  BNB / ADA / LINK / AVAX / SUI / PEPE: CERO. No tienen opciones listadas.

Las unidades: los dos venues sirven la IV en FRACCION (0,3701 = 37,01%). Aca se pasa a
% para igualar la escala de `iv_diaria/`, que ya guarda %.
"""
import re

import pandas as pd
import requests

S = requests.Session()
S.headers.update({"User-Agent": "Mozilla/5.0"})

# Las seis que tienen opciones listadas. El resto del universo cripto no tiene
# instrumento, que es el hallazgo del que sale `PREREGISTRO_SKEW.md`.
MONEDAS = ["BTC", "ETH", "SOL", "XRP", "DOGE", "HYPE"]

COLS = ["venue", "moneda", "vence", "dias", "strike", "tipo", "iv", "delta",
        "iv_bid", "iv_ask", "mark", "subyacente", "oi"]


def _get(url, params, intentos=3):
    for i in range(intentos):
        try:
            r = S.get(url, params=params, timeout=45)
            r.raise_for_status()
            return r.json()
        except Exception:
            if i == intentos - 1:
                return None
            import time
            time.sleep(1.5 * (i + 1))
    return None


def _vacia():
    return pd.DataFrame(columns=COLS)


def _f(x, escala=1.0):
    """float tolerante: los venues mandan '' y '0' donde no hay dato."""
    try:
        v = float(x) * escala
    except (TypeError, ValueError):
        return float("nan")
    return v if v == v else float("nan")


def bybit(moneda, ahora):
    """`/v5/market/tickers?category=option`. Simbolo: BTC-25SEP26-170000-P-USDT."""
    r = _get("https://api.bybit.com/v5/market/tickers",
             {"category": "option", "baseCoin": moneda})
    lst = ((r or {}).get("result") or {}).get("list") or []
    filas = []
    for k in lst:
        p = str(k.get("symbol", "")).split("-")
        if len(p) < 4:
            continue
        try:
            vence = pd.Timestamp(p[1], tz="UTC") + pd.Timedelta(hours=8)
        except ValueError:
            continue
        iv = _f(k.get("markIv"), 100)
        d = _f(k.get("delta"))
        if not (iv > 0) or d != d:
            continue
        filas.append(dict(venue="bybit", moneda=moneda, vence=vence,
                          dias=(vence - ahora).total_seconds() / 86400,
                          strike=_f(p[2]), tipo=p[3], iv=iv, delta=d,
                          iv_bid=_f(k.get("bid1Iv"), 100),
                          iv_ask=_f(k.get("ask1Iv"), 100),
                          mark=_f(k.get("markPrice")),
                          subyacente=_f(k.get("underlyingPrice")),
                          oi=_f(k.get("openInterest"))))
    return pd.DataFrame(filas, columns=COLS) if filas else _vacia()


def okx(moneda, ahora):
    """`/api/v5/public/opt-summary`. instId trae un sufijo variable (`BTC-USD_UM-...`),
    asi que la fecha y el tipo se sacan por PATRON, no por posicion: el dia que OKX le
    agregue otro segmento, esto sigue andando en vez de romperse en silencio."""
    r = _get("https://www.okx.com/api/v5/public/opt-summary", {"uly": f"{moneda}-USD"})
    data = (r or {}).get("data") or []
    filas = []
    for k in data:
        inst = str(k.get("instId", ""))
        m = re.search(r"-(\d{6})-(\d+(?:\.\d+)?)-([CP])$", inst)
        if not m:
            continue
        try:
            vence = pd.Timestamp(f"20{m.group(1)}", tz="UTC") + pd.Timedelta(hours=8)
        except ValueError:
            continue
        iv = _f(k.get("markVol"), 100)
        d = _f(k.get("delta"))
        if not (iv > 0) or d != d:
            continue
        filas.append(dict(venue="okx", moneda=moneda, vence=vence,
                          dias=(vence - ahora).total_seconds() / 86400,
                          strike=_f(m.group(2)), tipo=m.group(3), iv=iv, delta=d,
                          iv_bid=_f(k.get("bidVol"), 100),
                          iv_ask=_f(k.get("askVol"), 100),
                          mark=float("nan"), subyacente=_f(k.get("fwdPx")),
                          oi=float("nan")))
    return pd.DataFrame(filas, columns=COLS) if filas else _vacia()


def bajar(moneda, ahora=None):
    """Las dos cadenas de una moneda, apiladas. Un venue caido no tumba al otro."""
    ahora = ahora if ahora is not None else pd.Timestamp.now(tz="UTC")
    partes = [f for f in (bybit(moneda, ahora), okx(moneda, ahora)) if not f.empty]
    return pd.concat(partes, ignore_index=True) if partes else _vacia()


def vencimiento(C, objetivo=30, minimo=7, maximo=75):
    """El vencimiento con mas instrumentos entre los cercanos a `objetivo` dias.

    Se elige por CALENDARIO —el mas cercano a 30 dias dentro de [7, 75]— y, a igual
    distancia, el que tenga mas instrumentos. Nunca por IV ni por nada que dependa del
    resultado: elegir el vencimiento mirando el numero que se va a medir es la forma
    mas facil de fabricar una serie que diga lo que uno quiere.
    """
    V = C[(C.dias >= minimo) & (C.dias <= maximo)]
    if V.empty:
        return None
    g = V.groupby("vence").agg(n=("iv", "size"), dias=("dias", "first")).reset_index()
    g["lejos"] = (g.dias - objetivo).abs()
    g = g.sort_values(["lejos", "n"], ascending=[True, False])
    return g.iloc[0]["vence"]


def _cerca(D, objetivo, tol=0.12):
    """El instrumento con delta mas cercano al objetivo, o None si no hay ninguno cerca.

    La tolerancia es para que una cadena flaca no devuelva un 'delta 25' que en realidad
    es un delta 5: preferible una fila faltante que una fila que miente.
    """
    if D.empty:
        return None
    d = D.assign(lejos=(D.delta - objetivo).abs()).sort_values("lejos")
    return None if d.iloc[0]["lejos"] > tol else d.iloc[0]


def rr25(C, venue, objetivo=30):
    """Risk reversal 25d, mariposa 25d e IV ATM de un venue, para el vencimiento elegido.

    rr25 = IV(put 25d) - IV(call 25d). POSITIVO = el seguro contra la caida esta MAS
    caro que la apuesta a la suba, que es el estado normal de un mercado que teme abajo.

    Devuelve None si falta cualquiera de las tres patas: media medicion no sirve y una
    serie con huecos tapados por defaults es peor que una serie corta.
    """
    V = C[C.venue == venue]
    if V.empty:
        return None
    vence = vencimiento(V, objetivo)
    if vence is None:
        return None
    E = V[V.vence == vence]
    put, call = _cerca(E[E.tipo == "P"], -0.25), _cerca(E[E.tipo == "C"], +0.25)
    atm = _cerca(E[E.tipo == "C"], +0.50, tol=0.15)
    if put is None or call is None or atm is None:
        return None
    oi_c, oi_p = E[E.tipo == "C"]["oi"].sum(), E[E.tipo == "P"]["oi"].sum()
    return dict(venue=venue, vence=pd.Timestamp(vence).strftime("%Y-%m-%d"),
                dias=round(float(E["dias"].iloc[0]), 2),
                iv_put25=round(float(put.iv), 4), iv_call25=round(float(call.iv), 4),
                iv_atm=round(float(atm.iv), 4),
                rr25=round(float(put.iv - call.iv), 4),
                mariposa25=round(float((put.iv + call.iv) / 2 - atm.iv), 4),
                delta_put=round(float(put.delta), 4),
                delta_call=round(float(call.delta), 4),
                n=int(len(E)),
                oi_calls=round(float(oi_c), 2), oi_puts=round(float(oi_p), 2),
                subyacente=round(float(E["subyacente"].median()), 6))
