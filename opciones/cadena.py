"""
Cadena de opciones VIVA de Bybit y OKX, normalizada, con delta servido por el venue.

POR QUE ESTO EXISTE SEPARADO. Lo usan dos colectores con cadencias distintas
(`juntar_skew.py` una vez por dia, `prima_radar.py` cada 2h) y la unica forma de que
midan lo mismo es que bajen y normalicen la cadena con el mismo codigo.

EL PUNTO QUE ABARATA TODO: Bybit (`markIv`, `delta`) y OKX (`markVol`, `deltaBS`)
sirven el delta CALCULADO. O sea que el risk reversal a 25 delta sale eligiendo el
instrumento cuyo delta esta mas cerca del objetivo, SIN interpolar el smile — que es
la parte que normalmente ensucia esta medicion y la que hace que dos
implementaciones no coincidan.

DE OKX HAY QUE PEDIR `deltaBS`, no `delta`: en la familia de settle en moneda el
segundo viene denominado en moneda y cruza 0,50 en K≈S/2, no en el dinero. Estuvo
mal del 2026-09-19 al 10-07 y se lo llevo puesto a las tres filas de skew de OKX.

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
import os
import re
import time

import pandas as pd
import requests

S = requests.Session()
S.headers.update({"User-Agent": "Mozilla/5.0"})

# Las seis que tienen opciones listadas. El resto del universo cripto no tiene
# instrumento, que es el hallazgo del que sale `PREREGISTRO_SKEW.md`.
MONEDAS = ["BTC", "ETH", "SOL", "XRP", "DOGE", "HYPE"]

COLS = ["venue", "moneda", "vence", "dias", "strike", "tipo", "iv", "delta",
        "iv_bid", "iv_ask", "mark", "subyacente", "oi"]

# LAS DOS FAMILIAS DE OKX, que `opt-summary` devuelve MEZCLADAS bajo el mismo `uly` y
# que hasta hoy se apilaban como un solo venue:
#
#     BTC-USD-...      settle BTC   cotiza en fraccion del subyacente
#     BTC-USD_UM-...   settle USD   cotiza en USD
#
# Medido el 2026-10-07 sobre el vencimiento de 0,32 dias, el ancho relativo del ATM:
#
#     BTC   inversa  call 3,1%  put 3,4%      _UM  call  61,7%  put  63,9%
#     ETH   inversa  call 4,1%  put 7,8%      _UM  call 107,0%  put 114,7%
#     SOL   no existe inversa                 _UM  call  64,8%  put  76,0%
#
# Las dos coinciden en el precio del straddle (BTC 0,6200% contra 0,6133%; ETH 0,6300%
# contra 0,6264%), asi que no es que una miente: es que en la de USD no hay nadie del
# otro lado. Por eso se guardan como venues DISTINTOS. Poolearlas ensucia el ancho del
# libro, que es justo la columna que dice si un resultado es ejecutable — y es lo que
# venia pasando: los `spread_iv` de 12 a 27 puntos de IV que `prima_radar` guardo para
# OKX salen de promediar las dos familias, no de un libro real.
OKX_FAM = {"": "okx", "_UM": "okx_um"}
OKX_INST = re.compile(r"^[A-Z]+-USD(_UM)?-(\d{6})-(\d+(?:\.\d+)?)-([CP])$")

# HOSTS DE BYBIT, y viven aca porque los usan los DOS colectores (`juntar_iv.py` y
# `juntar_skew.py` via este modulo). `api.bybit.com` devuelve 403 (Forbidden) desde las
# IPs de EE.UU., que es donde corren los runners de GitHub — el mismo geo-bloqueo que
# Binance sirve como 451 y que ya esta arreglado en `radar/radar.py`. Del 2026-09-20 al
# 09-30 el cron diario fallo once veces seguidas por esto, y local nunca se vio porque la
# maquina de desarrollo esta fuera de EE.UU.
#
# `api.bytick.com` es el dominio alterno oficial de Bybit, misma API; los otros dos son
# mirrors regionales. Se usa el primero que responda y el log dice cual fue. Se puede
# forzar uno con la env var BYBIT_API.
BYBIT_HOSTS = ["https://api.bytick.com", "https://api.bybit.com",
               "https://api.bybit.nl", "https://api.byhkbit.com"]
BYBIT = BYBIT_HOSTS[0]

_ULTIMO_ERROR = [None]


def _get(url, params=None, intentos=3):
    """Devuelve el JSON o None, y guarda el motivo en `_ULTIMO_ERROR`.

    Antes esto se tragaba la excepcion entera sin siquiera imprimirla, asi que un 403 se
    volvia "no hay cadena para esta moneda" — indistinguible de un dia flojo. Y
    reintentaba tres veces cualquier error, incluido el geo-bloqueo, que no se arregla
    reintentando.
    """
    for i in range(intentos):
        try:
            r = S.get(url, params=params, timeout=45)
            if r.status_code == 200:
                return r.json()
            _ULTIMO_ERROR[0] = f"HTTP {r.status_code} {r.text[:120]}"
            if r.status_code in (418, 429):
                time.sleep(2 ** i)
            else:
                return None          # 403, 451 y demas no se arreglan reintentando
        except Exception as e:
            _ULTIMO_ERROR[0] = f"{type(e).__name__}: {str(e)[:120]}"
            time.sleep(1.5 * (i + 1))
    return None


def elegir_bybit():
    """Primer host de Bybit que responda al ping. Devuelve la base, o None si ninguno.

    No corta el proceso a proposito: OKX y Deribit son fuentes independientes y su dato
    se guarda igual aunque Bybit este bloqueado. Ese acoplamiento es el que tiro 11 dias
    de DVOL y 11 de skew.
    """
    global BYBIT
    forzado = os.environ.get("BYBIT_API")
    for base in ([forzado] if forzado else BYBIT_HOSTS):
        if _get(f"{base}/v5/market/time") is not None:
            BYBIT = base
            print(f"bybit api: {base}", flush=True)
            return base
        print(f"  {base} no responde ({_ULTIMO_ERROR[0]})", flush=True)
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
    r = _get(f"{BYBIT}/v5/market/tickers",
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


def _okx_precios(moneda):
    """El `mark` que `opt-summary` NO trae: el medio del libro vivo de
    `/market/tickers`, en USD por unidad de subyacente (la unidad del `markPrice` de
    Bybit, para que las dos columnas se comparen sin convertir nada despues).

    Sigue sin inventarse un precio con un Black-Scholes propio —eso seguiria no siendo
    lo que se paga—: son el bid y el ask publicados. Una sola request por moneda trae
    las dos familias. Devuelve {(venue, vto, strike, tipo): (medio_crudo, ancho)}, con
    el medio en la unidad en que cotiza cada familia; la conversion a USD se hace en
    `okx()`, que es donde esta el forward.
    """
    r = _get("https://www.okx.com/api/v5/market/tickers",
             {"instType": "OPTION", "uly": f"{moneda}-USD"})
    out = {}
    for k in (r or {}).get("data") or []:
        m = OKX_INST.match(str(k.get("instId", "")))
        if not m:
            continue
        bid, ask = _f(k.get("bidPx")), _f(k.get("askPx"))
        if not (bid > 0 and ask > 0):
            continue          # sin las dos patas no hay medio, y un lado solo miente
        clave = (OKX_FAM[m.group(1) or ""], m.group(2), _f(m.group(3)), m.group(4))
        out[clave] = (bid + ask) / 2
    return out


def okx(moneda, ahora):
    """`/api/v5/public/opt-summary` + `/market/tickers`, separadas por familia.

    El instId trae un sufijo variable (`BTC-USD_UM-...`), asi que la fecha y el tipo se
    sacan por PATRON y no por posicion: el dia que OKX le agregue otro segmento, esto
    sigue andando en vez de romperse en silencio. Ese mismo sufijo es el que distingue
    las dos familias (ver `OKX_FAM`), que antes se apilaban juntas.
    """
    r = _get("https://www.okx.com/api/v5/public/opt-summary", {"uly": f"{moneda}-USD"})
    data = (r or {}).get("data") or []
    precios = _okx_precios(moneda) if data else {}
    filas = []
    for k in data:
        m = OKX_INST.match(str(k.get("instId", "")))
        if not m:
            continue
        try:
            vence = pd.Timestamp(f"20{m.group(2)}", tz="UTC") + pd.Timedelta(hours=8)
        except ValueError:
            continue
        iv = _f(k.get("markVol"), 100)
        # `deltaBS` Y NO `delta`. En la familia inversa (settle en moneda) OKX sirve en
        # `delta` el delta DENOMINADO EN MONEDA, que para un call muy dentro del dinero
        # tiende a K/S en vez de a 1: el 2026-10-07, con ETH a 2.576, el call de strike
        # 1.100 a 22 dias figuraba con delta 0,427 (= 1100/2576) en vez de 0,9999. Con
        # eso, buscar "el delta 0,50" devolvia el strike de la MITAD del spot y una IV de
        # 91,9% donde Bybit medía 46,5%, y "el delta 25" no era un delta 25. `deltaBS` es
        # el delta Black-Scholes y viene en el mismo payload; en la familia `_UM` los dos
        # campos son identicos, asi que el cambio solo toca a la inversa.
        d = _f(k.get("deltaBS"))
        if d != d:
            d = _f(k.get("delta"))
        if not (iv > 0) or d != d:
            continue
        venue, strike, tipo = OKX_FAM[m.group(1) or ""], _f(m.group(3)), m.group(4)
        fwd = _f(k.get("fwdPx"))
        medio = precios.get((venue, m.group(2), strike, tipo), float("nan"))
        # La inversa cotiza en fraccion del subyacente; la `_UM` ya viene en USD.
        mark = medio if venue == "okx_um" else medio * fwd
        filas.append(dict(venue=venue, moneda=moneda, vence=vence,
                          dias=(vence - ahora).total_seconds() / 86400,
                          strike=strike, tipo=tipo, iv=iv, delta=d,
                          iv_bid=_f(k.get("bidVol"), 100),
                          iv_ask=_f(k.get("askVol"), 100),
                          mark=mark, subyacente=fwd,
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
