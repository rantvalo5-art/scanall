"""
Guarda la volatilidad IMPLICITA de todos los dias. Es lo unico del repo donde no
hacer nada tiene un costo que se acumula.

POR QUE EXISTE. La corrida 8 (`banco/PREREGISTRO_OPCIONES.md`) cerro la direccion de
vender volatilidad en alts, y no la cerro por el efecto: la cerro por POTENCIA. El
instrumento existe y cruzar cuesta 1-2% de la prima, pero la unica historia de
implicita para alts es el indice de Bybit, y en agosto de 2026 eso eran 18 meses para
SOL, 10 para XRP y 7 semanas para HYPE. Con n = 18 meses el MDE da 39%/anio contra un
umbral preregistrado de 10%, y la calibracion es lo incontestable: BTC con esa MISMA
ventana da 27,1%, o sea que su efecto conocido tampoco seria detectable.

Lo unico que reabre esa direccion es HISTORIA. No un metodo mejor, no otra feature:
mas anios del mismo dato. Y la historia solo se puede empezar a juntar, no se puede
recuperar despues — por eso esto es un cron y no un script que se corre cuando hace
falta.

QUE JUNTA, Y QUE NO. Solo la IMPLICITA, a proposito. La realizada sale de klines de
Binance, que estan completos hacia atras y se pueden bajar cualquier dia; la implicita
no: los venues sirven una ventana limitada y lo que no se guardo hoy no esta manana.
Se guarda lo perecedero.

    py -3.13 -u juntar_iv.py              # completa lo que falte (esto corre el cron)
    py -3.13 -u juntar_iv.py --sembrar    # primera vez: baja toda la historia que haya
    py -3.13 -u juntar_iv.py --dias 30    # ventana explicita al completar

CUANTO SE PUEDE RECUPERAR, medido el 2026-09-30 y no supuesto. La frase de arriba ("lo que
no se guardo hoy no esta manana") es la razon de ser de este cron y sigue siendo cierta a
la larga, pero hay un colchon: `historical-volatility` de Bybit sirve la ventana horaria
completa de los ultimos ~25 dias (600 filas por request), y el DVOL de Deribit llega a
2021. O sea que un cron caido se recupera con `--dias 30` mientras no pase de un mes. Eso
es lo que salvo los 11 dias del 2026-09-20 al 09-30. Mas alla de eso, no hay vuelta.

Sale a `opciones/iv_diaria/<fuente>_<MONEDA>.csv`, una fila por dia, append idempotente
por fecha: correrlo dos veces el mismo dia no duplica nada.
"""
import argparse
import os
import sys
import time

import pandas as pd

# El cliente HTTP, la lista de hosts de Bybit y la eleccion de host viven en `cadena.py`
# porque los usan los DOS colectores de este directorio, y el geo-bloqueo les pega igual a
# los dos. Ver el comentario de `cadena.BYBIT_HOSTS` para el 403 de los runners. Se importa
# el modulo y no `BYBIT`, porque `elegir_bybit()` reasigna esa global y una copia queda
# vieja.
import cadena
from cadena import _ULTIMO_ERROR, _get, elegir_bybit

HERE = os.path.dirname(os.path.abspath(__file__))
SALIDA = os.path.join(HERE, "iv_diaria")

# Los mismos candidatos que barre `viabilidad.py`. Se prueban todos: los que no tengan
# opciones listadas simplemente no devuelven nada, y el dia que Bybit liste uno nuevo
# esto lo empieza a juntar solo, sin tocar el archivo.
CANDIDATOS = ("BTC ETH SOL XRP DOGE BNB LTC ADA AVAX LINK TON TRX HYPE PEPE SUI APT "
              "ARB OP NEAR DOT MATIC SHIB WLD").split()

# Deribit publica un indice de vol implicita a 30d (el DVOL, el "VIX de cripto") solo
# para BTC y ETH. Es la serie larga —2021 en adelante— y la que uso `iv_rv.py`.
DVOL = ["BTC", "ETH"]


def bybit_iv(moneda, desde, hasta):
    """Indice de vol implicita a 30d de Bybit, horario, entre dos timestamps.

    GOTCHA, y da un falso silencioso: EXIGE `quoteCoin=USDT`. Sin eso devuelve
    `retCode: 0, SUCCESS` con la lista VACIA, que se lee como "esta moneda no tiene
    datos" y no lo es. Costo un rato en la corrida 8.

    Ademas la ventana no puede pasar de ~30 dias por request, asi que se pagina hacia
    atras de a 25.
    """
    filas, fin, vacias = [], hasta, 0
    while fin > desde:
        ini = max(fin - pd.Timedelta(days=25), desde)
        r = _get(f"{cadena.BYBIT}/v5/market/historical-volatility",
                 {"category": "option", "baseCoin": moneda, "quoteCoin": "USDT",
                  "period": "30", "startTime": int(ini.timestamp() * 1000),
                  "endTime": int(fin.timestamp() * 1000)})
        res = (r or {}).get("result") or []
        if res:
            filas.extend(res)
            vacias = 0
        else:
            vacias += 1
            if vacias >= 2:      # dos ventanas seguidas sin nada: se acabo la historia
                break
        fin = ini
        time.sleep(0.12)

    if not filas:
        return pd.DataFrame(columns=["fecha", "iv"])
    d = pd.DataFrame(filas).drop_duplicates("time")
    d["fecha"] = pd.to_datetime(d["time"].astype("int64"), unit="ms",
                                utc=True).dt.tz_localize(None).dt.normalize()
    # el indice viene en fraccion (0,3736 = 37,36%); a % para igualar a iv_rv.py
    d["iv"] = d["value"].astype(float) * 100
    return d.groupby("fecha", as_index=False)["iv"].mean()


def deribit_dvol(moneda, desde, hasta):
    """DVOL diario. Se pide anio por anio.

    GOTCHA anotado en `iv_rv.py`: la API devuelve los ULTIMOS ~1000 puntos de la
    ventana pedida, no los primeros, asi que un pedido de 2021 a 2026 de una sola vez
    arranca en 2023-11 y cualquier paginacion hacia adelante sale en la primera vuelta.
    """
    filas = []
    for a in range(desde.year, hasta.year + 1):
        ini = max(pd.Timestamp(f"{a}-01-01", tz="UTC"), desde)
        fin = min(pd.Timestamp(f"{a + 1}-01-01", tz="UTC"), hasta)
        if ini >= fin:
            continue
        r = _get("https://www.deribit.com/api/v2/public/get_volatility_index_data",
                 {"currency": moneda, "start_timestamp": int(ini.timestamp() * 1000),
                  "end_timestamp": int(fin.timestamp() * 1000), "resolution": "86400"})
        filas.extend(((r or {}).get("result") or {}).get("data") or [])
        time.sleep(0.15)

    if not filas:
        return pd.DataFrame(columns=["fecha", "iv"])
    d = pd.DataFrame(filas, columns=["t", "o", "h", "l", "c"]).drop_duplicates("t")
    d["fecha"] = pd.to_datetime(d["t"], unit="ms", utc=True).dt.tz_localize(None).dt.normalize()
    return d.groupby("fecha", as_index=False)["c"].last().rename(columns={"c": "iv"})


def guardar(fuente, moneda, nuevo):
    """Mergea contra lo que ya hay y reescribe. Idempotente por fecha.

    Las filas viejas NUNCA se pisan con las nuevas: si una fecha ya estaba guardada, se
    queda la guardada. Un cambio retroactivo del indice en el venue no deberia poder
    reescribir historia que ya se uso para medir.
    """
    os.makedirs(SALIDA, exist_ok=True)
    p = os.path.join(SALIDA, f"{fuente}_{moneda}.csv")
    viejo = pd.read_csv(p, parse_dates=["fecha"]) if os.path.exists(p) else None
    if viejo is None or viejo.empty:
        todo, viejo = nuevo.copy(), pd.DataFrame(columns=["fecha", "iv"])
    elif nuevo.empty:
        todo = viejo.copy()
    else:
        todo = pd.concat([viejo, nuevo], ignore_index=True)
    if todo.empty:
        return 0, 0
    todo = todo.dropna(subset=["fecha", "iv"]).drop_duplicates("fecha", keep="first")
    todo = todo.sort_values("fecha")
    agregadas = len(todo) - len(viejo)
    todo.to_csv(p, index=False, date_format="%Y-%m-%d")
    return agregadas, len(todo)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--sembrar", action="store_true",
                    help="baja toda la historia que el venue tenga (primera corrida)")
    ap.add_argument("--dias", type=int, default=7,
                    help="ventana a completar cuando no se siembra (default 7)")
    a = ap.parse_args()

    hasta = pd.Timestamp.utcnow().tz_convert("UTC") if pd.Timestamp.utcnow().tz \
        else pd.Timestamp.utcnow().tz_localize("UTC")
    # Sembrar: 2021 cubre toda la historia de las dos fuentes. Bybit corta solo cuando
    # se le acaban los datos (dos ventanas vacias seguidas), asi que pedir de mas es
    # barato y no inventa filas.
    desde = pd.Timestamp("2021-01-01", tz="UTC") if a.sembrar \
        else hasta - pd.Timedelta(days=a.dias)

    modo = "SEMBRANDO (historia completa)" if a.sembrar else f"completando {a.dias}d"
    print(f"iv_diaria — {modo} — hasta {hasta:%Y-%m-%d %H:%M} UTC\n", flush=True)

    total_nuevas, con_datos, dvol_ok = 0, 0, 0
    print("bybit (indice 30d):", flush=True)
    if elegir_bybit() is None:
        print(f"  ningun host de Bybit responde. Ultimo error: {_ULTIMO_ERROR[0]}",
              flush=True)
    else:
        for m in CANDIDATOS:
            d = bybit_iv(m, desde, hasta)
            if d.empty:
                continue
            nuevas, tot = guardar("bybit", m, d)
            total_nuevas += nuevas
            con_datos += 1
            print(f"  {m:6} +{nuevas:4} filas   total {tot:5}   "
                  f"{d['fecha'].min():%Y-%m-%d} -> {d['fecha'].max():%Y-%m-%d}", flush=True)

    print("\nderibit (DVOL):", flush=True)
    for m in DVOL:
        d = deribit_dvol(m, desde, hasta)
        if d.empty:
            continue
        nuevas, tot = guardar("deribit", m, d)
        total_nuevas += nuevas
        dvol_ok += 1
        print(f"  {m:6} +{nuevas:4} filas   total {tot:5}   "
              f"{d['fecha'].min():%Y-%m-%d} -> {d['fecha'].max():%Y-%m-%d}", flush=True)

    print(f"\n{total_nuevas} filas nuevas · {con_datos} monedas con opciones en bybit · "
          f"{dvol_ok} de {len(DVOL)} DVOL")

    # QUE FALLA Y QUE NO, y esto ya se equivoco una vez en la direccion caraque:
    #
    # El `exit 1` original saltaba cuando Bybit no devolvia nada, y estaba bien pensado
    # —la forma en que un colector se muere es quedando en verde durante meses— pero
    # estaba DEMASIADO ARRIBA: mataba el paso que commitea, asi que las 16 filas diarias
    # de DVOL que SI se bajaban se tiraban a la basura. Once dias asi.
    #
    # Ahora la regla es por fuente. Si una responde y la otra no, se guarda lo que hay y
    # se avisa fuerte (`::warning::` lo pinta en el resumen del run, no solo en el log).
    # Solo se corta cuando NINGUNA de las dos junto nada, que es el caso que de verdad
    # significa "esto dejo de funcionar".
    if con_datos == 0 and dvol_ok == 0:
        print("FATAL: ninguna de las dos fuentes devolvio datos. Ultimo error: "
              f"{_ULTIMO_ERROR[0]}", file=sys.stderr)
        sys.exit(1)

    if con_datos == 0:
        print("::warning title=Bybit caido::Bybit no devolvio ninguna moneda "
              f"({_ULTIMO_ERROR[0]}). Se guardo solo el DVOL de Deribit. La implicita de "
              "alts es lo unico que reabre la corrida 8 y es lo que se esta perdiendo.",
              flush=True)
    if dvol_ok == 0:
        print("::warning title=Deribit caido::Deribit no devolvio DVOL "
              f"({_ULTIMO_ERROR[0]}). Se guardo solo el indice de Bybit.", flush=True)


if __name__ == "__main__":
    main()
