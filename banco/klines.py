"""
Capa de datos del banco: universo de pares USDT spot + velas cacheadas en disco.

Ventana FIJA con fechas explicitas, nunca relativa a hoy. Dos corridas separadas con
`--weeks` cubren periodos distintos y eso solo ya fabrico artefactos de varios puntos
porcentuales en este repo. Ver README.
"""
import json
import os
import time
from datetime import datetime, timezone

import pandas as pd
import requests

HERE = os.path.dirname(os.path.abspath(__file__))
CACHE = os.path.join(HERE, ".kline_cache")
SPOT = "https://api.binance.com"
FUT = "https://fapi.binance.com"

# Los dos mercados hablan el mismo dialecto de klines (mismo array, mismos indices) y
# casi el mismo de exchangeInfo, asi que toda la capa de abajo se parametriza con
# `mercado` en vez de duplicarse. Lo unico que NO es igual es el filtro de simbolos
# (spot: isSpotTradingAllowed; fut: contractType == PERPETUAL) y el prefijo de la ruta.
API = {"spot": (SPOT, "/api/v3"), "fut": (FUT, "/fapi/v1")}


def to_ms(d):
    return int(datetime.strptime(d, "%Y-%m-%d")
               .replace(tzinfo=timezone.utc).timestamp() * 1000)


def _get(url, params=None, tries=5):
    for i in range(tries):
        try:
            r = requests.get(url, params=params, timeout=25)
            if r.status_code == 200:
                return r.json()
            if r.status_code in (418, 429):      # rate limit
                time.sleep(2 ** i)
                continue
            time.sleep(0.4 * (i + 1))
        except Exception:
            time.sleep(0.4 * (i + 1))
    return None


def universe(n=200, mercado="spot"):
    """Top-n pares USDT por volumen de 24h, del mercado que se pida.

    `mercado="fut"` devuelve PERPETUOS USDT-margen de Binance Futures. Ojo con los
    simbolos: en perp una moneda barata cotiza como `1000PEPEUSDT`. Los retornos son
    identicos al spot (es la misma serie x1000), pero el string no.

    OJO — SESGO DE UNIVERSO: es el ranking de HOY. Los pares deslistados durante la
    ventana no estan, y los que sobrevivieron son los que mejor les fue. Sesga hacia
    mejor. Declararlo en todo resultado.
    """
    base, pref = API[mercado]
    ei = _get(f"{base}{pref}/exchangeInfo")
    if not ei:
        return []
    if mercado == "fut":
        ok = {s["symbol"] for s in ei["symbols"]
              if s.get("quoteAsset") == "USDT" and s.get("status") == "TRADING"
              and s.get("contractType") == "PERPETUAL"}
    else:
        ok = {s["symbol"] for s in ei["symbols"]
              if s["quoteAsset"] == "USDT" and s["status"] == "TRADING"
              and s.get("isSpotTradingAllowed")}
    tk = _get(f"{base}{pref}/ticker/24hr") or []
    rows = [(d["symbol"], float(d["quoteVolume"])) for d in tk if d["symbol"] in ok]
    rows.sort(key=lambda r: -r[1])
    return [s for s, _ in rows[:n]]


def _paths(sym, start_ms, end_ms, tf, full=False, mercado="spot"):
    # el sufijo _v2 separa el cache ANCHO del angosto: los parquet ya bajados tienen
    # solo [t,h,l,c] y si compartieran nombre devolverian un frame sin volumen.
    # `_fut` separa el perpetuo del spot por la misma razon: BTCUSDT existe en los dos
    # y NO es la misma serie (la base se mueve). Compartir nombre mezclaria mercados.
    suf = ("_v2" if full else "") + ("_fut" if mercado == "fut" else "")
    base = os.path.join(CACHE, f"{sym}_{tf}_{start_ms}_{end_ms}{suf}")
    return base + ".parquet", base + ".csv"


def klines(sym, start_ms, end_ms, tf="1h", full=False, mercado="spot"):
    """Velas de un par, cacheadas. Devuelve DataFrame [t, h, l, c] o None.

    `full=True` conserva ademas lo que la version angosta tiraba y que por eso el banco
    nunca pudo probar: apertura, volumen, volumen en USD, numero de trades y **volumen
    taker comprador** (lo mas parecido a order flow que hay en una vela). Usa un cache
    aparte, asi que no invalida lo ya bajado.
    """
    os.makedirs(CACHE, exist_ok=True)
    p_pq, p_csv = _paths(sym, start_ms, end_ms, tf, full, mercado)
    # Mirar LAS DOS extensiones: si el write de parquet falla se guarda csv, y si
    # solo se chequea parquet el cache nunca pega y se re-descarga todo cada corrida.
    for path, reader in ((p_pq, pd.read_parquet), (p_csv, pd.read_csv)):
        if os.path.exists(path):
            try:
                return reader(path)
            except Exception:
                pass

    rows, cursor = [], start_ms
    while cursor < end_ms:
        base, pref = API[mercado]
        d = _get(f"{base}{pref}/klines",
                 {"symbol": sym, "interval": tf, "startTime": cursor,
                  "endTime": end_ms, "limit": 1000})
        if not d:
            break
        rows.extend(d)
        last = int(d[-1][0])
        if len(d) < 1000 or last <= cursor:
            break
        cursor = last + 1
    if not rows:
        return None

    if full:
        # indices del array de Binance: 1 open, 5 volumen base, 7 volumen quote (USD),
        # 8 numero de trades, 9 volumen taker comprador en base.
        df = pd.DataFrame([{"t": int(r[0]), "o": float(r[1]), "h": float(r[2]),
                            "l": float(r[3]), "c": float(r[4]), "v": float(r[5]),
                            "qv": float(r[7]), "n": int(r[8]),
                            "vb": float(r[9])} for r in rows])
    else:
        df = pd.DataFrame([{"t": int(r[0]), "h": float(r[2]),
                            "l": float(r[3]), "c": float(r[4])} for r in rows])
    df = df.drop_duplicates("t").sort_values("t").reset_index(drop=True)
    try:
        df.to_parquet(p_pq, index=False)
    except Exception:
        df.to_csv(p_csv, index=False)
    return df


def _universo_fijo(n, pin, mercado="spot"):
    """El universo tambien tiene que ser FIJO, no solo la ventana.

    `universe()` consulta el ranking de volumen EN VIVO, asi que dos corridas
    separadas por horas devuelven listas distintas y la linea base se mueve sola
    (se vio: 48,63% -> 48,71% entre dos corridas del mismo lote). Con `pin` la
    lista se congela en disco la primera vez y despues se reusa.
    """
    if not pin:
        return universe(n, mercado)
    path = os.path.join(CACHE, f"universo_{pin}.json")
    if os.path.exists(path):
        with open(path, encoding="utf-8") as f:
            return json.load(f)
    syms = universe(n, mercado)
    os.makedirs(CACHE, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(syms, f)
    return syms


def load_panel(start, end, n=200, tf="1h", min_bars=2000, verbose=True, pin=None,
               full=False, workers=1, mercado="spot", syms=None):
    """{symbol: df} para toda la ventana. Descarga lo que falte, usa cache si esta.

    `pin`: nombre para congelar el universo en disco y que la corrida sea
    reproducible. Sin pin, el ranking de volumen se re-consulta cada vez.
    `full`: traer tambien apertura/volumen/trades/taker (ver `klines`).
    `workers`: descarga en paralelo POR PAR (los chunks de un mismo par siguen siendo
    secuenciales por el cursor). Con `tf` fino esto no es un lujo: un ano de 5m son ~106
    requests por par a ~2,5s de latencia = 4,5 min/par, o sea 15 horas para 200 pares en
    serie. Con 12 workers baja a ~75 min. Default 1 = el comportamiento de siempre.
    """
    s_ms, e_ms = to_ms(start), to_ms(end)
    syms = list(syms) if syms is not None else _universo_fijo(n, pin, mercado)
    if verbose:
        print(f"Ventana FIJA {start} -> {end} | pidiendo {len(syms)} pares ({tf})"
              f"{' | PERPETUOS' if mercado == 'fut' else ''}"
              f"{' | ANCHO' if full else ''}{f' | {workers} workers' if workers > 1 else ''}"
              f"{' | lista de simbolos EXPLICITA' if syms is not None else (' | universo FIJO: ' + pin if pin else ' | universo EN VIVO (no reproducible)')}")
    panel, t0, hechos = {}, time.time(), [0]

    def _uno(s):
        try:
            return s, klines(s, s_ms, e_ms, tf, full=full, mercado=mercado)
        except Exception as ex:
            print(f"  {s}: {type(ex).__name__} {ex}", flush=True)
            return s, None

    def _guardar(s, df):
        if df is not None and len(df) >= min_bars:
            panel[s] = df
        hechos[0] += 1
        if verbose and hechos[0] % 25 == 0:
            print(f"  {hechos[0]}/{len(syms)}  ({time.time()-t0:.0f}s)", flush=True)

    if workers > 1:
        from concurrent.futures import ThreadPoolExecutor, as_completed
        with ThreadPoolExecutor(workers) as ex:
            for f in as_completed([ex.submit(_uno, s) for s in syms]):
                _guardar(*f.result())
        # el orden de as_completed es arbitrario; el panel se reordena como el universo
        panel = {s: panel[s] for s in syms if s in panel}
    else:
        for s in syms:
            _guardar(*_uno(s))

    if verbose:
        print(f"  {len(panel)} pares con >= {min_bars} velas | {time.time()-t0:.0f}s")
    return panel
