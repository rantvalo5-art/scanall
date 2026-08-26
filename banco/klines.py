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


def universe(n=200):
    """Top-n pares USDT spot por volumen de 24h.

    OJO — SESGO DE UNIVERSO: es el ranking de HOY. Los pares deslistados durante la
    ventana no estan, y los que sobrevivieron son los que mejor les fue. Sesga hacia
    mejor. Declararlo en todo resultado.
    """
    ei = _get(f"{SPOT}/api/v3/exchangeInfo")
    if not ei:
        return []
    ok = {s["symbol"] for s in ei["symbols"]
          if s["quoteAsset"] == "USDT" and s["status"] == "TRADING"
          and s.get("isSpotTradingAllowed")}
    tk = _get(f"{SPOT}/api/v3/ticker/24hr") or []
    rows = [(d["symbol"], float(d["quoteVolume"])) for d in tk if d["symbol"] in ok]
    rows.sort(key=lambda r: -r[1])
    return [s for s, _ in rows[:n]]


def _paths(sym, start_ms, end_ms, tf, full=False):
    # el sufijo _v2 separa el cache ANCHO del angosto: los parquet ya bajados tienen
    # solo [t,h,l,c] y si compartieran nombre devolverian un frame sin volumen.
    suf = "_v2" if full else ""
    base = os.path.join(CACHE, f"{sym}_{tf}_{start_ms}_{end_ms}{suf}")
    return base + ".parquet", base + ".csv"


def klines(sym, start_ms, end_ms, tf="1h", full=False):
    """Velas de un par, cacheadas. Devuelve DataFrame [t, h, l, c] o None.

    `full=True` conserva ademas lo que la version angosta tiraba y que por eso el banco
    nunca pudo probar: apertura, volumen, volumen en USD, numero de trades y **volumen
    taker comprador** (lo mas parecido a order flow que hay en una vela). Usa un cache
    aparte, asi que no invalida lo ya bajado.
    """
    os.makedirs(CACHE, exist_ok=True)
    p_pq, p_csv = _paths(sym, start_ms, end_ms, tf, full)
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
        d = _get(f"{SPOT}/api/v3/klines",
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


def _universo_fijo(n, pin):
    """El universo tambien tiene que ser FIJO, no solo la ventana.

    `universe()` consulta el ranking de volumen EN VIVO, asi que dos corridas
    separadas por horas devuelven listas distintas y la linea base se mueve sola
    (se vio: 48,63% -> 48,71% entre dos corridas del mismo lote). Con `pin` la
    lista se congela en disco la primera vez y despues se reusa.
    """
    if not pin:
        return universe(n)
    path = os.path.join(CACHE, f"universo_{pin}.json")
    if os.path.exists(path):
        with open(path, encoding="utf-8") as f:
            return json.load(f)
    syms = universe(n)
    os.makedirs(CACHE, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(syms, f)
    return syms


def load_panel(start, end, n=200, tf="1h", min_bars=2000, verbose=True, pin=None,
               full=False, workers=1):
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
    syms = _universo_fijo(n, pin)
    if verbose:
        print(f"Ventana FIJA {start} -> {end} | pidiendo {len(syms)} pares ({tf})"
              f"{' | ANCHO' if full else ''}{f' | {workers} workers' if workers > 1 else ''}"
              f"{' | universo FIJO: ' + pin if pin else ' | universo EN VIVO (no reproducible)'}")
    panel, t0, hechos = {}, time.time(), [0]

    def _uno(s):
        try:
            return s, klines(s, s_ms, e_ms, tf, full=full)
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
