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


def _paths(sym, start_ms, end_ms, tf):
    base = os.path.join(CACHE, f"{sym}_{tf}_{start_ms}_{end_ms}")
    return base + ".parquet", base + ".csv"


def klines(sym, start_ms, end_ms, tf="1h"):
    """Velas de un par, cacheadas. Devuelve DataFrame [t, h, l, c] o None."""
    os.makedirs(CACHE, exist_ok=True)
    p_pq, p_csv = _paths(sym, start_ms, end_ms, tf)
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


def load_panel(start, end, n=200, tf="1h", min_bars=2000, verbose=True, pin=None):
    """{symbol: df} para toda la ventana. Descarga lo que falte, usa cache si esta.

    `pin`: nombre para congelar el universo en disco y que la corrida sea
    reproducible. Sin pin, el ranking de volumen se re-consulta cada vez.
    """
    s_ms, e_ms = to_ms(start), to_ms(end)
    syms = _universo_fijo(n, pin)
    if verbose:
        print(f"Ventana FIJA {start} -> {end} | pidiendo {len(syms)} pares ({tf})"
              f"{' | universo FIJO: ' + pin if pin else ' | universo EN VIVO (no reproducible)'}")
    panel, t0 = {}, time.time()
    for i, s in enumerate(syms, 1):
        df = klines(s, s_ms, e_ms, tf)
        if df is not None and len(df) >= min_bars:
            panel[s] = df
        if verbose and i % 50 == 0:
            print(f"  {i}/{len(syms)}...", flush=True)
    if verbose:
        print(f"  {len(panel)} pares con >= {min_bars} velas | {time.time()-t0:.0f}s")
    return panel
