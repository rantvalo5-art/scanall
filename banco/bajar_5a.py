"""Baja 5 anios de klines 1h ANCHO para los pares que tienen metricas de futuros.

El caché de `.metrics_cache/` tiene 47 pares con el rango 2021-08-01 -> 2026-07-31
completo (5 anios, granularidad horaria). Lo que falta para poder rankearlos es el
precio: `.kline_cache/` solo cubre 2025-08 -> 2026-08.

Se corre una vez y deja el caché listo:

    py -3.13 -u bajar_5a.py
"""
import json
import os
import sys
import time
from concurrent.futures import ThreadPoolExecutor

from klines import klines, to_ms

INICIO, FIN = "2021-08-01", "2026-08-01"
PIN = os.path.join(os.path.dirname(os.path.abspath(__file__)), "universo_5a.json")

SYMS = [
    "AAVEUSDT", "ACEUSDT", "ADAUSDT", "APTUSDT", "ARBUSDT", "AVAXUSDT", "BCHUSDT",
    "BICOUSDT", "BNBUSDT", "BONKUSDT", "BTCUSDT", "CAKEUSDT", "CRVUSDT", "DASHUSDT",
    "DOGEUSDT", "DOTUSDT", "ETHUSDT", "FETUSDT", "FILUSDT", "GALAUSDT", "HBARUSDT",
    "ICPUSDT", "INJUSDT", "LDOUSDT", "LINKUSDT", "LTCUSDT", "NEARUSDT", "ONDOUSDT",
    "ONGUSDT", "ONTUSDT", "OPUSDT", "ORDIUSDT", "PENDLEUSDT", "PEOPLEUSDT", "PEPEUSDT",
    "PYTHUSDT", "SHIBUSDT", "SOLUSDT", "SUIUSDT", "TRXUSDT", "UNIUSDT", "WIFUSDT",
    "WLDUSDT", "XLMUSDT", "XRPUSDT", "ZECUSDT",
]
# USDCUSDT queda AFUERA a proposito: es un par de stablecoin, volatilidad ~0, y en un
# ranking transversal por volatilidad seria ruido estructural, no una moneda.


def main():
    s_ms, e_ms = to_ms(INICIO), to_ms(FIN)
    print(f"{len(SYMS)} pares | {INICIO} -> {FIN} | 1h ANCHO", flush=True)
    t0 = [time.time()]
    hechos = [0]

    def uno(s):
        try:
            df = klines(s, s_ms, e_ms, "1h", full=True)
            n = 0 if df is None else len(df)
        except Exception as ex:
            print(f"  {s}: {type(ex).__name__} {ex}", flush=True)
            n = 0
        hechos[0] += 1
        print(f"  [{hechos[0]:2d}/{len(SYMS)}] {s:14s} {n:6,d} velas  "
              f"({time.time() - t0[0]:.0f}s)", flush=True)
        return s, n

    with ThreadPoolExecutor(8) as ex:
        res = list(ex.map(uno, SYMS))

    ok = [s for s, n in res if n > 2000]
    json.dump(ok, open(PIN, "w"))
    print(f"\n{len(ok)}/{len(SYMS)} pares con >=2000 velas -> {PIN}")
    print(f"total {time.time() - t0[0]:.0f}s")
    if len(ok) < 30:
        print("OJO: menos de 30 pares utiles, no hay seccion cruzada suficiente")
        sys.exit(1)


if __name__ == "__main__":
    main()
