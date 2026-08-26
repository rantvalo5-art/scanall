"""
FUNDING — el costo/ingreso de mantener un perpetuo abierto.

Por que importa aca: el test del PREREGISTRO_OI shortea y aguanta hasta 7 dias. Binance
cobra funding cada 8h, o sea hasta 21 pagos por trade. Aun a 0,01% por pago eso es 0,63%
en 7 dias — TRES VECES la comision de 0,20% que el banco ya cuenta. Ignorarlo no es
conservador, es simplemente no medirlo.

Signo (esto se equivoca facil):
  funding > 0  -> el perp cotiza sobre el indice, los LARGOS pagan a los cortos
                  -> un SHORT COBRA
  funding < 0  -> al reves: un SHORT PAGA

    py -3.13 funding.py     # self-test
"""
import os
import time

import pandas as pd
import requests

HERE = os.path.dirname(os.path.abspath(__file__))
CACHE = os.path.join(HERE, ".funding_cache")
FAPI = "https://fapi.binance.com/fapi/v1/fundingRate"


def _variantes(spot):
    return [spot, f"1000{spot}", f"1000000{spot}"]


def bajar(spot, ini_ms, fin_ms):
    """Serie de funding de un par. Cacheada. None si no hay perp."""
    os.makedirs(CACHE, exist_ok=True)
    p = os.path.join(CACHE, f"{spot}_{ini_ms}_{fin_ms}.csv")
    if os.path.exists(p):
        d = pd.read_csv(p)
        return None if d.empty else d

    for perp in _variantes(spot):
        filas, cursor = [], ini_ms
        while cursor < fin_ms:
            try:
                r = requests.get(FAPI, params={"symbol": perp, "startTime": cursor,
                                               "endTime": fin_ms, "limit": 1000},
                                 timeout=25)
                if r.status_code != 200:
                    break
                b = r.json()
            except Exception:
                time.sleep(1)
                continue
            if not b:
                break
            filas.extend(b)
            ultimo = int(b[-1]["fundingTime"])
            if len(b) < 1000 or ultimo <= cursor:
                break
            cursor = ultimo + 1
        if filas:
            d = pd.DataFrame([{"t": int(x["fundingTime"]),
                               "rate": float(x["fundingRate"])} for x in filas])
            d = d.drop_duplicates("t").sort_values("t").reset_index(drop=True)
            d.to_csv(p, index=False)
            return d

    pd.DataFrame(columns=["t", "rate"]).to_csv(p, index=False)   # marca "sin perp"
    return None


def acumulado(fund, t0_ms, horas):
    """Suma de funding entre t0 y t0+horas. Es lo que COBRA un short (signo directo)."""
    if fund is None or fund.empty:
        return 0.0
    t1 = t0_ms + horas * 3_600_000
    s = fund["t"].to_numpy()
    lo = s.searchsorted(t0_ms, "left")
    hi = s.searchsorted(t1, "right")
    return float(fund["rate"].to_numpy()[lo:hi].sum())


if __name__ == "__main__":
    from klines import to_ms
    for s in ("BTCUSDT", "SOLUSDT", "AAVEUSDT"):
        d = bajar(s, to_ms("2021-08-01"), to_ms("2026-08-01"))
        if d is None:
            print(f"{s:12s} SIN PERP")
        else:
            anual = d.rate.mean() * 3 * 365 * 100
            print(f"{s:12s} {len(d):6,d} pagos | medio {d.rate.mean()*100:+.5f}% "
                  f"| anualizado {anual:+.2f}%")
