"""
ITEM 4.4 — vender volatilidad: la cuenta que va ANTES de construir nada.

Regla de parada, textual del handoff:
    "Antes de escribir una linea de codigo: comparar la volatilidad implicita
     contra la realizada en BTC/ETH sobre 2 anos. Si la implicita no supera a la
     realizada por un margen que cubra comisiones Y el riesgo de cola, se cierra
     ahi."

Por que este item era distinto: es el unico lugar donde la habilidad DEMOSTRADA
del screener tiene comprador. Esta medido que predice CUANTO se mueve una moneda
y que no predice PARA QUE LADO. Las opciones pagan exactamente por lo primero.

Implicita = DVOL de Deribit (indice de vol implicita a 30d, el VIX de cripto).
Realizada = vol de los 30 dias SIGUIENTES — contra eso se cobra de verdad.
La diferencia es el variance risk premium.

    py -3.13 iv_rv.py

VEREDICTO (2026-08-16): CERRADO. Ver el handoff. El premio existio y se
compitio: BTC paso de +16,6% a +9,9% de la prima, ETH de +8,4% a +0,5%. En el
regimen reciente BTC rinde ~+7%/ano neto, que cae DENTRO del piso de
stablecoins, con drawdown de 11% y la cola abierta.

Este script queda para RE-CHEQUEAR dentro de un ano: lo unico que reabriria el
item es que el premio relativo (IV/RV) se vuelva a abrir.
"""
import os

import numpy as np
import pandas as pd
import requests

HERE = os.path.dirname(os.path.abspath(__file__))
CACHE = os.path.join(HERE, ".dvol_cache")
INICIO, FIN = "2021-01-01", "2026-08-16"
DIAS = 30                 # horizonte del DVOL
COSTO_PRIMA = 0.05        # round trip en Deribit como fraccion de la prima
SPOT = "https://api.binance.com"


def _ms(d):
    return int(pd.Timestamp(d, tz="UTC").timestamp() * 1000)


def dvol(moneda):
    """Indice de vol implicita a 30d, diario.

    GOTCHA: la API devuelve los ULTIMOS ~1000 puntos de la ventana pedida, no los
    primeros. Pedir 2021->2026 de una sola vez devuelve 2023-11 en adelante y
    cualquier paginacion hacia adelante sale en la primera vuelta. Hay que pedir
    ano por ano. Con la ventana corta faltaba justo el bear 2022, que es donde
    estan las colas que este item necesita medir.
    """
    os.makedirs(CACHE, exist_ok=True)
    p = os.path.join(CACHE, f"dvol_{moneda}.csv")
    if os.path.exists(p):
        return pd.read_csv(p, parse_dates=["fecha"]).set_index("fecha")["iv"]
    filas = []
    for a in range(2021, 2027):
        ini, fin = _ms(f"{a}-01-01"), min(_ms(f"{a+1}-01-01"), _ms(FIN))
        if ini >= fin:
            continue
        r = requests.get(
            "https://www.deribit.com/api/v2/public/get_volatility_index_data",
            params={"currency": moneda, "start_timestamp": ini,
                    "end_timestamp": fin, "resolution": "86400"}, timeout=30)
        r.raise_for_status()
        filas.extend(r.json().get("result", {}).get("data", []))
    s = pd.DataFrame(filas, columns=["t", "o", "h", "l", "c"]).drop_duplicates("t")
    s["fecha"] = pd.to_datetime(s["t"], unit="ms", utc=True).dt.tz_localize(None).dt.normalize()
    s = s.groupby("fecha")["c"].last().rename("iv")
    s.to_frame().reset_index().to_csv(p, index=False)
    return s


def cierres(sym):
    """Cierres diarios de Binance spot, cacheados."""
    os.makedirs(CACHE, exist_ok=True)
    p = os.path.join(CACHE, f"px_{sym}.csv")
    if os.path.exists(p):
        return pd.read_csv(p, parse_dates=["fecha"]).set_index("fecha")["c"]
    filas, cur, fin = [], _ms(INICIO), _ms(FIN)
    while cur < fin:
        r = requests.get(f"{SPOT}/api/v3/klines",
                         params={"symbol": sym, "interval": "1d", "startTime": cur,
                                 "endTime": fin, "limit": 1000}, timeout=30)
        r.raise_for_status()
        d = r.json()
        if not d:
            break
        filas.extend(d)
        nuevo = int(d[-1][0])
        if len(d) < 1000 or nuevo <= cur:
            break
        cur = nuevo + 1
    s = pd.DataFrame([{"t": int(x[0]), "c": float(x[4])} for x in filas]).drop_duplicates("t")
    s["fecha"] = pd.to_datetime(s["t"], unit="ms", utc=True).dt.tz_localize(None).dt.normalize()
    s = s.groupby("fecha")["c"].last()
    s.to_frame().reset_index().to_csv(p, index=False)
    return s


def analizar(moneda, sym):
    iv = dvol(moneda)
    px = cierres(sym)
    rv = np.log(px).diff().rolling(DIAS).std().shift(-DIAS) * np.sqrt(365) * 100

    d = pd.DataFrame({"iv": iv, "rv": rv}).dropna()
    d["vrp"] = d.iv - d.rv
    d["frac"] = d.vrp / d.iv * 100                            # % de la prima cobrada
    d["ratio"] = d.iv / d.rv
    # P&L de vender una straddle ATM 30d, en % del spot. La prima ATM vale
    # 0,7979 x sigma x sqrt(T) x S, asi que la diferencia de vol se traduce igual.
    d["pnl"] = 0.7979 * d.vrp / 100 * np.sqrt(DIAS / 365) * 100

    print(f"\n{'='*84}\n{moneda}   {d.index[0]:%Y-%m} -> {d.index[-1]:%Y-%m}   "
          f"({len(d)} dias, {len(d)/365:.1f} anos)\n{'='*84}")
    print(f"  implicita media {d.iv.mean():.2f}%   realizada media {d.rv.mean():.2f}%   "
          f"premio {d.vrp.mean():+.2f}pp  (positivo {100*(d.vrp>0).mean():.0f}% de los dias)")

    m = d.iloc[::DIAS]
    costo = COSTO_PRIMA * 0.7979 * d.iv.mean() / 100 * np.sqrt(DIAS / 365) * 100
    neto = m.pnl - costo
    print(f"\n  --- 1 straddle ATM por mes, {len(m)} meses NO SOLAPADOS ---")
    print(f"  costo {COSTO_PRIMA:.0%} de la prima = {costo:.2f}% del spot por trade")
    print(f"  BRUTO {12*m.pnl.mean():+6.2f}%/ano    NETO {12*neto.mean():+6.2f}%/ano")
    print(f"  meses negativos {100*(neto<0).mean():.0f}%   PEOR MES {neto.min():+.2f}%")

    eq = neto.cumsum()
    dd = (eq - eq.cummax()).min()
    sin3 = neto.drop(neto.nlargest(3).index)
    print(f"  acumulado {eq.iloc[-1]:+.1f}%  |  DRAWDOWN MAX {dd:.1f}%  |  "
          f"retorno/DD {abs(eq.iloc[-1]/dd):.2f}")
    print(f"  sin los 3 mejores meses: {12*sin3.mean():+.2f}%/ano   "
          f"<- si esto se cae, era concentracion")

    print("\n  NIVEL vs PREMIO — la distincion que decide como leer la caida:")
    print(f"  {'ano':>5s} {'implicita':>10s} {'realizada':>10s} {'IV/RV':>7s} "
          f"{'% de la prima':>14s} {'neto':>9s}")
    for a, g in d.groupby(d.index.year):
        n = neto[neto.index.year == a]
        print(f"  {a:>5d} {g.iv.mean():9.1f}% {g.rv.mean():9.1f}% {g.ratio.median():7.3f} "
              f"{g.frac.mean():13.1f}% {n.sum():+8.2f}%")

    pri, seg = d[d.index.year <= 2022], d[d.index.year >= 2024]
    print(f"\n  2021-2022:  IV/RV {pri.ratio.median():.3f}   premio {pri.frac.mean():+.1f}% "
          f"de la prima")
    print(f"  2024-2026:  IV/RV {seg.ratio.median():.3f}   premio {seg.frac.mean():+.1f}% "
          f"de la prima")
    if seg.ratio.median() < pri.ratio.median() - 0.02:
        print("  -> el premio RELATIVO se comprimio: el edge se COMPITIO, no es")
        print("     que bajo el nivel de vol. Subir el tamano no lo recupera.")
    else:
        print("  -> el premio relativo se mantiene: la caida es de NIVEL y se")
        print("     compensa con tamano. El edge sigue vivo.")

    reciente = neto[neto.index.year >= 2023]
    print(f"\n  REGIMEN RECIENTE (2023->): {12*reciente.mean():+.2f}%/ano neto")
    print("  contra el piso de stablecoins (5-10%/ano, sin drawdown, sin cola):")
    r = 12 * reciente.mean()
    if r <= 10:
        print(f"    {r:+.2f}%/ano NO lo supera con margen. Y falta descontar el delta")
        print("    hedging (~30 rebalanceos por trade, sin contar), vender al bid y no")
        print("    al medio, y el riesgo de liquidacion por margen en un spike.")
        print("    LA REGLA DE PARADA DISPARA.")
    else:
        print(f"    {r:+.2f}%/ano lo supera. Falta descontar hedging y liquidacion.")
    return d


if __name__ == "__main__":
    print("=" * 84)
    print("4.4 — VENDER VOLATILIDAD EN BTC/ETH: implicita contra realizada")
    print("=" * 84)
    for moneda, sym in (("BTC", "BTCUSDT"), ("ETH", "ETHUSDT")):
        analizar(moneda, sym)
    print(f"\n{'='*84}")
    print("Lo que NO esta contado, y todo empuja para el mismo lado:")
    print("  - delta hedging: la formula modela captura de varianza, que requiere")
    print("    cubrir. ~30 rebalanceos por trade en fees y slippage de spot.")
    print("  - el -11% del peor mes es mark-to-market; con margen, un spike te")
    print("    liquida al peor precio y eso no tiene piso.")
    print("  - vender al bid, no al medio.")
    print("  - todo el capital en un solo venue offshore. FTX esta en la muestra.")
    print("=" * 84)
