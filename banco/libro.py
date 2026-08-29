"""
LIBRO - spread y profundidad REALES desde el order book. Fase 0 del item 4.3.

Por que no se estima con OHLC. Se probo (`costos.py`) y no funciona a esta
resolucion: el rango high-low de una hora de BTCUSDT son ~49 bps contra un spread
real de ~1 bp, asi que Corwin-Schultz mide volatilidad, no spread — da 8,4 bps en
1h y 42,9 en 1d para BTCUSDT, y sale PLANO entre cuartiles de volumen (0,21% /
0,24% / 0,23% / 0,21%). Roll queda indefinido en ETH y SOL (autocovarianza
positiva). Peor todavia: el piso de ruido de esos estimadores ESCALA CON LA
VOLATILIDAD, y la cola iliquida es mas volatil — o sea que el "spread" subiria en
la cola por ser volatil y no por ser ilquida, que es exactamente el artefacto que
el item 4.3 tiene que evitar.

Que hace en cambio. Mide el libro de verdad con `/api/v3/depth` y CAMINA el libro
para una orden de tamano dado. Eso da el costo real de cruzar, no una proxy:

    costo_roundtrip = 2*fee + (slippage_compra + slippage_venta)

donde el slippage sale de consumir niveles del libro hasta llenar la orden.

LIMITACION, declarada de entrada: el libro es de AHORA, no del pasado. Aplicar la
liquidez de hoy a una ventana historica es un supuesto, y la direccion del sesgo
no es obvia (un par pudo ser mas liquido antes, o menos). Por eso se muestrea
varias veces y se reporta la dispersion: si el spread de un par se mueve mucho
entre snapshots, su costo no es un numero confiable.

    py -3.13 -u libro.py --top 600 --orden 1000 --reps 3
"""
import argparse
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

import numpy as np
import pandas as pd

from klines import API, _get

# taker por lado, en %. El de futuros es la MITAD, y esa es justamente la mitad del
# caso de 4.2 que la corrida 5 midio en +0,077 ATR sobre 140 brazos: mueve todo por
# igual. Lo que NO estaba medido -y es lo que este archivo agrega- es el otro sumando:
# spread y slippage del libro del PERPETUO.
FEE = {"spot": 0.10, "fut": 0.05}


def universo_por_volumen(top=600, min_usd=50_000, mercado="spot"):
    """Pares USDT ordenados por volumen en USD de las ultimas 24h.

    `mercado="fut"` devuelve PERPETUOS USDT-margen en vez de spot.
    """
    base, pref = API[mercado]
    tick = _get(f"{base}{pref}/ticker/24hr")
    if not tick:
        raise RuntimeError("no se pudo bajar el ticker de 24h")
    info = _get(f"{base}{pref}/exchangeInfo")
    if info and mercado == "fut":
        vivos = {s["symbol"] for s in info["symbols"]
                 if s["status"] == "TRADING" and s.get("quoteAsset") == "USDT"
                 and s.get("contractType") == "PERPETUAL"}
    elif info:
        vivos = {s["symbol"] for s in info["symbols"]
                 if s["status"] == "TRADING" and s["quoteAsset"] == "USDT"}
    else:
        vivos = None
    filas = []
    for t in tick:
        s = t["symbol"]
        if not s.endswith("USDT"):
            continue
        if vivos is not None and s not in vivos:
            continue
        qv = float(t.get("quoteVolume", 0))
        if qv < min_usd:
            continue
        filas.append((s, qv))
    R = pd.DataFrame(filas, columns=["sym", "qv24"]).sort_values("qv24", ascending=False)
    R["rank"] = np.arange(1, len(R) + 1)
    return R.head(top).reset_index(drop=True)


def _caminar(niveles, usd):
    """Consume el libro hasta llenar `usd`. Devuelve el precio promedio pagado.

    Si el libro no alcanza para llenar la orden, devuelve NaN: eso NO es costo
    infinito, es 'no se puede operar ese tamano', y hay que reportarlo aparte.
    """
    resta, costo, base = usd, 0.0, 0.0
    for p, q in niveles:
        p, q = float(p), float(q)
        disp = p * q
        if disp <= 0:
            continue
        toma = min(resta, disp)
        base += toma / p
        costo += toma
        resta -= toma
        if resta <= 0:
            break
    if resta > 0 or base <= 0:
        return np.nan
    return costo / base


def medir(sym, usd, limite=100, mercado="spot"):
    """Spread y slippage de ida y vuelta para una orden de `usd` dolares."""
    base, pref = API[mercado]
    d = _get(f"{base}{pref}/depth", {"symbol": sym, "limit": limite})
    if not d or not d.get("bids") or not d.get("asks"):
        return None
    bid = float(d["bids"][0][0])
    ask = float(d["asks"][0][0])
    if bid <= 0 or ask <= 0 or ask < bid:
        return None
    mid = (bid + ask) / 2
    spread = (ask - bid) / mid * 100                    # spread cotizado, %
    p_compra = _caminar(d["asks"], usd)                 # comprar consume asks
    p_venta = _caminar(d["bids"], usd)                  # vender consume bids
    slip = np.nan
    if np.isfinite(p_compra) and np.isfinite(p_venta):
        slip = (p_compra / mid - 1) * 100 + (1 - p_venta / mid) * 100
    prof_ask = sum(float(p) * float(q) for p, q in d["asks"])
    prof_bid = sum(float(p) * float(q) for p, q in d["bids"])
    fee = FEE[mercado]
    return dict(sym=sym, bid=bid, ask=ask, spread=spread, slip=slip,
                prof_ask=prof_ask, prof_bid=prof_bid,
                costo=2 * fee + slip if np.isfinite(slip) else np.nan)


def snapshot(syms, usd, workers=8, mercado="spot"):
    filas = []
    with ThreadPoolExecutor(max_workers=workers) as ex:
        futs = {ex.submit(medir, s, usd, 100, mercado): s for s in syms}
        for i, f in enumerate(as_completed(futs), 1):
            r = f.result()
            if r:
                filas.append(r)
            if i % 100 == 0:
                print(f"    {i}/{len(syms)}...", flush=True)
    return pd.DataFrame(filas).set_index("sym")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--top", type=int, default=600)
    ap.add_argument("--orden", type=float, default=1000.0)
    ap.add_argument("--reps", type=int, default=3)
    ap.add_argument("--espera", type=float, default=20.0)
    ap.add_argument("--csv", default="libro.csv")
    ap.add_argument("--mercado", default="spot", choices=["spot", "fut"])
    a = ap.parse_args()

    U = universo_por_volumen(a.top, mercado=a.mercado)
    print(f"universo: {len(U)} pares USDT {'PERP' if a.mercado == 'fut' else 'spot'} "
          f"vivos, ordenados por volumen 24h | fee {FEE[a.mercado]:.2f}%/lado")
    print(f"  rank 1   : {U.iloc[0]['sym']:14s} ${U.iloc[0]['qv24']:,.0f}")
    print(f"  rank 200 : {U.iloc[199]['sym']:14s} ${U.iloc[199]['qv24']:,.0f}"
          if len(U) > 200 else "")
    print(f"  rank {len(U):<4d}: {U.iloc[-1]['sym']:14s} ${U.iloc[-1]['qv24']:,.0f}")

    reps = []
    for r in range(a.reps):
        print(f"\n  snapshot {r+1}/{a.reps} (orden ${a.orden:,.0f})...", flush=True)
        S = snapshot(U.sym.tolist(), a.orden, mercado=a.mercado)
        S["rep"] = r
        reps.append(S)
        if r < a.reps - 1:
            time.sleep(a.espera)

    A = pd.concat(reps)
    # mediana entre snapshots + dispersion (si un par se mueve mucho, no es confiable)
    G = A.groupby(level=0).agg(
        spread=("spread", "median"), spread_sd=("spread", "std"),
        slip=("slip", "median"), costo=("costo", "median"),
        prof_ask=("prof_ask", "median"), n_ok=("costo", "count"))
    G = G.join(U.set_index("sym")[["qv24", "rank"]]).sort_values("rank")
    G.to_csv(a.csv)

    print("\n" + "=" * 70)
    print(f"COSTO ROUND-TRIP TAKER REAL, orden de ${a.orden:,.0f}")
    print("=" * 70)
    banda = pd.cut(G["rank"], [0, 50, 200, 400, 600, 10_000],
                   labels=["1-50", "51-200", "201-400", "401-600", "600+"])
    t = G.groupby(banda, observed=True).agg(
        pares=("costo", "size"), spread=("spread", "median"),
        slip=("slip", "median"), costo=("costo", "median"),
        no_llena=("slip", lambda s: float(s.isna().mean() * 100)))
    print(t.to_string(float_format=lambda x: f"{x:9.3f}"))
    print("\n  'no_llena' = % de pares donde el libro no alcanza para la orden.")
    print(f"\n  el banco asume COSTO_PCT = 0,200% para TODOS.")
    for lo, hi, nom in [(1, 200, "top-200"), (201, 600, "cola 201-600")]:
        sub = G[(G["rank"] >= lo) & (G["rank"] <= hi)]
        if len(sub):
            print(f"  {nom:14s}: costo mediano {sub.costo.median():.3f}%  "
                  f"-> subestima por {sub.costo.median()-0.20:+.3f} pp")
    print(f"\n-> {a.csv}")


if __name__ == "__main__":
    main()
