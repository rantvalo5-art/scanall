"""
ON-CHAIN — la unica clase de informacion que el banco nunca toco y que NO sale del precio.

Es la direccion 4.3 de `HANDOFF_SIGUIENTE.md`. El handoff la puso tercera por una razon
concreta: **el cuello es el acceso a los datos**, no el metodo. Glassnode y Nansen son
pagas; las gratis suelen tener granularidad pobre o cubrir cuatro monedas.

Fuente elegida: **CoinMetrics Community API** — gratis, sin API key, historia diaria desde
el genesis de cada cadena. Lo que efectivamente da (medido, no de memoria):

    138 activos con metricas on-chain diarias, de los cuales **48 cotizan en Binance**
    y TODOS tienen historia desde 2021-08 o antes.

Las metricas que se usan son las que NO son precio. Se excluyen a proposito `PriceUSD`,
`ReferenceRate*`, `ROI*`, `CapMrktCurUSD/EstUSD` (precio x oferta) y el volumen reportado:
meter esas seria volver a rankear precio con otro nombre.

    AdrActCnt   direcciones activas por dia      <- la metrica canonica de actividad
    AdrBalCnt   direcciones con saldo > 0        <- cuantos tenedores hay
    TxCnt       transacciones por dia
    TxTfrCnt    transferencias por dia
    IssTotNtv   emision nueva del dia            <- presion de oferta, mecanismo directo
    SplyCur     oferta circulante
    CapMVRVCur  market cap / realized cap        <- HIBRIDA: el numerador es precio (§ del preregistro)

LO QUE NO HAY, y hay que decirlo: **flujos de exchange** (`FlowInExNtv`/`FlowOutExNtv`)
existen para **2 activos** en el tier gratis. O sea que la idea titular de 4.3 —"monedas
saliendo de exchanges = menos oferta vendedora"— **no se puede medir con datos gratis**.
Lo que sigue mide actividad, tenedores y emision, que es otra cosa.

## El lookahead, que aca es el riesgo serio

Una metrica diaria del dia D cubre 00:00-23:59 de D y **no existe hasta que D termina**.
Usarla para entrar durante D es mirar el futuro. CoinMetrics publica
`AssetEODCompletionTime`: el instante en que el dato del dia queda firme (medido: ~3h
despues de que el dia cierra, o sea D+1 ~03:00 UTC).

Por eso `alinear()` **no usa un lag fijo**: une por *cuando el dato estuvo disponible*.
Para una entrada en `t` toma la ultima fila diaria con `completion <= t`. Es exacto,
es verificable, y se adapta solo a los activos que completan mas lento.

    py -3.13 onchain.py     # self-test: cobertura y demora real de publicacion
"""
import json
import os
import time

import numpy as np
import pandas as pd
import requests

HERE = os.path.dirname(os.path.abspath(__file__))
CACHE = os.path.join(HERE, ".onchain_cache")
API = "https://community-api.coinmetrics.io/v4"

# las que NO son precio. `CapMVRVCur` va aparte porque es hibrida (ver preregistro).
METRICAS = ["AdrActCnt", "AdrBalCnt", "TxCnt", "TxTfrCnt", "IssTotNtv", "SplyCur"]
HIBRIDAS = ["CapMVRVCur"]
HORA_MS = 3_600_000


def _get(url, params, tries=5):
    for i in range(tries):
        try:
            r = requests.get(url, params=params, timeout=60)
            if r.status_code == 200:
                return r.json()
            time.sleep(1.5 * (i + 1))
        except Exception:
            time.sleep(1.5 * (i + 1))
    return None


def activos_binance(universo_binance):
    """Los activos con on-chain diario que ademas cotizan como par USDT en Binance.

    `universo_binance`: lista de simbolos tipo BTCUSDT (usar `klines.universe(3000)`).
    Devuelve {ticker_coinmetrics: simbolo_binance}.
    """
    cat = _get(f"{API}/catalog/assets", {})
    if not cat:
        return {}
    mapa = {s[:-4].lower(): s for s in universo_binance if s.endswith("USDT")}
    out = {}
    for a in cat["data"]:
        ms = {m["metric"] for m in a.get("metrics", [])
              if any(f["frequency"] == "1d" for f in m["frequencies"])}
        if "AdrActCnt" in ms and a["asset"] in mapa:
            out[a["asset"]] = mapa[a["asset"]]
    return out


def bajar(assets, start, end, metricas=None):
    """Panel diario de metricas on-chain. Cacheado en disco.

    Trae ademas `AssetEODCompletionTime`, que es lo que hace posible unir sin lookahead.
    """
    metricas = list(metricas or (METRICAS + HIBRIDAS))
    os.makedirs(CACHE, exist_ok=True)
    clave = f"{len(assets)}a_{start}_{end}_{len(metricas)}m"
    p = os.path.join(CACHE, f"{clave}.csv")
    if os.path.exists(p):
        return pd.read_csv(p)

    filas, token = [], None
    pedidas = ",".join(metricas + ["AssetEODCompletionTime"])
    while True:
        params = {"assets": ",".join(assets), "metrics": pedidas, "frequency": "1d",
                  "start_time": start, "end_time": end, "page_size": 10000}
        if token:
            params["next_page_token"] = token
        d = _get(f"{API}/timeseries/asset-metrics", params)
        if not d or not d.get("data"):
            break
        filas.extend(d["data"])
        token = d.get("next_page_token")
        print(f"  on-chain: {len(filas):,} filas...", flush=True)
        if not token:
            break

    if not filas:
        return None
    M = pd.DataFrame(filas)
    M["t_dia"] = (pd.to_datetime(M["time"]).astype("int64") // 10**6)
    M["completo"] = pd.to_numeric(M["AssetEODCompletionTime"], errors="coerce") * 1000
    for c in metricas:
        M[c] = pd.to_numeric(M.get(c), errors="coerce")
    M = M[["asset", "t_dia", "completo"] + metricas].sort_values(["asset", "t_dia"])
    M.to_csv(p, index=False)
    return M


def _features(g, metricas):
    """Transformaciones de una serie diaria, TODAS mirando solo al pasado.

    Las mismas tres formas que la corrida 3 le aplico a los derivados: nivel crudo,
    cambio, y percentil contra la propia historia. El percentil es el comparable ENTRE
    monedas — un nivel crudo rankea 'que cadena es' (bitcoin siempre tiene mas direcciones
    activas que decred) y no 'que esta pasando', que fue justo el sintoma que mato al
    candidato de la corrida 4.
    """
    out = {}
    for c in metricas:
        s = g[c].astype(float)
        out[c] = s
        out[f"{c}_chg7"] = s / s.shift(7) - 1.0
        out[f"{c}_chg30"] = s / s.shift(30) - 1.0
        # z contra la propia historia de un ano, solo pasado
        mu = s.rolling(365, min_periods=90).mean()
        sd = s.rolling(365, min_periods=90).std()
        out[f"{c}_z"] = (s - mu) / sd.replace(0, np.nan)
        # percentil propio: la forma correcta de comparar monedas de escala distinta
        out[f"{c}_pct"] = s.rolling(365, min_periods=90).rank(pct=True)
    return pd.DataFrame(out, index=g.index)


def alinear(claves, M, mapa, metricas=None):
    """Une el panel diario al tablero de `ranking.py` SIN lookahead.

    `claves`: DataFrame con `sym` (BTCUSDT) y `t` (apertura de la barra horaria).
    `mapa`: {ticker_coinmetrics: simbolo_binance}, de `activos_binance`.

    La union es por **momento de publicacion**: para una entrada al cierre de la barra
    `t` (o sea `t + 1h`) se toma la ultima fila diaria cuyo `completo <= t + 1h`. No es
    un lag fijo de N dias: es el dato que REALMENTE existia en ese instante.
    """
    metricas = list(metricas or (METRICAS + HIBRIDAS))
    inv = {v: k for k, v in mapa.items()}
    cols = [c for m in metricas
            for c in (m, f"{m}_chg7", f"{m}_chg30", f"{m}_z", f"{m}_pct")]
    out = pd.DataFrame(index=claves.index, columns=cols, dtype=float)

    for sym, idx in claves.groupby("sym").groups.items():
        a = inv.get(sym)
        if a is None:
            continue
        g = M[M["asset"] == a].sort_values("t_dia").reset_index(drop=True)
        if g.empty:
            continue
        F = _features(g, metricas)
        comp = g["completo"].to_numpy(float)
        # completion time monotono: si algun dia sale desordenado, el maximo acumulado
        # es la garantia conservadora de "esto ya estaba publicado"
        comp = np.maximum.accumulate(np.nan_to_num(comp, nan=np.inf))
        t_ent = claves.loc[idx, "t"].to_numpy(np.int64) + HORA_MS
        pos = np.searchsorted(comp, t_ent, "right") - 1
        ok = pos >= 0
        if not ok.any():
            continue
        vals = F.to_numpy(float)[pos[ok]]
        out.loc[np.asarray(idx)[ok], cols] = vals
    return out


if __name__ == "__main__":
    from klines import universe
    mapa = activos_binance(universe(3000))
    print(f"activos on-chain que cotizan en Binance: {len(mapa)}")
    M = bajar(sorted(mapa)[:5], "2026-08-01", "2026-08-27")
    demora = (M["completo"] - M["t_dia"]) / 3_600_000
    print(f"\ndemora de publicacion (horas desde el inicio del dia que describe):")
    print(f"  mediana {demora.median():.1f}h | p95 {demora.quantile(.95):.1f}h "
          f"| max {demora.max():.1f}h")
    print("  (24h = el dia recien cierra; todo lo que pase de 24 es la demora real)")
    print(f"\ncobertura por metrica:")
    for c in METRICAS + HIBRIDAS:
        print(f"  {c:12s} {M[c].notna().mean():5.1%}")
