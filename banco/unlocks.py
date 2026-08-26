"""
UNLOCKS — calendarios de vesting como fuente de eventos, no de correlaciones.

POR QUE ESTA FAMILIA ES DISTINTA. Todo lo que este repo cerro son transformaciones de la
serie de precios: momentum, volatilidad, rango, volumen, forma de vela, flujo taker. El
techo condicional y el techo oraculo dicen que en esa serie no queda informacion. Un
desbloqueo de tokens **no sale del precio**: es un shock de OFERTA, con fecha publicada
meses antes y mecanismo causal explicito. Es lo unico disponible que no comparte causa de
muerte con lo ya muerto.

FUENTE. `defillama-datasets.llama.fi/emissions/{slug}` (la CDN sigue abierta; el endpoint
`api.llama.fi/emissions` paso a plan pago y devuelve 402). De cada protocolo se usan:
  metadata.events[]        eventos discretos: timestamp, noOfTokens, category, unlockType
  documentedData.data[]    curva diaria acumulada por categoria -> circulante aproximado
  supplyMetrics.maxSupply  para normalizar

LOS DOS SESGOS, declarados y no resueltos:

1. **No es point-in-time.** El calendario es el snapshot de HOY. Si un proyecto reprogramo
   su vesting, la historia quedo reescrita. Para eventos `cliff` de un vesting documentado
   el riesgo es bajo (son contractuales), pero no es cero y no se puede verificar desde
   aca.
2. **Supervivencia, y va en una direccion conocida.** Solo estan los protocolos que hoy
   existen y que DefiLlama sigue trackeando. Un proyecto que se murio despues de un
   desbloqueo grande no esta en la lista. Eso **censura los peores resultados**, o sea
   sesga en contra de encontrar el efecto negativo que la hipotesis predice. Si igual
   aparece efecto negativo, aparece a pesar del sesgo.

    py -3.13 unlocks.py            # baja y cachea, e imprime el censo
"""
import json
import os
import time

import numpy as np
import pandas as pd
import requests

HERE = os.path.dirname(os.path.abspath(__file__))
CACHE = os.path.join(HERE, ".unlock_cache")
CDN = "https://defillama-datasets.llama.fi"
LISTA = f"{CDN}/emissionsProtocolsList"
DIA = 86400


def _get(url, tries=4):
    for i in range(tries):
        try:
            r = requests.get(url, timeout=120)
            if r.status_code == 200:
                return r.json()
            if r.status_code == 404:
                return None
        except Exception:
            pass
        time.sleep(2 * (i + 1))
    return None


def protocolos():
    """La lista de slugs con calendario documentado."""
    os.makedirs(CACHE, exist_ok=True)
    p = os.path.join(CACHE, "_lista.json")
    if os.path.exists(p):
        return json.load(open(p, encoding="utf-8"))
    d = _get(LISTA) or []
    json.dump(d, open(p, "w", encoding="utf-8"))
    return d


def _extraer(d):
    """Del archivo crudo (1-2 MB) se guarda solo lo que se usa (unos pocos KB)."""
    md = d.get("metadata") or {}
    ev = []
    for e in md.get("events") or []:
        # 'cliff' trae [cantidad]; 'linear' trae [de, a] por periodo — no es un shock
        # puntual, asi que se guarda el tipo y se filtra despues. Algunos vienen con
        # None adentro de la lista, asi que se limpian antes de tomar el ultimo.
        toks = [x for x in (e.get("noOfTokens") or []) if isinstance(x, (int, float))]
        ev.append(dict(t=int(e.get("timestamp") or 0),
                       tokens=float(toks[-1]) if toks else 0.0,
                       cat=e.get("category"), tipo=e.get("unlockType")))

    # curva de circulante: suma de las categorias, dia a dia
    curva = {}
    for serie in (d.get("documentedData") or {}).get("data") or []:
        for p in serie.get("data") or []:
            ts = int(p.get("timestamp") or 0)
            curva[ts] = curva.get(ts, 0.0) + float(p.get("unlocked") or 0.0)

    return dict(gecko=(md.get("token") or "").replace("coingecko:", ""),
                name=d.get("name"),
                max_supply=float((d.get("supplyMetrics") or {}).get("maxSupply") or 0),
                eventos=ev,
                curva=sorted(curva.items()))


def bajar(slug):
    """Un protocolo, cacheado ya extraido."""
    os.makedirs(CACHE, exist_ok=True)
    p = os.path.join(CACHE, f"{slug}.json")
    if os.path.exists(p):
        try:
            return json.load(open(p, encoding="utf-8"))
        except Exception:
            pass
    d = _get(f"{CDN}/emissions/{slug}")
    if not d:
        json.dump({"vacio": True}, open(p, "w", encoding="utf-8"))
        return {"vacio": True}
    out = _extraer(d)
    json.dump(out, open(p, "w", encoding="utf-8"))
    return out


def binance_usdt():
    """{baseAsset: symbol} de los pares USDT que operan hoy."""
    ex = _get("https://api.binance.com/api/v3/exchangeInfo") or {}
    return {s["baseAsset"]: s["symbol"] for s in ex.get("symbols", [])
            if s.get("quoteAsset") == "USDT" and s.get("status") == "TRADING"}


def _gecko_symbols():
    p = os.path.join(CACHE, "_gecko.json")
    if os.path.exists(p):
        return json.load(open(p, encoding="utf-8"))
    cg = _get("https://api.coingecko.com/api/v3/coins/list") or []
    m = {c["id"]: c["symbol"].upper() for c in cg}
    os.makedirs(CACHE, exist_ok=True)
    json.dump(m, open(p, "w", encoding="utf-8"))
    return m


def tabla_eventos(verbose=True):
    """Un DataFrame de eventos de desbloqueo mapeados a pares de Binance.

    Columnas: sym, slug, t (ms), fecha, tokens, cat, tipo, circulante, pct.
    `pct` = tokens del evento / circulante ese dia. Es LA variable: un desbloqueo de
    0,1% no es un shock y uno de 20% si, y el conteo crudo de eventos los mezcla.
    """
    slugs = protocolos()
    id2sym = _gecko_symbols()
    base = binance_usdt()
    filas, sin_par, sin_datos = [], 0, 0

    for i, s in enumerate(slugs, 1):
        d = bajar(s)
        if not d or d.get("vacio"):
            sin_datos += 1
            continue
        tick = id2sym.get(d.get("gecko") or "") or id2sym.get(s)
        sym = base.get(tick) if tick else None
        if not sym:
            sin_par += 1
            continue
        curva = d.get("curva") or []
        if not curva:
            continue
        ct = np.array([c[0] for c in curva], dtype=np.int64)
        cv = np.array([c[1] for c in curva], dtype=float)
        for e in d["eventos"]:
            if not e["t"] or e["tokens"] <= 0:
                continue
            j = int(np.searchsorted(ct, e["t"], side="right")) - 1
            circ = float(cv[j]) if 0 <= j < len(cv) else np.nan
            filas.append(dict(sym=sym, slug=s, t=e["t"] * 1000,
                              fecha=pd.Timestamp(e["t"], unit="s", tz="UTC"),
                              tokens=e["tokens"], cat=e["cat"], tipo=e["tipo"],
                              circulante=circ,
                              pct=e["tokens"] / circ if circ and circ > 0 else np.nan))
        if verbose and i % 50 == 0:
            print(f"  {i}/{len(slugs)} protocolos...", flush=True)

    E = pd.DataFrame(filas)
    if verbose:
        print(f"\n{len(slugs)} protocolos | sin par en Binance {sin_par} | sin datos {sin_datos}")
        print(f"{len(E):,} eventos sobre {E.sym.nunique() if len(E) else 0} pares")
    return E


def main():
    E = tabla_eventos()
    if E.empty:
        print("sin eventos")
        return
    E.to_csv(os.path.join(HERE, "unlocks_eventos.csv"), index=False)

    hoy = pd.Timestamp.utcnow()
    pas = E[(E.fecha < hoy) & (E.fecha >= "2021-01-01")]
    print(f"\npasados y desde 2021: {len(pas):,}  ({pas.sym.nunique()} pares)")
    print(f"cliff: {(pas.tipo == 'cliff').sum():,}  linear: {(pas.tipo == 'linear').sum():,}")

    c = pas[(pas.tipo == "cliff") & pas.pct.notna()]
    print(f"\ncliff con pct calculable: {len(c):,}")
    print("\ntamano del desbloqueo (% del circulante):")
    for lo, hi in ((0, .001), (.001, .005), (.005, .01), (.01, .02),
                   (.02, .05), (.05, .10), (.10, 1e9)):
        n = ((c.pct >= lo) & (c.pct < hi)).sum()
        print(f"  {lo:>6.1%} a {hi:>6.1%}: {n:>6,}")
    print("\npor categoria (solo >= 0,5% del circulante):")
    g = c[c.pct >= 0.005]
    print(g.cat.value_counts().to_string())
    print(f"\n-> eventos 'grandes' (>=0,5%): {len(g):,} sobre {g.sym.nunique()} pares")
    print(f"   por ano: {g.groupby(g.fecha.dt.year).size().to_dict()}")
    print(f"\ntabla -> unlocks_eventos.csv")


if __name__ == "__main__":
    main()
