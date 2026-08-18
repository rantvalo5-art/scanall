"""
ITEM 4.7 — fadear las senales de extension (EXPLOSION y BREAKOUT).

Es lo UNICO de este repo que cruzo su regla de parada escrita y despues
sobrevivio a todo lo que se le tiro encima. Este script re-corre la evaluacion
completa sobre datos frescos, que es como se hace el forward test:

    py -3.13 evaluar.py                 # baja lo que falte y evalua
    py -3.13 evaluar.py --desde 2026-08-17   # solo alertas nuevas

LO QUE FALTA PARA CREERLE: la ventana medida (2026-06-26 -> 08-16) son 51 dias
de UN SOLO regimen bear. Todo lo que brillo en una ventana corta bajista de este
repo murio despues. Hace falta que aguante un tramo alcista.

Las cuatro compuertas son las de la regla escrita en el handoff, mas el
bootstrap de bloques (que mato a las dos hipotesis hermanas: mkt_vol_168 del
banco y el funding extremo del item 4.2).
"""
import argparse
import glob
import json
import os

import numpy as np
import pandas as pd
import requests

HERE = os.path.dirname(os.path.abspath(__file__))
CACHE = os.path.join(HERE, ".cache")
FUNDING = os.path.join(os.path.dirname(HERE), "basis", ".funding_cache")
URL = "https://ecgdswroygkfckkaguxp.supabase.co"
KEY = os.environ.get("SUPABASE_KEY") or (
    "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9."
    "eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImVjZ2Rzd3JveWdrZmNra2FndXhwIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NzM1MTUyNzEsImV4cCI6MjA4OTA5MTI3MX0."
    "N_qJsJWTJaqRHpugzlnRTpoZI84mUoctt3RKmUshIrU")

EXTENSION = ["EXPLOSION", "BREAKOUT"]
COSTO = 0.0040      # 0,09% fee perp ida+vuelta + 0,30% slippage asumido
RNG = np.random.default_rng(11)


def bajar(desde=None):
    """daytrader_outcomes paginado. Cachea; --desde fuerza el tramo nuevo."""
    os.makedirs(CACHE, exist_ok=True)
    p = os.path.join(CACHE, f"dt_{desde or 'all'}.json")
    if os.path.exists(p):
        return pd.DataFrame(json.load(open(p, encoding="utf-8")))
    h = {"apikey": KEY, "Authorization": f"Bearer {KEY}"}
    rows, off = [], 0
    while True:
        params = {"select": "*", "order": "alerted_at.asc",
                  "signal_type": f"in.({','.join(EXTENSION)})"}
        if desde:
            params["alerted_at"] = f"gte.{desde}"
        r = requests.get(f"{URL}/rest/v1/daytrader_outcomes",
                         headers={**h, "Range": f"{off}-{off+999}"},
                         params=params, timeout=60)
        r.raise_for_status()
        b = r.json()
        if not b:
            break
        rows.extend(b)
        if len(b) < 1000:
            break
        off += 1000
    json.dump(rows, open(p, "w", encoding="utf-8"))
    return pd.DataFrame(rows)


def con_perp():
    """Simbolos con perpetuo USDT. Sin perp no hay short: no es opcional."""
    s = set()
    for f in glob.glob(os.path.join(FUNDING, "*.csv")):
        n = os.path.basename(f).split("_")[0]
        s.add(n)
        for pre in ("1000000", "1000"):
            if n.startswith(pre):
                s.add(n[len(pre):])
    return s


def p_bloques(d, col="f", bloque=2, reps=4000):
    """p-valor remuestreando tiras de semanas consecutivas.

    El binomial/t supone alertas independientes y aca no lo son: se amontonan en
    el tiempo. Esta es la prueba que mato a las hipotesis hermanas.
    """
    sem = [g[col].to_numpy() for _, g in d.groupby("week", sort=True)]
    k = len(sem)
    if k < bloque * 2:
        return 1.0, (np.nan, np.nan)
    m = []
    for _ in range(reps):
        ini = RNG.integers(0, k - bloque + 1, size=max(1, k // bloque))
        m.append(np.concatenate([np.concatenate(sem[i:i + bloque]) for i in ini]).mean())
    m = np.array(m)
    return float((m <= 0).mean()), tuple(np.percentile(m, [2.5, 97.5]))


def evaluar(df, horizonte="24h", fill="price_15m", solo_perp=True, costo=COSTO):
    d = df.copy()
    d["alerted_at"] = pd.to_datetime(d["alerted_at"], utc=True, format="mixed")
    d["week"] = d["alerted_at"].dt.tz_localize(None).dt.to_period("W")
    if solo_perp:
        d = d[d.symbol.isin(con_perp())]
    # fadear = shortear: se gana cuando el precio BAJA desde el fill
    d["f"] = -(d[f"price_{horizonte}"] / d[fill] - 1) - costo
    d = d.dropna(subset=["f"])

    ap = d.groupby("symbol").f.sum().sort_values()
    sin3 = d[~d.symbol.isin(ap.tail(3).index)].f
    sin_peor = d[d.symbol != ap.index[0]].f
    w = d.groupby("week").f.agg(["size", "mean"])
    w = w[w["size"] >= 20]
    p, ic = p_bloques(d)

    g = {
        "(a) media > 0": (d.f.mean() > 0, f"{100*d.f.mean():+.3f}%"),
        "(b) sin top-3": (sin3.mean() > 0, f"{100*sin3.mean():+.3f}%"),
        "(c) >=75% semanas": ((w["mean"] > 0).sum() >= np.ceil(0.75 * len(w)),
                              f"{(w['mean']>0).sum()}/{len(w)}"),
        "(d) sin el peor simbolo": (sin_peor.mean() > 0, f"{100*sin_peor.mean():+.3f}%"),
        "(e) bloques: IC no cruza 0": (ic[0] > 0,
                                       f"p={p:.4f} IC[{100*ic[0]:+.2f},{100*ic[1]:+.2f}]"),
    }
    print(f"\n--- {horizonte} | fill={fill} | {'solo perps' if solo_perp else 'todas'} "
          f"| costo {100*costo:.2f}% | n={len(d)} ---")
    for k, (ok, v) in g.items():
        print(f"  {k:28s} {v:>26s}   {'OK' if ok else 'FALLA'}")
    todo = all(ok for ok, _ in g.values())
    print(f"  ---> {'SOBREVIVE' if todo else 'LA REGLA DISPARA'}")
    return todo


if __name__ == "__main__":
    ap_ = argparse.ArgumentParser()
    ap_.add_argument("--desde", default=None, help="ISO date; solo alertas posteriores")
    ap_.add_argument("--costo", type=float, default=COSTO)
    a = ap_.parse_args()

    df = bajar(a.desde)
    print("=" * 78)
    print("4.7 — FADEAR LAS SENALES DE EXTENSION")
    print("=" * 78)
    print(f"{len(df)} alertas  |  {df.signal_type.value_counts().to_dict()}")
    if len(df):
        print(f"ventana {df.alerted_at.min()[:10]} -> {df.alerted_at.max()[:10]}")
    print("\nCONDICION REALISTA (fill 15m despues de la alerta, solo simbolos con perp):")
    for h in ("4h", "24h"):
        evaluar(df, h, "price_15m", True, a.costo)
    print("\nreferencia optimista (fill al precio de la alerta, todos los simbolos):")
    for h in ("4h", "24h"):
        evaluar(df, h, "entry_price", False, a.costo)
    print("\n" + "=" * 78)
    print("RECORDATORIO: 51 dias de un solo regimen bear. Lo que falta no es")
    print("otro test estadistico — es un tramo ALCISTA. Correr esto de nuevo")
    print("cuando haya 4+ semanas nuevas, sobre todo si el mercado se dio vuelta.")
    print("=" * 78)
