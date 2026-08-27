"""MEDIR — el forward test del radar, contra datos que nadie miro al construirlo.

Todo lo que promete `radar.py` se midio sobre historia (2021-10 -> 2026-07). Esto lo
mide sobre lo que el radar dijo EN VIVO: lee `radar_runs` de Supabase, reconstruye lo que
efectivamente paso con velas posteriores, y compara contra los numeros preregistrados.

    py -3.13 -u medir.py                # todo lo que haya
    py -3.13 -u medir.py --desde 2026-09-01

Lo que se compara, y esta fijado ANTES de que existan datos:

    spread     +1,008 ATR base   (el numero medido sobre 251 semanas)
    multiplo    1,15x            (camino del top-8 / camino del universo)
    tasa        61,3%            (veces que la elegida supera la mediana de su barra)
    linea base  49,5%

REGLA DE PARADA, escrita ahora:

  - Con menos de 8 SEMANAS de corridas no se concluye nada. La semana es la unidad
    independiente, no la corrida: 60 corridas de 60 dias consecutivos no son 60 datos.
  - Si a las 8 semanas el spread es <= 0, el radar no replico y se apaga.
  - Si esta entre 0 y +0,5 (la mitad de lo medido), sigue vivo pero se reporta como
    "replica debil": la primera medicion de cualquier cosa exagera, porque se encontro
    mirando, y lo que se encuentra mirando es la parte alta del ruido.
  - No se toca `n_surge` ni `k` por lo que salga aca. Ajustar el screener con el
    resultado del forward test convierte el out-of-sample en in-sample y no queda
    ninguna ventana limpia.
"""
import argparse
import os
import sys
import time
from concurrent.futures import ThreadPoolExecutor

import numpy as np
import pandas as pd
import requests

SUPABASE_URL = "https://ecgdswroygkfckkaguxp.supabase.co"
SPOT = "https://api.binance.com"
TABLA = "radar_runs"
H = 24                      # horizonte, en horas — el mismo que se valido

PRE_SPREAD, PRE_MULT, PRE_TASA, PRE_BASE = 1.008, 1.15, 0.613, 0.495
SEM_MIN = 8


def bajar(desde):
    key = os.environ.get("SUPABASE_KEY")
    if not key:
        print("FATAL: falta SUPABASE_KEY", file=sys.stderr); sys.exit(1)
    h = {"apikey": key, "Authorization": f"Bearer {key}"}
    filas, off = [], 0
    while True:
        r = requests.get(f"{SUPABASE_URL}/rest/v1/{TABLA}", headers=h, timeout=30,
                         params={"select": "*", "run_at": f"gte.{desde}",
                                 "order": "run_at.asc", "limit": 1000,
                                 "offset": off})
        r.raise_for_status()
        d = r.json()
        filas += d
        if len(d) < 1000:
            break
        off += 1000
    return pd.DataFrame(filas)


def camino(args):
    """Camino real (maximo - minimo) en las H horas POSTERIORES a la corrida."""
    sym, t_ms, precio = args
    d = None
    for _ in range(3):
        try:
            r = requests.get(f"{SPOT}/api/v3/klines", timeout=20,
                             params={"symbol": sym, "interval": "1h",
                                     "startTime": t_ms, "limit": H + 2})
            if r.status_code == 200:
                d = r.json()
                break
        except Exception:
            time.sleep(1)
    if not d or len(d) < H:
        return np.nan
    hi = max(float(x[2]) for x in d[:H])
    lo = min(float(x[3]) for x in d[:H])
    return (hi - lo) / precio


def main():
    ap = argparse.ArgumentParser(description="Forward test del radar")
    ap.add_argument("--desde", default="2020-01-01")
    ap.add_argument("--out", default=None)
    a = ap.parse_args()

    D = bajar(a.desde)
    if D.empty:
        print("todavia no hay corridas guardadas."); return
    D["run_at"] = pd.to_datetime(D["run_at"], utc=True, format="mixed")
    D["t_ms"] = D["run_at"].astype("int64") // 10**6

    # solo corridas con horizonte COMPLETO: truncar sesgaria hacia lo que ya se movio
    corte = pd.Timestamp.utcnow() - pd.Timedelta(hours=H + 1)
    D = D[D["run_at"] <= corte]
    if D.empty:
        print("hay corridas, pero ninguna cumplio todavia las 24h de horizonte."); return

    print(f"corridas: {D.run_at.nunique()} | filas: {len(D):,} | "
          f"{D.run_at.min():%Y-%m-%d} -> {D.run_at.max():%Y-%m-%d}", flush=True)

    with ThreadPoolExecutor(12) as ex:
        D["camino"] = list(ex.map(camino, zip(D.symbol, D.t_ms, D.precio)))
    D = D[D["camino"].notna() & (D["atr_base"] > 0)]
    D["y"] = D["camino"] / D["atr_base"]

    # el estadistico validado: top-k menos el universo DE LA MISMA CORRIDA
    g = D.groupby("run_at")
    spread = (g.apply(lambda x: x.loc[x.en_top, "y"].mean() - x["y"].mean(),
                      include_groups=False).dropna())
    D["semana"] = D["run_at"].dt.strftime("%G-W%V")
    sem = spread.groupby(D.groupby("run_at")["semana"].first().reindex(spread.index)).mean()

    top, uni = D[D.en_top], D
    mult = top["camino"].median() / uni["camino"].median()
    med_barra = g["camino"].median()
    j = top.join(med_barra.rename("med"), on="run_at")
    tasa = float((j["camino"] > j["med"]).mean())
    jb = uni.join(med_barra.rename("med"), on="run_at")
    base = float((jb["camino"] > jb["med"]).mean())

    print("\n" + "=" * 64)
    print(f"{'':22s}{'medido antes':>16s}{'EN VIVO':>14s}")
    print("=" * 64)
    print(f"{'spread (ATR base)':22s}{PRE_SPREAD:>+16.3f}{sem.mean():>+14.3f}")
    print(f"{'multiplo de camino':22s}{PRE_MULT:>15.2f}x{mult:>13.2f}x")
    print(f"{'tasa de acierto':22s}{100*PRE_TASA:>15.1f}%{100*tasa:>13.1f}%")
    print(f"{'linea base':22s}{100*PRE_BASE:>15.1f}%{100*base:>13.1f}%")
    print(f"{'semanas':22s}{251:>16d}{len(sem):>14d}")
    print(f"{'corridas > 0':22s}{'':>16s}{100*(spread > 0).mean():>13.0f}%")

    print("\n" + "-" * 64)
    if len(sem) < SEM_MIN:
        print(f"AUN NO CONCLUYE: {len(sem)} de {SEM_MIN} semanas minimas.")
        print("La unidad independiente es la SEMANA, no la corrida.")
    elif sem.mean() <= 0:
        print("NO REPLICO. Por la regla de parada, el radar se apaga.")
    elif sem.mean() < PRE_SPREAD / 2:
        print(f"REPLICA DEBIL: {sem.mean():+.3f} contra {PRE_SPREAD:+.3f} preregistrado.")
        print("Esperable — la primera medicion de cualquier cosa exagera. Sigue vivo.")
    else:
        print(f"REPLICA: {sem.mean():+.3f} contra {PRE_SPREAD:+.3f} preregistrado.")
    print("\nNO ajustar `n_surge` ni `k` por este resultado: eso convierte el")
    print("out-of-sample en in-sample y no queda ninguna ventana limpia.")

    if a.out:
        D.to_csv(a.out, index=False)
        print(f"\ndetalle -> {a.out}")


if __name__ == "__main__":
    main()
