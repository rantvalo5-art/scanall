"""
ARCHIVAR — saca las alertas de Supabase a disco antes de que la purga las borre.

POR QUE URGE. `update_outcomes.py` corre todas las noches y borra todo lo que tenga mas
de `outcomes.RETENTION_DAYS` = **90 dias**. Eso choca de frente con el calendario del
handoff de 4.7:

    2026-10-19 (9 sem)   -> OK, cae adentro
    2026-12-21 (18 sem)  -> ago-20 a sep-22 YA borrado
    2027-12-13 (69 sem)  -> IMPOSIBLE, la tabla nunca tiene mas de ~13 semanas

O sea que la confirmacion larga —la que el handoff llama "la fila realista"— no podia
ocurrir. Archivando, la retencion deja de importar.

Y ahora urge de verdad: el tramo alcista de BTC arranco el 2026-08-17 (+20% en 7 dias),
que es exactamente el regimen que a 4.7 le faltaba. Esas semanas se borran a mediados
de noviembre.

Es INCREMENTAL: guarda un parquet/csv por tabla y solo pide lo que falta.

    py -3.13 archivar_outcomes.py
    py -3.13 archivar_outcomes.py --tabla screener_outcomes
"""
import argparse
import json
import os
from datetime import datetime, timezone

import pandas as pd
import requests

HERE = os.path.dirname(os.path.abspath(__file__))
DEST = os.path.join(HERE, "archivo_outcomes")
URL = "https://ecgdswroygkfckkaguxp.supabase.co"
KEY = os.environ.get("SUPABASE_KEY") or (
    "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9."
    "eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImVjZ2Rzd3JveWdrZmNra2FndXhwIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NzM1MTUyNzEsImV4cCI6MjA4OTA5MTI3MX0."
    "N_qJsJWTJaqRHpugzlnRTpoZI84mUoctt3RKmUshIrU")


def archivar(tabla):
    os.makedirs(DEST, exist_ok=True)
    p = os.path.join(DEST, f"{tabla}.csv")
    viejo = pd.read_csv(p) if os.path.exists(p) else pd.DataFrame()
    desde = None
    if not viejo.empty and "alerted_at" in viejo:
        desde = str(viejo["alerted_at"].max())
        print(f"  archivo existente: {len(viejo):,} filas, hasta {desde[:19]}")

    h = {"apikey": KEY, "Authorization": f"Bearer {KEY}"}
    filas, off = [], 0
    while True:
        params = {"select": "*", "order": "alerted_at.asc"}
        if desde:
            params["alerted_at"] = f"gt.{desde}"
        r = requests.get(f"{URL}/rest/v1/{tabla}", headers={**h, "Range": f"{off}-{off+999}"},
                         params=params, timeout=60)
        if r.status_code >= 400:
            print(f"  ERROR {r.status_code}: {r.text[:120]}")
            return
        b = r.json()
        if not b:
            break
        filas.extend(b)
        print(f"    +{len(b)} (total nuevo {len(filas):,})", flush=True)
        if len(b) < 1000:
            break
        off += 1000

    nuevo = pd.DataFrame(filas)
    D = pd.concat([viejo, nuevo], ignore_index=True) if not viejo.empty else nuevo
    if D.empty:
        print("  nada que guardar.")
        return
    if "id" in D:
        D = D.drop_duplicates("id")
    D = D.sort_values("alerted_at").reset_index(drop=True)
    D.to_csv(p, index=False)
    print(f"  GUARDADO {p}")
    print(f"  {len(D):,} filas | {D.alerted_at.min()[:10]} -> {D.alerted_at.max()[:10]} "
          f"| +{len(nuevo):,} nuevas")


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--tabla", nargs="+",
                    default=["daytrader_outcomes", "screener_outcomes"])
    a = ap.parse_args()
    print(f"archivando a {DEST}  ({datetime.now(timezone.utc):%Y-%m-%d %H:%M} UTC)\n")
    for t in a.tabla:
        print(f"[{t}]")
        archivar(t)
        print()
