"""Dump screener_outcomes a JSON local para analizar offline.

Requiere SUPABASE_KEY en el entorno (anon o service_role — anon alcanza para SELECT).
En PowerShell: $env:SUPABASE_KEY = "eyJ..."
"""
import argparse
import json
import os
import sys

import requests

ap = argparse.ArgumentParser(description="Volcar una tabla de outcomes a JSON")
ap.add_argument("--tabla", default="daytrader_outcomes",
                help="daytrader_outcomes (root) o screener_outcomes (swing)")
ap.add_argument("--out", default=None, help="por defecto <tabla>_dump.json")
args = ap.parse_args()
SALIDA = args.out or ("outcomes_dump.json" if args.tabla == "daytrader_outcomes"
                      else f"{args.tabla}_dump.json")

SUPABASE_URL = "https://ecgdswroygkfckkaguxp.supabase.co"
SUPABASE_KEY = os.environ.get("SUPABASE_KEY") or os.environ.get("SUPABASE_ANON_KEY")
if not SUPABASE_KEY:
    sys.exit("FATAL: definí SUPABASE_KEY (o SUPABASE_ANON_KEY) en el entorno antes de correr.")

headers = {
    "apikey": SUPABASE_KEY,
    "Authorization": f"Bearer {SUPABASE_KEY}",
}

all_rows = []
offset = 0
page_size = 1000

while True:
    r = requests.get(
        f"{SUPABASE_URL}/rest/v1/{args.tabla}",
        headers={**headers, "Range": f"{offset}-{offset + page_size - 1}"},
        params={"select": "*", "order": "alerted_at.desc"},
        timeout=30,
    )
    r.raise_for_status()
    rows = r.json()
    if not rows:
        break
    all_rows.extend(rows)
    print(f"  fetched {len(rows)} (total {len(all_rows)})")
    if len(rows) < page_size:
        break
    offset += page_size

with open(SALIDA, "w", encoding="utf-8") as f:
    json.dump(all_rows, f, ensure_ascii=False, indent=2)

print(f"\n✓ Guardado outcomes_dump.json — {len(all_rows)} filas")
