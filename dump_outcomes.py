"""Dump screener_outcomes a JSON local para analizar offline.

Requiere SUPABASE_KEY en el entorno (anon o service_role — anon alcanza para SELECT).
En PowerShell: $env:SUPABASE_KEY = "eyJ..."
"""
import json
import os
import sys
import requests

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
        f"{SUPABASE_URL}/rest/v1/screener_outcomes",
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

with open("outcomes_dump.json", "w", encoding="utf-8") as f:
    json.dump(all_rows, f, ensure_ascii=False, indent=2)

print(f"\n✓ Guardado outcomes_dump.json — {len(all_rows)} filas")
