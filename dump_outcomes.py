"""Dump screener_outcomes a JSON local para analizar offline."""
import json
import requests

SUPABASE_URL = "https://ecgdswroygkfckkaguxp.supabase.co"
ANON_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImVjZ2Rzd3JveWdrZmNra2FndXhwIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NzM1MTUyNzEsImV4cCI6MjA4OTA5MTI3MX0.N_qJsJWTJaqRHpugzlnRTpoZI84mUoctt3RKmUshIrU"

headers = {
    "apikey": ANON_KEY,
    "Authorization": f"Bearer {ANON_KEY}",
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
