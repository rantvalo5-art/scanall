import argparse
import json
import os
import sys
import requests
from datetime import datetime, timezone, timedelta
from collections import Counter

SUPABASE_URL = "https://ecgdswroygkfckkaguxp.supabase.co"
SUPABASE_KEY = os.environ.get("SUPABASE_KEY") or os.environ.get("SUPABASE_ANON_KEY")
if not SUPABASE_KEY:
    sys.exit("FATAL: definí SUPABASE_KEY")

parser = argparse.ArgumentParser()
parser.add_argument("--days",    type=int,   default=5,                   help="Ventana en días (default 5)")
parser.add_argument("--buckets", nargs="+",  default=["BEST", "STRONG"],  help="Buckets a incluir (default: BEST STRONG)")
parser.add_argument("--out",     default=None,                            help="Archivo de salida (default: outcomes_Nd.json)")
args = parser.parse_args()

out_file = args.out or f"outcomes_{args.days}d.json"
cutoff   = (datetime.now(timezone.utc) - timedelta(days=args.days)).isoformat()
headers  = {"apikey": SUPABASE_KEY, "Authorization": f"Bearer {SUPABASE_KEY}"}

# Armo el filtro de bucket
if len(args.buckets) == 1:
    bucket_filter = {"bucket": f"eq.{args.buckets[0]}"}
else:
    bucket_filter = {"bucket": f"in.({','.join(args.buckets)})"}

all_rows = []
offset = 0
while True:
    r = requests.get(
        f"{SUPABASE_URL}/rest/v1/screener_outcomes",
        headers={**headers, "Range": f"{offset}-{offset+999}"},
        params={
            "select": "*",
            **bucket_filter,
            "alerted_at": f"gte.{cutoff}",
            "order": "alerted_at.desc",
        },
        timeout=30,
    )
    r.raise_for_status()
    rows = r.json()
    if not rows:
        break
    all_rows.extend(rows)
    if len(rows) < 1000:
        break
    offset += 1000

with open(out_file, "w", encoding="utf-8") as f:
    json.dump(all_rows, f, ensure_ascii=False, indent=2)

by_bucket = Counter(r.get("bucket") for r in all_rows)
print(f"FETCHED {len(all_rows)} rows desde {cutoff[:16]}  →  {out_file}")
for b, n in sorted(by_bucket.items()):
    print(f"  {b}: {n}")
