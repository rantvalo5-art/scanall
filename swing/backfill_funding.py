"""
Backfill de funding desde data.binance.vision (GitHub-hosted, sin geo-block) — job separado.

Por qué vision y no fapi: fapi.binance.com está geo-bloqueado (451/403 CloudFront) desde TODO
runner cloud-US (GitHub-hosted) y desde Cloudflare → no se puede fetchear funding live desde la
infra del user (GitHub + cronjob). data.binance.vision (mismo host del spot, NO bloqueado) publica
el funding histórico en dumps MENSUALES → backfill retroactivo. Ver [[reference-fapi-geoblocked-github-runner]].

Caveat: dumps mensuales → una alerta obtiene funding cuando CIERRA su mes (lag ≤1 mes). Para el
forward-test del gate de funding (que se analiza retrospectivamente en un bear) es válido: para
cuando se analiza, los meses ya cerraron.

Diseño (espejo de backfill_7d.py):
- Lee filas swing (timeframe 1h/4h/1d/1w) con funding_rate=null cuyo mes YA cerró (alerted < 1ro del
  mes actual UTC).
- Resuelve spot→perp probando dumps (SYM, 1000+base, 1000000+base) y baja el dump mensual del mes
  de la alerta (+ mes previo para el borde de inicio de mes). Cachea por (perp, mes) dentro del run.
- Join: settled funding del último calc_time <= alerted_at (misma semántica que sim_funding.funding_at).
- PATCH funding_rate. Idempotente (funding_rate=null). Sin perp/sin dump → skip (reintenta; las filas
  se purgan a los 90d). Fail-safe. --dry-run.

ubuntu-latest, geo-free. python 3.11+ (requests). NO toca producción de detección.
"""
import os
import sys
import io
import zipfile
import requests
from datetime import datetime, timezone, timedelta

SUPABASE_URL = "https://ecgdswroygkfckkaguxp.supabase.co"
SUPABASE_KEY = os.environ.get("SUPABASE_KEY", "")
VISION = "https://data.binance.vision/data/futures/um/monthly/fundingRate"

SWING_TFS = "(1h,4h,1d,1w)"
BATCH_SIZE = 300


def _headers(write=False):
    h = {"apikey": SUPABASE_KEY, "Authorization": f"Bearer {SUPABASE_KEY}"}
    if write:
        h["Content-Type"] = "application/json"
        h["Prefer"] = "return=minimal"
    return h


def first_of_month_utc(dt):
    return dt.replace(day=1, hour=0, minute=0, second=0, microsecond=0)


def fetch_pending():
    """Filas swing con funding null cuyo mes ya cerró (alerted < 1ro del mes actual UTC)."""
    cutoff = first_of_month_utc(datetime.now(timezone.utc)).strftime("%Y-%m-%dT%H:%M:%SZ")
    try:
        r = requests.get(
            f"{SUPABASE_URL}/rest/v1/screener_outcomes",
            headers=_headers(),
            params={
                "select": "id,symbol,alerted_at",
                "funding_rate": "is.null",
                "timeframe": f"in.{SWING_TFS}",
                "alerted_at": f"lt.{cutoff}",
                "order": "alerted_at.asc",
                "limit": str(BATCH_SIZE),
            },
            timeout=20,
        )
        r.raise_for_status()
        return r.json()
    except Exception as e:
        print(f"fetch_pending error: {e}")
        return []


_dump_cache = {}   # (perp, "YYYY-MM") -> [(calc_time_ms, rate)] | None
_perp_cache = {}   # spot_sym -> perp | None


def _download_dump(perp, ym):
    key = (perp, ym)
    if key in _dump_cache:
        return _dump_cache[key]
    url = f"{VISION}/{perp}/{perp}-fundingRate-{ym}.zip"
    rows = None
    try:
        r = requests.get(url, timeout=30)
        if r.status_code == 200:
            z = zipfile.ZipFile(io.BytesIO(r.content))
            csv = z.read(z.namelist()[0]).decode().splitlines()
            rows = []
            for line in csv:
                p = line.split(",")
                if len(p) < 3 or not p[0].isdigit():   # saltea header
                    continue
                rows.append((int(p[0]), float(p[2])))
            rows.sort()
    except Exception as e:
        print(f"  dump {perp} {ym} error: {e}")
        rows = None
    _dump_cache[key] = rows
    return rows


def resolve_perp(spot_sym, ym):
    """Devuelve el perp cuyo dump mensual existe (SYM, 1000+base, 1000000+base), o None."""
    if spot_sym in _perp_cache:
        return _perp_cache[spot_sym]
    base = spot_sym[:-4]
    perp = None
    for cand in (spot_sym, f"1000{base}USDT", f"1000000{base}USDT"):
        if _download_dump(cand, ym) is not None:
            perp = cand
            break
    _perp_cache[spot_sym] = perp
    return perp


def funding_at(spot_sym, ts_ms, ym, prev_ym):
    """Settled funding del último calc_time <= ts (carga mes + mes previo para el borde)."""
    perp = resolve_perp(spot_sym, ym)
    if perp is None:
        return None
    rows = list(_download_dump(perp, ym) or [])
    prev = _download_dump(perp, prev_ym)
    if prev:
        rows = prev + rows
    rows.sort()
    best = None
    for ct, rate in rows:
        if ct <= ts_ms:
            best = rate
        else:
            break
    return best


def patch(row_id, funding, dry):
    if dry:
        return
    try:
        r = requests.patch(
            f"{SUPABASE_URL}/rest/v1/screener_outcomes",
            headers=_headers(write=True),
            params={"id": f"eq.{row_id}"},
            json={"funding_rate": funding}, timeout=15,
        )
        r.raise_for_status()
    except Exception as e:
        print(f"  patch id={row_id} error: {e}")


def main(dry=False):
    if not SUPABASE_KEY:
        print("FATAL: falta SUPABASE_KEY en el entorno"); sys.exit(1)
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    print(f"[{now}] backfill_funding{' (DRY-RUN)' if dry else ''} (vision monthly)")
    pending = fetch_pending()
    print(f"  {len(pending)} filas pendientes (swing, funding=null, mes cerrado)")
    done = no_perp = no_rate = 0
    for row in pending:
        dt = datetime.fromisoformat(row["alerted_at"])
        ts_ms = int(dt.timestamp() * 1000)
        ym = dt.strftime("%Y-%m")
        prev = (dt.replace(day=1) - timedelta(days=1)).strftime("%Y-%m")
        fr = funding_at(row["symbol"], ts_ms, ym, prev)
        if fr is None:
            if resolve_perp(row["symbol"], ym) is None:
                no_perp += 1
            else:
                no_rate += 1
            continue
        patch(row["id"], fr, dry)
        done += 1
        print(f"  {row['symbol']} ({row['id']}) {row['alerted_at'][:10]} → funding={fr}" + (" [DRY]" if dry else ""))
    print(f"Done. patcheadas: {done} | sin-perp(spot-only): {no_perp} | sin-rate(borde): {no_rate}")


if __name__ == "__main__":
    main(dry="--dry-run" in sys.argv)
