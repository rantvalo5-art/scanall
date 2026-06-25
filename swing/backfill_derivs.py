"""
Backfill de funding/OI para el swing — corre en SELF-HOSTED (IP no-US).

Por qué: el screener corre en GitHub-hosted ubuntu-latest (IP US) donde fapi.binance.com da
451 (geo-block) → fetch_derivatives_snapshot deja funding/OI null cada run. El spot
(data-api.binance.vision) no se bloquea, solo el Futures. Solución: fetchear funding/OI desde una
IP no-US y patchear las filas recientes. Ver [[reference-fapi-geoblocked-github-runner]].

Diseño (espejo de backfill_7d.py, pero self-hosted):
- Lee filas swing (timeframe 1h/4h/1d/1w) con funding_rate=null, RECIENTES (alerted >= now-WINDOW_H).
  Solo recientes: funding es cadencia 8h (lastFundingRate constante dentro de la ventana), así que el
  valor de AHORA ≈ el del momento de la alerta. Para filas viejas el funding-al-momento ya no es
  recuperable → se dejan null (honesto, no inventar dato).
- premiumIndex bulk (una llamada, da funding de todos los perps + el set activo) + openInterest por
  símbolo. Mapeo spot→perp con prefijo 1000x. Mismo método que screener.fetch_derivatives_snapshot.
- PATCH funding_rate/open_interest. Idempotente (filtro funding_rate=null). Fail-safe, --dry-run.

Cron horario (que cada alerta se patchee dentro de su ventana de funding de 8h). python 3.11+, requests.
"""
import os
import sys
import requests
from datetime import datetime, timezone, timedelta

SUPABASE_URL = "https://ecgdswroygkfckkaguxp.supabase.co"
SUPABASE_KEY = os.environ.get("SUPABASE_KEY", "")
FAPI_URL = "https://fapi.binance.com"

SWING_TFS = "(1h,4h,1d,1w)"
WINDOW_HOURS = 6        # solo alertas de las últimas N horas (funding aún representativo)
BATCH_SIZE = 200


def _headers(write=False):
    h = {"apikey": SUPABASE_KEY, "Authorization": f"Bearer {SUPABASE_KEY}"}
    if write:
        h["Content-Type"] = "application/json"
        h["Prefer"] = "return=minimal"
    return h


def fetch_pending():
    """Filas swing recientes con funding aún sin loguear."""
    cutoff = (datetime.now(timezone.utc) - timedelta(hours=WINDOW_HOURS)).isoformat()
    try:
        r = requests.get(
            f"{SUPABASE_URL}/rest/v1/screener_outcomes",
            headers=_headers(),
            params={
                "select": "id,symbol,alerted_at",
                "funding_rate": "is.null",
                "timeframe": f"in.{SWING_TFS}",
                "alerted_at": f"gte.{cutoff}",
                "order": "alerted_at.desc",
                "limit": str(BATCH_SIZE),
            },
            timeout=20,
        )
        r.raise_for_status()
        return r.json()
    except Exception as e:
        print(f"fetch_pending error: {e}")
        return []


def perp_name(spot_sym, perp_set):
    """spot→perp USDT-M, maneja prefijo 1000x (PEPEUSDT→1000PEPEUSDT)."""
    if spot_sym in perp_set:
        return spot_sym
    base = spot_sym[:-4]
    for pref in ("1000", "1000000"):
        cand = f"{pref}{base}USDT"
        if cand in perp_set:
            return cand
    return None


def fetch_funding_map():
    """{perp: lastFundingRate} + set de perps activos. None ante fallo (geo-block/red)."""
    try:
        r = requests.get(f"{FAPI_URL}/fapi/v1/premiumIndex", timeout=15)
        r.raise_for_status()
        fmap = {d["symbol"]: float(d["lastFundingRate"]) for d in r.json()
                if d.get("lastFundingRate") not in (None, "")}
        return fmap
    except Exception as e:
        print(f"premiumIndex error ({e}) — ¿IP geo-bloqueada? Este run no patchea.")
        return None


def fetch_oi(perp):
    try:
        r = requests.get(f"{FAPI_URL}/fapi/v1/openInterest", params={"symbol": perp}, timeout=15)
        if r.status_code == 200:
            return float(r.json().get("openInterest"))
    except Exception:
        pass
    return None


def patch(row_id, funding, oi, dry):
    if dry:
        return
    try:
        r = requests.patch(
            f"{SUPABASE_URL}/rest/v1/screener_outcomes",
            headers=_headers(write=True),
            params={"id": f"eq.{row_id}"},
            json={"funding_rate": funding, "open_interest": oi}, timeout=15,
        )
        r.raise_for_status()
    except Exception as e:
        print(f"  patch id={row_id} error: {e}")


def main(dry=False):
    if not SUPABASE_KEY:
        print("FATAL: falta SUPABASE_KEY en el entorno"); sys.exit(1)
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    print(f"[{now}] backfill_derivs{' (DRY-RUN)' if dry else ''} — últimas {WINDOW_HOURS}h")
    pending = fetch_pending()
    print(f"  {len(pending)} filas pendientes (swing, funding=null, recientes)")
    if not pending:
        print("Done. Nada que patchear."); return
    fmap = fetch_funding_map()
    if fmap is None:
        print("Done. Sin funding map (geo-block/red) — reintentar próximo run."); return
    perp_set = set(fmap.keys())
    done = no_perp = 0
    for row in pending:
        perp = perp_name(row["symbol"], perp_set)
        if perp is None:
            no_perp += 1; continue        # spot-only sin perp → no hay funding
        fr = fmap.get(perp)
        oi = fetch_oi(perp)
        patch(row["id"], fr, oi, dry)
        done += 1
        print(f"  {row['symbol']} ({row['id']}) → fr={fr} oi={oi}" + (" [DRY]" if dry else ""))
    print(f"Done. patcheadas: {done}, sin-perp(spot-only): {no_perp}")


if __name__ == "__main__":
    main(dry="--dry-run" in sys.argv)
