"""
Backfill de outcomes a 7d para el swing — job separado, NO toca el tracker compartido.

El tracker live (update_outcomes.py, compartido con el daytrader) solo llena checkpoints hasta
24h y marca outcomes_complete=true ahí. El payoff del swing madura a 7d, así que este job RELLENA
columnas separadas (price_7d / max_high_7d / min_low_7d) usando una bandera propia (complete_7d),
sin tocar la lógica de complete-a-24h del daytrader.

Diseño (espejo de exit_tracker.py: self-contained, su propio cron):
- Lee filas con complete_7d=false que ya cumplieron 7d, de timeframes del swing (1h/4h/1d/1w).
- Baja klines 1h cubriendo alerted→alerted+7d (168 velas, un request) y calcula close@+7d +
  máximo/mínimo de la ventana.
- PATCH: setea las 3 columnas + complete_7d=true. Idempotente vía complete_7d.
- Fail-safe: error de red → skip (reintenta al próximo run). Filas muy viejas sin klines
  (delisted) se marcan complete tras un grace para no reintentar infinito.
- --dry-run: no escribe, solo imprime qué haría.

Prerequisito: columnas creadas (ALTER TABLE ya corrido). Corre con python 3.11+ (solo requests).
"""
import os
import sys
import time
import requests
from datetime import datetime, timezone, timedelta

SUPABASE_URL = "https://ecgdswroygkfckkaguxp.supabase.co"
SUPABASE_KEY = os.environ.get("SUPABASE_KEY", "")
BINANCE_KLINES = "https://data-api.binance.vision/api/v3/klines"

WINDOW_DAYS = 7
SWING_TFS = "(1h,4h,1d,1w)"     # filtro PostgREST; corta el grueso del daytrader (5m/15m)
BATCH_SIZE = 200
GRACE_DAYS = 9                  # tras esto, marcar complete aunque no haya klines (anti-retry infinito)
DAY_MS = 86_400_000


def _headers(write=False):
    h = {"apikey": SUPABASE_KEY, "Authorization": f"Bearer {SUPABASE_KEY}"}
    if write:
        h["Content-Type"] = "application/json"
        h["Prefer"] = "return=minimal"
    return h


def fetch_pending():
    """Filas swing maduras a 7d, sin backfill 7d aún."""
    cutoff = (datetime.now(timezone.utc) - timedelta(days=WINDOW_DAYS)).isoformat()
    try:
        r = requests.get(
            f"{SUPABASE_URL}/rest/v1/screener_outcomes",
            headers=_headers(),
            params={
                "select": "id,symbol,alerted_at,entry_price",
                "complete_7d": "eq.false",
                "timeframe": f"in.{SWING_TFS}",
                "alerted_at": f"lte.{cutoff}",
                "entry_price": "not.is.null",
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


def get_klines_1h(symbol, start_ms, end_ms):
    """Klines 1h en [start,end]. 7d = 168 velas → un request. Devuelve la lista (puede ser []
    si el símbolo está delisted = respuesta vacía GENUINA), o None ante error de red persistente.
    Distinguir None (red, reintentar) de [] (delisted, grace) evita perder datos por timeouts
    transitorios. Retry con backoff ante fallos transitorios."""
    for attempt in range(3):
        try:
            r = requests.get(BINANCE_KLINES, params={
                "symbol": symbol, "interval": "1h",
                "startTime": start_ms, "endTime": end_ms, "limit": 1000,
            }, timeout=20)
            r.raise_for_status()
            return r.json()                 # puede ser [] (delisted) → ≠ None
        except Exception as e:
            if attempt < 2:
                time.sleep(1.5 * (attempt + 1))
                continue
            print(f"  klines {symbol} error de red (reintentar próximo run): {e}")
            return None


def compute_7d(row):
    """Devuelve dict de update (price_7d/max_high_7d/min_low_7d/complete_7d) o None si reintentar."""
    alerted = datetime.fromisoformat(row["alerted_at"])
    t0 = int(alerted.timestamp() * 1000)
    t1 = t0 + (WINDOW_DAYS + 1) * DAY_MS
    kl = get_klines_1h(row["symbol"], t0, t1)
    if kl is None:
        return None  # error de RED → siempre reintentar, nunca grace (no perder el dato)
    if not kl:
        # respuesta VACÍA genuina (delisted): grace para no reintentar infinito
        if datetime.now(timezone.utc) - alerted >= timedelta(days=GRACE_DAYS):
            return {"complete_7d": True}
        return None  # aún joven → reintentar (puede aparecer)
    target = t0 + WINDOW_DAYS * DAY_MS
    win = [k for k in kl if t0 < int(k[0]) <= target]
    if len(win) < 3:
        if datetime.now(timezone.utc) - alerted >= timedelta(days=GRACE_DAYS):
            return {"complete_7d": True}
        return None
    # price_7d = close de la vela que contiene target (o la más cercana posterior dentro de win)
    price_7d = None
    for k in win:
        if int(k[0]) <= target <= int(k[6]):
            price_7d = float(k[4]); break
    if price_7d is None:
        price_7d = float(win[-1][4])
    return {
        "price_7d": price_7d,
        "max_high_7d": max(float(k[2]) for k in win),
        "min_low_7d": min(float(k[3]) for k in win),
        "complete_7d": True,
    }


def patch(row_id, update, dry):
    if dry:
        return
    try:
        r = requests.patch(
            f"{SUPABASE_URL}/rest/v1/screener_outcomes",
            headers=_headers(write=True),
            params={"id": f"eq.{row_id}"},
            json=update, timeout=15,
        )
        r.raise_for_status()
    except Exception as e:
        print(f"  patch id={row_id} error: {e}")


def main(dry=False):
    if not SUPABASE_KEY:
        print("FATAL: falta SUPABASE_KEY en el entorno"); sys.exit(1)
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    print(f"[{now}] backfill_7d{' (DRY-RUN)' if dry else ''} — batch {BATCH_SIZE}")
    pending = fetch_pending()
    print(f"  {len(pending)} filas pendientes (swing, maduras 7d)")
    done = skip = 0
    for row in pending:
        upd = compute_7d(row)
        if upd is None:
            skip += 1; continue
        patch(row["id"], upd, dry)
        done += 1
        if "price_7d" in upd:
            ep = float(row["entry_price"]) if row.get("entry_price") else None
            pct = (upd["price_7d"] / ep - 1) * 100 if ep else 0
            mfe = (upd["max_high_7d"] / ep - 1) * 100 if ep else 0
            print(f"  {row['symbol']} ({row['id']}) → 7d {pct:+.2f}% | MFE {mfe:+.2f}%"
                  + (" [DRY]" if dry else ""))
        else:
            print(f"  {row['symbol']} ({row['id']}) → sin klines, marcado complete (grace)")
    print(f"Done. backfilled: {done}, skip(retry): {skip}")


if __name__ == "__main__":
    main(dry="--dry-run" in sys.argv)
