"""
Job separado de tracking de outcomes — FORK del SWING.

Copia de ../update_outcomes.py pero apunta a `screener_outcomes` (tabla del
swinger) y usa swing/config.json (retención propia, 90d). El day trader raíz usa
daytrader_outcomes y su propio filler; por eso swing NO comparte el del root
(antes lo hacía y mezclaba tabla + config — ver swing/CLAUDE.md).

Lee filas de screener_outcomes con outcomes incompletos y completa los precios
a 15min, 1h, 4h, 24h post-alerta + máximos/mínimos en esas ventanas.

Diseño:
- Corre desde swing.yml (paso "Fill outcomes") tras el scan del swing.
- Una alerta se considera "completa" cuando ya pasaron 24h desde alerted_at.
- Actualizamos todos los checkpoints que ya vencieron en cada run.
- Procesamos en batches para no saturar Binance ni Supabase.
- Auto-purge de filas viejas (>RETENTION_DAYS).
"""

import json
import os
from datetime import datetime, timezone, timedelta
from pathlib import Path

import pandas as pd
import requests

# ── Carga de configuración ────────────────────────────────────────────────────
CONFIG_PATH = Path(__file__).parent / "config.json"
if not CONFIG_PATH.exists():
    raise SystemExit(f"FATAL: config.json no encontrado en {CONFIG_PATH}")

try:
    with open(CONFIG_PATH, "r", encoding="utf-8") as f:
        CONFIG = json.load(f)
except json.JSONDecodeError as e:
    raise SystemExit(f"FATAL: config.json tiene JSON inválido: {e}")

def _cfg(section, key):
    try:
        return CONFIG[section][key]
    except KeyError:
        raise SystemExit(f"FATAL: config.json no tiene {section}.{key}")

OUTCOMES_ENABLED   = _cfg("outcomes", "ENABLED")
BATCH_SIZE         = _cfg("outcomes", "BATCH_SIZE")
RETENTION_DAYS     = _cfg("outcomes", "RETENTION_DAYS")

# ── Variables de entorno ──────────────────────────────────────────────────────
SUPABASE_KEY = os.environ["SUPABASE_KEY"]
SUPABASE_URL = "https://ecgdswroygkfckkaguxp.supabase.co"


def _sb_headers():
    return {
        "apikey": SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}",
        "Content-Type": "application/json",
        "Prefer": "return=minimal",
    }


def fetch_pending_outcomes():
    """Trae alertas que aún no están completas Y que tienen al menos un checkpoint
    sin llenar (price_15m/1h/4h/24h). El filtro OR evita que una alerta procesada
    parcialmente bloquee la cola hasta que cumpla 24h."""
    try:
        r = requests.get(
            f"{SUPABASE_URL}/rest/v1/screener_outcomes",
            headers={**_sb_headers(), "Prefer": ""},
            params={
                "select": "id,alerted_at,symbol,entry_price,price_15m,price_1h,price_4h,price_24h,outcomes_complete",
                "outcomes_complete": "eq.false",
                "or": "(price_15m.is.null,price_1h.is.null,price_4h.is.null,price_24h.is.null)",
                "order": "alerted_at.asc",
                "limit": str(BATCH_SIZE),
            },
            timeout=15,
        )
        r.raise_for_status()
        return r.json()
    except Exception as e:
        print(f"fetch_pending_outcomes error: {e}")
        return []


def fetch_pending_7d():
    """Filas que YA cumplieron 7 dias y todavia no tienen el horizonte largo.

    Va en una consulta APARTE y no mezclada con la de arriba a proposito: aquella
    filtra `outcomes_complete=false`, y una fila se marca completa a las 24h — o sea
    que si el 7d colgara de ese mismo filtro NUNCA se calcularia. Son dos ciclos de
    vida distintos sobre la misma fila.

    El filtro por fecha es lo que evita el trabajo al pedo: sin el, cada corrida
    traeria las ~270 filas que todavia estan madurando y les pediria klines para nada.
    """
    corte = (datetime.now(timezone.utc) - timedelta(days=7)).isoformat()
    try:
        r = requests.get(
            f"{SUPABASE_URL}/rest/v1/screener_outcomes",
            headers={**_sb_headers(), "Prefer": ""},
            params={
                "select": "id,alerted_at,symbol,entry_price,complete_7d",
                "complete_7d": "is.false",
                "alerted_at": f"lt.{corte}",
                "order": "alerted_at.asc",
                "limit": str(BATCH_SIZE),
            },
            timeout=15,
        )
        r.raise_for_status()
        return r.json()
    except Exception as e:
        print(f"fetch_pending_7d error: {e}")
        return []


def compute_7d(row):
    """price_7d / max_high_7d / min_low_7d, con velas de 15m.

    15m y no 1m: 7 dias en 1m son 10.080 velas = 11 requests por fila; en 15m son 672
    = UN request. Para un horizonte de una semana esa resolucion sobra, y la diferencia
    es la que hace que el relleno hacia atras de 90 dias sea viable.
    """
    alerted = datetime.fromisoformat(row["alerted_at"].replace("Z", "+00:00"))
    fin = alerted + timedelta(days=7)
    if datetime.now(timezone.utc) < fin:
        return None
    fin_ms = int(fin.timestamp() * 1000)
    # Se pide un poco MAS ALLA de `fin` a proposito: `price_at` busca la vela que
    # CONTIENE el instante, y si el rango corta justo en el borde esa vela no viene y
    # devuelve None. El margen se descarta despues para el max/min.
    ks = get_klines_range(row["symbol"], int(alerted.timestamp() * 1000),
                          fin_ms + 30 * 60 * 1000, interval="15m")
    if not ks:
        return None
    p7 = price_at(ks, fin_ms)
    if p7 is None:
        return None
    dentro = [k for k in ks if int(k[0]) <= fin_ms]   # el recorrido es de la VENTANA
    if not dentro:
        return None
    return {
        "price_7d": p7,
        "max_high_7d": max(float(k[2]) for k in dentro),
        "min_low_7d": min(float(k[3]) for k in dentro),
        "complete_7d": True,
        "last_updated": datetime.now(timezone.utc).isoformat(),
    }


def get_klines_range(symbol, start_ms, end_ms, interval="1m"):
    """Obtiene klines en un rango específico. interval=1m da el detalle más fino
    para calcular precios exactos en checkpoints y máximos/mínimos intermedios.
    Binance limita a 1000 klines por request — para 24h en 1m son 1440, así que
    en algunos casos hay que paginar."""
    all_klines = []
    cursor = start_ms
    while cursor < end_ms:
        try:
            r = requests.get(
                "https://data-api.binance.vision/api/v3/klines",
                params={
                    "symbol": symbol,
                    "interval": interval,
                    "startTime": cursor,
                    "endTime": end_ms,
                    "limit": 1000,
                },
                timeout=10,
            )
            r.raise_for_status()
            data = r.json()
            if not data:
                break
            all_klines.extend(data)
            # Avanzo al siguiente ms después de la última vela recibida
            last_close_time = int(data[-1][6])
            cursor = last_close_time + 1
            if len(data) < 1000:
                break
        except Exception as e:
            print(f"  get_klines_range {symbol} error: {e}")
            break
    return all_klines


def price_at(klines, target_ms):
    """Devuelve el close de la vela que contiene target_ms, o None si no hay datos."""
    for k in klines:
        open_time = int(k[0])
        close_time = int(k[6])
        if open_time <= target_ms <= close_time:
            return float(k[4])  # close
    # Fallback: la vela más cercana después del target
    for k in klines:
        if int(k[0]) > target_ms:
            return float(k[4])
    return None


def compute_outcomes(row):
    """Calcula los checkpoints que correspondan según cuánto tiempo pasó desde alerted_at."""
    alerted = datetime.fromisoformat(row["alerted_at"].replace("Z", "+00:00"))
    now = datetime.now(timezone.utc)
    elapsed = now - alerted

    alerted_ms = int(alerted.timestamp() * 1000)
    now_ms = int(now.timestamp() * 1000)

    # Solo descargo desde alerted hasta el min(now, alerted+24h) para no traer de más
    end_window_ms = int((alerted + timedelta(hours=24)).timestamp() * 1000)
    end_ms = min(now_ms, end_window_ms)

    klines = get_klines_range(row["symbol"], alerted_ms, end_ms, interval="1m")
    if not klines:
        return None

    update = {}

    # Checkpoints
    checkpoints = [
        ("price_15m", timedelta(minutes=15)),
        ("price_1h",  timedelta(hours=1)),
        ("price_4h",  timedelta(hours=4)),
        ("price_24h", timedelta(hours=24)),
    ]
    for col, delta in checkpoints:
        # Solo lo seteamos si:
        # 1) ya pasó suficiente tiempo
        # 2) todavía no estaba seteado
        target = alerted + delta
        if now < target:
            continue
        if row.get(col) is not None:
            continue
        target_ms = int(target.timestamp() * 1000)
        p = price_at(klines, target_ms)
        if p is not None:
            update[col] = p

    # Máximos/mínimos en ventanas
    # Considera todas las velas hasta now, dentro de las primeras 4h y 24h respectivamente
    four_h_end_ms   = int((alerted + timedelta(hours=4)).timestamp()  * 1000)
    twentyfour_end_ms = int((alerted + timedelta(hours=24)).timestamp() * 1000)

    klines_4h  = [k for k in klines if int(k[0]) <= four_h_end_ms]
    klines_24h = [k for k in klines if int(k[0]) <= twentyfour_end_ms]

    if klines_4h:
        update["max_high_4h"] = max(float(k[2]) for k in klines_4h)
        update["min_low_4h"]  = min(float(k[3]) for k in klines_4h)
    if klines_24h:
        update["max_high_24h"] = max(float(k[2]) for k in klines_24h)
        update["min_low_24h"]  = min(float(k[3]) for k in klines_24h)

    # Completo cuando: (a) los 4 checkpoints están llenos (combinando lo que ya estaba
    # con lo que esta corrida acaba de calcular), o (b) ya pasaron 24h y nos rendimos.
    # Bug fix: antes solo chequeaba (b), lo que dejaba filas con todos los precios llenos
    # pero outcomes_complete=false invisibles para el query (que filtra rows con algún null),
    # y a la vez marcaba complete=true con precios faltantes apenas pasaban 24h.
    price_cols = ["price_15m", "price_1h", "price_4h", "price_24h"]
    final_prices = {c: update.get(c, row.get(c)) for c in price_cols}
    all_filled = all(v is not None for v in final_prices.values())
    if all_filled or elapsed >= timedelta(hours=24):
        update["outcomes_complete"] = True

    update["last_updated"] = now.isoformat()

    return update if update else None


def patch_outcome(row_id, update):
    """PATCH a Supabase para una sola fila."""
    try:
        r = requests.patch(
            f"{SUPABASE_URL}/rest/v1/screener_outcomes",
            headers=_sb_headers(),
            params={"id": f"eq.{row_id}"},
            json=update,
            timeout=10,
        )
        r.raise_for_status()
    except Exception as e:
        print(f"  patch_outcome id={row_id} error: {e}")


def purge_old():
    """Borra filas más viejas que RETENTION_DAYS."""
    purge_before = (datetime.now(timezone.utc) - timedelta(days=RETENTION_DAYS)).isoformat()
    try:
        r = requests.delete(
            f"{SUPABASE_URL}/rest/v1/screener_outcomes",
            headers=_sb_headers(),
            params={"alerted_at": f"lt.{purge_before}"},
            timeout=15,
        )
        r.raise_for_status()
        print(f"  purge: outcomes >{RETENTION_DAYS}d eliminados")
    except Exception as e:
        print(f"  purge error: {e}")


def main():
    if not OUTCOMES_ENABLED:
        print("outcomes.ENABLED = false — skipping")
        return

    now_str = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    print(f"[{now_str}] Update outcomes — batch size {BATCH_SIZE}")

    pending = fetch_pending_outcomes()
    print(f"  {len(pending)} alertas pendientes")

    updated = 0
    completed = 0
    for row in pending:
        update = compute_outcomes(row)
        if not update:
            continue
        patch_outcome(row["id"], update)
        updated += 1
        if update.get("outcomes_complete"):
            completed += 1
        # Log conciso
        diffs = []
        for k in ("price_15m", "price_1h", "price_4h", "price_24h"):
            if k in update:
                p = update[k]
                ep = float(row["entry_price"])
                pct = (p / ep - 1) * 100 if ep else 0
                diffs.append(f"{k}={pct:+.2f}%")
        print(f"  {row['symbol']} ({row['id']}) → {' '.join(diffs) if diffs else 'min/max only'}"
              + (" [COMPLETE]" if update.get("outcomes_complete") else ""))

    # ── horizonte de 7 dias ───────────────────────────────────────────────────
    # El swing opera en 1h/4h/1d/1w y hasta ahora se media hasta 24h — los horizontes
    # del DAYTRADER. `dt_vivo.py --sistema swing` midio los cuatro y los cuatro caen
    # DENTRO del MDE: no es que el swing pierda, es que no se lo puede medir en su
    # propio horizonte porque nadie lo estaba guardando.
    #
    # Las columnas price_7d/max_high_7d/min_low_7d/complete_7d YA EXISTEN en la tabla
    # (estan en las 3.483 filas, todas en NULL). No hace falta ningun ALTER TABLE: solo
    # faltaba el codigo. Y como existen, esto RELLENA HACIA ATRAS los 90 dias que ya
    # hay, asi que el registro a 7d no arranca de cero.
    p7 = fetch_pending_7d()
    print(f"\n  {len(p7)} alertas pendientes de 7d")
    hechas = 0
    for row in p7:
        u = compute_7d(row)
        if not u:
            continue
        patch_outcome(row["id"], u)
        hechas += 1
        ep = float(row["entry_price"]) if row.get("entry_price") else 0
        pct = (u["price_7d"] / ep - 1) * 100 if ep and u.get("price_7d") else 0
        print(f"  {row['symbol']} ({row['id']}) → price_7d={pct:+.2f}% [7D]")

    purge_old()
    print(f"Done. Updated: {updated}, completed: {completed}, 7d: {hechas}")


if __name__ == "__main__":
    main()
