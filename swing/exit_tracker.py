"""
Capa de salida (trailing-stop / take-profit) para swing — job separado.

Problema que ataca: el diagnóstico mostró que las alertas BEST tocan +11-20% de MFE a
7d pero devuelven ~30pp (la mitad round-trip a ≤0) porque el sistema solo detecta, no
gestiona la salida. La simulación (sim_exit.py) confirmó que un trailing-stop simple
(ARM 12% / TRAIL 8%) lleva el win 44%→56% y la mediana de -1.4%→+2.3% sobre 8 semanas.

Diseño (espejo de update_outcomes.py: self-contained, su propio cron):
- Lee alertas BEST recientes de screener_outcomes (tipos de señal de swing).
- Replaya klines 1h forward desde alerted_at (cap WINDOW_DAYS).
- Cuando una alerta ARMADA (pico ≥ ARM_PCT) está actualmente ≥ TRAIL_PCT por debajo de su
  máximo, envía un aviso Telegram "tomar ganancia / mover stop".
- Idempotente: una vez por alerta vía tabla swing_exit_alerts (unique outcome_id).
- Default OFF (exit_mgmt.ENABLED). Solo presentación/gestión: NO toca detección ni scoring.

ACCIÓN REQUERIDA antes del primer run real: crear la tabla en Supabase (si no, el dedup
falla y se repetirían avisos). SQL:
    create table swing_exit_alerts (
      id bigserial primary key,
      outcome_id bigint not null,
      symbol text not null,
      alerted_at timestamptz not null,
      exit_sent_at timestamptz not null default now(),
      entry_price double precision,
      max_price double precision,
      exit_price double precision,
      max_gain_pct double precision,
      drop_from_max_pct double precision,
      reason text,
      unique (outcome_id)
    );
    create index on swing_exit_alerts (exit_sent_at);
"""
import argparse
import json
import os
from datetime import datetime, timezone, timedelta
from pathlib import Path

import requests

# ── Configuración ───────────────────────────────────────────────────────────
CONFIG_PATH = Path(__file__).parent / "config.json"
with open(CONFIG_PATH, "r", encoding="utf-8") as f:
    CONFIG = json.load(f)

_EX = CONFIG.get("exit_mgmt", {})
ENABLED      = _EX.get("ENABLED", False)
ARM_PCT      = _EX.get("ARM_PCT", 0.12)
TRAIL_PCT    = _EX.get("TRAIL_PCT", 0.08)
STOP_PCT     = _EX.get("STOP_PCT", 0.0)          # 0 = sin stop duro (config validada)
WINDOW_DAYS  = _EX.get("WINDOW_DAYS", 7)
BATCH_SIZE   = _EX.get("BATCH_SIZE", 100)
RETENTION_DAYS = _EX.get("RETENTION_DAYS", 30)

# Tipos de señal de swing (la tabla screener_outcomes es compartida; esto evita
# disparar avisos sobre filas de otro proyecto que escriba en la misma tabla).
SWING_SIGNALS = ("PREBREAK", "BREAKOUT", "RIDING", "HOLD", "COILING")

SUPABASE_URL = "https://ecgdswroygkfckkaguxp.supabase.co"

BINANCE_KLINES = "https://data-api.binance.vision/api/v3/klines"


# ── Lógica pura (testeable sin red ni credenciales) ──────────────────────────
def eval_exit(entry, highs, lows, closes, arm_pct, trail_pct, stop_pct=0.0):
    """Evalúa el estado ACTUAL de la posición. Devuelve dict con la salida sugerida
    o None si todavía hay que aguantar.

    - run_max: máximo alcanzado desde la entrada (incluye la entrada como piso).
    - armado: el pico superó arm_pct (recién ahí el trailing tiene sentido).
    - trail: armado y el precio actual está ≥ trail_pct por debajo del máximo.
    - stop (opcional, pre-arme): el mínimo perforó -stop_pct sin haber armado nunca.
    """
    if not entry or not closes:
        return None
    run_max = entry
    for hi in highs:
        if hi > run_max:
            run_max = hi
    max_gain = run_max / entry - 1
    armed = max_gain >= arm_pct
    cur = closes[-1]

    if stop_pct and not armed:
        worst = min(lows) if lows else entry
        if worst / entry - 1 <= -stop_pct:
            px = entry * (1 - stop_pct)
            return {"reason": "stop", "exit_price": px, "max_price": run_max,
                    "max_gain": max_gain, "drop_from_max": px / run_max - 1}

    if armed and cur <= run_max * (1 - trail_pct):
        return {"reason": "trail", "exit_price": cur, "max_price": run_max,
                "max_gain": max_gain, "drop_from_max": cur / run_max - 1}
    return None


# ── I/O ──────────────────────────────────────────────────────────────────────
def _sb_headers(minimal=False):
    h = {
        "apikey": os.environ["SUPABASE_KEY"],
        "Authorization": f"Bearer {os.environ['SUPABASE_KEY']}",
        "Content-Type": "application/json",
    }
    if minimal:
        h["Prefer"] = "return=minimal"
    return h


def fetch_candidates():
    """Alertas BEST de swing dentro de la ventana, aún relevantes para gestión."""
    since = (datetime.now(timezone.utc) - timedelta(days=WINDOW_DAYS)).isoformat()
    try:
        r = requests.get(
            f"{SUPABASE_URL}/rest/v1/screener_outcomes",
            headers=_sb_headers(),
            params={
                "select": "id,alerted_at,symbol,entry_price,signal_type,bucket",
                "bucket": "eq.BEST",
                "signal_type": f"in.({','.join(SWING_SIGNALS)})",
                "alerted_at": f"gte.{since}",
                "order": "alerted_at.asc",
                "limit": str(BATCH_SIZE),
            },
            timeout=15,
        )
        r.raise_for_status()
        return r.json()
    except Exception as e:
        print(f"fetch_candidates error: {e}")
        return []


def fetch_already_sent():
    """outcome_id ya avisados, para no repetir."""
    since = (datetime.now(timezone.utc) - timedelta(days=WINDOW_DAYS + 1)).isoformat()
    try:
        r = requests.get(
            f"{SUPABASE_URL}/rest/v1/swing_exit_alerts",
            headers=_sb_headers(),
            params={"select": "outcome_id", "exit_sent_at": f"gte.{since}"},
            timeout=15,
        )
        r.raise_for_status()
        return {row["outcome_id"] for row in r.json()}
    except Exception as e:
        print(f"fetch_already_sent error: {e}")
        return set()


def get_klines_1h(symbol, start_ms, end_ms):
    rows = []
    cursor = start_ms
    while cursor < end_ms:
        try:
            r = requests.get(BINANCE_KLINES, params={
                "symbol": symbol, "interval": "1h",
                "startTime": cursor, "endTime": end_ms, "limit": 1000,
            }, timeout=10)
            r.raise_for_status()
            data = r.json()
        except Exception as e:
            print(f"  klines {symbol} error: {e}")
            break
        if not data:
            break
        rows.extend(data)
        cursor = int(data[-1][6]) + 1
        if len(data) < 1000:
            break
    return rows


def send_telegram(text):
    try:
        r = requests.post(
            f"https://api.telegram.org/bot{os.environ['TELEGRAM_TOKEN']}/sendMessage",
            json={"chat_id": os.environ["TELEGRAM_CHAT_ID"], "text": text,
                  "parse_mode": "Markdown"},
            timeout=10,
        )
        r.raise_for_status()
    except Exception as e:
        print(f"  send_telegram error: {e}")


def record_exit(row, res):
    payload = {
        "outcome_id": row["id"],
        "symbol": row["symbol"],
        "alerted_at": row["alerted_at"],
        "exit_sent_at": datetime.now(timezone.utc).isoformat(),
        "entry_price": row["entry_price"],
        "max_price": res["max_price"],
        "exit_price": res["exit_price"],
        "max_gain_pct": res["max_gain"] * 100,
        "drop_from_max_pct": res["drop_from_max"] * 100,
        "reason": res["reason"],
    }
    try:
        r = requests.post(
            f"{SUPABASE_URL}/rest/v1/swing_exit_alerts",
            headers=_sb_headers(minimal=True), json=payload, timeout=10,
        )
        r.raise_for_status()
    except Exception as e:
        print(f"  record_exit {row['symbol']} error: {e}")


def format_msg(row, res):
    age = datetime.now(timezone.utc) - datetime.fromisoformat(
        row["alerted_at"].replace("Z", "+00:00"))
    age_str = f"{age.days}d{age.seconds // 3600}h" if age.days else f"{age.seconds // 3600}h"
    tag = "🛑 STOP" if res["reason"] == "stop" else "🔔 SALIDA"
    return (
        f"{tag} — *{row['symbol']}*\n"
        f"pico *+{res['max_gain'] * 100:.1f}%* desde entrada · "
        f"ahora *{res['drop_from_max'] * 100:.1f}%* desde el pico\n"
        f"👉 tomar ganancia / mover stop "
        f"(_{row['signal_type']}, hace {age_str}_)"
    )


def purge_old():
    before = (datetime.now(timezone.utc) - timedelta(days=RETENTION_DAYS)).isoformat()
    try:
        r = requests.delete(
            f"{SUPABASE_URL}/rest/v1/swing_exit_alerts",
            headers=_sb_headers(minimal=True),
            params={"exit_sent_at": f"lt.{before}"}, timeout=15,
        )
        r.raise_for_status()
    except Exception as e:
        print(f"  purge error: {e}")


def main(dry_run=False):
    if not ENABLED:
        print("exit_mgmt.ENABLED = false — skipping")
        return
    now_str = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    print(f"[{now_str}] Exit tracker — ARM {ARM_PCT:.0%} TRAIL {TRAIL_PCT:.0%} "
          f"STOP {STOP_PCT:.0%} | dry_run={dry_run}")

    candidates = fetch_candidates()
    sent = set() if dry_run else fetch_already_sent()
    print(f"  {len(candidates)} candidatas BEST | {len(sent)} ya avisadas")

    fired = 0
    now = datetime.now(timezone.utc)
    for row in candidates:
        if row["id"] in sent or not row.get("entry_price"):
            continue
        t0 = datetime.fromisoformat(row["alerted_at"].replace("Z", "+00:00"))
        start_ms = int(t0.timestamp() * 1000)
        end_ms = int(min(now, t0 + timedelta(days=WINDOW_DAYS)).timestamp() * 1000)
        kl = get_klines_1h(row["symbol"], start_ms, end_ms)
        if len(kl) < 3:
            continue
        highs = [float(k[2]) for k in kl]
        lows = [float(k[3]) for k in kl]
        closes = [float(k[4]) for k in kl]
        res = eval_exit(float(row["entry_price"]), highs, lows, closes,
                        ARM_PCT, TRAIL_PCT, STOP_PCT)
        if not res:
            continue
        fired += 1
        msg = format_msg(row, res)
        if dry_run:
            print(f"  [DRY] {msg.splitlines()[0]} | pico +{res['max_gain']*100:.1f}% "
                  f"drop {res['drop_from_max']*100:.1f}% ({res['reason']})")
        else:
            send_telegram(msg)
            record_exit(row, res)
            print(f"  AVISO {row['symbol']} pico +{res['max_gain']*100:.1f}% "
                  f"drop {res['drop_from_max']*100:.1f}% ({res['reason']})")

    if not dry_run:
        purge_old()
    print(f"Done. Avisos {'(dry) ' if dry_run else ''}disparados: {fired}")


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true",
                    help="No envía Telegram ni escribe Supabase; solo imprime qué avisaría.")
    args = ap.parse_args()
    main(dry_run=args.dry_run)
