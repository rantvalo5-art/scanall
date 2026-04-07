"""
Binance Spot USDT Crypto Screener — TODOS los pares, paralelo, MULTI-TIMEFRAME
Indicadores activos:
- BB squeeze filtrado (EARLY)
- BB width expansion + volume spike (EARLY)
- BB width expansion + volume spike + price up (CONFIRMED)
- Volumen spike standalone (EARLY)
Etiquetas: EARLY / CONFIRMED / LATE
Alertas vía Telegram + resumen final
"""

import os
import requests
import pandas as pd
import ta
from datetime import datetime, timezone, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed

# ── Configuración ──────────────────────────────────────────────────────────────
TELEGRAM_TOKEN   = os.environ["TELEGRAM_TOKEN"]
TELEGRAM_CHAT_ID = os.environ["TELEGRAM_CHAT_ID"]

# ── Supabase ──────────────────────────────────────────────────────────────────
SUPABASE_URL  = "https://ecgdswroygkfckkaguxp.supabase.co"
SUPABASE_KEY  = os.environ["SUPABASE_KEY"]
HISTORY_HOURS = 8

def _sb_headers():
    return {
        "apikey":        SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}",
        "Content-Type":  "application/json",
        "Prefer":        "return=minimal",
    }

def fetch_history():
    """Devuelve dict {(symbol, timeframe): count} de las últimas HISTORY_HOURS."""
    since = (datetime.now(timezone.utc) - timedelta(hours=HISTORY_HOURS)).isoformat()
    try:
        r = requests.get(
            f"{SUPABASE_URL}/rest/v1/screener_history",
            headers={**_sb_headers(), "Prefer": ""},
            params={"select": "symbol,timeframe", "alerted_at": f"gte.{since}"},
            timeout=10
        )
        r.raise_for_status()
        counts = {}
        for row in r.json():
            key = (row["symbol"], row["timeframe"])
            counts[key] = counts.get(key, 0) + 1
        return counts
    except Exception as e:
        print(f"⚠ Supabase fetch_history error: {e}")
        return {}

def insert_history(signals_by_tf):
    """Inserta una fila por cada alerta disparada en este run y limpia registros viejos."""
    now = datetime.now(timezone.utc)
    now_iso = now.isoformat()
    since = (now - timedelta(hours=HISTORY_HOURS)).isoformat()

    rows = [
        {"symbol": item["symbol"], "timeframe": tf, "alerted_at": now_iso}
        for tf, items in signals_by_tf.items()
        for item in items
    ]
    if not rows:
        return

    try:
        r = requests.post(
            f"{SUPABASE_URL}/rest/v1/screener_history",
            headers=_sb_headers(),
            json=rows,
            timeout=10
        )
        r.raise_for_status()
        print(f"✓ Supabase: {len(rows)} filas insertadas")

        r2 = requests.delete(
            f"{SUPABASE_URL}/rest/v1/screener_history",
            headers=_sb_headers(),
            params={"alerted_at": f"lt.{since}"},
            timeout=10
        )
        r2.raise_for_status()
        print("✓ Supabase: registros viejos eliminados")
    except Exception as e:
        print(f"⚠ Supabase insert_history error: {e}")

# ── Timeframes ────────────────────────────────────────────────────────────────
INTERVALS    = ["1m", "5m", "15m"]
LIMIT        = 100
TOP_N        = 9999
MAX_WORKERS  = 20

# ── Filtro de liquidez ────────────────────────────────────────────────────────
MIN_QUOTE_VOLUME = 100000

# ── BB Squeeze filtrado (para que no spamee) ─────────────────────────────────
BB_WIDTH_MIN            = 0.02
SQUEEZE_LOOKBACK        = 3
SQUEEZE_MAX_PREV_WIDTH  = 0.03
SQUEEZE_MIN_VOL_RATIO   = 1.8
SQUEEZE_ONLY_IF_TIGHTEN = True

# ── BB Width Expansion + Volume ───────────────────────────────────────────────
BB_EXPANSION_MIN = 0.095
BB_EXPANSION_PCT = 0.03
BB_WIDTH_MAX     = 5.0
EXP_VOL_NORMAL   = 2.0
EXP_VOL_FUERTE   = 5.0
EXP_VOL_EXTREMO  = 10.0

# ── Vol Spike standalone ──────────────────────────────────────────────────────
VOL_NORMAL       = 3.0
VOL_FUERTE       = 5.0
VOL_EXTREMO      = 10.0

# ── Etiquetas / urgencia ──────────────────────────────────────────────────────
LATE_REPEAT_COUNT      = 2      # si ya alertó 2+ veces en 8h, nueva alerta = LATE
IMMEDIATE_ALERT_TAGS   = {"EARLY", "CONFIRMED"}  # LATE queda solo para resumen
MAX_IMMEDIATE_PER_RUN  = 200

# ── Datos ─────────────────────────────────────────────────────────────────────
def get_active_usdt_symbols():
    """Devuelve el conjunto de símbolos USDT con status TRADING en Binance."""
    r = requests.get("https://data-api.binance.vision/api/v3/exchangeInfo", timeout=20)
    r.raise_for_status()
    return {
        s["symbol"]
        for s in r.json()["symbols"]
        if s["symbol"].endswith("USDT") and s["status"] == "TRADING"
    }

def get_all_usdt_pairs(n=TOP_N):
    active_symbols = get_active_usdt_symbols()

    r = requests.get("https://data-api.binance.vision/api/v3/ticker/24hr", timeout=15)
    r.raise_for_status()
    pairs = [
        x for x in r.json()
        if x["symbol"] in active_symbols
        and x["symbol"].encode("ascii", errors="ignore").decode() == x["symbol"]
        and float(x["quoteVolume"]) > MIN_QUOTE_VOLUME
    ]
    pairs.sort(key=lambda x: float(x["quoteVolume"]), reverse=True)
    return [x["symbol"] for x in pairs[:n]]

def get_klines(symbol, interval):
    r = requests.get(
        "https://data-api.binance.vision/api/v3/klines",
        params={"symbol": symbol, "interval": interval, "limit": LIMIT},
        timeout=10
    )
    r.raise_for_status()
    df = pd.DataFrame(r.json(), columns=[
        "open_time", "open", "high", "low", "close", "volume",
        "close_time", "quote_vol", "trades", "taker_buy_base", "taker_buy_quote", "ignore"
    ])
    for col in ["open", "high", "low", "close", "volume"]:
        df[col] = df[col].astype(float)
    return df

# ── Helpers ───────────────────────────────────────────────────────────────────
def severity_rank(tag):
    order = {"EARLY": 1, "CONFIRMED": 2, "LATE": 3}
    return order.get(tag, 0)

def vol_label(ratio, normal, fuerte, extremo):
    if ratio >= extremo:
        return "🔴 vol extremo"
    if ratio >= fuerte:
        return "🟡 vol fuerte"
    if ratio >= normal:
        return "🟢 vol normal"
    return None

def classify_alert(messages, prev_alerts_8h):
    """
    Regla simple:
    - Si ya alertó varias veces en 8h => LATE
    - Si tiene señal de confirmación => CONFIRMED
    - Si no => EARLY
    """
    if prev_alerts_8h >= LATE_REPEAT_COUNT:
        return "LATE"
    if any("[CONFIRMED]" in msg for msg in messages):
        return "CONFIRMED"
    return "EARLY"

def format_signal_lines(messages, final_tag):
    lines = []
    for msg in messages:
        cleaned = (
            msg.replace("[EARLY] ", "")
               .replace("[CONFIRMED] ", "")
               .replace("[LATE] ", "")
        )
        lines.append(f"{final_tag} {cleaned}")
    return lines

# ── Análisis ──────────────────────────────────────────────────────────────────
def analyze(symbol, interval):
    try:
        df = get_klines(symbol, interval)
    except Exception:
        return symbol, interval, None, 0

    if len(df) < 25:
        return symbol, interval, None, 0

    signals = []
    close   = df["close"]
    open_   = df["open"]
    volume  = df["volume"]

    # ── Bollinger Bands ───────────────────────────────────────────────────────
    bb     = ta.volatility.BollingerBands(close, window=20, window_dev=2)
    hband  = bb.bollinger_hband()
    lband  = bb.bollinger_lband()
    mavg   = bb.bollinger_mavg()

    mid_curr = mavg.iloc[-1]
    mid_prev = mavg.iloc[-2]
    price    = close.iloc[-1]

    width_curr = (hband.iloc[-1] - lband.iloc[-1]) / mid_curr if mid_curr != 0 else 0
    width_prev = (hband.iloc[-2] - lband.iloc[-2]) / mid_prev if mid_prev != 0 else 0

    width_hist = []
    for i in range(1, SQUEEZE_LOOKBACK + 1):
        mid_i = mavg.iloc[-i]
        if mid_i == 0:
            width_hist.append(999)
        else:
            width_hist.append((hband.iloc[-i] - lband.iloc[-i]) / mid_i)

    # ── Volumen base ──────────────────────────────────────────────────────────
    vol_mean  = volume.iloc[-21:-1].mean()
    vol_curr  = volume.iloc[-1]
    vol_ratio = vol_curr / vol_mean if vol_mean > 0 else 0

    # ── Contexto expansión / precio ───────────────────────────────────────────
    price_up      = close.iloc[-1] > open_.iloc[-1]
    width_delta   = width_curr - width_prev
    width_pct_chg = width_delta / width_prev if width_prev > 0 else 0
    expansion_ok  = width_delta >= BB_EXPANSION_MIN or width_pct_chg >= BB_EXPANSION_PCT

    # ── BB squeeze filtrado (EARLY) ───────────────────────────────────────────
    recent_min_width = min(width_hist)
    squeeze_basic    = width_curr <= BB_WIDTH_MIN
    squeeze_recent   = recent_min_width <= BB_WIDTH_MIN
    squeeze_tight    = max(width_hist) <= SQUEEZE_MAX_PREV_WIDTH
    squeeze_vol_ok   = vol_ratio >= SQUEEZE_MIN_VOL_RATIO
    squeeze_tighten  = (width_curr <= width_prev) if SQUEEZE_ONLY_IF_TIGHTEN else True

    if squeeze_recent and squeeze_tight and squeeze_vol_ok and squeeze_tighten:
        signals.append(
            "[EARLY] 🤏 BB squeeze filtrado "
            f"(width={width_curr:.2%}, min{SQUEEZE_LOOKBACK}={recent_min_width:.2%}, vol={vol_ratio:.1f}x)"
        )
    elif squeeze_basic and squeeze_tight and vol_ratio >= max(SQUEEZE_MIN_VOL_RATIO, VOL_FUERTE):
        # backup: si está súper apretado y además volumen fuerte, igual avisar
        signals.append(
            "[EARLY] 🤏 BB squeeze fuerte "
            f"(width={width_curr:.2%}, vol={vol_ratio:.1f}x)"
        )

    # ── BB expansion + volumen, sin price_up (EARLY) ─────────────────────────
    exp_vol_label = vol_label(vol_ratio, EXP_VOL_NORMAL, EXP_VOL_FUERTE, EXP_VOL_EXTREMO)

    if expansion_ok and exp_vol_label and width_curr < BB_WIDTH_MAX:
        signals.append(
            "[EARLY] "
            f"{exp_vol_label} {vol_ratio:.1f}x | BB expansion "
            f"{width_prev:.2%} → {width_curr:.2%} (+{width_pct_chg:.0%})"
        )

    # ── BB expansion + volumen + vela verde (CONFIRMED) ──────────────────────
    if expansion_ok and exp_vol_label and price_up and width_curr < BB_WIDTH_MAX:
        signals.append(
            "[CONFIRMED] "
            f"{exp_vol_label} {vol_ratio:.1f}x | BB expansion + vela verde "
            f"{width_prev:.2%} → {width_curr:.2%} (+{width_pct_chg:.0%})"
        )

    # ── Vol Spike standalone (EARLY) ──────────────────────────────────────────
    standalone_vol_label = vol_label(vol_ratio, VOL_NORMAL, VOL_FUERTE, VOL_EXTREMO)
    if standalone_vol_label:
        signals.append(
            "[EARLY] "
            f"{standalone_vol_label} standalone {vol_ratio:.1f}x promedio"
        )

    # Deduplicación liviana: si no hay nada, salir
    if not signals:
        return symbol, interval, None, vol_ratio

    return symbol, interval, signals, vol_ratio

# ── Telegram ──────────────────────────────────────────────────────────────────
def send_telegram(text):
    requests.post(
        f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage",
        json={
            "chat_id": TELEGRAM_CHAT_ID,
            "text": text,
            "parse_mode": "Markdown",
            "disable_web_page_preview": True
        },
        timeout=10
    ).raise_for_status()

def binance_link(symbol):
    """Convierte DASHUSDT → enlace Markdown a Binance spot."""
    pair = symbol[:-4] + "_USDT"
    url  = f"https://www.binance.com/en/trade/{pair}?type=spot"
    return f"[🔗]({url})"

def build_block(symbol, interval, lines, prev_alerts_8h, tag):
    hist_tag = f"  _(+{prev_alerts_8h} en 8h)_" if prev_alerts_8h > 0 else ""
    return (
        f"▶ {symbol} [{interval}] {binance_link(symbol)}{hist_tag}\n"
        + "\n".join(f"  {line}" for line in lines)
        + "\n\n"
    )

# ── Ordenar timeframes de menor a mayor ──────────────────────────────────────
_TF_ORDER = {"1m": 0, "3m": 1, "5m": 2, "15m": 3, "30m": 4, "1h": 5, "2h": 6, "4h": 7, "6h": 8, "8h": 9, "12h": 10, "1d": 11, "3d": 12, "1w": 13}

def sort_intervals(intervals):
    return sorted(intervals, key=lambda tf: _TF_ORDER.get(tf, 99))

# ── Main ──────────────────────────────────────────────────────────────────────
def main():
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    pairs = get_all_usdt_pairs()

    intervals_sorted = sort_intervals(INTERVALS)
    tf_label = " + ".join(intervals_sorted)
    print(f"[{now}] Escaneando {len(pairs)} pares USDT ({tf_label}) con {MAX_WORKERS} workers...")

    history = fetch_history()
    tasks = [(sym, tf) for sym in pairs for tf in intervals_sorted]

    results = {tf: {} for tf in intervals_sorted}
    immediate_sent = 0

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(analyze, sym, tf): (sym, tf) for sym, tf in tasks}
        for future in as_completed(futures):
            symbol, interval, raw_sigs, vol_ratio = future.result()
            prev = history.get((symbol, interval), 0)

            if raw_sigs:
                final_tag = classify_alert(raw_sigs, prev)
                formatted_lines = format_signal_lines(raw_sigs, final_tag)

                item = {
                    "symbol": symbol,
                    "interval": interval,
                    "tag": final_tag,
                    "signals": formatted_lines,
                    "vol_ratio": vol_ratio,
                    "prev": prev,
                }
                results[interval][symbol] = item

                print(f"  ✅ {symbol} [{interval}] {final_tag} vol={vol_ratio:.1f}x: {formatted_lines}")

                if final_tag in IMMEDIATE_ALERT_TAGS and immediate_sent < MAX_IMMEDIATE_PER_RUN:
                    try:
                        send_telegram(
                            f"🚨 *{final_tag}* inmediata\n"
                            f"{build_block(symbol, interval, formatted_lines, prev, final_tag)}"
                        )
                        immediate_sent += 1
                    except Exception as e:
                        print(f"⚠ Telegram immediate error {symbol} [{interval}]: {e}")
            else:
                results[interval][symbol] = None

    signals_by_tf = {}
    for tf in intervals_sorted:
        raw = [item for sym, item in results[tf].items() if item]
        raw.sort(key=lambda x: (severity_rank(x["tag"]), x["vol_ratio"]), reverse=True)
        signals_by_tf[tf] = raw

    total_signals = sum(len(v) for v in signals_by_tf.values())

    if total_signals == 0:
        print("Sin señales en este scan. No se envía nada a Telegram.")
        return

    insert_history(signals_by_tf)

    bar = "━" * 24
    tf_counts = "  ".join(f"{tf}: {len(signals_by_tf[tf])}" for tf in intervals_sorted)
    run_header = (
        f"{bar}\n"
        f"🟣  NUEVO SCAN  •  {now}\n"
        f"     {tf_label}  •  {len(pairs)} pares\n"
        f"     {tf_counts}\n"
        f"     inmediatas: {immediate_sent}\n"
        f"{bar}"
    )
    send_telegram(run_header)

    for tf in intervals_sorted:
        all_items = signals_by_tf[tf]

        if not all_items:
            print(f"Sin señales en {tf}.")
            continue

        tf_header = f"📊 [{tf}]  {len(all_items)} alertas\n{'─' * 20}\n\n"
        current = tf_header

        for item in all_items:
            block = build_block(
                item["symbol"],
                tf,
                item["signals"],
                item["prev"],
                item["tag"],
            )
            if len(current) + len(block) > 4000:
                send_telegram(current)
                current = block
            else:
                current += block

        if current.strip():
            send_telegram(current)

    print(f"\nTotal alertas: {total_signals} / {len(pairs)} pares × {len(intervals_sorted)} timeframes ({tf_label})")
    print("✅ Mensajes enviados a Telegram.")

if __name__ == "__main__":
    main()
