"""
Binance Spot USDT Crypto Screener — enfoque intradía hacia 1h
Lógica principal:
- 15m = setup temprano (EARLY)
- 30m = confirmación intermedia (CONFIRMED)
- 1h = filtro de tendencia / resistencia / continuidad
- LATE = símbolo/timeframe que ya alertó en la ventana histórica

Objetivo:
Priorizar menos trades, pero con más chances de sostener un movimiento serio
(tipo 8-10%+) en vez de muchas alertas cortas de 3-5%.
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
LATE_REPEAT_COUNT = 1  # si ya hubo >=1 alerta en 8h, etiquetar como LATE


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


def insert_history(alert_rows):
    """Inserta historial real de alertas enviadas y limpia registros viejos."""
    now = datetime.now(timezone.utc)
    now_iso = now.isoformat()
    since = (now - timedelta(hours=HISTORY_HOURS)).isoformat()

    rows = [
        {"symbol": row["symbol"], "timeframe": row["timeframe"], "alerted_at": now_iso}
        for row in alert_rows
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


INTERVALS    = ["15m", "30m", "1h"]
LIMIT        = 120
TOP_N        = 9999
MAX_WORKERS  = 20

# ── Filtro de liquidez ────────────────────────────────────────────────────────
MIN_QUOTE_VOLUME = 150000

# ── Bollinger / expansión ─────────────────────────────────────────────────────
BB_WIDTH_MIN            = 0.022
SQUEEZE_MAX_PREV_WIDTH  = 0.030
SQUEEZE_MIN_VOL_RATIO   = 1.4
BB_EXPANSION_MIN        = 0.010
BB_EXPANSION_PCT        = 0.15
BB_WIDTH_MAX            = 0.22
EXP_VOL_NORMAL          = 1.8
EXP_VOL_FUERTE          = 2.5
EXP_VOL_EXTREMO         = 4.0

# ── Tendencia / estructura ────────────────────────────────────────────────────
EMA_FAST                = 9
EMA_SLOW                = 21
EMA_TREND_MIN_PCT       = 0.0015
RECENT_LOOKBACK         = 12   # velas para swing/resistencia intermedia
BREAKOUT_BUFFER         = 0.001
MIN_BREAKOUT_DISTANCE   = 0.006
VOLUME_GROWTH_MIN       = 1.10
RECENT_GREEN_MIN        = 2     # cuántas de las últimas 3 velas cerraron verdes
HOLD_CANDLES_MIN        = 2     # cuántos closes sosteniendo arriba de EMA21 / mitad de banda
ONE_H_RESIST_BUFFER     = 0.015 # evitar entrar justo debajo de resistencia 1h
ONE_H_BREAK_LOOKBACK    = 24

# ── Vol Spike standalone ──────────────────────────────────────────────────────
VOL_NORMAL              = 2.2
VOL_FUERTE              = 3.5
VOL_EXTREMO             = 5.0


# ── Datos ─────────────────────────────────────────────────────────────────────
def get_active_usdt_symbols():
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


# ── Análisis ──────────────────────────────────────────────────────────────────
def analyze(symbol, interval):
    try:
        df = get_klines(symbol, interval)
    except Exception:
        return symbol, interval, None

    if len(df) < 60:
        return symbol, interval, None

    close  = df["close"]
    open_  = df["open"]
    high   = df["high"]
    volume = df["volume"]

    bb     = ta.volatility.BollingerBands(close, window=20, window_dev=2)
    hband  = bb.bollinger_hband()
    lband  = bb.bollinger_lband()
    mavg   = bb.bollinger_mavg()

    mid_curr = mavg.iloc[-1]
    mid_prev = mavg.iloc[-2]
    width_curr = (hband.iloc[-1] - lband.iloc[-1]) / mid_curr if mid_curr else 0
    width_prev = (hband.iloc[-2] - lband.iloc[-2]) / mid_prev if mid_prev else 0
    width_delta   = width_curr - width_prev
    width_pct_chg = width_delta / width_prev if width_prev > 0 else 0
    expansion_ok  = width_delta >= BB_EXPANSION_MIN or width_pct_chg >= BB_EXPANSION_PCT

    vol_mean   = volume.iloc[-21:-1].mean()
    vol_curr   = volume.iloc[-1]
    vol_ratio  = vol_curr / vol_mean if vol_mean > 0 else 0
    vol_short  = volume.iloc[-3:].mean()
    vol_prior  = volume.iloc[-6:-3].mean()
    vol_growth = vol_short / vol_prior if vol_prior > 0 else 0
    volume_growing = vol_growth >= VOLUME_GROWTH_MIN

    price    = close.iloc[-1]
    price_up = close.iloc[-1] > open_.iloc[-1]

    ema_fast = ta.trend.EMAIndicator(close, window=EMA_FAST).ema_indicator()
    ema_slow = ta.trend.EMAIndicator(close, window=EMA_SLOW).ema_indicator()
    ema_slow_slope_pct = (ema_slow.iloc[-1] - ema_slow.iloc[-4]) / ema_slow.iloc[-4] if ema_slow.iloc[-4] else 0
    trend_up = (
        ema_fast.iloc[-1] > ema_slow.iloc[-1]
        and price > ema_slow.iloc[-1]
        and ema_slow_slope_pct >= EMA_TREND_MIN_PCT
    )

    breakout_ref = high.iloc[-(RECENT_LOOKBACK + 2):-2].max()
    structure_break = price > breakout_ref * (1 + BREAKOUT_BUFFER)
    breakout_distance = (price / breakout_ref - 1) if breakout_ref > 0 else 0
    meaningful_break = structure_break and breakout_distance >= MIN_BREAKOUT_DISTANCE

    recent_green = int((close.iloc[-3:] > open_.iloc[-3:]).sum())
    sustained_green = recent_green >= RECENT_GREEN_MIN

    hold_line = max(ema_slow.iloc[-1], mavg.iloc[-1])
    hold_count = int((close.iloc[-3:] > hold_line).sum())
    holding_above = hold_count >= HOLD_CANDLES_MIN

    resistance_ref = high.iloc[-(ONE_H_BREAK_LOOKBACK + 2):-2].max()
    dist_to_res = (resistance_ref - price) / price if price > 0 else 0
    breakout_1h = price > resistance_ref * (1 + BREAKOUT_BUFFER) if resistance_ref > 0 else False
    not_near_resistance = dist_to_res > ONE_H_RESIST_BUFFER or breakout_1h

    if vol_ratio >= EXP_VOL_EXTREMO:
        exp_vol_label = "🔴 vol extremo"
    elif vol_ratio >= EXP_VOL_FUERTE:
        exp_vol_label = "🟡 vol fuerte"
    elif vol_ratio >= EXP_VOL_NORMAL:
        exp_vol_label = "🟢 vol normal"
    else:
        exp_vol_label = None

    if vol_ratio >= VOL_EXTREMO:
        vol_label = "🔴 vol extremo standalone"
    elif vol_ratio >= VOL_FUERTE:
        vol_label = "🟡 vol fuerte standalone"
    elif vol_ratio >= VOL_NORMAL:
        vol_label = "🟢 vol fuerte standalone"
    else:
        vol_label = None

    squeeze_recent = width_curr <= BB_WIDTH_MIN and width_prev <= SQUEEZE_MAX_PREV_WIDTH
    squeeze_tightening = width_curr <= width_prev * 1.08
    squeeze_ok = squeeze_recent and squeeze_tightening and vol_ratio >= SQUEEZE_MIN_VOL_RATIO and volume_growing

    expansion_early_ok = expansion_ok and exp_vol_label and width_curr < BB_WIDTH_MAX and volume_growing
    expansion_confirmed_ok = expansion_early_ok and price_up and sustained_green and holding_above

    early_reasons = []
    if squeeze_ok:
        early_reasons.append(
            f"🤏 squeeze 15m/30m filtrado {width_prev:.2%} → {width_curr:.2%} | vol {vol_ratio:.1f}x"
        )
    if expansion_early_ok:
        early_reasons.append(
            f"{exp_vol_label} {vol_ratio:.1f}x | BB expansion {width_prev:.2%} → {width_curr:.2%}"
        )
    if vol_label and volume_growing and sustained_green:
        early_reasons.append(f"{vol_label} {vol_ratio:.1f}x | volumen creciendo {vol_growth:.2f}x")
    if meaningful_break:
        early_reasons.append(f"📈 ruptura de estructura +{breakout_distance:.2%}")

    confirmed_reasons = []
    if expansion_confirmed_ok:
        confirmed_reasons.append(
            f"✅ expansión confirmada | {exp_vol_label} {vol_ratio:.1f}x | sostén {hold_count}/3 velas"
        )
    if meaningful_break and sustained_green and holding_above and volume_growing:
        confirmed_reasons.append(
            f"✅ breakout sostenido +{breakout_distance:.2%} | {recent_green}/3 velas verdes | vol creciendo {vol_growth:.2f}x"
        )

    return symbol, interval, {
        "price": price,
        "vol_ratio": vol_ratio,
        "vol_growth": vol_growth,
        "trend_up": trend_up,
        "structure_break": structure_break,
        "meaningful_break": meaningful_break,
        "breakout_distance": breakout_distance,
        "holding_above": holding_above,
        "hold_count": hold_count,
        "sustained_green": sustained_green,
        "recent_green": recent_green,
        "not_near_resistance": not_near_resistance,
        "dist_to_res": dist_to_res,
        "breakout_1h": breakout_1h,
        "early_reasons": early_reasons,
        "confirmed_reasons": confirmed_reasons,
    }


# ── Telegram ──────────────────────────────────────────────────────────────────
def send_telegram(text):
    requests.post(
        f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage",
        json={"chat_id": TELEGRAM_CHAT_ID, "text": text, "parse_mode": "Markdown", "disable_web_page_preview": True},
        timeout=10
    ).raise_for_status()


def binance_link(symbol):
    pair = symbol[:-4] + "_USDT"
    url  = f"https://www.binance.com/en/trade/{pair}?type=spot"
    return f"[🔗]({url})"


_TF_ORDER = {"1m": 0, "3m": 1, "5m": 2, "15m": 3, "30m": 4, "1h": 5, "2h": 6, "4h": 7, "6h": 8, "8h": 9, "12h": 10, "1d": 11, "3d": 12, "1w": 13}

def sort_intervals(intervals):
    return sorted(intervals, key=lambda tf: _TF_ORDER.get(tf, 99))


def classify_symbol(symbol, tf_map, history):
    tf15 = tf_map.get("15m") or {}
    tf30 = tf_map.get("30m") or {}
    tf1h = tf_map.get("1h") or {}

    # Filtro 1h: queremos contexto real a favor, no movernos debajo de resistencia dura.
    trend_ok = tf1h.get("trend_up", False)
    resistance_ok = tf1h.get("not_near_resistance", False)
    continuity_ok = tf1h.get("holding_above", False) or tf1h.get("sustained_green", False)
    if not (trend_ok and resistance_ok and continuity_ok):
        return None

    prev_15m = history.get((symbol, "15m"), 0)
    prev_30m = history.get((symbol, "30m"), 0)

    # Confirmación de mejor calidad: 30m confirma y 15m ya venía construyendo.
    thirty_confirms = bool(tf30.get("confirmed_reasons"))
    fifteen_constructive = bool(tf15.get("early_reasons")) or tf15.get("meaningful_break", False)
    one_hour_bonus = []
    if tf1h.get("breakout_1h"):
        one_hour_bonus.append("🧱 1h rompiendo resistencia")
    else:
        one_hour_bonus.append(f"🧱 1h lejos de resistencia ({tf1h.get('dist_to_res', 0):.2%})")
    one_hour_bonus.append(f"📌 1h sostén {tf1h.get('hold_count', 0)}/3 velas arriba")

    if thirty_confirms and fifteen_constructive:
        label = "LATE" if prev_30m >= LATE_REPEAT_COUNT else "CONFIRMED"
        return {
            "symbol": symbol,
            "timeframe": "30m",
            "label": label,
            "priority": 2,
            "vol_ratio": tf30.get("vol_ratio", 0),
            "reasons": tf30.get("confirmed_reasons", []) + one_hour_bonus
        }

    # EARLY: 15m detecta setup y 30m no niega la idea.
    thirty_not_bad = tf30.get("trend_up", False) or tf30.get("holding_above", False) or tf30.get("sustained_green", False)
    if tf15.get("early_reasons") and thirty_not_bad:
        label = "LATE" if prev_15m >= LATE_REPEAT_COUNT else "EARLY"
        return {
            "symbol": symbol,
            "timeframe": "15m",
            "label": label,
            "priority": 1,
            "vol_ratio": tf15.get("vol_ratio", 0),
            "reasons": tf15.get("early_reasons", []) + ["🧭 30m todavía constructivo"] + one_hour_bonus
        }

    return None


def format_alert(alert, history):
    symbol = alert["symbol"]
    tf = alert["timeframe"]
    prev = history.get((symbol, tf), 0)
    hist_tag = f"  _(+{prev} en {HISTORY_HOURS}h)_" if prev > 0 else ""
    header = f"[{alert['label']}] {symbol} [{tf}] {binance_link(symbol)}{hist_tag}"
    body = "\n".join(f"  {r}" for r in alert["reasons"])
    return f"{header}\n{body}"


# ── Main ──────────────────────────────────────────────────────────────────────
def main():
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    pairs = get_all_usdt_pairs()
    intervals_sorted = sort_intervals(INTERVALS)
    tf_label = " + ".join(intervals_sorted)
    print(f"[{now}] Escaneando {len(pairs)} pares USDT ({tf_label}) con {MAX_WORKERS} workers...")

    history = fetch_history()
    tasks = [(sym, tf) for sym in pairs for tf in intervals_sorted]

    per_symbol = {sym: {} for sym in pairs}
    alerts = []
    sent_now = set()

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(analyze, sym, tf): (sym, tf) for sym, tf in tasks}
        for future in as_completed(futures):
            symbol, interval, data = future.result()
            per_symbol[symbol][interval] = data or {}

            ready = all(tf in per_symbol[symbol] for tf in intervals_sorted)
            if not ready or symbol in sent_now:
                continue

            alert = classify_symbol(symbol, per_symbol[symbol], history)
            if not alert:
                sent_now.add(symbol)
                continue

            alerts.append(alert)
            if alert["label"] in {"EARLY", "CONFIRMED"}:
                try:
                    send_telegram("🚨 ALERTA INMEDIATA\n" + format_alert(alert, history))
                except Exception as e:
                    print(f"⚠ Error enviando alerta inmediata {symbol}: {e}")
            sent_now.add(symbol)
            print(f"  ✅ {symbol} -> {alert['label']} [{alert['timeframe']}] {alert['reasons']}")

    if not alerts:
        print("Sin señales filtradas en este scan. No se envía nada a Telegram.")
        return

    alerts.sort(key=lambda x: (x["priority"], x["vol_ratio"]), reverse=True)
    insert_history(alerts)

    bar = "━" * 24
    counts = {
        "EARLY": sum(1 for a in alerts if a["label"] == "EARLY"),
        "CONFIRMED": sum(1 for a in alerts if a["label"] == "CONFIRMED"),
        "LATE": sum(1 for a in alerts if a["label"] == "LATE"),
    }
    run_header = (
        f"{bar}\n"
        f"🟣 NUEVO SCAN • {now}\n"
        f"   {tf_label} • {len(pairs)} pares\n"
        f"   EARLY: {counts['EARLY']}  CONFIRMED: {counts['CONFIRMED']}  LATE: {counts['LATE']}\n"
        f"{bar}"
    )
    send_telegram(run_header)

    current = "📊 Resumen filtrado 1h\n" + "─" * 20 + "\n\n"
    for alert in alerts:
        block = format_alert(alert, history) + "\n\n"
        if len(current) + len(block) > 4000:
            send_telegram(current)
            current = block
        else:
            current += block
    if current.strip():
        send_telegram(current)

    print(f"\nTotal alertas: {len(alerts)} / {len(pairs)} pares")
    print("✅ Mensajes enviados a Telegram.")


if __name__ == "__main__":
    main()
