"""
Binance Spot USDT Crypto Screener — enfoque intradía hacia 1h
Versión enfocada en EARLY premium:
- Menos ruido, más calidad
- Solo TOP 3 EARLY por run a Telegram
- Vol spike standalone NO va a Telegram
- Score por símbolo
- Cooldown para no repetir EARLY seguido
- 15m detecta, 30m acompaña, 1h filtra contexto
"""

import os
import requests
import pandas as pd
import ta
from datetime import datetime, timezone, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed

# ── Configuración ──────────────────────────────────────────────────────────────
TELEGRAM_TOKEN = os.environ["TELEGRAM_TOKEN"]
TELEGRAM_CHAT_ID = os.environ["TELEGRAM_CHAT_ID"]

# ── Supabase ──────────────────────────────────────────────────────────────────
SUPABASE_URL = "https://ecgdswroygkfckkaguxp.supabase.co"
SUPABASE_KEY = os.environ["SUPABASE_KEY"]
HISTORY_HOURS = 8
EARLY_COOLDOWN_MINUTES = 1
LATE_REPEAT_COUNT = 8

# ── Escaneo / universo ────────────────────────────────────────────────────────
INTERVALS = ["15m", "30m", "1h"]
LIMIT = 120
TOP_N = 9999
MAX_WORKERS = 20
MIN_QUOTE_VOLUME = 300000  # subir liquidez para reducir ruido

# ── Bollinger / expansión ─────────────────────────────────────────────────────
BB_WIDTH_MIN = 0.022
SQUEEZE_MAX_PREV_WIDTH = 0.030
SQUEEZE_MIN_VOL_RATIO = 1.5
BB_EXPANSION_MIN = 0.010
BB_EXPANSION_PCT = 0.15
BB_WIDTH_MAX = 0.22
EXP_VOL_NORMAL = 2.0
EXP_VOL_FUERTE = 2.8
EXP_VOL_EXTREMO = 4.2

# ── Tendencia / estructura ────────────────────────────────────────────────────
EMA_FAST = 9
EMA_SLOW = 21
EMA_TREND_MIN_PCT = 0.0015
RECENT_LOOKBACK = 12
BREAKOUT_BUFFER = 0.001
MIN_BREAKOUT_DISTANCE = 0.006
VOLUME_GROWTH_MIN = 1.12
RECENT_GREEN_MIN = 2
HOLD_CANDLES_MIN = 2
ONE_H_RESIST_BUFFER = 0.015
ONE_H_BREAK_LOOKBACK = 24
CLOSE_TOP_PORTION_MIN = 0.70   # cierre en el 30% superior del rango
EARLY_MIN_SCORE = 6
TOP_EARLY_COUNT = 10


def _sb_headers():
    return {
        "apikey": SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}",
        "Content-Type": "application/json",
        "Prefer": "return=minimal",
    }


def fetch_history():
    """Devuelve dict con recuento 8h y último aviso por (symbol,timeframe)."""
    since = (datetime.now(timezone.utc) - timedelta(hours=HISTORY_HOURS)).isoformat()
    try:
        r = requests.get(
            f"{SUPABASE_URL}/rest/v1/screener_history",
            headers={**_sb_headers(), "Prefer": ""},
            params={"select": "symbol,timeframe,alerted_at", "alerted_at": f"gte.{since}"},
            timeout=10,
        )
        r.raise_for_status()
        counts = {}
        last_seen = {}
        for row in r.json():
            key = (row["symbol"], row["timeframe"])
            counts[key] = counts.get(key, 0) + 1
            ts = datetime.fromisoformat(row["alerted_at"].replace("Z", "+00:00"))
            if key not in last_seen or ts > last_seen[key]:
                last_seen[key] = ts
        return counts, last_seen
    except Exception as e:
        print(f"⚠ Supabase fetch_history error: {e}")
        return {}, {}


def insert_history(alert_rows):
    now = datetime.now(timezone.utc)
    now_iso = now.isoformat()
    since = (now - timedelta(hours=HISTORY_HOURS)).isoformat()
    rows = [{"symbol": row["symbol"], "timeframe": row["timeframe"], "alerted_at": now_iso} for row in alert_rows]
    if not rows:
        return
    try:
        r = requests.post(f"{SUPABASE_URL}/rest/v1/screener_history", headers=_sb_headers(), json=rows, timeout=10)
        r.raise_for_status()
        r2 = requests.delete(
            f"{SUPABASE_URL}/rest/v1/screener_history",
            headers=_sb_headers(),
            params={"alerted_at": f"lt.{since}"},
            timeout=10,
        )
        r2.raise_for_status()
        print(f"✓ Supabase: {len(rows)} filas insertadas y limpieza OK")
    except Exception as e:
        print(f"⚠ Supabase insert_history error: {e}")


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
        timeout=10,
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

    close = df["close"]
    open_ = df["open"]
    high = df["high"]
    low = df["low"]
    volume = df["volume"]

    bb = ta.volatility.BollingerBands(close, window=20, window_dev=2)
    hband = bb.bollinger_hband()
    lband = bb.bollinger_lband()
    mavg = bb.bollinger_mavg()

    mid_curr = mavg.iloc[-1]
    mid_prev = mavg.iloc[-2]
    width_curr = (hband.iloc[-1] - lband.iloc[-1]) / mid_curr if mid_curr else 0
    width_prev = (hband.iloc[-2] - lband.iloc[-2]) / mid_prev if mid_prev else 0
    width_delta = width_curr - width_prev
    width_pct_chg = width_delta / width_prev if width_prev > 0 else 0
    expansion_ok = width_delta >= BB_EXPANSION_MIN or width_pct_chg >= BB_EXPANSION_PCT

    vol_mean = volume.iloc[-21:-1].mean()
    vol_curr = volume.iloc[-1]
    vol_ratio = vol_curr / vol_mean if vol_mean > 0 else 0
    vol_short = volume.iloc[-3:].mean()
    vol_prior = volume.iloc[-6:-3].mean()
    vol_growth = vol_short / vol_prior if vol_prior > 0 else 0
    volume_growing = vol_growth >= VOLUME_GROWTH_MIN

    price = close.iloc[-1]
    price_up = close.iloc[-1] > open_.iloc[-1]

    candle_range = max(high.iloc[-1] - low.iloc[-1], 1e-12)
    close_position = (price - low.iloc[-1]) / candle_range
    closes_strong = close_position >= CLOSE_TOP_PORTION_MIN

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

    squeeze_recent = width_curr <= BB_WIDTH_MIN and width_prev <= SQUEEZE_MAX_PREV_WIDTH
    squeeze_tightening = width_curr <= width_prev * 1.08
    squeeze_ok = squeeze_recent and squeeze_tightening and vol_ratio >= SQUEEZE_MIN_VOL_RATIO and volume_growing

    exp_vol_ok = vol_ratio >= EXP_VOL_NORMAL
    expansion_early_ok = expansion_ok and exp_vol_ok and width_curr < BB_WIDTH_MAX and volume_growing
    expansion_confirmed_ok = expansion_early_ok and price_up and sustained_green and holding_above and closes_strong

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
        "squeeze_ok": squeeze_ok,
        "expansion_early_ok": expansion_early_ok,
        "expansion_confirmed_ok": expansion_confirmed_ok,
        "closes_strong": closes_strong,
        "close_position": close_position,
        "volume_growing": volume_growing,
    }


# ── Telegram ──────────────────────────────────────────────────────────────────
def send_telegram(text):
    requests.post(
        f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage",
        json={"chat_id": TELEGRAM_CHAT_ID, "text": text, "parse_mode": "Markdown", "disable_web_page_preview": True},
        timeout=10,
    ).raise_for_status()


def binance_link(symbol):
    pair = symbol[:-4] + "_USDT"
    url = f"https://www.binance.com/en/trade/{pair}?type=spot"
    return f"[🔗]({url})"


def early_in_cooldown(symbol, last_seen):
    ts = last_seen.get((symbol, "15m"))
    if not ts:
        return False
    return datetime.now(timezone.utc) - ts < timedelta(minutes=EARLY_COOLDOWN_MINUTES)


def score_early(tf15, tf30, tf1h):
    score = 0
    reasons = []

    if tf15.get("meaningful_break"):
        score += 2
        reasons.append(f"📈 ruptura 15m +{tf15.get('breakout_distance', 0):.2%}")
    if tf15.get("volume_growing"):
        score += 1
        reasons.append(f"📊 volumen creciendo {tf15.get('vol_growth', 0):.2f}x")

    if tf15.get("expansion_early_ok"):
        score += 2
        reasons.append(f"🌀 expansión 15m con vol {tf15.get('vol_ratio', 0):.1f}x")
    if tf15.get("squeeze_ok"):
        score += 1
        reasons.append("🤏 squeeze filtrado")
    if tf15.get("closes_strong"):
        score += 1
        reasons.append(f"🕯 cierre fuerte 15m ({tf15.get('close_position', 0):.0%} rango)")

    if tf30.get("trend_up") or tf30.get("holding_above") or tf30.get("sustained_green"):
        score += 1
        reasons.append("🧭 30m constructivo")
    if tf30.get("meaningful_break"):
        score += 1
        reasons.append(f"📈 30m acompaña +{tf30.get('breakout_distance', 0):.2%}")

    if tf1h.get("trend_up"):
        score += 2
        reasons.append("🟣 1h tendencia alcista")
    if tf1h.get("not_near_resistance"):
        score += 1
        if tf1h.get("breakout_1h"):
            reasons.append("🧱 1h rompiendo resistencia")
        else:
            reasons.append(f"🧱 1h con espacio ({tf1h.get('dist_to_res', 0):.2%})")
    if tf1h.get("holding_above") or tf1h.get("sustained_green"):
        score += 1
        reasons.append(f"📌 1h sostén {tf1h.get('hold_count', 0)}/3 velas")

    return score, reasons


def classify_symbol(symbol, tf_map, counts_history, last_seen):
    tf15 = tf_map.get("15m") or {}
    tf30 = tf_map.get("30m") or {}
    tf1h = tf_map.get("1h") or {}

    if not tf15 or not tf30 or not tf1h:
        return None

    trend_ok = tf1h.get("trend_up", False)
    resistance_ok = tf1h.get("not_near_resistance", False)
    continuity_ok = tf1h.get("holding_above", False) or tf1h.get("sustained_green", False)
    if not (trend_ok and resistance_ok and continuity_ok):
        return None

    # Queremos focus total en EARLY premium; CONFIRMED y LATE quedan fuera de Telegram.
    score, reasons = score_early(tf15, tf30, tf1h)
    if score < EARLY_MIN_SCORE:
        return None

    if early_in_cooldown(symbol, last_seen):
        return None

    prev_15m = counts_history.get((symbol, "15m"), 0)
    label = "LATE" if prev_15m >= LATE_REPEAT_COUNT else "EARLY"

    return {
        "symbol": symbol,
        "timeframe": "15m",
        "label": label,
        "priority": 1,
        "score": score,
        "vol_ratio": tf15.get("vol_ratio", 0),
        "reasons": reasons,
    }


def format_alert(alert, counts_history):
    symbol = alert["symbol"]
    tf = alert["timeframe"]
    prev = counts_history.get((symbol, tf), 0)
    hist_tag = f"  _(+{prev} en {HISTORY_HOURS}h)_" if prev > 0 else ""
    header = f"[{alert['label']}] {symbol} [{tf}] score={alert['score']} {binance_link(symbol)}{hist_tag}"
    body = "\n".join(f"  {r}" for r in alert["reasons"])
    return f"{header}\n{body}"


def main():
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    pairs = get_all_usdt_pairs()
    print(f"[{now}] Escaneando {len(pairs)} pares USDT ({' + '.join(INTERVALS)}) con {MAX_WORKERS} workers...")

    counts_history, last_seen = fetch_history()
    tasks = [(sym, tf) for sym in pairs for tf in INTERVALS]
    per_symbol = {sym: {} for sym in pairs}
    candidates = []
    processed = set()

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(analyze, sym, tf): (sym, tf) for sym, tf in tasks}
        for future in as_completed(futures):
            symbol, interval, data = future.result()
            per_symbol[symbol][interval] = data or {}

            ready = all(tf in per_symbol[symbol] for tf in INTERVALS)
            if not ready or symbol in processed:
                continue

            alert = classify_symbol(symbol, per_symbol[symbol], counts_history, last_seen)
            processed.add(symbol)
            if alert:
                candidates.append(alert)
                print(f"  ✅ {symbol} -> {alert['label']} score={alert['score']} {alert['reasons']}")

    if not candidates:
        print("Sin EARLY premium en este scan. No se envía nada a Telegram.")
        return

    # Focus total en EARLY y limpieza: descartamos LATE del Telegram.
    early_candidates = [a for a in candidates if a["label"] == "EARLY"]
    if not early_candidates:
        print("Solo hubo repetidas/LATE; no se envía Telegram para evitar ruido.")
        return

    early_candidates.sort(key=lambda x: (x["score"], x["vol_ratio"]), reverse=True)
    top_early = early_candidates[:TOP_EARLY_COUNT]
    insert_history(top_early)

    bar = "━" * 24
    header = (
        f"{bar}\n"
        f"🟡 TOP {len(top_early)} EARLY • {now}\n"
        f"   15m + 30m + 1h • {len(pairs)} pares\n"
        f"   cooldown {EARLY_COOLDOWN_MINUTES}m • min score {EARLY_MIN_SCORE}\n"
        f"{bar}"
    )
    body = "\n\n".join(format_alert(alert, counts_history) for alert in top_early)
    send_telegram(header + "\n" + body)

    print(f"\nTotal candidatos EARLY: {len(candidates)}")
    print(f"✅ Telegram: enviados solo TOP {len(top_early)} EARLY")


if __name__ == "__main__":
    main()
