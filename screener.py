"""
Binance Spot USDT Crypto Screener — breakout / continuation
Arquitectura pedida:
- 5m = PRE-BREAK (radar temprano)
- 15m = BREAKOUT (ruptura real)
- 30m / 1h = filtros de contexto
- Telegram inmediato solo para PRE-BREAK fuerte y BREAKOUT

Notas:
- mantiene la cantidad de monedas escaneadas (TOP_N no se reduce)
- evita esperar al resumen final para avisos tempranos clave
- resumen final excluye lo ya enviado de inmediato para no duplicar spam
"""

import os
from datetime import datetime, timezone, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed

import pandas as pd
import requests
import ta

# ── Configuración ──────────────────────────────────────────────────────────────
TELEGRAM_TOKEN = os.environ["TELEGRAM_TOKEN"]
TELEGRAM_CHAT_ID = os.environ["TELEGRAM_CHAT_ID"]
SUPABASE_KEY = os.environ["SUPABASE_KEY"]
SUPABASE_URL = "https://ecgdswroygkfckkaguxp.supabase.co"

# ── Universo / run ─────────────────────────────────────────────────────────────
INTERVALS = ["5m", "15m", "30m", "1h"]
LIMIT = 180
TOP_N = 9999
MAX_WORKERS = 20
MIN_QUOTE_VOLUME = 300000
TOP_ALERT_COUNT = 5

# ── Historial / spam control ──────────────────────────────────────────────────
HISTORY_HOURS = 8
STATE_COOLDOWN_MINUTES = 0
LATE_REPEAT_COUNT = 1

# ── Indicadores base ───────────────────────────────────────────────────────────
EMA_FAST = 9
EMA_SLOW = 21
BB_WIDTH_MIN = 0.022
SQUEEZE_MAX_PREV_WIDTH = 0.030
SQUEEZE_MIN_VOL_RATIO = 1.30
BB_EXPANSION_MIN = 0.010
BB_EXPANSION_PCT = 0.15
EXP_VOL_NORMAL = 2.0
VOLUME_GROWTH_MIN = 1.10
RECENT_LOOKBACK = 12
BREAKOUT_BUFFER = 0.001
MIN_BREAKOUT_DISTANCE = 0.006
CLOSE_TOP_PORTION_MIN = 0.70
ONE_H_RESIST_BUFFER = 0.015
ONE_H_BREAK_LOOKBACK = 24

# ── PRE-BREAK 5m ──────────────────────────────────────────────────────────────
PREBREAK_SCORE_MIN = 4
PREBREAK_IMMEDIATE_SCORE = 6
PREBREAK_TOP_N = 2
PREBREAK_NEAR_BREAK_MAX = 0.018
PREBREAK_TIGHT_RANGE_MAX = 0.028
PREBREAK_RISING_CANDLES_MIN = 1

# ── BREAKOUT 15m ───────────────────────────────────────────────────────────────
BREAKOUT_SCORE_MIN = 6
BREAKOUT_IMMEDIATE_SCORE = 7
BREAKOUT_TOP_N = 2
BREAKOUT_NOT_OVEREXTENDED_MAX = 0.035

# ── HOLD / RETEST 15m filtrado por 30m / 1h ──────────────────────────────────
HOLD_SCORE_MIN = 7
HOLD_TOP_N = 2
HOLD_LOOKBACK_BARS = 8
HOLD_RECENT_BREAK_MAX_BARS = 6
HOLD_PULLBACK_MAX = 0.025
HOLD_ZONE_BUFFER = 0.003
HOLD_STRONG_CLOSES_MIN = 2


# ── Supabase ───────────────────────────────────────────────────────────────────
def _sb_headers():
    return {
        "apikey": SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}",
        "Content-Type": "application/json",
        "Prefer": "return=minimal",
    }


def fetch_history():
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
    rows = [{"symbol": row["symbol"], "timeframe": row["history_tf"], "alerted_at": now_iso} for row in alert_rows]
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


# ── Datos Binance ──────────────────────────────────────────────────────────────
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


# ── Helpers ────────────────────────────────────────────────────────────────────
def last_n_strong_closes(close_series, high_series, low_series, n=3):
    count = 0
    for i in range(1, n + 1):
        rng = max(high_series.iloc[-i] - low_series.iloc[-i], 1e-12)
        close_pos = (close_series.iloc[-i] - low_series.iloc[-i]) / rng
        if close_pos >= CLOSE_TOP_PORTION_MIN:
            count += 1
    return count


def safe_pct(a, b):
    return (a / b - 1) if b else 0.0


# ── Análisis por timeframe ─────────────────────────────────────────────────────
def analyze(symbol, interval):
    try:
        df = get_klines(symbol, interval)
    except Exception:
        return symbol, interval, None

    if len(df) < 80:
        return symbol, interval, None

    close = df["close"]
    open_ = df["open"]
    high = df["high"]
    low = df["low"]
    volume = df["volume"]
    price = close.iloc[-1]

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

    ema_fast = ta.trend.EMAIndicator(close, window=EMA_FAST).ema_indicator()
    ema_slow = ta.trend.EMAIndicator(close, window=EMA_SLOW).ema_indicator()
    ema_slow_slope_pct = safe_pct(ema_slow.iloc[-1], ema_slow.iloc[-4]) if ema_slow.iloc[-4] else 0
    trend_up = (
        ema_fast.iloc[-1] > ema_slow.iloc[-1]
        and price > ema_slow.iloc[-1]
        and ema_slow_slope_pct >= 0.001
    )

    candle_range = max(high.iloc[-1] - low.iloc[-1], 1e-12)
    close_position = (price - low.iloc[-1]) / candle_range
    closes_strong = close_position >= CLOSE_TOP_PORTION_MIN
    strong_closes_3 = last_n_strong_closes(close, high, low, 3)
    rising_candles_3 = int((close.iloc[-3:] > open_.iloc[-3:]).sum())

    breakout_ref = high.iloc[-(RECENT_LOOKBACK + 2):-2].max()
    structure_break = price > breakout_ref * (1 + BREAKOUT_BUFFER)
    breakout_distance = safe_pct(price, breakout_ref)
    meaningful_break = structure_break and breakout_distance >= MIN_BREAKOUT_DISTANCE
    near_break = breakout_ref > 0 and 0 <= (breakout_ref - price) / breakout_ref <= PREBREAK_NEAR_BREAK_MAX

    hold_line = max(ema_slow.iloc[-1], mavg.iloc[-1])
    hold_count = int((close.iloc[-3:] > hold_line).sum())
    holding_above = hold_count >= 2

    resistance_ref = high.iloc[-(ONE_H_BREAK_LOOKBACK + 2):-2].max()
    dist_to_res = (resistance_ref - price) / price if price > 0 else 0
    breakout_tf = price > resistance_ref * (1 + BREAKOUT_BUFFER) if resistance_ref > 0 else False
    not_near_resistance = dist_to_res > ONE_H_RESIST_BUFFER or breakout_tf

    squeeze_recent = width_curr <= BB_WIDTH_MIN and width_prev <= SQUEEZE_MAX_PREV_WIDTH
    squeeze_tightening = width_curr <= width_prev * 1.08
    squeeze_ok = squeeze_recent and squeeze_tightening and vol_ratio >= SQUEEZE_MIN_VOL_RATIO and volume_growing

    recent_width_min = ((hband.iloc[-5:] - lband.iloc[-5:]) / mavg.iloc[-5:]).min()
    tight_range = recent_width_min <= PREBREAK_TIGHT_RANGE_MAX

    # Detectar ruptura reciente para HOLD/RETEST en 15m
    recent_break_idx = None
    recent_break_ref = None
    recent_break_close = None
    start = max(25, len(df) - HOLD_LOOKBACK_BARS - 2)
    for idx in range(start, len(df) - 1):
        ref_slice = high.iloc[max(0, idx - RECENT_LOOKBACK):idx]
        if len(ref_slice) < RECENT_LOOKBACK:
            continue
        ref = ref_slice.max()
        if close.iloc[idx] > ref * (1 + BREAKOUT_BUFFER):
            recent_break_idx = idx
            recent_break_ref = ref
            recent_break_close = close.iloc[idx]

    hold_recent_break = False
    hold_kept_zone = False
    hold_pullback_ok = False
    hold_after_break_closes = 0
    bars_since_break = None
    if recent_break_idx is not None and recent_break_ref:
        bars_since_break = len(df) - 1 - recent_break_idx
        if 1 <= bars_since_break <= HOLD_RECENT_BREAK_MAX_BARS:
            post = df.iloc[recent_break_idx + 1:]
            if len(post) >= 2:
                min_post_close = post["close"].min()
                min_post_low = post["low"].min()
                hold_kept_zone = min_post_low >= recent_break_ref * (1 - HOLD_ZONE_BUFFER)
                hold_pullback = (recent_break_close - min_post_close) / recent_break_close if recent_break_close else 0
                hold_pullback_ok = hold_pullback <= HOLD_PULLBACK_MAX
                hold_after_break_closes = last_n_strong_closes(post["close"], post["high"], post["low"], min(3, len(post)))
                hold_recent_break = True

    return symbol, interval, {
        "price": price,
        "vol_ratio": vol_ratio,
        "vol_growth": vol_growth,
        "volume_growing": volume_growing,
        "trend_up": trend_up,
        "width_curr": width_curr,
        "expansion_ok": expansion_ok,
        "close_position": close_position,
        "closes_strong": closes_strong,
        "strong_closes_3": strong_closes_3,
        "rising_candles_3": rising_candles_3,
        "breakout_ref": breakout_ref,
        "structure_break": structure_break,
        "meaningful_break": meaningful_break,
        "breakout_distance": breakout_distance,
        "near_break": near_break,
        "tight_range": tight_range,
        "squeeze_ok": squeeze_ok,
        "holding_above": holding_above,
        "hold_count": hold_count,
        "not_near_resistance": not_near_resistance,
        "dist_to_res": dist_to_res,
        "breakout_tf": breakout_tf,
        "hold_recent_break": hold_recent_break,
        "hold_kept_zone": hold_kept_zone,
        "hold_pullback_ok": hold_pullback_ok,
        "hold_after_break_closes": hold_after_break_closes,
        "bars_since_break": bars_since_break,
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


def in_cooldown(symbol, history_tf, last_seen):
    ts = last_seen.get((symbol, history_tf))
    if not ts:
        return False
    return datetime.now(timezone.utc) - ts < timedelta(minutes=STATE_COOLDOWN_MINUTES)


# ── Scoring / clasificación ───────────────────────────────────────────────────
def score_prebreak(tf5, tf30, tf1h):
    score = 0
    reasons = []
    if tf5.get("squeeze_ok"):
        score += 2
        reasons.append("🤏 compresión filtrada 5m")
    elif tf5.get("tight_range"):
        score += 1
        reasons.append("📦 rango aún comprimido 5m")
    if tf5.get("near_break"):
        score += 2
        reasons.append("🧱 muy cerca de romper máximo reciente 5m")
    if tf5.get("volume_growing"):
        score += 1
        reasons.append(f"📊 volumen creciendo {tf5.get('vol_growth', 0):.2f}x")
    elif tf5.get("vol_ratio", 0) >= 1.15:
        score += 1
        reasons.append(f"📊 volumen ya encima de lo normal ({tf5.get('vol_ratio', 0):.1f}x)")
    if tf5.get("closes_strong"):
        score += 1
        reasons.append(f"🕯 cierre fuerte 5m ({tf5.get('close_position', 0):.0%} rango)")
    if tf5.get("rising_candles_3", 0) >= PREBREAK_RISING_CANDLES_MIN:
        score += 1
        reasons.append("↗ presión compradora 5m")
    if tf30.get("trend_up") or tf30.get("holding_above"):
        score += 1
        reasons.append("🧭 30m acompaña")
    if tf1h.get("trend_up"):
        score += 1
        reasons.append("🟣 1h a favor")
    if tf1h.get("not_near_resistance"):
        score += 1
        reasons.append("🧱 1h con espacio")
    return score, reasons


def score_breakout(tf15, tf30, tf1h):
    score = 0
    reasons = []
    if tf15.get("meaningful_break"):
        score += 3
        reasons.append(f"📈 ruptura 15m +{tf15.get('breakout_distance', 0):.2%}")
    if tf15.get("expansion_ok") and tf15.get("vol_ratio", 0) >= EXP_VOL_NORMAL:
        score += 2
        reasons.append(f"🌀 expansión 15m con vol {tf15.get('vol_ratio', 0):.1f}x")
    if tf15.get("closes_strong"):
        score += 1
        reasons.append(f"🕯 cierre fuerte ({tf15.get('close_position', 0):.0%} rango)")
    if tf15.get("breakout_distance", 0) <= BREAKOUT_NOT_OVEREXTENDED_MAX:
        score += 1
        reasons.append("📏 aún no muy extendida")
    if tf30.get("trend_up") or tf30.get("meaningful_break"):
        score += 1
        reasons.append("🧭 30m acompaña")
    if tf1h.get("trend_up"):
        score += 1
        reasons.append("🟣 1h alcista")
    if tf1h.get("not_near_resistance"):
        score += 1
        reasons.append("🧱 1h con espacio / rompiendo")
    return score, reasons


def score_hold(tf15, tf30, tf1h):
    score = 0
    reasons = []
    if tf15.get("hold_recent_break"):
        score += 2
        reasons.append(f"📍 ruptura reciente hace {tf15.get('bars_since_break', 0)} velas")
    if tf15.get("hold_kept_zone"):
        score += 2
        reasons.append("🛡 no perdió la zona rota")
    if tf15.get("hold_pullback_ok"):
        score += 1
        reasons.append("↘ retroceso sano")
    if tf15.get("hold_after_break_closes", 0) >= HOLD_STRONG_CLOSES_MIN:
        score += 2
        reasons.append("🕯 sostiene con cierres firmes")
    if tf30.get("holding_above") or tf30.get("trend_up"):
        score += 1
        reasons.append("🧭 30m sigue constructivo")
    if tf1h.get("trend_up"):
        score += 1
        reasons.append("🟣 1h sigue a favor")
    if tf1h.get("not_near_resistance"):
        score += 1
        reasons.append("🧱 1h aún con espacio")
    return score, reasons


def classify_symbol(symbol, tf_map, counts_history, last_seen):
    tf5 = tf_map.get("5m") or {}
    tf15 = tf_map.get("15m") or {}
    tf30 = tf_map.get("30m") or {}
    tf1h = tf_map.get("1h") or {}
    if not tf5 or not tf15 or not tf30 or not tf1h:
        return None

    # filtro macro: 30m y 1h deben estar al menos constructivos
    macro_ok = (
        (tf30.get("trend_up") or tf30.get("holding_above"))
        and tf1h.get("trend_up")
        and tf1h.get("not_near_resistance")
    )
    if not macro_ok:
        return None

    candidates = []

    pre_score, pre_reasons = score_prebreak(tf5, tf30, tf1h)
    if (
        tf5.get("near_break")
        and (tf5.get("squeeze_ok") or tf5.get("tight_range"))
        and (tf5.get("volume_growing") or tf5.get("vol_ratio", 0) >= 1.15)
        and (tf5.get("closes_strong") or tf5.get("rising_candles_3", 0) >= PREBREAK_RISING_CANDLES_MIN)
        and pre_score >= PREBREAK_SCORE_MIN
        and not in_cooldown(symbol, "PREBREAK", last_seen)
    ):
        prev = counts_history.get((symbol, "PREBREAK"), 0)
        candidates.append({
            "symbol": symbol,
            "label": "PRE-BREAK",
            "history_tf": "PREBREAK",
            "score": pre_score,
            "priority": 1,
            "vol_ratio": tf5.get("vol_ratio", 0),
            "reasons": pre_reasons,
            "late": prev >= LATE_REPEAT_COUNT,
            "timeframe": "5m",
            "immediate": pre_score >= PREBREAK_IMMEDIATE_SCORE and prev < LATE_REPEAT_COUNT,
        })

    breakout_score, breakout_reasons = score_breakout(tf15, tf30, tf1h)
    if (
        tf15.get("meaningful_break")
        and tf15.get("expansion_ok")
        and tf15.get("vol_ratio", 0) >= EXP_VOL_NORMAL
        and tf15.get("breakout_distance", 0) <= BREAKOUT_NOT_OVEREXTENDED_MAX
        and breakout_score >= BREAKOUT_SCORE_MIN
        and not in_cooldown(symbol, "BREAKOUT", last_seen)
    ):
        prev = counts_history.get((symbol, "BREAKOUT"), 0)
        candidates.append({
            "symbol": symbol,
            "label": "BREAKOUT",
            "history_tf": "BREAKOUT",
            "score": breakout_score,
            "priority": 2,
            "vol_ratio": tf15.get("vol_ratio", 0),
            "reasons": breakout_reasons,
            "late": prev >= LATE_REPEAT_COUNT,
            "timeframe": "15m",
            "immediate": breakout_score >= BREAKOUT_IMMEDIATE_SCORE and prev < LATE_REPEAT_COUNT,
        })

    hold_score, hold_reasons = score_hold(tf15, tf30, tf1h)
    if (
        tf15.get("hold_recent_break")
        and tf15.get("hold_kept_zone")
        and tf15.get("hold_pullback_ok")
        and tf15.get("hold_after_break_closes", 0) >= HOLD_STRONG_CLOSES_MIN
        and hold_score >= HOLD_SCORE_MIN
        and not in_cooldown(symbol, "HOLD", last_seen)
    ):
        prev = counts_history.get((symbol, "HOLD"), 0)
        candidates.append({
            "symbol": symbol,
            "label": "HOLD/RETEST",
            "history_tf": "HOLD",
            "score": hold_score,
            "priority": 3,
            "vol_ratio": tf15.get("vol_ratio", 0),
            "reasons": hold_reasons,
            "late": prev >= LATE_REPEAT_COUNT,
            "timeframe": "15m",
            "immediate": False,
        })

    if not candidates:
        return None

    # priorizamos HOLD > BREAKOUT > PRE-BREAK; luego score y vol
    candidates.sort(key=lambda x: (x["priority"], x["score"], x["vol_ratio"]), reverse=True)
    return candidates[0]


# ── Formato ───────────────────────────────────────────────────────────────────
def format_alert(alert, counts_history):
    prev = counts_history.get((alert["symbol"], alert["history_tf"]), 0)
    late_tag = " [LATE]" if alert.get("late") else ""
    hist_tag = f"  _(+{prev} en {HISTORY_HOURS}h)_" if prev > 0 else ""
    header = f"[{alert['label']}{late_tag}] {alert['symbol']} {alert['timeframe']} score={alert['score']} {binance_link(alert['symbol'])}{hist_tag}"
    body = "\n".join(f"  {r}" for r in alert["reasons"])
    return f"{header}\n{body}"


def dedupe_and_rank(alerts):
    best = {}
    for alert in alerts:
        cur = best.get(alert["symbol"])
        if cur is None or (alert["priority"], alert["score"], alert["vol_ratio"]) > (cur["priority"], cur["score"], cur["vol_ratio"]):
            best[alert["symbol"]] = alert

    ranked = list(best.values())
    ranked.sort(key=lambda x: (x["priority"], x["score"], x["vol_ratio"]), reverse=True)

    final = []
    caps = {"PRE-BREAK": PREBREAK_TOP_N, "BREAKOUT": BREAKOUT_TOP_N, "HOLD/RETEST": HOLD_TOP_N}
    used = {k: 0 for k in caps}
    for alert in ranked:
        label = alert["label"]
        if used[label] >= caps[label]:
            continue
        final.append(alert)
        used[label] += 1
        if len(final) >= TOP_ALERT_COUNT:
            break
    return final


def send_immediate(alert, counts_history):
    prefix = "⚡ PRE-BREAK FUERTE" if alert["label"] == "PRE-BREAK" else "🚀 BREAKOUT"
    text = (
        f"{prefix}\n"
        f"{format_alert(alert, counts_history)}\n"
        f"  filtros: 30m+1h OK"
    )
    send_telegram(text)


# ── Main ──────────────────────────────────────────────────────────────────────
def main():
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    pairs = get_all_usdt_pairs()
    print(f"[{now}] Escaneando {len(pairs)} pares USDT ({' + '.join(INTERVALS)}) con {MAX_WORKERS} workers...")

    counts_history, last_seen = fetch_history()
    tasks = [(sym, tf) for sym in pairs for tf in INTERVALS]
    per_symbol = {sym: {} for sym in pairs}
    candidates = []
    processed = set()
    immediate_sent_keys = set()

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
            if not alert:
                continue

            candidates.append(alert)
            print(f"  ✅ {symbol} -> {alert['label']} {alert['timeframe']} score={alert['score']} late={alert['late']}")

            key = (alert["symbol"], alert["history_tf"])
            if alert.get("immediate") and key not in immediate_sent_keys:
                try:
                    send_immediate(alert, counts_history)
                    immediate_sent_keys.add(key)
                    print(f"  ⚡ inmediato enviado: {symbol} {alert['label']}")
                except Exception as e:
                    print(f"  ⚠ error envío inmediato {symbol}: {e}")

    if not candidates:
        print("Sin setups PRE-BREAK / BREAKOUT / HOLD-RETEST en este run.")
        return

    selected = dedupe_and_rank(candidates)
    if not selected:
        print("No hubo setups seleccionados tras dedupe/ranking.")
        return

    insert_history(selected)

    # Excluir del resumen lo ya enviado de inmediato para evitar duplicados
    summary_alerts = [a for a in selected if (a["symbol"], a["history_tf"]) not in immediate_sent_keys]
    if summary_alerts:
        bar = "━" * 24
        counts_by_label = {}
        for a in summary_alerts:
            counts_by_label[a["label"]] = counts_by_label.get(a["label"], 0) + 1
        summary = " • ".join(f"{k}:{v}" for k, v in counts_by_label.items())
        header = (
            f"{bar}\n"
            f"🎯 RESUMEN SETUPS • {now}\n"
            f"   {summary} • 5m + 15m + 30m + 1h\n"
            f"   5m PRE-BREAK • 15m BREAKOUT • filtros 30m/1h\n"
            f"   cooldown {STATE_COOLDOWN_MINUTES}m • {len(pairs)} pares\n"
            f"{bar}"
        )
        body = "\n\n".join(format_alert(alert, counts_history) for alert in summary_alerts)
        send_telegram(header + "\n" + body)

    print(f"\nTotal candidatos: {len(candidates)}")
    print(f"⚡ inmediatos: {len(immediate_sent_keys)}")
    print(f"✅ resumen final: {len(summary_alerts)}")


if __name__ == "__main__":
    main()
