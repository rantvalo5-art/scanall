"""
Screener simplificado con prioridad real:
- PRE-BREAK: 5m cerca de romper, volumen creciendo, BB comprimida
- BREAKOUT: 15m rompe máximo reciente con volumen y expansión
- RIDING: breakout que sigue subiendo y sosteniendo — se repite cada run
- FADING: precio devolviendo después de un breakout — avisa una vez para salir
- HOLD: 15m rompe y sostiene la zona

Además:
- calcula un score por símbolo
- elige el mejor setup por moneda
- rankea las mejores oportunidades
- clasifica en BEST / STRONG / WATCH
- manda solo pocas alertas, ordenadas por prioridad
- RIDING no tiene penalización por repetición — se repite mientras sea válido
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

INTERVALS = ["5m", "15m", "1h"]
LIMIT = 180
TOP_N = 9999
MAX_WORKERS = 20
MIN_QUOTE_VOLUME = 300000
TOP_ALERT_COUNT = 3

# Historial / spam
HISTORY_HOURS = 8
LATE_REPEAT_COUNT = 1

# Cooldown por tipo de estado (minutos)
COOLDOWN_BY_STATE = {
    "PREBREAK": 0,
    "BREAKOUT": 0,
    "RIDING":   15,
    "FADING":  120,
    "HOLD":     45,
}

# ── Señales activas ────────────────────────────────────────────────────────────
# Controlado desde el panel HTML vía variable en screener.py
# Valores posibles: PREBREAK, BREAKOUT, RIDING, FADING, HOLD
ACTIVE_SIGNALS_PREBREAK = False
ACTIVE_SIGNALS_BREAKOUT = True
ACTIVE_SIGNALS_RIDING   = False
ACTIVE_SIGNALS_FADING   = False
ACTIVE_SIGNALS_HOLD     = False

# Indicadores simples
EMA_SLOW = 21
RECENT_LOOKBACK = 15
PREBREAK_NEAR_MAX = 0.010
PREBREAK_MIN_VOL_RATIO = 20.0
PREBREAK_VOLUME_GROWTH_MIN = 1.10
PREBREAK_BB_WIDTH_MAX = 0.035

BREAKOUT_BUFFER = 0.001
BREAKOUT_MIN_VOL_RATIO = 1.80
BREAKOUT_MAX_EXTENDED = 0.040
BREAKOUT_BB_EXPANSION_MIN = 0.12
BREAKOUT_5M_MIN_VOL_RATIO = 1.50
BREAKOUT_MIN_BODY_PCT = 0.60

HOLD_LOOKBACK_BARS = 8
HOLD_RECENT_BREAK_MAX_BARS = 5
HOLD_ZONE_BUFFER = 0.003
HOLD_PULLBACK_MAX = 0.030
STRONG_CLOSE_MIN = 0.65
ONE_H_RESIST_LOOKBACK = 24
ONE_H_RESIST_BUFFER = 0.015

RIDING_LOOKBACK_BARS = 20
RIDING_MIN_GAIN = 0.005
RIDING_MAX_GAIN = 0.15
RIDING_ZONE_BUFFER = 0.012
RIDING_MIN_VOL_RATIO = 1.20
RIDING_EMA_MUST_TREND = True

FADING_REVERSAL_MIN = 0.015
FADING_BELOW_ZONE = 0.008

BEST_MIN_SCORE = 10
STRONG_MIN_SCORE = 8
IMMEDIATE_MIN_SCORE = 9


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
        print(f"Supabase fetch_history error: {e}")
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
    except Exception as e:
        print(f"Supabase insert_history error: {e}")


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
def safe_pct(a, b):
    return (a / b - 1) if b else 0.0


def close_position(close_value, high_value, low_value):
    rng = max(high_value - low_value, 1e-12)
    return (close_value - low_value) / rng


def in_cooldown(symbol, history_tf, last_seen):
    ts = last_seen.get((symbol, history_tf))
    if not ts:
        return False
    cooldown_minutes = COOLDOWN_BY_STATE.get(history_tf, 60)
    return datetime.now(timezone.utc) - ts < timedelta(minutes=cooldown_minutes)


def send_telegram(text):
    requests.post(
        f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage",
        json={"chat_id": TELEGRAM_CHAT_ID, "text": text, "parse_mode": "Markdown", "disable_web_page_preview": True},
        timeout=10,
    ).raise_for_status()


def binance_link(symbol):
    pair = symbol[:-4] + "_USDT"
    url = f"https://www.binance.com/en/trade/{pair}?type=spot"
    return f"[{symbol}]({url})"


def final_bucket(score):
    if score >= BEST_MIN_SCORE:
        return "BEST"
    if score >= STRONG_MIN_SCORE:
        return "STRONG"
    return "WATCH"


# ── Analisis por timeframe ─────────────────────────────────────────────────────
def analyze(symbol, interval):
    try:
        df = get_klines(symbol, interval)
    except Exception:
        return symbol, interval, None

    if len(df) < 80:
        return symbol, interval, None

    close = df["close"]
    high = df["high"]
    low = df["low"]
    volume = df["volume"]
    price = close.iloc[-1]

    ema_slow = ta.trend.EMAIndicator(close, window=EMA_SLOW).ema_indicator()
    ema_trend_up = price > ema_slow.iloc[-1] and ema_slow.iloc[-1] > ema_slow.iloc[-4]

    bb = ta.volatility.BollingerBands(close, window=20, window_dev=2)
    hband = bb.bollinger_hband()
    lband = bb.bollinger_lband()
    mavg = bb.bollinger_mavg()
    width_curr = ((hband.iloc[-1] - lband.iloc[-1]) / mavg.iloc[-1]) if mavg.iloc[-1] else 0.0
    width_prev = ((hband.iloc[-2] - lband.iloc[-2]) / mavg.iloc[-2]) if mavg.iloc[-2] else 0.0
    width_expansion = safe_pct(width_curr, width_prev)

    vol_mean = volume.iloc[-21:-1].mean()
    vol_ratio = vol_mean and (volume.iloc[-1] / vol_mean) or 0.0
    vol_recent = volume.iloc[-3:].mean()
    vol_prev = volume.iloc[-6:-3].mean()
    vol_growth = vol_prev and (vol_recent / vol_prev) or 0.0

    close_pos = close_position(price, high.iloc[-1], low.iloc[-1])
    strong_close = close_pos >= STRONG_CLOSE_MIN

    candle_range = max(high.iloc[-1] - low.iloc[-1], 1e-12)
    candle_body_pct = abs(close.iloc[-1] - df["open"].iloc[-1]) / candle_range

    recent_max = high.iloc[-(RECENT_LOOKBACK + 2):-2].max()
    near_recent_max = recent_max > 0 and 0 <= (recent_max - price) / recent_max <= PREBREAK_NEAR_MAX
    breakout = recent_max > 0 and price > recent_max * (1 + BREAKOUT_BUFFER)
    breakout_distance = safe_pct(price, recent_max)

    one_h_resist = high.iloc[-(ONE_H_RESIST_LOOKBACK + 2):-2].max()
    dist_to_res = (one_h_resist - price) / price if price > 0 else 0.0
    not_near_resistance = dist_to_res > ONE_H_RESIST_BUFFER or breakout

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
    hold_strong = False
    bars_since_break = None
    if recent_break_idx is not None and recent_break_ref:
        bars_since_break = len(df) - 1 - recent_break_idx
        if 1 <= bars_since_break <= HOLD_RECENT_BREAK_MAX_BARS:
            post = df.iloc[recent_break_idx + 1:]
            if len(post) >= 1:
                hold_recent_break = True
                min_post_low = post["low"].min()
                min_post_close = post["close"].min()
                hold_kept_zone = min_post_low >= recent_break_ref * (1 - HOLD_ZONE_BUFFER)
                pullback = (recent_break_close - min_post_close) / recent_break_close if recent_break_close else 0.0
                hold_pullback_ok = pullback <= HOLD_PULLBACK_MAX
                last_post = post.iloc[-1]
                hold_strong = close_position(last_post["close"], last_post["high"], last_post["low"]) >= STRONG_CLOSE_MIN

    riding_break_idx = None
    riding_break_ref = None
    riding_break_close = None
    ride_start = max(25, len(df) - RIDING_LOOKBACK_BARS - 2)
    for idx in range(ride_start, len(df) - 1):
        ref_slice = high.iloc[max(0, idx - RECENT_LOOKBACK):idx]
        if len(ref_slice) < RECENT_LOOKBACK:
            continue
        ref = ref_slice.max()
        if close.iloc[idx] > ref * (1 + BREAKOUT_BUFFER):
            riding_break_idx = idx
            riding_break_ref = ref
            riding_break_close = close.iloc[idx]

    riding_bars_since = None
    riding_gain = None
    riding_above_zone = None
    riding_vol_ok = None
    post_break_high = None
    fading_reversal = None
    fading_below_zone = None

    if riding_break_idx is not None and riding_break_ref and riding_break_close:
        riding_bars_since = len(df) - 1 - riding_break_idx
        riding_gain = safe_pct(price, riding_break_close)
        post_slice = df.iloc[riding_break_idx + 1:]
        post_break_high = post_slice["high"].max() if len(post_slice) > 0 else price
        riding_above_zone = price >= riding_break_ref * (1 - RIDING_ZONE_BUFFER)
        vol_mean_riding = volume.iloc[-21:-1].mean()
        riding_vol_ok = vol_mean_riding > 0 and (volume.iloc[-3:].mean() / vol_mean_riding) >= RIDING_MIN_VOL_RATIO
        fading_reversal = safe_pct(price, post_break_high) if post_break_high else 0.0
        fading_below_zone = price < riding_break_ref * (1 - FADING_BELOW_ZONE)

    return symbol, interval, {
        "price": price,
        "ema_trend_up": ema_trend_up,
        "width_curr": width_curr,
        "width_expansion": width_expansion,
        "vol_ratio": vol_ratio,
        "vol_growth": vol_growth,
        "strong_close": strong_close,
        "candle_body_pct": candle_body_pct,
        "recent_max": recent_max,
        "near_recent_max": near_recent_max,
        "breakout": breakout,
        "breakout_distance": breakout_distance,
        "not_near_resistance": not_near_resistance,
        "dist_to_res": dist_to_res,
        "hold_recent_break": hold_recent_break,
        "hold_kept_zone": hold_kept_zone,
        "hold_pullback_ok": hold_pullback_ok,
        "hold_strong": hold_strong,
        "bars_since_break": bars_since_break,
        "riding_bars_since": riding_bars_since,
        "riding_gain": riding_gain,
        "riding_above_zone": riding_above_zone,
        "riding_vol_ok": riding_vol_ok,
        "riding_break_ref": riding_break_ref,
        "riding_break_close": riding_break_close,
        "post_break_high": post_break_high,
        "fading_reversal": fading_reversal,
        "fading_below_zone": fading_below_zone,
    }


# ── Clasificacion ───────────────────────────────────────────────────────────────
def classify_symbol(symbol, tf_map, counts_history, last_seen):
    tf5 = tf_map.get("5m") or {}
    tf15 = tf_map.get("15m") or {}
    tf1h = tf_map.get("1h") or {}
    if not tf5 or not tf15 or not tf1h:
        return None

    if not (tf1h.get("ema_trend_up") and tf1h.get("not_near_resistance")):
        return None

    candidates = []

    # ── PRE-BREAK ──────────────────────────────────────────────────────────────
    if ACTIVE_SIGNALS_PREBREAK:
        pre_ok = (
            tf5.get("near_recent_max")
            and tf5.get("width_curr", 9) <= PREBREAK_BB_WIDTH_MAX
            and tf5.get("vol_ratio", 0) >= PREBREAK_MIN_VOL_RATIO
            and tf5.get("vol_growth", 0) >= PREBREAK_VOLUME_GROWTH_MIN
        )
        if pre_ok and not in_cooldown(symbol, "PREBREAK", last_seen):
            prev = counts_history.get((symbol, "PREBREAK"), 0)
            score = 3
            reasons = []
            near_pct = ((tf5['recent_max'] - tf5['price']) / tf5['recent_max']) if tf5['recent_max'] else 0.0
            reasons.append(f"5m a {near_pct:.2%} del maximo reciente")
            if tf5['vol_ratio'] >= 1.6:
                score += 2
                reasons.append(f"volumen 5m fuerte ({tf5['vol_ratio']:.1f}x)")
            else:
                score += 1
                reasons.append(f"volumen 5m arriba de lo normal ({tf5['vol_ratio']:.1f}x)")
            if tf5['vol_growth'] >= 1.3:
                score += 2
                reasons.append(f"volumen creciendo bien ({tf5['vol_growth']:.2f}x)")
            else:
                score += 1
                reasons.append(f"volumen creciendo ({tf5['vol_growth']:.2f}x)")
            if tf5['strong_close']:
                score += 1
                reasons.append("ultima vela 5m cerro fuerte")
            if tf5['width_curr'] <= 0.025:
                score += 1
                reasons.append(f"BB 5m bien comprimida ({tf5['width_curr']:.2%})")
            else:
                reasons.append(f"BB 5m todavia comprimida ({tf5['width_curr']:.2%})")
            if tf1h['dist_to_res'] > 0.04:
                score += 2
                reasons.append(f"1h con buen espacio arriba ({tf1h['dist_to_res']:.2%})")
            else:
                score += 1
                reasons.append("1h alcista y con espacio")
            if prev >= LATE_REPEAT_COUNT:
                score -= 1
            candidates.append({
                "symbol": symbol,
                "label": "PRE-BREAK",
                "history_tf": "PREBREAK",
                "score": score,
                "priority": 1,
                "bucket": final_bucket(score),
                "reasons": reasons,
                "late": prev >= LATE_REPEAT_COUNT,
                "timeframe": "5m",
                "immediate": prev < LATE_REPEAT_COUNT and score >= IMMEDIATE_MIN_SCORE,
                "price": tf5["price"],
                "ref_price": tf5["recent_max"],
            })

    # ── BREAKOUT ───────────────────────────────────────────────────────────────
    if ACTIVE_SIGNALS_BREAKOUT:
        breakout_ok = (
            tf15.get("breakout")
            and tf15.get("vol_ratio", 0) >= BREAKOUT_MIN_VOL_RATIO
            and tf15.get("breakout_distance", 9) <= BREAKOUT_MAX_EXTENDED
            and tf15.get("width_expansion", -9) >= BREAKOUT_BB_EXPANSION_MIN
            and tf15.get("strong_close", False)
            and tf15.get("candle_body_pct", 0) >= BREAKOUT_MIN_BODY_PCT
            and tf5.get("vol_ratio", 0) >= BREAKOUT_5M_MIN_VOL_RATIO
            and tf5.get("strong_close", False)
        )
        if breakout_ok and not in_cooldown(symbol, "BREAKOUT", last_seen):
            prev = counts_history.get((symbol, "BREAKOUT"), 0)
            score = 4
            reasons = [f"15m rompio el maximo reciente (+{tf15['breakout_distance']:.2%})"]
            body_pct = tf15.get("candle_body_pct", 0)
            if body_pct >= 0.75:
                score += 2
                reasons.append(f"vela 15m muy solida (cuerpo {body_pct:.0%} del rango)")
            else:
                score += 1
                reasons.append(f"vela 15m solida (cuerpo {body_pct:.0%} del rango)")
            if tf5['vol_ratio'] >= 2.5:
                score += 2
                reasons.append(f"5m confirmando con volumen muy fuerte ({tf5['vol_ratio']:.1f}x)")
            else:
                score += 1
                reasons.append(f"5m confirmando con volumen ({tf5['vol_ratio']:.1f}x)")
            if tf15['width_expansion'] >= 0.25:
                score += 2
                reasons.append(f"expansion BB marcada ({tf15['width_expansion']:.0%})")
            else:
                score += 1
                reasons.append(f"expansion BB valida ({tf15['width_expansion']:.0%})")
            if tf15['strong_close']:
                score += 1
                reasons.append("cierre 15m fuerte")
            if tf15['breakout_distance'] <= 0.02:
                score += 1
                reasons.append("todavia no esta muy extendida")
            if tf1h['dist_to_res'] > 0.04:
                score += 2
                reasons.append(f"1h con espacio real ({tf1h['dist_to_res']:.2%})")
            else:
                score += 1
                reasons.append("1h alcista y con espacio")
            if prev >= LATE_REPEAT_COUNT:
                score -= 1
            candidates.append({
                "symbol": symbol,
                "label": "BREAKOUT",
                "history_tf": "BREAKOUT",
                "score": score,
                "priority": 2,
                "bucket": final_bucket(score),
                "reasons": reasons,
                "late": prev >= LATE_REPEAT_COUNT,
                "timeframe": "15m",
                "immediate": prev < LATE_REPEAT_COUNT and score >= IMMEDIATE_MIN_SCORE,
                "price": tf15["price"],
                "ref_price": tf15["recent_max"],
            })

    # ── RIDING ─────────────────────────────────────────────────────────────────
    if ACTIVE_SIGNALS_RIDING:
        riding_gain = tf15.get("riding_gain") or 0.0
        riding_ok = (
            tf15.get("riding_above_zone")
            and tf15.get("riding_vol_ok")
            and RIDING_MIN_GAIN <= riding_gain <= RIDING_MAX_GAIN
            and tf15.get("riding_bars_since") is not None
            and tf15["riding_bars_since"] >= 1
            and (not RIDING_EMA_MUST_TREND or tf1h.get("ema_trend_up"))
            and not tf15.get("breakout")
        )
        if riding_ok and not in_cooldown(symbol, "RIDING", last_seen):
            prev = counts_history.get((symbol, "RIDING"), 0)
            score = 6
            reasons = [
                f"sigue subiendo desde el breakout (+{riding_gain:.2%}, "
                f"{tf15['riding_bars_since']} velas atras)"
            ]
            if riding_gain >= 0.05:
                score += 3
                reasons.append(f"movimiento fuerte (+{riding_gain:.2%} total)")
            elif riding_gain >= 0.02:
                score += 2
                reasons.append(f"ganancia solida (+{riding_gain:.2%} total)")
            else:
                score += 1
                reasons.append(f"ganancia inicial (+{riding_gain:.2%} total)")
            if tf15.get("riding_vol_ok"):
                score += 1
                reasons.append("volumen sostenido — no hay colapso de momentum")
            if tf15.get("strong_close"):
                score += 1
                reasons.append("ultima vela 15m cierra fuerte")
            if tf1h.get("ema_trend_up"):
                score += 1
                reasons.append("1h EMA sigue alcista")
            if tf1h.get("dist_to_res", 0) > 0.04:
                score += 2
                reasons.append(f"1h con espacio real ({tf1h['dist_to_res']:.2%})")
            else:
                score += 1
                reasons.append("1h alcista")
            candidates.append({
                "symbol": symbol,
                "label": "RIDING",
                "history_tf": "RIDING",
                "score": score,
                "priority": 2,
                "bucket": final_bucket(score),
                "reasons": reasons,
                "late": False,
                "timeframe": "15m",
                "immediate": score >= IMMEDIATE_MIN_SCORE,
                "riding_repeat": prev,
                "price": tf15["price"],
                "ref_price": tf15.get("riding_break_close"),
            })

    # ── FADING ─────────────────────────────────────────────────────────────────
    if ACTIVE_SIGNALS_FADING:
        fading_reversal = tf15.get("fading_reversal") or 0.0
        fading_ok = (
            tf15.get("riding_bars_since") is not None
            and tf15["riding_bars_since"] >= 1
            and fading_reversal <= -FADING_REVERSAL_MIN
            and (
                tf15.get("fading_below_zone")
                or fading_reversal <= -0.03
            )
            and not tf15.get("breakout")
        )
        if fading_ok and not in_cooldown(symbol, "FADING", last_seen):
            score = 5
            reasons = [f"devolviendo {abs(fading_reversal):.2%} desde el maximo post-break"]
            if tf15.get("fading_below_zone"):
                score += 2
                reasons.append("precio perforo la zona de soporte rota — senal de salida")
            else:
                score += 1
                reasons.append("pullback significativo — monitoreá zona de soporte")
            if not tf15.get("riding_vol_ok"):
                score += 1
                reasons.append("volumen tambien cayo — momentum perdido")
            candidates.append({
                "symbol": symbol,
                "label": "FADING",
                "history_tf": "FADING",
                "score": score,
                "priority": 4,
                "bucket": "WATCH",
                "reasons": reasons,
                "late": False,
                "timeframe": "15m",
                "immediate": bool(tf15.get("fading_below_zone")),
                "price": tf15["price"],
                "ref_price": tf15.get("post_break_high"),
            })

    # ── HOLD ───────────────────────────────────────────────────────────────────
    if ACTIVE_SIGNALS_HOLD:
        hold_ok = (
            tf15.get("hold_recent_break")
            and tf15.get("hold_kept_zone")
            and tf15.get("hold_pullback_ok")
            and tf15.get("hold_strong")
        )
        if hold_ok and not in_cooldown(symbol, "HOLD", last_seen):
            prev = counts_history.get((symbol, "HOLD"), 0)
            score = 5
            reasons = [f"ruptura reciente en 15m hace {tf15['bars_since_break']} velas"]
            score += 2
            reasons.append("sigue arriba de la resistencia rota")
            score += 1
            reasons.append("pullback sano")
            score += 1
            reasons.append("ultima vela post-break cierra fuerte")
            if tf1h['dist_to_res'] > 0.04:
                score += 2
                reasons.append(f"1h con buen espacio arriba ({tf1h['dist_to_res']:.2%})")
            else:
                score += 1
                reasons.append("1h acompaña")
            if prev >= LATE_REPEAT_COUNT:
                score -= 1
            candidates.append({
                "symbol": symbol,
                "label": "HOLD",
                "history_tf": "HOLD",
                "score": score,
                "priority": 3,
                "bucket": final_bucket(score),
                "reasons": reasons,
                "late": prev >= LATE_REPEAT_COUNT,
                "timeframe": "15m",
                "immediate": False,
                "price": tf15["price"],
                "ref_price": tf15.get("riding_break_ref") or tf15["recent_max"],
            })

    if not candidates:
        return None

    candidates.sort(key=lambda x: (x["score"], x["priority"]), reverse=True)
    return candidates[0]


# ── Formato ───────────────────────────────────────────────────────────────────
def format_alert(alert, counts_history, with_reasons=True):
    prev = counts_history.get((alert["symbol"], alert["history_tf"]), 0)
    late_tag = " [LATE]" if alert.get("late") else ""

    if alert["history_tf"] == "RIDING" and prev > 0:
        hist_tag = f"  _(confirmo {prev}x en {HISTORY_HOURS}h)_"
    elif alert["history_tf"] == "FADING":
        hist_tag = "  _(SALIDA)_"
    elif prev > 0:
        hist_tag = f"  _(+{prev} en {HISTORY_HOURS}h)_"
    else:
        hist_tag = ""

    emoji = {
        "RIDING": "🚀",
        "FADING": "⚠️",
        "BREAKOUT": "💥",
        "PRE-BREAK": "👀",
        "HOLD": "🤝",
    }.get(alert["label"], "")

    header = (
        f"{emoji} [{alert['bucket']}] [{alert['label']}{late_tag}] {alert['symbol']} "
        f"score {alert['score']} • {alert['timeframe']} {binance_link(alert['symbol'])}{hist_tag}"
    )

    if not with_reasons:
        return header

    # Línea de precio con % desde referencia
    price = alert.get("price", 0)
    ref = alert.get("ref_price") or 0
    if ref and ref > 0 and price > 0:
        pct = (price - ref) / ref * 100
        if alert["history_tf"] == "FADING":
            price_line = f"  💰 {price:.6g} USDT ({pct:+.2f}% desde máx post-break)"
        elif alert["history_tf"] == "RIDING":
            price_line = f"  💰 {price:.6g} USDT ({pct:+.2f}% desde breakout)"
        else:
            price_line = f"  💰 {price:.6g} USDT ({pct:+.2f}% desde zona de ruptura)"
    else:
        price_line = f"  💰 {price:.6g} USDT"

    body = "\n".join(f"  - {r}" for r in alert["reasons"][:3])
    return f"{header}\n{price_line}\n{body}"


def dedupe_and_rank(alerts):
    best = {}
    for alert in alerts:
        cur = best.get(alert["symbol"])
        if cur is None:
            best[alert["symbol"]] = alert
        elif alert["history_tf"] == "FADING" and cur["history_tf"] != "FADING":
            best[alert["symbol"]] = alert
        elif cur["history_tf"] != "FADING" and (alert["score"], alert["priority"]) > (cur["score"], cur["priority"]):
            best[alert["symbol"]] = alert
    ranked = list(best.values())
    ranked.sort(key=lambda x: (
        1 if x["history_tf"] == "FADING" else 0,
        x["score"],
        x["priority"],
    ), reverse=True)
    return ranked[:TOP_ALERT_COUNT]


def send_immediate(alert, counts_history):
    label = "⚠️ SALIDA — precio devolviendo" if alert["history_tf"] == "FADING" else "🔥 PRIORITY NOW"
    send_telegram(f"{label}\n{format_alert(alert, counts_history)}")


# ── Main ──────────────────────────────────────────────────────────────────────
def main():
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    pairs = get_all_usdt_pairs()

    # Resumen de señales activas para el log
    active_names = []
    if ACTIVE_SIGNALS_PREBREAK: active_names.append("PRE-BREAK")
    if ACTIVE_SIGNALS_BREAKOUT: active_names.append("BREAKOUT")
    if ACTIVE_SIGNALS_RIDING:   active_names.append("RIDING")
    if ACTIVE_SIGNALS_FADING:   active_names.append("FADING")
    if ACTIVE_SIGNALS_HOLD:     active_names.append("HOLD")

    print(f"[{now}] Escaneando {len(pairs)} pares USDT ({' + '.join(INTERVALS)}) con {MAX_WORKERS} workers...")
    print(f"Señales activas: {', '.join(active_names) if active_names else 'NINGUNA'}")

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
            print(f"  {alert['label']} {symbol} score={alert['score']} bucket={alert['bucket']}")

            key = (alert["symbol"], alert["history_tf"])
            if alert.get("immediate") and key not in immediate_sent_keys:
                try:
                    send_immediate(alert, counts_history)
                    immediate_sent_keys.add(key)
                except Exception as e:
                    print(f"  error envio inmediato {symbol}: {e}")

    if not candidates:
        print("Sin setups en este run.")
        return

    selected = dedupe_and_rank(candidates)
    insert_history(selected)

    summary_lines = []
    for idx, alert in enumerate(selected, start=1):
        if alert["history_tf"] == "FADING":
            prefix = "⚠️ SALIDA"
        elif alert["history_tf"] == "RIDING":
            prefix = f"🚀 RIDING #{idx}"
        elif idx == 1:
            prefix = "👑 BEST"
        else:
            prefix = "💪 STRONG"
        summary_lines.append(f"{prefix}\n{format_alert(alert, counts_history)}")

    body = "\n\n".join(summary_lines)

    riding_count = sum(1 for a in candidates if a["history_tf"] == "RIDING")
    fading_count = sum(1 for a in candidates if a["history_tf"] == "FADING")
    extra = ""
    if riding_count:
        extra += f"| 🚀 {riding_count} riding "
    if fading_count:
        extra += f"| ⚠️ {fading_count} fading "

    active_tag = " • ".join(active_names) if active_names else "ninguna"
    header = (
        f"🎯 TOP SETUPS • {now}\n"
        f"top {TOP_ALERT_COUNT} {extra}• {len(pairs)} pares\n"
        f"señales: {active_tag}\n"
    )
    send_telegram(header + "\n" + body)

    print(f"Total candidatos: {len(candidates)} (RIDING: {riding_count}, FADING: {fading_count})")
    print(f"Inmediatos: {len(immediate_sent_keys)}")
    print(f"Top final: {len(selected)}")


if __name__ == "__main__":
    main()
