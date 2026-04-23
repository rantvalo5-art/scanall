"""
Screener simplificado con prioridad real:
- PRE-BREAK: 5m cerca de romper, volumen creciendo, BB comprimida
- BREAKOUT: 15m rompe máximo reciente con volumen y expansión
- HOLD: 15m rompe y sostiene la zona

Además:
- calcula un score por símbolo
- elige el mejor setup por moneda
- rankea las mejores oportunidades
- clasifica en BEST / STRONG / WATCH
- manda solo pocas alertas, ordenadas por prioridad
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
TOP_ALERT_COUNT = 2

# Historial / spam
HISTORY_HOURS = 8
STATE_COOLDOWN_MINUTES = 60
LATE_REPEAT_COUNT = 1

# Indicadores simples
EMA_SLOW = 21
RECENT_LOOKBACK = 15
PREBREAK_NEAR_MAX = 0.010          # <= 1% debajo del máximo reciente
PREBREAK_MIN_VOL_RATIO = 1.20
PREBREAK_VOLUME_GROWTH_MIN = 1.10
PREBREAK_BB_WIDTH_MAX = 0.035

BREAKOUT_BUFFER = 0.001            # 0.1% arriba del máximo reciente
BREAKOUT_MIN_VOL_RATIO = 1.80
BREAKOUT_MAX_EXTENDED = 0.040      # no > 4% arriba del breakout
BREAKOUT_BB_EXPANSION_MIN = 0.12   # 12% más ancho que vela previa

HOLD_LOOKBACK_BARS = 8
HOLD_RECENT_BREAK_MAX_BARS = 5
HOLD_ZONE_BUFFER = 0.003           # puede perforar 0.3%
HOLD_PULLBACK_MAX = 0.030          # pullback máximo 3%
STRONG_CLOSE_MIN = 0.65            # cierre en 65% superior del rango
ONE_H_RESIST_LOOKBACK = 24
ONE_H_RESIST_BUFFER = 0.015

# Ranking final
BEST_MIN_SCORE = 10
STRONG_MIN_SCORE = 8
IMMEDIATE_MIN_SCORE = 9

# Confluencia multi-TF para BREAKOUT
# El 5m debe mostrar momentum activo antes de confirmar el 15m
BREAKOUT_5M_MIN_VOL_RATIO = 1.50   # volumen 5m > 1.5x promedio
BREAKOUT_5M_STRONG_CLOSE = True    # última vela 5m cierra en 65%+ del rango
# Estructura de la vela de breakout en 15m
BREAKOUT_MIN_BODY_PCT = 0.60       # cuerpo ocupa al menos 60% del rango HL


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
def safe_pct(a, b):
    return (a / b - 1) if b else 0.0


def close_position(close_value, high_value, low_value):
    rng = max(high_value - low_value, 1e-12)
    return (close_value - low_value) / rng


def in_cooldown(symbol, history_tf, last_seen):
    ts = last_seen.get((symbol, history_tf))
    if not ts:
        return False
    return datetime.now(timezone.utc) - ts < timedelta(minutes=STATE_COOLDOWN_MINUTES)


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


def final_bucket(score):
    if score >= BEST_MIN_SCORE:
        return "BEST"
    if score >= STRONG_MIN_SCORE:
        return "STRONG"
    return "WATCH"


# ── Análisis por timeframe ─────────────────────────────────────────────────────
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

    # Tamaño del cuerpo de la última vela como % del rango HL
    candle_range = max(high.iloc[-1] - low.iloc[-1], 1e-12)
    candle_body_pct = abs(close.iloc[-1] - df["open"].iloc[-1]) / candle_range

    recent_max = high.iloc[-(RECENT_LOOKBACK + 2):-2].max()
    near_recent_max = recent_max > 0 and 0 <= (recent_max - price) / recent_max <= PREBREAK_NEAR_MAX
    breakout = recent_max > 0 and price > recent_max * (1 + BREAKOUT_BUFFER)
    breakout_distance = safe_pct(price, recent_max)

    one_h_resist = high.iloc[-(ONE_H_RESIST_LOOKBACK + 2):-2].max()
    dist_to_res = (one_h_resist - price) / price if price > 0 else 0.0
    not_near_resistance = dist_to_res > ONE_H_RESIST_BUFFER or breakout

    # HOLD sencillo: detectar una ruptura reciente y medir qué pasó después
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
    }


# ── Clasificación ───────────────────────────────────────────────────────────────
def classify_symbol(symbol, tf_map, counts_history, last_seen):
    tf5 = tf_map.get("5m") or {}
    tf15 = tf_map.get("15m") or {}
    tf1h = tf_map.get("1h") or {}
    if not tf5 or not tf15 or not tf1h:
        return None

    if not (tf1h.get("ema_trend_up") and tf1h.get("not_near_resistance")):
        return None

    candidates = []

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
        reasons.append(f"5m a {near_pct:.2%} del máximo reciente")
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
            reasons.append("última vela 5m cerró fuerte")
        if tf5['width_curr'] <= 0.025:
            score += 1
            reasons.append(f"BB 5m bien comprimida ({tf5['width_curr']:.2%})")
        else:
            reasons.append(f"BB 5m todavía comprimida ({tf5['width_curr']:.2%})")
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
        })

    breakout_ok = (
        tf15.get("breakout")
        and tf15.get("vol_ratio", 0) >= BREAKOUT_MIN_VOL_RATIO
        and tf15.get("breakout_distance", 9) <= BREAKOUT_MAX_EXTENDED
        and tf15.get("width_expansion", -9) >= BREAKOUT_BB_EXPANSION_MIN
        # Filtro de estructura: la vela de breakout en 15m debe tener cuerpo sólido
        # y cierre en la parte alta del rango
        and tf15.get("strong_close", False)
        and tf15.get("candle_body_pct", 0) >= BREAKOUT_MIN_BODY_PCT
        # Confluencia multi-TF: el 5m debe mostrar momentum activo ahora mismo
        and tf5.get("vol_ratio", 0) >= BREAKOUT_5M_MIN_VOL_RATIO
        and tf5.get("strong_close", False)
    )
    if breakout_ok and not in_cooldown(symbol, "BREAKOUT", last_seen):
        prev = counts_history.get((symbol, "BREAKOUT"), 0)
        score = 4
        reasons = [f"15m rompió el máximo reciente (+{tf15['breakout_distance']:.2%})"]
        # Estructura de la vela de breakout
        body_pct = tf15.get("candle_body_pct", 0)
        if body_pct >= 0.75:
            score += 2
            reasons.append(f"vela 15m muy sólida (cuerpo {body_pct:.0%} del rango)")
        else:
            score += 1
            reasons.append(f"vela 15m sólida (cuerpo {body_pct:.0%} del rango)")
        # Confluencia 5m
        if tf5['vol_ratio'] >= 2.5:
            score += 2
            reasons.append(f"5m confirmando con volumen muy fuerte ({tf5['vol_ratio']:.1f}x)")
        else:
            score += 1
            reasons.append(f"5m confirmando con volumen ({tf5['vol_ratio']:.1f}x)")
        if tf15['width_expansion'] >= 0.25:
            score += 2
            reasons.append(f"expansión BB marcada ({tf15['width_expansion']:.0%})")
        else:
            score += 1
            reasons.append(f"expansión BB válida ({tf15['width_expansion']:.0%})")
        if tf15['strong_close']:
            score += 1
            reasons.append("cierre 15m fuerte")
        if tf15['breakout_distance'] <= 0.02:
            score += 1
            reasons.append("todavía no está muy extendida")
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
        })

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
        reasons.append("última vela post-break cierra fuerte")
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
        })

    if not candidates:
        return None

    candidates.sort(key=lambda x: (x["score"], x["priority"]), reverse=True)
    return candidates[0]


# ── Formato ───────────────────────────────────────────────────────────────────
def format_alert(alert, counts_history, with_reasons=True):
    prev = counts_history.get((alert["symbol"], alert["history_tf"]), 0)
    late_tag = " [LATE]" if alert.get("late") else ""
    hist_tag = f"  _(+{prev} en {HISTORY_HOURS}h)_" if prev > 0 else ""
    header = (
        f"[{alert['bucket']}] [{alert['label']}{late_tag}] {alert['symbol']} "
        f"score {alert['score']} • {alert['timeframe']} {binance_link(alert['symbol'])}{hist_tag}"
    )
    if not with_reasons:
        return header
    body = "\n".join(f"  - {r}" for r in alert["reasons"][:3])
    return f"{header}\n{body}"


def dedupe_and_rank(alerts):
    best = {}
    for alert in alerts:
        cur = best.get(alert["symbol"])
        if cur is None or (alert["score"], alert["priority"]) > (cur["score"], cur["priority"]):
            best[alert["symbol"]] = alert
    ranked = list(best.values())
    ranked.sort(key=lambda x: (x["score"], x["priority"]), reverse=True)
    return ranked[:TOP_ALERT_COUNT]


def send_immediate(alert, counts_history):
    send_telegram(f"🔥 PRIORITY NOW\n{format_alert(alert, counts_history)}")


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
            print(f"  ✅ {symbol} -> {alert['bucket']} {alert['label']} score={alert['score']} late={alert['late']}")

            key = (alert["symbol"], alert["history_tf"])
            if alert.get("immediate") and key not in immediate_sent_keys:
                try:
                    send_immediate(alert, counts_history)
                    immediate_sent_keys.add(key)
                except Exception as e:
                    print(f"  ⚠ error envío inmediato {symbol}: {e}")

    if not candidates:
        print("Sin setups en este run.")
        return

    selected = dedupe_and_rank(candidates)
    insert_history(selected)

    summary_lines = []
    for idx, alert in enumerate(selected, start=1):
        prefix = "👑 BEST" if idx == 1 else ("💪 STRONG" if idx == 2 else "👀 WATCH")
        summary_lines.append(f"{prefix} #{idx}\n{format_alert(alert, counts_history)}")

    body = "\n\n".join(summary_lines)
    header = (
        f"🎯 TOP SETUPS • {now}\n"
        f"solo top {TOP_ALERT_COUNT} ordenadas por prioridad real\n"
        f"1) score  2) estructura  3) espacio 1h\n"
        f"cooldown {STATE_COOLDOWN_MINUTES}m • {len(pairs)} pares\n"
    )
    send_telegram(header + "\n" + body)

    print(f"Total candidatos: {len(candidates)}")
    print(f"Inmediatos: {len(immediate_sent_keys)}")
    print(f"Top final: {len(selected)}")


if __name__ == "__main__":
    main()
