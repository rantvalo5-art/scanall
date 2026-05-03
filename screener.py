"""
Screener simplificado con prioridad real:
- PRE-BREAK: 5m cerca de romper, volumen creciendo, BB comprimida
- BREAKOUT: 15m rompe máximo reciente con volumen y expansión
- RIDING: breakout que sigue subiendo y sosteniendo — se repite cada run
- FADING: precio devolviendo después de un breakout — avisa una vez para salir
- HOLD: 15m rompe y sostiene la zonas

Configuración en config.json (raíz del repo). Si falta o está roto, el script aborta.
"""

import json
import os
from datetime import datetime, timezone, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
from io import BytesIO
from pathlib import Path

import pandas as pd
import requests
import ta

# mplfinance es opcional: si no está instalado, los charts se desactivan en runtime
try:
    import matplotlib
    matplotlib.use("Agg")  # backend sin display, necesario en CI
    import mplfinance as mpf
    _HAS_MPLFINANCE = True
except ImportError:
    _HAS_MPLFINANCE = False

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
    """Acceso estricto: si falta una clave, abortamos para que el error sea visible."""
    try:
        return CONFIG[section][key]
    except KeyError:
        raise SystemExit(f"FATAL: config.json no tiene {section}.{key}")

# ── Variables de entorno ──────────────────────────────────────────────────────
TELEGRAM_TOKEN = os.environ["TELEGRAM_TOKEN"]
TELEGRAM_CHAT_ID = os.environ["TELEGRAM_CHAT_ID"]
SUPABASE_KEY = os.environ["SUPABASE_KEY"]
SUPABASE_URL = "https://ecgdswroygkfckkaguxp.supabase.co"

# ── Configuración derivada ────────────────────────────────────────────────────
INTERVALS         = _cfg("general", "INTERVALS")
LIMIT             = _cfg("general", "LIMIT")
TOP_N             = _cfg("general", "TOP_N")
MAX_WORKERS       = _cfg("general", "MAX_WORKERS")
MIN_QUOTE_VOLUME  = _cfg("general", "MIN_QUOTE_VOLUME")
TOP_ALERT_COUNT   = _cfg("general", "TOP_ALERT_COUNT")

HISTORY_HOURS            = _cfg("history", "HISTORY_HOURS")
LATE_REPEAT_COUNT        = _cfg("history", "LATE_REPEAT_COUNT")
SNAPSHOT_RETENTION_DAYS  = _cfg("history", "SNAPSHOT_RETENTION_DAYS")

MAX_IMMEDIATE_PER_RUN = _cfg("anti_spam", "MAX_IMMEDIATE_PER_RUN")
BURST_THRESHOLD       = _cfg("anti_spam", "BURST_THRESHOLD")

COOLDOWN_BY_STATE = CONFIG.get("cooldowns_minutes", {})

ACTIVE_SIGNALS_PREBREAK = _cfg("active_signals", "PREBREAK")
ACTIVE_SIGNALS_BREAKOUT = _cfg("active_signals", "BREAKOUT")
ACTIVE_SIGNALS_RIDING   = _cfg("active_signals", "RIDING")
ACTIVE_SIGNALS_FADING   = _cfg("active_signals", "FADING")
ACTIVE_SIGNALS_HOLD     = _cfg("active_signals", "HOLD")

EMA_SLOW        = _cfg("indicators", "EMA_SLOW")
RECENT_LOOKBACK = _cfg("indicators", "RECENT_LOOKBACK")
ATR_MIN_PCT     = _cfg("indicators", "ATR_MIN_PCT")

PREBREAK_NEAR_MAX           = _cfg("prebreak", "PREBREAK_NEAR_MAX")
PREBREAK_MIN_VOL_RATIO      = _cfg("prebreak", "PREBREAK_MIN_VOL_RATIO")
PREBREAK_VOLUME_GROWTH_MIN  = _cfg("prebreak", "PREBREAK_VOLUME_GROWTH_MIN")
PREBREAK_BB_WIDTH_MAX       = _cfg("prebreak", "PREBREAK_BB_WIDTH_MAX")

BREAKOUT_BUFFER             = _cfg("breakout", "BREAKOUT_BUFFER")
BREAKOUT_MIN_VOL_RATIO      = _cfg("breakout", "BREAKOUT_MIN_VOL_RATIO")
BREAKOUT_MAX_EXTENDED       = _cfg("breakout", "BREAKOUT_MAX_EXTENDED")
BREAKOUT_BB_EXPANSION_MIN   = _cfg("breakout", "BREAKOUT_BB_EXPANSION_MIN")
BREAKOUT_5M_MIN_VOL_RATIO   = _cfg("breakout", "BREAKOUT_5M_MIN_VOL_RATIO")
BREAKOUT_MIN_BODY_PCT       = _cfg("breakout", "BREAKOUT_MIN_BODY_PCT")

HOLD_LOOKBACK_BARS          = _cfg("hold", "HOLD_LOOKBACK_BARS")
HOLD_RECENT_BREAK_MAX_BARS  = _cfg("hold", "HOLD_RECENT_BREAK_MAX_BARS")
HOLD_ZONE_BUFFER            = _cfg("hold", "HOLD_ZONE_BUFFER")
HOLD_PULLBACK_MAX           = _cfg("hold", "HOLD_PULLBACK_MAX")
STRONG_CLOSE_MIN            = _cfg("hold", "STRONG_CLOSE_MIN")
ONE_H_RESIST_LOOKBACK       = _cfg("hold", "ONE_H_RESIST_LOOKBACK")
ONE_H_RESIST_BUFFER         = _cfg("hold", "ONE_H_RESIST_BUFFER")

RIDING_LOOKBACK_BARS    = _cfg("riding", "RIDING_LOOKBACK_BARS")
RIDING_MIN_GAIN         = _cfg("riding", "RIDING_MIN_GAIN")
RIDING_MAX_GAIN         = _cfg("riding", "RIDING_MAX_GAIN")
RIDING_ZONE_BUFFER      = _cfg("riding", "RIDING_ZONE_BUFFER")
RIDING_MIN_VOL_RATIO    = _cfg("riding", "RIDING_MIN_VOL_RATIO")
RIDING_EMA_MUST_TREND   = _cfg("riding", "RIDING_EMA_MUST_TREND")

FADING_REVERSAL_MIN = _cfg("fading", "FADING_REVERSAL_MIN")
FADING_BELOW_ZONE   = _cfg("fading", "FADING_BELOW_ZONE")

BEST_MIN_SCORE      = _cfg("scoring", "BEST_MIN_SCORE")
STRONG_MIN_SCORE    = _cfg("scoring", "STRONG_MIN_SCORE")
IMMEDIATE_MIN_SCORE = _cfg("scoring", "IMMEDIATE_MIN_SCORE")
FORMING_CANDLE_PENALTY = _cfg("scoring", "FORMING_CANDLE_PENALTY")

CHART_ENABLED = _cfg("chart", "ENABLED")
CHART_BARS    = _cfg("chart", "BARS")
CHART_WIDTH   = _cfg("chart", "WIDTH")
CHART_HEIGHT  = _cfg("chart", "HEIGHT")
CHART_STYLE   = _cfg("chart", "STYLE")

OUTCOMES_ENABLED              = _cfg("outcomes", "ENABLED")
OUTCOMES_TRACK_ALL_CANDIDATES = _cfg("outcomes", "TRACK_ALL_CANDIDATES")


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


def insert_pairs_snapshot(pairs):
    """Snapshot de qué pares cotizaban en este run + auto-purge de filas viejas.
    Sirve para reconstruir el universo histórico cuando armemos tracking de outcomes."""
    if not pairs:
        return
    now = datetime.now(timezone.utc)
    now_iso = now.isoformat()
    purge_before = (now - timedelta(days=SNAPSHOT_RETENTION_DAYS)).isoformat()
    rows = [{"run_at": now_iso, "symbol": s} for s in pairs]
    try:
        # Insert en batches por si Supabase tiene límite de payload
        BATCH = 500
        for i in range(0, len(rows), BATCH):
            r = requests.post(
                f"{SUPABASE_URL}/rest/v1/screener_pairs_snapshot",
                headers=_sb_headers(),
                json=rows[i:i + BATCH],
                timeout=15,
            )
            r.raise_for_status()
        # Auto-purge
        r2 = requests.delete(
            f"{SUPABASE_URL}/rest/v1/screener_pairs_snapshot",
            headers=_sb_headers(),
            params={"run_at": f"lt.{purge_before}"},
            timeout=15,
        )
        r2.raise_for_status()
        print(f"  snapshot: {len(rows)} pares guardados (purge >{SNAPSHOT_RETENTION_DAYS}d)")
    except Exception as e:
        print(f"Supabase insert_pairs_snapshot error: {e}")


def insert_outcomes(alerts):
    """Inserta filas en screener_outcomes para tracking de cómo evolucionan las alertas.
    Solo guarda los datos del momento de la alerta — los outcomes (precio +15m/+1h/+4h/+24h)
    los completa el job update_outcomes.py que corre después."""
    if not OUTCOMES_ENABLED or not alerts:
        return
    now_iso = datetime.now(timezone.utc).isoformat()
    rows = []
    for a in alerts:
        rows.append({
            "alerted_at": now_iso,
            "symbol": a["symbol"],
            "signal_type": a["history_tf"],
            "timeframe": a["timeframe"],
            "score": int(a["score"]),
            "bucket": a.get("bucket"),
            "entry_price": float(a["price"]),
            "ref_price": float(a["ref_price"]) if a.get("ref_price") else None,
            "candle_status": a.get("candle_status", "closed"),
        })
    try:
        r = requests.post(
            f"{SUPABASE_URL}/rest/v1/screener_outcomes",
            headers=_sb_headers(),
            json=rows,
            timeout=10,
        )
        r.raise_for_status()
        print(f"  outcomes: {len(rows)} alertas trackeadas")
    except Exception as e:
        print(f"Supabase insert_outcomes error: {e}")


# ── Datos Binance ──────────────────────────────────────────────────────────────
def get_active_usdt_symbols():
    r = requests.get("https://data-api.binance.vision/api/v3/exchangeInfo", timeout=20)
    r.raise_for_status()
    return {
        s["symbol"]
        for s in r.json()["symbols"]
        if s["symbol"].endswith("USDT") and s["status"] == "TRADING"
    }


def get_all_usdt_pairs(n=None):
    if n is None:
        n = TOP_N
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
    # close_time viene en ms UTC desde Binance; lo dejo como int para usar luego
    df["close_time"] = df["close_time"].astype("int64")
    df["open_time"] = df["open_time"].astype("int64")
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


# TF del screener → intervalo de TradingView
TV_TF_MAP = {
    "1m": "1", "5m": "5", "15m": "15", "30m": "30",
    "1h": "60", "2h": "120", "4h": "240", "1d": "D",
}


def tradingview_link(symbol, timeframe="15m"):
    tv_interval = TV_TF_MAP.get(timeframe, "15")
    url = f"https://www.tradingview.com/chart/?symbol=BINANCE:{symbol}&interval={tv_interval}"
    return f"[{symbol}]({url})"


# ── Chart image (mplfinance) ──────────────────────────────────────────────────
def generate_chart_image(df, symbol, timeframe, ref_price=None):
    """Genera una PNG en memoria con las últimas N velas + BB(20,2) + EMA21 + volumen.
    Si ref_price está, dibuja una línea horizontal (zona de ruptura/breakout/etc).
    Devuelve bytes (BytesIO) o None si mplfinance no está disponible o falla."""
    if not _HAS_MPLFINANCE or not CHART_ENABLED:
        return None
    try:
        # Últimas N velas
        chart_df = df.tail(CHART_BARS).copy()
        # mplfinance espera columnas Open/High/Low/Close/Volume capitalizadas con DatetimeIndex
        chart_df["timestamp"] = pd.to_datetime(chart_df["open_time"], unit="ms", utc=True)
        chart_df = chart_df.set_index("timestamp")
        chart_df = chart_df.rename(columns={
            "open": "Open", "high": "High", "low": "Low",
            "close": "Close", "volume": "Volume",
        })
        chart_df = chart_df[["Open", "High", "Low", "Close", "Volume"]]

        # Calculo BB y EMA sobre el df recortado para que coincida el largo
        bb = ta.volatility.BollingerBands(chart_df["Close"], window=20, window_dev=2)
        hband = bb.bollinger_hband()
        lband = bb.bollinger_lband()
        mavg  = bb.bollinger_mavg()
        ema21 = ta.trend.EMAIndicator(chart_df["Close"], window=EMA_SLOW).ema_indicator()

        addplots = [
            mpf.make_addplot(hband, color="#888", width=0.7),
            mpf.make_addplot(lband, color="#888", width=0.7),
            mpf.make_addplot(mavg,  color="#888", width=0.5, linestyle="--"),
            mpf.make_addplot(ema21, color="#e3b341", width=1.0),
        ]

        hlines = None
        if ref_price and ref_price > 0:
            hlines = dict(hlines=[float(ref_price)], colors=["#39d353"], linestyle="--", linewidths=0.8)

        buf = BytesIO()
        # figratio acepta tuplas; el tamaño final se controla con figscale
        # WIDTH/HEIGHT en px → convertir a pulgadas asumiendo 100 dpi
        fig_w_inch = CHART_WIDTH / 100
        fig_h_inch = CHART_HEIGHT / 100

        kwargs = dict(
            type="candle",
            style=CHART_STYLE,
            addplot=addplots,
            volume=True,
            figsize=(fig_w_inch, fig_h_inch),
            title=f"\n{symbol} • {timeframe}",
            tight_layout=True,
            savefig=dict(fname=buf, format="png", dpi=100, bbox_inches="tight"),
        )
        if hlines:
            kwargs["hlines"] = hlines

        mpf.plot(chart_df, **kwargs)
        buf.seek(0)
        return buf
    except Exception as e:
        print(f"  generate_chart_image error {symbol} {timeframe}: {e}")
        return None


def send_telegram_photo(image_buf, caption):
    """Envía una imagen a Telegram con caption. La caption tiene límite de 1024 chars."""
    if len(caption) > 1024:
        caption = caption[:1020] + "…"
    try:
        files = {"photo": ("chart.png", image_buf, "image/png")}
        data = {
            "chat_id": TELEGRAM_CHAT_ID,
            "caption": caption,
            "parse_mode": "Markdown",
        }
        r = requests.post(
            f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendPhoto",
            data=data,
            files=files,
            timeout=15,
        )
        r.raise_for_status()
    except Exception as e:
        print(f"  send_telegram_photo error: {e}")
        # Fallback: si falla la imagen, mando solo el texto para no perder la alerta
        try:
            send_telegram(caption)
        except Exception:
            pass


def final_bucket(score):
    if score >= BEST_MIN_SCORE:
        return "BEST"
    if score >= STRONG_MIN_SCORE:
        return "STRONG"
    return "WATCH"


# ── Contexto de mercado (BTC) ─────────────────────────────────────────────────
def get_btc_context():
    """Régimen de BTC en 4h para el header del batch."""
    try:
        df = get_klines("BTCUSDT", "4h")
        if len(df) < 30:
            return ""
        close = df["close"]
        last = close.iloc[-1]
        prev = close.iloc[-2]
        change_pct = (last / prev - 1) * 100

        ema_slow = ta.trend.EMAIndicator(close, window=EMA_SLOW).ema_indicator()
        trend_up = last > ema_slow.iloc[-1] and ema_slow.iloc[-1] > ema_slow.iloc[-4]
        trend_down = last < ema_slow.iloc[-1] and ema_slow.iloc[-1] < ema_slow.iloc[-4]

        if trend_up:
            trend_label = "alcista 🟢"
        elif trend_down:
            trend_label = "bajista 🔴"
        else:
            trend_label = "lateral 🟡"

        return f"BTC 4h: {change_pct:+.2f}% • tendencia: {trend_label}"
    except Exception as e:
        print(f"get_btc_context error: {e}")
        return ""


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

    # ATR como % del precio — mide volatilidad real del par
    atr_indicator = ta.volatility.AverageTrueRange(high, low, close, window=14)
    atr = atr_indicator.average_true_range().iloc[-1]
    atr_pct = (atr / price * 100) if price > 0 else 0.0

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

    # Detección vela cerrada vs en formación.
    # Binance entrega close_time como ms UTC del último ms de la vela.
    # Si now < close_time, la vela todavía se está formando.
    last_close_time_ms = int(df["close_time"].iloc[-1])
    now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
    candle_status = "closed" if now_ms >= last_close_time_ms else "forming"

    return symbol, interval, {
        "price": price,
        "ema_trend_up": ema_trend_up,
        "width_curr": width_curr,
        "width_expansion": width_expansion,
        "atr_pct": atr_pct,
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
        "candle_status": candle_status,
        # df completo guardado SOLO para generar charts después; no se usa en classify
        "_df": df,
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

    # Filtro ATR: descartar pares muertos (volatilidad insuficiente en 1h)
    if tf1h.get("atr_pct", 0) < ATR_MIN_PCT:
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

            # Score multiplicativo: factores clave se multiplican, no se suman
            vol_factor = min(tf5['vol_ratio'] / 2.5, 3.0)       # 1.0x a 2.5, hasta 3.0 cap
            growth_factor = min(tf5['vol_growth'] / 1.2, 2.0)    # 1.0x a 1.2, hasta 2.0 cap
            bb_factor = max(1.0 - (tf5['width_curr'] / 0.03), 0.3)  # más comprimida = mejor, 0.3 floor

            raw_score = vol_factor * growth_factor * bb_factor
            # Normalizar a escala ~5-15
            score = round(4 + raw_score * 3)

            reasons = []
            near_pct = ((tf5['recent_max'] - tf5['price']) / tf5['recent_max']) if tf5['recent_max'] else 0.0
            reasons.append(f"5m a {near_pct:.2%} del maximo reciente")
            reasons.append(f"volumen 5m {tf5['vol_ratio']:.1f}x (factor {vol_factor:.2f})")
            reasons.append(f"volumen creciendo {tf5['vol_growth']:.2f}x (factor {growth_factor:.2f})")
            reasons.append(f"BB 5m {tf5['width_curr']:.2%} (factor {bb_factor:.2f})")
            if tf5['strong_close']:
                score += 1
                reasons.append("ultima vela 5m cerro fuerte")
            if tf1h['dist_to_res'] > 0.04:
                reasons.append(f"1h con buen espacio arriba ({tf1h['dist_to_res']:.2%})")
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

            # Score multiplicativo
            vol_factor = min(tf5['vol_ratio'] / 2.0, 3.5)        # 5m vol confirma
            bb_exp_factor = min(tf15['width_expansion'] / 0.15, 3.0)  # expansión BB
            body_factor = min(tf15.get('candle_body_pct', 0.5) / 0.5, 2.0)  # cuerpo sólido

            raw_score = vol_factor * bb_exp_factor * body_factor
            # Normalizar a escala ~5-15
            score = round(5 + raw_score * 2)

            reasons = [f"15m rompio el maximo reciente (+{tf15['breakout_distance']:.2%})"]
            reasons.append(f"5m volumen {tf5['vol_ratio']:.1f}x (factor {vol_factor:.2f})")
            reasons.append(f"expansion BB {tf15['width_expansion']:.0%} (factor {bb_exp_factor:.2f})")
            reasons.append(f"cuerpo vela {tf15.get('candle_body_pct', 0):.0%} (factor {body_factor:.2f})")
            if tf15['strong_close']:
                score += 1
                reasons.append("cierre 15m fuerte")
            if tf15['breakout_distance'] <= 0.02:
                score += 1
                reasons.append("todavia no esta muy extendida")
            if tf1h['dist_to_res'] > 0.04:
                reasons.append(f"1h con espacio real ({tf1h['dist_to_res']:.2%})")
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

    # Aplicar candle_status a cada candidate y penalizar score si está en formación.
    # candle_status viene del TF principal de la alerta.
    for c in candidates:
        tf_data = tf_map.get(c["timeframe"]) or {}
        cs = tf_data.get("candle_status", "closed")
        c["candle_status"] = cs
        if cs == "forming":
            c["score"] = max(0, c["score"] - FORMING_CANDLE_PENALTY)
            c["bucket"] = final_bucket(c["score"])
            # Si por la penalización ya no llega a IMMEDIATE_MIN_SCORE, bajamos el flag
            if c.get("immediate") and c["score"] < IMMEDIATE_MIN_SCORE:
                c["immediate"] = False

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
        f"score {alert['score']} • {alert['timeframe']} {tradingview_link(alert['symbol'], alert['timeframe'])}{hist_tag}"
    )

    if not with_reasons:
        return header

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

    # Marca de vela cerrada vs en formación
    cs = alert.get("candle_status", "closed")
    candle_line = "  ✅ vela cerrada" if cs == "closed" else "  🟡 vela en formación"

    body = "\n".join(f"  - {r}" for r in alert["reasons"][:3])
    return f"{header}\n{price_line}\n{candle_line}\n{body}"


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


def send_immediate(alert, counts_history, tf_map):
    """Envía alerta inmediata. Si hay imagen disponible la manda con sendPhoto;
    si no, fallback a texto plano con sendMessage."""
    label = "⚠️ SALIDA — precio devolviendo" if alert["history_tf"] == "FADING" else "🔥 PRIORITY NOW"
    text = f"{label}\n{format_alert(alert, counts_history)}"

    # Intentar imagen
    tf_data = tf_map.get(alert["timeframe"]) or {}
    df = tf_data.get("_df")
    if df is not None and CHART_ENABLED and _HAS_MPLFINANCE:
        img = generate_chart_image(
            df, alert["symbol"], alert["timeframe"], ref_price=alert.get("ref_price"),
        )
        if img is not None:
            send_telegram_photo(img, text)
            return

    # Fallback: texto plano
    send_telegram(text)


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

    # Snapshot de pares activos en este run (para reconstruir universo histórico)
    insert_pairs_snapshot(pairs)

    counts_history, last_seen = fetch_history()
    tasks = [(sym, tf) for sym in pairs for tf in INTERVALS]
    per_symbol = {sym: {} for sym in pairs}
    candidates = []
    processed = set()
    immediate_sent_keys = set()
    immediate_sent_alerts = []  # alertas inmediatas que sí se enviaron, para trackearlas
    immediate_skipped = 0  # Anti-spam: cuántas inmediatas saltamos por tope

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
                # Anti-spam: tope de inmediatas por run
                if len(immediate_sent_keys) >= MAX_IMMEDIATE_PER_RUN:
                    immediate_skipped += 1
                    print(f"  inmediata saltada (tope {MAX_IMMEDIATE_PER_RUN}): {symbol}")
                    continue
                try:
                    send_immediate(alert, counts_history, per_symbol[symbol])
                    immediate_sent_keys.add(key)
                    immediate_sent_alerts.append(alert)
                except Exception as e:
                    print(f"  error envio inmediato {symbol}: {e}")

    if not candidates:
        print("Sin setups en este run.")
        # Aún sin candidates, las inmediatas (si hubiera) ya se mandaron y se trackearon abajo
        if immediate_sent_alerts:
            insert_outcomes(immediate_sent_alerts)
        return

    selected = dedupe_and_rank(candidates)
    insert_history(selected)

    # Tracking de outcomes: combinamos selected + inmediatas que NO estén ya en selected
    selected_keys = {(a["symbol"], a["history_tf"]) for a in selected}
    extra_immediates = [a for a in immediate_sent_alerts
                        if (a["symbol"], a["history_tf"]) not in selected_keys]
    insert_outcomes(selected + extra_immediates)

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

    # Anti-spam: flag de burst si hay muchos candidates
    burst_line = ""
    if len(candidates) >= BURST_THRESHOLD:
        burst_line = f"🌊 BURST: {len(candidates)} setups detectados — movimiento de mercado\n"

    # Anti-spam: avisar si saltamos inmediatas
    skipped_line = ""
    if immediate_skipped > 0:
        skipped_line = f"ℹ️ {immediate_skipped} alertas inmediatas saltadas (tope: {MAX_IMMEDIATE_PER_RUN})\n"

    active_tag = " • ".join(active_names) if active_names else "ninguna"
    btc_ctx = get_btc_context()
    btc_line = f"{btc_ctx}\n" if btc_ctx else ""

    header = (
        f"🎯 TOP SETUPS • {now}\n"
        f"{btc_line}"
        f"{burst_line}"
        f"{skipped_line}"
        f"top {TOP_ALERT_COUNT} {extra}• {len(pairs)} pares\n"
        f"señales: {active_tag}\n"
    )
    send_telegram(header + "\n" + body)

    print(f"Total candidatos: {len(candidates)} (RIDING: {riding_count}, FADING: {fading_count})")
    print(f"Inmediatos enviados: {len(immediate_sent_keys)} | saltados: {immediate_skipped}")
    print(f"Top final: {len(selected)}")


if __name__ == "__main__":
    main()
