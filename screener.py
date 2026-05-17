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

def _cfg(section, key, default=None):
    """Acceso a config. Si default está definido, lo usa cuando falta la clave (modo
    backwards-compat). Si default es None, aborta con error explícito."""
    try:
        return CONFIG[section][key]
    except KeyError:
        if default is not None:
            return default
        raise SystemExit(f"FATAL: config.json no tiene {section}.{key}")

def _cfg_score(section, key, default):
    """Helper específico para secciones de scoring parametrizado (scoring_breakout,
    scoring_prebreak, etc.). Siempre tiene default — si la sección o clave no existe,
    cae al valor hardcodeado de v3. Permite que un config v3 viejo siga funcionando."""
    try:
        return CONFIG[section][key]
    except KeyError:
        return default

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

ACTIVE_SIGNALS_PREBREAK  = _cfg("active_signals", "PREBREAK")
ACTIVE_SIGNALS_BREAKOUT  = _cfg("active_signals", "BREAKOUT")
ACTIVE_SIGNALS_RIDING    = _cfg("active_signals", "RIDING")
ACTIVE_SIGNALS_FADING    = _cfg("active_signals", "FADING")
ACTIVE_SIGNALS_HOLD      = _cfg("active_signals", "HOLD")
ACTIVE_SIGNALS_EXPLOSION = _cfg("active_signals", "EXPLOSION", default=False)

EMA_SLOW        = _cfg("indicators", "EMA_SLOW")
RECENT_LOOKBACK = _cfg("indicators", "RECENT_LOOKBACK")
ATR_MIN_PCT     = _cfg("indicators", "ATR_MIN_PCT")

# Lookback extendido: usado para validar que el rompimiento sea contra un nivel real,
# no un mini-pico de las últimas 15 velas. Default 25 velas.
RECENT_LOOKBACK_LONG  = _cfg("indicators", "RECENT_LOOKBACK_LONG")
# Si el precio no rompe el long, debe estar al menos a esta distancia (ej 1%) para considerarse
# un breakout estructural. Más allá de eso, asumimos que está enterrado bajo un nivel mayor.
RECENT_LONG_PROXIMITY = _cfg("indicators", "RECENT_LONG_PROXIMITY")

# OBV: cuántas velas atrás miramos para calcular slope.
OBV_SLOPE_LOOKBACK = _cfg("indicators", "OBV_SLOPE_LOOKBACK")
OBV_RISING_MIN     = _cfg("indicators", "OBV_RISING_MIN")

# CVD: ventana y threshold mínimo (como ratio sobre el volumen total de la ventana).
CVD_LOOKBACK     = _cfg("indicators", "CVD_LOOKBACK")
CVD_BULLISH_MIN  = _cfg("indicators", "CVD_BULLISH_MIN")

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
MAJOR_STRUCT_LOOKBACK       = _cfg("hold", "MAJOR_STRUCT_LOOKBACK")
MAJOR_STRUCT_MAX_DIST       = _cfg("hold", "MAJOR_STRUCT_MAX_DIST")

RIDING_LOOKBACK_BARS      = _cfg("riding", "RIDING_LOOKBACK_BARS")
RIDING_MIN_GAIN           = _cfg("riding", "RIDING_MIN_GAIN")
RIDING_MAX_GAIN           = _cfg("riding", "RIDING_MAX_GAIN")
RIDING_ZONE_BUFFER        = _cfg("riding", "RIDING_ZONE_BUFFER")
RIDING_MIN_VOL_RATIO      = _cfg("riding", "RIDING_MIN_VOL_RATIO")
RIDING_EMA_MUST_TREND     = _cfg("riding", "RIDING_EMA_MUST_TREND")
RIDING_MAX_FADE_FROM_HIGH = _cfg("riding", "RIDING_MAX_FADE_FROM_HIGH", default=0.025)

FADING_REVERSAL_MIN  = _cfg("fading", "FADING_REVERSAL_MIN")
FADING_BELOW_ZONE    = _cfg("fading", "FADING_BELOW_ZONE")
# Pullback "duro" que dispara FADING aunque no haya perforado la zona. Antes hardcoded -0.03.
FADING_PULLBACK_HARD = _cfg("fading", "FADING_PULLBACK_HARD", default=0.03)

BEST_MIN_SCORE      = _cfg("scoring", "BEST_MIN_SCORE")
STRONG_MIN_SCORE    = _cfg("scoring", "STRONG_MIN_SCORE")
IMMEDIATE_MIN_SCORE = _cfg("scoring", "IMMEDIATE_MIN_SCORE")
FORMING_CANDLE_PENALTY = _cfg("scoring", "FORMING_CANDLE_PENALTY")
SCORE_CAP           = _cfg("scoring", "SCORE_CAP")

CHART_ENABLED = _cfg("chart", "ENABLED")
CHART_BARS    = _cfg("chart", "BARS")
CHART_WIDTH   = _cfg("chart", "WIDTH")
CHART_HEIGHT  = _cfg("chart", "HEIGHT")
CHART_STYLE   = _cfg("chart", "STYLE")

OUTCOMES_ENABLED              = _cfg("outcomes", "ENABLED")
OUTCOMES_TRACK_ALL_CANDIDATES = _cfg("outcomes", "TRACK_ALL_CANDIDATES")

# Derivatives (futures OI + funding rate). Off por default — requiere backtest A/B antes
# de prender en prod. Cuando OFF, no se hace ningún request a fapi.binance.com.
DERIV_ENABLED         = _cfg("derivatives", "ENABLED", default=False)
DERIV_FETCH_TIMEOUT   = _cfg("derivatives", "FUTURES_FETCH_TIMEOUT", default=10)
DERIV_OI_LOOKBACK_MIN = _cfg("derivatives", "OI_LOOKBACK_MIN", default=30)

_CAL_CFG = CONFIG.get("scoring_calibration") or {}


def _compute_atr_threshold(atr_pct_series, mode: str, floor: float, fixed_min: float,
                           lookback: int, rank_0_to_100: int) -> float:
    """ATR threshold dinámico por par. mode='fixed' → fixed_min. mode='percentile' →
    max(floor, quantile(historia, rank)), adaptando threshold al régimen de cada par."""
    if mode != "percentile":
        return fixed_min
    series = atr_pct_series.iloc[-lookback:] if len(atr_pct_series) > lookback else atr_pct_series
    series = series.dropna()
    if len(series) < 24:
        return floor
    return max(floor, float(series.quantile(rank_0_to_100 / 100.0)))


def _normalize_score(raw_score: float, signal_type: str) -> float:
    """Percentil empírico (0.0–1.0) del score dentro del CDF hardcoded por signal type.
    Permite ranking inter-signal justo: un score raro en PREBREAK supera a uno común en HOLD.
    Si calibración off o signal sin CDF → fallback proporcional sobre SCORE_CAP."""
    if not _CAL_CFG.get("ENABLED", False):
        return float(raw_score) / SCORE_CAP
    cdf = _CAL_CFG.get("cdf", {}).get(signal_type)
    if not cdf:
        return _CAL_CFG.get("FALLBACK_PERCENTILE", 0.5)
    points = sorted((float(k), float(v)) for k, v in cdf.items())
    if raw_score <= points[0][0]:
        return points[0][1]
    if raw_score >= points[-1][0]:
        return points[-1][1]
    for (s1, p1), (s2, p2) in zip(points, points[1:]):
        if s1 <= raw_score <= s2:
            return p1 + (p2 - p1) * (raw_score - s1) / (s2 - s1) if s2 > s1 else p1
    return _CAL_CFG.get("FALLBACK_PERCENTILE", 0.5)


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
    los completa el job update_outcomes.py que corre después.

    Campos nuevos (requieren migración previa de la tabla en Supabase):
      - obv_slope:      slope del OBV en 15m (cómo viene la acumulación)
      - cvd_ratio:      ratio de Cumulative Volume Delta en 15m (presión taker buy vs sell)
      - recent_long_ok: si la alerta pasó el filtro de estructura mayor (lookback 25)
    Si la migración no está aplicada, la inserción seguirá fallando — agregalos antes."""
    if not OUTCOMES_ENABLED or not alerts:
        return
    now_iso = datetime.now(timezone.utc).isoformat()
    rows = []
    for a in alerts:
        # Convertir floats con None-safety. Estos campos son nuevos y pueden no estar
        # en alertas viejas que quedaron en cola, así que toleramos su ausencia.
        obv = a.get("obv_slope")
        cvd = a.get("cvd_ratio")
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
            "obv_slope": float(obv) if obv is not None else None,
            "cvd_ratio": float(cvd) if cvd is not None else None,
            "recent_long_ok": bool(a["recent_long_ok"]) if a.get("recent_long_ok") is not None else None,
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
    for col in ["open", "high", "low", "close", "volume", "taker_buy_base", "taker_buy_quote"]:
        df[col] = df[col].astype(float)
    # close_time viene en ms UTC desde Binance; lo dejo como int para usar luego
    df["close_time"] = df["close_time"].astype("int64")
    df["open_time"] = df["open_time"].astype("int64")
    return df


# ── Derivatives (Binance Futures USDT-M) ──────────────────────────────────────
# Solo se usan si DERIV_ENABLED=true. Endpoints públicos (sin auth).
def get_futures_usdt_symbols():
    """Set de símbolos USDT-M perpetuos en Binance Futures. Usado para gatear que
    NO pidamos OI/funding de un par sin perp (404 garantizado)."""
    r = requests.get(
        "https://fapi.binance.com/fapi/v1/exchangeInfo",
        timeout=DERIV_FETCH_TIMEOUT,
    )
    r.raise_for_status()
    return {
        s["symbol"]
        for s in r.json().get("symbols", [])
        if s.get("contractType") == "PERPETUAL"
        and s.get("quoteAsset") == "USDT"
        and s.get("status") == "TRADING"
    }


def get_oi_delta(symbol):
    """% change de Open Interest en los últimos OI_LOOKBACK_MIN minutos.
    Usa /fapi/v1/openInterestHist con period=5m. Retorna float o None si falla
    o no hay suficiente historia."""
    # Pedimos lookback/5 + 1 puntos (ej: 30/5 + 1 = 7) para tener un valor "now" y otro "lookback atrás".
    limit = max(2, DERIV_OI_LOOKBACK_MIN // 5 + 1)
    try:
        r = requests.get(
            "https://fapi.binance.com/futures/data/openInterestHist",
            params={"symbol": symbol, "period": "5m", "limit": limit},
            timeout=DERIV_FETCH_TIMEOUT,
        )
        if r.status_code != 200:
            return None
        data = r.json()
        if not data or len(data) < 2:
            return None
        oi_now = float(data[-1]["sumOpenInterest"])
        oi_then = float(data[0]["sumOpenInterest"])
        if oi_then <= 0:
            return None
        return (oi_now - oi_then) / oi_then
    except Exception:
        return None


def get_funding_rate(symbol):
    """Funding rate actual (último valor publicado, refresca cada 8h).
    Retorna float o None si falla."""
    try:
        r = requests.get(
            "https://fapi.binance.com/fapi/v1/premiumIndex",
            params={"symbol": symbol},
            timeout=DERIV_FETCH_TIMEOUT,
        )
        if r.status_code != 200:
            return None
        data = r.json()
        fr = data.get("lastFundingRate")
        return float(fr) if fr is not None else None
    except Exception:
        return None


def fetch_derivatives(symbol):
    """Combina OI delta + funding en un dict {oi_delta_30m, funding_rate}.
    Si una pata falla, queda None en esa clave (el scoring la ignora)."""
    return {
        "oi_delta_30m": get_oi_delta(symbol),
        "funding_rate": get_funding_rate(symbol),
    }


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
        # Recortar vela en formación si existe (mismo fix que analyze()).
        # En 4h, la vela viva pasa la mayor parte del tiempo sin cerrar — el % cambio
        # contra una vela que recién empieza no es informativo.
        last_close_time_ms = int(df["close_time"].iloc[-1])
        now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
        if now_ms < last_close_time_ms:
            df = df.iloc[:-1].reset_index(drop=True)

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

    # Detectar si la última vela está en formación y recortarla.
    # Binance devuelve klines incluyendo la vela actual (la que se está formando),
    # cuyo close_time está en el futuro. Trabajar con vela viva mete ruido en TODO:
    # vol_ratio incompleto, breakouts intra-vela que se devuelven, strong_close inválido,
    # candle_body_pct sin sentido. Solución: si la última vela todavía no cerró,
    # la descartamos. El resto del análisis usa iloc[-1] = última vela cerrada.
    last_close_time_ms = int(df["close_time"].iloc[-1])
    now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
    if now_ms < last_close_time_ms:
        df = df.iloc[:-1].reset_index(drop=True)

    if len(df) < 80:
        return symbol, interval, None

    close = df["close"]
    high = df["high"]
    low = df["low"]
    volume = df["volume"]
    price = close.iloc[-1]
    # Cambio % de la vela actual vs la anterior (feature usada por EXPLOSION).
    close_change_curr = safe_pct(price, close.iloc[-2]) if len(close) >= 2 else 0.0

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
    atr_series = atr_indicator.average_true_range()
    atr = atr_series.iloc[-1]
    atr_pct = (atr / price * 100) if price > 0 else 0.0
    atr_pct_series = (atr_series / close * 100).fillna(0.0)
    atr_threshold = _compute_atr_threshold(
        atr_pct_series,
        _cfg("indicators", "ATR_MODE", default="fixed"),
        _cfg("indicators", "ATR_MIN_PCT_FLOOR", default=1.0),
        ATR_MIN_PCT,
        _cfg("indicators", "ATR_PERCENTILE_LOOKBACK", default=168),
        _cfg("indicators", "ATR_PERCENTILE_RANK", default=30),
    )

    vol_mean = volume.iloc[-21:-1].mean()
    vol_ratio = vol_mean and (volume.iloc[-1] / vol_mean) or 0.0
    vol_recent = volume.iloc[-3:].mean()
    vol_prev = volume.iloc[-6:-3].mean()
    vol_growth = vol_prev and (vol_recent / vol_prev) or 0.0
    vol_mean_prev = volume.iloc[-22:-2].mean()
    vol_ratio_prev = (volume.iloc[-2] / vol_mean_prev) if vol_mean_prev > 0 else 0.0

    close_pos = close_position(price, high.iloc[-1], low.iloc[-1])
    strong_close = close_pos >= STRONG_CLOSE_MIN

    candle_range = max(high.iloc[-1] - low.iloc[-1], 1e-12)
    candle_body_pct = abs(close.iloc[-1] - df["open"].iloc[-1]) / candle_range

    # ── OBV slope: dirección del On-Balance Volume.
    # OBV suma volumen en velas alcistas y resta en bajistas.
    # Si el precio está cerca de un breakout y el OBV viene subiendo, hay acumulación real.
    # Si el precio sube pero el OBV está plano o bajando, es divergencia bajista (fakeout probable).
    # Usamos slope = (OBV_now - OBV_n_atras) / |OBV_now| como % normalizado.
    try:
        obv = ta.volume.OnBalanceVolumeIndicator(close, volume).on_balance_volume()
        obv_now = obv.iloc[-1]
        obv_ref = obv.iloc[-OBV_SLOPE_LOOKBACK]
        obv_slope = (obv_now - obv_ref) / abs(obv_now) if abs(obv_now) > 1e-12 else 0.0
        obv_rising = obv_slope >= OBV_RISING_MIN
        obv_prev = obv.iloc[-2]
        obv_ref_prev = obv.iloc[-OBV_SLOPE_LOOKBACK - 1]
        obv_slope_prev = (obv_prev - obv_ref_prev) / abs(obv_prev) if abs(obv_prev) > 1e-12 else 0.0
    except Exception:
        obv_slope = 0.0
        obv_rising = False
        obv_slope_prev = 0.0

    # ── CVD (Cumulative Volume Delta) con taker buy/sell de Binance.
    # Binance reporta cuánto del volumen de cada vela fue iniciado por el comprador agresivo
    # (taker_buy_base). El delta de cada vela = 2*taker_buy - volume_total
    # (porque el resto fue iniciado por vendedor agresivo). CVD acumula esos deltas.
    # Si CVD sube → compradores dominan. Si CVD baja con precio subiendo → distribución (fakeout).
    try:
        if "taker_buy_base" in df.columns:
            taker_buy = df["taker_buy_base"].astype(float)
            delta_per_candle = 2 * taker_buy - volume
            cvd = delta_per_candle.cumsum()
            cvd_now = cvd.iloc[-1]
            cvd_ref = cvd.iloc[-CVD_LOOKBACK]
            # Normalizamos por el volumen acumulado en la ventana para tener un ratio comparable
            vol_window = volume.iloc[-CVD_LOOKBACK:].sum()
            cvd_ratio = (cvd_now - cvd_ref) / vol_window if vol_window > 0 else 0.0
            cvd_bullish = cvd_ratio >= CVD_BULLISH_MIN
        else:
            cvd_ratio = 0.0
            cvd_bullish = False
    except Exception:
        cvd_ratio = 0.0
        cvd_bullish = False

    # ── recent_max corto (RECENT_LOOKBACK velas) y extendido (RECENT_LOOKBACK_LONG velas).
    # El corto se usa para detectar el momento del breakout (señal rápida).
    # El extendido valida que el rompimiento sea contra un nivel real, no un mini-pico.
    # Un breakout verdadero rompe AMBOS niveles (o está cerca del extendido). Si solo rompe
    # el corto pero está enterrado bajo el extendido, es un bounce dentro de un trend bajista.
    recent_max = high.iloc[-(RECENT_LOOKBACK + 2):-2].max()
    near_recent_max = recent_max > 0 and 0 <= (recent_max - price) / recent_max <= PREBREAK_NEAR_MAX
    breakout = recent_max > 0 and price > recent_max * (1 + BREAKOUT_BUFFER)
    breakout_distance = safe_pct(price, recent_max)

    # Lookback extendido para validar estructura más amplia.
    # Si len < lookback_long+2, queda igual al corto (no penaliza pares con poca historia).
    if len(high) >= RECENT_LOOKBACK_LONG + 2:
        recent_max_long = high.iloc[-(RECENT_LOOKBACK_LONG + 2):-2].max()
        # breakout_long: el precio debe estar al menos cerca del nivel extendido para considerarse
        # un breakout estructural (no un mini-pico dentro de tendencia bajista).
        # "Cerca" = dentro de RECENT_LONG_PROXIMITY del nivel extendido.
        if recent_max_long > 0:
            dist_to_long = (recent_max_long - price) / recent_max_long
            recent_long_ok = price > recent_max_long * (1 + BREAKOUT_BUFFER) or dist_to_long <= RECENT_LONG_PROXIMITY
        else:
            recent_long_ok = True
    else:
        recent_max_long = recent_max
        recent_long_ok = True

    one_h_resist = high.iloc[-(ONE_H_RESIST_LOOKBACK + 2):-2].max()
    dist_to_res = (one_h_resist - price) / price if price > 0 else 0.0
    not_near_resistance = dist_to_res > ONE_H_RESIST_BUFFER or breakout

    # Validación de estructura mayor (lookback largo, ~60 velas).
    # En 1h son ~2.5 días — captura niveles que sí son estructura real.
    # Si el precio está rompiendo el recent_max corto pero todavía está enterrado bajo
    # un máximo mayor, probablemente es un bounce dentro de tendencia bajista.
    # major_struct_ok = True si el precio rompió el máximo mayor o está cerca de tocarlo.
    if len(high) >= MAJOR_STRUCT_LOOKBACK + 2:
        major_struct_max = high.iloc[-(MAJOR_STRUCT_LOOKBACK + 2):-2].max()
        major_struct_dist = (major_struct_max - price) / price if price > 0 else 0.0
        major_struct_ok = major_struct_dist <= MAJOR_STRUCT_MAX_DIST
    else:
        major_struct_max = None
        major_struct_dist = None
        major_struct_ok = True  # sin datos suficientes, no penalizar

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

    # candle_status siempre es "closed" porque al inicio de analyze() recortamos la vela
    # en formación si existía. Se mantiene el campo por compatibilidad con el resto del
    # código (logging, format_alert, etc.) y por si más adelante queremos diferenciar.
    candle_status = "closed"

    return symbol, interval, {
        "price": price,
        "ema_trend_up": ema_trend_up,
        "width_curr": width_curr,
        "width_expansion": width_expansion,
        "atr_pct": atr_pct,
        "atr_threshold": atr_threshold,
        "vol_ratio": vol_ratio,
        "vol_ratio_prev": vol_ratio_prev,
        "vol_growth": vol_growth,
        "strong_close": strong_close,
        "candle_body_pct": candle_body_pct,
        "close_change_curr": close_change_curr,
        "recent_max": recent_max,
        "near_recent_max": near_recent_max,
        "breakout": breakout,
        "breakout_distance": breakout_distance,
        "recent_max_long": recent_max_long,
        "recent_long_ok": recent_long_ok,
        "obv_slope": obv_slope,
        "obv_slope_prev": obv_slope_prev,
        "obv_rising": obv_rising,
        "cvd_ratio": cvd_ratio,
        "cvd_bullish": cvd_bullish,
        "not_near_resistance": not_near_resistance,
        "dist_to_res": dist_to_res,
        "major_struct_max": major_struct_max,
        "major_struct_dist": major_struct_dist,
        "major_struct_ok": major_struct_ok,
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

    # Gates universales (aplican a TODOS los signals incl. EXPLOSION).
    if not tf1h.get("not_near_resistance"):
        return None
    if not tf1h.get("major_struct_ok", True):
        if not (_cfg("hold", "MAJOR_STRUCT_BYPASS_ON_BREAKOUT", default=False) and tf15.get("breakout", False)):
            return None

    candidates = []
    _persist_on = _cfg("persistence", "ENABLED", default=False)

    # ── EXPLOSION ──────────────────────────────────────────────────────────────
    # Detector agresivo de pumps explosivos en 5m. Bypaaa EMA/ATR gates — su
    # criterio (vol_ratio ≥ 5, body ≥ 0.85, close_change ≥ 2.5%, BB expansion ≥ 0.5)
    # ya es restrictivo por construcción. Captura los pumps que PREBREAK/BREAKOUT
    # pierden por timing o gates duros (low-ATR, V-reversals, blast candles).
    if ACTIVE_SIGNALS_EXPLOSION:
        ex_min_vol  = _cfg("scoring_explosion", "MIN_VOL_RATIO", default=5.0)
        ex_min_body = _cfg("scoring_explosion", "MIN_BODY_PCT", default=0.85)
        ex_min_chg  = _cfg("scoring_explosion", "MIN_CLOSE_CHANGE", default=0.025)
        ex_min_bb   = _cfg("scoring_explosion", "MIN_BB_EXPANSION", default=0.5)
        explosion_ok = (
            tf5.get("vol_ratio", 0)         >= ex_min_vol
            and tf5.get("candle_body_pct", 0) >= ex_min_body
            and tf5.get("close_change_curr", 0) >= ex_min_chg
            and tf5.get("width_expansion", 0)   >= ex_min_bb
        )
        if explosion_ok and not in_cooldown(symbol, "EXPLOSION", last_seen):
            ex_base = _cfg("scoring_explosion", "BASE_SCORE", default=12)
            ex_late_max = _cfg("scoring_explosion", "LATE_ENTRY_MAX_DIST", default=0.03)
            ex_late_pen = _cfg("scoring_explosion", "LATE_ENTRY_PENALTY", default=-3)
            score = ex_base
            reasons = [
                f"vol 5m {tf5['vol_ratio']:.1f}x",
                f"body {tf5['candle_body_pct']:.0%}",
                f"close 5m +{tf5['close_change_curr']:.2%}",
                f"BB expansion +{tf5['width_expansion']:.0%}",
            ]
            if tf5.get("breakout_distance", 0) > ex_late_max:
                score += ex_late_pen
                reasons.append(f"⚠ ya extendido {tf5['breakout_distance']:.2%} sobre máximo")
            score = max(0, min(score, SCORE_CAP))
            candidates.append({
                "symbol": symbol,
                "label": "EXPLOSION",
                "history_tf": "EXPLOSION",
                "score": score,
                "priority": 5,
                "bucket": final_bucket(score),
                "reasons": reasons,
                "late": False,
                "timeframe": "5m",
                "immediate": score >= IMMEDIATE_MIN_SCORE,
                "price": tf5["price"],
                "ref_price": tf5.get("recent_max", tf5["price"]),
                "obv_slope": tf15.get("obv_slope"),
                "cvd_ratio": tf15.get("cvd_ratio"),
                "recent_long_ok": tf15.get("recent_long_ok"),
            })

    # Gates para signals tradicionales (PREBREAK/BREAKOUT/RIDING/HOLD/FADING).
    # EMA hard: corta acá. EMA soft: aplica penalty después al score.
    _ema_gate_hard = _cfg("indicators", "EMA_GATE_HARD", default=True)
    _ema_blocks = (not tf1h.get("ema_trend_up")) and _ema_gate_hard
    _atr_blocks = tf1h.get("atr_pct", 0) < tf1h.get("atr_threshold", ATR_MIN_PCT)
    _trad_signals_eligible = not (_ema_blocks or _atr_blocks)
    if not _trad_signals_eligible and not candidates:
        return None

    # ── PRE-BREAK ──────────────────────────────────────────────────────────────
    if _trad_signals_eligible and ACTIVE_SIGNALS_PREBREAK:
        _pre_vol_bars = _cfg("persistence", "PREBREAK_VOL_BARS", default=1) if _persist_on else 1
        _vol_ok_pre = tf5.get("vol_ratio", 0) >= PREBREAK_MIN_VOL_RATIO
        if _pre_vol_bars >= 2:
            _vol_ok_pre = _vol_ok_pre and tf5.get("vol_ratio_prev", 0) >= PREBREAK_MIN_VOL_RATIO
        pre_ok = (
            tf5.get("near_recent_max")
            and tf5.get("width_curr", 9) <= PREBREAK_BB_WIDTH_MAX
            and _vol_ok_pre
            and tf5.get("vol_growth", 0) >= PREBREAK_VOLUME_GROWTH_MIN
        )
        if pre_ok and not in_cooldown(symbol, "PREBREAK", last_seen):
            prev = counts_history.get((symbol, "PREBREAK"), 0)

            # ── Scoring parametrizado vía config.scoring_prebreak ─────────────
            # Defaults coinciden con v3 hardcodeado para back-compat.
            sp = "scoring_prebreak"
            base_offset    = _cfg_score(sp, "BASE_OFFSET", 4)
            base_mult      = _cfg_score(sp, "BASE_MULTIPLIER", 3)
            vol_div        = _cfg_score(sp, "VOL_FACTOR_DIV", 2.5)
            vol_cap        = _cfg_score(sp, "VOL_FACTOR_CAP", 3.0)
            grow_div       = _cfg_score(sp, "GROWTH_FACTOR_DIV", 1.2)
            grow_cap       = _cfg_score(sp, "GROWTH_FACTOR_CAP", 2.0)
            bb_div         = _cfg_score(sp, "BB_FACTOR_DIV", 0.03)
            bb_floor       = _cfg_score(sp, "BB_FACTOR_FLOOR", 0.3)
            close_bonus    = _cfg_score(sp, "STRONG_CLOSE_BONUS", 1)
            obv_up_bonus   = _cfg_score(sp, "OBV_RISING_BONUS", 2)
            obv_dn_pen     = _cfg_score(sp, "OBV_FALLING_PENALTY", -1)
            struct_pen     = _cfg_score(sp, "STRUCT_PENALTY", -1)
            late_pen       = _cfg_score(sp, "LATE_REPEAT_PENALTY", -1)

            # Score multiplicativo: factores clave se multiplican, no se suman
            vol_factor = min(tf5['vol_ratio'] / vol_div, vol_cap)
            growth_factor = min(tf5['vol_growth'] / grow_div, grow_cap)
            bb_factor = max(1.0 - (tf5['width_curr'] / bb_div), bb_floor)

            raw_score = vol_factor * growth_factor * bb_factor
            score = round(base_offset + raw_score * base_mult)

            reasons = []
            near_pct = ((tf5['recent_max'] - tf5['price']) / tf5['recent_max']) if tf5['recent_max'] else 0.0
            reasons.append(f"5m a {near_pct:.2%} del maximo reciente")
            reasons.append(f"volumen 5m {tf5['vol_ratio']:.1f}x (factor {vol_factor:.2f})")
            reasons.append(f"volumen creciendo {tf5['vol_growth']:.2f}x (factor {growth_factor:.2f})")
            reasons.append(f"BB 5m {tf5['width_curr']:.2%} (factor {bb_factor:.2f})")
            if tf5['strong_close']:
                score += close_bonus
                reasons.append("ultima vela 5m cerro fuerte")
            if tf1h['dist_to_res'] > 0.04:
                reasons.append(f"1h con buen espacio arriba ({tf1h['dist_to_res']:.2%})")
            # Bonus por OBV ascendente en 15m (acumulación real, no solo volumen aislado).
            if tf15.get("obv_rising"):
                score += obv_up_bonus
                reasons.append(f"OBV 15m subiendo ({tf15['obv_slope']:+.1%}) — acumulación real")
            elif tf15.get("obv_slope", 0) < -OBV_RISING_MIN:
                # OBV bajando con precio cerca del máx → divergencia bajista, fakeout probable
                score += obv_dn_pen
                reasons.append(f"⚠ OBV 15m bajando ({tf15['obv_slope']:+.1%}) — divergencia")
            # NOTA: el bonus/penalty de CVD se removió de PREBREAK porque el backtest mostró
            # que en PREBREAK CVD bullish rinde PEOR (+2.22%) que neutral/bearish (+5.5%).
            # CVD se mantiene activo en BREAKOUT donde sí funciona como confirmación.
            if tf15.get("recent_long_ok"):
                reasons.append("rompe nivel estructural (lookback extendido)")
            else:
                score += struct_pen
                reasons.append("⚠ enterrado bajo máximo mayor — posible bounce")
            if prev >= LATE_REPEAT_COUNT:
                score += late_pen
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
                "obv_slope": tf15.get("obv_slope"),
                "cvd_ratio": tf15.get("cvd_ratio"),
                "recent_long_ok": tf15.get("recent_long_ok"),
            })

    # ── BREAKOUT ───────────────────────────────────────────────────────────────
    if _trad_signals_eligible and ACTIVE_SIGNALS_BREAKOUT:
        # Filtro OBV>=0 ahora opcional vía config. En v3 era hardcoded True.
        require_obv_nonneg = _cfg("breakout", "BREAKOUT_REQUIRE_OBV_NON_NEGATIVE", default=True)
        breakout_ok = (
            tf15.get("breakout")
            and tf15.get("vol_ratio", 0) >= BREAKOUT_MIN_VOL_RATIO
            and tf15.get("breakout_distance", 9) <= BREAKOUT_MAX_EXTENDED
            and tf15.get("width_expansion", -9) >= BREAKOUT_BB_EXPANSION_MIN
            and tf15.get("strong_close", False)
            and tf15.get("candle_body_pct", 0) >= BREAKOUT_MIN_BODY_PCT
            and tf5.get("vol_ratio", 0) >= BREAKOUT_5M_MIN_VOL_RATIO
            and tf5.get("strong_close", False)
            and (not require_obv_nonneg or tf15.get("obv_slope", 0) >= 0)
        )
        if breakout_ok and not in_cooldown(symbol, "BREAKOUT", last_seen):
            prev = counts_history.get((symbol, "BREAKOUT"), 0)

            # ── Scoring parametrizado vía config.scoring_breakout ─────────────
            # Defaults coinciden con v3 hardcodeado para back-compat.
            sb = "scoring_breakout"
            base_score        = _cfg_score(sb, "BASE_SCORE", 8)
            obv_explosive_min = _cfg_score(sb, "OBV_TIER_EXPLOSIVE_MIN", 0.3)
            obv_explosive_b   = _cfg_score(sb, "OBV_TIER_EXPLOSIVE_BONUS", 4)
            obv_strong_min    = _cfg_score(sb, "OBV_TIER_STRONG_MIN", 0.1)
            obv_strong_b      = _cfg_score(sb, "OBV_TIER_STRONG_BONUS", 3)
            obv_rising_min    = _cfg_score(sb, "OBV_TIER_RISING_MIN", 0.05)
            obv_rising_b      = _cfg_score(sb, "OBV_TIER_RISING_BONUS", 2)
            obv_neutral_min   = _cfg_score(sb, "OBV_TIER_NEUTRAL_MIN", 0)
            obv_neutral_b     = _cfg_score(sb, "OBV_TIER_NEUTRAL_BONUS", 0)
            obv_falling_pen   = _cfg_score(sb, "OBV_TIER_FALLING_PENALTY", -2)
            cvd_vbull_min     = _cfg_score(sb, "CVD_TIER_VERY_BULLISH_MIN", 0.1)
            cvd_vbull_b       = _cfg_score(sb, "CVD_TIER_VERY_BULLISH_BONUS", 2)
            cvd_bull_min      = _cfg_score(sb, "CVD_TIER_BULLISH_MIN", 0.05)
            cvd_bull_b        = _cfg_score(sb, "CVD_TIER_BULLISH_BONUS", 1)
            cvd_neutral_min   = _cfg_score(sb, "CVD_TIER_NEUTRAL_MIN", -0.05)
            cvd_neutral_b     = _cfg_score(sb, "CVD_TIER_NEUTRAL_BONUS", 0)
            cvd_bear_pen      = _cfg_score(sb, "CVD_TIER_BEARISH_PENALTY", -1)
            climax_vol_min    = _cfg_score(sb, "CLIMAX_VOL_MIN", 5.0)
            climax_bb_min     = _cfg_score(sb, "CLIMAX_BB_EXP_MIN", 0.4)
            climax_body_min   = _cfg_score(sb, "CLIMAX_BODY_MIN", 0.85)
            climax_thresh     = _cfg_score(sb, "CLIMAX_THRESHOLD", 2)
            climax_pen        = _cfg_score(sb, "CLIMAX_PENALTY", -2)
            early_max         = _cfg_score(sb, "EARLY_ENTRY_MAX", 0.015)
            early_bonus       = _cfg_score(sb, "EARLY_ENTRY_BONUS", 2)
            late_min          = _cfg_score(sb, "LATE_ENTRY_MIN", 0.025)
            late_pen_entry    = _cfg_score(sb, "LATE_ENTRY_PENALTY", -1)
            struct_pen        = _cfg_score(sb, "STRUCT_PENALTY", -1)
            late_repeat_pen   = _cfg_score(sb, "LATE_REPEAT_PENALTY", -1)

            score = base_score

            obv_v = tf15.get("obv_slope", 0)
            cvd_v = tf15.get("cvd_ratio", 0)

            # OBV — predictor más fuerte (tiers parametrizados)
            if obv_v >= obv_explosive_min:
                score += obv_explosive_b
                obv_label = "OBV explosivo"
            elif obv_v >= obv_strong_min:
                score += obv_strong_b
                obv_label = "OBV fuerte"
            elif obv_v >= obv_rising_min:
                score += obv_rising_b
                obv_label = "OBV subiendo"
            elif obv_v >= obv_neutral_min:
                score += obv_neutral_b
                obv_label = "OBV plano"
            else:
                score += obv_falling_pen
                obv_label = "⚠ OBV bajando"

            # CVD — peso medio (tiers parametrizados)
            if cvd_v >= cvd_vbull_min:
                score += cvd_vbull_b
                cvd_label = "CVD muy bullish"
            elif cvd_v >= cvd_bull_min:
                score += cvd_bull_b
                cvd_label = "CVD bullish"
            elif cvd_v >= cvd_neutral_min:
                score += cvd_neutral_b
                cvd_label = "CVD neutral"
            else:
                score += cvd_bear_pen
                cvd_label = "⚠ CVD bearish"

            # Climax penalty parametrizado
            climax_signals = 0
            if tf15.get("vol_ratio", 0) >= climax_vol_min:
                climax_signals += 1
            if tf15.get("width_expansion", 0) >= climax_bb_min:
                climax_signals += 1
            if tf15.get("candle_body_pct", 0) >= climax_body_min:
                climax_signals += 1
            if climax_signals >= climax_thresh:
                score += climax_pen
                climax_note = f"⚠ posible climax ({climax_signals} factores extremos)"
            else:
                climax_note = None

            # Entrada temprana / tardía
            if tf15["breakout_distance"] <= early_max:
                score += early_bonus
                entry_note = "entrada temprana"
            elif tf15["breakout_distance"] >= late_min:
                score += late_pen_entry
                entry_note = "breakout extendido — entrada tardía"
            else:
                entry_note = None

            # recent_long_ok — UNA sola aplicación (v3 lo aplicaba dos veces, bug fix)
            if not tf15.get("recent_long_ok", True):
                score += struct_pen
                struct_note = "⚠ enterrado bajo máximo mayor"
            else:
                struct_note = None

            # Late repeat penalty — UNA sola aplicación (v3 lo aplicaba dos veces, bug fix)
            if prev >= LATE_REPEAT_COUNT:
                score += late_repeat_pen

            # Derivatives: OI delta + funding rate (sólo si DERIV_ENABLED y el par tiene perp)
            deriv_note = None
            if DERIV_ENABLED:
                oi = tf15.get("oi_delta_30m")
                fr = tf15.get("funding_rate")
                if oi is not None:
                    if oi >= _cfg_score(sb, "OI_RISING_MIN", 0.02):
                        score += _cfg_score(sb, "OI_RISING_BONUS", 2)
                        deriv_note = f"OI +{oi:.1%} ({DERIV_OI_LOOKBACK_MIN}m) — convicción real"
                    elif oi <= _cfg_score(sb, "OI_FALLING_MAX", -0.01):
                        score += _cfg_score(sb, "OI_FALLING_PENALTY", -2)
                        deriv_note = f"⚠ OI {oi:.1%} ({DERIV_OI_LOOKBACK_MIN}m) — short cover"
                if fr is not None:
                    if fr <= _cfg_score(sb, "FUNDING_HEALTHY_MAX", 0.0003):
                        score += _cfg_score(sb, "FUNDING_HEALTHY_BONUS", 1)
                    elif fr >= _cfg_score(sb, "FUNDING_HOT_MIN", 0.0008):
                        score += _cfg_score(sb, "FUNDING_HOT_PENALTY", -2)

            # Cap final
            score = min(score, SCORE_CAP)

            # Reasons (visualmente)
            reasons = [
                f"15m rompio +{tf15['breakout_distance']:.2%} (vol {tf15.get('vol_ratio', 0):.1f}x, "
                f"BB exp {tf15.get('width_expansion', 0):.0%})"
            ]
            reasons.append(f"{obv_label} ({obv_v:+.1%})")
            reasons.append(f"{cvd_label} ({cvd_v:+.1%})")
            if climax_note:
                reasons.append(climax_note)
            if entry_note:
                reasons.append(entry_note)
            if struct_note:
                reasons.append(struct_note)
            else:
                reasons.append("rompe nivel estructural (lookback 25)")
            if deriv_note:
                reasons.append(deriv_note)
            if tf1h.get("dist_to_res", 0) > 0.04:
                reasons.append(f"1h con espacio ({tf1h['dist_to_res']:.2%})")
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
                "obv_slope": tf15.get("obv_slope"),
                "cvd_ratio": tf15.get("cvd_ratio"),
                "recent_long_ok": tf15.get("recent_long_ok"),
            })

    # ── RIDING ─────────────────────────────────────────────────────────────────
    if _trad_signals_eligible and ACTIVE_SIGNALS_RIDING:
        riding_gain = tf15.get("riding_gain") or 0.0
        riding_ok = (
            tf15.get("riding_above_zone")
            and tf15.get("riding_vol_ok")
            and RIDING_MIN_GAIN <= riding_gain <= RIDING_MAX_GAIN
            and tf15.get("riding_bars_since") is not None
            and tf15["riding_bars_since"] >= 1
            and (not RIDING_EMA_MUST_TREND or tf1h.get("ema_trend_up"))
            and not tf15.get("breakout")
            and (tf15.get("fading_reversal") or 0.0) >= -RIDING_MAX_FADE_FROM_HIGH
        )
        if riding_ok and not in_cooldown(symbol, "RIDING", last_seen):
            prev = counts_history.get((symbol, "RIDING"), 0)

            # ── Scoring parametrizado vía config.scoring_riding ───────────────
            # Defaults coinciden con v3 hardcodeado para back-compat.
            sr = "scoring_riding"
            base_score      = _cfg_score(sr, "BASE_SCORE", 4)
            gain_strong_min = _cfg_score(sr, "GAIN_TIER_STRONG_MIN", 0.05)
            gain_strong_b   = _cfg_score(sr, "GAIN_TIER_STRONG_BONUS", 3)
            gain_solid_min  = _cfg_score(sr, "GAIN_TIER_SOLID_MIN", 0.02)
            gain_solid_b    = _cfg_score(sr, "GAIN_TIER_SOLID_BONUS", 2)
            gain_initial_b  = _cfg_score(sr, "GAIN_TIER_INITIAL_BONUS", 1)
            vol_ok_b        = _cfg_score(sr, "VOL_OK_BONUS", 1)
            close_b         = _cfg_score(sr, "STRONG_CLOSE_BONUS", 1)
            ema_b           = _cfg_score(sr, "EMA_TREND_BONUS", 1)
            dist_high_min   = _cfg_score(sr, "DIST_RES_HIGH_MIN", 0.04)
            dist_high_b     = _cfg_score(sr, "DIST_RES_HIGH_BONUS", 2)
            dist_low_b      = _cfg_score(sr, "DIST_RES_LOW_BONUS", 1)
            obv_up_b        = _cfg_score(sr, "OBV_RISING_BONUS", 1)
            obv_dn_pen      = _cfg_score(sr, "OBV_FALLING_PENALTY", -2)
            cvd_up_b        = _cfg_score(sr, "CVD_BULLISH_BONUS", 1)
            cvd_dn_pen      = _cfg_score(sr, "CVD_BEARISH_PENALTY", -2)
            # Variante G: bonus por gain excepcional (default 0 = inactivo)
            strong_gain_b   = _cfg_score(sr, "STRONG_GAIN_BONUS", 0)
            strong_gain_min = _cfg_score(sr, "STRONG_GAIN_MIN", 0.08)

            score = base_score
            reasons = [
                f"sigue subiendo desde el breakout (+{riding_gain:.2%}, "
                f"{tf15['riding_bars_since']} velas atras)"
            ]
            if riding_gain >= gain_strong_min:
                score += gain_strong_b
                reasons.append(f"movimiento fuerte (+{riding_gain:.2%} total)")
            elif riding_gain >= gain_solid_min:
                score += gain_solid_b
                reasons.append(f"ganancia solida (+{riding_gain:.2%} total)")
            else:
                score += gain_initial_b
                reasons.append(f"ganancia inicial (+{riding_gain:.2%} total)")
            # Bonus extra por gain excepcional (>=8% por defecto)
            if riding_gain >= strong_gain_min and strong_gain_b > 0:
                score += strong_gain_b
                reasons.append(f"🚀 gain excepcional (>={strong_gain_min:.0%})")
            if tf15.get("riding_vol_ok"):
                score += vol_ok_b
                reasons.append("volumen sostenido — no hay colapso de momentum")
            if tf15.get("strong_close"):
                score += close_b
                reasons.append("ultima vela 15m cierra fuerte")
            if tf1h.get("ema_trend_up"):
                score += ema_b
                reasons.append("1h EMA sigue alcista")
            if tf1h.get("dist_to_res", 0) > dist_high_min:
                score += dist_high_b
                reasons.append(f"1h con espacio real ({tf1h['dist_to_res']:.2%})")
            else:
                score += dist_low_b
                reasons.append("1h alcista")
            # OBV/CVD bonuses para diferenciar RIDING entre sí
            if tf15.get("obv_rising"):
                score += obv_up_b
                reasons.append(f"OBV 15m sigue subiendo ({tf15['obv_slope']:+.1%})")
            elif tf15.get("obv_slope", 0) < 0:
                score += obv_dn_pen
                reasons.append(f"⚠ OBV 15m bajando ({tf15['obv_slope']:+.1%}) — momentum se agota")
            if tf15.get("cvd_bullish"):
                score += cvd_up_b
                reasons.append("compradores agresivos siguen dominando")
            elif tf15.get("cvd_ratio", 0) < -CVD_BULLISH_MIN:
                score += cvd_dn_pen
                reasons.append(f"⚠ CVD bajista — vendedores tomando control")
            # Derivatives: OI delta + funding rate
            if DERIV_ENABLED:
                oi = tf15.get("oi_delta_30m")
                fr = tf15.get("funding_rate")
                if oi is not None:
                    if oi >= _cfg_score(sr, "OI_RISING_MIN", 0.02):
                        score += _cfg_score(sr, "OI_RISING_BONUS", 1)
                        reasons.append(f"OI +{oi:.1%} ({DERIV_OI_LOOKBACK_MIN}m) — flujo entrando")
                    elif oi <= _cfg_score(sr, "OI_FALLING_MAX", -0.01):
                        score += _cfg_score(sr, "OI_FALLING_PENALTY", -1)
                        reasons.append(f"⚠ OI {oi:.1%} — flujo saliendo")
                if fr is not None:
                    if fr <= _cfg_score(sr, "FUNDING_HEALTHY_MAX", 0.0003):
                        score += _cfg_score(sr, "FUNDING_HEALTHY_BONUS", 1)
                    elif fr >= _cfg_score(sr, "FUNDING_HOT_MIN", 0.0008):
                        score += _cfg_score(sr, "FUNDING_HOT_PENALTY", -1)
                        reasons.append("⚠ funding caliente — long crowded")
            late_repeat_pen = _cfg_score(sr, "LATE_REPEAT_PENALTY", 0)
            if prev > 0 and late_repeat_pen < 0:
                score += late_repeat_pen
                reasons.append("repeticion tardia")
            score = min(score, SCORE_CAP)
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
                "obv_slope": tf15.get("obv_slope"),
                "cvd_ratio": tf15.get("cvd_ratio"),
                "recent_long_ok": tf15.get("recent_long_ok"),
            })

    # ── FADING ─────────────────────────────────────────────────────────────────
    if _trad_signals_eligible and ACTIVE_SIGNALS_FADING:
        fading_reversal = tf15.get("fading_reversal") or 0.0
        fading_ok = (
            tf15.get("riding_bars_since") is not None
            and tf15["riding_bars_since"] >= 1
            and fading_reversal <= -FADING_REVERSAL_MIN
            and (
                tf15.get("fading_below_zone")
                or fading_reversal <= -FADING_PULLBACK_HARD
            )
            and not tf15.get("breakout")
        )
        if fading_ok and not in_cooldown(symbol, "FADING", last_seen):
            sf = "scoring_fading"
            base       = _cfg_score(sf, "BASE_SCORE", 5)
            below_b    = _cfg_score(sf, "BELOW_ZONE_BONUS", 2)
            pullback_b = _cfg_score(sf, "PULLBACK_SIGNIFICANT_BONUS", 1)
            voldead_b  = _cfg_score(sf, "VOL_DEAD_BONUS", 1)
            score = base
            reasons = [f"devolviendo {abs(fading_reversal):.2%} desde el maximo post-break"]
            if tf15.get("fading_below_zone"):
                score += below_b
                reasons.append("precio perforo la zona de soporte rota — senal de salida")
            else:
                score += pullback_b
                reasons.append("pullback significativo — monitoreá zona de soporte")
            if not tf15.get("riding_vol_ok"):
                score += voldead_b
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
                "obv_slope": tf15.get("obv_slope"),
                "cvd_ratio": tf15.get("cvd_ratio"),
                "recent_long_ok": tf15.get("recent_long_ok"),
            })

    # ── HOLD ───────────────────────────────────────────────────────────────────
    if _trad_signals_eligible and ACTIVE_SIGNALS_HOLD:
        require_obv = _cfg("hold", "HOLD_REQUIRE_OBV_RISING", default=False)
        require_cvd = _cfg("hold", "HOLD_REQUIRE_CVD_BULLISH", default=False)
        _hold_obv_bars = _cfg("persistence", "HOLD_OBV_BARS", default=1) if _persist_on else 1
        _obv_ok_hold = (not require_obv) or tf15.get("obv_rising", False)
        if require_obv and _hold_obv_bars >= 2:
            _obv_ok_hold = _obv_ok_hold and tf15.get("obv_slope_prev", 0) >= OBV_RISING_MIN
        _cvd_ok_hold = (not require_cvd) or tf15.get("cvd_bullish", False)
        hold_ok = (
            tf15.get("hold_recent_break")
            and tf15.get("hold_kept_zone")
            and tf15.get("hold_pullback_ok")
            and tf15.get("hold_strong")
            and _obv_ok_hold
            and _cvd_ok_hold
        )
        if hold_ok and not in_cooldown(symbol, "HOLD", last_seen):
            prev = counts_history.get((symbol, "HOLD"), 0)
            sh = "scoring_hold"
            base          = _cfg_score(sh, "BASE_SCORE", 5)
            above_b       = _cfg_score(sh, "ABOVE_RESIST_BONUS", 2)
            pullback_b    = _cfg_score(sh, "PULLBACK_BONUS", 1)
            close_b       = _cfg_score(sh, "STRONG_CLOSE_BONUS", 1)
            dist_high_min = _cfg_score(sh, "DIST_RES_HIGH_MIN", 0.04)
            dist_high_b   = _cfg_score(sh, "DIST_RES_HIGH_BONUS", 2)
            dist_low_b    = _cfg_score(sh, "DIST_RES_LOW_BONUS", 1)
            obv_up_b      = _cfg_score(sh, "OBV_RISING_BONUS", 1)
            obv_dn_pen    = _cfg_score(sh, "OBV_FALLING_PENALTY", -2)
            cvd_up_b      = _cfg_score(sh, "CVD_BULLISH_BONUS", 1)
            cvd_dn_pen    = _cfg_score(sh, "CVD_BEARISH_PENALTY", -2)
            struct_pen    = _cfg_score(sh, "STRUCT_PENALTY", -1)
            late_pen      = _cfg_score(sh, "LATE_REPEAT_PENALTY", -1)
            # Variante G: bonus por momentum extremo (default 0 = inactivo)
            momentum_b    = _cfg_score(sh, "STRONG_MOMENTUM_BONUS", 0)
            momentum_obv  = _cfg_score(sh, "STRONG_MOMENTUM_OBV_MIN", 0.2)
            momentum_dist = _cfg_score(sh, "STRONG_MOMENTUM_DIST_MIN", 0.05)

            score = base
            reasons = [f"ruptura reciente en 15m hace {tf15['bars_since_break']} velas"]
            score += above_b
            reasons.append("sigue arriba de la resistencia rota")
            score += pullback_b
            reasons.append("pullback sano")
            score += close_b
            reasons.append("ultima vela post-break cierra fuerte")
            if tf1h['dist_to_res'] > dist_high_min:
                score += dist_high_b
                reasons.append(f"1h con buen espacio arriba ({tf1h['dist_to_res']:.2%})")
            else:
                score += dist_low_b
                reasons.append("1h acompaña")
            # Bonus OBV/CVD para diferenciar HOLD entre sí
            if tf15.get("obv_rising"):
                score += obv_up_b
                reasons.append(f"OBV sigue subiendo ({tf15['obv_slope']:+.1%}) — zona fortalecida")
            elif tf15.get("obv_slope", 0) < -OBV_RISING_MIN:
                score += obv_dn_pen
                reasons.append(f"⚠ OBV bajando ({tf15['obv_slope']:+.1%}) — zona debilitada")
            if tf15.get("cvd_bullish"):
                score += cvd_up_b
                reasons.append("compradores siguen activos en la zona")
            elif tf15.get("cvd_ratio", 0) < -CVD_BULLISH_MIN:
                score += cvd_dn_pen
                reasons.append(f"⚠ vendedores ganando — zona en riesgo")
            # Penalty si está enterrado bajo máximo mayor
            if not tf15.get("recent_long_ok", True):
                score += struct_pen
                reasons.append("⚠ enterrado bajo máximo mayor")
            # Bonus por momentum extremo: OBV explosivo + CVD bullish + lejos de resistencia
            if (momentum_b > 0
                and tf15.get("obv_slope", 0) >= momentum_obv
                and tf15.get("cvd_bullish")
                and tf1h.get("dist_to_res", 0) >= momentum_dist):
                score += momentum_b
                reasons.append(f"🚀 momentum extremo (OBV {tf15['obv_slope']:+.1%} + CVD bullish + dist {tf1h['dist_to_res']:.2%})")
            if prev >= LATE_REPEAT_COUNT:
                score += late_pen
            score = min(score, SCORE_CAP)
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
                "obv_slope": tf15.get("obv_slope"),
                "cvd_ratio": tf15.get("cvd_ratio"),
                "recent_long_ok": tf15.get("recent_long_ok"),
            })

    if not candidates:
        return None

    # Asignar candle_status a cada candidate.
    # NOTA: con el fix de recorte de vela en formación al inicio de analyze(),
    # cs siempre debería ser "closed". El bloque de penalización queda como defensa
    # en profundidad por si alguien en el futuro agrega un código path que produzca
    # datos forming (ej: otra fuente de datos).
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

    # Penalización suave por EMA 1h no alcista (solo cuando EMA_GATE_HARD=false).
    # Se aplica a candidatos tradicionales — EXPLOSION queda exenta (bypassa EMA por diseño).
    if not _ema_gate_hard and not tf1h.get("ema_trend_up"):
        _ema_pen = int(_cfg("indicators", "EMA_SOFT_PENALTY", default=-2))
        for c in candidates:
            if c.get("history_tf") == "EXPLOSION":
                continue
            c["score"] = max(0, c["score"] + _ema_pen)
            c["bucket"] = final_bucket(c["score"])

    # Cap superior: scores de 20+, 30+, 40+ en BREAKOUT son outliers que NO predicen mejor
    # rendimiento (datos empíricos). Capeamos para que la escala 0-15 sea legible y
    # los thresholds tengan sentido. Se aplica al final, después de bonuses/penalties.
    for c in candidates:
        if c["score"] > SCORE_CAP:
            c["score"] = SCORE_CAP
            c["bucket"] = final_bucket(c["score"])

    candidates.sort(
        key=lambda x: (_normalize_score(x["score"], x["history_tf"]), x["priority"], x["score"]),
        reverse=True,
    )
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
        elif cur["history_tf"] != "FADING" and (
            _normalize_score(alert["score"], alert["history_tf"]),
            alert["priority"],
            alert["score"],
        ) > (
            _normalize_score(cur["score"], cur["history_tf"]),
            cur["priority"],
            cur["score"],
        ):
            best[alert["symbol"]] = alert
    ranked = list(best.values())
    ranked.sort(key=lambda x: (
        1 if x["history_tf"] == "FADING" else 0,
        _normalize_score(x["score"], x["history_tf"]),
        x["priority"],
        x["score"],
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
    if ACTIVE_SIGNALS_PREBREAK:  active_names.append("PRE-BREAK")
    if ACTIVE_SIGNALS_BREAKOUT:  active_names.append("BREAKOUT")
    if ACTIVE_SIGNALS_RIDING:    active_names.append("RIDING")
    if ACTIVE_SIGNALS_FADING:    active_names.append("FADING")
    if ACTIVE_SIGNALS_HOLD:      active_names.append("HOLD")
    if ACTIVE_SIGNALS_EXPLOSION: active_names.append("EXPLOSION")

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

    # Derivatives: pre-fetch en paralelo, en su propio pool para que corra
    # concurrentemente con el scan de klines. Si DERIV_ENABLED=false, no se hace
    # ningún request a fapi.binance.com.
    deriv_map = {}  # symbol -> {"oi_delta_30m", "funding_rate"}
    deriv_executor = None
    deriv_futures = {}
    if DERIV_ENABLED:
        try:
            futures_set = get_futures_usdt_symbols()
        except Exception as e:
            print(f"  [deriv] error fetching futures exchangeInfo: {e}")
            futures_set = set()
        if futures_set:
            deriv_executor = ThreadPoolExecutor(max_workers=MAX_WORKERS)
            deriv_futures = {
                deriv_executor.submit(fetch_derivatives, sym): sym
                for sym in pairs if sym in futures_set
            }

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(analyze, sym, tf): (sym, tf) for sym, tf in tasks}

        # Recolectar derivatives mientras los klines corren en paralelo.
        for fut, sym in deriv_futures.items():
            try:
                deriv_map[sym] = fut.result()
            except Exception:
                deriv_map[sym] = {"oi_delta_30m": None, "funding_rate": None}
        if deriv_executor is not None:
            deriv_executor.shutdown(wait=False)

        for future in as_completed(futures):
            symbol, interval, data = future.result()
            per_symbol[symbol][interval] = data or {}
            ready = all(tf in per_symbol[symbol] for tf in INTERVALS)
            if not ready or symbol in processed:
                continue

            # Inyectar features de derivatives en el 15m TF antes de classify
            if DERIV_ENABLED and per_symbol[symbol].get("15m"):
                d = deriv_map.get(symbol, {})
                per_symbol[symbol]["15m"]["oi_delta_30m"] = d.get("oi_delta_30m")
                per_symbol[symbol]["15m"]["funding_rate"] = d.get("funding_rate")

            alert = classify_symbol(symbol, per_symbol[symbol], counts_history, last_seen)
            processed.add(symbol)
            if not alert:
                continue

            candidates.append(alert)
            print(f"  {alert['label']} {symbol} score={alert['score']} bucket={alert['bucket']}")

            key = (alert["symbol"], alert["history_tf"])
            if alert.get("bucket") == "BEST" and key not in immediate_sent_keys:
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

    riding_count = sum(1 for a in candidates if a["history_tf"] == "RIDING")
    fading_count = sum(1 for a in candidates if a["history_tf"] == "FADING")
    print(f"Total candidatos: {len(candidates)} (RIDING: {riding_count}, FADING: {fading_count})")
    print(f"Inmediatos enviados: {len(immediate_sent_keys)} | saltados: {immediate_skipped}")
    print(f"Top final: {len(selected)}")


if __name__ == "__main__":
    main()
