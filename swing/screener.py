"""
Screener SWING en vivo — espejo del backtest de swing.

A diferencia del daytrader (screener.py en la raíz), swing opera en timeframes
altos (1h/4h/1d/1w) y con sus propias señales: PRE-BREAK, BREAKOUT, RIDING, HOLD
y COILING (sin EXPLOSION ni derivatives).

Arquitectura: la lógica de detección/scoring NO se duplica aquí. Se importa
directamente del motor del backtest (backtest.py) — `analyze_at_time()` calcula los
features de una vela cerrada y `classify()` decide la señal. Así screener y backtest
NO pueden desincronizarse: corren exactamente el mismo código. Este archivo solo
aporta el andamiaje de I/O en vivo (Binance, Telegram, Supabase, anti-spam),
copiado/adaptado del screener.py raíz.

Mismo chat de Telegram y mismas tablas de Supabase que el daytrader (sin aislamiento).

Variables de entorno requeridas: TELEGRAM_TOKEN, TELEGRAM_CHAT_ID, SUPABASE_KEY.
Configuración en swing/config.json.
"""

import os
import re
from datetime import datetime, timezone, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
from io import BytesIO
from pathlib import Path

import pandas as pd
import requests
from requests.adapters import HTTPAdapter
import ta

# Motor de detección/scoring compartido con el backtest de swing. Importarlo es seguro:
# argparse vive dentro de main() (guardado por __main__) y a nivel módulo solo hay
# constantes. classify() referencia internamente final_bucket/_normalize_score/etc.,
# que resuelven dentro del módulo backtest — no hay que tocarlos.
from backtest import Config, analyze_at_time, classify, _normalize_score

SESSION = requests.Session()

adapter = HTTPAdapter(
    pool_connections=100,
    pool_maxsize=100,
)
SESSION.mount("https://", adapter)
SESSION.mount("http://", adapter)

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

# Config es la misma clase que usa el backtest: g() con defaults y .raw con el dict crudo.
CFG = Config(str(CONFIG_PATH))


def _g(section, key, default=None):
    """Atajo a CFG.g con default explícito (None aborta solo si falta sin default)."""
    return CFG.g(section, key, default=default)


# ── Variables de entorno ──────────────────────────────────────────────────────
TELEGRAM_TOKEN = os.environ["TELEGRAM_TOKEN"]
TELEGRAM_CHAT_ID = os.environ["TELEGRAM_CHAT_ID"]
SUPABASE_KEY = os.environ["SUPABASE_KEY"]
SUPABASE_URL = "https://ecgdswroygkfckkaguxp.supabase.co"

# ── Configuración derivada (solo I/O; el scoring lo lee classify vía CFG) ───────
INTERVALS         = _g("general", "INTERVALS", default=["1h", "4h", "1d", "1w"])
LIMIT             = _g("general", "LIMIT", default=200)
TOP_N             = _g("general", "TOP_N", default=9999)
MAX_WORKERS       = _g("general", "MAX_WORKERS", default=20)
MIN_QUOTE_VOLUME  = _g("general", "MIN_QUOTE_VOLUME", default=1000000)
TOP_ALERT_COUNT   = _g("general", "TOP_ALERT_COUNT", default=5)

HISTORY_HOURS           = _g("history", "HISTORY_HOURS", default=24)
SNAPSHOT_RETENTION_DAYS = _g("history", "SNAPSHOT_RETENTION_DAYS", default=30)

# Badge de convicción: atr_pct_1d es el único separador robusto de movers (gate a BEST=5).
# Por encima de ATR_BADGE_HIGH la alerta se marca como alta convicción (HOME estaba en ~6.5,
# o sea gated-pero-no-🔥, que es la historia correcta).
ATR_BADGE_HIGH = _g("history", "ATR_BADGE_HIGH", default=8.0)

# Racha cross-señal (Idea 2): ventana multi-día para detectar reincidencia/convicción.
# counts_history (24h) está acoplado a LATE_REPEAT (scoring) y no se puede ensanchar; en
# cambio screener_outcomes retiene RETENTION_DAYS y trae signal_type, así que la racha se
# computa ahí. 14d cubre arcos tipo HOME (COILING→HOLD→RIDING en ~13d) sin diluir.
STREAK_LOOKBACK_DAYS = _g("history", "STREAK_LOOKBACK_DAYS", default=14)

# Watchlist momentum (Idea 3): dragnet diario F/H que NO se pinguea (recall-recovery sin
# spam). F=precio↑3d + OBV slope≥0; H=F + %B>0.8 (banda alta). Recupera ~+30pp de movers que
# las señales BEST se pierden, a costa de ~55 hits/día / ~90% falsos → SOLO sink (tabla
# swing_watchlist), jamás Telegram. Def de _faseB_recall.daily_flags (validada).
WATCHLIST_ENABLED        = _g("watchlist", "ENABLED", default=True)
WATCHLIST_OBV_LB         = _g("watchlist", "OBV_LOOKBACK", default=10)
WATCHLIST_RETENTION_DAYS = _g("watchlist", "RETENTION_DAYS", default=14)

MAX_IMMEDIATE_PER_RUN = _g("anti_spam", "MAX_IMMEDIATE_PER_RUN", default=5)

COOLDOWN_BY_STATE = CFG.raw.get("cooldowns_minutes", {})

SCORE_CAP   = _g("scoring", "SCORE_CAP", default=15)
EMA_SLOW    = _g("indicators", "EMA_SLOW", default=21)

CHART_ENABLED = _g("chart", "ENABLED", default=False)
CHART_BARS    = _g("chart", "BARS", default=50)
CHART_WIDTH   = _g("chart", "WIDTH", default=1000)
CHART_HEIGHT  = _g("chart", "HEIGHT", default=600)
CHART_STYLE   = _g("chart", "STYLE", default="nightclouds")

OUTCOMES_ENABLED = _g("outcomes", "ENABLED", default=False)

_CAL_CFG = CFG.raw.get("scoring_calibration") or {}
_REGIME_CFG = CFG.raw.get("regime_filter") or {}


def market_regime_up():
    """Estado risk-on/risk-off del mercado para este scan (global, no per-símbolo).

    Espejo en vivo de backtest.MarketRegime: close 1d del símbolo de referencia sobre su
    SMA. Devuelve None si el filtro está off o si falla el fetch → classify no aplica nada
    (fail-safe: ante duda no bloqueamos señales).
    """
    if not _REGIME_CFG.get("ENABLED", False):
        return None
    sym = _REGIME_CFG.get("SYMBOL", "BTCUSDT")
    bars = int(_REGIME_CFG.get("MA_BARS_1D", 20))
    try:
        df = get_klines(sym, "1d")
    except Exception as e:
        print(f"  regime: fetch {sym} 1d falló ({e}) → filtro inerte este run")
        return None
    # Descartar la vela diaria en formación: el régimen se evalúa sobre la última CERRADA.
    df = df.iloc[:-1]
    if len(df) < bars + 1:
        print(f"  regime: {len(df)} barras 1d < {bars + 1} → filtro inerte este run")
        return None
    close = df["close"].astype(float)
    return bool(close.iloc[-1] > close.rolling(bars).mean().iloc[-1])


def _norm(raw_score, signal_type):
    """Wrapper sobre el _normalize_score del backtest (firma de 4 args)."""
    return _normalize_score(raw_score, signal_type, _CAL_CFG, SCORE_CAP)


# ── Supabase ───────────────────────────────────────────────────────────────────
def _sb_headers():
    return {
        "apikey": SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}",
        "Content-Type": "application/json",
        "Prefer": "return=minimal",
    }


def fetch_history():
    """Lee alertas de las últimas HISTORY_HOURS de screener_history → (counts, last_seen).
    counts[(symbol, history_tf)] = nº de alertas (para LATE_REPEAT en classify).
    last_seen[(symbol, history_tf)] = timestamp de la última (para cooldowns)."""
    since = (datetime.now(timezone.utc) - timedelta(hours=HISTORY_HOURS)).isoformat()
    try:
        r = SESSION.get(
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


def fetch_symbol_streak(symbol):
    """Racha cross-señal de un símbolo en los últimos STREAK_LOOKBACK_DAYS, leída de
    screener_outcomes (retención larga, trae signal_type). Devuelve dict o None:
      {seq: [tipos distintos en orden cronológico], n_types, total, span_days}
    La repetición — sobre todo cross-tipo (COILING→HOLD→RIDING) — es la huella del mover
    sostenido. Guardrail: elevar, NO suprimir. Query acotada a un símbolo (≤5/run)."""
    since = (datetime.now(timezone.utc) - timedelta(days=STREAK_LOOKBACK_DAYS)).isoformat()
    try:
        r = SESSION.get(
            f"{SUPABASE_URL}/rest/v1/screener_outcomes",
            headers={**_sb_headers(), "Prefer": ""},
            # desc+limit: si PostgREST trunca (cap ~1000), nos quedamos con lo MÁS reciente
            # (que es el punto de la racha), no con lo más viejo. Reordenamos asc en Python.
            params={"select": "signal_type,alerted_at", "symbol": f"eq.{symbol}",
                    "alerted_at": f"gte.{since}", "order": "alerted_at.desc", "limit": 500},
            timeout=10,
        )
        r.raise_for_status()
        rows = r.json()
        if not rows:
            return None
        rows.reverse()   # → orden cronológico (asc) para construir el arco seq
        seq = []
        for row in rows:
            t = row.get("signal_type")
            if t and (not seq or seq[-1] != t):   # colapsa repeticiones consecutivas
                seq.append(t)
        first = datetime.fromisoformat(rows[0]["alerted_at"].replace("Z", "+00:00"))
        last  = datetime.fromisoformat(rows[-1]["alerted_at"].replace("Z", "+00:00"))
        span_days = (last - first).total_seconds() / 86400.0
        return {"seq": seq, "n_types": len(set(seq)), "total": len(rows), "span_days": span_days}
    except Exception as e:
        print(f"Supabase fetch_symbol_streak error: {e}")
        return None


def insert_watchlist(rows):
    """Escribe el watchlist momentum (Idea 3) en swing_watchlist y purga filas viejas.
    Sink paralelo: NO toca screener_history/outcomes ni Telegram. No-op si está deshabilitado
    o no hay hits. Si la tabla no existe en Supabase, falla en silencio (logueado)."""
    if not WATCHLIST_ENABLED or not rows:
        return
    now = datetime.now(timezone.utc)
    since = (now - timedelta(days=WATCHLIST_RETENTION_DAYS)).isoformat()
    try:
        # upsert por (symbol, scan_date): merge-duplicates pisa la fila del día con el último scan.
        up_headers = {**_sb_headers(), "Prefer": "resolution=merge-duplicates,return=minimal"}
        r = SESSION.post(f"{SUPABASE_URL}/rest/v1/swing_watchlist",
                         headers=up_headers, params={"on_conflict": "symbol,scan_date"},
                         json=rows, timeout=10)
        r.raise_for_status()
        resp = SESSION.delete(
            f"{SUPABASE_URL}/rest/v1/swing_watchlist",
            headers=_sb_headers(),
            params={"scanned_at": f"lt.{since}"},
            timeout=10,
        )
        resp.raise_for_status()
        print(f"  watchlist: {len(rows)} hits F/H escritos")
    except Exception as e:
        print(f"Supabase insert_watchlist error: {e}")


def insert_history(alert_rows):
    """Registra las alertas seleccionadas en screener_history (timeframe = history_tf)
    y purga filas más viejas que HISTORY_HOURS."""
    now = datetime.now(timezone.utc)
    now_iso = now.isoformat()
    since = (now - timedelta(hours=HISTORY_HOURS)).isoformat()
    rows = [{"symbol": row["symbol"], "timeframe": row["history_tf"], "alerted_at": now_iso}
            for row in alert_rows]
    if not rows:
        return
    try:
        r = SESSION.post(f"{SUPABASE_URL}/rest/v1/screener_history",
                         headers=_sb_headers(), json=rows, timeout=10)
        r.raise_for_status()
        resp = SESSION.delete(
            f"{SUPABASE_URL}/rest/v1/screener_history",
            headers=_sb_headers(),
            params={"alerted_at": f"lt.{since}"},
            timeout=10,
        )
        resp.raise_for_status()
    except Exception as e:
        print(f"Supabase insert_history error: {e}")


def insert_pairs_snapshot(pairs):
    """Snapshot de qué pares cotizaban en este run + auto-purge de filas viejas.
    Sirve para reconstruir el universo histórico al armar tracking de outcomes."""
    if not pairs:
        return
    now = datetime.now(timezone.utc)
    now_iso = now.isoformat()
    purge_before = (now - timedelta(days=SNAPSHOT_RETENTION_DAYS)).isoformat()
    row = {"run_at": now_iso, "symbols": sorted(pairs)}
    try:
        r = SESSION.post(
            f"{SUPABASE_URL}/rest/v1/screener_runs",
            headers=_sb_headers(),
            json=row,
            timeout=15,
        )
        r.raise_for_status()
        # Auto-purge
        r2 = SESSION.delete(
            f"{SUPABASE_URL}/rest/v1/screener_runs",
            headers=_sb_headers(),
            params={"run_at": f"lt.{purge_before}"},
            timeout=15,
        )
        r2.raise_for_status()
        print(f"  snapshot: {len(row['symbols'])} pares guardados (purge >{SNAPSHOT_RETENTION_DAYS}d)")
    except Exception as e:
        print(f"Supabase insert_pairs_snapshot error: {e}")


def insert_outcomes(alerts):
    """Inserta filas en screener_outcomes para tracking de cómo evolucionan las alertas.
    Solo guarda el snapshot del momento; los precios futuros los completa update_outcomes.py.
    No-op si outcomes.ENABLED=false (default en swing)."""
    if not OUTCOMES_ENABLED or not alerts:
        return
    now_iso = datetime.now(timezone.utc).isoformat()
    rows = []
    for a in alerts:
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
        r = SESSION.post(
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
    r = SESSION.get("https://data-api.binance.vision/api/v3/exchangeInfo", timeout=20)
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
    r = SESSION.get("https://data-api.binance.vision/api/v3/ticker/24hr", timeout=15)
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
    """Klines en vivo. Devuelve el df con las columnas exactas que analyze_at_time espera
    (close_time int64, OHLCV float, taker_buy_base para el CVD)."""
    r = SESSION.get(
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
    df["close_time"] = df["close_time"].astype("int64")
    df["open_time"] = df["open_time"].astype("int64")
    return df


def safe_pct(a, b):
    return (a / b - 1) if b else 0.0


def in_cooldown(symbol, history_tf, last_seen):
    ts = last_seen.get((symbol, history_tf))
    if not ts:
        return False
    cooldown_minutes = COOLDOWN_BY_STATE.get(history_tf, 60)
    return datetime.now(timezone.utc) - ts < timedelta(minutes=cooldown_minutes)


# ── Telegram ─────────────────────────────────────────────────────────────────
def _sin_token(e):
    """Saca el token del mensaje de error antes de imprimirlo.

    El mensaje de una excepcion de requests incluye la URL completa, y la URL de la
    API de Telegram lleva el token adentro (`/bot<TOKEN>/sendMessage`). Imprimir la
    excepcion cruda lo escribe en el log de Actions, que en un repo publico lee
    cualquiera: fue el vector por el que se filtro el token el 2026-08-22.
    Nunca imprimir `e` directo en un except que envuelva una llamada a Telegram.
    """
    return re.sub(r"bot\d{6,12}:[A-Za-z0-9_-]{30,}", "bot***", str(e))


def send_telegram(text):
    try:
        r = SESSION.post(
            f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage",
            json={"chat_id": TELEGRAM_CHAT_ID, "text": text, "parse_mode": "Markdown"},
            timeout=10,
        )
        r.raise_for_status()
    except Exception as e:
        print(f"  send_telegram error: {_sin_token(e)}")
        try:
            SESSION.post(
                f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage",
                json={"chat_id": TELEGRAM_CHAT_ID, "text": text},
                timeout=10,
            )
        except Exception:
            pass


# TF del screener → intervalo de TradingView
TV_TF_MAP = {
    "1h": "60", "2h": "120", "4h": "240", "1d": "D", "1w": "W",
}


def tradingview_link(symbol, timeframe="4h"):
    tv_interval = TV_TF_MAP.get(timeframe, "240")
    url = f"https://www.tradingview.com/chart/?symbol=BINANCE:{symbol}&interval={tv_interval}"
    return f"[{symbol}]({url})"


def generate_chart_image(df, symbol, timeframe, ref_price=None):
    """Genera una PNG en memoria con las últimas CHART_BARS velas + BB(20,2) + EMA + volumen.
    Si ref_price está, dibuja una línea horizontal (zona de ruptura/breakout). Devuelve
    BytesIO o None si mplfinance no está disponible, el chart está off, o algo falla.
    Adaptado de generate_chart_image del screener.py raíz."""
    if not _HAS_MPLFINANCE or not CHART_ENABLED or df is None:
        return None
    try:
        chart_df = df.tail(CHART_BARS).copy()
        # mplfinance espera OHLCV capitalizado con DatetimeIndex.
        chart_df["timestamp"] = pd.to_datetime(chart_df["open_time"], unit="ms", utc=True)
        chart_df = chart_df.set_index("timestamp")
        chart_df = chart_df.rename(columns={
            "open": "Open", "high": "High", "low": "Low",
            "close": "Close", "volume": "Volume",
        })
        chart_df = chart_df[["Open", "High", "Low", "Close", "Volume"]]

        # BB y EMA sobre el df recortado para que coincida el largo de las series.
        bb = ta.volatility.BollingerBands(chart_df["Close"], window=20, window_dev=2)
        ema = ta.trend.EMAIndicator(chart_df["Close"], window=EMA_SLOW).ema_indicator()
        addplots = [
            mpf.make_addplot(bb.bollinger_hband(), color="#888", width=0.7),
            mpf.make_addplot(bb.bollinger_lband(), color="#888", width=0.7),
            mpf.make_addplot(bb.bollinger_mavg(),  color="#888", width=0.5, linestyle="--"),
            mpf.make_addplot(ema, color="#e3b341", width=1.0),
        ]

        buf = BytesIO()
        kwargs = dict(
            type="candle",
            style=CHART_STYLE,
            addplot=addplots,
            volume=True,
            figsize=(CHART_WIDTH / 100, CHART_HEIGHT / 100),  # px → pulgadas a 100 dpi
            title=f"\n{symbol} • {timeframe}",
            tight_layout=True,
            savefig=dict(fname=buf, format="png", dpi=100, bbox_inches="tight"),
        )
        if ref_price and ref_price > 0:
            kwargs["hlines"] = dict(hlines=[float(ref_price)], colors=["#39d353"],
                                    linestyle="--", linewidths=0.8)

        mpf.plot(chart_df, **kwargs)
        buf.seek(0)
        return buf
    except Exception as e:
        print(f"  generate_chart_image error {symbol} {timeframe}: {e}")
        return None


def send_telegram_photo(image_buf, caption):
    """Envía una imagen a Telegram con caption (límite 1024 chars)."""
    if len(caption) > 1024:
        caption = caption[:1020] + "…"
    try:
        files = {"photo": ("chart.png", image_buf, "image/png")}
        data = {"chat_id": TELEGRAM_CHAT_ID, "caption": caption, "parse_mode": "Markdown"}
        r = SESSION.post(
            f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendPhoto",
            data=data, files=files, timeout=15,
        )
        r.raise_for_status()
    except Exception as e:
        print(f"  send_telegram_photo error: {_sin_token(e)}")
        try:
            send_telegram(caption)
        except Exception:
            pass


# ── Formato de alerta ──────────────────────────────────────────────────────────
def _reasons_from_alert(alert):
    """Construye líneas legibles a partir del breakdown de scoring + contexto OBV/CVD.
    El classify de swing entrega 'breakdown' (dict componente→puntos), no 'reasons'."""
    reasons = []
    obv = alert.get("obv_slope")
    cvd = alert.get("cvd_ratio")
    if obv is not None:
        reasons.append(f"OBV slope {obv:+.2%}")
    if cvd is not None:
        reasons.append(f"CVD ratio {cvd:+.2%}")
    bd = alert.get("breakdown") or {}
    items = [(k, v) for k, v in bd.items()
             if isinstance(v, (int, float)) and not isinstance(v, bool) and k != "BASE"]
    items.sort(key=lambda kv: abs(kv[1]), reverse=True)
    for k, v in items:
        # Backticks (code span) para que los '_' de los nombres de componente no rompan
        # el parser Markdown de Telegram (un nº impar de '_' → 400 y la alerta se pierde).
        reasons.append(f"`{k}`: {v:+g}")
    return reasons or ["sin desglose de scoring"]


def _streak_line(streak):
    """Línea de convicción (Idea 2): reincidencia multi-día, sobre todo cross-tipo.
    None si no hay racha que valga la pena mostrar (≤1 alerta en la ventana)."""
    if not streak or streak["total"] < 2:
        return None
    span = max(1, round(streak["span_days"]))
    if streak["n_types"] >= 2:
        arc = "→".join(streak["seq"])
        return f"  🔁 convicción: {arc} ({streak['total']}× / {span}d)"
    return f"  🔁 reincide: {streak['seq'][0]} {streak['total']}× / {span}d"


def _hold_candidate_line(alert, prev, streak):
    """Línea de tendencia sostenida (Idea 4): RIDING/HOLD que reinciden = candidata a hold,
    darle correa larga. El payoff madura >7d y RIDING rinde mejor a 21d (+30.8%); el valor
    está en aguantar, no en adelantar. None si no aplica."""
    if alert["history_tf"] not in ("RIDING", "HOLD"):
        return None
    repite = prev > 0 or bool(streak and streak["total"] >= 2)
    if not repite:
        return None
    return "  🪢 candidata a hold — correa larga (seguir 14/21d)"


def format_alert(alert, counts_history, with_reasons=True, streak=None):
    prev = counts_history.get((alert["symbol"], alert["history_tf"]), 0)

    if alert["history_tf"] == "RIDING" and prev > 0:
        hist_tag = f"  _(confirmo {prev}x en {HISTORY_HOURS}h)_"
    elif prev > 0:
        hist_tag = f"  _(+{prev} en {HISTORY_HOURS}h)_"
    else:
        hist_tag = ""

    emoji = {
        "RIDING": "🚀",
        "BREAKOUT": "💥",
        "PRE-BREAK": "👀",
        "HOLD": "🤝",
        "COILING": "🌀",
    }.get(alert["label"], "")

    # Badge de convicción por atr_pct_1d (separador robusto). Siempre muestra el valor;
    # 🔥 sólo cuando supera ATR_BADGE_HIGH (alta convicción).
    atr1d = alert.get("atr_pct_1d")
    if atr1d is not None:
        atr_badge = f" 🔥atr{atr1d:.1f}" if atr1d >= ATR_BADGE_HIGH else f" atr{atr1d:.1f}"
    else:
        atr_badge = ""

    header = (
        f"{emoji} [{alert['bucket']}] [{alert['label']}] {alert['symbol']} "
        f"score {alert['score']}{atr_badge} • {alert['timeframe']} "
        f"{tradingview_link(alert['symbol'], alert['timeframe'])}{hist_tag}"
    )

    if not with_reasons:
        return header

    price = alert.get("price", 0)
    ref = alert.get("ref_price") or 0
    if ref and ref > 0 and price > 0:
        pct = (price - ref) / ref * 100
        if alert["history_tf"] == "RIDING":
            price_line = f"  💰 {price:.6g} USDT ({pct:+.2f}% desde breakout)"
        else:
            price_line = f"  💰 {price:.6g} USDT ({pct:+.2f}% desde zona de ruptura)"
    else:
        price_line = f"  💰 {price:.6g} USDT"

    cs = alert.get("candle_status", "closed")
    candle_line = "  ✅ vela cerrada" if cs == "closed" else "  🟡 vela en formación"

    reasons = alert.get("reasons") or _reasons_from_alert(alert)
    body = "\n".join(f"  - {r}" for r in reasons[:3])
    lines = [header, price_line, candle_line]
    streak_line = _streak_line(streak)
    if streak_line:
        lines.append(streak_line)
    hold_line = _hold_candidate_line(alert, prev, streak)
    if hold_line:
        lines.append(hold_line)
    lines.append(body)
    return "\n".join(lines)


def dedupe_and_rank(alerts):
    """Una alerta por símbolo (la de mayor score normalizado) y top TOP_ALERT_COUNT."""
    best = {}
    for alert in alerts:
        cur = best.get(alert["symbol"])
        if cur is None:
            best[alert["symbol"]] = alert
        elif (
            _norm(alert["score"], alert["history_tf"]),
            alert["priority"],
            alert["score"],
        ) > (
            _norm(cur["score"], cur["history_tf"]),
            cur["priority"],
            cur["score"],
        ):
            best[alert["symbol"]] = alert
    ranked = list(best.values())
    ranked.sort(key=lambda x: (
        _norm(x["score"], x["history_tf"]),
        x["priority"],
        x["score"],
    ), reverse=True)
    return ranked[:TOP_ALERT_COUNT]


def send_immediate(alert, counts_history, dfs=None):
    """Envía alerta inmediata (bucket BEST). Si hay chart disponible lo manda con sendPhoto;
    si no, fallback a texto plano. `dfs` = {(symbol, timeframe): df_cerrado} del scan."""
    streak = fetch_symbol_streak(alert["symbol"])
    text = f"🔥 PRIORITY NOW\n{format_alert(alert, counts_history, streak=streak)}"

    if dfs is not None and CHART_ENABLED and _HAS_MPLFINANCE:
        df = dfs.get((alert["symbol"], alert["timeframe"]))
        img = generate_chart_image(df, alert["symbol"], alert["timeframe"],
                                   ref_price=alert.get("ref_price"))
        if img is not None:
            send_telegram_photo(img, text)
            return

    send_telegram(text)


# ── Análisis por (símbolo, intervalo) ──────────────────────────────────────────
def _watch_flags(df1d):
    """Flags F/H (watchlist momentum, Idea 3) sobre la ÚLTIMA barra diaria cerrada.
    Espejo exacto de _faseB_recall.daily_flags (definición validada):
      F = close>close[-3] & obv_slope≥0;  H = F & %B>0.8.
    Devuelve (F:bool, H:bool, pctb:float) o None si no hay barras suficientes."""
    if df1d is None or len(df1d) < 25:
        return None
    c, v = df1d["close"], df1d["volume"]
    bb = ta.volatility.BollingerBands(c, window=20, window_dev=2)
    hb, lb = bb.bollinger_hband(), bb.bollinger_lband()
    pctb = ((c - lb) / (hb - lb)).clip(-1, 2)
    obv = ta.volume.OnBalanceVolumeIndicator(c, v).on_balance_volume()
    obv_slope = (obv - obv.shift(WATCHLIST_OBV_LB)) / obv.abs()
    up3 = bool(c.iloc[-1] > c.iloc[-4])
    os_last = obv_slope.iloc[-1]
    pb_last = pctb.iloc[-1]
    if pd.isna(os_last) or pd.isna(pb_last):
        return None
    F = up3 and (os_last >= 0)
    H = F and (pb_last > 0.8)
    return bool(F), bool(H), float(pb_last)


def analyze(symbol, interval):
    """Fetch klines en vivo, descarta la vela en formación y calcula los features con el
    motor del backtest (analyze_at_time) mirando la última vela CERRADA. Devuelve
    (symbol, interval, feature_dict | None, df_cerrado | None). El df se conserva sólo
    para dibujar el chart de los BEST inmediatos (mismas velas que se analizaron)."""
    try:
        df = get_klines(symbol, interval)
    except Exception:
        return symbol, interval, None, None

    # Binance incluye la vela en formación (close_time futuro). Trabajar con vela viva
    # mete ruido (vol_ratio incompleto, breakouts intra-vela, strong_close inválido).
    # Si la última todavía no cerró, la descartamos → iloc[-1] = última vela cerrada,
    # espejo exacto de cómo el backtest escanea en bordes de vela.
    if len(df):
        last_close_time_ms = int(df["close_time"].iloc[-1])
        now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
        if now_ms < last_close_time_ms:
            df = df.iloc[:-1]

    if len(df) < 80:
        return symbol, interval, None, None

    try:
        feat = analyze_at_time(df, len(df) - 1, CFG)
    except Exception as e:
        print(f"  analyze error {symbol} {interval}: {e}")
        return symbol, interval, None, None

    # Watchlist momentum (Idea 3): F/H se computan sobre el daily crudo. Reusa el df ya
    # fetcheado (sin red extra); se stashea en el feat 1d para que main() lo recoja sin
    # tocar el flujo de alerts. Path separado, no afecta classify ni el push.
    if interval == "1d" and WATCHLIST_ENABLED and feat is not None:
        wf = _watch_flags(df)
        if wf is not None:
            feat["watch_F"], feat["watch_H"], feat["watch_pctb"] = wf

    return symbol, interval, feat, df


def main():
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    pairs = get_all_usdt_pairs()

    active_names = []
    for name, label in (("PREBREAK", "PRE-BREAK"), ("BREAKOUT", "BREAKOUT"),
                        ("RIDING", "RIDING"), ("HOLD", "HOLD"), ("COILING", "COILING")):
        if CFG.g("active_signals", name, default=False):
            active_names.append(label)

    print(f"[{now}] SWING — escaneando {len(pairs)} pares USDT "
          f"({' + '.join(INTERVALS)}) con {MAX_WORKERS} workers...")
    print(f"Señales activas: {', '.join(active_names) if active_names else 'NINGUNA'}")
    # Diagnóstico de charts: si mplfinance no importó (o el chart está off), los BEST salen
    # como texto+link sin PNG. Este log confirma de un vistazo por qué no aparece el chart.
    print(f"Charts: ENABLED={CHART_ENABLED} mplfinance={_HAS_MPLFINANCE} "
          f"→ {'PNG vía sendPhoto' if (CHART_ENABLED and _HAS_MPLFINANCE) else 'solo texto+link'}")

    insert_pairs_snapshot(pairs)

    # Régimen de mercado: se resuelve UNA vez por scan (es global) y se le pasa a classify.
    regime_up = market_regime_up()
    if regime_up is not None:
        print(f"Régimen de mercado ({_REGIME_CFG.get('SYMBOL', 'BTCUSDT')} 1d vs SMA"
              f"{_REGIME_CFG.get('MA_BARS_1D', 20)}): "
              f"{'RISK-ON' if regime_up else 'RISK-OFF'} → "
              f"{'sin ajuste' if regime_up else _REGIME_CFG.get('MODE', 'soft').upper()}")

    counts_history, last_seen = fetch_history()
    tasks = [(sym, tf) for sym in pairs for tf in INTERVALS]
    per_symbol = {sym: {} for sym in pairs}
    # df de velas cerradas por (symbol, interval); sólo para dibujar charts de BEST inmediatos.
    dfs = {} if CHART_ENABLED and _HAS_MPLFINANCE else None
    candidates = []
    processed = set()

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(analyze, sym, tf): (sym, tf) for sym, tf in tasks}

        for future in as_completed(futures):
            symbol, interval, data, df = future.result()
            per_symbol[symbol][interval] = data or {}
            if dfs is not None and df is not None:
                dfs[(symbol, interval)] = df
            ready = all(tf in per_symbol[symbol] for tf in INTERVALS)
            if not ready or symbol in processed:
                continue

            # classify recibe el dict {tf: features}; requiere 1h/4h/1d (1w opcional, fail-safe).
            alert = classify(symbol, per_symbol[symbol], CFG, counts_history,
                             regime_up=regime_up)
            processed.add(symbol)
            if not alert:
                continue

            # classify no setea 'symbol' ni 'reasons' — los inyectamos para el I/O.
            alert["symbol"] = symbol
            alert["reasons"] = _reasons_from_alert(alert)

            # Cooldown: el swing aplica cooldown al best candidate retornado (no por bloque),
            # igual que _classify_pass del backtest.
            if in_cooldown(symbol, alert["history_tf"], last_seen):
                continue

            candidates.append(alert)
            print(f"  {alert['label']} {symbol} score={alert['score']} bucket={alert['bucket']}")

    # Watchlist momentum (Idea 3): sink paralelo sobre TODO el universo (no solo alertados),
    # por eso va antes del early-return de candidates. F es superset de H, así que escribimos
    # las filas con F=true y marcamos flag_h en las más fuertes. Nunca se pinguea.
    if WATCHLIST_ENABLED:
        wl_dt = datetime.now(timezone.utc)
        wl_now, wl_date = wl_dt.isoformat(), wl_dt.date().isoformat()
        wl_rows = []
        for sym, tfs in per_symbol.items():
            d1 = tfs.get("1d") or {}
            if not d1.get("watch_F"):
                continue
            # scan_date + upsert → 1 fila por símbolo/día (F/H es flag DIARIA: la última barra
            # cerrada es la misma en cada run del día; sin esto se acumularían dupes por run).
            wl_rows.append({
                "scan_date": wl_date,
                "scanned_at": wl_now,
                "symbol": sym,
                "flag_f": True,
                "flag_h": bool(d1.get("watch_H")),
                "price": float(d1["price"]) if d1.get("price") is not None else None,
                "atr_pct_1d": float(d1["atr_pct"]) if d1.get("atr_pct") is not None else None,
                "pctb_1d": float(d1["watch_pctb"]) if d1.get("watch_pctb") is not None else None,
            })
        insert_watchlist(wl_rows)

    # Inmediatos (BEST → "PRIORITY NOW"): rankear por convicción y pushear top-N (D1a).
    # Antes se enviaban en orden de finalización de thread (≈ azar) y se cortaba al llegar al
    # tope, así que con >5 BEST se perdían los mejores. Ahora se juntan todos los BEST y se
    # mandan los de mayor atr_pct_1d (único separador robusto; el score no separa). Diferir el
    # envío al final del scan no agrega latencia material (el scan es el cuello de botella).
    best_imm = {}
    for a in candidates:
        if a.get("bucket") != "BEST":
            continue
        k = (a["symbol"], a["history_tf"])
        if k not in best_imm or a["score"] > best_imm[k]["score"]:
            best_imm[k] = a
    ranked_imm = sorted(
        best_imm.values(),
        key=lambda a: (a.get("atr_pct_1d") or 0, _norm(a["score"], a["history_tf"]), a["score"]),
        reverse=True,
    )
    immediate_sent_alerts = []
    for a in ranked_imm[:MAX_IMMEDIATE_PER_RUN]:
        try:
            send_immediate(a, counts_history, dfs)
            immediate_sent_alerts.append(a)
            print(f"  inmediata BEST {a['symbol']} (atr{(a.get('atr_pct_1d') or 0):.1f})")
        except Exception as e:
            print(f"  error envio inmediato {a['symbol']}: {e}")
    immediate_skipped = max(0, len(ranked_imm) - MAX_IMMEDIATE_PER_RUN)
    for a in ranked_imm[MAX_IMMEDIATE_PER_RUN:]:
        print(f"  inmediata saltada (tope {MAX_IMMEDIATE_PER_RUN}): {a['symbol']}")

    if not candidates:
        print("Sin setups en este run.")
        return

    selected = dedupe_and_rank(candidates)
    selected_keys = {(a["symbol"], a["history_tf"]) for a in selected}
    extra_immediates = [a for a in immediate_sent_alerts
                        if (a["symbol"], a["history_tf"]) not in selected_keys]
    # D4: grabar también los inmediatos en history (no solo el top-5 seleccionado) → marcan
    # cooldown el próximo run y no se re-pushean a la hora (la fuga que causaba doble-push,
    # sobre todo COILING horario). extra_immediates no se solapa con selected (filtrado por key).
    insert_history(selected + extra_immediates)
    insert_outcomes(selected + extra_immediates)

    riding_count = sum(1 for a in candidates if a["history_tf"] == "RIDING")
    coiling_count = sum(1 for a in candidates if a["history_tf"] == "COILING")
    print(f"Total candidatos: {len(candidates)} (RIDING: {riding_count}, COILING: {coiling_count})")
    print(f"Inmediatos enviados: {len(immediate_sent_alerts)} | saltados: {immediate_skipped}")
    print(f"Top final: {len(selected)}")


if __name__ == "__main__":
    main()
