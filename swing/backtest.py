"""
Backtest del screener sobre datos históricos de Binance.

Uso básico:
    python backtest.py --weeks 1
    python backtest.py --weeks 1 --config config.json
    python backtest.py --weeks 1 --compare config_old.json config_new.json
    python backtest.py --weeks 1 --ablation

Refactor v4: la función classify() ahora lee todos los magic numbers de scoring
desde config.json (secciones scoring_breakout, scoring_prebreak, scoring_riding,
scoring_hold, scoring_fading). Permite probar variantes sin tocar Python — solo
cambiar config_X.json y comparar contra config base.

Backwards compat: si una clave de scoring no está en config.json, se usa el
valor hardcodeado de v3 como default (gracias al método g() del Config).
"""

import argparse
import json
import os
import pickle
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone, timedelta
from pathlib import Path
from joblib import Parallel, delayed

import numpy as np
import pandas as pd
import requests
import ta


# ════════════════════════════════════════════════════════════════════════════
# CONFIGURACIÓN GENERAL
# ════════════════════════════════════════════════════════════════════════════

# Scan cadence simulado. Production usa cron */5min, pero GH Actions delays + cooldowns
# 15-120min hacen que la cadencia efectiva ronde 7-15min. 15 es buen balance entre
# fidelidad a producción y runtime del backtest. Override por CLI con --scan-interval-min.
SCAN_INTERVAL_MIN = 60  # swing: escanear cada hora
OUTCOME_OFFSETS_MIN = [240, 1440, 4320, 10080]
OUTCOME_NAMES = ["price_4h", "price_1d", "price_3d", "price_7d"]
MAX_DOWNLOAD_WORKERS = 20
MAX_PAIRS = 200

BINANCE_DATA_URL = "https://data-api.binance.vision/api/v3"
BINANCE_FALLBACK_URL = "https://api.binance.com/api/v3"
CACHE_DIR = Path(r"I:\.backtest_cache")
_NO_CACHE = False  # override con --no-cache en CLI

# Trace de features por-barra (instrumentación PASO 1, off por default).
_TRACE_SET = None  # None = sin trace. set(...) = sólo esos símbolos. "ALL" = todos.
_TRACE_OUT = None  # Path del JSONL de salida.

SUPABASE_URL = os.environ.get("SUPABASE_URL", "https://ecgdswroygkfckkaguxp.supabase.co")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY", "")


# ════════════════════════════════════════════════════════════════════════════
# CONFIG: lectura del config.json
# ════════════════════════════════════════════════════════════════════════════

# Overrides de CLI aplicados a TODA Config que se instancie ({sección: {clave: valor}}).
# Necesario porque main() crea varias Configs (base, variantes, compare) y los flags
# --portfolio/--regime/--no-costs tienen que valer para todas.
_CLI_OVERRIDES = {}


class Config:
    """Wrapper sobre config.json. g() admite defaults para back-compat."""

    def __init__(self, path):
        with open(path, "r", encoding="utf-8") as f:
            self.raw = json.load(f)
        self.path = path
        for section, kv in _CLI_OVERRIDES.items():
            self.raw.setdefault(section, {}).update(kv)

    def g(self, *keys, default=None):
        """Acceso anidado: cfg.g('breakout', 'BREAKOUT_MIN_VOL_RATIO').
        Si default está definido lo usa cuando falta la clave (back-compat)."""
        cur = self.raw
        for k in keys:
            if not isinstance(cur, dict) or k not in cur:
                if default is not None:
                    return default
                raise KeyError(f"config[{'/'.join(keys)}] no encontrado en {self.path}")
            cur = cur[k]
        return cur


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


def round_trip_cost(cfg) -> float:
    """Costo ida+vuelta como fracción (fee + slippage por lado, x2). 0.0 si está off.
    Ningún número del backtest incluía costos antes de esto; con ~1200 trades/mes
    un round-trip de 0.3% no es despreciable."""
    c = cfg.raw.get("costs") or {}
    if not c.get("ENABLED", False):
        return 0.0
    return 2.0 * (float(c.get("FEE_PCT_PER_SIDE", 0.0)) +
                  float(c.get("SLIPPAGE_PCT_PER_SIDE", 0.0)))


def net_of_costs(gross_ret, cfg):
    """Aplica el costo round-trip a un retorno bruto (fracción, no %). None-safe."""
    if gross_ret is None:
        return None
    return gross_ret - round_trip_cost(cfg)


class Benchmark:
    """Serie de referencia (BTC por default) para medir retorno RELATIVO.

    Sin esto todo se mide en absoluto y no se distingue "la moneda subió" de
    "el mercado subió". price_at() usa búsqueda binaria sobre close_time del 4h,
    que es el mismo TF con el que calculate_outcomes arma los outcomes forward.
    Fail-safe: si no hay klines del símbolo, queda inerte y ret() devuelve None.
    """

    def __init__(self, klines, cfg):
        b = cfg.raw.get("benchmark") or {}
        self.enabled = bool(b.get("ENABLED", False))
        self.symbol = b.get("SYMBOL", "BTCUSDT")
        self.trailing_days = int(b.get("TRAILING_DAYS", 7))
        self._ct = None
        self._close = None
        df = (klines.get(self.symbol) or {}).get("4h") if klines else None
        if self.enabled and df is not None and len(df) > 0:
            self._ct = df["close_time"].values.astype(np.int64)
            self._close = df["close"].values.astype(float)

    @property
    def active(self):
        return self._ct is not None

    def price_at(self, ts_ms):
        """Último close del benchmark en o antes de ts_ms. None si cae fuera de rango."""
        if not self.active:
            return None
        i = int(np.searchsorted(self._ct, np.int64(ts_ms), side="right")) - 1
        if i < 0:
            return None
        return float(self._close[i])

    def ret(self, ts_from_ms, ts_to_ms):
        """Retorno del benchmark entre dos instantes (fracción). None si falta data."""
        a = self.price_at(ts_from_ms)
        b = self.price_at(ts_to_ms)
        if a is None or b is None or a == 0:
            return None
        return b / a - 1


class MarketRegime:
    """Estado risk-on/risk-off del mercado, global (no per-símbolo).

    Regla: close 1d del símbolo de referencia por encima de su SMA(MA_BARS_1D) → risk-on.
    Es el gate que le falta al sistema: barriendo ARM×TRAIL×STOP, en un mes plano ninguna
    config de salida le gana a no hacer nada y en una ventana bajista le ganan todas —
    o sea que el régimen decide el signo, no el parámetro.

    OJO: distinto de trend_filter_1w, que es per-símbolo y está inerte.
    Fail-safe: sin datos suficientes, up() devuelve True (no bloquea nada).
    """

    def __init__(self, klines, cfg):
        r = cfg.raw.get("regime_filter") or {}
        self.enabled = bool(r.get("ENABLED", False))
        self.symbol = r.get("SYMBOL", "BTCUSDT")
        self.mode = r.get("MODE", "soft")
        self.penalty = float(r.get("PENALTY", 0))
        self.signals = set(r.get("SIGNALS") or [])
        self.ma_bars = int(r.get("MA_BARS_1D", 20))
        self._ct = None
        self._up = None
        if not self.enabled:
            return
        df = (klines.get(self.symbol) or {}).get("1d") if klines else None
        if df is None or len(df) < self.ma_bars + 1:
            return
        close = df["close"].astype(float)
        sma = close.rolling(self.ma_bars).mean()
        self._ct = df["close_time"].values.astype(np.int64)
        self._up = (close > sma).values           # NaN de warm-up → False

    @property
    def active(self):
        return self.enabled and self._ct is not None

    def up(self, ts_ms):
        """True = risk-on (o fail-safe si no hay data). Usa la última barra 1d CERRADA."""
        if not self.active:
            return True
        i = int(np.searchsorted(self._ct, np.int64(ts_ms), side="right")) - 1
        if i < self.ma_bars:          # warm-up de la SMA: no bloquear
            return True
        return bool(self._up[i])

    def apply(self, signal_type, score, ts_ms):
        """Devuelve (score_ajustado, bloqueado). En risk-on no toca nada."""
        if not self.active or signal_type not in self.signals or self.up(ts_ms):
            return score, False
        if self.mode == "hard":
            return score, True
        return score - self.penalty, False


def _normalize_score(raw_score: float, signal_type: str, cal_cfg: dict, score_cap: float) -> float:
    """Percentil empírico (0.0–1.0) del score dentro del CDF hardcoded por signal type.
    Permite ranking inter-signal justo: un score raro en PREBREAK supera a uno común en HOLD.
    Si calibración off o signal sin CDF → fallback proporcional sobre score_cap."""
    if not cal_cfg or not cal_cfg.get("ENABLED", False):
        return float(raw_score) / score_cap
    cdf = cal_cfg.get("cdf", {}).get(signal_type)
    if not cdf:
        return cal_cfg.get("FALLBACK_PERCENTILE", 0.5)
    points = sorted((float(k), float(v)) for k, v in cdf.items())
    if raw_score <= points[0][0]:
        return points[0][1]
    if raw_score >= points[-1][0]:
        return points[-1][1]
    for (s1, p1), (s2, p2) in zip(points, points[1:]):
        if s1 <= raw_score <= s2:
            return p1 + (p2 - p1) * (raw_score - s1) / (s2 - s1) if s2 > s1 else p1
    return cal_cfg.get("FALLBACK_PERCENTILE", 0.5)


# ════════════════════════════════════════════════════════════════════════════
# FETCH DE DATOS
# ════════════════════════════════════════════════════════════════════════════

def _binance_get(path, params, retries=3):
    for url_base in (BINANCE_DATA_URL, BINANCE_FALLBACK_URL):
        for attempt in range(retries):
            try:
                r = requests.get(f"{url_base}{path}", params=params, timeout=15)
                if r.status_code == 200:
                    return r.json()
                if r.status_code == 429:
                    time.sleep(2 ** attempt)
                    continue
            except requests.exceptions.RequestException:
                time.sleep(1)
    raise RuntimeError(f"Binance request failed after retries: {path} {params}")


def get_active_usdt_symbols():
    data = _binance_get("/exchangeInfo", {})
    return {
        s["symbol"]
        for s in data["symbols"]
        if s["symbol"].endswith("USDT") and s["status"] == "TRADING"
    }


def get_top_pairs_today(min_quote_vol, top_n=MAX_PAIRS):
    active = get_active_usdt_symbols()
    data = _binance_get("/ticker/24hr", {})
    pairs = [
        x for x in data
        if x["symbol"] in active
        and x["symbol"].encode("ascii", errors="ignore").decode() == x["symbol"]
        and float(x["quoteVolume"]) > min_quote_vol
    ]
    pairs.sort(key=lambda x: float(x["quoteVolume"]), reverse=True)
    return [x["symbol"] for x in pairs[:top_n]]


def get_pairs_from_snapshot(start_dt, end_dt):
    if not SUPABASE_KEY:
        print("  [snapshot] SUPABASE_KEY no seteada, salteando snapshots.")
        return {}, set()

    headers = {"apikey": SUPABASE_KEY, "Authorization": f"Bearer {SUPABASE_KEY}"}
    all_symbols = set()
    runs_to_pairs = {}
    PAGE = 1000
    WINDOW_HOURS = 24
    cursor_dt = start_dt
    while cursor_dt < end_dt:
        win_end = min(cursor_dt + timedelta(hours=WINDOW_HOURS), end_dt)
        offset = 0
        while True:
            params = {
                "select": "run_at,symbol",
                "run_at": f"gte.{cursor_dt.isoformat()}",
                "and": f"(run_at.lt.{win_end.isoformat()})",
                "limit": PAGE,
                "offset": offset,
                "order": "run_at.asc",
            }
            rows = None
            for attempt in range(3):
                try:
                    r = requests.get(
                        f"{SUPABASE_URL}/rest/v1/screener_pairs_snapshot",
                        headers=headers, params=params, timeout=30,
                    )
                    r.raise_for_status()
                    rows = r.json()
                    break
                except Exception as e:
                    wait = 2 ** attempt
                    print(f"  [snapshot] error {cursor_dt.date()} off={offset} (intento {attempt+1}/3): {e}; reintentando en {wait}s")
                    time.sleep(wait)
            if rows is None:
                print(f"  [snapshot] ventana {cursor_dt.date()} abortada tras reintentos; sigo con la siguiente")
                break
            if not rows:
                break
            for row in rows:
                runs_to_pairs.setdefault(row["run_at"], []).append(row["symbol"])
                all_symbols.add(row["symbol"])
            if len(rows) < PAGE:
                break
            offset += PAGE
        print(f"  [snapshot] ventana {cursor_dt.date()} ok ({len(all_symbols)} símbolos acum)")
        cursor_dt = win_end
    print(f"  [snapshot] {len(runs_to_pairs)} runs distintos, {len(all_symbols)} símbolos únicos")
    return runs_to_pairs, all_symbols


def get_klines_range(symbol, interval, start_ms, end_ms):
    all_rows = []
    cursor = start_ms
    while cursor < end_ms:
        params = {
            "symbol": symbol, "interval": interval,
            "startTime": cursor, "endTime": end_ms, "limit": 1000,
        }
        try:
            data = _binance_get("/klines", params)
        except Exception:
            return None
        if not data:
            break
        all_rows.extend(data)
        last_close = data[-1][6]
        cursor = last_close + 1
        if len(data) < 1000:
            break
    if not all_rows:
        return None
    df = pd.DataFrame(all_rows, columns=[
        "open_time", "open", "high", "low", "close", "volume",
        "close_time", "quote_vol", "trades", "taker_buy_base", "taker_buy_quote", "ignore"
    ])
    for col in ["open", "high", "low", "close", "volume", "taker_buy_base", "taker_buy_quote"]:
        df[col] = df[col].astype(float)
    df["close_time"] = df["close_time"].astype("int64")
    df["open_time"] = df["open_time"].astype("int64")
    return df.sort_values("open_time").reset_index(drop=True)


def download_all_klines(symbols, start_dt, end_dt):
    intervals = ["1h", "4h", "1d", "1w"]
    buffer_days = 90  # 1d necesita ≥80 barras (~90d con este buffer)
    fetch_start = start_dt - timedelta(days=buffer_days)
    fetch_end = end_dt
    start_ms = int(fetch_start.timestamp() * 1000)
    end_ms = int(fetch_end.timestamp() * 1000)
    data = {}
    n_tfs = len(intervals)
    print(f"\n  Descargando {len(symbols)} pares × {n_tfs} timeframes = {len(symbols)*n_tfs} requests")
    print(f"  Rango: {fetch_start.date()} a {fetch_end.date()} ({buffer_days}d buffer + período)")
    completed = 0

    def fetch(sym, tf):
        p = _cache_path("klines", sym, tf, start_ms, end_ms)
        cached = _cache_get(p)
        if cached is not None:
            return sym, tf, cached
        result = get_klines_range(sym, tf, start_ms, end_ms)
        if result is not None:
            _cache_put(p, result)
        return sym, tf, result

    with ThreadPoolExecutor(max_workers=MAX_DOWNLOAD_WORKERS) as ex:
        futures = [ex.submit(fetch, s, tf) for s in symbols for tf in intervals]
        for fut in as_completed(futures):
            sym, tf, df = fut.result()
            if df is None or len(df) < 50:
                continue
            data.setdefault(sym, {})[tf] = df
            completed += 1
            if completed % 50 == 0:
                print(f"    {completed}/{len(symbols)*n_tfs} descargados...")
    valid = {s: tfs for s, tfs in data.items() if all(t in tfs for t in ("1h","4h","1d"))}
    print(f"  {len(valid)}/{len(symbols)} pares con data completa (1h/4h/1d requeridos)")
    return valid




# ════════════════════════════════════════════════════════════════════════════
# CACHE DE DISCO (evita re-descargar klines entre runs)
# ════════════════════════════════════════════════════════════════════════════

def _cache_path(kind, symbol, tf_or_tag, start_ms, end_ms):
    CACHE_DIR.mkdir(exist_ok=True)
    return CACHE_DIR / f"{kind}_{symbol}_{tf_or_tag}_{start_ms}_{end_ms}.pkl"


def _cache_get(p):
    if _NO_CACHE or not p.exists():
        return None
    try:
        with open(p, "rb") as f:
            return pickle.load(f)
    except Exception:
        return None


def _cache_put(p, obj):
    if _NO_CACHE:
        return
    try:
        tmp = p.with_suffix(".tmp")
        with open(tmp, "wb") as f:
            pickle.dump(obj, f)
        tmp.replace(p)
    except Exception:
        pass


# ════════════════════════════════════════════════════════════════════════════
# INDICADORES (replica exacta de la lógica del screener)
# ════════════════════════════════════════════════════════════════════════════

def safe_pct(a, b):
    return (a / b - 1) if b else 0.0


def close_position(c, h, l):
    rng = max(h - l, 1e-12)
    return (c - l) / rng


def analyze_at_time(df_full, end_idx, cfg):
    """Calcula indicadores como si el screener corriera mirando df_full[:end_idx+1]."""
    df = df_full.iloc[:end_idx + 1]
    if len(df) < 80:
        return None

    EMA_SLOW = cfg.g("indicators", "EMA_SLOW")
    RECENT_LOOKBACK = cfg.g("indicators", "RECENT_LOOKBACK")
    RECENT_LOOKBACK_LONG = cfg.g("indicators", "RECENT_LOOKBACK_LONG", default=25)
    RECENT_LONG_PROXIMITY = cfg.g("indicators", "RECENT_LONG_PROXIMITY", default=0.01)
    OBV_SLOPE_LOOKBACK = cfg.g("indicators", "OBV_SLOPE_LOOKBACK", default=10)
    OBV_RISING_MIN = cfg.g("indicators", "OBV_RISING_MIN", default=0.05)
    CVD_LOOKBACK = cfg.g("indicators", "CVD_LOOKBACK", default=10)
    CVD_BULLISH_MIN = cfg.g("indicators", "CVD_BULLISH_MIN", default=0.05)
    BREAKOUT_BUFFER = cfg.g("breakout", "BREAKOUT_BUFFER")
    BREAKOUT_MIN_VOL_RATIO    = cfg.g("breakout", "BREAKOUT_MIN_VOL_RATIO")
    BREAKOUT_MAX_EXTENDED     = cfg.g("breakout", "BREAKOUT_MAX_EXTENDED")
    BREAKOUT_BB_EXPANSION_MIN = cfg.g("breakout", "BREAKOUT_BB_EXPANSION_MIN")
    BREAKOUT_MIN_BODY_PCT     = cfg.g("breakout", "BREAKOUT_MIN_BODY_PCT")
    DEFER_ENABLED   = cfg.g("breakout", "DEFER_ENABLED", default=False)
    DEFER_BARS      = cfg.g("breakout", "DEFER_BARS", default=12)
    PREBREAK_NEAR_MAX = cfg.g("prebreak", "PREBREAK_NEAR_MAX")
    ONE_H_RESIST_LOOKBACK = cfg.g("hold", "ONE_H_RESIST_LOOKBACK")
    ONE_H_RESIST_BUFFER = cfg.g("hold", "ONE_H_RESIST_BUFFER")
    MAJOR_STRUCT_LOOKBACK = cfg.g("hold", "MAJOR_STRUCT_LOOKBACK")
    MAJOR_STRUCT_MAX_DIST = cfg.g("hold", "MAJOR_STRUCT_MAX_DIST")
    HOLD_LOOKBACK_BARS = cfg.g("hold", "HOLD_LOOKBACK_BARS")
    HOLD_RECENT_BREAK_MAX_BARS = cfg.g("hold", "HOLD_RECENT_BREAK_MAX_BARS")
    HOLD_ZONE_BUFFER = cfg.g("hold", "HOLD_ZONE_BUFFER")
    HOLD_PULLBACK_MAX = cfg.g("hold", "HOLD_PULLBACK_MAX")
    STRONG_CLOSE_MIN = cfg.g("hold", "STRONG_CLOSE_MIN")
    RIDING_LOOKBACK_BARS = cfg.g("riding", "RIDING_LOOKBACK_BARS")
    RIDING_ZONE_BUFFER = cfg.g("riding", "RIDING_ZONE_BUFFER")
    RIDING_MIN_VOL_RATIO = cfg.g("riding", "RIDING_MIN_VOL_RATIO")
    FADING_BELOW_ZONE = cfg.g("fading", "FADING_BELOW_ZONE")

    close = df["close"]
    high = df["high"]
    low = df["low"]
    volume = df["volume"]
    price = close.iloc[-1]
    # Cambio % de la vela actual vs la anterior.
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
    # Serie completa: la necesita el breakout diferido para evaluar barras pasadas.
    bb_width_series = ((hband - lband) / mavg.replace(0, np.nan)).fillna(0.0)

    atr_series = ta.volatility.AverageTrueRange(high, low, close, window=14).average_true_range()
    atr = atr_series.iloc[-1]
    atr_pct = (atr / price * 100) if price > 0 else 0.0
    atr_pct_series = (atr_series / close * 100).fillna(0.0)
    atr_threshold = _compute_atr_threshold(
        atr_pct_series,
        cfg.g("indicators", "ATR_MODE", default="fixed"),
        cfg.g("indicators", "ATR_MIN_PCT_FLOOR", default=1.0),
        cfg.g("indicators", "ATR_MIN_PCT"),
        cfg.g("indicators", "ATR_PERCENTILE_LOOKBACK", default=168),
        cfg.g("indicators", "ATR_PERCENTILE_RANK", default=30),
    )

    vol_mean = volume.iloc[-21:-1].mean()
    vol_ratio = (volume.iloc[-1] / vol_mean) if vol_mean else 0.0
    # vol_ratio una barra atrás (para persistence check en PREBREAK).
    if len(volume) >= 22:
        vol_mean_prev = volume.iloc[-22:-2].mean()
        vol_ratio_prev = (volume.iloc[-2] / vol_mean_prev) if vol_mean_prev else 0.0
    else:
        vol_ratio_prev = 0.0
    vol_recent = volume.iloc[-3:].mean()
    vol_prev = volume.iloc[-6:-3].mean()
    vol_growth = (vol_recent / vol_prev) if vol_prev else 0.0

    close_pos = close_position(price, high.iloc[-1], low.iloc[-1])
    strong_close = close_pos >= STRONG_CLOSE_MIN

    candle_range = max(high.iloc[-1] - low.iloc[-1], 1e-12)
    candle_body_pct = abs(close.iloc[-1] - df["open"].iloc[-1]) / candle_range

    try:
        obv = ta.volume.OnBalanceVolumeIndicator(close, volume).on_balance_volume()
        obv_now = obv.iloc[-1]
        obv_ref = obv.iloc[-OBV_SLOPE_LOOKBACK]
        obv_slope = (obv_now - obv_ref) / abs(obv_now) if abs(obv_now) > 1e-12 else 0.0
        obv_rising = obv_slope >= OBV_RISING_MIN
        # obv_slope una barra atrás (para persistence check en HOLD).
        if len(obv) >= OBV_SLOPE_LOOKBACK + 1:
            obv_prev = obv.iloc[-2]
            obv_ref_prev = obv.iloc[-(OBV_SLOPE_LOOKBACK + 1)]
            obv_slope_prev = (obv_prev - obv_ref_prev) / abs(obv_prev) if abs(obv_prev) > 1e-12 else 0.0
        else:
            obv_slope_prev = 0.0
    except Exception:
        obv_slope = 0.0
        obv_rising = False
        obv_slope_prev = 0.0

    try:
        if "taker_buy_base" in df.columns:
            taker_buy = df["taker_buy_base"].astype(float)
            delta = 2 * taker_buy - volume
            cvd = delta.cumsum()
            cvd_now = cvd.iloc[-1]
            cvd_ref = cvd.iloc[-CVD_LOOKBACK]
            vol_window = volume.iloc[-CVD_LOOKBACK:].sum()
            cvd_ratio = (cvd_now - cvd_ref) / vol_window if vol_window > 0 else 0.0
            cvd_bullish = cvd_ratio >= CVD_BULLISH_MIN
        else:
            cvd_ratio = 0.0
            cvd_bullish = False
    except Exception:
        cvd_ratio = 0.0
        cvd_bullish = False

    recent_max = high.iloc[-(RECENT_LOOKBACK + 2):-2].max()
    near_recent_max = recent_max > 0 and 0 <= (recent_max - price) / recent_max <= PREBREAK_NEAR_MAX
    breakout = recent_max > 0 and price > recent_max * (1 + BREAKOUT_BUFFER)
    breakout_distance = safe_pct(price, recent_max)

    if len(high) >= RECENT_LOOKBACK_LONG + 2:
        recent_max_long = high.iloc[-(RECENT_LOOKBACK_LONG + 2):-2].max()
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

    if len(high) >= MAJOR_STRUCT_LOOKBACK + 2:
        _mm_slice_a = high.iloc[-(MAJOR_STRUCT_LOOKBACK + 2):-2]
        major_max = float(_mm_slice_a.max())
        major_dist = (major_max - price) / price if price > 0 else 0.0
        major_struct_ok = major_dist <= MAJOR_STRUCT_MAX_DIST
        bars_since_major_max = MAJOR_STRUCT_LOOKBACK + 1 - int(np.argmax(_mm_slice_a.values))
    else:
        major_dist = None
        major_struct_ok = True
        bars_since_major_max = None

    hold_recent_break = False
    hold_kept_zone = False
    hold_pullback_ok = False
    hold_strong = False
    bars_since_break = None
    start = max(25, len(df) - HOLD_LOOKBACK_BARS - 2)
    for idx in range(start, len(df) - 1):
        ref_slice = high.iloc[max(0, idx - RECENT_LOOKBACK):idx]
        if len(ref_slice) < RECENT_LOOKBACK:
            continue
        ref = ref_slice.max()
        if close.iloc[idx] > ref * (1 + BREAKOUT_BUFFER):
            bars_since_break = len(df) - 1 - idx
            if 1 <= bars_since_break <= HOLD_RECENT_BREAK_MAX_BARS:
                post = df.iloc[idx + 1:]
                hold_recent_break = True
                hold_kept_zone = post["low"].min() >= ref * (1 - HOLD_ZONE_BUFFER)
                pullback = (close.iloc[idx] - post["close"].min()) / close.iloc[idx]
                hold_pullback_ok = pullback <= HOLD_PULLBACK_MAX
                last = post.iloc[-1]
                hold_strong = close_position(last["close"], last["high"], last["low"]) >= STRONG_CLOSE_MIN

    # ── BREAKOUT DIFERIDO ─────────────────────────────────────────────────
    # Espejo de analyze_at_index(): ver el comentario largo allá. Conserva la detección
    # del breakout pero mueve la entrada 24-48h, que es donde está el defecto medido.
    defer_breakout = False
    defer_bars_since = None
    defer_distance = None
    if DEFER_ENABLED:
        idx = len(df) - 1 - DEFER_BARS
        _ref_slice = high.iloc[max(0, idx - RECENT_LOOKBACK):idx] if idx >= 26 else []
        if len(_ref_slice) >= RECENT_LOOKBACK:
            _lvl = float(_ref_slice.max())
            _vmean = df["volume"].iloc[max(0, idx - 20):idx].mean()
            _vr = float(df["volume"].iloc[idx] / _vmean) if _vmean else 0.0
            _rng = max(float(high.iloc[idx] - low.iloc[idx]), 1e-12)
            _bp = abs(float(close.iloc[idx] - df["open"].iloc[idx])) / _rng
            _cp = close_position(float(close.iloc[idx]), float(high.iloc[idx]), float(low.iloc[idx]))
            _wpv = float(bb_width_series.iloc[idx - 1]) if idx >= 1 else 0.0
            _wexp = safe_pct(float(bb_width_series.iloc[idx]), _wpv) if _wpv else 0.0
            if (_lvl > 0
                    and close.iloc[idx] > _lvl * (1 + BREAKOUT_BUFFER)
                    and _vr >= BREAKOUT_MIN_VOL_RATIO
                    and safe_pct(float(close.iloc[idx]), _lvl) <= BREAKOUT_MAX_EXTENDED
                    and _wexp >= BREAKOUT_BB_EXPANSION_MIN
                    and _cp >= STRONG_CLOSE_MIN
                    and _bp >= BREAKOUT_MIN_BODY_PCT
                    and price > _lvl):
                defer_breakout = True
                defer_bars_since = DEFER_BARS
                defer_distance = safe_pct(price, _lvl)

    riding_break_idx = None
    riding_break_close = None
    riding_break_ref = None
    rstart = max(25, len(df) - RIDING_LOOKBACK_BARS - 2)
    for idx in range(rstart, len(df) - 1):
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
    if riding_break_idx is not None and riding_break_ref:
        riding_bars_since = len(df) - 1 - riding_break_idx
        riding_gain = safe_pct(price, riding_break_close)
        post_slice = df.iloc[riding_break_idx + 1:]
        post_break_high = post_slice["high"].max() if len(post_slice) > 0 else price
        riding_above_zone = price >= riding_break_ref * (1 - RIDING_ZONE_BUFFER)
        vm = volume.iloc[-21:-1].mean()
        riding_vol_ok = vm > 0 and (volume.iloc[-3:].mean() / vm) >= RIDING_MIN_VOL_RATIO
        fading_reversal = safe_pct(price, post_break_high) if post_break_high else 0.0
        fading_below_zone = price < riding_break_ref * (1 - FADING_BELOW_ZONE)

    return {
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
        "major_struct_ok": major_struct_ok,
        "major_struct_dist": major_dist,
        "bars_since_major_max": bars_since_major_max,
        "defer_breakout": defer_breakout,
        "defer_bars_since": defer_bars_since,
        "defer_distance": defer_distance,
        "hold_recent_break": hold_recent_break,
        "hold_kept_zone": hold_kept_zone,
        "hold_pullback_ok": hold_pullback_ok,
        "hold_strong": hold_strong,
        "bars_since_break": bars_since_break,
        "riding_bars_since": riding_bars_since,
        "riding_gain": riding_gain,
        "riding_above_zone": riding_above_zone,
        "riding_vol_ok": riding_vol_ok,
        "riding_break_close": riding_break_close,
        "riding_break_ref": riding_break_ref,
        "post_break_high": post_break_high,
        "fading_reversal": fading_reversal,
        "fading_below_zone": fading_below_zone,
        "candle_status": "closed",  # siempre cerrada en backtest
    }


# ════════════════════════════════════════════════════════════════════════════
# PRECOMPUTE — calcula columnas de indicadores una sola vez por (símbolo, TF)
# ════════════════════════════════════════════════════════════════════════════

def _indicator_key(cfg):
    """Hash para identificar si dos cfgs comparten los mismos parámetros de indicadores.
    Si la clave coincide, se pueden compartir los DataFrames precomputados."""
    return json.dumps({
        "EMA_SLOW": cfg.g("indicators", "EMA_SLOW"),
        "RECENT_LOOKBACK": cfg.g("indicators", "RECENT_LOOKBACK"),
        "RECENT_LOOKBACK_LONG": cfg.g("indicators", "RECENT_LOOKBACK_LONG", default=25),
        "CVD_LOOKBACK": cfg.g("indicators", "CVD_LOOKBACK", default=10),
        "OBV_SLOPE_LOOKBACK": cfg.g("indicators", "OBV_SLOPE_LOOKBACK", default=10),
        "ONE_H_RESIST_LOOKBACK": cfg.g("hold", "ONE_H_RESIST_LOOKBACK"),
        "MAJOR_STRUCT_LOOKBACK": cfg.g("hold", "MAJOR_STRUCT_LOOKBACK"),
        "HOLD_LOOKBACK_BARS": cfg.g("hold", "HOLD_LOOKBACK_BARS"),
        "RIDING_LOOKBACK_BARS": cfg.g("riding", "RIDING_LOOKBACK_BARS"),
        "ATR_MODE":                cfg.g("indicators", "ATR_MODE", default="fixed"),
        "ATR_PERCENTILE_RANK":     cfg.g("indicators", "ATR_PERCENTILE_RANK", default=30),
        "ATR_PERCENTILE_LOOKBACK": cfg.g("indicators", "ATR_PERCENTILE_LOOKBACK", default=168),
        "ATR_MIN_PCT_FLOOR":       cfg.g("indicators", "ATR_MIN_PCT_FLOOR", default=1.0),
        "ATR_MIN_PCT":             cfg.g("indicators", "ATR_MIN_PCT"),
    }, sort_keys=True)


def _analyze_key(cfg):
    """Hash para identificar si dos cfgs producen el mismo output de analyze_at_index().
    Cfgs con la misma clave comparten el primer pase del scan (candidate building)."""
    return json.dumps({
        "RECENT_LOOKBACK":         cfg.g("indicators", "RECENT_LOOKBACK"),
        "RECENT_LOOKBACK_LONG":    cfg.g("indicators", "RECENT_LOOKBACK_LONG", default=25),
        "RECENT_LONG_PROXIMITY":   cfg.g("indicators", "RECENT_LONG_PROXIMITY", default=0.01),
        "OBV_SLOPE_LOOKBACK":      cfg.g("indicators", "OBV_SLOPE_LOOKBACK", default=10),
        "CVD_LOOKBACK":            cfg.g("indicators", "CVD_LOOKBACK", default=10),
        "BREAKOUT_BUFFER":         cfg.g("breakout", "BREAKOUT_BUFFER"),
        "BREAKOUT_MIN_VOL_RATIO":     cfg.g("breakout", "BREAKOUT_MIN_VOL_RATIO"),
        "BREAKOUT_MAX_EXTENDED":      cfg.g("breakout", "BREAKOUT_MAX_EXTENDED"),
        "BREAKOUT_BB_EXPANSION_MIN":  cfg.g("breakout", "BREAKOUT_BB_EXPANSION_MIN"),
        "BREAKOUT_MIN_BODY_PCT":      cfg.g("breakout", "BREAKOUT_MIN_BODY_PCT"),
        "DEFER_ENABLED":           cfg.g("breakout", "DEFER_ENABLED", default=False),
        "DEFER_BARS":              cfg.g("breakout", "DEFER_BARS", default=12),
        "PREBREAK_NEAR_MAX":       cfg.g("prebreak", "PREBREAK_NEAR_MAX"),
        "ONE_H_RESIST_BUFFER":     cfg.g("hold", "ONE_H_RESIST_BUFFER"),
        "MAJOR_STRUCT_LOOKBACK":   cfg.g("hold", "MAJOR_STRUCT_LOOKBACK"),
        "MAJOR_STRUCT_MAX_DIST":           cfg.g("hold", "MAJOR_STRUCT_MAX_DIST"),
        "MAJOR_STRUCT_BYPASS_ON_BREAKOUT":   cfg.g("hold", "MAJOR_STRUCT_BYPASS_ON_BREAKOUT", default=False),
        "MAJOR_STRUCT_BYPASS_ON_STRONG_PRE": cfg.g("hold", "MAJOR_STRUCT_BYPASS_ON_STRONG_PRE", default=False),
        "HOLD_LOOKBACK_BARS":              cfg.g("hold", "HOLD_LOOKBACK_BARS"),
        "HOLD_RECENT_BREAK_MAX_BARS": cfg.g("hold", "HOLD_RECENT_BREAK_MAX_BARS"),
        "HOLD_ZONE_BUFFER":        cfg.g("hold", "HOLD_ZONE_BUFFER"),
        "HOLD_PULLBACK_MAX":       cfg.g("hold", "HOLD_PULLBACK_MAX"),
        "STRONG_CLOSE_MIN":        cfg.g("hold", "STRONG_CLOSE_MIN"),
        "RIDING_LOOKBACK_BARS":    cfg.g("riding", "RIDING_LOOKBACK_BARS"),
        "RIDING_ZONE_BUFFER":      cfg.g("riding", "RIDING_ZONE_BUFFER"),
        "RIDING_MIN_VOL_RATIO":    cfg.g("riding", "RIDING_MIN_VOL_RATIO"),
        "FADING_BELOW_ZONE":       cfg.g("fading", "FADING_BELOW_ZONE"),
        "ATR_MIN_PCT":             cfg.g("indicators", "ATR_MIN_PCT"),
        "ATR_MODE":                cfg.g("indicators", "ATR_MODE", default="fixed"),
        "ATR_PERCENTILE_RANK":     cfg.g("indicators", "ATR_PERCENTILE_RANK", default=30),
        "ATR_PERCENTILE_LOOKBACK": cfg.g("indicators", "ATR_PERCENTILE_LOOKBACK", default=168),
        "ATR_MIN_PCT_FLOOR":       cfg.g("indicators", "ATR_MIN_PCT_FLOOR", default=1.0),
    }, sort_keys=True)


def precompute_indicators(df, cfg):
    """Precalcula columnas de indicadores sobre el df completo (una sola vez por símbolo/TF).
    analyze_at_index() lee desde estas columnas en O(1) por barra."""
    RECENT_LOOKBACK = cfg.g("indicators", "RECENT_LOOKBACK")
    RECENT_LOOKBACK_LONG = cfg.g("indicators", "RECENT_LOOKBACK_LONG", default=25)
    CVD_LOOKBACK = cfg.g("indicators", "CVD_LOOKBACK", default=10)
    EMA_SLOW = cfg.g("indicators", "EMA_SLOW")
    ONE_H_RESIST_LOOKBACK = cfg.g("hold", "ONE_H_RESIST_LOOKBACK")
    MAJOR_STRUCT_LOOKBACK = cfg.g("hold", "MAJOR_STRUCT_LOOKBACK")

    df = df.copy()
    close = df["close"]
    high = df["high"]
    low = df["low"]
    volume = df["volume"]

    df["_ema_slow"] = ta.trend.EMAIndicator(close, window=EMA_SLOW).ema_indicator()

    bb = ta.volatility.BollingerBands(close, window=20, window_dev=2)
    df["_bb_hband"] = bb.bollinger_hband()
    df["_bb_lband"] = bb.bollinger_lband()
    df["_bb_mavg"] = bb.bollinger_mavg()
    mavg_s = df["_bb_mavg"].replace(0, np.nan)
    df["_bb_width"] = (df["_bb_hband"] - df["_bb_lband"]) / mavg_s
    df["_bb_width"] = df["_bb_width"].fillna(0.0)

    df["_atr"] = ta.volatility.AverageTrueRange(high, low, close, window=14).average_true_range()
    df["_atr_pct"] = (df["_atr"] / df["close"] * 100).fillna(0.0)
    atr_mode = cfg.g("indicators", "ATR_MODE", default="fixed")
    if atr_mode == "percentile":
        _atr_lookback = cfg.g("indicators", "ATR_PERCENTILE_LOOKBACK", default=168)
        _atr_rank    = cfg.g("indicators", "ATR_PERCENTILE_RANK", default=30) / 100.0
        _atr_floor   = cfg.g("indicators", "ATR_MIN_PCT_FLOOR", default=1.0)
        df["_atr_threshold"] = (
            df["_atr_pct"].rolling(_atr_lookback, min_periods=24).quantile(_atr_rank)
            .clip(lower=_atr_floor).fillna(_atr_floor)
        )
    else:
        df["_atr_threshold"] = cfg.g("indicators", "ATR_MIN_PCT")

    df["_vol_mean20"] = volume.rolling(20).mean().shift(1)
    df["_vol_ratio"] = volume / df["_vol_mean20"].replace(0, np.nan)
    df["_vol_ratio"] = df["_vol_ratio"].fillna(0.0)
    df["_vol_recent3"] = volume.rolling(3).mean()
    df["_vol_prev3"] = volume.rolling(3).mean().shift(3)

    df["_obv"] = ta.volume.OnBalanceVolumeIndicator(close, volume).on_balance_volume()

    if "taker_buy_base" in df.columns:
        taker_buy = df["taker_buy_base"].astype(float)
        df["_cvd"] = (2 * taker_buy - volume).cumsum()
    else:
        df["_cvd"] = 0.0
    df["_cvd_vol_sum"] = volume.rolling(CVD_LOOKBACK).sum()

    candle_range = (high - low).clip(lower=1e-12)
    df["_candle_body_pct"] = (close - df["open"]).abs() / candle_range
    df["_close_pos"] = (close - low) / candle_range

    # shift(2): excluye la barra actual y la anterior, igual que analyze_at_time
    df["_recent_max_short"] = high.rolling(RECENT_LOOKBACK).max().shift(2)
    df["_recent_max_long"] = high.rolling(RECENT_LOOKBACK_LONG).max().shift(2)
    df["_one_h_resist"] = high.rolling(ONE_H_RESIST_LOOKBACK).max().shift(2)
    df["_major_max"] = high.rolling(MAJOR_STRUCT_LOOKBACK).max().shift(2)
    # shift(1): para los loops internos de HOLD/RIDING (ref = max antes de la barra idx)
    df["_recent_max_shift1"] = high.rolling(RECENT_LOOKBACK).max().shift(1)

    # Pre-extrae numpy arrays para analyze_at_index — evita ~3.3M lookups df[col] en el hot loop
    _np_cols = [
        "open", "high", "low", "close", "volume",
        "_ema_slow", "_bb_width", "_atr", "_atr_threshold",
        "_vol_mean20", "_vol_ratio", "_vol_recent3", "_vol_prev3",
        "_obv", "_cvd", "_cvd_vol_sum",
        "_candle_body_pct", "_close_pos",
        "_recent_max_short", "_recent_max_long",
        "_one_h_resist", "_major_max", "_recent_max_shift1",
    ]
    df.attrs["np"] = {c: df[c].to_numpy() for c in _np_cols if c in df.columns}

    return df


def _build_analyze_params(cfg):
    """Extrae todos los parámetros de cfg usados por analyze_at_index a un dict plano,
    evitando repetir cfg.g() en cada llamada del hot loop (806k+ veces en un compare 2w)."""
    return {
        "RECENT_LOOKBACK":           cfg.g("indicators", "RECENT_LOOKBACK"),
        "RECENT_LOOKBACK_LONG":      cfg.g("indicators", "RECENT_LOOKBACK_LONG", default=25),
        "RECENT_LONG_PROXIMITY":     cfg.g("indicators", "RECENT_LONG_PROXIMITY", default=0.01),
        "OBV_SLOPE_LOOKBACK":        cfg.g("indicators", "OBV_SLOPE_LOOKBACK", default=10),
        "OBV_RISING_MIN":            cfg.g("indicators", "OBV_RISING_MIN", default=0.05),
        "CVD_LOOKBACK":              cfg.g("indicators", "CVD_LOOKBACK", default=10),
        "CVD_BULLISH_MIN":           cfg.g("indicators", "CVD_BULLISH_MIN", default=0.05),
        "BREAKOUT_BUFFER":           cfg.g("breakout", "BREAKOUT_BUFFER"),
        "BREAKOUT_MIN_VOL_RATIO":       cfg.g("breakout", "BREAKOUT_MIN_VOL_RATIO"),
        "BREAKOUT_MAX_EXTENDED":        cfg.g("breakout", "BREAKOUT_MAX_EXTENDED"),
        "BREAKOUT_BB_EXPANSION_MIN":    cfg.g("breakout", "BREAKOUT_BB_EXPANSION_MIN"),
        "BREAKOUT_MIN_BODY_PCT":        cfg.g("breakout", "BREAKOUT_MIN_BODY_PCT"),
        "DEFER_ENABLED":             cfg.g("breakout", "DEFER_ENABLED", default=False),
        "DEFER_BARS":                cfg.g("breakout", "DEFER_BARS", default=12),
        "PREBREAK_NEAR_MAX":         cfg.g("prebreak", "PREBREAK_NEAR_MAX"),
        "ONE_H_RESIST_BUFFER":       cfg.g("hold", "ONE_H_RESIST_BUFFER"),
        "MAJOR_STRUCT_LOOKBACK":     cfg.g("hold", "MAJOR_STRUCT_LOOKBACK"),
        "MAJOR_STRUCT_MAX_DIST":     cfg.g("hold", "MAJOR_STRUCT_MAX_DIST"),
        "HOLD_LOOKBACK_BARS":        cfg.g("hold", "HOLD_LOOKBACK_BARS"),
        "HOLD_RECENT_BREAK_MAX_BARS":cfg.g("hold", "HOLD_RECENT_BREAK_MAX_BARS"),
        "HOLD_ZONE_BUFFER":          cfg.g("hold", "HOLD_ZONE_BUFFER"),
        "HOLD_PULLBACK_MAX":         cfg.g("hold", "HOLD_PULLBACK_MAX"),
        "STRONG_CLOSE_MIN":          cfg.g("hold", "STRONG_CLOSE_MIN"),
        "RIDING_LOOKBACK_BARS":      cfg.g("riding", "RIDING_LOOKBACK_BARS"),
        "RIDING_ZONE_BUFFER":        cfg.g("riding", "RIDING_ZONE_BUFFER"),
        "RIDING_MIN_VOL_RATIO":      cfg.g("riding", "RIDING_MIN_VOL_RATIO"),
        "FADING_BELOW_ZONE":         cfg.g("fading", "FADING_BELOW_ZONE"),
        "ATR_MIN_PCT":               cfg.g("indicators", "ATR_MIN_PCT"),
    }


def analyze_at_index(df, end_idx, params):
    """Lee indicadores precomputados en O(1). df debe venir de precompute_indicators().
    params: dict pre-construido con _build_analyze_params(cfg)."""
    if end_idx < 80:
        return None

    RECENT_LOOKBACK = params["RECENT_LOOKBACK"]
    RECENT_LOOKBACK_LONG = params["RECENT_LOOKBACK_LONG"]
    RECENT_LONG_PROXIMITY = params["RECENT_LONG_PROXIMITY"]
    OBV_SLOPE_LOOKBACK = params["OBV_SLOPE_LOOKBACK"]
    OBV_RISING_MIN = params["OBV_RISING_MIN"]
    CVD_LOOKBACK = params["CVD_LOOKBACK"]
    CVD_BULLISH_MIN = params["CVD_BULLISH_MIN"]
    BREAKOUT_BUFFER = params["BREAKOUT_BUFFER"]
    BREAKOUT_MIN_VOL_RATIO    = params["BREAKOUT_MIN_VOL_RATIO"]
    BREAKOUT_MAX_EXTENDED     = params["BREAKOUT_MAX_EXTENDED"]
    BREAKOUT_BB_EXPANSION_MIN = params["BREAKOUT_BB_EXPANSION_MIN"]
    BREAKOUT_MIN_BODY_PCT     = params["BREAKOUT_MIN_BODY_PCT"]
    DEFER_ENABLED   = params["DEFER_ENABLED"]
    DEFER_BARS      = params["DEFER_BARS"]
    PREBREAK_NEAR_MAX = params["PREBREAK_NEAR_MAX"]
    ONE_H_RESIST_BUFFER = params["ONE_H_RESIST_BUFFER"]
    MAJOR_STRUCT_LOOKBACK = params["MAJOR_STRUCT_LOOKBACK"]
    MAJOR_STRUCT_MAX_DIST = params["MAJOR_STRUCT_MAX_DIST"]
    HOLD_LOOKBACK_BARS = params["HOLD_LOOKBACK_BARS"]
    HOLD_RECENT_BREAK_MAX_BARS = params["HOLD_RECENT_BREAK_MAX_BARS"]
    HOLD_ZONE_BUFFER = params["HOLD_ZONE_BUFFER"]
    HOLD_PULLBACK_MAX = params["HOLD_PULLBACK_MAX"]
    STRONG_CLOSE_MIN = params["STRONG_CLOSE_MIN"]
    RIDING_LOOKBACK_BARS = params["RIDING_LOOKBACK_BARS"]
    RIDING_ZONE_BUFFER = params["RIDING_ZONE_BUFFER"]
    RIDING_MIN_VOL_RATIO = params["RIDING_MIN_VOL_RATIO"]
    FADING_BELOW_ZONE = params["FADING_BELOW_ZONE"]

    arr = df.attrs["np"]

    price = float(arr["close"][end_idx])
    # Cambio % de la vela actual vs la anterior.
    close_change_curr = (price / float(arr["close"][end_idx - 1]) - 1) if end_idx >= 1 and float(arr["close"][end_idx - 1]) > 0 else 0.0

    # EMA
    ema_val = arr["_ema_slow"][end_idx]
    ema_val_prev = arr["_ema_slow"][end_idx - 3] if end_idx >= 3 else np.nan
    ema_trend_up = (bool(price > ema_val and ema_val > ema_val_prev)
                    if pd.notna(ema_val) and pd.notna(ema_val_prev) else False)

    # BB
    width_curr_raw = arr["_bb_width"][end_idx]
    width_curr = float(width_curr_raw) if pd.notna(width_curr_raw) else 0.0
    width_prev_raw = arr["_bb_width"][end_idx - 1] if end_idx >= 1 else np.nan
    width_prev = float(width_prev_raw) if pd.notna(width_prev_raw) and width_prev_raw != 0 else 0.0
    width_expansion = safe_pct(width_curr, width_prev) if width_prev != 0 else 0.0

    # ATR
    atr_raw = arr["_atr"][end_idx]
    atr = float(atr_raw) if pd.notna(atr_raw) else 0.0
    atr_pct = (atr / price * 100) if price > 0 else 0.0
    atr_thresh_raw = arr["_atr_threshold"][end_idx] if "_atr_threshold" in arr else params["ATR_MIN_PCT"]
    atr_threshold = float(atr_thresh_raw) if pd.notna(atr_thresh_raw) else params["ATR_MIN_PCT"]

    # Volume
    vol_mean_raw = arr["_vol_mean20"][end_idx]
    vol_mean = float(vol_mean_raw) if pd.notna(vol_mean_raw) and vol_mean_raw > 0 else 0.0
    vol_curr = float(arr["volume"][end_idx])
    vol_ratio = (vol_curr / vol_mean) if vol_mean > 0 else 0.0
    vr_prev_raw = arr["_vol_ratio"][end_idx - 1] if end_idx >= 1 else 0.0
    vol_ratio_prev = float(vr_prev_raw) if pd.notna(vr_prev_raw) else 0.0
    vol_recent_raw = arr["_vol_recent3"][end_idx]
    vol_prev_raw = arr["_vol_prev3"][end_idx]
    vol_recent = float(vol_recent_raw) if pd.notna(vol_recent_raw) else 0.0
    vol_prev = float(vol_prev_raw) if pd.notna(vol_prev_raw) and vol_prev_raw > 0 else 0.0
    vol_growth = (vol_recent / vol_prev) if vol_prev > 0 else 0.0

    # Candle
    close_pos = float(arr["_close_pos"][end_idx])
    strong_close = close_pos >= STRONG_CLOSE_MIN
    candle_body_pct = float(arr["_candle_body_pct"][end_idx])

    # OBV
    try:
        obv_now = float(arr["_obv"][end_idx])
        obv_ref_idx = max(0, end_idx - OBV_SLOPE_LOOKBACK + 1)
        obv_ref = float(arr["_obv"][obv_ref_idx])
        obv_slope = (obv_now - obv_ref) / abs(obv_now) if abs(obv_now) > 1e-12 else 0.0
        obv_rising = obv_slope >= OBV_RISING_MIN
        obv_prev = float(arr["_obv"][end_idx - 1]) if end_idx >= 1 else 0.0
        obv_ref_prev_idx = max(0, end_idx - 1 - OBV_SLOPE_LOOKBACK + 1)
        obv_ref_prev = float(arr["_obv"][obv_ref_prev_idx])
        obv_slope_prev = (obv_prev - obv_ref_prev) / abs(obv_prev) if abs(obv_prev) > 1e-12 else 0.0
    except Exception:
        obv_slope = 0.0
        obv_rising = False
        obv_slope_prev = 0.0

    # CVD
    try:
        cvd_now = float(arr["_cvd"][end_idx])
        cvd_ref_idx = max(0, end_idx - CVD_LOOKBACK + 1)
        cvd_ref = float(arr["_cvd"][cvd_ref_idx])
        vol_window = float(arr["_cvd_vol_sum"][end_idx])
        cvd_ratio = (cvd_now - cvd_ref) / vol_window if vol_window > 0 else 0.0
        cvd_bullish = cvd_ratio >= CVD_BULLISH_MIN
    except Exception:
        cvd_ratio = 0.0
        cvd_bullish = False

    # Recent max (shift 2 — excluye barra actual y anterior)
    rm_raw = arr["_recent_max_short"][end_idx]
    recent_max = float(rm_raw) if pd.notna(rm_raw) and rm_raw > 0 else 0.0
    near_recent_max = recent_max > 0 and 0 <= (recent_max - price) / recent_max <= PREBREAK_NEAR_MAX
    breakout = recent_max > 0 and price > recent_max * (1 + BREAKOUT_BUFFER)
    breakout_distance = safe_pct(price, recent_max)

    # Recent max long
    rml_raw = arr["_recent_max_long"][end_idx]
    if pd.notna(rml_raw) and end_idx >= RECENT_LOOKBACK_LONG + 1:
        recent_max_long = float(rml_raw)
        if recent_max_long > 0:
            dist_to_long = (recent_max_long - price) / recent_max_long
            recent_long_ok = (price > recent_max_long * (1 + BREAKOUT_BUFFER)
                              or dist_to_long <= RECENT_LONG_PROXIMITY)
        else:
            recent_long_ok = True
    else:
        recent_max_long = recent_max
        recent_long_ok = True

    # Resistance
    oh_raw = arr["_one_h_resist"][end_idx]
    one_h_resist = float(oh_raw) if pd.notna(oh_raw) else price
    dist_to_res = (one_h_resist - price) / price if price > 0 else 0.0
    not_near_resistance = dist_to_res > ONE_H_RESIST_BUFFER or breakout

    # Major structure
    mm_raw = arr["_major_max"][end_idx]
    if pd.notna(mm_raw) and end_idx >= MAJOR_STRUCT_LOOKBACK + 1:
        major_dist = (float(mm_raw) - price) / price if price > 0 else 0.0
        major_struct_ok = major_dist <= MAJOR_STRUCT_MAX_DIST
        _lb_start = end_idx - MAJOR_STRUCT_LOOKBACK - 1
        _mm_slice = arr["high"][_lb_start:end_idx - 1]
        bars_since_major_max = end_idx - (_lb_start + int(np.argmax(_mm_slice)))
    else:
        major_struct_ok = True
        major_dist = None
        bars_since_major_max = None

    # HOLD lookback (vectorizado sobre slice de arrays numpy)
    hold_recent_break = False
    hold_kept_zone = False
    hold_pullback_ok = False
    hold_strong = False
    bars_since_break = None
    hold_start = max(25, end_idx - HOLD_LOOKBACK_BARS - 1)
    _sl_close_h = arr["close"][hold_start:end_idx]
    _sl_rm1_h   = arr["_recent_max_shift1"][hold_start:end_idx]
    _sl_local   = np.arange(len(_sl_close_h))
    _bsb_arr    = end_idx - (hold_start + _sl_local)
    _mask_h = (
        np.isfinite(_sl_rm1_h) & (_sl_rm1_h > 0)
        & (_sl_close_h > _sl_rm1_h * (1 + BREAKOUT_BUFFER))
        & (_bsb_arr >= 1) & (_bsb_arr <= HOLD_RECENT_BREAK_MAX_BARS)
    )
    if _mask_h.any():
        _hit_local = int(np.flatnonzero(_mask_h)[-1])
        _hit_abs   = hold_start + _hit_local
        bars_since_break = int(_bsb_arr[_hit_local])
        _ref_h     = float(_sl_rm1_h[_hit_local])
        _brk_close = float(_sl_close_h[_hit_local])
        _post_low  = arr["low"][_hit_abs + 1:end_idx + 1]
        _post_cls  = arr["close"][_hit_abs + 1:end_idx + 1]
        if len(_post_low) > 0:
            hold_recent_break = True
            hold_kept_zone    = float(_post_low.min()) >= _ref_h * (1 - HOLD_ZONE_BUFFER)
            pullback          = (_brk_close - float(_post_cls.min())) / _brk_close
            hold_pullback_ok  = pullback <= HOLD_PULLBACK_MAX
            hold_strong       = (close_position(float(arr["close"][end_idx]),
                                                float(arr["high"][end_idx]),
                                                float(arr["low"][end_idx])) >= STRONG_CLOSE_MIN)

    # ── BREAKOUT DIFERIDO (vectorizado) ───────────────────────────────────
    # BREAKOUT dispara en la vela de ruptura y ahí compra el techo: contra un momento al
    # azar de la MISMA moneda pierde -4.2pp (926 alertas reales de jul-2026) y -3.6pp
    # (alertas del backtest, jul 18-31), con el 84% de las monedas repitiendo la brecha.
    # Apagarlo empeora (elige bien la moneda, mal el instante), así que se conserva la
    # detección y se mueve la entrada: se busca una barra que HABRÍA disparado BREAKOUT
    # hace DEFER_BARS y se exige que el precio siga sobre el nivel roto.
    # Sin estado: se re-deriva de la serie, igual que HOLD.
    # Se mira UNA sola barra (la de hace DEFER_BARS), no una ventana: si se acepta
    # cualquier barra del rango, la misma ruptura vuelve a disparar scan tras scan y
    # multiplica las alertas por ~3.7, diluyendo el bucket BEST.
    defer_breakout = False
    defer_bars_since = None
    defer_distance = None
    if DEFER_ENABLED:
        _b_d = end_idx - DEFER_BARS
        if _b_d >= 26:
            _lvl_d = arr["_recent_max_shift1"][_b_d]
            if np.isfinite(_lvl_d) and _lvl_d > 0:
                _lvl_d = float(_lvl_d)
                _c_d = float(arr["close"][_b_d])
                _wp_d = float(arr["_bb_width"][_b_d - 1])
                _wexp_d = safe_pct(float(arr["_bb_width"][_b_d]), _wp_d) if _wp_d > 0 else 0.0
                if (_c_d > _lvl_d * (1 + BREAKOUT_BUFFER)
                        and arr["_vol_ratio"][_b_d] >= BREAKOUT_MIN_VOL_RATIO
                        and safe_pct(_c_d, _lvl_d) <= BREAKOUT_MAX_EXTENDED
                        and _wexp_d >= BREAKOUT_BB_EXPANSION_MIN
                        and arr["_close_pos"][_b_d] >= STRONG_CLOSE_MIN
                        and arr["_candle_body_pct"][_b_d] >= BREAKOUT_MIN_BODY_PCT
                        and price > _lvl_d):                # sigue sobre el nivel roto
                    defer_breakout = True
                    defer_bars_since = DEFER_BARS
                    defer_distance = safe_pct(price, _lvl_d)

    # RIDING lookback (vectorizado sobre slice de arrays numpy)
    riding_break_idx = None
    riding_break_close = None
    riding_break_ref = None
    riding_start = max(25, end_idx - RIDING_LOOKBACK_BARS - 1)
    _sl_close_r = arr["close"][riding_start:end_idx]
    _sl_rm1_r   = arr["_recent_max_shift1"][riding_start:end_idx]
    _mask_r = (
        np.isfinite(_sl_rm1_r) & (_sl_rm1_r > 0)
        & (_sl_close_r > _sl_rm1_r * (1 + BREAKOUT_BUFFER))
    )
    if _mask_r.any():
        _hit_local_r   = int(np.flatnonzero(_mask_r)[-1])
        riding_break_idx   = riding_start + _hit_local_r
        riding_break_ref   = float(_sl_rm1_r[_hit_local_r])
        riding_break_close = float(_sl_close_r[_hit_local_r])

    riding_bars_since = None
    riding_gain = None
    riding_above_zone = None
    riding_vol_ok = None
    post_break_high = None
    fading_reversal = None
    fading_below_zone = None
    if riding_break_idx is not None and riding_break_ref:
        riding_bars_since = end_idx - riding_break_idx
        riding_gain = safe_pct(price, riding_break_close)
        post_highs = arr["high"][riding_break_idx + 1:end_idx + 1]
        post_break_high = float(post_highs.max()) if len(post_highs) > 0 else price
        riding_above_zone = price >= riding_break_ref * (1 - RIDING_ZONE_BUFFER)
        vm = float(arr["_vol_mean20"][end_idx]) if pd.notna(arr["_vol_mean20"][end_idx]) else 0.0
        vr3 = float(arr["_vol_recent3"][end_idx]) if pd.notna(arr["_vol_recent3"][end_idx]) else 0.0
        riding_vol_ok = vm > 0 and (vr3 / vm) >= RIDING_MIN_VOL_RATIO
        fading_reversal = safe_pct(price, post_break_high) if post_break_high else 0.0
        fading_below_zone = price < riding_break_ref * (1 - FADING_BELOW_ZONE)

    return {
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
        "major_struct_ok": major_struct_ok,
        "major_struct_dist": major_dist,
        "bars_since_major_max": bars_since_major_max,
        "defer_breakout": defer_breakout,
        "defer_bars_since": defer_bars_since,
        "defer_distance": defer_distance,
        "hold_recent_break": hold_recent_break,
        "hold_kept_zone": hold_kept_zone,
        "hold_pullback_ok": hold_pullback_ok,
        "hold_strong": hold_strong,
        "bars_since_break": bars_since_break,
        "riding_bars_since": riding_bars_since,
        "riding_gain": riding_gain,
        "riding_above_zone": riding_above_zone,
        "riding_vol_ok": riding_vol_ok,
        "riding_break_close": riding_break_close,
        "riding_break_ref": riding_break_ref,
        "post_break_high": post_break_high,
        "fading_reversal": fading_reversal,
        "fading_below_zone": fading_below_zone,
        "candle_status": "closed",
    }


# ════════════════════════════════════════════════════════════════════════════
# CLASIFICACIÓN — refactorizada con lectura de scoring desde config.json
# ════════════════════════════════════════════════════════════════════════════

def final_bucket(score, signal_type, cfg):
    cal_cfg = cfg.raw.get("scoring_calibration") or {}
    mode = cal_cfg.get("bucket_mode", "absolute")
    if mode == "absolute":
        per_best   = cfg.raw.get("scoring", {}).get("BEST_MIN_SCORE_PER_SIGNAL", {})
        per_strong = cfg.raw.get("scoring", {}).get("STRONG_MIN_SCORE_PER_SIGNAL", {})
        best_thr   = per_best.get(signal_type, cfg.g("scoring", "BEST_MIN_SCORE"))
        strong_thr = per_strong.get(signal_type, cfg.g("scoring", "STRONG_MIN_SCORE"))
        if score >= best_thr:   return "BEST"
        if score >= strong_thr: return "STRONG"
        return "WATCH"
    score_cap = cfg.g("scoring", "SCORE_CAP")
    pct = _normalize_score(score, signal_type, cal_cfg, score_cap)
    if mode == "percentile_global":
        if pct >= cal_cfg.get("BEST_PCT", 0.85):   return "BEST"
        if pct >= cal_cfg.get("STRONG_PCT", 0.50): return "STRONG"
        return "WATCH"
    if mode == "percentile_per_signal":
        thr = cal_cfg.get("thresholds", {}).get(signal_type, {"BEST": 0.85, "STRONG": 0.50})
        if pct >= thr["BEST"]:   return "BEST"
        if pct >= thr["STRONG"]: return "STRONG"
        return "WATCH"
    if mode == "hybrid":
        if score >= cal_cfg.get("BEST_MIN_RAW", 10) and pct >= cal_cfg.get("BEST_PCT", 0.80):   return "BEST"
        if score >= cal_cfg.get("STRONG_MIN_RAW", 8) and pct >= cal_cfg.get("STRONG_PCT", 0.45): return "STRONG"
        return "WATCH"
    if score >= cfg.g("scoring", "BEST_MIN_SCORE"):   return "BEST"   # fallback safe
    if score >= cfg.g("scoring", "STRONG_MIN_SCORE"): return "STRONG"
    return "WATCH"


def classify(symbol, tf_data, cfg, counts_history=None, regime_up=None):
    """Replica classify_symbol del screener con scoring totalmente parametrizado.
    tf_data = {'1h': dict, '4h': dict, '1d': dict}
    counts_history: dict {(symbol, history_tf): n_alertas_recientes} — usado para
    aplicar LATE_REPEAT_PENALTY igual que en producción. None = sin late penalty."""
    if counts_history is None:
        counts_history = {}
    tf_1h = tf_data.get("1h") or {}
    tf_4h = tf_data.get("4h") or {}
    tf_1d = tf_data.get("1d") or {}
    if not tf_1h or not tf_4h or not tf_1d:
        return None
    tf_1w = tf_data.get("1w") or {}
    _tf4_up = tf_1w.get("ema_trend_up", True)  # fail-safe: 4h ausente => no bloquear
    _tf4_enabled = cfg.g("trend_filter_1w", "ENABLED", default=False)
    _tf4_mode    = cfg.g("trend_filter_1w", "MODE",    default="soft")
    _tf4_penalty = cfg.g("trend_filter_1w", "PENALTY", default=2)
    _tf4_signals = set(cfg.g("trend_filter_1w", "SIGNALS", default=["BREAKOUT","HOLD","RIDING"]))

    # Los thresholds OBV/CVD varían entre variantes: se re-derivan aquí para que
    # _build_candidates() pueda compartirse entre todas las variantes (una sola entrada en analyze_cache).
    tf_4h["obv_rising"]  = tf_4h.get("obv_slope", 0) >= cfg.g("indicators", "OBV_RISING_MIN", default=0.05)
    tf_4h["cvd_bullish"] = tf_4h.get("cvd_ratio", 0) >= cfg.g("indicators", "CVD_BULLISH_MIN", default=0.05)

    # Gates universales (aplican a todos los signals).
    if not tf_1d.get("not_near_resistance"):
        _bypass_resist = (
            cfg.g("hold", "RESIST_BYPASS_ON_15M_BREAKOUT", default=False)
            and tf_4h.get("breakout", False)
            and tf_4h.get("vol_ratio", 0) >= cfg.g("hold", "RESIST_BYPASS_MIN_VOL_15M", default=999.0)
            and (tf_4h.get("bars_since_break") or 999) <= cfg.g("hold", "RESIST_BYPASS_MAX_BARS_SINCE_BREAK", default=0)
        )
        if not _bypass_resist:
            return None
    if not tf_1d.get("major_struct_ok", True):
        _bypass_brk = cfg.g("hold", "MAJOR_STRUCT_BYPASS_ON_BREAKOUT", default=False) and tf_4h.get("breakout", False)
        _bypass_pre = cfg.g("hold", "MAJOR_STRUCT_BYPASS_ON_STRONG_PRE", default=False) and tf_4h.get("cvd_bullish", False) and tf_4h.get("obv_rising", False)
        _bypass_vol = tf_1d.get("vol_ratio", 0) >= cfg.g("hold", "MAJOR_STRUCT_BYPASS_VOL_RATIO_1H", default=999.0)
        _age_thr    = cfg.g("hold", "MAJOR_STRUCT_BYPASS_AGE_BARS", default=999)
        _bsm        = tf_1d.get("bars_since_major_max")
        _bypass_age = _bsm is not None and _bsm >= _age_thr
        _soft_dist  = cfg.g("hold", "MAJOR_STRUCT_BYPASS_SOFTZONE_DIST", default=0.0)
        _soft_max   = cfg.g("hold", "MAJOR_STRUCT_BYPASS_SOFTZONE_HOLD_MAX_BARS", default=0)
        _msd        = tf_1d.get("major_struct_dist")
        _bsb        = tf_4h.get("bars_since_break")
        _bypass_soft = (
            _soft_dist > 0 and _soft_max > 0
            and _msd is not None and _msd <= _soft_dist
            and tf_4h.get("hold_recent_break", False)
            and _bsb is not None and _bsb <= _soft_max
        )
        _struct_gate_hard = cfg.g("hold", "MAJOR_STRUCT_GATE_HARD", default=True)
        if not (_bypass_brk or _bypass_pre or _bypass_vol or _bypass_age or _bypass_soft):
            if _struct_gate_hard:
                return None
            _struct_soft_pen = True
        else:
            _struct_soft_pen = False
    else:
        _struct_soft_pen = False

    candidates = []
    SCORE_CAP = cfg.g("scoring", "SCORE_CAP")
    LATE_REPEAT_COUNT = cfg.g("history", "LATE_REPEAT_COUNT", default=1)
    _cal_cfg = cfg.raw.get("scoring_calibration") or {}

    _persist_on = cfg.g("persistence", "ENABLED", default=False)

    # Gates para signals tradicionales (PREBREAK/BREAKOUT/RIDING/HOLD/FADING).
    _ema_gate_hard = cfg.g("indicators", "EMA_GATE_HARD", default=True)
    _ema_blocks = (not tf_1d.get("ema_trend_up")) and _ema_gate_hard
    _atr_pct_v      = tf_1d.get("atr_pct", 0)
    _atr_threshold_v = tf_1d.get("atr_threshold", cfg.g("indicators", "ATR_MIN_PCT"))
    _atr_blocks = _atr_pct_v < _atr_threshold_v
    _trad_signals_eligible = not (_ema_blocks or _atr_blocks)
    if not _trad_signals_eligible and not candidates:
        return None

    # ── PREBREAK ──────────────────────────────────────────────────────────
    if _trad_signals_eligible and cfg.g("active_signals", "PREBREAK"):
        _pre_vol_bars = cfg.g("persistence", "PREBREAK_VOL_BARS", default=1) if _persist_on else 1
        _vol_ok_pre = tf_1h.get("vol_ratio", 0) >= cfg.g("prebreak", "PREBREAK_MIN_VOL_RATIO")
        if _pre_vol_bars >= 2:
            _vol_ok_pre = _vol_ok_pre and tf_1h.get("vol_ratio_prev", 0) >= cfg.g("prebreak", "PREBREAK_MIN_VOL_RATIO")
        if (tf_1h.get("near_recent_max")
            and tf_1h.get("width_curr", 9) <= cfg.g("prebreak", "PREBREAK_BB_WIDTH_MAX")
            and _vol_ok_pre
            and tf_1h.get("vol_growth", 0) >= cfg.g("prebreak", "PREBREAK_VOLUME_GROWTH_MIN")):

            base_offset    = cfg.g("scoring_prebreak", "BASE_OFFSET", default=4)
            base_mult      = cfg.g("scoring_prebreak", "BASE_MULTIPLIER", default=3)
            vol_div        = cfg.g("scoring_prebreak", "VOL_FACTOR_DIV", default=2.5)
            vol_cap        = cfg.g("scoring_prebreak", "VOL_FACTOR_CAP", default=3.0)
            grow_div       = cfg.g("scoring_prebreak", "GROWTH_FACTOR_DIV", default=1.2)
            grow_cap       = cfg.g("scoring_prebreak", "GROWTH_FACTOR_CAP", default=2.0)
            bb_div         = cfg.g("scoring_prebreak", "BB_FACTOR_DIV", default=0.03)
            bb_floor       = cfg.g("scoring_prebreak", "BB_FACTOR_FLOOR", default=0.3)
            close_bonus    = cfg.g("scoring_prebreak", "STRONG_CLOSE_BONUS", default=1)
            obv_up_bonus   = cfg.g("scoring_prebreak", "OBV_RISING_BONUS", default=2)
            obv_dn_pen     = cfg.g("scoring_prebreak", "OBV_FALLING_PENALTY", default=-1)
            struct_pen     = cfg.g("scoring_prebreak", "STRUCT_PENALTY", default=-1)

            vol_factor = min(tf_1h["vol_ratio"] / vol_div, vol_cap)
            growth_factor = min(tf_1h["vol_growth"] / grow_div, grow_cap)
            bb_factor = max(1.0 - (tf_1h["width_curr"] / bb_div), bb_floor)
            score = round(base_offset + vol_factor * growth_factor * bb_factor * base_mult)
            bd = {"BASE_FORMULA": score}

            if tf_1h.get("strong_close"):
                score += close_bonus
                bd["STRONG_CLOSE"] = close_bonus
            if tf_4h.get("obv_rising"):
                score += obv_up_bonus
                bd["OBV_RISING"] = obv_up_bonus
            elif tf_4h.get("obv_slope", 0) < -cfg.g("indicators", "OBV_RISING_MIN", default=0.05):
                score += obv_dn_pen
                bd["OBV_FALLING"] = obv_dn_pen
            if not tf_4h.get("recent_long_ok", True):
                score += struct_pen
                bd["STRUCT"] = struct_pen

            # LATE_REPEAT_PENALTY (mismo comportamiento que screener.py)
            late_repeat_pen = cfg.g("scoring_prebreak", "LATE_REPEAT_PENALTY", default=-1)
            prev_pb = counts_history.get((symbol, "PREBREAK"), 0)
            if prev_pb >= LATE_REPEAT_COUNT:
                score += late_repeat_pen
                bd["LATE_REPEAT"] = late_repeat_pen

            score = min(score, SCORE_CAP)
            candidates.append({
                "label": "PRE-BREAK", "history_tf": "PREBREAK", "score": score,
                "priority": 1, "bucket": final_bucket(score, "PREBREAK", cfg),
                "timeframe": "1h", "price": tf_1h["price"],
                "ref_price": tf_1h["recent_max"],
                "obv_slope": tf_4h.get("obv_slope"),
                "cvd_ratio": tf_4h.get("cvd_ratio"),
                "recent_long_ok": tf_4h.get("recent_long_ok"),
                "htf_1d_up": bool(tf_1d.get("ema_trend_up")),
                "htf_1w_up": bool(tf_1w.get("ema_trend_up")),
                "breakdown": bd,
                # features extendidas para diagnóstico
                "vol_ratio": tf_1h.get("vol_ratio"),
                "bb_width": tf_1h.get("width_curr"),
                "vol_growth": tf_1h.get("vol_growth"),
                "dist_to_res": tf_4h.get("dist_to_res"),
                # features candidatas (Fase A separabilidad) — ya disponibles, cero precómputo
                "width_expansion": tf_1h.get("width_expansion"),
                "atr_pct": tf_1h.get("atr_pct"),
                "atr_pct_1d": tf_1d.get("atr_pct"),
                "close_change_curr": tf_1h.get("close_change_curr"),
                "breakout_distance": tf_1h.get("breakout_distance"),
                "bars_since_major_max": tf_1h.get("bars_since_major_max"),
            })

    # ── BREAKOUT ──────────────────────────────────────────────────────────
    if _trad_signals_eligible and cfg.g("active_signals", "BREAKOUT"):
        require_obv_nn = cfg.g("breakout", "BREAKOUT_REQUIRE_OBV_NON_NEGATIVE", default=True)
        if cfg.g("breakout", "DEFER_ENABLED", default=False):
            # Modo diferido: los gates de la ruptura ya se evaluaron sobre la barra de
            # hace DEFER_BARS dentro de analyze_*; acá solo queda exigir que el
            # precio siga sobre el nivel. Los gates de 1h ("está rompiendo AHORA con
            # volumen") no aplican 24-48h después, por eso no se piden.
            _bo_fires = bool(tf_4h.get("defer_breakout"))
            # Para el bonus/penalty de entrada importa cuán extendido está respecto del
            # nivel ROTO, no del máximo reciente de hoy.
            _bo_dist = tf_4h.get("defer_distance") or 0.0
        else:
            _bo_fires = bool(
                tf_4h.get("breakout")
                and tf_4h.get("vol_ratio", 0) >= cfg.g("breakout", "BREAKOUT_MIN_VOL_RATIO")
                and tf_4h.get("breakout_distance", 9) <= cfg.g("breakout", "BREAKOUT_MAX_EXTENDED")
                and tf_4h.get("width_expansion", -9) >= cfg.g("breakout", "BREAKOUT_BB_EXPANSION_MIN")
                and tf_4h.get("strong_close", False)
                and tf_4h.get("candle_body_pct", 0) >= cfg.g("breakout", "BREAKOUT_MIN_BODY_PCT")
                and tf_1h.get("vol_ratio", 0) >= cfg.g("breakout", "BREAKOUT_1H_MIN_VOL_RATIO")
                and tf_1h.get("strong_close", False))
            _bo_dist = tf_4h.get("breakout_distance", 0)
        if _bo_fires and (not require_obv_nn or tf_4h.get("obv_slope", 0) >= 0):

            base_score        = cfg.g("scoring_breakout", "BASE_SCORE", default=8)
            obv_explosive_min = cfg.g("scoring_breakout", "OBV_TIER_EXPLOSIVE_MIN", default=0.3)
            obv_explosive_b   = cfg.g("scoring_breakout", "OBV_TIER_EXPLOSIVE_BONUS", default=4)
            obv_strong_min    = cfg.g("scoring_breakout", "OBV_TIER_STRONG_MIN", default=0.1)
            obv_strong_b      = cfg.g("scoring_breakout", "OBV_TIER_STRONG_BONUS", default=3)
            obv_rising_min    = cfg.g("scoring_breakout", "OBV_TIER_RISING_MIN", default=0.05)
            obv_rising_b      = cfg.g("scoring_breakout", "OBV_TIER_RISING_BONUS", default=2)
            obv_neutral_min   = cfg.g("scoring_breakout", "OBV_TIER_NEUTRAL_MIN", default=0)
            obv_neutral_b     = cfg.g("scoring_breakout", "OBV_TIER_NEUTRAL_BONUS", default=0)
            obv_falling_pen   = cfg.g("scoring_breakout", "OBV_TIER_FALLING_PENALTY", default=-2)
            cvd_vbull_min     = cfg.g("scoring_breakout", "CVD_TIER_VERY_BULLISH_MIN", default=0.1)
            cvd_vbull_b       = cfg.g("scoring_breakout", "CVD_TIER_VERY_BULLISH_BONUS", default=2)
            cvd_bull_min      = cfg.g("scoring_breakout", "CVD_TIER_BULLISH_MIN", default=0.05)
            cvd_bull_b        = cfg.g("scoring_breakout", "CVD_TIER_BULLISH_BONUS", default=1)
            cvd_neutral_min   = cfg.g("scoring_breakout", "CVD_TIER_NEUTRAL_MIN", default=-0.05)
            cvd_neutral_b     = cfg.g("scoring_breakout", "CVD_TIER_NEUTRAL_BONUS", default=0)
            cvd_bear_pen      = cfg.g("scoring_breakout", "CVD_TIER_BEARISH_PENALTY", default=-1)
            climax_vol_min    = cfg.g("scoring_breakout", "CLIMAX_VOL_MIN", default=5.0)
            climax_bb_min     = cfg.g("scoring_breakout", "CLIMAX_BB_EXP_MIN", default=0.4)
            climax_body_min   = cfg.g("scoring_breakout", "CLIMAX_BODY_MIN", default=0.85)
            climax_thresh     = cfg.g("scoring_breakout", "CLIMAX_THRESHOLD", default=2)
            climax_pen        = cfg.g("scoring_breakout", "CLIMAX_PENALTY", default=-2)
            early_max         = cfg.g("scoring_breakout", "EARLY_ENTRY_MAX", default=0.015)
            early_bonus       = cfg.g("scoring_breakout", "EARLY_ENTRY_BONUS", default=2)
            late_min          = cfg.g("scoring_breakout", "LATE_ENTRY_MIN", default=0.025)
            late_pen_entry    = cfg.g("scoring_breakout", "LATE_ENTRY_PENALTY", default=-1)
            struct_pen        = cfg.g("scoring_breakout", "STRUCT_PENALTY", default=-1)

            score = base_score
            bd = {"BASE": base_score}
            obv_v = tf_4h.get("obv_slope", 0)
            cvd_v = tf_4h.get("cvd_ratio", 0)

            if obv_v >= obv_explosive_min:
                score += obv_explosive_b; bd["OBV_EXPLOSIVE"] = obv_explosive_b
            elif obv_v >= obv_strong_min:
                score += obv_strong_b;   bd["OBV_STRONG"]    = obv_strong_b
            elif obv_v >= obv_rising_min:
                score += obv_rising_b;   bd["OBV_RISING"]    = obv_rising_b
            elif obv_v >= obv_neutral_min:
                score += obv_neutral_b;  bd["OBV_NEUTRAL"]   = obv_neutral_b
            else:
                score += obv_falling_pen; bd["OBV_FALLING"]  = obv_falling_pen

            if cvd_v >= cvd_vbull_min:
                score += cvd_vbull_b;  bd["CVD_VERY_BULLISH"] = cvd_vbull_b
            elif cvd_v >= cvd_bull_min:
                score += cvd_bull_b;   bd["CVD_BULLISH"]      = cvd_bull_b
            elif cvd_v >= cvd_neutral_min:
                score += cvd_neutral_b; bd["CVD_NEUTRAL"]     = cvd_neutral_b
            else:
                score += cvd_bear_pen; bd["CVD_BEARISH"]      = cvd_bear_pen

            climax_signals = 0
            if tf_4h.get("vol_ratio", 0) >= climax_vol_min:
                climax_signals += 1
            if tf_4h.get("width_expansion", 0) >= climax_bb_min:
                climax_signals += 1
            if tf_4h.get("candle_body_pct", 0) >= climax_body_min:
                climax_signals += 1
            _climax_late_only = cfg.g("indicators", "CLIMAX_REQUIRES_LATE_ENTRY", default=False)
            if climax_signals >= climax_thresh and (not _climax_late_only or _bo_dist >= late_min):
                score += climax_pen
                bd["CLIMAX"] = climax_pen

            if _bo_dist <= early_max:
                score += early_bonus
                bd["EARLY_ENTRY"] = early_bonus
            elif _bo_dist >= late_min:
                score += late_pen_entry
                bd["LATE_ENTRY"] = late_pen_entry

            if not tf_4h.get("recent_long_ok", True):
                score += struct_pen
                bd["STRUCT"] = struct_pen

            # LATE_REPEAT_PENALTY (mismo comportamiento que screener.py)
            late_repeat_pen = cfg.g("scoring_breakout", "LATE_REPEAT_PENALTY", default=-1)
            prev_bo = counts_history.get((symbol, "BREAKOUT"), 0)
            if prev_bo >= LATE_REPEAT_COUNT:
                score += late_repeat_pen
                bd["LATE_REPEAT"] = late_repeat_pen

            if _tf4_enabled and "BREAKOUT" in _tf4_signals and not _tf4_up and _tf4_mode == "soft":
                score -= _tf4_penalty; bd["NO_1W_TREND"] = -_tf4_penalty
            score = min(score, SCORE_CAP)
            if not (_tf4_enabled and _tf4_mode == "hard" and "BREAKOUT" in _tf4_signals and not _tf4_up):
                candidates.append({
                    "label": "BREAKOUT", "history_tf": "BREAKOUT", "score": score,
                    "priority": 2, "bucket": final_bucket(score, "BREAKOUT", cfg),
                    "timeframe": "4h", "price": tf_4h["price"],
                    "ref_price": tf_4h["recent_max"],
                    "obv_slope": tf_4h.get("obv_slope"),
                    "cvd_ratio": tf_4h.get("cvd_ratio"),
                    "recent_long_ok": tf_4h.get("recent_long_ok"),
                    "htf_1d_up": bool(tf_1d.get("ema_trend_up")),
                    "htf_1w_up": bool(tf_1w.get("ema_trend_up")),
                    "breakdown": bd,
                    # features extendidas para diagnóstico
                    "vol_ratio": tf_4h.get("vol_ratio"),
                    "bb_width": tf_4h.get("width_curr"),
                    "breakout_distance": tf_4h.get("breakout_distance"),
                    "dist_to_res": tf_1d.get("dist_to_res"),
                    # En modo diferido: cuántas barras pasaron desde la ruptura y cuánto
                    # está por encima del nivel roto (None si disparó en la vela misma).
                    "defer_bars_since": tf_4h.get("defer_bars_since"),
                    "defer_distance": tf_4h.get("defer_distance"),
                })

    # ── RIDING ────────────────────────────────────────────────────────────
    if _trad_signals_eligible and cfg.g("active_signals", "RIDING"):
        rg = tf_4h.get("riding_gain") or 0.0
        if (tf_4h.get("riding_above_zone")
            and tf_4h.get("riding_vol_ok")
            and cfg.g("riding", "RIDING_MIN_GAIN") <= rg <= cfg.g("riding", "RIDING_MAX_GAIN")
            and tf_4h.get("riding_bars_since") is not None
            and tf_4h["riding_bars_since"] >= 1
            and (not cfg.g("riding", "RIDING_EMA_MUST_TREND") or tf_1d.get("ema_trend_up"))
            and not tf_4h.get("breakout")
            and (tf_4h.get("fading_reversal") or 0.0) >= -cfg.g("riding", "RIDING_MAX_FADE_FROM_HIGH", default=0.025)):

            base_score      = cfg.g("scoring_riding", "BASE_SCORE", default=4)
            gain_strong_min = cfg.g("scoring_riding", "GAIN_TIER_STRONG_MIN", default=0.05)
            gain_strong_b   = cfg.g("scoring_riding", "GAIN_TIER_STRONG_BONUS", default=3)
            gain_solid_min  = cfg.g("scoring_riding", "GAIN_TIER_SOLID_MIN", default=0.02)
            gain_solid_b    = cfg.g("scoring_riding", "GAIN_TIER_SOLID_BONUS", default=2)
            gain_initial_b  = cfg.g("scoring_riding", "GAIN_TIER_INITIAL_BONUS", default=1)
            vol_ok_b        = cfg.g("scoring_riding", "VOL_OK_BONUS", default=1)
            close_b         = cfg.g("scoring_riding", "STRONG_CLOSE_BONUS", default=1)
            ema_b           = cfg.g("scoring_riding", "EMA_TREND_BONUS", default=1)
            dist_high_min   = cfg.g("scoring_riding", "DIST_RES_HIGH_MIN", default=0.04)
            dist_high_b     = cfg.g("scoring_riding", "DIST_RES_HIGH_BONUS", default=2)
            dist_low_b      = cfg.g("scoring_riding", "DIST_RES_LOW_BONUS", default=1)
            obv_up_b        = cfg.g("scoring_riding", "OBV_RISING_BONUS", default=1)
            obv_dn_pen      = cfg.g("scoring_riding", "OBV_FALLING_PENALTY", default=-2)
            cvd_up_b        = cfg.g("scoring_riding", "CVD_BULLISH_BONUS", default=1)
            cvd_dn_pen      = cfg.g("scoring_riding", "CVD_BEARISH_PENALTY", default=-2)
            # Variante G: bonus extra por gain excepcional (>=8% por defecto, default 0 = inactivo)
            strong_gain_b   = cfg.g("scoring_riding", "STRONG_GAIN_BONUS", default=0)
            strong_gain_min = cfg.g("scoring_riding", "STRONG_GAIN_MIN", default=0.08)

            score = base_score
            bd = {"BASE": base_score}
            if rg >= gain_strong_min:
                score += gain_strong_b; bd["GAIN_STRONG"] = gain_strong_b
            elif rg >= gain_solid_min:
                score += gain_solid_b;  bd["GAIN_SOLID"]  = gain_solid_b
            else:
                score += gain_initial_b; bd["GAIN_INITIAL"] = gain_initial_b
            # Bonus adicional para gains excepcionales
            if rg >= strong_gain_min and strong_gain_b:
                score += strong_gain_b; bd["STRONG_GAIN_BONUS"] = strong_gain_b
            if tf_4h.get("riding_vol_ok"):
                score += vol_ok_b; bd["VOL_OK"] = vol_ok_b
            if tf_4h.get("strong_close"):
                score += close_b; bd["STRONG_CLOSE"] = close_b
            _ema_up_effective = tf_1d.get("ema_trend_up")
            if _ema_up_effective:
                score += ema_b; bd["EMA_TREND"] = ema_b
            if tf_1d.get("dist_to_res", 0) > dist_high_min:
                score += dist_high_b; bd["DIST_HIGH"] = dist_high_b
            else:
                score += dist_low_b; bd["DIST_LOW"] = dist_low_b
            if tf_4h.get("obv_rising"):
                score += obv_up_b; bd["OBV_RISING"] = obv_up_b
            elif tf_4h.get("obv_slope", 0) < 0:
                score += obv_dn_pen; bd["OBV_FALLING"] = obv_dn_pen
            if tf_4h.get("cvd_bullish"):
                score += cvd_up_b; bd["CVD_BULLISH"] = cvd_up_b
            elif tf_4h.get("cvd_ratio", 0) < -cfg.g("indicators", "CVD_BULLISH_MIN", default=0.05):
                score += cvd_dn_pen; bd["CVD_BEARISH"] = cvd_dn_pen

            # Bonus por frescura: RIDING detectado en las primeras N barras desde el break
            _fresh_max = cfg.g("scoring_riding", "FRESH_MAX_BARS", default=0)
            _fresh_b   = cfg.g("scoring_riding", "FRESH_BONUS", default=0)
            _bars_since = tf_4h.get("riding_bars_since")
            if _fresh_b and _fresh_max and _bars_since is not None and _bars_since <= _fresh_max:
                score += _fresh_b; bd["FRESH"] = _fresh_b

            # LATE_REPEAT_PENALTY (mismo comportamiento que screener.py)
            prev_riding = counts_history.get((symbol, "RIDING"), 0)
            late_repeat_pen = cfg.g("scoring_riding", "LATE_REPEAT_PENALTY", default=0)
            if prev_riding >= LATE_REPEAT_COUNT and late_repeat_pen < 0:
                score += late_repeat_pen
                bd["LATE_REPEAT"] = late_repeat_pen

            if _tf4_enabled and "RIDING" in _tf4_signals and not _tf4_up and _tf4_mode == "soft":
                score -= _tf4_penalty; bd["NO_1W_TREND"] = -_tf4_penalty
            score = min(score, SCORE_CAP)
            if not (_tf4_enabled and _tf4_mode == "hard" and "RIDING" in _tf4_signals and not _tf4_up):
                candidates.append({
                    "label": "RIDING", "history_tf": "RIDING", "score": score,
                    "priority": 2, "bucket": final_bucket(score, "RIDING", cfg),
                    "timeframe": "4h", "price": tf_4h["price"],
                    "ref_price": tf_4h.get("riding_break_close"),
                    "obv_slope": tf_4h.get("obv_slope"),
                    "cvd_ratio": tf_4h.get("cvd_ratio"),
                    "recent_long_ok": tf_4h.get("recent_long_ok"),
                    "htf_1d_up": bool(tf_1d.get("ema_trend_up")),
                    "htf_1w_up": bool(tf_1w.get("ema_trend_up")),
                    "breakdown": bd,
                    # features extendidas para diagnóstico
                    "vol_ratio": tf_4h.get("vol_ratio"),
                    "riding_gain": tf_4h.get("riding_gain"),
                    "bars_since_break": tf_4h.get("bars_since_break"),
                })

    # ── HOLD ──────────────────────────────────────────────────────────────
    if _trad_signals_eligible and cfg.g("active_signals", "HOLD"):
        _require_obv_hold = cfg.g("hold", "HOLD_REQUIRE_OBV_RISING", default=False)
        _hold_obv_bars = cfg.g("persistence", "HOLD_OBV_BARS", default=1) if _persist_on else 1
        _obv_ok_hold = (not _require_obv_hold) or tf_4h.get("obv_rising", False)
        if _require_obv_hold and _hold_obv_bars >= 2:
            _obv_ok_hold = _obv_ok_hold and (tf_4h.get("obv_slope_prev", 0) >= cfg.g("indicators", "OBV_RISING_MIN", default=0.05))
        _cvd_ok_hold = (not cfg.g("hold", "HOLD_REQUIRE_CVD_BULLISH", default=False)) or tf_4h.get("cvd_bullish", False)
        if (tf_4h.get("hold_recent_break") and tf_4h.get("hold_kept_zone")
            and tf_4h.get("hold_pullback_ok") and tf_4h.get("hold_strong")
            and _obv_ok_hold and _cvd_ok_hold):

            base          = cfg.g("scoring_hold", "BASE_SCORE", default=5)
            above_b       = cfg.g("scoring_hold", "ABOVE_RESIST_BONUS", default=2)
            pullback_b    = cfg.g("scoring_hold", "PULLBACK_BONUS", default=1)
            close_b       = cfg.g("scoring_hold", "STRONG_CLOSE_BONUS", default=1)
            dist_high_min = cfg.g("scoring_hold", "DIST_RES_HIGH_MIN", default=0.04)
            dist_high_b   = cfg.g("scoring_hold", "DIST_RES_HIGH_BONUS", default=2)
            dist_low_b    = cfg.g("scoring_hold", "DIST_RES_LOW_BONUS", default=1)
            obv_up_b      = cfg.g("scoring_hold", "OBV_RISING_BONUS", default=1)
            obv_dn_pen    = cfg.g("scoring_hold", "OBV_FALLING_PENALTY", default=-2)
            cvd_up_b      = cfg.g("scoring_hold", "CVD_BULLISH_BONUS", default=1)
            cvd_dn_pen    = cfg.g("scoring_hold", "CVD_BEARISH_PENALTY", default=-2)
            struct_pen    = cfg.g("scoring_hold", "STRUCT_PENALTY", default=-1)
            # Variante G: bonus por momentum genuinamente fuerte (default 0 = inactivo)
            momentum_b    = cfg.g("scoring_hold", "STRONG_MOMENTUM_BONUS", default=0)
            momentum_obv  = cfg.g("scoring_hold", "STRONG_MOMENTUM_OBV_MIN", default=0.2)
            momentum_dist = cfg.g("scoring_hold", "STRONG_MOMENTUM_DIST_MIN", default=0.05)

            score = base + above_b + pullback_b + close_b
            bd = {"BASE": base, "ABOVE_RESIST": above_b, "PULLBACK": pullback_b, "STRONG_CLOSE": close_b}
            if tf_1d.get("dist_to_res", 0) > dist_high_min:
                score += dist_high_b; bd["DIST_HIGH"] = dist_high_b
            else:
                score += dist_low_b;  bd["DIST_LOW"]  = dist_low_b
            if tf_4h.get("obv_rising"):
                score += obv_up_b; bd["OBV_RISING"] = obv_up_b
            elif tf_4h.get("obv_slope", 0) < -cfg.g("indicators", "OBV_RISING_MIN", default=0.05):
                score += obv_dn_pen; bd["OBV_FALLING"] = obv_dn_pen
            if tf_4h.get("cvd_bullish"):
                score += cvd_up_b; bd["CVD_BULLISH"] = cvd_up_b
            elif tf_4h.get("cvd_ratio", 0) < -cfg.g("indicators", "CVD_BULLISH_MIN", default=0.05):
                score += cvd_dn_pen; bd["CVD_BEARISH"] = cvd_dn_pen
            if not tf_4h.get("recent_long_ok", True):
                score += struct_pen; bd["STRUCT"] = struct_pen
            # Bonus por momentum extremo: OBV explosivo + CVD bullish + lejos de resistencia 1h
            if (tf_4h.get("obv_slope", 0) >= momentum_obv
                and tf_4h.get("cvd_bullish")
                and tf_1d.get("dist_to_res", 0) >= momentum_dist):
                score += momentum_b
                if momentum_b:
                    bd["MOMENTUM"] = momentum_b

            # LATE_REPEAT_PENALTY (mismo comportamiento que screener.py)
            late_repeat_pen = cfg.g("scoring_hold", "LATE_REPEAT_PENALTY", default=-1)
            prev_hold = counts_history.get((symbol, "HOLD"), 0)
            if prev_hold >= LATE_REPEAT_COUNT:
                score += late_repeat_pen
                bd["LATE_REPEAT"] = late_repeat_pen

            if _tf4_enabled and "HOLD" in _tf4_signals and not _tf4_up and _tf4_mode == "soft":
                score -= _tf4_penalty; bd["NO_1W_TREND"] = -_tf4_penalty
            score = min(score, SCORE_CAP)
            if not (_tf4_enabled and _tf4_mode == "hard" and "HOLD" in _tf4_signals and not _tf4_up):
                candidates.append({
                    "label": "HOLD", "history_tf": "HOLD", "score": score,
                    "priority": 3, "bucket": final_bucket(score, "HOLD", cfg),
                    "timeframe": "4h", "price": tf_4h["price"],
                    "ref_price": tf_4h.get("riding_break_ref") or tf_4h["recent_max"],
                    "obv_slope": tf_4h.get("obv_slope"),
                    "cvd_ratio": tf_4h.get("cvd_ratio"),
                    "recent_long_ok": tf_4h.get("recent_long_ok"),
                    "htf_1d_up": bool(tf_1d.get("ema_trend_up")),
                    "htf_1w_up": bool(tf_1w.get("ema_trend_up")),
                    "breakdown": bd,
                    # features extendidas para diagnóstico
                    "vol_ratio": tf_4h.get("vol_ratio"),
                    "dist_to_res": tf_1d.get("dist_to_res"),
                    "bars_since_break": tf_4h.get("bars_since_break"),
                })

    # ── COILING ───────────────────────────────────────────────────────────
    if _trad_signals_eligible and cfg.g("active_signals", "COILING", default=False):
        _coil_max_dist = cfg.g("coiling", "COILING_MAX_DIST", default=0.04)
        _coil_min_vol  = cfg.g("coiling", "COILING_MIN_VOL_RATIO", default=1.0)
        _coil_bb_max   = cfg.g("coiling", "COILING_BB_WIDTH_MAX", default=9.0)
        _bd = tf_4h.get("breakout_distance", -999)
        if (not tf_4h.get("breakout", False)
            and -_coil_max_dist <= _bd < 0
            and tf_4h.get("obv_rising", False)
            and tf_4h.get("cvd_bullish", False)
            and tf_4h.get("vol_ratio", 0) >= _coil_min_vol
            and tf_4h.get("width_curr", 9) <= _coil_bb_max):

            base_score     = cfg.g("scoring_coiling", "BASE_SCORE",               default=3)
            obv_exp_min    = cfg.g("scoring_coiling", "OBV_TIER_EXPLOSIVE_MIN",    default=0.3)
            obv_exp_b      = cfg.g("scoring_coiling", "OBV_TIER_EXPLOSIVE_BONUS",  default=3)
            obv_strong_min = cfg.g("scoring_coiling", "OBV_TIER_STRONG_MIN",       default=0.1)
            obv_strong_b   = cfg.g("scoring_coiling", "OBV_TIER_STRONG_BONUS",     default=2)
            obv_rising_b   = cfg.g("scoring_coiling", "OBV_TIER_RISING_BONUS",     default=1)
            cvd_vbull_min  = cfg.g("scoring_coiling", "CVD_TIER_VERY_BULLISH_MIN", default=0.1)
            cvd_vbull_b    = cfg.g("scoring_coiling", "CVD_TIER_VERY_BULLISH_BONUS", default=2)
            cvd_bull_b     = cfg.g("scoring_coiling", "CVD_TIER_BULLISH_BONUS",    default=1)
            close_b        = cfg.g("scoring_coiling", "STRONG_CLOSE_BONUS",        default=1)
            dist_tight_max = cfg.g("scoring_coiling", "DIST_TIGHT_MAX",            default=0.01)
            dist_tight_b   = cfg.g("scoring_coiling", "DIST_TIGHT_BONUS",          default=2)
            dist_wide_min  = cfg.g("scoring_coiling", "DIST_WIDE_MIN",             default=0.03)
            dist_wide_pen  = cfg.g("scoring_coiling", "DIST_WIDE_PENALTY",         default=-1)
            late_repeat_pen = cfg.g("scoring_coiling", "LATE_REPEAT_PENALTY",      default=-1)

            score = base_score
            bd_obj = {"BASE": base_score}

            obv_v = tf_4h.get("obv_slope", 0)
            if obv_v >= obv_exp_min:
                score += obv_exp_b;    bd_obj["OBV_EXPLOSIVE"] = obv_exp_b
            elif obv_v >= obv_strong_min:
                score += obv_strong_b; bd_obj["OBV_STRONG"]    = obv_strong_b
            else:
                score += obv_rising_b; bd_obj["OBV_RISING"]    = obv_rising_b

            cvd_v = tf_4h.get("cvd_ratio", 0)
            if cvd_v >= cvd_vbull_min:
                score += cvd_vbull_b;  bd_obj["CVD_VERY_BULLISH"] = cvd_vbull_b
            else:
                score += cvd_bull_b;   bd_obj["CVD_BULLISH"]      = cvd_bull_b

            if tf_4h.get("strong_close"):
                score += close_b;      bd_obj["STRONG_CLOSE"] = close_b

            # Bonus por coiling muy tight (< 1%), penalidad por alejado (> 3%)
            if abs(_bd) <= dist_tight_max:
                score += dist_tight_b; bd_obj["DIST_TIGHT"] = dist_tight_b
            elif abs(_bd) >= dist_wide_min:
                score += dist_wide_pen; bd_obj["DIST_WIDE"] = dist_wide_pen

            prev_coil = counts_history.get((symbol, "COILING"), 0)
            if prev_coil >= LATE_REPEAT_COUNT:
                score += late_repeat_pen
                bd_obj["LATE_REPEAT"] = late_repeat_pen

            score = min(score, SCORE_CAP)
            candidates.append({
                "label": "COILING", "history_tf": "COILING", "score": score,
                "priority": 0, "bucket": final_bucket(score, "COILING", cfg),
                "timeframe": "4h", "price": tf_4h["price"],
                "ref_price": tf_4h["recent_max"],
                "obv_slope": tf_4h.get("obv_slope"),
                "cvd_ratio": tf_4h.get("cvd_ratio"),
                "recent_long_ok": tf_4h.get("recent_long_ok"),
                "htf_1d_up": bool(tf_1d.get("ema_trend_up")),
                "htf_1w_up": bool(tf_1w.get("ema_trend_up")),
                "breakdown": bd_obj,
            })

    if not candidates:
        return None

    IMMEDIATE_MIN_SCORE = cfg.g("scoring", "IMMEDIATE_MIN_SCORE", default=13)

    for c in candidates:
        c["candle_status"] = (tf_data.get(c["timeframe"]) or {}).get("candle_status", "closed")
        # atr_pct_1d (ATR% diario HTF) es el único separador robusto de movers (gate a BEST
        # en PREBREAK/COILING). Exponerlo en TODA señal alimenta el badge de convicción del
        # mensaje de Telegram. PREBREAK ya lo trae en su dict; aquí se unifica para el resto.
        c["atr_pct_1d"] = tf_1d.get("atr_pct")

    # Penalización suave por EMA 1d no alcista (solo cuando EMA_GATE_HARD=false).
    if not _ema_gate_hard and not tf_1d.get("ema_trend_up"):
        _ema_pen = int(cfg.g("indicators", "EMA_SOFT_PENALTY", default=-2))
        for c in candidates:
            c["score"] = max(0, c["score"] + _ema_pen)
            c["bucket"] = final_bucket(c["score"], c["history_tf"], cfg)
            if "breakdown" in c:
                c["breakdown"]["EMA_SOFT"] = _ema_pen

    # ── Filtro de régimen de mercado (risk-off) ──
    # regime_up lo resuelve el caller (backtest con la serie histórica, screener con la
    # última barra 1d): acá sólo se aplica. None = sin info → no toca nada (fail-safe).
    _reg_cfg = cfg.raw.get("regime_filter") or {}
    if _reg_cfg.get("ENABLED", False) and regime_up is False:
        _reg_signals = set(_reg_cfg.get("SIGNALS") or [])
        _reg_hard = _reg_cfg.get("MODE", "soft") == "hard"
        _reg_pen = int(_reg_cfg.get("PENALTY", 2))
        _kept = []
        for c in candidates:
            if c["history_tf"] not in _reg_signals:
                _kept.append(c)
                continue
            if _reg_hard:
                continue                      # risk-off: la señal no se emite
            c["score"] = max(0, c["score"] - _reg_pen)
            c["bucket"] = final_bucket(c["score"], c["history_tf"], cfg)
            if "breakdown" in c:
                c["breakdown"]["REGIME_OFF"] = -_reg_pen
            _kept.append(c)
        candidates = _kept
        if not candidates:
            return None

    # Penalización suave por major_struct fallido (solo cuando MAJOR_STRUCT_GATE_HARD=false).
    if _struct_soft_pen:
        _struct_pen = int(cfg.g("hold", "MAJOR_STRUCT_SOFT_PENALTY", default=-2))
        for c in candidates:
            c["score"] = max(0, c["score"] + _struct_pen)
            c["bucket"] = final_bucket(c["score"], c["history_tf"], cfg)
            if "breakdown" in c:
                c["breakdown"]["STRUCT_SOFT"] = _struct_pen

    # Final cap loop (mismo screener.py). Redundante porque cada bloque ya
    # aplicó min(score, SCORE_CAP), pero se mantiene para que la simulación sea espejo.
    for c in candidates:
        if c["score"] > SCORE_CAP:
            orig = c["score"]
            c["score"] = SCORE_CAP
            c["bucket"] = final_bucket(c["score"], c["history_tf"], cfg)
            if "breakdown" in c:
                c["breakdown"]["SCORE_CAP_TRUNC"] = SCORE_CAP - orig

    # ── Gate atr_pct_1d → BEST (Fase B, config-driven, default OFF) ──────────
    # El bonus aditivo está muerto (gap score→12 es +3..+8 variable); el mecanismo
    # es un OVERRIDE de bucket. atr_pct_1d (ATR% diario HTF) es el único separador
    # robusto h1/h2 (AUC ~0.66/0.67). DEBE ir DESPUÉS de las penalizaciones soft
    # (EMA/struct/cap recomputan bucket vía final_bucket y lo pisarían si fuera
    # antes). Usa el score FINAL (post-penalización), que es la barra medida en
    # Fase A (banda 5–8). MIN_SCORE floor evita promover fuera de esa banda.
    atr1d_gate_min   = cfg.g("scoring_prebreak", "ATR1D_GATE_MIN", default=999.0)
    atr1d_gate_floor = cfg.g("scoring_prebreak", "ATR1D_GATE_MIN_SCORE", default=99)
    _atr1d_v = tf_1d.get("atr_pct", 0) or 0
    if _atr1d_v > atr1d_gate_min:
        for c in candidates:
            if (c["history_tf"] == "PREBREAK" and c["score"] >= atr1d_gate_floor
                    and c["bucket"] != "BEST"):
                c["bucket"] = "BEST"
                if "breakdown" in c:
                    c["breakdown"]["ATR1D_GATE"] = f">{atr1d_gate_min}@{_atr1d_v:.2f}"

    # ── Gate atr_pct_1d → BEST para COILING (mismo mecanismo que PREBREAK) ───
    # atr_pct_1d separa COILING-movers de no-movers (AUC 0.67 @ net≥10%, direccional
    # no-volatilidad: P(+10) 9→27% vs P(-10) 5→9%). El score interno de COILING NO es
    # predictivo, así que el floor va bajo (0 = promover todo COILING con atr>thr).
    # Valor = entrada más temprana sobre movers que el suite agarra tarde (lateness).
    coil_atr_gate_min   = cfg.g("scoring_coiling", "ATR1D_GATE_MIN", default=999.0)
    coil_atr_gate_floor = cfg.g("scoring_coiling", "ATR1D_GATE_MIN_SCORE", default=99)
    if _atr1d_v > coil_atr_gate_min:
        for c in candidates:
            if (c["history_tf"] == "COILING" and c["score"] >= coil_atr_gate_floor
                    and c["bucket"] != "BEST"):
                c["bucket"] = "BEST"
                if "breakdown" in c:
                    c["breakdown"]["ATR1D_GATE"] = f">{coil_atr_gate_min}@{_atr1d_v:.2f}"

    # ── Bucket por banda de ATR (rediseño jul-2026, config-driven, default OFF) ──
    # El score NO predice el cierre (Spearman ≈ −0.06 backtest / −0.14 live) pero
    # banda-ATR × score alto SÍ concentra movers (MFE≥10%: 48% vs 39% base, WF 7/7),
    # y con la capa de exit (trail+stop) lo que importa es densidad de pop, no cierre.
    # Regla: BEST = atr_pct_1d ∈ [BAND_MIN, BAND_MAX] y score ≥ BEST_MIN_SCORE;
    # STRONG = resto de la banda (mantiene cobertura del exit tracker: 99% de movers
    # per-símbolo en BEST+STRONG); WATCH = fuera de banda (atr<5 casi no mueve,
    # atr>15 es vol-junk con EV mediana −8.5%). Cuando está ON pisa los buckets de
    # score y los gates ATR per-señal de arriba (quedan solo para el modo legacy).
    _band_cfg = cfg.raw.get("bucket_atr_band", {})
    if _band_cfg.get("ENABLED", False):
        _band_min  = float(_band_cfg.get("BAND_MIN", 5.0))
        _band_max  = float(_band_cfg.get("BAND_MAX", 15.0))
        _band_best = int(_band_cfg.get("BEST_MIN_SCORE", 11))
        _inband = _band_min <= _atr1d_v <= _band_max
        for c in candidates:
            if _inband and c["score"] >= _band_best:
                c["bucket"] = "BEST"
            elif _inband:
                c["bucket"] = "STRONG"
            else:
                c["bucket"] = "WATCH"
            if "breakdown" in c:
                c["breakdown"]["BUCKET_ATR_BAND"] = (
                    f"atr1d={_atr1d_v:.2f} {'in' if _inband else 'out'} [{_band_min},{_band_max}]")

    candidates.sort(
        key=lambda x: (_normalize_score(x["score"], x["history_tf"], _cal_cfg, SCORE_CAP), x["priority"], x["score"]),
        reverse=True,
    )
    return candidates[0]


# ════════════════════════════════════════════════════════════════════════════
# SIMULACIÓN
# ════════════════════════════════════════════════════════════════════════════

def find_idx_at_or_before(df, ts_ms, timecol="close_time"):
    arr = df[timecol].values
    lo, hi = 0, len(arr) - 1
    if arr[0] > ts_ms:
        return -1
    if arr[-1] <= ts_ms:
        return len(arr) - 1
    while lo < hi:
        mid = (lo + hi + 1) // 2
        if arr[mid] <= ts_ms:
            lo = mid
        else:
            hi = mid - 1
    return lo


def calculate_outcomes(df_4h, alert_idx, alert_price):
    alert_ts = int(df_4h["close_time"].iloc[alert_idx])
    outcomes = {}
    for offset_min, name in zip(OUTCOME_OFFSETS_MIN, OUTCOME_NAMES):
        target_ts = alert_ts + offset_min * 60 * 1000
        idx = find_idx_at_or_before(df_4h, target_ts)
        if idx > alert_idx:
            outcomes[name] = float(df_4h["close"].iloc[idx])
        else:
            outcomes[name] = None

    end_1d = alert_ts + 1440 * 60 * 1000
    end_7d = alert_ts + 10080 * 60 * 1000
    end_1d_idx = find_idx_at_or_before(df_4h, end_1d)
    end_7d_idx = find_idx_at_or_before(df_4h, end_7d)
    max_high_1d = min_low_1d = max_high_7d = min_low_7d = None
    if end_1d_idx > alert_idx:
        w = df_4h.iloc[alert_idx + 1:end_1d_idx + 1]
        if len(w) > 0:
            max_high_1d = float(w["high"].max())
            min_low_1d = float(w["low"].min())
    if end_7d_idx > alert_idx:
        w = df_4h.iloc[alert_idx + 1:end_7d_idx + 1]
        if len(w) > 0:
            max_high_7d = float(w["high"].max())
            min_low_7d = float(w["low"].min())
    outcomes["max_high_1d"] = max_high_1d
    outcomes["min_low_1d"] = min_low_1d
    outcomes["max_high_7d"] = max_high_7d
    outcomes["min_low_7d"] = min_low_7d
    outcomes["entry_price"] = alert_price
    outcomes["complete"] = max_high_7d is not None
    return outcomes



def _build_candidates(klines, prepared, cfg, scan_ts, snapshot_pairs, derivatives):
    """Primer pase del scan: construye la lista de candidatos (sym, ts, tf_data, idx_4h)
    que pasan validación de datos. Variant-independiente para cfgs con el mismo _analyze_key.
    Cada elemento: (scan_i, ts_ms, sym, tf_data, idx_4h)."""
    # Pre-cachear parámetros de cfg una sola vez (evita ~24M cfg.g() en el hot loop)
    params = _build_analyze_params(cfg)

    # Precalcular índices de barras por (sym, TF) con searchsorted vectorizado.
    scan_ts_arr = np.asarray(scan_ts, dtype=np.int64)
    bar_idx_cache = {}
    for sym, tfs in klines.items():
        by_tf = {}
        for tf in ("1h", "4h", "1d", "1w"):
            sym_prep = (prepared.get(sym) or {}) if prepared else {}
            df = sym_prep.get(tf)
            if df is None:
                df = tfs.get(tf)
            if df is None:
                continue
            ct = df["close_time"].values.astype(np.int64)
            idxs = np.searchsorted(ct, scan_ts_arr, side="right") - 1
            idxs[scan_ts_arr < ct[0]] = -1
            by_tf[tf] = idxs
        bar_idx_cache[sym] = by_tf

    snap_runs_sorted = sorted(snapshot_pairs.keys()) if snapshot_pairs else []

    def pairs_for_scan(ts_ms):
        if not snapshot_pairs:
            return list(klines.keys())
        scan_iso = datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc).isoformat()
        best = None
        for run_iso in snap_runs_sorted:
            if run_iso <= scan_iso:
                best = run_iso
            else:
                break
        if best is None:
            return list(klines.keys())
        return [s for s in snapshot_pairs[best] if s in klines]

    # ── Función pura que procesa UN símbolo en UN scan ──
    def _process_symbol(i, ts_ms, sym):
        """Procesa un símbolo en un scan. Retorna candidato o None."""
        by_tf = bar_idx_cache.get(sym, {})
        tf_data = {}
        valid = True
        for tf in ("1h", "4h", "1d"):
            idxs = by_tf.get(tf)
            if idxs is None:
                valid = False
                break
            idx = int(idxs[i])
            if idx < 80:
                valid = False
                break
            sym_prep = (prepared.get(sym) or {}) if prepared else {}
            prep_df = sym_prep.get(tf)
            df = prep_df if prep_df is not None else klines.get(sym, {}).get(tf)
            result = analyze_at_index(df, idx, params) if prep_df is not None \
                     else analyze_at_time(df, idx, cfg)
            if result is None:
                valid = False
                break
            tf_data[tf] = result
        if not valid:
            return None
        # 1w opcional para filtro de tendencia (fail-safe si falta o índice insuficiente)
        _idxs_1w = by_tf.get("1w")
        if _idxs_1w is not None:
            _idx_1w = int(_idxs_1w[i])
            if _idx_1w >= 12:  # 1w: umbral bajo (12 semanas ≈ 90d buffer)
                _df_1w = klines.get(sym, {}).get("1w")
                if _df_1w is not None:
                    _r_1w = analyze_at_time(_df_1w, _idx_1w, cfg)
                    if _r_1w is not None:
                        tf_data["1w"] = _r_1w
        idx_4h = int(by_tf["4h"][i])
        return (i, ts_ms, sym, tf_data, idx_4h)

    # ── Lista plana de tareas: un Parallel para todas las combinaciones scan×símbolo ──
    # Evita crear/destruir el thread pool 1344 veces (overhead ~100ms cada una).
    all_tasks = []
    for i, ts_ms in enumerate(scan_ts):
        for sym in pairs_for_scan(ts_ms):
            all_tasks.append((i, ts_ms, sym))

    total_tasks = len(all_tasks)
    total_scans = len(scan_ts)
    print(f"    [analyze] {total_scans} scans × ~{len(klines)} pares = {total_tasks} tareas")

    raw = Parallel(n_jobs=-1, prefer="threads")(
        delayed(_process_symbol)(i, ts_ms, sym)
        for i, ts_ms, sym in all_tasks
    )

    # joblib preserva el orden de los inputs → all_tasks ya está ordenado por scan index
    all_candidates = [cand for cand in raw if cand is not None]
    print(f"    [analyze] {len(all_candidates)} candidatos encontrados")
    return all_candidates


def _classify_pass(candidates, cfg, cooldown_min_by_state, history_window_ms,
                   klines, outcomes_cache, audit_mode=False):
    """Segundo pase del scan: classify + cooldowns + outcomes sobre candidatos pre-extraídos.
    outcomes_cache[(sym, idx_4h)] permite reutilizar outcomes entre variantes."""
    last_alert_ts = {}
    # sim_alert_history: lista (ts_ms, symbol, history_tf) para simular fetch_history()
    # del screener. Cada scan recomputa counts_history a partir de las alertas emitidas
    # en las últimas HISTORY_HOURS, y se la pasa a classify() para aplicar
    # LATE_REPEAT_PENALTY igual que en producción.
    sim_alert_history = []
    alerts = []
    bench = Benchmark(klines, cfg)
    regime = MarketRegime(klines, cfg)
    if regime.enabled and not regime.active:
        print(f"    [regime] AVISO: sin klines 1d de {regime.symbol} → filtro inerte")
    _trail_ms = bench.trailing_days * 86400 * 1000
    _bars_4h_trail = bench.trailing_days * 6  # 6 barras de 4h por día

    def _rs_fields(sym, ts_ms, idx_4h, entry, outcomes):
        """Retorno relativo al benchmark: lo mismo que ya medimos, pero descontando
        lo que hizo el mercado en la MISMA ventana. rs>0 = le ganó a BTC."""
        out = {"bench_ret_1d": None, "bench_ret_7d": None,
               "rs_1d": None, "rs_7d": None, "rs_trailing": None}
        if not bench.active or not entry:
            return out
        for name, off_min, key_b, key_rs in (("price_1d", 1440, "bench_ret_1d", "rs_1d"),
                                             ("price_7d", 10080, "bench_ret_7d", "rs_7d")):
            b = bench.ret(ts_ms, ts_ms + off_min * 60 * 1000)
            out[key_b] = b
            px = outcomes.get(name)
            if b is not None and px:
                out[key_rs] = (float(px) / entry - 1) - b
        # Fuerza relativa PREVIA a la alerta (candidata a feature de scoring: mide si
        # la moneda ya venía ganándole al mercado antes de disparar).
        b_prev = bench.ret(ts_ms - _trail_ms, ts_ms)
        if b_prev is not None:
            df_4h = klines.get(sym, {}).get("4h")
            j = idx_4h - _bars_4h_trail
            if df_4h is not None and j >= 0:
                prev = float(df_4h["close"].iloc[j])
                if prev:
                    out["rs_trailing"] = (entry / prev - 1) - b_prev
        return out

    for (i, ts_ms, sym, tf_data, idx_4h) in candidates:
        # Ventana móvil: descartar alertas más viejas que HISTORY_HOURS para que counts_history
        # refleje sólo lo que el screener real vería en su fetch_history().
        cutoff_ms = ts_ms - history_window_ms
        if sim_alert_history and sim_alert_history[0][0] < cutoff_ms:
            sim_alert_history = [h for h in sim_alert_history if h[0] >= cutoff_ms]
        counts_history = {}
        for (_, s, h) in sim_alert_history:
            counts_history[(s, h)] = counts_history.get((s, h), 0) + 1

        alert = classify(sym, tf_data, cfg, counts_history,
                         regime_up=regime.up(ts_ms) if regime.active else None)
        if alert is None:
            continue

        key = (sym, alert["history_tf"])
        cooldown_ms = cooldown_min_by_state.get(alert["history_tf"], 60) * 60 * 1000
        if key in last_alert_ts and (ts_ms - last_alert_ts[key]) < cooldown_ms:
            continue
        last_alert_ts[key] = ts_ms

        # Registrar en historia simulada para la próxima scan (LATE_REPEAT)
        sim_alert_history.append((ts_ms, sym, alert["history_tf"]))

        oc_key = (sym, idx_4h)
        if outcomes_cache is not None and oc_key in outcomes_cache:
            outcomes = outcomes_cache[oc_key]
        else:
            df_4h = klines[sym]["4h"]
            outcomes = calculate_outcomes(df_4h, idx_4h, alert["price"])
            if outcomes_cache is not None:
                outcomes_cache[oc_key] = outcomes

        alert_record = {
            "alerted_at": datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc).isoformat(),
            "symbol": sym, "signal_type": alert["history_tf"],
            "label": alert["label"], "score": alert["score"],
            "bucket": alert["bucket"], "timeframe": alert["timeframe"],
            "entry_price": alert["price"], "ref_price": alert["ref_price"],
            "obv_slope": alert.get("obv_slope"),
            "cvd_ratio": alert.get("cvd_ratio"),
            "recent_long_ok": alert.get("recent_long_ok"),
            "htf_1d_up": alert.get("htf_1d_up"),
            "htf_1w_up": alert.get("htf_1w_up"),
            "candle_status": alert.get("candle_status"),
            # features extendidas (diagnóstico de scoring gap)
            "vol_ratio": alert.get("vol_ratio"),
            "bb_width": alert.get("bb_width"),
            "vol_growth": alert.get("vol_growth"),
            "dist_to_res": alert.get("dist_to_res"),
            "breakout_distance": alert.get("breakout_distance"),
            "bars_since_break": alert.get("bars_since_break"),
            "riding_gain": alert.get("riding_gain"),
            # features candidatas Fase A (separabilidad PREBREAK banda baja)
            "width_expansion": alert.get("width_expansion"),
            "atr_pct": alert.get("atr_pct"),
            "atr_pct_1d": alert.get("atr_pct_1d"),
            "close_change_curr": alert.get("close_change_curr"),
            "bars_since_major_max": alert.get("bars_since_major_max"),
            **outcomes,
            **_rs_fields(sym, ts_ms, idx_4h, alert["price"], outcomes),
        }
        if audit_mode and alert.get("breakdown"):
            alert_record["breakdown"] = alert["breakdown"]
        alerts.append(alert_record)
    return alerts


def simulate(cfg, klines, start_dt, end_dt, snapshot_pairs=None,
             scan_interval_min=SCAN_INTERVAL_MIN, derivatives=None, prepared=None,
             analyze_cache=None, outcomes_cache=None, audit_mode=False):
    print(f"\n  Simulando {(end_dt - start_dt).total_seconds() / 3600:.0f}h con scans cada {scan_interval_min}min...")

    cooldown_min_by_state = cfg.g("cooldowns_minutes")
    HISTORY_HOURS = cfg.g("history", "HISTORY_HOURS", default=8)
    history_window_ms = HISTORY_HOURS * 3600 * 1000

    scan_ts = []
    cur = start_dt
    while cur <= end_dt:
        scan_ts.append(int(cur.timestamp() * 1000))
        cur += timedelta(minutes=scan_interval_min)

    akey = _analyze_key(cfg)
    if analyze_cache is not None and akey in analyze_cache:
        candidates = analyze_cache[akey]
        print(f"    [analyze] Reutilizando {len(candidates)} candidatos cacheados ({len(scan_ts)} scans)")
    else:
        print(f"    [analyze] Extrayendo candidatos ({len(scan_ts)} scans × {len(klines)} pares)...")
        candidates = _build_candidates(klines, prepared, cfg, scan_ts, snapshot_pairs, derivatives)
        print(f"    [analyze] {len(candidates)} candidatos encontrados")
        if analyze_cache is not None:
            analyze_cache[akey] = candidates

    # ── Instrumentación PASO 1: trayectorias de features por-barra (gated, aditivo) ──
    # Tap acá (no en _build_candidates) para emitir aunque candidates venga de analyze_cache.
    # candidates ya trae tf_data de TODO símbolo×scan con datos válidos (pre-classify/cooldown).
    if _TRACE_OUT is not None:
        def _flat(v):
            # numpy.int64/bool_ NO subclasean int/bool → default=str los volvería strings y
            # envenenaría el análisis offline. .item() devuelve nativo int/float/bool de una.
            return v.item() if isinstance(v, np.generic) else v
        n_trace = 0
        with open(_TRACE_OUT, "w", encoding="utf-8") as ft:
            for (i, ts_ms, sym, tf_data, idx_4h) in candidates:
                if _TRACE_SET != "ALL" and sym not in _TRACE_SET:
                    continue
                row = {"ts_ms": ts_ms,
                       "iso": datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc).isoformat(),
                       "sym": sym}
                for tf, pref in (("1h", "h1"), ("4h", "h4"), ("1d", "d1"), ("1w", "w1")):
                    d = tf_data.get(tf)
                    if d:
                        for k, val in d.items():
                            row[f"{pref}_{k}"] = _flat(val)
                ft.write(json.dumps(row, default=str) + "\n")
                n_trace += 1
        print(f"    [trace] {n_trace} filas escritas a {_TRACE_OUT}")

    alerts = _classify_pass(candidates, cfg, cooldown_min_by_state, history_window_ms,
                            klines, outcomes_cache, audit_mode=audit_mode)
    print(f"    Total alertas simuladas: {len(alerts)}")
    return alerts


# ════════════════════════════════════════════════════════════════════════════
# ANÁLISIS Y RESUMEN
# ════════════════════════════════════════════════════════════════════════════

def pct_move(end, start):
    if end is None or start is None or start == 0:
        return None
    return (end / start - 1) * 100


def _avg(xs):
    xs = [x for x in xs if x is not None]
    return sum(xs) / len(xs) if xs else None


def _median(xs):
    xs = sorted(x for x in xs if x is not None)
    if not xs:
        return None
    n = len(xs)
    return xs[n // 2] if n % 2 else (xs[n // 2 - 1] + xs[n // 2]) / 2


def tail_rates(xs, tail):
    """(P(x >= +tail), P(x <= -tail), ratio sube/baja, es_cota).

    Si no hubo ni una caída grande el ratio sería infinito; en vez de eso se usa media
    observación (corrección de continuidad) y se devuelve es_cota=True, que se imprime
    como ">N". Con n chico eso pasa seguido y un "inf" haría ver ventaja donde no la hay.
    """
    xs = [x for x in xs if x is not None]
    if not xs:
        return None
    up = sum(1 for x in xs if x >= tail) / len(xs)
    dn = sum(1 for x in xs if x <= -tail) / len(xs)
    if dn > 0:
        return up, dn, up / dn, False
    if up > 0:
        return up, dn, up / (0.5 / len(xs)), True
    return up, dn, None, False


def universe_tail_baseline(klines, cfg, start_dt, end_dt):
    """Distribución de exceso 7d de TODO el universo en la misma ventana.

    Es el denominador honesto del ratio de colas: sin esto un ratio de 1.8 parece bueno
    cuando el universo ya lo regala. Grilla por símbolo cada BASELINE_STEP_HOURS, forward
    7d sobre 4h y neto del benchmark — el mismo cálculo que rs_7d, para que sea comparable.
    """
    e_cfg = cfg.raw.get("evaluation") or {}
    if not e_cfg.get("BASELINE_ENABLED", True):
        return None
    bench = Benchmark(klines, cfg)
    if not bench.active:
        return None
    step_ms = max(1, int(e_cfg.get("BASELINE_STEP_HOURS", 24))) * 3600 * 1000
    fwd_ms = 10080 * 60 * 1000                      # 7d, igual que price_7d
    # La grilla cubre la MISMA ventana de scan que las alertas; el forward sale de las
    # klines, que se descargan más allá de end_dt justamente para poder medir outcomes.
    # (Restarle 7d a end_dt dejaba la grilla vacía en corridas de 1 semana.)
    t0 = int(start_dt.timestamp() * 1000)
    t1 = int(end_dt.timestamp() * 1000)
    if t1 <= t0:
        return None
    out = []
    for sym, tfs in klines.items():
        if sym == bench.symbol:
            continue
        df = tfs.get("4h")
        if df is None or len(df) == 0:
            continue
        ct = df["close_time"].values.astype(np.int64)
        close = df["close"].values.astype(float)
        for ts in range(t0, t1 + 1, step_ms):
            if ct[-1] < ts + fwd_ms:
                break                               # sin forward completo: no inventar
            i = int(np.searchsorted(ct, np.int64(ts), side="right")) - 1
            j = int(np.searchsorted(ct, np.int64(ts + fwd_ms), side="right")) - 1
            if i < 0 or j <= i or close[i] == 0:
                continue
            b = bench.ret(ts, ts + fwd_ms)
            if b is not None:
                out.append(close[j] / close[i] - 1 - b)
    return out or None


def summarize_tails(alerts, cfg, baseline=None):
    """★ Métrica de cabecera: ¿el bucket mejora el RATIO sube/baja del universo?

    Reemplaza al CATCH RATE como número principal. Medido en ago-2026: subir el corte de
    score lleva P(+30%) de 2.0% a 4.9% y P(-30%) de 1.1% a 2.7% — el ratio queda en ~1.8,
    que es exactamente el que el universo regala. Detectar movers = detectar volatilidad, y
    la volatilidad es simétrica; el CATCH RATE premia justo eso. Lo que sí es ventaja:
    lift > 1 (el bucket mejora el ratio del universo) y una MEDIANA de exceso que suba.
    """
    if cfg is None:
        return
    e_cfg = cfg.raw.get("evaluation") or {}
    tail = float(e_cfg.get("TAIL_PCT", 0.30))
    cost = round_trip_cost(cfg)
    con_rs = [a for a in alerts if a.get("rs_7d") is not None]
    if not con_rs:
        return

    base = tail_rates(baseline, tail) if baseline else None
    base_ratio = base[2] if base else None

    def _r_txt(t):
        """ratio formateado; '>' marca cota inferior (no hubo cola izquierda)."""
        if t is None or t[2] is None:
            return f"{'—':>8}"
        return f"{('>' if t[3] else '') + format(t[2], '.2f'):>8}"

    print(f"\n★ COLAS — ¿el bucket aporta DIRECCIÓN o solo volatilidad? "
          f"(exceso 7d vs {cfg.raw.get('benchmark', {}).get('SYMBOL', 'BTCUSDT')}, "
          f"umbral ±{tail*100:.0f}%, alertas netas de costos)")
    print(f"  {'':<10} {'n':>6} {'P(+)':>8} {'P(-)':>8} {'ratio':>8} {'lift':>7} "
          f"{'exc medio':>11} {'exc mediana':>12}")
    for b in ["BEST", "STRONG", "WATCH", "TODAS"]:
        items = con_rs if b == "TODAS" else [a for a in con_rs if a["bucket"] == b]
        if not items:
            continue
        exc = [a["rs_7d"] - cost for a in items]
        t = tail_rates(exc, tail)
        if not t:
            continue
        up, dn, ratio, _ = t
        lift = (ratio / base_ratio) if (ratio is not None and base_ratio) else None
        l_txt = f"{lift:>6.2f}x" if lift is not None else f"{'—':>7}"
        print(f"  {b:<10} {len(items):>6} {up*100:>7.2f}% {dn*100:>7.2f}% {_r_txt(t)} {l_txt} "
              f"{_avg(exc)*100:>+10.2f}% {_median(exc)*100:>+11.2f}%")

    if base:
        print(f"  {'-'*10} {'-'*6} {'-'*8} {'-'*8} {'-'*8} {'-'*7} {'-'*11} {'-'*12}")
        print(f"  {'universo':<10} {len(baseline):>6} {base[0]*100:>7.2f}% {base[1]*100:>7.2f}% "
              f"{_r_txt(base)} {'1.00x':>7} {_avg(baseline)*100:>+10.2f}% "
              f"{_median(baseline)*100:>+11.2f}%")
        print(f"  lift = ratio del bucket / ratio del universo. lift ≤ 1.00 → el bucket "
              f"NO aporta dirección,\n  solo volatilidad (más cola derecha Y más cola "
              f"izquierda). Mirar también la mediana.")
    else:
        print(f"  (sin baseline del universo: el ratio no se puede juzgar solo — "
              f"el universo ya trae ~1.5)")


def summarize_vs_benchmark(alerts, cfg):
    """Retorno REALIZADO a 7d: bruto, neto de costos y contra el benchmark.

    El resto del resumen mide max_7d (el pico, o sea el mejor caso). Esta tabla mide
    lo que efectivamente queda al cierre de la ventana, descuenta costos, y lo compara
    con lo que hizo el mercado en la MISMA ventana. rs>0 = la alerta le ganó a BTC.
    Sin esta comparación un mes alcista hace ver bien a cualquier screener long-only.
    """
    if cfg is None:
        return
    b_cfg = cfg.raw.get("benchmark") or {}
    cost = round_trip_cost(cfg)
    con_rs = [a for a in alerts if a.get("rs_7d") is not None]
    if not b_cfg.get("ENABLED", False) or not con_rs:
        if cost:
            print(f"\nCostos: round-trip {cost*100:.2f}% (fee+slippage x2) — "
                  f"benchmark sin datos, no se puede comparar contra el mercado")
        return

    print(f"\nRetorno realizado 7d vs {b_cfg.get('SYMBOL', 'BTCUSDT')} "
          f"(costos round-trip {cost*100:.2f}%):")
    print(f"  {'':<8} {'n':>5} {'bruto':>9} {'neto':>9} {'benchmark':>11} {'exceso':>9} {'gana-bench':>11}")
    for b in ["BEST", "STRONG", "WATCH", "TODAS"]:
        items = con_rs if b == "TODAS" else [a for a in con_rs if a["bucket"] == b]
        if not items:
            continue
        brutos = [a["price_7d"] / a["entry_price"] - 1 for a in items
                  if a.get("price_7d") and a.get("entry_price")]
        if not brutos:
            continue
        bench = [a["bench_ret_7d"] for a in items]
        # El exceso se mide neto: los costos los paga la alerta, no el benchmark.
        exceso = [a["rs_7d"] - cost for a in items]
        gana = sum(1 for x in exceso if x > 0) * 100 / len(exceso)
        print(f"  {b:<8} {len(items):>5} {_avg(brutos)*100:>+8.2f}% "
              f"{(_avg(brutos)-cost)*100:>+8.2f}% {_avg(bench)*100:>+10.2f}% "
              f"{_avg(exceso)*100:>+8.2f}% {gana:>10.0f}%")

    trail = [a for a in con_rs if a.get("rs_trailing") is not None]
    if len(trail) >= 30:
        # ¿La fuerza relativa PREVIA predice la posterior? Si separa, es candidata a
        # entrar al scoring; si no, confirma que la entrada no tiene ventaja.
        print("\n  Fuerza relativa previa a la alerta → exceso posterior:")
        fuerte = [a["rs_7d"] - cost for a in trail if a["rs_trailing"] > 0]
        debil = [a["rs_7d"] - cost for a in trail if a["rs_trailing"] <= 0]
        for nombre, grupo in (("ya le ganaba a BTC", fuerte), ("venía perdiendo", debil)):
            if grupo:
                print(f"    {nombre:<22} n={len(grupo):>4}  exceso {_avg(grupo)*100:+6.2f}%")


def summarize(alerts, label="resultados", cfg=None, baseline=None):
    print()
    print("═" * 70)
    print(f" {label.upper()}")
    print("═" * 70)
    if not alerts:
        print("  (no hubo alertas)")
        return
    print(f"\nTotal alertas: {len(alerts)}")

    from collections import Counter
    by_type = Counter(a["signal_type"] for a in alerts)
    print("\nPor tipo:")
    for t, n in by_type.most_common():
        print(f"  {t:<10} {n:>4}  ({n*100//len(alerts)}%)")

    by_bucket = Counter(a["bucket"] for a in alerts)
    print("\nPor bucket:")
    for b in ["BEST", "STRONG", "WATCH"]:
        items = [a for a in alerts if a["bucket"] == b]
        if not items:
            continue
        moves = [pct_move(a["max_high_7d"], a["entry_price"]) for a in items]
        moves = [m for m in moves if m is not None]
        drops = [pct_move(a["min_low_7d"], a["entry_price"]) for a in items]
        drops = [d for d in drops if d is not None]
        wins = sum(1 for m in moves if m >= 2.0)
        avg_max = sum(moves) / len(moves) if moves else 0
        avg_dd = sum(drops) / len(drops) if drops else 0
        winrate = wins * 100 / len(moves) if moves else 0
        print(f"  {b:<6} {len(items):>4} — max_7d: {avg_max:+5.2f}%, drawdown: {avg_dd:+5.2f}%, "
              f"win >2%: {winrate:.0f}%")

    summarize_tails(alerts, cfg, baseline)
    summarize_vs_benchmark(alerts, cfg)
    if cfg is not None:
        summarize_portfolio(alerts, cfg)

    print("\nCVD análisis:")
    cvd_groups = {"bullish (>+0.05)": [], "neutral": [], "bearish (<-0.05)": []}
    for a in alerts:
        c = a.get("cvd_ratio") or 0
        m = pct_move(a["max_high_7d"], a["entry_price"])
        if m is None:
            continue
        if c >= 0.05:
            cvd_groups["bullish (>+0.05)"].append(m)
        elif c <= -0.05:
            cvd_groups["bearish (<-0.05)"].append(m)
        else:
            cvd_groups["neutral"].append(m)
    for name, moves in cvd_groups.items():
        if moves:
            print(f"  CVD {name:<20} n={len(moves):>3}  avg max 7d: {sum(moves)/len(moves):+5.2f}%")

    print("\nOBV análisis:")
    obv_groups = {"rising (>+0.05)": [], "neutral": [], "falling (<-0.05)": []}
    for a in alerts:
        o = a.get("obv_slope") or 0
        m = pct_move(a["max_high_7d"], a["entry_price"])
        if m is None:
            continue
        if o >= 0.05:
            obv_groups["rising (>+0.05)"].append(m)
        elif o <= -0.05:
            obv_groups["falling (<-0.05)"].append(m)
        else:
            obv_groups["neutral"].append(m)
    for name, moves in obv_groups.items():
        if moves:
            print(f"  OBV {name:<20} n={len(moves):>3}  avg max 7d: {sum(moves)/len(moves):+5.2f}%")

    print("\nrecent_long_ok análisis:")
    for ok in (True, False):
        items = [a for a in alerts if a.get("recent_long_ok") == ok]
        moves = [pct_move(a["max_high_7d"], a["entry_price"]) for a in items]
        moves = [m for m in moves if m is not None]
        drops = [pct_move(a["min_low_7d"], a["entry_price"]) for a in items]
        drops = [d for d in drops if d is not None]
        if moves:
            print(f"  {str(ok):<5} n={len(moves):>3}  avg max 7d: {sum(moves)/len(moves):+5.2f}%, "
                  f"drawdown: {sum(drops)/len(drops):+5.2f}%")

    ranked = [(pct_move(a["max_high_7d"], a["entry_price"]) or -999, a) for a in alerts]
    ranked.sort(key=lambda x: x[0], reverse=True)
    print("\nTOP 30 movers detectados (alert-derived, SESGADA — rankea por fila, no por símbolo):")
    print(f"  {'#':>3} {'Symbol':<14} {'Type':<10} {'Score':>5} {'Bucket':<7} {'max7d':>8} "
          f"{'cvd':>7} {'obv':>7} {'long_ok':>7}")
    for i, (move, a) in enumerate(ranked[:30], start=1):
        cvd = a.get("cvd_ratio") or 0
        obv = a.get("obv_slope") or 0
        print(f"  {i:>3} {a['symbol']:<14} {a['signal_type']:<10} {a['score']:>5} {a['bucket']:<7} "
              f"{move:>+7.2f}% {cvd:>+6.2f} {obv:>+6.2f} {str(a.get('recent_long_ok')):>7}")

    # Distribución de scores por signal_type (entender dónde aterrizan los explosivos)
    print("\nDistribución de scores por signal_type (todos los alerts):")
    from collections import defaultdict
    buckets_by_type = defaultdict(lambda: defaultdict(int))
    for a in alerts:
        buckets_by_type[a["signal_type"]][a["score"]] += 1
    for st in sorted(buckets_by_type.keys()):
        scores = buckets_by_type[st]
        score_list = sorted(scores.keys())
        total = sum(scores.values())
        score_str = " ".join(f"{s}:{scores[s]}" for s in score_list)
        print(f"  {st:<10} (n={total:>3})  {score_str}")

    # Análisis de los top 30: cuántos son BEST/STRONG/WATCH y por signal_type
    print("\nTop 30 movers — desglose:")
    top30 = [a for _, a in ranked[:30]]
    bucket_counts = {"BEST": 0, "STRONG": 0, "WATCH": 0}
    type_counts = defaultdict(int)
    for a in top30:
        bucket_counts[a["bucket"]] += 1
        type_counts[a["signal_type"]] += 1
    print(f"  Buckets: BEST={bucket_counts['BEST']}, STRONG={bucket_counts['STRONG']}, WATCH={bucket_counts['WATCH']}")
    print(f"  Tipos:   {dict(type_counts)}")
    avg_score_top30 = sum(a["score"] for a in top30) / len(top30) if top30 else 0
    print(f"  Score promedio top 30: {avg_score_top30:.1f}")
    avg_move_top30 = sum(m for m, _ in ranked[:30]) / 30 if ranked else 0
    print(f"  Move promedio top 30: +{avg_move_top30:.2f}%")

    # ── Vista POR SÍMBOLO (mejor bucket alcanzado) — métrica honesta de bucketing ──
    # La tabla de arriba rankea CADA fila de alerta por su propio move realizado y no
    # deduplica por símbolo: entradas tardías del mismo símbolo desplazan a sus filas
    # BEST tempranas. Acá: 1 fila por símbolo, rankeado por su mejor move, contando el
    # mejor bucket que ese símbolo alcanzó en CUALQUIER alerta.
    _BUCKET_RANK = {"BEST": 3, "STRONG": 2, "WATCH": 1}
    by_symbol = defaultdict(list)
    for a in alerts:
        by_symbol[a["symbol"]].append(a)
    sym_rows = []
    for sym, items in by_symbol.items():
        moves = [pct_move(a["max_high_7d"], a["entry_price"]) for a in items]
        moves = [m for m in moves if m is not None]
        if not moves:
            continue
        best_move = max(moves)
        best_bucket = max((it["bucket"] for it in items), key=lambda b: _BUCKET_RANK.get(b, 0))
        # alerta que alcanzó ese bucket más temprano (para mostrar señal/score representativos)
        best_alert = min((it for it in items if it["bucket"] == best_bucket),
                         key=lambda it: it.get("alerted_at") or "")
        sym_rows.append((best_move, best_bucket, best_alert, len(items)))
    sym_rows.sort(key=lambda x: x[0], reverse=True)

    print("\nTOP 30 movers POR SÍMBOLO (mejor bucket alcanzado — métrica honesta):")
    print(f"  {'#':>3} {'Symbol':<14} {'BestBkt':<7} {'Type':<10} {'Score':>5} {'bestMove':>9} {'#alerts':>7}")
    for i, (move, bkt, a, nalerts) in enumerate(sym_rows[:30], start=1):
        print(f"  {i:>3} {a['symbol']:<14} {bkt:<7} {a['signal_type']:<10} {a['score']:>5} "
              f"{move:>+8.2f}% {nalerts:>7}")
    top30_sym = sym_rows[:30]
    sym_bucket_counts = {"BEST": 0, "STRONG": 0, "WATCH": 0}
    for _, bkt, _, _ in top30_sym:
        sym_bucket_counts[bkt] += 1
    print(f"  Buckets (por símbolo, mejor alcanzado): BEST={sym_bucket_counts['BEST']}, "
          f"STRONG={sym_bucket_counts['STRONG']}, WATCH={sym_bucket_counts['WATCH']}")


def simulate_portfolio(alerts, cfg):
    """Cartera con capital finito: qué pasa si además de detectar, OPERÁS las alertas.

    El screener emite ~1200 alertas BEST+STRONG/mes sin decir cuánto poner. A 7d de hold
    serían ~210 posiciones simultáneas — y como las alts van todas juntas, eso no es
    diversificación sino apalancamiento a un solo factor. Acá se impone un tope real de
    posiciones concurrentes: las alertas que llegan con la cartera llena se DESCARTAN
    (es lo que pasaría de verdad, no se puede comprar sin capital libre).

    Sizing por riesgo: cada posición arriesga RISK_PCT_PER_TRADE del equity, asumiendo
    una pérdida de STOP_PCT_FOR_SIZING → nominal = equity * riesgo / stop.
    Salida: al cierre de HOLD_DAYS (price_7d), neta de costos.
    Devuelve dict con métricas, o None si falta config/datos.
    """
    p = cfg.raw.get("portfolio") or {}
    if not p.get("ENABLED", False):
        return None
    buckets = set(p.get("BUCKETS") or ["BEST", "STRONG"])
    max_conc = int(p.get("MAX_CONCURRENT", 10))
    max_per_sym = int(p.get("MAX_PER_SYMBOL", 1))
    risk_pct = float(p.get("RISK_PCT_PER_TRADE", 0.01))
    stop_sizing = float(p.get("STOP_PCT_FOR_SIZING", 0.10))
    hold_days = float(p.get("HOLD_DAYS", 7))
    cost = round_trip_cost(cfg)
    hold_ms = int(hold_days * 86400 * 1000)

    ops = []
    for a in alerts:
        if a.get("bucket") not in buckets or not a.get("entry_price") or not a.get("price_7d"):
            continue
        try:
            ts = int(datetime.fromisoformat(a["alerted_at"]).timestamp() * 1000)
        except (ValueError, KeyError):
            continue
        ops.append((ts, a))
    ops.sort(key=lambda x: x[0])
    if not ops:
        return None

    equity = 1.0
    abiertas = []        # (cierre_ms, symbol, nominal, ret_neto)
    tomadas, descartadas_llena, descartadas_sym = 0, 0, 0
    curva, cerradas = [], []

    def cerrar_hasta(ts):
        nonlocal equity
        vivas = []
        for cierre_ms, sym, nominal, ret in abiertas:
            if cierre_ms <= ts:
                equity += nominal * ret
                cerradas.append(ret)
                curva.append((cierre_ms, equity))
            else:
                vivas.append((cierre_ms, sym, nominal, ret))
        abiertas[:] = vivas

    for ts, a in ops:
        cerrar_hasta(ts)
        if len(abiertas) >= max_conc:
            descartadas_llena += 1
            continue
        if max_per_sym and sum(1 for o in abiertas if o[1] == a["symbol"]) >= max_per_sym:
            descartadas_sym += 1
            continue
        ret = (a["price_7d"] / a["entry_price"] - 1) - cost
        # Nominal por riesgo, topeado al equity libre (sin apalancamiento).
        libre = max(0.0, equity - sum(o[2] for o in abiertas))
        nominal = min(equity * risk_pct / stop_sizing, libre)
        if nominal <= 0:
            descartadas_llena += 1
            continue
        abiertas.append((ts + hold_ms, a["symbol"], nominal, ret))
        tomadas += 1
    cerrar_hasta(float("inf"))

    if not cerradas:
        return None
    dias = (ops[-1][0] - ops[0][0]) / 86400000 or 1
    pico, max_dd = 1.0, 0.0
    for _, eq in curva:
        pico = max(pico, eq)
        max_dd = min(max_dd, eq / pico - 1)
    return {
        "equity_final": equity, "ret_total": equity - 1.0,
        "ret_mensual": (equity - 1.0) / dias * 30, "max_drawdown": max_dd,
        "tomadas": tomadas, "descartadas_llena": descartadas_llena,
        "descartadas_sym": descartadas_sym, "elegibles": len(ops),
        "ret_medio": sum(cerradas) / len(cerradas),
        "win": sum(1 for r in cerradas if r > 0) * 100 / len(cerradas),
        "dias": dias, "max_conc": max_conc, "cost": cost,
    }


def summarize_portfolio(alerts, cfg):
    r = simulate_portfolio(alerts, cfg)
    if not r:
        return
    print(f"\nCartera simulada (tope {r['max_conc']} posiciones, "
          f"costos {r['cost']*100:.2f}% round-trip):")
    print(f"  Alertas elegibles      : {r['elegibles']}")
    print(f"  Operadas               : {r['tomadas']}  "
          f"(descartadas: {r['descartadas_llena']} sin capital, "
          f"{r['descartadas_sym']} ya en cartera)")
    print(f"  Retorno por trade      : {r['ret_medio']*100:+.2f}%   win {r['win']:.1f}%")
    print(f"  Retorno del CAPITAL    : {r['ret_total']*100:+.2f}% en {r['dias']:.0f}d  "
          f"→ {r['ret_mensual']*100:+.2f}%/mes")
    print(f"  Max drawdown           : {r['max_drawdown']*100:.2f}%")


def compare_runs(alerts_a, label_a, alerts_b, label_b, period_days=7, cfg=None,
                 baseline=None):
    print()
    print("═" * 70)
    print(f" COMPARACIÓN: {label_a}  vs  {label_b}  ({period_days} días)")
    print("═" * 70)
    e_cfg = (cfg.raw.get("evaluation") or {}) if cfg is not None else {}
    tail = float(e_cfg.get("TAIL_PCT", 0.30))
    cost = round_trip_cost(cfg) if cfg is not None else 0.0
    _b = tail_rates(baseline, tail) if baseline else None
    base_ratio = _b[2] if _b else None

    def stats(alerts):
        if not alerts:
            return {"n": 0, "avg_max_24h": 0, "avg_drawdown": 0, "win_2pct": 0, "win_5pct": 0,
                    "best_n": 0, "strong_n": 0, "breakout_n": 0,
                    "best_max": 0, "strong_max": 0, "explosivos_n": 0,
                    "best_per_day": 0, "best_win_5pct": 0, "best_win_10pct": 0,
                    "top30_in_best": 0, "top30_in_best_strong": 0,
                    "best_dd": 0, "best_rr": 0,
                    "ratio_bs": None, "lift_bs": None, "exc_med_bs": None,
                    "ratio_best": None, "exc_med_best": None}
        moves = [pct_move(a["max_high_7d"], a["entry_price"]) for a in alerts]
        moves = [m for m in moves if m is not None]
        drops = [pct_move(a["min_low_7d"], a["entry_price"]) for a in alerts]
        drops = [d for d in drops if d is not None]
        best = [a for a in alerts if a["bucket"] == "BEST"]
        strong = [a for a in alerts if a["bucket"] == "STRONG"]
        breakouts = [a for a in alerts if a["signal_type"] == "BREAKOUT"]
        best_moves = [pct_move(a["max_high_7d"], a["entry_price"]) for a in best]
        best_moves = [m for m in best_moves if m is not None]
        best_drops = [pct_move(a["min_low_7d"], a["entry_price"]) for a in best]
        best_drops = [d for d in best_drops if d is not None]
        strong_moves = [pct_move(a["max_high_7d"], a["entry_price"]) for a in strong]
        strong_moves = [m for m in strong_moves if m is not None]
        explosivos = [m for m in moves if m >= 20]

        # Top 30 movers — cuántos están en BEST y BEST+STRONG
        ranked = sorted(alerts,
                        key=lambda a: pct_move(a["max_high_7d"], a["entry_price"]) or -999,
                        reverse=True)
        top30 = ranked[:30]
        top30_best = sum(1 for a in top30 if a["bucket"] == "BEST")
        top30_best_strong = sum(1 for a in top30 if a["bucket"] in ("BEST", "STRONG"))

        # Win rate específico de BEST
        best_win_5 = sum(1 for m in best_moves if m >= 5) * 100 / len(best_moves) if best_moves else 0
        best_win_10 = sum(1 for m in best_moves if m >= 10) * 100 / len(best_moves) if best_moves else 0

        # Avg drawdown y R/R en BEST
        best_avg_dd = sum(best_drops) / len(best_drops) if best_drops else 0
        best_avg_max = sum(best_moves) / len(best_moves) if best_moves else 0
        best_rr = abs(best_avg_max / best_avg_dd) if best_avg_dd != 0 else 0

        # Colas: la métrica que reemplaza al CATCH RATE. Se mide sobre BEST+STRONG (lo
        # que se opera) y sobre BEST solo. Ver summarize_tails() para el porqué.
        def _cola(items):
            exc = [a["rs_7d"] - cost for a in items if a.get("rs_7d") is not None]
            t = tail_rates(exc, tail)
            return (t[2] if t else None), _median(exc)

        ratio_bs, exc_med_bs = _cola([a for a in alerts if a["bucket"] in ("BEST", "STRONG")])
        ratio_best, exc_med_best = _cola(best)

        return {
            "n": len(alerts),
            "ratio_bs": ratio_bs, "exc_med_bs": exc_med_bs,
            "lift_bs": (ratio_bs / base_ratio) if (ratio_bs and base_ratio) else None,
            "ratio_best": ratio_best, "exc_med_best": exc_med_best,
            "avg_max_24h": sum(moves) / len(moves) if moves else 0,
            "avg_drawdown": sum(drops) / len(drops) if drops else 0,
            "win_2pct": sum(1 for m in moves if m >= 2) * 100 / len(moves) if moves else 0,
            "win_5pct": sum(1 for m in moves if m >= 5) * 100 / len(moves) if moves else 0,
            "best_n": len(best),
            "strong_n": len(strong),
            "breakout_n": len(breakouts),
            "best_max": best_avg_max,
            "strong_max": sum(strong_moves) / len(strong_moves) if strong_moves else 0,
            "explosivos_n": len(explosivos),
            "best_per_day": (len(best) / period_days) if period_days > 0 else 0,
            "best_win_5pct": best_win_5,
            "best_win_10pct": best_win_10,
            "top30_in_best": top30_best,
            "top30_in_best_strong": top30_best_strong,
            "best_dd": best_avg_dd,
            "best_rr": best_rr,
        }

    s_a = stats(alerts_a)
    s_b = stats(alerts_b)
    print(f"  {'Métrica':<32} {label_a:>15} {label_b:>15}")
    print(f"  {'-'*32} {'-'*15} {'-'*15}")
    print(f"  {'Total alertas':<32} {s_a['n']:>15} {s_b['n']:>15}")
    print(f"  {'BREAKOUT count':<32} {s_a['breakout_n']:>15} {s_b['breakout_n']:>15}")
    print(f"  {'BEST count':<32} {s_a['best_n']:>15} {s_b['best_n']:>15}")
    print(f"  {'BEST por día (target: 10-20)':<32} {s_a['best_per_day']:>15.1f} {s_b['best_per_day']:>15.1f}")
    print(f"  {'STRONG count':<32} {s_a['strong_n']:>15} {s_b['strong_n']:>15}")
    print(f"  {'Explosivos totales (>20%)':<32} {s_a['explosivos_n']:>15} {s_b['explosivos_n']:>15}")
    print(f"  {'-'*32} {'-'*15} {'-'*15}")
    print(f"  ★ COLAS (lo más importante) — ±{tail*100:.0f}% de exceso 7d, neto de costos:")

    def _f(v, fmt):
        """Celda de 15 caracteres; '—' cuando no hay dato (n insuficiente o sin cola)."""
        return format(v, fmt) if v is not None else f"{'—':>15}"

    def _fp(v):
        """Idem pero en porcentaje, con el % adentro para no correr la columna."""
        return f"{format(v * 100, '+.2f') + '%':>15}" if v is not None else f"{'—':>15}"

    print(f"  {'Ratio sube/baja BEST+STRONG':<32} {_f(s_a['ratio_bs'], '>15.2f')} "
          f"{_f(s_b['ratio_bs'], '>15.2f')}")
    def _fx(v):
        """Lift como '1.25x' en celda de 15."""
        return f"{format(v, '.2f') + 'x':>15}" if v is not None else f"{'—':>15}"

    if base_ratio:
        print(f"  {'  lift vs universo (>1 = aporta)':<32} "
              f"{_fx(s_a['lift_bs'])} {_fx(s_b['lift_bs'])}")
    print(f"  {'Ratio sube/baja BEST':<32} {_f(s_a['ratio_best'], '>15.2f')} "
          f"{_f(s_b['ratio_best'], '>15.2f')}")
    print(f"  {'Exceso MEDIANA BEST+STRONG':<32} {_fp(s_a['exc_med_bs'])} {_fp(s_b['exc_med_bs'])}")
    print(f"  {'Exceso MEDIANA BEST':<32} {_fp(s_a['exc_med_best'])} {_fp(s_b['exc_med_best'])}")
    if base_ratio:
        print(f"  (universo en la misma ventana: ratio {base_ratio:.2f})")
    print(f"  {'-'*32} {'-'*15} {'-'*15}")
    print(f"  CATCH RATE (ojo: recall sobre movers — sube solo con volatilidad,")
    print(f"   no distingue dirección. Usarlo como control de que no se rompió nada):")
    print(f"  {'Top 30 movers en BEST':<32} {s_a['top30_in_best']:>14}/30 {s_b['top30_in_best']:>14}/30")
    print(f"  {'Top 30 movers en BEST+STRONG':<32} {s_a['top30_in_best_strong']:>14}/30 {s_b['top30_in_best_strong']:>14}/30")
    print(f"  {'-'*32} {'-'*15} {'-'*15}")
    print(f"  ★ CALIDAD del bucket BEST:")
    print(f"  {'Avg max 7d BEST':<32} {s_a['best_max']:>+14.2f}% {s_b['best_max']:>+14.2f}%")
    print(f"  {'Avg drawdown BEST':<32} {s_a['best_dd']:>+14.2f}% {s_b['best_dd']:>+14.2f}%")
    print(f"  {'R/R BEST':<32} {s_a['best_rr']:>15.2f} {s_b['best_rr']:>15.2f}")
    print(f"  {'Win >5% en BEST':<32} {s_a['best_win_5pct']:>14.0f}%  {s_b['best_win_5pct']:>14.0f}%")
    print(f"  {'Win >10% en BEST':<32} {s_a['best_win_10pct']:>14.0f}%  {s_b['best_win_10pct']:>14.0f}%")
    print(f"  {'-'*32} {'-'*15} {'-'*15}")
    print(f"  Otros:")
    print(f"  {'Avg max 7d global':<32} {s_a['avg_max_24h']:>+14.2f}% {s_b['avg_max_24h']:>+14.2f}%")
    print(f"  {'Avg max 7d STRONG':<32} {s_a['strong_max']:>+14.2f}% {s_b['strong_max']:>+14.2f}%")
    print(f"  {'Avg drawdown global':<32} {s_a['avg_drawdown']:>+14.2f}% {s_b['avg_drawdown']:>+14.2f}%")
    print(f"  {'Win >2% global':<32} {s_a['win_2pct']:>14.0f}%  {s_b['win_2pct']:>14.0f}%")
    print(f"  {'Win >5% global':<32} {s_a['win_5pct']:>14.0f}%  {s_b['win_5pct']:>14.0f}%")


# ════════════════════════════════════════════════════════════════════════════
# MAIN
# ════════════════════════════════════════════════════════════════════════════

def run_backtest(cfg, weeks, klines, start_dt, end_dt, snapshot_pairs=None,
                 label="run", scan_interval_min=SCAN_INTERVAL_MIN, derivatives=None,
                 prepared_cache=None, analyze_cache=None, outcomes_cache=None,
                 audit_mode=False, baseline_cache=None):
    print(f"\n  >>> Corriendo backtest: {label}")
    key = _indicator_key(cfg)
    if prepared_cache is not None and key in prepared_cache:
        print(f"    [precompute] Reutilizando indicadores cacheados ({len(prepared_cache[key])} pares)")
        prepared = prepared_cache[key]
    else:
        print(f"    [precompute] Calculando indicadores para {len(klines)} pares...")
        n_workers = min(8, len(klines))
        futures_prep = {}
        with ThreadPoolExecutor(max_workers=n_workers) as ex:
            for sym, tfs in klines.items():
                for tf in ("1h", "4h", "1d", "1w"):
                    if tf in tfs:
                        futures_prep[(sym, tf)] = ex.submit(precompute_indicators, tfs[tf], cfg)
        prepared = {}
        for (sym, tf), fut in futures_prep.items():
            prepared.setdefault(sym, {})[tf] = fut.result()
        if prepared_cache is not None:
            prepared_cache[key] = prepared
    alerts = simulate(cfg, klines, start_dt, end_dt, snapshot_pairs,
                      scan_interval_min=scan_interval_min, derivatives=derivatives,
                      prepared=prepared, analyze_cache=analyze_cache,
                      outcomes_cache=outcomes_cache, audit_mode=audit_mode)
    # Baseline del universo: no depende de la cfg de señales, así que entre variantes se
    # calcula una sola vez (la ventana y las klines son las mismas).
    if baseline_cache is not None and "base" in baseline_cache:
        baseline = baseline_cache["base"]
    else:
        baseline = universe_tail_baseline(klines, cfg, start_dt, end_dt)
        if baseline_cache is not None:
            baseline_cache["base"] = baseline
    summarize(alerts, label=label, cfg=cfg, baseline=baseline)
    return alerts


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--weeks", type=int, default=1)
    parser.add_argument("--config", default="config.json")
    parser.add_argument("--compare", nargs=2, metavar=("OLD", "NEW"))
    parser.add_argument("--variants", nargs="+", metavar="CONFIG",
                        help="Compara N configs contra el primero (--variants base.json A.json B.json C.json)")
    parser.add_argument("--ablation", action="store_true")
    parser.add_argument("--out", default=None, help="Path opcional para guardar JSON con resultados")
    parser.add_argument("--max-pairs", type=int, default=MAX_PAIRS)
    parser.add_argument("--scan-interval-min", type=int, default=SCAN_INTERVAL_MIN,
                        help=f"Minutos entre scans simulados (default {SCAN_INTERVAL_MIN}, "
                             f"production cron usa 5min)")
    parser.add_argument("--no-cache", action="store_true",
                        help="Ignora y no escribe caché de disco de klines")
    parser.add_argument("--workers", type=int, default=1,
                        help="Procesos paralelos para candidate building (default 1; "
                             "no usar cuando sweep.py ya paraleliza ventanas)")
    parser.add_argument("--results-dir", default=None,
                        help="Directorio para resultados incrementales por-config (usado por sweep.py). "
                             "Cada variante se escribe al terminar y se salta si ya existe.")
    parser.add_argument("--end-date", default=None,
                        help="YYYY-MM-DD: fin del backtest (default: ahora). Útil para validación multi-ventana.")
    parser.add_argument("--quick", action="store_true",
                        help="Modo dev rápido: fuerza weeks=1 y max-pairs=100. Útil para iterar.")
    parser.add_argument("--cache-dir", default=None,
                        help="Directorio para caché de klines (default: I:\\.backtest_cache). "
                             "Pasar ruta alternativa si se quiere otro disco/ubicación.")
    parser.add_argument("--audit-scoring", action="store_true",
                        help="Incluye breakdown de scoring por alerta en el JSON de salida. "
                             "Necesario para audit_scoring.py.")
    parser.add_argument("--trace-features", nargs="+", metavar="SYM",
                        help="Dumpea trayectorias de features por-barra. Lista de símbolos o 'ALL'.")
    parser.add_argument("--trace-out", default="trace_features.jsonl",
                        help="Path del JSONL de trace (default trace_features.jsonl).")
    parser.add_argument("--portfolio", action="store_true",
                        help="Fuerza portfolio.ENABLED=true: simula cartera con capital "
                             "finito y tope de posiciones concurrentes.")
    parser.add_argument("--regime", action="store_true",
                        help="Fuerza regime_filter.ENABLED=true: aplica el gate risk-on/off "
                             "de mercado. Sin esto se usa lo que diga config.json.")
    parser.add_argument("--no-costs", action="store_true",
                        help="Fuerza costs.ENABLED=false (reporta bruto, como antes).")
    parser.add_argument("--extra-symbols", nargs="+", metavar="SYM", default=None,
                        help="Símbolos extra a forzar dentro del universo (UNION con el top-N/snapshot). "
                             "Útil para escanear movers fuera del universo por volumen sin tocar --max-pairs.")
    args = parser.parse_args()
    _audit = getattr(args, "audit_scoring", False)

    # Overrides de CLI: se registran ANTES de instanciar cualquier Config.
    if args.portfolio:
        _CLI_OVERRIDES["portfolio"] = {"ENABLED": True}
    if args.regime:
        _CLI_OVERRIDES["regime_filter"] = {"ENABLED": True}
    if args.no_costs:
        _CLI_OVERRIDES["costs"] = {"ENABLED": False}

    global CACHE_DIR
    if args.cache_dir:
        CACHE_DIR = Path(args.cache_dir).expanduser()

    if args.quick:
        args.weeks     = min(args.weeks, 1)
        args.max_pairs = min(args.max_pairs, 100)
        print(f"  [quick] weeks={args.weeks}, max-pairs={args.max_pairs}")

    global _NO_CACHE
    _NO_CACHE = args.no_cache
    if _NO_CACHE:
        print("  [cache] Desactivado por --no-cache")

    global _TRACE_SET, _TRACE_OUT
    if args.trace_features:
        _TRACE_SET = "ALL" if args.trace_features == ["ALL"] else set(args.trace_features)
        _TRACE_OUT = Path(args.trace_out)
        print(f"  [trace] Activado → {_TRACE_OUT} "
              f"({'ALL' if _TRACE_SET == 'ALL' else f'{len(_TRACE_SET)} símbolos'})")

    results_dir = None
    if args.results_dir:
        results_dir = Path(args.results_dir)
        results_dir.mkdir(parents=True, exist_ok=True)
    elif args.out and args.variants:
        results_dir = Path(args.out).parent / "results"
        results_dir.mkdir(parents=True, exist_ok=True)
    result_prefix = Path(args.out).stem if args.out else "run"

    if args.end_date:
        end_dt = datetime.strptime(args.end_date, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    else:
        end_dt = datetime.now(timezone.utc).replace(minute=0, second=0, microsecond=0)
    start_dt = end_dt - timedelta(weeks=args.weeks)
    period_days = args.weeks * 7
    print(f"Período backtest: {start_dt} → {end_dt} ({args.weeks} semana(s), {period_days}d)")
    print(f"Scan interval simulado: {args.scan_interval_min} min")

    cfg_main = Config(args.config)
    min_vol = cfg_main.g("general", "MIN_QUOTE_VOLUME")

    snapshot_pairs = None
    snap_symbols = None
    if SUPABASE_KEY:
        print("\n[1/4] Cargando snapshots de Supabase...")
        snapshot_pairs, snap_symbols = get_pairs_from_snapshot(start_dt, end_dt)
        if snap_symbols:
            print(f"  Usando snapshots: {len(snap_symbols)} símbolos únicos en el período")
    if not snap_symbols:
        print("\n[1/4] Sin snapshots; usando top pares actuales por volumen...")
        symbols = get_top_pairs_today(min_vol, top_n=args.max_pairs)
        print(f"  {len(symbols)} pares calificantes hoy (top {args.max_pairs} por volumen)")
    else:
        symbols = list(snap_symbols)[:args.max_pairs]

    if args.extra_symbols:
        base_set = set(symbols)
        added = [s for s in args.extra_symbols if s not in base_set]
        symbols = symbols + added
        print(f"  +extra-symbols: {len(added)} agregados (UNION); universo total {len(symbols)}")
        missing = [s for s in args.extra_symbols if s in base_set]
        if missing:
            print(f"  (ya estaban en el universo base: {' '.join(missing)})")

    # El benchmark y el filtro de régimen leen su símbolo de klines, así que tiene que
    # estar sí o sí en el universo aunque no califique por volumen ni esté en el snapshot.
    _bench_syms = {
        (cfg_main.raw.get("benchmark") or {}).get("SYMBOL", "BTCUSDT"),
        (cfg_main.raw.get("regime_filter") or {}).get("SYMBOL", "BTCUSDT"),
    }
    _bench_add = [s for s in sorted(_bench_syms) if s and s not in set(symbols)]
    if _bench_add:
        symbols = symbols + _bench_add
        print(f"  +benchmark/régimen: {' '.join(_bench_add)} agregado(s) al universo")

    print(f"\n[2/4] Descargando klines históricas...")
    klines = download_all_klines(symbols, start_dt, end_dt)
    if not klines:
        print("ERROR: no se pudo descargar data.")
        sys.exit(1)

    # Determinar si algún cfg que vamos a correr necesita derivatives
    cfg_paths_for_run = [args.config]
    if args.variants:
        cfg_paths_for_run = list(args.variants)
    elif args.compare:
        cfg_paths_for_run = list(args.compare)
    derivatives = None
    print(f"\n[3/4] Ejecutando simulación...")
    all_results = {}
    prepared_cache = {}  # compartido entre cfgs con mismos parámetros de indicadores
    analyze_cache  = {}  # compartido entre cfgs con mismo _analyze_key (variant-independent)
    outcomes_cache = {}  # compartido entre cfgs: outcomes(sym, idx_4h) son variant-independent
    baseline_cache = {}  # baseline de colas del universo: misma ventana para todas las cfgs

    if args.variants:
        # Modo nuevo: --variants base.json A.json B.json C.json
        base_path = args.variants[0]
        variant_paths = args.variants[1:]
        if not variant_paths:
            print("ERROR: --variants requiere al menos 2 archivos (base + 1 variante)")
            sys.exit(1)
        cfg_base = Config(base_path)
        alerts_base = run_backtest(cfg_base, args.weeks, klines, start_dt, end_dt,
                                   snapshot_pairs, label=f"BASE ({Path(base_path).stem})",
                                   scan_interval_min=args.scan_interval_min,
                                   derivatives=derivatives, prepared_cache=prepared_cache,
                                   analyze_cache=analyze_cache, outcomes_cache=outcomes_cache,
                                   audit_mode=_audit, baseline_cache=baseline_cache)
        all_results[Path(base_path).stem] = alerts_base
        n_variants = len(variant_paths)
        for vi, vp in enumerate(variant_paths, 1):
            cfg_stem = Path(vp).stem
            if results_dir:
                result_file = results_dir / f"{result_prefix}__{cfg_stem}.json"
                if result_file.exists():
                    try:
                        cached = json.loads(result_file.read_text(encoding="utf-8"))
                        all_results[cfg_stem] = cached
                        print(f"  [cfg {vi}/{n_variants}] {cfg_stem}  skip (ya guardado)")
                        continue
                    except Exception:
                        pass
            t_cfg     = time.perf_counter()
            prev_prep = len(prepared_cache)
            prev_ana  = len(analyze_cache)
            cfg_v = Config(vp)
            alerts_v = run_backtest(cfg_v, args.weeks, klines, start_dt, end_dt,
                                    snapshot_pairs, label=f"VARIANT ({cfg_stem})",
                                    scan_interval_min=args.scan_interval_min,
                                    derivatives=derivatives, prepared_cache=prepared_cache,
                                    analyze_cache=analyze_cache, outcomes_cache=outcomes_cache,
                                    audit_mode=_audit, baseline_cache=baseline_cache)
            elapsed  = time.perf_counter() - t_cfg
            prep_st  = "miss" if len(prepared_cache) > prev_prep else "hit"
            ana_st   = "miss" if len(analyze_cache)  > prev_ana  else "hit"
            print(f"  [cfg {vi}/{n_variants}] {cfg_stem}  {elapsed:.1f}s  "
                  f"prep={prep_st} ana={ana_st}")
            all_results[cfg_stem] = alerts_v
            compare_runs(alerts_base, Path(base_path).stem, alerts_v, cfg_stem,
                         period_days=period_days, cfg=cfg_v,
                         baseline=baseline_cache.get("base"))
            if results_dir:
                result_file = results_dir / f"{result_prefix}__{cfg_stem}.json"
                tmp_file    = result_file.with_suffix(".tmp")
                try:
                    tmp_file.write_text(json.dumps(alerts_v, default=str), encoding="utf-8")
                    os.replace(str(tmp_file), str(result_file))
                except OSError:
                    pass

    elif args.compare:
        cfg_old = Config(args.compare[0])
        cfg_new = Config(args.compare[1])
        alerts_old = run_backtest(cfg_old, args.weeks, klines, start_dt, end_dt,
                                  snapshot_pairs, label=f"OLD ({args.compare[0]})",
                                  scan_interval_min=args.scan_interval_min,
                                  derivatives=derivatives, prepared_cache=prepared_cache,
                                  analyze_cache=analyze_cache, outcomes_cache=outcomes_cache,
                                  audit_mode=_audit, baseline_cache=baseline_cache)
        alerts_new = run_backtest(cfg_new, args.weeks, klines, start_dt, end_dt,
                                  snapshot_pairs, label=f"NEW ({args.compare[1]})",
                                  scan_interval_min=args.scan_interval_min,
                                  derivatives=derivatives, prepared_cache=prepared_cache,
                                  analyze_cache=analyze_cache, outcomes_cache=outcomes_cache,
                                  audit_mode=_audit, baseline_cache=baseline_cache)
        compare_runs(alerts_old, args.compare[0], alerts_new, args.compare[1],
                     period_days=period_days, cfg=cfg_new,
                     baseline=baseline_cache.get("base"))
        all_results = {"old": alerts_old, "new": alerts_new}

    elif args.ablation:
        import copy
        variants = {"full (todo activo)": cfg_main.raw}
        no_cvd = copy.deepcopy(cfg_main.raw)
        no_cvd.setdefault("indicators", {})["CVD_BULLISH_MIN"] = 999
        variants["sin CVD bonus"] = no_cvd
        no_obv = copy.deepcopy(cfg_main.raw)
        no_obv.setdefault("indicators", {})["OBV_RISING_MIN"] = 999
        variants["sin OBV bonus"] = no_obv
        no_long = copy.deepcopy(cfg_main.raw)
        no_long.setdefault("indicators", {})["RECENT_LONG_PROXIMITY"] = 999
        no_long.setdefault("indicators", {})["RECENT_LOOKBACK_LONG"] = 1
        variants["sin lookback 25 penalty"] = no_long
        for name, raw_cfg in variants.items():
            tmp_cfg = Config.__new__(Config)
            tmp_cfg.raw = raw_cfg
            tmp_cfg.path = f"<{name}>"
            alerts = run_backtest(tmp_cfg, args.weeks, klines, start_dt, end_dt,
                                  snapshot_pairs, label=name,
                                  scan_interval_min=args.scan_interval_min,
                                  derivatives=derivatives, prepared_cache=prepared_cache,
                                  analyze_cache=analyze_cache, outcomes_cache=outcomes_cache,
                                  audit_mode=_audit, baseline_cache=baseline_cache)
            all_results[name] = alerts
        if "full (todo activo)" in all_results:
            for name, alerts in all_results.items():
                if name != "full (todo activo)":
                    compare_runs(all_results["full (todo activo)"], "full", alerts, name,
                                 period_days=period_days, cfg=cfg_main,
                                 baseline=baseline_cache.get("base"))
    else:
        alerts = run_backtest(cfg_main, args.weeks, klines, start_dt, end_dt,
                              snapshot_pairs, label=f"config: {args.config}",
                              scan_interval_min=args.scan_interval_min,
                              derivatives=derivatives, prepared_cache=prepared_cache,
                              analyze_cache=analyze_cache, outcomes_cache=outcomes_cache,
                              audit_mode=_audit, baseline_cache=baseline_cache)
        all_results = {"main": alerts}

    if args.out:
        print(f"\n[4/4] Guardando resultados...")
        out_path = Path(args.out)
        try:
            with open(out_path, "w", encoding="utf-8") as f:
                json.dump(all_results, f, indent=2, default=str)
            print(f"  Guardado: {out_path.absolute()}")
        except OSError as e:
            print(f"  ⚠ No se pudo guardar JSON: {e}")
            print(f"  (la comparativa por consola ya está completa, no perdiste datos)")
    else:
        print(f"\n[4/4] Skipping JSON save (no se pasó --out)")

    print(f"\nDONE")


if __name__ == "__main__":
    main()
