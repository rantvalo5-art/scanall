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
SCAN_INTERVAL_MIN = 15
OUTCOME_OFFSETS_MIN = [15, 60, 240, 1440]
OUTCOME_NAMES = ["price_15m", "price_1h", "price_4h", "price_24h"]
MAX_DOWNLOAD_WORKERS = 20
MAX_PAIRS = 200

BINANCE_DATA_URL = "https://data-api.binance.vision/api/v3"
BINANCE_FALLBACK_URL = "https://api.binance.com/api/v3"
BINANCE_FAPI_URL = "https://fapi.binance.com"

CACHE_DIR = Path(r"G:\.backtest_cache")
_NO_CACHE = False  # override con --no-cache en CLI

SUPABASE_URL = os.environ.get("SUPABASE_URL", "https://ecgdswroygkfckkaguxp.supabase.co")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY", "")


# ════════════════════════════════════════════════════════════════════════════
# CONFIG: lectura del config.json
# ════════════════════════════════════════════════════════════════════════════

class Config:
    """Wrapper sobre config.json. g() admite defaults para back-compat."""

    def __init__(self, path):
        with open(path, "r", encoding="utf-8") as f:
            self.raw = json.load(f)
        self.path = path

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
    intervals = ["5m", "15m", "1h", "4h"]
    buffer_days = 15 if "4h" in intervals else 3  # 4h necesita ≥80 barras (~13d)
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
    valid = {s: tfs for s, tfs in data.items() if all(t in tfs for t in ("5m","15m","1h"))}
    print(f"  {len(valid)}/{len(symbols)} pares con data completa (5m/15m/1h requeridos)")
    return valid


def download_1m_klines(symbols, start_dt, end_dt):
    """Descarga klines 1m para la lista de símbolos. No agrega buffer extra — el caller
    pasa las fechas exactas que necesita. Cachea igual que download_all_klines."""
    start_ms = int(start_dt.timestamp() * 1000)
    end_ms   = int(end_dt.timestamp() * 1000)
    data = {}
    print(f"\n  Descargando {len(symbols)} pares × 1m klines (para análisis forming)...")
    completed = 0

    def fetch(sym):
        p = _cache_path("klines", sym, "1m", start_ms, end_ms)
        cached = _cache_get(p)
        if cached is not None:
            return sym, cached
        result = get_klines_range(sym, "1m", start_ms, end_ms)
        if result is not None:
            _cache_put(p, result)
        return sym, result

    with ThreadPoolExecutor(max_workers=MAX_DOWNLOAD_WORKERS) as ex:
        futures = [ex.submit(fetch, s) for s in symbols]
        for fut in as_completed(futures):
            sym, df = fut.result()
            if df is None or len(df) < 5:
                continue
            data[sym] = df
            completed += 1
            if completed % 50 == 0:
                print(f"    {completed}/{len(symbols)} pares 1m descargados...")
    print(f"  {len(data)}/{len(symbols)} pares con 1m data")
    return data


# ════════════════════════════════════════════════════════════════════════════
# FETCH DE DERIVATIVES (Binance Futures USDT-M)
# ════════════════════════════════════════════════════════════════════════════

def _fapi_get(path, params, retries=3):
    for attempt in range(retries):
        try:
            r = requests.get(f"{BINANCE_FAPI_URL}{path}", params=params, timeout=20)
            if r.status_code == 200:
                return r.json()
            if r.status_code == 429:
                time.sleep(2 ** attempt)
                continue
            return None
        except requests.exceptions.RequestException:
            time.sleep(1)
    return None


def get_futures_perp_symbols():
    """Set de pares USDT-M perp activos en Binance Futures."""
    data = _fapi_get("/fapi/v1/exchangeInfo", {})
    if not data:
        return set()
    return {
        s["symbol"]
        for s in data.get("symbols", [])
        if s.get("contractType") == "PERPETUAL"
        and s.get("quoteAsset") == "USDT"
        and s.get("status") == "TRADING"
    }


def get_oi_history_range(symbol, start_ms, end_ms):
    """OI 5m granularity. Paginado (max 500 por request).
    Retorna DataFrame [timestamp_ms, oi] o None si falla."""
    rows = []
    cursor = start_ms
    while cursor < end_ms:
        data = _fapi_get(
            "/futures/data/openInterestHist",
            {"symbol": symbol, "period": "5m",
             "startTime": cursor, "endTime": end_ms, "limit": 500},
        )
        if not data:
            break
        rows.extend(data)
        last_ts = int(data[-1]["timestamp"])
        if len(data) < 500 or last_ts <= cursor:
            break
        cursor = last_ts + 1
    if not rows:
        return None
    df = pd.DataFrame([{"timestamp": int(r["timestamp"]),
                        "oi": float(r["sumOpenInterest"])} for r in rows])
    return df.drop_duplicates("timestamp").sort_values("timestamp").reset_index(drop=True)


def get_funding_history_range(symbol, start_ms, end_ms):
    """Funding rate (cada 8h). Paginado (max 1000).
    Retorna DataFrame [funding_time_ms, funding_rate] o None si falla."""
    rows = []
    cursor = start_ms
    while cursor < end_ms:
        data = _fapi_get(
            "/fapi/v1/fundingRate",
            {"symbol": symbol, "startTime": cursor, "endTime": end_ms, "limit": 1000},
        )
        if not data:
            break
        rows.extend(data)
        last_ts = int(data[-1]["fundingTime"])
        if len(data) < 1000 or last_ts <= cursor:
            break
        cursor = last_ts + 1
    if not rows:
        return None
    df = pd.DataFrame([{"funding_time": int(r["fundingTime"]),
                        "funding_rate": float(r["fundingRate"])} for r in rows])
    return df.drop_duplicates("funding_time").sort_values("funding_time").reset_index(drop=True)


def download_all_derivatives(symbols, start_dt, end_dt):
    """Bulk-fetch OI history + funding history para todos los pares con perp.
    Retorna {symbol: {"oi": df, "fr": df}}. Pares sin perp se omiten."""
    perp_set = get_futures_perp_symbols()
    if not perp_set:
        print("  [deriv] no se obtuvo lista de futuros perp; omitiendo derivatives")
        return {}
    targets = [s for s in symbols if s in perp_set]
    print(f"  [deriv] {len(targets)}/{len(symbols)} pares tienen perp USDT-M")
    if not targets:
        return {}

    # OI 5m necesita un buffer hacia atrás para el lookback de 30min.
    fetch_start = start_dt - timedelta(hours=2)
    start_ms = int(fetch_start.timestamp() * 1000)
    end_ms = int(end_dt.timestamp() * 1000)

    data = {}

    def fetch(sym):
        p_oi = _cache_path("oi", sym, "5m", start_ms, end_ms)
        p_fr = _cache_path("fr", sym, "8h", start_ms, end_ms)
        oi = _cache_get(p_oi)
        fr = _cache_get(p_fr)
        if oi is None:
            oi = get_oi_history_range(sym, start_ms, end_ms)
            if oi is not None:
                _cache_put(p_oi, oi)
        if fr is None:
            fr = get_funding_history_range(sym, start_ms, end_ms)
            if fr is not None:
                _cache_put(p_fr, fr)
        return sym, oi, fr

    with ThreadPoolExecutor(max_workers=MAX_DOWNLOAD_WORKERS) as ex:
        futures = [ex.submit(fetch, s) for s in targets]
        completed = 0
        for fut in as_completed(futures):
            sym, oi, fr = fut.result()
            if oi is not None or fr is not None:
                data[sym] = {"oi": oi, "fr": fr}
            completed += 1
            if completed % 50 == 0:
                print(f"    [deriv] {completed}/{len(targets)}...")
    print(f"  [deriv] {len(data)}/{len(targets)} pares con data de derivatives")
    return data


def lookup_derivatives_at(deriv_for_symbol, ts_ms, oi_lookback_min=30):
    """Dado el dict {oi: df, fr: df} de UN símbolo y un timestamp, retorna
    (oi_delta, funding_rate). Cualquiera puede ser None si no hay data suficiente."""
    oi_delta = None
    funding_rate = None
    if not deriv_for_symbol:
        return None, None

    oi_df = deriv_for_symbol.get("oi")
    if oi_df is not None and len(oi_df) >= 2:
        ts_arr = oi_df["timestamp"].values
        # Valor "ahora": el último <= ts_ms
        idx_now = ts_arr.searchsorted(ts_ms, side="right") - 1
        # Valor "lookback atrás": el último <= ts_ms - lookback
        target_then = ts_ms - oi_lookback_min * 60 * 1000
        idx_then = ts_arr.searchsorted(target_then, side="right") - 1
        if idx_now >= 0 and idx_then >= 0 and idx_then != idx_now:
            oi_now = float(oi_df["oi"].iloc[idx_now])
            oi_then = float(oi_df["oi"].iloc[idx_then])
            if oi_then > 0:
                oi_delta = (oi_now - oi_then) / oi_then

    fr_df = deriv_for_symbol.get("fr")
    if fr_df is not None and len(fr_df) >= 1:
        ts_arr = fr_df["funding_time"].values
        idx = ts_arr.searchsorted(ts_ms, side="right") - 1
        if idx >= 0:
            funding_rate = float(fr_df["funding_rate"].iloc[idx])

    return oi_delta, funding_rate


# ════════════════════════════════════════════════════════════════════════════
# CACHE DE DISCO (evita re-descargar klines/derivatives entre runs)
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
        # En backtest todas las velas son históricas y por ende cerradas.
        # Mantenemos el campo para que el bloque FORMING_CANDLE_PENALTY de classify()
        # tenga un dato consistente con el screener (donde candle_status también es "closed"
        # post-truncation-fix).
        "candle_status": "closed",
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
        "DERIV_ENABLED":           cfg.g("derivatives", "ENABLED", default=False),
        "DERIV_OI_LOOKBACK_MIN":   cfg.g("derivatives", "OI_LOOKBACK_MIN", default=30),
        "ATR_MIN_PCT":             cfg.g("indicators", "ATR_MIN_PCT"),
        "ATR_MODE":                cfg.g("indicators", "ATR_MODE", default="fixed"),
        "ATR_PERCENTILE_RANK":     cfg.g("indicators", "ATR_PERCENTILE_RANK", default=30),
        "ATR_PERCENTILE_LOOKBACK": cfg.g("indicators", "ATR_PERCENTILE_LOOKBACK", default=168),
        "ATR_MIN_PCT_FLOOR":       cfg.g("indicators", "ATR_MIN_PCT_FLOOR", default=1.0),
        "ATR_BYPASS_ON_EXPLOSION_CRITERIA": cfg.g("indicators", "ATR_BYPASS_ON_EXPLOSION_CRITERIA", default=False),
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
    # Cambio % de la vela actual vs la anterior (feature usada por EXPLOSION).
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


def classify(symbol, tf_data, cfg, counts_history=None):
    """Replica classify_symbol del screener con scoring totalmente parametrizado.
    tf_data = {'5m': dict, '15m': dict, '1h': dict}
    counts_history: dict {(symbol, history_tf): n_alertas_recientes} — usado para
    aplicar LATE_REPEAT_PENALTY igual que en producción. None = sin late penalty."""
    if counts_history is None:
        counts_history = {}
    tf5 = tf_data.get("5m") or {}
    tf15 = tf_data.get("15m") or {}
    tf1h = tf_data.get("1h") or {}
    if not tf5 or not tf15 or not tf1h:
        return None
    tf4h = tf_data.get("4h") or {}
    _tf4_up = tf4h.get("ema_trend_up", True)  # fail-safe: 4h ausente => no bloquear
    _tf4_enabled = cfg.g("trend_filter_4h", "ENABLED", default=False)
    _tf4_mode    = cfg.g("trend_filter_4h", "MODE",    default="soft")
    _tf4_penalty = cfg.g("trend_filter_4h", "PENALTY", default=2)
    _tf4_signals = set(cfg.g("trend_filter_4h", "SIGNALS", default=["BREAKOUT","HOLD","RIDING"]))

    # Los thresholds OBV/CVD varían entre variantes: se re-derivan aquí para que
    # _build_candidates() pueda compartirse entre todas las variantes (una sola entrada en analyze_cache).
    tf15["obv_rising"]  = tf15.get("obv_slope", 0) >= cfg.g("indicators", "OBV_RISING_MIN", default=0.05)
    tf15["cvd_bullish"] = tf15.get("cvd_ratio", 0) >= cfg.g("indicators", "CVD_BULLISH_MIN", default=0.05)

    # Gates universales (aplican a TODOS los signals incl. EXPLOSION).
    if not tf1h.get("not_near_resistance"):
        _bypass_resist = (
            cfg.g("hold", "RESIST_BYPASS_ON_15M_BREAKOUT", default=False)
            and tf15.get("breakout", False)
            and tf15.get("vol_ratio", 0) >= cfg.g("hold", "RESIST_BYPASS_MIN_VOL_15M", default=999.0)
            and (tf15.get("bars_since_break") or 999) <= cfg.g("hold", "RESIST_BYPASS_MAX_BARS_SINCE_BREAK", default=0)
        )
        if not _bypass_resist:
            return None
    if not tf1h.get("major_struct_ok", True):
        _bypass_brk = cfg.g("hold", "MAJOR_STRUCT_BYPASS_ON_BREAKOUT", default=False) and tf15.get("breakout", False)
        _bypass_pre = cfg.g("hold", "MAJOR_STRUCT_BYPASS_ON_STRONG_PRE", default=False) and tf15.get("cvd_bullish", False) and tf15.get("obv_rising", False)
        _bypass_vol = tf1h.get("vol_ratio", 0) >= cfg.g("hold", "MAJOR_STRUCT_BYPASS_VOL_RATIO_1H", default=999.0)
        _age_thr    = cfg.g("hold", "MAJOR_STRUCT_BYPASS_AGE_BARS", default=999)
        _bsm        = tf1h.get("bars_since_major_max")
        _bypass_age = _bsm is not None and _bsm >= _age_thr
        _soft_dist  = cfg.g("hold", "MAJOR_STRUCT_BYPASS_SOFTZONE_DIST", default=0.0)
        _soft_max   = cfg.g("hold", "MAJOR_STRUCT_BYPASS_SOFTZONE_HOLD_MAX_BARS", default=0)
        _msd        = tf1h.get("major_struct_dist")
        _bsb        = tf15.get("bars_since_break")
        _bypass_soft = (
            _soft_dist > 0 and _soft_max > 0
            and _msd is not None and _msd <= _soft_dist
            and tf15.get("hold_recent_break", False)
            and _bsb is not None and _bsb <= _soft_max
        )
        if not (_bypass_brk or _bypass_pre or _bypass_vol or _bypass_age or _bypass_soft):
            return None

    candidates = []
    SCORE_CAP = cfg.g("scoring", "SCORE_CAP")
    LATE_REPEAT_COUNT = cfg.g("history", "LATE_REPEAT_COUNT", default=1)
    _cal_cfg = cfg.raw.get("scoring_calibration") or {}

    _persist_on = cfg.g("persistence", "ENABLED", default=False)

    # ── EXPLOSION ─────────────────────────────────────────────────────────
    # Detector agresivo de pumps explosivos en 5m. Bypaaa EMA y ATR gates
    # — su criterio (vol_ratio ≥ 5, body ≥ 0.85, close_change ≥ 2.5%, BB
    # expansion ≥ 0.5) ya es restrictivo por construcción. Captura los
    # pumps que PREBREAK/BREAKOUT pierden por timing o gates duros.
    if cfg.g("active_signals", "EXPLOSION", default=False):
        ex_min_vol  = cfg.g("scoring_explosion", "MIN_VOL_RATIO", default=5.0)
        ex_min_body = cfg.g("scoring_explosion", "MIN_BODY_PCT", default=0.85)
        ex_min_chg  = cfg.g("scoring_explosion", "MIN_CLOSE_CHANGE", default=0.025)
        ex_min_bb   = cfg.g("scoring_explosion", "MIN_BB_EXPANSION", default=0.5)
        if (tf5.get("vol_ratio", 0)         >= ex_min_vol
            and tf5.get("candle_body_pct", 0) >= ex_min_body
            and tf5.get("close_change_curr", 0) >= ex_min_chg
            and tf5.get("width_expansion", 0)   >= ex_min_bb):
            ex_base = cfg.g("scoring_explosion", "BASE_SCORE", default=12)
            ex_late_max = cfg.g("scoring_explosion", "LATE_ENTRY_MAX_DIST", default=0.03)
            ex_late_pen = cfg.g("scoring_explosion", "LATE_ENTRY_PENALTY", default=-3)
            score = ex_base
            bd = {"BASE": ex_base}
            # OBV tiers sobre 15m context (igual que BREAKOUT — diferencia explosiones reales de fakes)
            _ex_obv = tf15.get("obv_slope", 0) or 0
            _ex_obv_expl_b = cfg.g("scoring_explosion", "OBV_EXPLOSIVE_BONUS", default=0)
            _ex_obv_str_b  = cfg.g("scoring_explosion", "OBV_STRONG_BONUS",    default=0)
            _ex_obv_ris_b  = cfg.g("scoring_explosion", "OBV_RISING_BONUS",    default=0)
            if _ex_obv >= cfg.g("scoring_breakout", "OBV_TIER_EXPLOSIVE_MIN", default=0.5) and _ex_obv_expl_b:
                score += _ex_obv_expl_b; bd["OBV_EXPLOSIVE"] = _ex_obv_expl_b
            elif _ex_obv >= cfg.g("scoring_breakout", "OBV_TIER_STRONG_MIN", default=0.2) and _ex_obv_str_b:
                score += _ex_obv_str_b; bd["OBV_STRONG"] = _ex_obv_str_b
            elif _ex_obv >= cfg.g("scoring_breakout", "OBV_TIER_RISING_MIN", default=0.08) and _ex_obv_ris_b:
                score += _ex_obv_ris_b; bd["OBV_RISING"] = _ex_obv_ris_b
            if tf5.get("breakout_distance", 0) > ex_late_max:
                score += ex_late_pen
                bd["LATE_ENTRY"] = ex_late_pen
            score = min(max(score, 0), SCORE_CAP)
            candidates.append({
                "label": "EXPLOSION", "history_tf": "EXPLOSION", "score": score,
                "priority": 5, "bucket": final_bucket(score, "EXPLOSION", cfg),
                "timeframe": "5m", "price": tf5["price"],
                "ref_price": tf5.get("recent_max", tf5["price"]),
                "obv_slope": tf15.get("obv_slope"),
                "cvd_ratio": tf15.get("cvd_ratio"),
                "recent_long_ok": tf15.get("recent_long_ok"),
                "htf_1h_up": bool(tf1h.get("ema_trend_up")),
                "htf_4h_up": bool(tf4h.get("ema_trend_up")),
                "breakdown": bd,
            })

    # Gates para signals tradicionales (PREBREAK/BREAKOUT/RIDING/HOLD/FADING).
    # EXPLOSION ya pudo haber disparado antes — si EMA/ATR bloquean, devolvemos
    # EXPLOSION solo (si existe) en lugar de None.
    _ema_gate_hard = cfg.g("indicators", "EMA_GATE_HARD", default=True)
    _ema_blocks = (not tf1h.get("ema_trend_up")) and _ema_gate_hard
    _ema_bypassed_by_explosion = False
    if _ema_blocks and cfg.g("indicators", "EMA_BYPASS_ON_EXPLOSION_CRITERIA", default=False) and (
            tf15.get("vol_ratio", 0)         >= cfg.g("indicators", "EMA_BYPASS_EX_VOL_15M",  default=5.0)
            and tf15.get("candle_body_pct", 0)   >= cfg.g("indicators", "EMA_BYPASS_EX_BODY_15M", default=0.85)
            and tf15.get("close_change_curr", 0) >= cfg.g("indicators", "EMA_BYPASS_EX_CHG_15M",  default=0.025)
            and tf15.get("width_expansion", 0)   >= cfg.g("indicators", "EMA_BYPASS_EX_BB_15M",   default=0.5)):
        _ema_blocks = False
        _ema_bypassed_by_explosion = True
    _atr_pct_v      = tf1h.get("atr_pct", 0)
    _atr_threshold_v = tf1h.get("atr_threshold", cfg.g("indicators", "ATR_MIN_PCT"))
    _atr_blocks = _atr_pct_v < _atr_threshold_v
    _atr_ratio  = (_atr_pct_v / _atr_threshold_v) if _atr_threshold_v > 0 else 0.0
    if _atr_blocks:
        if cfg.g("indicators", "ATR_BYPASS_ON_EXPLOSION_CRITERIA", default=False) and (
                tf15.get("vol_ratio", 0)         >= cfg.g("indicators", "ATR_BYPASS_EX_VOL_15M",  default=cfg.g("scoring_explosion", "MIN_VOL_RATIO",    default=5.0))
                and tf15.get("candle_body_pct", 0)   >= cfg.g("indicators", "ATR_BYPASS_EX_BODY_15M", default=cfg.g("scoring_explosion", "MIN_BODY_PCT",     default=0.85))
                and tf15.get("close_change_curr", 0) >= cfg.g("indicators", "ATR_BYPASS_EX_CHG_15M",  default=cfg.g("scoring_explosion", "MIN_CLOSE_CHANGE", default=0.025))
                and tf15.get("width_expansion", 0)   >= cfg.g("indicators", "ATR_BYPASS_EX_BB_15M",   default=cfg.g("scoring_explosion", "MIN_BB_EXPANSION", default=0.5))):
            _atr_blocks = False
        elif (tf15.get("vol_ratio", 0) >= cfg.g("indicators", "ATR_BYPASS_VOL_RATIO_15M", default=999.0)
              and tf15.get("width_expansion", 0) >= cfg.g("indicators", "ATR_BYPASS_BB_EXP_15M_MIN", default=0.0)):
            _atr_blocks = False
        elif (cfg.g("indicators", "ATR_BYPASS_NEAR_COMPOUND", default=False)
              and _atr_ratio >= cfg.g("indicators", "ATR_BYPASS_NEAR_RATIO_MIN", default=0.80)
              and tf15.get("vol_ratio", 0) >= cfg.g("indicators", "ATR_BYPASS_NEAR_VOL15M_MIN", default=2.0)
              and tf5.get("close_change_curr", 0) >= cfg.g("indicators", "ATR_BYPASS_NEAR_CHANGE5M_MIN", default=0.005)):
            _atr_blocks = False
    _trad_signals_eligible = not (_ema_blocks or _atr_blocks)
    if not _trad_signals_eligible and not candidates:
        return None

    # ── PREBREAK ──────────────────────────────────────────────────────────
    if _trad_signals_eligible and cfg.g("active_signals", "PREBREAK"):
        _pre_vol_bars = cfg.g("persistence", "PREBREAK_VOL_BARS", default=1) if _persist_on else 1
        _vol_ok_pre = tf5.get("vol_ratio", 0) >= cfg.g("prebreak", "PREBREAK_MIN_VOL_RATIO")
        if _pre_vol_bars >= 2:
            _vol_ok_pre = _vol_ok_pre and tf5.get("vol_ratio_prev", 0) >= cfg.g("prebreak", "PREBREAK_MIN_VOL_RATIO")
        if (tf5.get("near_recent_max")
            and tf5.get("width_curr", 9) <= cfg.g("prebreak", "PREBREAK_BB_WIDTH_MAX")
            and _vol_ok_pre
            and tf5.get("vol_growth", 0) >= cfg.g("prebreak", "PREBREAK_VOLUME_GROWTH_MIN")):

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

            vol_factor = min(tf5["vol_ratio"] / vol_div, vol_cap)
            growth_factor = min(tf5["vol_growth"] / grow_div, grow_cap)
            bb_factor = max(1.0 - (tf5["width_curr"] / bb_div), bb_floor)
            score = round(base_offset + vol_factor * growth_factor * bb_factor * base_mult)
            bd = {"BASE_FORMULA": score}

            if tf5.get("strong_close"):
                score += close_bonus
                bd["STRONG_CLOSE"] = close_bonus
            if tf15.get("obv_rising"):
                score += obv_up_bonus
                bd["OBV_RISING"] = obv_up_bonus
            elif tf15.get("obv_slope", 0) < -cfg.g("indicators", "OBV_RISING_MIN", default=0.05):
                score += obv_dn_pen
                bd["OBV_FALLING"] = obv_dn_pen
            if not tf15.get("recent_long_ok", True):
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
                "timeframe": "5m", "price": tf5["price"],
                "ref_price": tf5["recent_max"],
                "obv_slope": tf15.get("obv_slope"),
                "cvd_ratio": tf15.get("cvd_ratio"),
                "recent_long_ok": tf15.get("recent_long_ok"),
                "htf_1h_up": bool(tf1h.get("ema_trend_up")),
                "htf_4h_up": bool(tf4h.get("ema_trend_up")),
                "breakdown": bd,
            })

    # ── BREAKOUT ──────────────────────────────────────────────────────────
    if _trad_signals_eligible and cfg.g("active_signals", "BREAKOUT"):
        require_obv_nn = cfg.g("breakout", "BREAKOUT_REQUIRE_OBV_NON_NEGATIVE", default=True)
        if (tf15.get("breakout")
            and tf15.get("vol_ratio", 0) >= cfg.g("breakout", "BREAKOUT_MIN_VOL_RATIO")
            and tf15.get("breakout_distance", 9) <= cfg.g("breakout", "BREAKOUT_MAX_EXTENDED")
            and tf15.get("width_expansion", -9) >= cfg.g("breakout", "BREAKOUT_BB_EXPANSION_MIN")
            and tf15.get("strong_close", False)
            and tf15.get("candle_body_pct", 0) >= cfg.g("breakout", "BREAKOUT_MIN_BODY_PCT")
            and tf5.get("vol_ratio", 0) >= cfg.g("breakout", "BREAKOUT_5M_MIN_VOL_RATIO")
            and tf5.get("strong_close", False)
            and (not require_obv_nn or tf15.get("obv_slope", 0) >= 0)):

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
            obv_v = tf15.get("obv_slope", 0)
            cvd_v = tf15.get("cvd_ratio", 0)

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
            if tf15.get("vol_ratio", 0) >= climax_vol_min:
                climax_signals += 1
            if tf15.get("width_expansion", 0) >= climax_bb_min:
                climax_signals += 1
            if tf15.get("candle_body_pct", 0) >= climax_body_min:
                climax_signals += 1
            _climax_late_only = cfg.g("indicators", "CLIMAX_REQUIRES_LATE_ENTRY", default=False)
            if climax_signals >= climax_thresh and (not _climax_late_only or tf15["breakout_distance"] >= late_min):
                score += climax_pen
                bd["CLIMAX"] = climax_pen

            if tf15["breakout_distance"] <= early_max:
                score += early_bonus
                bd["EARLY_ENTRY"] = early_bonus
            elif tf15["breakout_distance"] >= late_min:
                score += late_pen_entry
                bd["LATE_ENTRY"] = late_pen_entry

            if not tf15.get("recent_long_ok", True):
                score += struct_pen
                bd["STRUCT"] = struct_pen

            # LATE_REPEAT_PENALTY (mismo comportamiento que screener.py)
            late_repeat_pen = cfg.g("scoring_breakout", "LATE_REPEAT_PENALTY", default=-1)
            prev_bo = counts_history.get((symbol, "BREAKOUT"), 0)
            if prev_bo >= LATE_REPEAT_COUNT:
                score += late_repeat_pen
                bd["LATE_REPEAT"] = late_repeat_pen

            # Derivatives: OI delta + funding rate (mismo bloque que screener.py)
            if cfg.g("derivatives", "ENABLED", default=False):
                oi = tf15.get("oi_delta_30m")
                fr = tf15.get("funding_rate")
                if oi is not None:
                    if oi >= cfg.g("scoring_breakout", "OI_RISING_MIN", default=0.02):
                        score += cfg.g("scoring_breakout", "OI_RISING_BONUS", default=2)
                    elif oi <= cfg.g("scoring_breakout", "OI_FALLING_MAX", default=-0.01):
                        score += cfg.g("scoring_breakout", "OI_FALLING_PENALTY", default=-2)
                if fr is not None:
                    if fr <= cfg.g("scoring_breakout", "FUNDING_HEALTHY_MAX", default=0.0003):
                        score += cfg.g("scoring_breakout", "FUNDING_HEALTHY_BONUS", default=1)
                    elif fr >= cfg.g("scoring_breakout", "FUNDING_HOT_MIN", default=0.0008):
                        score += cfg.g("scoring_breakout", "FUNDING_HOT_PENALTY", default=-2)

            if _tf4_enabled and "BREAKOUT" in _tf4_signals and not _tf4_up and _tf4_mode == "soft":
                score -= _tf4_penalty; bd["NO_4H_TREND"] = -_tf4_penalty
            score = min(score, SCORE_CAP)
            if not (_tf4_enabled and _tf4_mode == "hard" and "BREAKOUT" in _tf4_signals and not _tf4_up):
                candidates.append({
                    "label": "BREAKOUT", "history_tf": "BREAKOUT", "score": score,
                    "priority": 2, "bucket": final_bucket(score, "BREAKOUT", cfg),
                    "timeframe": "15m", "price": tf15["price"],
                    "ref_price": tf15["recent_max"],
                    "obv_slope": tf15.get("obv_slope"),
                    "cvd_ratio": tf15.get("cvd_ratio"),
                    "recent_long_ok": tf15.get("recent_long_ok"),
                    "htf_1h_up": bool(tf1h.get("ema_trend_up")),
                    "htf_4h_up": bool(tf4h.get("ema_trend_up")),
                    "breakdown": bd,
                })

    # ── RIDING ────────────────────────────────────────────────────────────
    if _trad_signals_eligible and cfg.g("active_signals", "RIDING"):
        rg = tf15.get("riding_gain") or 0.0
        if (tf15.get("riding_above_zone")
            and tf15.get("riding_vol_ok")
            and cfg.g("riding", "RIDING_MIN_GAIN") <= rg <= cfg.g("riding", "RIDING_MAX_GAIN")
            and tf15.get("riding_bars_since") is not None
            and tf15["riding_bars_since"] >= 1
            and (not cfg.g("riding", "RIDING_EMA_MUST_TREND") or tf1h.get("ema_trend_up"))
            and not tf15.get("breakout")
            and (tf15.get("fading_reversal") or 0.0) >= -cfg.g("riding", "RIDING_MAX_FADE_FROM_HIGH", default=0.025)):

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
            if tf15.get("riding_vol_ok"):
                score += vol_ok_b; bd["VOL_OK"] = vol_ok_b
            if tf15.get("strong_close"):
                score += close_b; bd["STRONG_CLOSE"] = close_b
            _ema_up_effective = tf1h.get("ema_trend_up") or (
                _ema_bypassed_by_explosion and cfg.g("indicators", "EMA_BYPASS_GRANTS_TREND_BONUS", default=False))
            if _ema_up_effective:
                score += ema_b; bd["EMA_TREND"] = ema_b
            if tf1h.get("dist_to_res", 0) > dist_high_min:
                score += dist_high_b; bd["DIST_HIGH"] = dist_high_b
            else:
                score += dist_low_b; bd["DIST_LOW"] = dist_low_b
            if tf15.get("obv_rising"):
                score += obv_up_b; bd["OBV_RISING"] = obv_up_b
            elif tf15.get("obv_slope", 0) < 0:
                score += obv_dn_pen; bd["OBV_FALLING"] = obv_dn_pen
            if tf15.get("cvd_bullish"):
                score += cvd_up_b; bd["CVD_BULLISH"] = cvd_up_b
            elif tf15.get("cvd_ratio", 0) < -cfg.g("indicators", "CVD_BULLISH_MIN", default=0.05):
                score += cvd_dn_pen; bd["CVD_BEARISH"] = cvd_dn_pen

            # Derivatives: OI delta + funding rate (mismo bloque que screener.py)
            if cfg.g("derivatives", "ENABLED", default=False):
                oi = tf15.get("oi_delta_30m")
                fr = tf15.get("funding_rate")
                if oi is not None:
                    if oi >= cfg.g("scoring_riding", "OI_RISING_MIN", default=0.02):
                        score += cfg.g("scoring_riding", "OI_RISING_BONUS", default=1)
                    elif oi <= cfg.g("scoring_riding", "OI_FALLING_MAX", default=-0.01):
                        score += cfg.g("scoring_riding", "OI_FALLING_PENALTY", default=-1)
                if fr is not None:
                    if fr <= cfg.g("scoring_riding", "FUNDING_HEALTHY_MAX", default=0.0003):
                        score += cfg.g("scoring_riding", "FUNDING_HEALTHY_BONUS", default=1)
                    elif fr >= cfg.g("scoring_riding", "FUNDING_HOT_MIN", default=0.0008):
                        score += cfg.g("scoring_riding", "FUNDING_HOT_PENALTY", default=-1)

            # Bonus por frescura: RIDING detectado en las primeras N barras desde el break
            _fresh_max = cfg.g("scoring_riding", "FRESH_MAX_BARS", default=0)
            _fresh_b   = cfg.g("scoring_riding", "FRESH_BONUS", default=0)
            _bars_since = tf15.get("riding_bars_since")
            if _fresh_b and _fresh_max and _bars_since is not None and _bars_since <= _fresh_max:
                score += _fresh_b; bd["FRESH"] = _fresh_b

            # LATE_REPEAT_PENALTY (mismo comportamiento que screener.py)
            prev_riding = counts_history.get((symbol, "RIDING"), 0)
            late_repeat_pen = cfg.g("scoring_riding", "LATE_REPEAT_PENALTY", default=0)
            if prev_riding >= LATE_REPEAT_COUNT and late_repeat_pen < 0:
                score += late_repeat_pen
                bd["LATE_REPEAT"] = late_repeat_pen

            if _tf4_enabled and "RIDING" in _tf4_signals and not _tf4_up and _tf4_mode == "soft":
                score -= _tf4_penalty; bd["NO_4H_TREND"] = -_tf4_penalty
            score = min(score, SCORE_CAP)
            if not (_tf4_enabled and _tf4_mode == "hard" and "RIDING" in _tf4_signals and not _tf4_up):
                candidates.append({
                    "label": "RIDING", "history_tf": "RIDING", "score": score,
                    "priority": 2, "bucket": final_bucket(score, "RIDING", cfg),
                    "timeframe": "15m", "price": tf15["price"],
                    "ref_price": tf15.get("riding_break_close"),
                    "obv_slope": tf15.get("obv_slope"),
                    "cvd_ratio": tf15.get("cvd_ratio"),
                    "recent_long_ok": tf15.get("recent_long_ok"),
                    "htf_1h_up": bool(tf1h.get("ema_trend_up")),
                    "htf_4h_up": bool(tf4h.get("ema_trend_up")),
                    "breakdown": bd,
                })

    # ── HOLD ──────────────────────────────────────────────────────────────
    if _trad_signals_eligible and cfg.g("active_signals", "HOLD"):
        _require_obv_hold = cfg.g("hold", "HOLD_REQUIRE_OBV_RISING", default=False)
        _hold_obv_bars = cfg.g("persistence", "HOLD_OBV_BARS", default=1) if _persist_on else 1
        _obv_ok_hold = (not _require_obv_hold) or tf15.get("obv_rising", False)
        if _require_obv_hold and _hold_obv_bars >= 2:
            _obv_ok_hold = _obv_ok_hold and (tf15.get("obv_slope_prev", 0) >= cfg.g("indicators", "OBV_RISING_MIN", default=0.05))
        _cvd_ok_hold = (not cfg.g("hold", "HOLD_REQUIRE_CVD_BULLISH", default=False)) or tf15.get("cvd_bullish", False)
        if (tf15.get("hold_recent_break") and tf15.get("hold_kept_zone")
            and tf15.get("hold_pullback_ok") and tf15.get("hold_strong")
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
            if tf1h.get("dist_to_res", 0) > dist_high_min:
                score += dist_high_b; bd["DIST_HIGH"] = dist_high_b
            else:
                score += dist_low_b;  bd["DIST_LOW"]  = dist_low_b
            if tf15.get("obv_rising"):
                score += obv_up_b; bd["OBV_RISING"] = obv_up_b
            elif tf15.get("obv_slope", 0) < -cfg.g("indicators", "OBV_RISING_MIN", default=0.05):
                score += obv_dn_pen; bd["OBV_FALLING"] = obv_dn_pen
            if tf15.get("cvd_bullish"):
                score += cvd_up_b; bd["CVD_BULLISH"] = cvd_up_b
            elif tf15.get("cvd_ratio", 0) < -cfg.g("indicators", "CVD_BULLISH_MIN", default=0.05):
                score += cvd_dn_pen; bd["CVD_BEARISH"] = cvd_dn_pen
            if not tf15.get("recent_long_ok", True):
                score += struct_pen; bd["STRUCT"] = struct_pen
            # Bonus por momentum extremo: OBV explosivo + CVD bullish + lejos de resistencia 1h
            if (tf15.get("obv_slope", 0) >= momentum_obv
                and tf15.get("cvd_bullish")
                and tf1h.get("dist_to_res", 0) >= momentum_dist):
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
                score -= _tf4_penalty; bd["NO_4H_TREND"] = -_tf4_penalty
            score = min(score, SCORE_CAP)
            if not (_tf4_enabled and _tf4_mode == "hard" and "HOLD" in _tf4_signals and not _tf4_up):
                candidates.append({
                    "label": "HOLD", "history_tf": "HOLD", "score": score,
                    "priority": 3, "bucket": final_bucket(score, "HOLD", cfg),
                    "timeframe": "15m", "price": tf15["price"],
                    "ref_price": tf15.get("riding_break_ref") or tf15["recent_max"],
                    "obv_slope": tf15.get("obv_slope"),
                    "cvd_ratio": tf15.get("cvd_ratio"),
                    "recent_long_ok": tf15.get("recent_long_ok"),
                    "htf_1h_up": bool(tf1h.get("ema_trend_up")),
                    "htf_4h_up": bool(tf4h.get("ema_trend_up")),
                    "breakdown": bd,
                })

    if not candidates:
        return None

    # FORMING_CANDLE_PENALTY — mismo comportamiento que screener.py.
    # En backtest las velas son históricas (candle_status="closed") salvo que el caller
    # inyecte _is_forming=True en el candidato (via analyze_forming_lateness, futuro).
    FORMING_CANDLE_PENALTY      = cfg.g("scoring", "FORMING_CANDLE_PENALTY",       default=3)
    IMMEDIATE_MIN_SCORE         = cfg.g("scoring", "IMMEDIATE_MIN_SCORE",           default=13)
    IMMEDIATE_MIN_SCORE_FORMING = cfg.g("scoring", "IMMEDIATE_MIN_SCORE_FORMING",   default=9)
    BEST_MIN_SCORE_FORMING      = cfg.g("scoring", "BEST_MIN_SCORE_FORMING",        default=9)
    STRONG_MIN_SCORE_FORMING    = cfg.g("scoring", "STRONG_MIN_SCORE_FORMING",      default=8)

    def _final_bucket_forming(score):
        if score >= BEST_MIN_SCORE_FORMING:
            return "BEST"
        if score >= STRONG_MIN_SCORE_FORMING:
            return "STRONG"
        return "WATCH"

    for c in candidates:
        if c.get("_is_forming"):
            cs = "forming"
        else:
            cs = (tf_data.get(c["timeframe"]) or {}).get("candle_status", "closed")
        c["candle_status"] = cs
        if cs == "forming":
            c["score"] = max(0, c["score"] - FORMING_CANDLE_PENALTY)
            c["bucket"] = _final_bucket_forming(c["score"])
            if c.get("immediate") and c["score"] < IMMEDIATE_MIN_SCORE_FORMING:
                c["immediate"] = False

    # Penalización suave por EMA 1h no alcista (solo cuando EMA_GATE_HARD=false).
    # EXPLOSION queda exenta: tiene su criterio propio agresivo y bypassa EMA por diseño.
    if not _ema_gate_hard and not tf1h.get("ema_trend_up"):
        _ema_pen = int(cfg.g("indicators", "EMA_SOFT_PENALTY", default=-2))
        for c in candidates:
            if c.get("history_tf") == "EXPLOSION":
                continue
            c["score"] = max(0, c["score"] + _ema_pen)
            c["bucket"] = final_bucket(c["score"], c["history_tf"], cfg)
            if "breakdown" in c:
                c["breakdown"]["EMA_SOFT"] = _ema_pen

    # Final cap loop (mismo screener.py). Redundante porque cada bloque ya
    # aplicó min(score, SCORE_CAP), pero se mantiene para que la simulación sea espejo.
    for c in candidates:
        if c["score"] > SCORE_CAP:
            orig = c["score"]
            c["score"] = SCORE_CAP
            c["bucket"] = final_bucket(c["score"], c["history_tf"], cfg)
            if "breakdown" in c:
                c["breakdown"]["SCORE_CAP_TRUNC"] = SCORE_CAP - orig

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


def calculate_outcomes(df_15m, alert_idx, alert_price):
    alert_ts = int(df_15m["close_time"].iloc[alert_idx])
    outcomes = {}
    for offset_min, name in zip(OUTCOME_OFFSETS_MIN, OUTCOME_NAMES):
        target_ts = alert_ts + offset_min * 60 * 1000
        idx = find_idx_at_or_before(df_15m, target_ts)
        if idx > alert_idx:
            outcomes[name] = float(df_15m["close"].iloc[idx])
        else:
            outcomes[name] = None

    end_4h = alert_ts + 240 * 60 * 1000
    end_24h = alert_ts + 1440 * 60 * 1000
    end_4h_idx = find_idx_at_or_before(df_15m, end_4h)
    end_24h_idx = find_idx_at_or_before(df_15m, end_24h)
    max_high_4h = min_low_4h = max_high_24h = min_low_24h = None
    if end_4h_idx > alert_idx:
        w = df_15m.iloc[alert_idx + 1:end_4h_idx + 1]
        if len(w) > 0:
            max_high_4h = float(w["high"].max())
            min_low_4h = float(w["low"].min())
    if end_24h_idx > alert_idx:
        w = df_15m.iloc[alert_idx + 1:end_24h_idx + 1]
        if len(w) > 0:
            max_high_24h = float(w["high"].max())
            min_low_24h = float(w["low"].min())
    outcomes["max_high_4h"] = max_high_4h
    outcomes["min_low_4h"] = min_low_4h
    outcomes["max_high_24h"] = max_high_24h
    outcomes["min_low_24h"] = min_low_24h
    outcomes["entry_price"] = alert_price
    outcomes["complete"] = max_high_24h is not None
    return outcomes


# ════════════════════════════════════════════════════════════════════════════
# ANÁLISIS FORMING — lateness y fakeout sobre alertas EXPLOSION
# ════════════════════════════════════════════════════════════════════════════

def _build_partial_bar(df_1m, open_5m_ms, elapsed_min):
    """Agrega los primeros elapsed_min minutos de una vela 5m desde 1m bars.
    Retorna dict con OHLCV + elapsed_frac_real, o None si no hay datos 1m."""
    end_ms = open_5m_ms + elapsed_min * 60_000
    ot = df_1m["open_time"].values.astype(np.int64)
    lo = np.searchsorted(ot, open_5m_ms, side="left")
    hi = np.searchsorted(ot, end_ms, side="left")
    if hi <= lo:
        return None
    bars = df_1m.iloc[lo:hi]
    n = len(bars)
    return {
        "open":         float(bars["open"].iloc[0]),
        "high":         float(bars["high"].max()),
        "low":          float(bars["low"].min()),
        "close":        float(bars["close"].iloc[-1]),
        "volume":       float(bars["volume"].sum()),
        "elapsed_frac": n / 5.0,  # 5 bars de 1m en una vela 5m
        "n_bars":       n,
    }


def _check_forming_explosion(partial, prev_close, vol_mean, recent_max, cfg,
                             width_expansion_closed=None):
    """Aplica criterios EXPLOSION sobre una vela parcial.
    width_expansion_closed: BB expansion calculada con el partial close incluido — replica
    el cómputo de screener.py para el forming candle.
    Retorna (ok: bool, features: dict)."""
    if vol_mean <= 0 or partial is None:
        return False, {}
    elapsed_frac = max(partial["elapsed_frac"], 0.01)
    vol_pace = (partial["volume"] / elapsed_frac) / vol_mean
    rng = max(partial["high"] - partial["low"], 1e-12)
    body = abs(partial["close"] - partial["open"]) / rng
    chg  = (partial["close"] / prev_close - 1) if prev_close > 0 else 0.0
    ex_min_vol  = cfg.g("scoring_explosion", "MIN_VOL_RATIO",    default=5.0)
    ex_min_body = cfg.g("scoring_explosion", "MIN_BODY_PCT",     default=0.85)
    ex_min_chg  = cfg.g("scoring_explosion", "MIN_CLOSE_CHANGE", default=0.025)
    ex_min_bb   = cfg.g("scoring_explosion", "MIN_BB_EXPANSION", default=0.5)
    bkdist = (partial["close"] / recent_max - 1) if recent_max > 0 else 0.0
    bb_ok = (width_expansion_closed is None) or (width_expansion_closed >= ex_min_bb)
    features = {
        "vol_pace":              vol_pace,
        "body":                  body,
        "chg":                   chg,
        "bkdist":                bkdist,
        "elapsed_frac":          elapsed_frac,
        "width_expansion_proxy": width_expansion_closed,
    }
    ok = bool(vol_pace >= ex_min_vol and body >= ex_min_body and chg >= ex_min_chg and bb_ok)
    return ok, features


def _forming_quality(gain, dd):
    """Clasifica un entry por calidad basada en max_gain 24h y drawdown 24h."""
    if gain is None or dd is None:
        return "unknown"
    if gain < 0.02 and dd > 0.03:
        return "fakeout"
    if gain >= 0.05:
        return "good"
    return "mid"


def analyze_forming_lateness(alerts, klines_1m, klines, cfg,
                             scan_minutes=(1, 2, 3, 4)):
    """Análisis post-proceso: para cada alerta EXPLOSION cerrada, pregunta en qué
    minuto de esa vela 5m habrían disparado los criterios de EXPLOSION_FORMING.

    Parámetros:
      alerts       — lista de alert_record producidos por simulate()
      klines_1m    — {sym: df_1m}
      klines       — {sym: {'5m': df_5m, ...}} (klines cerradas, ya descargadas)
      cfg          — Config base
      scan_minutes — en qué minutos dentro de la vela verificar (default 1..4)

    Retorna lista de dicts por alerta EXPLOSION con métricas de lateness.
    También imprime un resumen por consola."""
    explosion_alerts = [a for a in alerts if a.get("signal_type") == "EXPLOSION"]
    if not explosion_alerts:
        print("  [forming] Sin alertas EXPLOSION — nada que analizar.")
        return []

    results = []
    ex_late_max = cfg.g("scoring_explosion", "LATE_ENTRY_MAX_DIST", default=0.03)
    RECENT_LOOKBACK = cfg.g("indicators", "RECENT_LOOKBACK", default=15)

    for a in explosion_alerts:
        sym = a["symbol"]
        df_1m = klines_1m.get(sym)
        df_5m = (klines.get(sym) or {}).get("5m")
        if df_1m is None or df_5m is None:
            continue

        # Encontrar la barra 5m cerrada que produjo la alerta
        alerted_ms = int(datetime.fromisoformat(a["alerted_at"]).timestamp() * 1000)
        idx_5m = find_idx_at_or_before(df_5m, alerted_ms, timecol="close_time")
        if idx_5m < 2:
            continue

        open_5m_ms  = int(df_5m["open_time"].iloc[idx_5m])
        prev_close  = float(df_5m["close"].iloc[idx_5m - 1])
        vol_mean    = float(df_5m["volume"].iloc[max(0, idx_5m - 20):idx_5m].mean())
        recent_max  = float(df_5m["high"].iloc[max(0, idx_5m - RECENT_LOOKBACK - 1):idx_5m - 1].max())
        closed_chg  = float(a["entry_price"] / prev_close - 1) if prev_close > 0 else 0.0
        closed_dist = float(a["entry_price"] / recent_max - 1) if recent_max > 0 else 0.0

        # Serie base de closes cerrados (hasta idx_5m - 1) para recalcular BB por minuto
        _bb_base = df_5m["close"].iloc[max(0, idx_5m - 40):idx_5m].reset_index(drop=True)

        first_fire_min = None
        per_min = {}
        for m in scan_minutes:
            partial = _build_partial_bar(df_1m, open_5m_ms, m)
            if partial is None:
                per_min[m] = {"ok": False}
                continue

            # BB con partial close incluido — mirror exacto de screener.py:683-689
            _series_m = pd.concat([_bb_base, pd.Series([partial["close"]])], ignore_index=True)
            _bb_t = ta.volatility.BollingerBands(_series_m, window=20, window_dev=2)
            _hb, _lb, _ma = _bb_t.bollinger_hband(), _bb_t.bollinger_lband(), _bb_t.bollinger_mavg()
            if (pd.notna(_ma.iloc[-1]) and pd.notna(_ma.iloc[-2])
                    and _ma.iloc[-1] > 0 and _ma.iloc[-2] > 0):
                _wc = (_hb.iloc[-1] - _lb.iloc[-1]) / _ma.iloc[-1]
                _wp = (_hb.iloc[-2] - _lb.iloc[-2]) / _ma.iloc[-2]
                wexp_m = safe_pct(_wc, _wp) if _wp else None
            else:
                wexp_m = None

            ok, feats = _check_forming_explosion(partial, prev_close, vol_mean, recent_max, cfg,
                                                 width_expansion_closed=wexp_m)
            per_min[m] = {"ok": ok, **feats}
            if ok and first_fire_min is None:
                first_fire_min = m

        # Precio al momento de first_fire_min (para calcular lateness real)
        entry_price_forming = None
        if first_fire_min is not None:
            pb = _build_partial_bar(df_1m, open_5m_ms, first_fire_min)
            if pb:
                entry_price_forming = pb["close"]

        # Calidad 24h: max_gain y drawdown desde cada entry usando df_5m forward
        fwd_end = min(idx_5m + 289, len(df_5m))
        df_fwd  = df_5m.iloc[idx_5m + 1 : fwd_end]
        if len(df_fwd) > 0:
            max_high_fwd = float(df_fwd["high"].max())
            min_low_fwd  = float(df_fwd["low"].min())
            ep_c = float(a["entry_price"])
            max_gain_24h_closed  = max_high_fwd / ep_c - 1  if ep_c > 0 else None
            drawdown_24h_closed  = (ep_c - min_low_fwd) / ep_c  if ep_c > 0 else None
            quality_closed       = _forming_quality(max_gain_24h_closed, drawdown_24h_closed)
            if entry_price_forming is not None and entry_price_forming > 0:
                max_gain_24h_forming = max_high_fwd / entry_price_forming - 1
                drawdown_24h_forming = (entry_price_forming - min_low_fwd) / entry_price_forming
                quality_forming      = _forming_quality(max_gain_24h_forming, drawdown_24h_forming)
            else:
                max_gain_24h_forming = drawdown_24h_forming = None
                quality_forming = None
        else:
            max_gain_24h_closed = drawdown_24h_closed = None
            quality_closed = "unknown"
            max_gain_24h_forming = drawdown_24h_forming = None
            quality_forming = None

        results.append({
            "symbol":               sym,
            "alerted_at":           a["alerted_at"],
            "score":                a.get("score"),
            "bucket":               a.get("bucket"),
            "entry_price_closed":   a["entry_price"],
            "closed_dist":          closed_dist,
            "closed_chg":           closed_chg,
            "first_fire_min":       first_fire_min,
            "entry_price_forming":  entry_price_forming,
            "forming_dist":         (entry_price_forming / recent_max - 1) if entry_price_forming and recent_max > 0 else None,
            "per_min":              per_min,
            "late_entry":           closed_dist > ex_late_max,
            "max_gain_24h_closed":  max_gain_24h_closed,
            "drawdown_24h_closed":  drawdown_24h_closed,
            "quality_closed":       quality_closed,
            "max_gain_24h_forming": max_gain_24h_forming,
            "drawdown_24h_forming": drawdown_24h_forming,
            "quality_forming":      quality_forming,
        })

    if not results:
        print("  [forming] Sin resultados (1m data faltante para todos los símbolos).")
        return results

    _print_forming_lateness_summary(results, scan_minutes)
    return results


def _print_forming_lateness_summary(results, scan_minutes):
    n = len(results)
    print()
    print("═" * 70)
    print(" ANÁLISIS FORMING — lateness EXPLOSION")
    print("═" * 70)
    print(f"\nAlertas EXPLOSION analizadas: {n}")

    # Distribución de first_fire_min
    fired = [r for r in results if r["first_fire_min"] is not None]
    never = n - len(fired)
    print(f"\n  Habrían disparado en forming: {len(fired)}/{n} ({len(fired)*100//n}%)")
    print(f"  No habrían disparado (1m insuficiente o criterios no cumplidos antes del cierre): {never}")

    if fired:
        by_min = {}
        for r in fired:
            m = r["first_fire_min"]
            by_min[m] = by_min.get(m, 0) + 1
        print("\n  Distribución first_fire_min:")
        for m in sorted(by_min):
            pct = by_min[m] * 100 // len(fired)
            print(f"    min {m}: {by_min[m]:>4} ({pct}%)")

        # Lateness: dist sobre recent_max en entry forming vs closed
        dists_closed  = [r["closed_dist"]  for r in fired if r["closed_dist"]  is not None]
        dists_forming = [r["forming_dist"] for r in fired if r["forming_dist"] is not None]
        if dists_closed and dists_forming:
            def pct_stats(vals, label):
                s = sorted(vals)
                n = len(s)
                med = s[n // 2]
                p90 = s[int(n * 0.9)]
                avg = sum(s) / n
                print(f"    {label}: mediana {med:+.2%}, p90 {p90:+.2%}, promedio {avg:+.2%}")
            print("\n  Distancia sobre recent_max al momento de alerta:")
            pct_stats(dists_closed,  "CLOSED  ")
            pct_stats(dists_forming, "FORMING ")
            saving = [c - f for c, f in zip(dists_closed, dists_forming) if c is not None and f is not None]
            if saving:
                s = sorted(saving)
                print(f"\n  Ahorro de lateness (closed - forming):")
                print(f"    mediana: {s[len(s)//2]:+.2%}, p90: {s[int(len(s)*0.9)]:+.2%}")

    # Alertas tardías (ya extendidas al cierre)
    late = sum(1 for r in results if r.get("late_entry"))
    if late:
        print(f"\n  Alertas que ya tenían late_entry_penalty al cierre: {late}/{n} ({late*100//n}%)")

    # Calidad de entry 24h: fakeout / mid / good
    def _quality_table(records, key_quality, key_gain, key_dd, label):
        vals = [(r.get(key_quality), r.get(key_gain), r.get(key_dd))
                for r in records if r.get(key_quality) is not None]
        if not vals:
            return
        total = len(vals)
        cats   = {"good": 0, "mid": 0, "fakeout": 0, "unknown": 0}
        gains, dds = [], []
        for q, g, d in vals:
            cats[q] = cats.get(q, 0) + 1
            if g is not None:
                gains.append(g)
            if d is not None:
                dds.append(d)
        print(f"\n  {label}  (n={total})")
        for cat in ("good", "mid", "fakeout", "unknown"):
            c = cats.get(cat, 0)
            if c:
                print(f"    {cat:<10}: {c:>3} ({c*100//total:>3}%)")
        if gains:
            gains.sort()
            print(f"    max_gain_24h mediana: {gains[len(gains)//2]:+.2%}")
        if dds:
            dds.sort()
            print(f"    drawdown_24h mediana: {dds[len(dds)//2]:+.2%}")

    print()
    print("─" * 70)
    print(" CALIDAD DE ENTRY 24h")
    print("─" * 70)
    _quality_table(results, "quality_closed",  "max_gain_24h_closed",  "drawdown_24h_closed",  "CLOSED  (todas las alertas EXPLOSION)")
    forming_with_data = [r for r in results if r.get("quality_forming") is not None]
    _quality_table(forming_with_data, "quality_forming", "max_gain_24h_forming", "drawdown_24h_forming", "FORMING (alertas que habrían disparado en forming)")


def _build_candidates(klines, prepared, cfg, scan_ts, snapshot_pairs, derivatives):
    """Primer pase del scan: construye la lista de candidatos (sym, ts, tf_data, idx_15m)
    que pasan validación de datos. Variant-independiente para cfgs con el mismo _analyze_key.
    Cada elemento: (scan_i, ts_ms, sym, tf_data, idx_15m)."""
    deriv_enabled = bool(derivatives) and cfg.g("derivatives", "ENABLED", default=False)
    deriv_lookback = cfg.g("derivatives", "OI_LOOKBACK_MIN", default=30)

    # Pre-cachear parámetros de cfg una sola vez (evita ~24M cfg.g() en el hot loop)
    params = _build_analyze_params(cfg)

    # Precalcular índices de barras por (sym, TF) con searchsorted vectorizado.
    scan_ts_arr = np.asarray(scan_ts, dtype=np.int64)
    bar_idx_cache = {}
    for sym, tfs in klines.items():
        by_tf = {}
        for tf in ("5m", "15m", "1h", "4h"):
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
        for tf in ("5m", "15m", "1h"):
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
        # 4h opcional para filtro de tendencia (fail-safe si falta o índice insuficiente)
        _idxs_4h = by_tf.get("4h")
        if _idxs_4h is not None:
            _idx4 = int(_idxs_4h[i])
            if _idx4 >= 80:
                _df4 = klines.get(sym, {}).get("4h")
                if _df4 is not None:
                    _r4 = analyze_at_time(_df4, _idx4, cfg)
                    if _r4 is not None:
                        tf_data["4h"] = _r4
        if deriv_enabled:
            oi_delta, funding_rate = lookup_derivatives_at(
                derivatives.get(sym), ts_ms, oi_lookback_min=deriv_lookback)
            tf_data["15m"]["oi_delta_30m"] = oi_delta
            tf_data["15m"]["funding_rate"] = funding_rate
        idx_15m = int(by_tf["15m"][i])
        return (i, ts_ms, sym, tf_data, idx_15m)

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
    outcomes_cache[(sym, idx_15m)] permite reutilizar outcomes entre variantes."""
    last_alert_ts = {}
    # sim_alert_history: lista (ts_ms, symbol, history_tf) para simular fetch_history()
    # del screener. Cada scan recomputa counts_history a partir de las alertas emitidas
    # en las últimas HISTORY_HOURS, y se la pasa a classify() para aplicar
    # LATE_REPEAT_PENALTY igual que en producción.
    sim_alert_history = []
    alerts = []

    for (i, ts_ms, sym, tf_data, idx_15m) in candidates:
        # Ventana móvil: descartar alertas más viejas que HISTORY_HOURS para que counts_history
        # refleje sólo lo que el screener real vería en su fetch_history().
        cutoff_ms = ts_ms - history_window_ms
        if sim_alert_history and sim_alert_history[0][0] < cutoff_ms:
            sim_alert_history = [h for h in sim_alert_history if h[0] >= cutoff_ms]
        counts_history = {}
        for (_, s, h) in sim_alert_history:
            counts_history[(s, h)] = counts_history.get((s, h), 0) + 1

        alert = classify(sym, tf_data, cfg, counts_history)
        if alert is None:
            continue

        key = (sym, alert["history_tf"])
        cooldown_ms = cooldown_min_by_state.get(alert["history_tf"], 60) * 60 * 1000
        if key in last_alert_ts and (ts_ms - last_alert_ts[key]) < cooldown_ms:
            continue
        last_alert_ts[key] = ts_ms

        # Registrar en historia simulada para la próxima scan (LATE_REPEAT)
        sim_alert_history.append((ts_ms, sym, alert["history_tf"]))

        oc_key = (sym, idx_15m)
        if outcomes_cache is not None and oc_key in outcomes_cache:
            outcomes = outcomes_cache[oc_key]
        else:
            df_15m = klines[sym]["15m"]
            outcomes = calculate_outcomes(df_15m, idx_15m, alert["price"])
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
            "htf_1h_up": alert.get("htf_1h_up"),
            "htf_4h_up": alert.get("htf_4h_up"),
            "candle_status": alert.get("candle_status"),
            **outcomes,
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


def summarize(alerts, label="resultados"):
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
        moves = [pct_move(a["max_high_24h"], a["entry_price"]) for a in items]
        moves = [m for m in moves if m is not None]
        drops = [pct_move(a["min_low_24h"], a["entry_price"]) for a in items]
        drops = [d for d in drops if d is not None]
        wins = sum(1 for m in moves if m >= 2.0)
        avg_max = sum(moves) / len(moves) if moves else 0
        avg_dd = sum(drops) / len(drops) if drops else 0
        winrate = wins * 100 / len(moves) if moves else 0
        print(f"  {b:<6} {len(items):>4} — max_24h: {avg_max:+5.2f}%, drawdown: {avg_dd:+5.2f}%, "
              f"win >2%: {winrate:.0f}%")

    print("\nCVD análisis:")
    cvd_groups = {"bullish (>+0.05)": [], "neutral": [], "bearish (<-0.05)": []}
    for a in alerts:
        c = a.get("cvd_ratio") or 0
        m = pct_move(a["max_high_24h"], a["entry_price"])
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
            print(f"  CVD {name:<20} n={len(moves):>3}  avg max 24h: {sum(moves)/len(moves):+5.2f}%")

    print("\nOBV análisis:")
    obv_groups = {"rising (>+0.05)": [], "neutral": [], "falling (<-0.05)": []}
    for a in alerts:
        o = a.get("obv_slope") or 0
        m = pct_move(a["max_high_24h"], a["entry_price"])
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
            print(f"  OBV {name:<20} n={len(moves):>3}  avg max 24h: {sum(moves)/len(moves):+5.2f}%")

    print("\nrecent_long_ok análisis:")
    for ok in (True, False):
        items = [a for a in alerts if a.get("recent_long_ok") == ok]
        moves = [pct_move(a["max_high_24h"], a["entry_price"]) for a in items]
        moves = [m for m in moves if m is not None]
        drops = [pct_move(a["min_low_24h"], a["entry_price"]) for a in items]
        drops = [d for d in drops if d is not None]
        if moves:
            print(f"  {str(ok):<5} n={len(moves):>3}  avg max 24h: {sum(moves)/len(moves):+5.2f}%, "
                  f"drawdown: {sum(drops)/len(drops):+5.2f}%")

    ranked = [(pct_move(a["max_high_24h"], a["entry_price"]) or -999, a) for a in alerts]
    ranked.sort(key=lambda x: x[0], reverse=True)
    print("\nTOP 30 movers detectados:")
    print(f"  {'#':>3} {'Symbol':<14} {'Type':<10} {'Score':>5} {'Bucket':<7} {'max24h':>8} "
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


def compare_runs(alerts_a, label_a, alerts_b, label_b, period_days=7):
    print()
    print("═" * 70)
    print(f" COMPARACIÓN: {label_a}  vs  {label_b}  ({period_days} días)")
    print("═" * 70)

    def stats(alerts):
        if not alerts:
            return {"n": 0, "avg_max_24h": 0, "avg_drawdown": 0, "win_2pct": 0, "win_5pct": 0,
                    "best_n": 0, "strong_n": 0, "breakout_n": 0,
                    "best_max": 0, "strong_max": 0, "explosivos_n": 0,
                    "best_per_day": 0, "best_win_5pct": 0, "best_win_10pct": 0,
                    "top30_in_best": 0, "top30_in_best_strong": 0,
                    "best_dd": 0, "best_rr": 0}
        moves = [pct_move(a["max_high_24h"], a["entry_price"]) for a in alerts]
        moves = [m for m in moves if m is not None]
        drops = [pct_move(a["min_low_24h"], a["entry_price"]) for a in alerts]
        drops = [d for d in drops if d is not None]
        best = [a for a in alerts if a["bucket"] == "BEST"]
        strong = [a for a in alerts if a["bucket"] == "STRONG"]
        breakouts = [a for a in alerts if a["signal_type"] == "BREAKOUT"]
        best_moves = [pct_move(a["max_high_24h"], a["entry_price"]) for a in best]
        best_moves = [m for m in best_moves if m is not None]
        best_drops = [pct_move(a["min_low_24h"], a["entry_price"]) for a in best]
        best_drops = [d for d in best_drops if d is not None]
        strong_moves = [pct_move(a["max_high_24h"], a["entry_price"]) for a in strong]
        strong_moves = [m for m in strong_moves if m is not None]
        explosivos = [m for m in moves if m >= 20]

        # Top 30 movers — cuántos están en BEST y BEST+STRONG
        ranked = sorted(alerts,
                        key=lambda a: pct_move(a["max_high_24h"], a["entry_price"]) or -999,
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

        return {
            "n": len(alerts),
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
    print(f"  ★ CATCH RATE (lo más importante):")
    print(f"  {'Top 30 movers en BEST':<32} {s_a['top30_in_best']:>14}/30 {s_b['top30_in_best']:>14}/30")
    print(f"  {'Top 30 movers en BEST+STRONG':<32} {s_a['top30_in_best_strong']:>14}/30 {s_b['top30_in_best_strong']:>14}/30")
    print(f"  {'-'*32} {'-'*15} {'-'*15}")
    print(f"  ★ CALIDAD del bucket BEST:")
    print(f"  {'Avg max 24h BEST':<32} {s_a['best_max']:>+14.2f}% {s_b['best_max']:>+14.2f}%")
    print(f"  {'Avg drawdown BEST':<32} {s_a['best_dd']:>+14.2f}% {s_b['best_dd']:>+14.2f}%")
    print(f"  {'R/R BEST':<32} {s_a['best_rr']:>15.2f} {s_b['best_rr']:>15.2f}")
    print(f"  {'Win >5% en BEST':<32} {s_a['best_win_5pct']:>14.0f}%  {s_b['best_win_5pct']:>14.0f}%")
    print(f"  {'Win >10% en BEST':<32} {s_a['best_win_10pct']:>14.0f}%  {s_b['best_win_10pct']:>14.0f}%")
    print(f"  {'-'*32} {'-'*15} {'-'*15}")
    print(f"  Otros:")
    print(f"  {'Avg max 24h global':<32} {s_a['avg_max_24h']:>+14.2f}% {s_b['avg_max_24h']:>+14.2f}%")
    print(f"  {'Avg max 24h STRONG':<32} {s_a['strong_max']:>+14.2f}% {s_b['strong_max']:>+14.2f}%")
    print(f"  {'Avg drawdown global':<32} {s_a['avg_drawdown']:>+14.2f}% {s_b['avg_drawdown']:>+14.2f}%")
    print(f"  {'Win >2% global':<32} {s_a['win_2pct']:>14.0f}%  {s_b['win_2pct']:>14.0f}%")
    print(f"  {'Win >5% global':<32} {s_a['win_5pct']:>14.0f}%  {s_b['win_5pct']:>14.0f}%")


# ════════════════════════════════════════════════════════════════════════════
# MAIN
# ════════════════════════════════════════════════════════════════════════════

def run_backtest(cfg, weeks, klines, start_dt, end_dt, snapshot_pairs=None,
                 label="run", scan_interval_min=SCAN_INTERVAL_MIN, derivatives=None,
                 prepared_cache=None, analyze_cache=None, outcomes_cache=None,
                 audit_mode=False):
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
                for tf in ("5m", "15m", "1h", "4h"):
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
    summarize(alerts, label=label)
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
                        help="Ignora y no escribe caché de disco de klines/derivatives")
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
    parser.add_argument("--forming-analysis", action="store_true",
                        help="Post-análisis de lateness forming sobre alertas EXPLOSION: "
                             "descarga 1m klines y mide en qué minuto habrían disparado.")
    parser.add_argument("--forming-scan-points", default="1,2,3,4",
                        help="Minutos dentro de la vela 5m a verificar para forming "
                             "(default '1,2,3,4'). Usado con --forming-analysis.")
    parser.add_argument("--audit-scoring", action="store_true",
                        help="Incluye breakdown de scoring por alerta en el JSON de salida. "
                             "Necesario para audit_scoring.py.")
    args = parser.parse_args()
    _audit = getattr(args, "audit_scoring", False)

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
    need_deriv = False
    for p in cfg_paths_for_run:
        try:
            if Config(p).g("derivatives", "ENABLED", default=False):
                need_deriv = True
                break
        except Exception:
            pass

    derivatives = None
    if need_deriv:
        print(f"\n[2b/4] Descargando derivatives (OI + funding) — al menos un cfg los pide...")
        deriv_symbols = list(klines.keys())
        derivatives = download_all_derivatives(deriv_symbols, start_dt, end_dt)

    print(f"\n[3/4] Ejecutando simulación...")
    all_results = {}
    prepared_cache = {}  # compartido entre cfgs con mismos parámetros de indicadores
    analyze_cache  = {}  # compartido entre cfgs con mismo _analyze_key (variant-independent)
    outcomes_cache = {}  # compartido entre cfgs: outcomes(sym, idx_15m) son variant-independent

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
                                   audit_mode=_audit)
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
                                    audit_mode=_audit)
            elapsed  = time.perf_counter() - t_cfg
            prep_st  = "miss" if len(prepared_cache) > prev_prep else "hit"
            ana_st   = "miss" if len(analyze_cache)  > prev_ana  else "hit"
            print(f"  [cfg {vi}/{n_variants}] {cfg_stem}  {elapsed:.1f}s  "
                  f"prep={prep_st} ana={ana_st}")
            all_results[cfg_stem] = alerts_v
            compare_runs(alerts_base, Path(base_path).stem, alerts_v, cfg_stem,
                         period_days=period_days)
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
                                  audit_mode=_audit)
        alerts_new = run_backtest(cfg_new, args.weeks, klines, start_dt, end_dt,
                                  snapshot_pairs, label=f"NEW ({args.compare[1]})",
                                  scan_interval_min=args.scan_interval_min,
                                  derivatives=derivatives, prepared_cache=prepared_cache,
                                  analyze_cache=analyze_cache, outcomes_cache=outcomes_cache,
                                  audit_mode=_audit)
        compare_runs(alerts_old, args.compare[0], alerts_new, args.compare[1],
                     period_days=period_days)
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
                                  audit_mode=_audit)
            all_results[name] = alerts
        if "full (todo activo)" in all_results:
            for name, alerts in all_results.items():
                if name != "full (todo activo)":
                    compare_runs(all_results["full (todo activo)"], "full", alerts, name,
                                 period_days=period_days)
    else:
        alerts = run_backtest(cfg_main, args.weeks, klines, start_dt, end_dt,
                              snapshot_pairs, label=f"config: {args.config}",
                              scan_interval_min=args.scan_interval_min,
                              derivatives=derivatives, prepared_cache=prepared_cache,
                              analyze_cache=analyze_cache, outcomes_cache=outcomes_cache,
                              audit_mode=_audit)
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

    # ── Análisis forming (opcional) ────────────────────────────────────────────
    if args.forming_analysis:
        scan_points = [int(m.strip()) for m in args.forming_scan_points.split(",") if m.strip()]
        # Tomamos las alertas del run principal (o del baseline en compare/variants)
        main_alerts = (all_results.get("main")
                       or all_results.get(args.config)
                       or next(iter(all_results.values()), []))
        explosion_syms = list({a["symbol"] for a in main_alerts if a.get("signal_type") == "EXPLOSION"})
        if not explosion_syms:
            print("\n[forming] Sin alertas EXPLOSION en el run — no hay nada que analizar.")
        else:
            print(f"\n[forming] Descargando 1m klines para {len(explosion_syms)} pares con EXPLOSION...")
            klines_1m = download_1m_klines(explosion_syms, start_dt, end_dt)
            forming_results = analyze_forming_lateness(
                main_alerts, klines_1m, klines, cfg_main,
                scan_minutes=tuple(scan_points),
            )
            if args.out and forming_results:
                forming_path = Path(args.out).with_stem(Path(args.out).stem + "_forming")
                try:
                    with open(forming_path, "w", encoding="utf-8") as f:
                        json.dump(forming_results, f, indent=2, default=str)
                    print(f"  Formings guardados: {forming_path.absolute()}")
                except OSError:
                    pass

    print(f"\nDONE")


if __name__ == "__main__":
    main()
