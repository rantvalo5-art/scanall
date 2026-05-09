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
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone, timedelta
from pathlib import Path

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
MAX_DOWNLOAD_WORKERS = 8
MAX_PAIRS = 200

BINANCE_DATA_URL = "https://data-api.binance.vision/api/v3"
BINANCE_FALLBACK_URL = "https://api.binance.com/api/v3"
BINANCE_FAPI_URL = "https://fapi.binance.com"

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
    offset = 0
    PAGE = 1000
    while True:
        params = {
            "select": "run_at,symbol",
            "run_at": f"gte.{start_dt.isoformat()}",
            "and": f"(run_at.lte.{end_dt.isoformat()})",
            "limit": PAGE,
            "offset": offset,
            "order": "run_at.asc",
        }
        try:
            r = requests.get(
                f"{SUPABASE_URL}/rest/v1/screener_pairs_snapshot",
                headers=headers, params=params, timeout=30,
            )
            r.raise_for_status()
            rows = r.json()
        except Exception as e:
            print(f"  [snapshot] error fetching: {e}")
            break
        if not rows:
            break
        for row in rows:
            run = row["run_at"]
            sym = row["symbol"]
            runs_to_pairs.setdefault(run, []).append(sym)
            all_symbols.add(sym)
        if len(rows) < PAGE:
            break
        offset += PAGE
        print(f"  [snapshot] cargadas {offset} filas...")
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
    buffer_days = 3
    fetch_start = start_dt - timedelta(days=buffer_days)
    fetch_end = end_dt
    start_ms = int(fetch_start.timestamp() * 1000)
    end_ms = int(fetch_end.timestamp() * 1000)
    intervals = ["5m", "15m", "1h"]
    data = {}
    print(f"\n  Descargando {len(symbols)} pares × 3 timeframes = {len(symbols)*3} requests")
    print(f"  Rango: {fetch_start.date()} a {fetch_end.date()} ({buffer_days}d buffer + período)")
    completed = 0

    def fetch(sym, tf):
        return sym, tf, get_klines_range(sym, tf, start_ms, end_ms)

    with ThreadPoolExecutor(max_workers=MAX_DOWNLOAD_WORKERS) as ex:
        futures = [ex.submit(fetch, s, tf) for s in symbols for tf in intervals]
        for fut in as_completed(futures):
            sym, tf, df = fut.result()
            if df is None or len(df) < 50:
                continue
            data.setdefault(sym, {})[tf] = df
            completed += 1
            if completed % 50 == 0:
                print(f"    {completed}/{len(symbols)*3} descargados...")
    valid = {s: tfs for s, tfs in data.items() if len(tfs) == 3}
    print(f"  {len(valid)}/{len(symbols)} pares con data completa en los 3 TF")
    return valid


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
        oi = get_oi_history_range(sym, start_ms, end_ms)
        fr = get_funding_history_range(sym, start_ms, end_ms)
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

    ema_slow = ta.trend.EMAIndicator(close, window=EMA_SLOW).ema_indicator()
    ema_trend_up = price > ema_slow.iloc[-1] and ema_slow.iloc[-1] > ema_slow.iloc[-4]

    bb = ta.volatility.BollingerBands(close, window=20, window_dev=2)
    hband = bb.bollinger_hband()
    lband = bb.bollinger_lband()
    mavg = bb.bollinger_mavg()
    width_curr = ((hband.iloc[-1] - lband.iloc[-1]) / mavg.iloc[-1]) if mavg.iloc[-1] else 0.0
    width_prev = ((hband.iloc[-2] - lband.iloc[-2]) / mavg.iloc[-2]) if mavg.iloc[-2] else 0.0
    width_expansion = safe_pct(width_curr, width_prev)

    atr = ta.volatility.AverageTrueRange(high, low, close, window=14).average_true_range().iloc[-1]
    atr_pct = (atr / price * 100) if price > 0 else 0.0

    vol_mean = volume.iloc[-21:-1].mean()
    vol_ratio = (volume.iloc[-1] / vol_mean) if vol_mean else 0.0
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
    except Exception:
        obv_slope = 0.0
        obv_rising = False

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
        major_max = high.iloc[-(MAJOR_STRUCT_LOOKBACK + 2):-2].max()
        major_dist = (major_max - price) / price if price > 0 else 0.0
        major_struct_ok = major_dist <= MAJOR_STRUCT_MAX_DIST
    else:
        major_struct_ok = True

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
        "vol_ratio": vol_ratio,
        "vol_growth": vol_growth,
        "strong_close": strong_close,
        "candle_body_pct": candle_body_pct,
        "recent_max": recent_max,
        "near_recent_max": near_recent_max,
        "breakout": breakout,
        "breakout_distance": breakout_distance,
        "recent_max_long": recent_max_long,
        "recent_long_ok": recent_long_ok,
        "obv_slope": obv_slope,
        "obv_rising": obv_rising,
        "cvd_ratio": cvd_ratio,
        "cvd_bullish": cvd_bullish,
        "not_near_resistance": not_near_resistance,
        "dist_to_res": dist_to_res,
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
# CLASIFICACIÓN — refactorizada con lectura de scoring desde config.json
# ════════════════════════════════════════════════════════════════════════════

def final_bucket(score, cfg):
    if score >= cfg.g("scoring", "BEST_MIN_SCORE"):
        return "BEST"
    if score >= cfg.g("scoring", "STRONG_MIN_SCORE"):
        return "STRONG"
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

    if not (tf1h.get("ema_trend_up") and tf1h.get("not_near_resistance")):
        return None
    if tf1h.get("atr_pct", 0) < cfg.g("indicators", "ATR_MIN_PCT"):
        return None
    if not tf1h.get("major_struct_ok", True):
        return None

    candidates = []
    SCORE_CAP = cfg.g("scoring", "SCORE_CAP")
    LATE_REPEAT_COUNT = cfg.g("history", "LATE_REPEAT_COUNT", default=1)

    # ── PREBREAK ──────────────────────────────────────────────────────────
    if cfg.g("active_signals", "PREBREAK"):
        if (tf5.get("near_recent_max")
            and tf5.get("width_curr", 9) <= cfg.g("prebreak", "PREBREAK_BB_WIDTH_MAX")
            and tf5.get("vol_ratio", 0) >= cfg.g("prebreak", "PREBREAK_MIN_VOL_RATIO")
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

            if tf5.get("strong_close"):
                score += close_bonus
            if tf15.get("obv_rising"):
                score += obv_up_bonus
            elif tf15.get("obv_slope", 0) < -cfg.g("indicators", "OBV_RISING_MIN", default=0.05):
                score += obv_dn_pen
            if not tf15.get("recent_long_ok", True):
                score += struct_pen

            # LATE_REPEAT_PENALTY (mismo comportamiento que screener.py)
            late_repeat_pen = cfg.g("scoring_prebreak", "LATE_REPEAT_PENALTY", default=-1)
            prev_pb = counts_history.get((symbol, "PREBREAK"), 0)
            if prev_pb >= LATE_REPEAT_COUNT:
                score += late_repeat_pen

            score = min(score, SCORE_CAP)
            candidates.append({
                "label": "PRE-BREAK", "history_tf": "PREBREAK", "score": score,
                "priority": 1, "bucket": final_bucket(score, cfg),
                "timeframe": "5m", "price": tf5["price"],
                "ref_price": tf5["recent_max"],
                "obv_slope": tf15.get("obv_slope"),
                "cvd_ratio": tf15.get("cvd_ratio"),
                "recent_long_ok": tf15.get("recent_long_ok"),
            })

    # ── BREAKOUT ──────────────────────────────────────────────────────────
    if cfg.g("active_signals", "BREAKOUT"):
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
            obv_v = tf15.get("obv_slope", 0)
            cvd_v = tf15.get("cvd_ratio", 0)

            if obv_v >= obv_explosive_min:
                score += obv_explosive_b
            elif obv_v >= obv_strong_min:
                score += obv_strong_b
            elif obv_v >= obv_rising_min:
                score += obv_rising_b
            elif obv_v >= obv_neutral_min:
                score += obv_neutral_b
            else:
                score += obv_falling_pen

            if cvd_v >= cvd_vbull_min:
                score += cvd_vbull_b
            elif cvd_v >= cvd_bull_min:
                score += cvd_bull_b
            elif cvd_v >= cvd_neutral_min:
                score += cvd_neutral_b
            else:
                score += cvd_bear_pen

            climax_signals = 0
            if tf15.get("vol_ratio", 0) >= climax_vol_min:
                climax_signals += 1
            if tf15.get("width_expansion", 0) >= climax_bb_min:
                climax_signals += 1
            if tf15.get("candle_body_pct", 0) >= climax_body_min:
                climax_signals += 1
            if climax_signals >= climax_thresh:
                score += climax_pen

            if tf15["breakout_distance"] <= early_max:
                score += early_bonus
            elif tf15["breakout_distance"] >= late_min:
                score += late_pen_entry

            if not tf15.get("recent_long_ok", True):
                score += struct_pen

            # LATE_REPEAT_PENALTY (mismo comportamiento que screener.py)
            late_repeat_pen = cfg.g("scoring_breakout", "LATE_REPEAT_PENALTY", default=-1)
            prev_bo = counts_history.get((symbol, "BREAKOUT"), 0)
            if prev_bo >= LATE_REPEAT_COUNT:
                score += late_repeat_pen

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

            score = min(score, SCORE_CAP)
            candidates.append({
                "label": "BREAKOUT", "history_tf": "BREAKOUT", "score": score,
                "priority": 2, "bucket": final_bucket(score, cfg),
                "timeframe": "15m", "price": tf15["price"],
                "ref_price": tf15["recent_max"],
                "obv_slope": tf15.get("obv_slope"),
                "cvd_ratio": tf15.get("cvd_ratio"),
                "recent_long_ok": tf15.get("recent_long_ok"),
            })

    # ── RIDING ────────────────────────────────────────────────────────────
    if cfg.g("active_signals", "RIDING"):
        rg = tf15.get("riding_gain") or 0.0
        if (tf15.get("riding_above_zone")
            and tf15.get("riding_vol_ok")
            and cfg.g("riding", "RIDING_MIN_GAIN") <= rg <= cfg.g("riding", "RIDING_MAX_GAIN")
            and tf15.get("riding_bars_since") is not None
            and tf15["riding_bars_since"] >= 1
            and (not cfg.g("riding", "RIDING_EMA_MUST_TREND") or tf1h.get("ema_trend_up"))
            and not tf15.get("breakout")):

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
            if rg >= gain_strong_min:
                score += gain_strong_b
            elif rg >= gain_solid_min:
                score += gain_solid_b
            else:
                score += gain_initial_b
            # Bonus adicional para gains excepcionales
            if rg >= strong_gain_min:
                score += strong_gain_b
            if tf15.get("riding_vol_ok"):
                score += vol_ok_b
            if tf15.get("strong_close"):
                score += close_b
            if tf1h.get("ema_trend_up"):
                score += ema_b
            if tf1h.get("dist_to_res", 0) > dist_high_min:
                score += dist_high_b
            else:
                score += dist_low_b
            if tf15.get("obv_rising"):
                score += obv_up_b
            elif tf15.get("obv_slope", 0) < 0:
                score += obv_dn_pen
            if tf15.get("cvd_bullish"):
                score += cvd_up_b
            elif tf15.get("cvd_ratio", 0) < -cfg.g("indicators", "CVD_BULLISH_MIN", default=0.05):
                score += cvd_dn_pen

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

            score = min(score, SCORE_CAP)
            candidates.append({
                "label": "RIDING", "history_tf": "RIDING", "score": score,
                "priority": 2, "bucket": final_bucket(score, cfg),
                "timeframe": "15m", "price": tf15["price"],
                "ref_price": tf15.get("riding_break_close"),
                "obv_slope": tf15.get("obv_slope"),
                "cvd_ratio": tf15.get("cvd_ratio"),
                "recent_long_ok": tf15.get("recent_long_ok"),
            })

    # ── HOLD ──────────────────────────────────────────────────────────────
    if cfg.g("active_signals", "HOLD"):
        if (tf15.get("hold_recent_break") and tf15.get("hold_kept_zone")
            and tf15.get("hold_pullback_ok") and tf15.get("hold_strong")):

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
            if tf1h.get("dist_to_res", 0) > dist_high_min:
                score += dist_high_b
            else:
                score += dist_low_b
            if tf15.get("obv_rising"):
                score += obv_up_b
            elif tf15.get("obv_slope", 0) < -cfg.g("indicators", "OBV_RISING_MIN", default=0.05):
                score += obv_dn_pen
            if tf15.get("cvd_bullish"):
                score += cvd_up_b
            elif tf15.get("cvd_ratio", 0) < -cfg.g("indicators", "CVD_BULLISH_MIN", default=0.05):
                score += cvd_dn_pen
            if not tf15.get("recent_long_ok", True):
                score += struct_pen
            # Bonus por momentum extremo: OBV explosivo + CVD bullish + lejos de resistencia 1h
            if (tf15.get("obv_slope", 0) >= momentum_obv
                and tf15.get("cvd_bullish")
                and tf1h.get("dist_to_res", 0) >= momentum_dist):
                score += momentum_b

            # LATE_REPEAT_PENALTY (mismo comportamiento que screener.py)
            late_repeat_pen = cfg.g("scoring_hold", "LATE_REPEAT_PENALTY", default=-1)
            prev_hold = counts_history.get((symbol, "HOLD"), 0)
            if prev_hold >= LATE_REPEAT_COUNT:
                score += late_repeat_pen

            score = min(score, SCORE_CAP)
            candidates.append({
                "label": "HOLD", "history_tf": "HOLD", "score": score,
                "priority": 3, "bucket": final_bucket(score, cfg),
                "timeframe": "15m", "price": tf15["price"],
                "ref_price": tf15.get("riding_break_ref") or tf15["recent_max"],
                "obv_slope": tf15.get("obv_slope"),
                "cvd_ratio": tf15.get("cvd_ratio"),
                "recent_long_ok": tf15.get("recent_long_ok"),
            })

    if not candidates:
        return None

    # FORMING_CANDLE_PENALTY (mismo comportamiento que screener.py:1279-1288).
    # En backtest todas las velas son históricas (candle_status="closed"), así que el
    # penalty no se dispara — pero el cableado es idéntico al screener para fidelidad.
    FORMING_CANDLE_PENALTY = cfg.g("scoring", "FORMING_CANDLE_PENALTY", default=3)
    IMMEDIATE_MIN_SCORE = cfg.g("scoring", "IMMEDIATE_MIN_SCORE", default=13)
    for c in candidates:
        cs = (tf_data.get(c["timeframe"]) or {}).get("candle_status", "closed")
        c["candle_status"] = cs
        if cs == "forming":
            c["score"] = max(0, c["score"] - FORMING_CANDLE_PENALTY)
            c["bucket"] = final_bucket(c["score"], cfg)
            if c.get("immediate") and c["score"] < IMMEDIATE_MIN_SCORE:
                c["immediate"] = False

    # Final cap loop (mismo screener.py:1293-1296). Redundante porque cada bloque ya
    # aplicó min(score, SCORE_CAP), pero se mantiene para que la simulación sea espejo.
    for c in candidates:
        if c["score"] > SCORE_CAP:
            c["score"] = SCORE_CAP
            c["bucket"] = final_bucket(c["score"], cfg)

    candidates.sort(key=lambda x: (x["score"], x["priority"]), reverse=True)
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


def simulate(cfg, klines, start_dt, end_dt, snapshot_pairs=None,
             scan_interval_min=SCAN_INTERVAL_MIN, derivatives=None):
    print(f"\n  Simulando {(end_dt - start_dt).total_seconds() / 3600:.0f}h con scans cada {scan_interval_min}min...")
    deriv_enabled = bool(derivatives) and cfg.g("derivatives", "ENABLED", default=False)
    deriv_lookback = cfg.g("derivatives", "OI_LOOKBACK_MIN", default=30)

    cooldown_min_by_state = cfg.g("cooldowns_minutes")
    HISTORY_HOURS = cfg.g("history", "HISTORY_HOURS", default=8)
    history_window_ms = HISTORY_HOURS * 3600 * 1000
    last_alert_ts = {}
    # sim_alert_history: lista (ts_ms, symbol, history_tf) para simular fetch_history()
    # del screener. Cada scan recomputa counts_history a partir de las alertas emitidas
    # en las últimas HISTORY_HOURS, y se la pasa a classify() para aplicar
    # LATE_REPEAT_PENALTY igual que en producción.
    sim_alert_history = []

    scan_ts = []
    cur = start_dt
    while cur <= end_dt:
        scan_ts.append(int(cur.timestamp() * 1000))
        cur += timedelta(minutes=scan_interval_min)

    snap_runs_sorted = sorted(snapshot_pairs.keys()) if snapshot_pairs else []

    def pairs_for_scan(scan_ts_ms):
        if not snapshot_pairs:
            return list(klines.keys())
        scan_iso = datetime.fromtimestamp(scan_ts_ms / 1000, tz=timezone.utc).isoformat()
        best = None
        for run_iso in snap_runs_sorted:
            if run_iso <= scan_iso:
                best = run_iso
            else:
                break
        if best is None:
            return list(klines.keys())
        return [s for s in snapshot_pairs[best] if s in klines]

    alerts = []
    total_scans = len(scan_ts)
    for i, ts_ms in enumerate(scan_ts):
        if i % 20 == 0:
            print(f"    scan {i}/{total_scans} ({i*100//max(total_scans,1)}%) — {len(alerts)} alertas hasta ahora")

        # Ventana móvil: descartar alertas más viejas que HISTORY_HOURS para que counts_history
        # refleje sólo lo que el screener real vería en su fetch_history().
        cutoff_ms = ts_ms - history_window_ms
        if sim_alert_history and sim_alert_history[0][0] < cutoff_ms:
            sim_alert_history = [h for h in sim_alert_history if h[0] >= cutoff_ms]
        counts_history = {}
        for (_, s, h) in sim_alert_history:
            counts_history[(s, h)] = counts_history.get((s, h), 0) + 1

        active_pairs = pairs_for_scan(ts_ms)
        for sym in active_pairs:
            tf_data = {}
            valid = True
            for tf in ("5m", "15m", "1h"):
                if tf not in klines.get(sym, {}):
                    valid = False
                    break
                df = klines[sym][tf]
                idx = find_idx_at_or_before(df, ts_ms)
                if idx < 0 or idx < 80:
                    valid = False
                    break
                tf_data[tf] = analyze_at_time(df, idx, cfg)
                if tf_data[tf] is None:
                    valid = False
                    break
            if not valid:
                continue

            # Inyectar features de derivatives en el 15m TF (mismo patrón que screener.py)
            if deriv_enabled:
                oi_delta, funding_rate = lookup_derivatives_at(
                    derivatives.get(sym), ts_ms, oi_lookback_min=deriv_lookback)
                tf_data["15m"]["oi_delta_30m"] = oi_delta
                tf_data["15m"]["funding_rate"] = funding_rate

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

            df_15m = klines[sym]["15m"]
            alert_idx_15m = find_idx_at_or_before(df_15m, ts_ms)
            outcomes = calculate_outcomes(df_15m, alert_idx_15m, alert["price"])

            alert_record = {
                "alerted_at": datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc).isoformat(),
                "symbol": sym, "signal_type": alert["history_tf"],
                "label": alert["label"], "score": alert["score"],
                "bucket": alert["bucket"], "timeframe": alert["timeframe"],
                "entry_price": alert["price"], "ref_price": alert["ref_price"],
                "obv_slope": alert.get("obv_slope"),
                "cvd_ratio": alert.get("cvd_ratio"),
                "recent_long_ok": alert.get("recent_long_ok"),
                "candle_status": alert.get("candle_status"),
                **outcomes,
            }
            alerts.append(alert_record)
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
                 label="run", scan_interval_min=SCAN_INTERVAL_MIN, derivatives=None):
    print(f"\n  >>> Corriendo backtest: {label}")
    alerts = simulate(cfg, klines, start_dt, end_dt, snapshot_pairs,
                      scan_interval_min=scan_interval_min, derivatives=derivatives)
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
    args = parser.parse_args()

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
                                   derivatives=derivatives)
        all_results[Path(base_path).stem] = alerts_base
        for vp in variant_paths:
            cfg_v = Config(vp)
            alerts_v = run_backtest(cfg_v, args.weeks, klines, start_dt, end_dt,
                                    snapshot_pairs, label=f"VARIANT ({Path(vp).stem})",
                                    scan_interval_min=args.scan_interval_min,
                                    derivatives=derivatives)
            all_results[Path(vp).stem] = alerts_v
            compare_runs(alerts_base, Path(base_path).stem, alerts_v, Path(vp).stem,
                         period_days=period_days)

    elif args.compare:
        cfg_old = Config(args.compare[0])
        cfg_new = Config(args.compare[1])
        alerts_old = run_backtest(cfg_old, args.weeks, klines, start_dt, end_dt,
                                  snapshot_pairs, label=f"OLD ({args.compare[0]})",
                                  scan_interval_min=args.scan_interval_min,
                                  derivatives=derivatives)
        alerts_new = run_backtest(cfg_new, args.weeks, klines, start_dt, end_dt,
                                  snapshot_pairs, label=f"NEW ({args.compare[1]})",
                                  scan_interval_min=args.scan_interval_min,
                                  derivatives=derivatives)
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
                                  derivatives=derivatives)
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
                              derivatives=derivatives)
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
