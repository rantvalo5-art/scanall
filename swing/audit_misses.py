"""
swing/audit_misses.py — Auditoría de cobertura del screener swing.

TF mapping: 1h (rápido/PREBREAK) / 4h (BO/RIDING/HOLD) / 1d (gates EMA/ATR/estructura).

Subcomandos:
  catch-rate     Mide catch-rate real vs universo de movers en 4h bars
  distributions  Distribucion empirica de ATR%, vol_ratio, OBV slope, CVD ratio
  post-mortem    Replay de analyze+classify para símbolo+timestamp

Uso:
  python audit_misses.py catch-rate [--days 28] [--max-pairs 200]
  python audit_misses.py distributions [--days 28] [--max-pairs 200]
  python audit_misses.py post-mortem BTCUSDT 2025-01-15T14:30:00
  python audit_misses.py post-mortem BTCUSDT 1705327800000
"""

import argparse
import json
import sys
from collections import Counter, defaultdict
from datetime import datetime, timezone, timedelta
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).parent))
from backtest import (
    Config,
    get_top_pairs_today,
    download_all_klines,
    precompute_indicators,
    analyze_at_index,
    _build_analyze_params,
    find_idx_at_or_before,
    classify,
)

CONFIG_PATH   = Path(__file__).parent / "config.json"
AUDIT_CACHE   = Path(__file__).parent / ".audit_cache"
AUDIT_RESULTS = Path(__file__).parent / ".audit_results"
AUDIT_CACHE.mkdir(exist_ok=True)
AUDIT_RESULTS.mkdir(exist_ok=True)

# Umbrales "mover" sobre barras 4h: (etiqueta, umbral_ganancia, ventana_barras_4h)
MOVER_THRESHOLDS = [
    ("10pct_3d",  0.10, 18),   # ≥10% en 3d  (18 × 4h)
    ("20pct_7d",  0.20, 42),   # ≥20% en 7d  (42 × 4h)
    ("30pct_7d",  0.30, 42),   # ≥30% en 7d  (42 × 4h)
]

# Ventana de pre-pump: cuántas barras 4h ANTES revisamos (42 × 4h = 168h = 7d)
PRE_PUMP_WINDOW = 42


# ── Diagnóstico de señales ────────────────────────────────────────────────────

def _get_signal_fail_reason(tf_1h, tf_4h, tf_1d, cfg):
    """Devuelve (razón, n_checks_pasados) del signal que más cerca estuvo de disparar.
    Elige el signal que más checks superó y devuelve ('<prefijo>:<primera_gate_fallida>', passes).
    Si algún signal pasa TODOS sus checks devuelve ('<prefijo>:cooldown_or_active', len(checks))."""
    require_obv = cfg.g("breakout", "BREAKOUT_REQUIRE_OBV_NON_NEGATIVE", default=True)
    min_g = cfg.g("riding", "RIDING_MIN_GAIN", default=0.015)
    max_g = cfg.g("riding", "RIDING_MAX_GAIN", default=0.30)
    riding_ema = cfg.g("riding", "RIDING_EMA_MUST_TREND", default=True)
    max_fade   = cfg.g("riding", "RIDING_MAX_FADE_FROM_HIGH", default=0.025)

    signals = {
        "pre": [
            ("near_max",   bool(tf_1h.get("near_recent_max"))),
            ("bb_width",   tf_1h.get("width_curr", 9) <= cfg.g("prebreak", "PREBREAK_BB_WIDTH_MAX")),
            ("vol_ratio",  tf_1h.get("vol_ratio", 0) >= cfg.g("prebreak", "PREBREAK_MIN_VOL_RATIO")),
            ("vol_growth", tf_1h.get("vol_growth", 0) >= cfg.g("prebreak", "PREBREAK_VOLUME_GROWTH_MIN")),
        ],
        "bo": [
            ("breakout",      bool(tf_4h.get("breakout"))),
            ("vol_ratio_4h",  tf_4h.get("vol_ratio", 0) >= cfg.g("breakout", "BREAKOUT_MIN_VOL_RATIO")),
            ("distance",      tf_4h.get("breakout_distance", 9) <= cfg.g("breakout", "BREAKOUT_MAX_EXTENDED")),
            ("bb_exp",        tf_4h.get("width_expansion", -9) >= cfg.g("breakout", "BREAKOUT_BB_EXPANSION_MIN")),
            ("strong_close",  bool(tf_4h.get("strong_close"))),
            ("body",          tf_4h.get("candle_body_pct", 0) >= cfg.g("breakout", "BREAKOUT_MIN_BODY_PCT")),
            ("1h_vol",        tf_1h.get("vol_ratio", 0) >= cfg.g("breakout", "BREAKOUT_1H_MIN_VOL_RATIO", default=1.0)),
            ("1h_close",      bool(tf_1h.get("strong_close"))),
            ("obv_nonneg",    (not require_obv) or tf_4h.get("obv_slope", 0) >= 0),
        ],
        "ri": [
            ("above_zone",  bool(tf_4h.get("riding_above_zone"))),
            ("vol_ok",      bool(tf_4h.get("riding_vol_ok"))),
            ("gain_range",  min_g <= (tf_4h.get("riding_gain") or 0) <= max_g),
            ("bars_since",  tf_4h.get("riding_bars_since") is not None and tf_4h.get("riding_bars_since", 0) >= 1),
            ("no_breakout", not bool(tf_4h.get("breakout"))),
            ("ema_trend",   (not riding_ema) or bool(tf_1d.get("ema_trend_up"))),
            ("fade_ok",     (tf_4h.get("fading_reversal") or 0.0) >= -max_fade),
        ],
        "ho": [
            ("recent_break", bool(tf_4h.get("hold_recent_break"))),
            ("kept_zone",    bool(tf_4h.get("hold_kept_zone"))),
            ("pullback_ok",  bool(tf_4h.get("hold_pullback_ok"))),
            ("strong",       bool(tf_4h.get("hold_strong"))),
        ],
    }

    if cfg.g("active_signals", "COILING", default=False):
        _coil_max_dist = cfg.g("coiling", "COILING_MAX_DIST", default=0.04)
        _bd_co = tf_4h.get("breakout_distance", -999)
        signals["co"] = [
            ("no_breakout", not bool(tf_4h.get("breakout"))),
            ("dist_range",  -_coil_max_dist <= _bd_co < 0),
            ("obv_rising",  tf_4h.get("obv_slope", 0) >= cfg.g("indicators", "OBV_RISING_MIN", default=0.05)),
            ("cvd_bullish", tf_4h.get("cvd_ratio", 0) >= cfg.g("indicators", "CVD_BULLISH_MIN", default=0.05)),
            ("vol_ratio",   tf_4h.get("vol_ratio", 0) >= cfg.g("coiling", "COILING_MIN_VOL_RATIO", default=1.0)),
            ("bb_width",    tf_4h.get("width_curr", 9) <= cfg.g("coiling", "COILING_BB_WIDTH_MAX", default=9.0)),
        ]

    best_prefix = "all"
    best_fail   = "signals"
    best_passes = -1

    for prefix, checks in signals.items():
        passes = sum(v for _, v in checks)
        first_fail = next((k for k, v in checks if not v), None)
        if first_fail is None:
            return f"{prefix}:cooldown_or_active", len(checks)
        if passes > best_passes:
            best_passes = passes
            best_prefix = prefix
            best_fail   = first_fail

    return f"{best_prefix}:{best_fail}", best_passes


def classify_explain(symbol, tf_data, cfg):
    """Llama classify() y si devuelve None explica la primera causa.
    Devuelve (alert_o_None, razon_str_o_None, proximity).

    proximity = "qué tan cerca estuvo de disparar" (mayor = más cerca):
      -1 data_missing · 0 pre_resist · 1 pre_major_struct · 2 pre_ema_trend
      · 3 pre_atr · 10+passes señales (pasó todos los pre-gates 1d).
    Cuando alert is not None, proximity=100 (no se consume)."""
    tf_1h = tf_data.get("1h") or {}
    tf_4h = tf_data.get("4h") or {}
    tf_1d = tf_data.get("1d") or {}
    if not tf_1h or not tf_4h or not tf_1d:
        return None, "data_missing", -1

    # classify() re-deriva obv_rising/cvd_bullish sobre tf_4h
    obv_min = cfg.g("indicators", "OBV_RISING_MIN", default=0.05)
    cvd_min = cfg.g("indicators", "CVD_BULLISH_MIN", default=0.05)
    tf_4h = dict(tf_4h)
    tf_4h["obv_rising"]  = tf_4h.get("obv_slope", 0) >= obv_min
    tf_4h["cvd_bullish"] = tf_4h.get("cvd_ratio", 0) >= cvd_min
    tf_data = {**tf_data, "4h": tf_4h}

    # Gate: resistencia (en tf_1d)
    if not tf_1d.get("not_near_resistance"):
        _bypass_resist = (
            cfg.g("hold", "RESIST_BYPASS_ON_15M_BREAKOUT", default=False)
            and tf_4h.get("breakout", False)
            and tf_4h.get("vol_ratio", 0) >= cfg.g("hold", "RESIST_BYPASS_MIN_VOL_15M", default=999.0)
            and (tf_4h.get("bars_since_break") or 999) <= cfg.g("hold", "RESIST_BYPASS_MAX_BARS_SINCE_BREAK", default=0)
        )
        if not _bypass_resist:
            return None, "pre_resist", 0

    # Gate: major_struct (en tf_1d)
    if not tf_1d.get("major_struct_ok", True):
        _bypass_brk  = cfg.g("hold", "MAJOR_STRUCT_BYPASS_ON_BREAKOUT", default=False) and tf_4h.get("breakout", False)
        _bypass_pre  = cfg.g("hold", "MAJOR_STRUCT_BYPASS_ON_STRONG_PRE", default=False) and tf_4h.get("cvd_bullish", False) and tf_4h.get("obv_rising", False)
        _bypass_vol  = tf_1d.get("vol_ratio", 0) >= cfg.g("hold", "MAJOR_STRUCT_BYPASS_VOL_RATIO_1H", default=999.0)
        _age_thr     = cfg.g("hold", "MAJOR_STRUCT_BYPASS_AGE_BARS", default=999)
        _bsm         = tf_1d.get("bars_since_major_max")
        _bypass_age  = _bsm is not None and _bsm >= _age_thr
        _soft_dist   = cfg.g("hold", "MAJOR_STRUCT_BYPASS_SOFTZONE_DIST", default=0.0)
        _soft_max    = cfg.g("hold", "MAJOR_STRUCT_BYPASS_SOFTZONE_HOLD_MAX_BARS", default=0)
        _msd         = tf_1d.get("major_struct_dist")
        _bsb         = tf_4h.get("bars_since_break")
        _bypass_soft = (
            _soft_dist > 0 and _soft_max > 0
            and _msd is not None and _msd <= _soft_dist
            and tf_4h.get("hold_recent_break", False)
            and _bsb is not None and _bsb <= _soft_max
        )
        _struct_gate_hard = cfg.g("hold", "MAJOR_STRUCT_GATE_HARD", default=True)
        if not (_bypass_brk or _bypass_pre or _bypass_vol or _bypass_age or _bypass_soft):
            if _struct_gate_hard:
                return None, "pre_major_struct", 1
            # soft-mode: continúa; classify() aplicará la penalidad

    result = classify(symbol, tf_data, cfg)
    if result is not None:
        return result, None, 100

    # Identify the blocking gate (EMA o ATR)
    ema_gate_hard = cfg.g("indicators", "EMA_GATE_HARD", default=True)
    if ema_gate_hard and not tf_1d.get("ema_trend_up"):
        return None, "pre_ema_trend", 2

    _atr_pct_v        = tf_1d.get("atr_pct", 0)
    _atr_threshold_v  = tf_1d.get("atr_threshold", cfg.g("indicators", "ATR_MIN_PCT"))
    if _atr_pct_v < _atr_threshold_v:
        return None, "pre_atr", 3

    reason, passes = _get_signal_fail_reason(tf_1h, tf_4h, tf_1d, cfg)
    return None, reason, 10 + passes


# ── Detección de pump-starts en barras 4h ─────────────────────────────────────

def find_pump_starts(df_4h, threshold, window_bars, start_ms, end_ms):
    """Devuelve lista de (bar_idx, gain_pct) donde empieza un pump >= threshold.
    Deduplicado: tras un pump_start no cuenta otro dentro de window_bars barras."""
    close = df_4h["close"].values.astype(float)
    high  = df_4h["high"].values.astype(float)
    cts   = df_4h["close_time"].values.astype(np.int64)
    n = len(close)

    h_s = pd.Series(high)
    fwd_max = (h_s[::-1]
               .rolling(window=window_bars, min_periods=window_bars)
               .max()[::-1]
               .shift(-1)
               .values)

    with np.errstate(invalid="ignore", divide="ignore"):
        gain = np.where(close > 0, fwd_max / close - 1, np.nan)

    starts = []
    last_t = -(window_bars + 1)
    for t in range(n):
        ts = int(cts[t])
        if ts < start_ms or ts > end_ms:
            continue
        if np.isnan(gain[t]):
            continue
        if gain[t] >= threshold and (t - last_t) > window_bars:
            if t + window_bars < n:
                starts.append((t, float(gain[t]) * 100))
                last_t = t
    return starts


# ── Datos de TF en un bar 4h ──────────────────────────────────────────────────

def get_tf_data_at(prepared_sym, bar_idx_4h, cfg):
    """Construye tf_data para un símbolo en bar_idx_4h.
    Mapea el timestamp 4h al bar correcto de 1h y 1d."""
    df_4h = prepared_sym.get("4h")
    df_1h = prepared_sym.get("1h")
    df_1d = prepared_sym.get("1d")
    if df_4h is None or df_1h is None or df_1d is None:
        return None
    if bar_idx_4h >= len(df_4h):
        return None

    ts_ms  = int(df_4h["close_time"].iat[bar_idx_4h])
    idx_1h = find_idx_at_or_before(df_1h, ts_ms)
    idx_1d = find_idx_at_or_before(df_1d, ts_ms)

    if idx_1h < 80 or bar_idx_4h < 40 or idx_1d < 30:
        return None

    params = _build_analyze_params(cfg)
    r_1h = analyze_at_index(df_1h, idx_1h,      params)
    r_4h = analyze_at_index(df_4h, bar_idx_4h,  params)
    r_1d = analyze_at_index(df_1d, idx_1d,      params)

    if r_1h is None or r_4h is None or r_1d is None:
        return None

    result = {"1h": r_1h, "4h": r_4h, "1d": r_1d}

    df_1w = prepared_sym.get("1w")
    if df_1w is not None:
        idx_1w = find_idx_at_or_before(df_1w, ts_ms)
        if idx_1w >= 5:
            r_1w = analyze_at_index(df_1w, idx_1w, params)
            if r_1w is not None:
                result["1w"] = r_1w

    return result


# ── Subcomando: catch-rate ────────────────────────────────────────────────────

def cmd_catch_rate(args, cfg):
    pre_pump_window = getattr(args, "pre_pump_window", PRE_PUMP_WINDOW)
    end_dt   = datetime.now(timezone.utc).replace(minute=0, second=0, microsecond=0)
    start_dt = end_dt - timedelta(days=args.days)
    start_ms = int(start_dt.timestamp() * 1000)
    end_ms   = int(end_dt.timestamp() * 1000)

    print(f"\n=== CATCH-RATE AUDIT (swing) — últimos {args.days} días ===")
    print(f"  Período: {start_dt.date()} → {end_dt.date()}")
    print(f"  PRE_PUMP_WINDOW: {pre_pump_window} barras 4h ({pre_pump_window*4}h)")

    min_vol = cfg.g("general", "MIN_QUOTE_VOLUME")
    print(f"\n[1/5] Obteniendo universo (vol >= {min_vol/1e6:.0f}M USDT)...")
    symbols = get_top_pairs_today(min_vol, top_n=args.max_pairs)
    print(f"  {len(symbols)} pares activos")

    print(f"\n[2/5] Descargando klines ({len(symbols)} pares)...")
    klines = download_all_klines(symbols, start_dt, end_dt)
    print(f"  {len(klines)} pares con datos completos")

    print(f"\n[3/5] Precomputando indicadores...")
    prepared = {}
    for sym, tfs in klines.items():
        entry = {}
        for tf in ("1h", "4h", "1d", "1w"):
            df = tfs.get(tf)
            if df is not None:
                entry[tf] = precompute_indicators(df, cfg)
        if all(t in entry for t in ("1h", "4h", "1d")):
            prepared[sym] = entry
    print(f"  {len(prepared)} pares listos")

    print(f"\n[4/5] Detectando pump-starts...")
    mover_events = {}
    for label, threshold, window in MOVER_THRESHOLDS:
        events = []
        for sym, sym_prep in prepared.items():
            df_4h = sym_prep.get("4h")
            if df_4h is None:
                continue
            starts = find_pump_starts(df_4h, threshold, window, start_ms, end_ms)
            for bar_t, gain_pct in starts:
                events.append((sym, bar_t, gain_pct))
        mover_events[label] = events
        print(f"  {label}: {len(events)} pump-starts detectados")

    print(f"\n[5/5] Evaluando cobertura del screener...")
    classify_cache = {}
    results = {}
    for label, threshold, window in MOVER_THRESHOLDS:
        events = mover_events[label]
        caught = []
        missed_list = []
        reasons = Counter()              # gate vinculante por mover (1 voto/mover)
        window_reason_hist = Counter()   # toda razón en cada barra de la ventana (agregado)
        caught_by_signal = Counter()
        lateness_by_signal = defaultdict(Counter)
        bo_breakout_feat_rows = []   # features 4h de cada miss bo:breakout (Step 0)

        for sym, pump_t, gain_pct in events:
            sym_prep = prepared.get(sym)
            if sym_prep is None:
                reasons["data_missing"] += 1
                missed_list.append({"symbol": sym, "gain_pct": round(gain_pct, 2),
                                    "reason": "data_missing", "ts": ""})
                continue

            caught_this  = False
            caught_signal = None
            # Atribución por máxima proximidad: el gate vinculante es el de la barra
            # donde MÁS cerca estuvo de disparar (no la barra del pump = offset 0).
            best_prox    = -99
            best_reason  = "data_missing"
            best_offset  = 0
            reason_first_offset = {}  # razón → offset más temprano donde apareció

            for offset in range(-pre_pump_window, 1):
                bar = pump_t + offset
                if bar < 0:
                    continue
                cache_key = (sym, bar)
                if cache_key in classify_cache:
                    alert, reason, prox = classify_cache[cache_key]
                else:
                    tf_data = get_tf_data_at(sym_prep, bar, cfg)
                    if tf_data is None:
                        alert, reason, prox = None, "data_missing", -1
                    else:
                        alert, reason, prox = classify_explain(sym, tf_data, cfg)
                    classify_cache[cache_key] = (alert, reason, prox)
                if alert is not None:
                    caught_this   = True
                    caught_signal = alert.get("history_tf") or "unknown"
                    break
                reason = reason or "unknown"
                window_reason_hist[reason] += 1
                reason_first_offset.setdefault(reason, offset)
                if prox > best_prox:
                    best_prox   = prox
                    best_reason = reason
                    best_offset = offset

            df_4h = sym_prep["4h"]
            ts_ms_pump = int(df_4h["close_time"].iat[pump_t])
            ts_iso = datetime.fromtimestamp(ts_ms_pump / 1000, tz=timezone.utc).isoformat()

            if caught_this:
                caught.append((sym, pump_t, gain_pct))
                caught_by_signal[caught_signal] += 1
                lateness_by_signal[caught_signal][offset] += 1
            else:
                # Gate vinculante = el de la barra más cercana a disparar (no offset 0).
                last_reason = best_reason
                bar = pump_t + best_offset
                # relax_offset: barra más temprana donde apareció ese gate vinculante
                # (proxy de "lateness mínima alcanzable si se relaja ese gate").
                relax_offset = reason_first_offset.get(best_reason, best_offset)

                # Enriquece "pre_ema_trend"
                if last_reason == "pre_ema_trend":
                    df_1d = sym_prep.get("1d")
                    if df_1d is not None and "_ema_slow" in df_1d.columns:
                        ts_ms_bar  = int(df_4h["close_time"].iat[bar])
                        idx_1d_bar = find_idx_at_or_before(df_1d, ts_ms_bar)
                        if idx_1d_bar >= 5:
                            ema_now    = float(df_1d["_ema_slow"].iat[idx_1d_bar])
                            ema_prev   = float(df_1d["_ema_slow"].iat[idx_1d_bar - 3])
                            price_now  = float(df_1d["close"].iat[idx_1d_bar])
                            price_above  = price_now > ema_now
                            ema_rising   = ema_now > ema_prev
                            recent_cross = False
                            if price_above:
                                for _k in range(1, 6):
                                    if idx_1d_bar - _k < 0:
                                        break
                                    if float(df_1d["close"].iat[idx_1d_bar - _k]) <= float(df_1d["_ema_slow"].iat[idx_1d_bar - _k]):
                                        recent_cross = True
                                        break
                            if recent_cross:
                                last_reason = "pre_ema:recent_cross"
                            elif price_above and not ema_rising:
                                last_reason = "pre_ema:price_only"
                            elif (not price_above) and ema_rising:
                                last_reason = "pre_ema:ema_only"
                            else:
                                last_reason = "pre_ema:both_fail"

                # Enriquece "pre_major_struct" — subdiv por qué bypass lo habría salvado
                if last_reason == "pre_major_struct":
                    _tf_data_bar_full = get_tf_data_at(sym_prep, bar, cfg) if sym_prep else None
                    _tf1d_b = (_tf_data_bar_full or {}).get("1d") or {}
                    _tf4h_b = (_tf_data_bar_full or {}).get("4h") or {}

                    _bsm    = _tf1d_b.get("bars_since_major_max")
                    _msd    = _tf1d_b.get("major_struct_dist")
                    _vol_1d = _tf1d_b.get("vol_ratio", 0.0)
                    # obv_rising/cvd_bullish se re-derivan igual que en classify_explain
                    _obv_r  = float(_tf4h_b.get("obv_slope", 0)) >= cfg.g("indicators", "OBV_RISING_MIN", default=0.05)
                    _cvd_b  = float(_tf4h_b.get("cvd_ratio", 0)) >= cfg.g("indicators", "CVD_BULLISH_MIN", default=0.05)
                    _hrb    = bool(_tf4h_b.get("hold_recent_break", False))
                    _bsb    = _tf4h_b.get("bars_since_break")

                    _pass_age  = (_bsm is not None and _bsm >= 20)
                    _pass_vol  = (_vol_1d >= 3.0)
                    _pass_pre  = (_cvd_b and _obv_r)
                    _pass_soft = (_msd is not None and _msd <= 0.25 and _hrb
                                  and _bsb is not None and _bsb <= 5)

                    if _pass_age:
                        last_reason = "pre_major_struct:would_pass_age"
                    elif _pass_vol:
                        last_reason = "pre_major_struct:would_pass_vol"
                    elif _pass_pre:
                        last_reason = "pre_major_struct:would_pass_strong_pre"
                    elif _pass_soft:
                        last_reason = "pre_major_struct:would_pass_softzone"
                    else:
                        last_reason = "pre_major_struct:would_pass_none"

                # Enriquece "pre_atr"
                if last_reason == "pre_atr":
                    df_1d = sym_prep.get("1d")
                    if df_1d is not None and "_atr_pct" in df_1d.columns and "_atr_threshold" in df_1d.columns:
                        ts_ms_bar  = int(df_4h["close_time"].iat[bar])
                        idx_1d_bar = find_idx_at_or_before(df_1d, ts_ms_bar)
                        if idx_1d_bar >= 0:
                            atr_now = float(df_1d["_atr_pct"].iat[idx_1d_bar])
                            thr_now = float(df_1d["_atr_threshold"].iat[idx_1d_bar])
                            ratio   = atr_now / thr_now if thr_now > 0 else 0.0
                            sub = "pre_atr:near" if ratio >= 0.80 else ("pre_atr:mid" if ratio >= 0.50 else "pre_atr:far")
                            last_reason = sub

                # Step 0: capturar features 4h cuando el gate vinculante es bo:breakout
                if last_reason == "bo:breakout":
                    _td_bo  = get_tf_data_at(sym_prep, bar, cfg) if sym_prep else None
                    _tf4_bo = (_td_bo or {}).get("4h") or {}
                    if _tf4_bo:
                        bo_breakout_feat_rows.append({
                            "breakout_distance": _tf4_bo.get("breakout_distance"),
                            "vol_ratio":         _tf4_bo.get("vol_ratio"),
                            "obv_slope":         _tf4_bo.get("obv_slope"),
                            "cvd_ratio":         _tf4_bo.get("cvd_ratio"),
                            "width_curr":        _tf4_bo.get("width_curr"),
                            "strong_close":      int(bool(_tf4_bo.get("strong_close"))),
                            "candle_body_pct":   _tf4_bo.get("candle_body_pct"),
                        })

                reasons[last_reason] += 1
                missed_list.append({"symbol": sym, "ts": ts_iso,
                                    "gain_pct": round(gain_pct, 2), "reason": last_reason,
                                    "closest_offset": int(best_offset),
                                    "relax_offset": int(relax_offset),
                                    # relax en el borde de la ventana → la relajación real
                                    # podría ser más antigua (valor censurado).
                                    "relax_censored": bool(relax_offset == -pre_pump_window)})

        total      = len(events)
        catch_rate = len(caught) * 100 / total if total else 0.0
        missed_list.sort(key=lambda x: x["gain_pct"], reverse=True)

        # Step 0: distribución de features para los misses bo:breakout
        bo_feat_dist = {}
        if bo_breakout_feat_rows:
            _feat_keys = ["breakout_distance", "vol_ratio", "obv_slope", "cvd_ratio",
                          "width_curr", "candle_body_pct"]
            for fk in _feat_keys:
                vals = [r[fk] for r in bo_breakout_feat_rows if r.get(fk) is not None]
                bo_feat_dist[fk] = _percentiles(vals)
            _sc = sum(r["strong_close"] for r in bo_breakout_feat_rows)
            bo_feat_dist["strong_close_rate"] = round(_sc / len(bo_breakout_feat_rows), 3)

        results[label] = {
            "total_movers":     total,
            "caught":           len(caught),
            "missed":           len(missed_list),
            "catch_rate":       round(catch_rate, 1),
            "miss_reasons":       dict(reasons),
            "window_reason_hist": dict(window_reason_hist),
            "caught_by_signal":   dict(caught_by_signal),
            "lateness_bars":      {sig: dict(ctr) for sig, ctr in lateness_by_signal.items()},
            "top_misses":         missed_list[:20],
            "bo_breakout_features": {
                "count": len(bo_breakout_feat_rows),
                "distributions": bo_feat_dist,
            } if bo_feat_dist else None,
        }

    # ── Reporte ──────────────────────────────────────────────────────────────
    print()
    print("═" * 70)
    print(" RESULTADO — CATCH RATE (swing)")
    print("═" * 70)
    for label, _, _ in MOVER_THRESHOLDS:
        r = results[label]
        print(f"\n  [{label}]")
        print(f"    Total movers  : {r['total_movers']}")
        print(f"    Pescados      : {r['caught']}  ({r['catch_rate']:.1f}%)")
        if r.get("caught_by_signal"):
            total_c = sum(r["caught_by_signal"].values())
            print(f"    Pescados por signal:")
            for signal, n in sorted(r["caught_by_signal"].items(), key=lambda x: -x[1]):
                pct = n * 100 / total_c if total_c else 0
                lb  = r.get("lateness_bars", {}).get(signal, {})
                early = sum(v for k, v in lb.items() if k < 0)
                at0   = lb.get(0, 0)
                late_str = f"  early={early} at_pump={at0}" if lb else ""
                print(f"      {signal:<14} {n:>4}  ({pct:.0f}%){late_str}")
        print(f"    Perdidos      : {r['missed']}")
        if r["miss_reasons"]:
            total_miss = sum(r["miss_reasons"].values())
            print(f"    Gate vinculante (1 voto/mover, barra más cercana a disparar):")
            for reason, n in sorted(r["miss_reasons"].items(), key=lambda x: -x[1]):
                pct = n * 100 / total_miss if total_miss else 0
                print(f"      {reason:<32} {n:>4}  ({pct:.0f}%)")
        if r.get("window_reason_hist"):
            total_win = sum(r["window_reason_hist"].values())
            print(f"    Histograma de gates en toda la ventana (todas las barras, agregado):")
            for reason, n in sorted(r["window_reason_hist"].items(), key=lambda x: -x[1]):
                pct = n * 100 / total_win if total_win else 0
                print(f"      {reason:<32} {n:>5}  ({pct:.0f}%)")
        if r["top_misses"]:
            print(f"\n    Top 10 misses más grandes  (relax@ = offset 4h más temprano del gate vinculante):")
            print(f"      {'#':>3} {'Symbol':<14} {'Gain':>8}  {'Razón':<30} {'relax@':>7} Timestamp")
            for i, m in enumerate(r["top_misses"][:10], 1):
                ts = m.get("ts", "")[:16]
                rel = m.get("relax_offset")
                rel_s = f"{rel:+d}" if isinstance(rel, int) else "—"
                print(f"      {i:>3} {m['symbol']:<14} {m['gain_pct']:>+7.1f}%  "
                      f"{m['reason']:<30} {rel_s:>7} {ts}")
        bo_f = r.get("bo_breakout_features")
        if bo_f and bo_f.get("distributions"):
            print(f"\n    [Step 0] Features 4h de los {bo_f['count']} misses bo:breakout")
            print(f"      {'feature':<20}  {'p10':>7} {'p25':>7} {'p50':>7} {'p75':>7} {'p90':>7}")
            _fkeys = ["breakout_distance", "vol_ratio", "obv_slope", "cvd_ratio",
                      "width_curr", "candle_body_pct"]
            for fk in _fkeys:
                dist = bo_f["distributions"].get(fk, {})
                print(f"      {fk:<20}  "
                      f"{dist.get('p10', float('nan')):>7.3f} "
                      f"{dist.get('p25', float('nan')):>7.3f} "
                      f"{dist.get('p50', float('nan')):>7.3f} "
                      f"{dist.get('p75', float('nan')):>7.3f} "
                      f"{dist.get('p90', float('nan')):>7.3f}")
            sc_rate = bo_f["distributions"].get("strong_close_rate", 0)
            print(f"      {'strong_close_rate':<20}  {sc_rate:.1%}")

    ts_str   = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M")
    out_path = AUDIT_RESULTS / f"catch_rate_swing_{ts_str}.json"
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump({
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "period_days":  args.days,
            "thresholds":   results,
        }, f, ensure_ascii=False, indent=2)
    print(f"\n  Resultado guardado en: {out_path}")
    print(f"  Post-mortem: python audit_misses.py post-mortem SYMBOL TIMESTAMP")


# ── Subcomando: distributions ─────────────────────────────────────────────────

def _percentiles(vals, ps=(10, 25, 50, 75, 90, 95)):
    if not vals:
        return {f"p{p}": float("nan") for p in ps}
    a = sorted(vals)
    n = len(a)
    result = {}
    for p in ps:
        idx = min(int(n * p / 100), n - 1)
        result[f"p{p}"] = round(a[idx], 4)
    return result


def cmd_distributions(args, cfg):
    end_dt   = datetime.now(timezone.utc).replace(minute=0, second=0, microsecond=0)
    start_dt = end_dt - timedelta(days=args.days)
    start_ms = int(start_dt.timestamp() * 1000)
    end_ms   = int(end_dt.timestamp() * 1000)

    print(f"\n=== DISTRIBUTIONS (swing) — últimos {args.days} días ===")
    print(f"  Período: {start_dt.date()} → {end_dt.date()}")

    min_vol = cfg.g("general", "MIN_QUOTE_VOLUME")
    print(f"\n[1/4] Obteniendo universo...")
    symbols = get_top_pairs_today(min_vol, top_n=args.max_pairs)
    print(f"  {len(symbols)} pares activos")

    print(f"\n[2/4] Descargando klines...")
    klines = download_all_klines(symbols, start_dt, end_dt)
    print(f"  {len(klines)} pares con datos")

    print(f"\n[3/4] Precomputando indicadores...")
    prepared = {}
    for sym, tfs in klines.items():
        entry = {}
        for tf in ("1h", "4h", "1d"):
            df = tfs.get(tf)
            if df is not None:
                entry[tf] = precompute_indicators(df, cfg)
        if all(t in entry for t in ("1h", "4h", "1d")):
            prepared[sym] = entry
    print(f"  {len(prepared)} pares listos")

    # Detectar pump-starts para filtrar barras "de mover"
    pump_bars = set()   # (sym, bar_idx_4h) dentro de PRE_PUMP_WINDOW de cualquier pump
    for label, threshold, window in MOVER_THRESHOLDS:
        for sym, sym_prep in prepared.items():
            df_4h = sym_prep.get("4h")
            if df_4h is None:
                continue
            for bar_t, _ in find_pump_starts(df_4h, threshold, window, start_ms, end_ms):
                for offset in range(-PRE_PUMP_WINDOW, 1):
                    pump_bars.add((sym, bar_t + offset))

    print(f"\n[4/4] Calculando distribuciones...")
    obv_lookback = cfg.g("indicators", "OBV_SLOPE_LOOKBACK", default=10)
    cvd_lookback = cfg.g("indicators", "CVD_LOOKBACK", default=10)

    # Colecciones: todas las barras vs barras-de-mover
    all_atr,  pump_atr  = [], []
    all_vol,  pump_vol  = [], []
    all_obv,  pump_obv  = [], []
    all_cvd,  pump_cvd  = [], []
    all_msd,  pump_msd  = [], []   # major_struct_dist (dist al máximo mayor 1d)
    all_bbw,  pump_bbw  = [], []   # bb_width 4h
    all_ema,  pump_ema  = [], []   # ema_trend_up 1d (0/1)
    all_giw,  pump_giw  = [], []   # gain_in_window (retorno fwd de PRE_PUMP_WINDOW barras)

    major_struct_max_dist = cfg.g("hold", "MAJOR_STRUCT_MAX_DIST", default=0.12)

    for sym, sym_prep in prepared.items():
        df_4h = sym_prep["4h"]
        df_1d = sym_prep["1d"]

        # Índices 4h dentro del período
        cts_4h = df_4h["close_time"].values.astype(np.int64)
        mask_4h = (cts_4h >= start_ms) & (cts_4h <= end_ms)
        idxs_4h = np.where(mask_4h)[0]

        # OBV slope vectorizado
        vm_4h = df_4h["_vol_mean20"].replace(0, np.nan)
        obv_slope_s = (df_4h["_obv"].diff(obv_lookback) / vm_4h / obv_lookback).fillna(0.0)

        # CVD ratio vectorizado
        cvd_slope_s   = df_4h["_cvd"].diff(cvd_lookback)
        cvd_vol_sum_s = df_4h["_cvd_vol_sum"].replace(0, np.nan)
        cvd_ratio_s   = (cvd_slope_s / cvd_vol_sum_s).fillna(0.0)

        # ATR y major_max desde 1d: mapeamos cada barra 4h → 1d más cercana
        cts_1d      = df_1d["close_time"].values.astype(np.int64)
        atr_1d      = df_1d["_atr_pct"].values.astype(float)
        close_1d    = df_1d["close"].values.astype(float)
        major_max_1d = df_1d["_major_max"].values.astype(float) if "_major_max" in df_1d.columns else None

        # ema_trend_up vectorizado sobre 1d: precio > EMA lento y EMA lento sube
        ema_slow_1d   = df_1d["_ema_slow"].values.astype(float)
        ema_trend_arr = (
            (close_1d > ema_slow_1d)
            & (ema_slow_1d > np.concatenate([[np.nan] * 3, ema_slow_1d[:-3]]))
        ).astype(float)  # 1.0 / 0.0 / nan al inicio

        # gain_in_window: retorno forward de PRE_PUMP_WINDOW barras sobre 4h
        close_4h = df_4h["close"].values.astype(float)
        high_4h  = df_4h["high"].values.astype(float)
        n4 = len(close_4h)
        h_ser = pd.Series(high_4h)
        fwd_max_4h = (
            h_ser[::-1]
            .rolling(window=PRE_PUMP_WINDOW, min_periods=PRE_PUMP_WINDOW)
            .max()[::-1]
            .shift(-1)
            .values
        )

        for idx in idxs_4h:
            if idx < 40:
                continue
            ts_bar = int(cts_4h[idx])

            # vol_ratio, bb_width, gain_in_window (4h)
            vr    = float(df_4h["_vol_ratio"].iat[idx])
            bbw_v = float(df_4h["_bb_width"].iat[idx])
            c4    = float(close_4h[idx])
            fw    = float(fwd_max_4h[idx]) if idx + PRE_PUMP_WINDOW < n4 else float("nan")
            giw_v = (fw / c4 - 1.0) if (c4 > 0 and not np.isnan(fw)) else float("nan")

            # obv slope, cvd ratio (4h)
            obv_v = float(obv_slope_s.iat[idx])
            cvd_v = float(cvd_ratio_s.iat[idx])

            # features desde 1d (mapeo 4h → 1d)
            idx_1d = np.searchsorted(cts_1d, ts_bar, side="right") - 1
            if idx_1d < 0 or idx_1d >= len(atr_1d):
                continue
            atr_v = float(atr_1d[idx_1d])

            # major_struct_dist: (major_max − precio) / precio  (>0 = por debajo del máx)
            msd_v = float("nan")
            if major_max_1d is not None and idx_1d < len(major_max_1d):
                mm = float(major_max_1d[idx_1d])
                c1 = float(close_1d[idx_1d])
                if c1 > 0 and not np.isnan(mm):
                    msd_v = (mm - c1) / c1

            # ema_trend_up 1d (0/1)
            ema_v = float(ema_trend_arr[idx_1d]) if idx_1d < len(ema_trend_arr) else float("nan")

            is_pump = (sym, idx) in pump_bars

            all_atr.append(atr_v);  all_vol.append(vr)
            all_obv.append(obv_v);  all_cvd.append(cvd_v)
            all_bbw.append(bbw_v)
            if not np.isnan(msd_v): all_msd.append(msd_v)
            if not np.isnan(ema_v): all_ema.append(ema_v)
            if not np.isnan(giw_v): all_giw.append(giw_v)
            if is_pump:
                pump_atr.append(atr_v); pump_vol.append(vr)
                pump_obv.append(obv_v); pump_cvd.append(cvd_v)
                pump_bbw.append(bbw_v)
                if not np.isnan(msd_v): pump_msd.append(msd_v)
                if not np.isnan(ema_v): pump_ema.append(ema_v)
                if not np.isnan(giw_v): pump_giw.append(giw_v)

    # ── Reporte ──────────────────────────────────────────────────────────────
    print()
    print("═" * 70)
    print(f" DISTRIBUCIONES — universo swing  (total 4h bars: {len(all_atr)})")
    print(f"                                  (pump-start bars: {len(pump_atr)})")
    print("═" * 70)

    def show_dist(label, all_v, pump_v):
        print(f"\n  [{label}]")
        d = _percentiles(all_v)
        print(f"    Todas las barras  (n={len(all_v):>7}): "
              f"p10={d['p10']:>7.4f}  p25={d['p25']:>7.4f}  p50={d['p50']:>7.4f}"
              f"  p75={d['p75']:>7.4f}  p90={d['p90']:>7.4f}  p95={d['p95']:>7.4f}")
        if pump_v:
            d2 = _percentiles(pump_v)
            print(f"    Barras de pump    (n={len(pump_v):>7}): "
                  f"p10={d2['p10']:>7.4f}  p25={d2['p25']:>7.4f}  p50={d2['p50']:>7.4f}"
                  f"  p75={d2['p75']:>7.4f}  p90={d2['p90']:>7.4f}  p95={d2['p95']:>7.4f}")
        return {"all": _percentiles(all_v), "pump": _percentiles(pump_v)}

    def show_rate(label, all_v, pump_v):
        """Para features binarias (0/1): reportar fracción de 1s."""
        r_all  = sum(all_v)  / len(all_v)  if all_v  else float("nan")
        r_pump = sum(pump_v) / len(pump_v) if pump_v else float("nan")
        print(f"\n  [{label}]")
        print(f"    Todas las barras  (n={len(all_v):>7}): tasa = {r_all:.3f}")
        print(f"    Barras de pump    (n={len(pump_v):>7}): tasa = {r_pump:.3f}")
        return {"all_rate": round(r_all, 4), "pump_rate": round(r_pump, 4)}

    dist_data = {}
    print("\n── Prioridad 1: gate pre_major_struct ──")
    dist_data["major_struct_dist"] = show_dist(
        f"major_struct_dist 1d  (gate ≤{major_struct_max_dist:.2f}; +grande = más lejos del máx, más bloqueado)",
        all_msd, pump_msd)
    # fracción de barras que PASARÍAN el gate
    n_all_pass  = sum(1 for v in all_msd  if v <= major_struct_max_dist)
    n_pump_pass = sum(1 for v in pump_msd if v <= major_struct_max_dist)
    r_all_pass  = n_all_pass  / len(all_msd)  if all_msd  else float("nan")
    r_pump_pass = n_pump_pass / len(pump_msd) if pump_msd else float("nan")
    print(f"    Fracción que pasa el gate (dist≤{major_struct_max_dist:.2f}):  "
          f"población={r_all_pass:.2%}  movers={r_pump_pass:.2%}")
    dist_data["major_struct_dist"]["pct_pass_gate"] = {
        "all": round(r_all_pass, 4), "pump": round(r_pump_pass, 4)}

    print("\n── Prioridad 2: tendencia y compresión ──")
    dist_data["ema_trend_up_1d"]  = show_rate(
        "ema_trend_up 1d  (fracción barras con EMA alcista)", all_ema, pump_ema)
    dist_data["bb_width_4h"]      = show_dist(
        "bb_width 4h  (compresión previa; menor = más comprimido)", all_bbw, pump_bbw)

    print("\n── Prioridad 3: señales clásicas ──")
    dist_data["atr_pct_1d"]   = show_dist("atr_pct_1d    (umbral ATR_MIN_PCT, gate duro)", all_atr, pump_atr)
    dist_data["vol_ratio_4h"] = show_dist("vol_ratio_4h  (BREAKOUT_MIN_VOL_RATIO, PREBREAK_MIN_VOL_RATIO)", all_vol, pump_vol)
    dist_data["obv_slope_4h"] = show_dist("obv_slope_4h  (OBV_RISING_MIN)", all_obv, pump_obv)
    dist_data["cvd_ratio_4h"] = show_dist("cvd_ratio_4h  (CVD_BULLISH_MIN)", all_cvd, pump_cvd)

    print("\n── Prioridad 4: lateness ──")
    dist_data["gain_in_window"] = show_dist(
        f"gain_in_window 4h  (retorno fwd {PRE_PUMP_WINDOW} barras; +alto = barra temprana)",
        all_giw, pump_giw)

    # Sugerencia de umbrales derivada de la distribución
    print("\n─── Sugerencia de umbrales (p30 de barras-de-pump) ─────────────")
    for key, label_cfg, pump_v in [
        ("major_struct_dist", "MAJOR_STRUCT_MAX_DIST",    pump_msd),
        ("atr_pct_1d",        "ATR_MIN_PCT",              pump_atr),
        ("vol_ratio_4h",      "BREAKOUT_MIN_VOL_RATIO",   pump_vol),
        ("obv_slope_4h",      "OBV_RISING_MIN",           pump_obv),
        ("cvd_ratio_4h",      "CVD_BULLISH_MIN",          pump_cvd),
    ]:
        if pump_v:
            a   = sorted(pump_v)
            p30 = a[max(0, int(len(a) * 0.30))]
            print(f"    {label_cfg:<30} p30 de pumps = {p30:.4f}")
        else:
            print(f"    {label_cfg:<30} sin datos de pumps")

    ts_str   = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M")
    out_path = AUDIT_RESULTS / f"distributions_swing_{ts_str}.json"
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump({
            "generated_at":  datetime.now(timezone.utc).isoformat(),
            "period_days":   args.days,
            "total_bars":    len(all_atr),
            "pump_bars":     len(pump_atr),
            "distributions": dist_data,
        }, f, ensure_ascii=False, indent=2)
    print(f"\n  Resultado guardado en: {out_path}")


# ── Subcomando: post-mortem ───────────────────────────────────────────────────

def cmd_post_mortem(args, cfg):
    symbol = args.symbol.upper()

    ts_raw = args.timestamp
    try:
        ts_ms = int(ts_raw)
    except ValueError:
        try:
            dt = datetime.fromisoformat(ts_raw.replace("Z", "+00:00"))
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            ts_ms = int(dt.timestamp() * 1000)
        except Exception:
            sys.exit(f"FATAL: timestamp '{ts_raw}' inválido. Usar ISO 8601 o ms Unix.")

    ts_human = datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc).isoformat()
    print(f"\n=== POST-MORTEM (swing): {symbol} @ {ts_human} ===")

    end_dt   = datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc) + timedelta(hours=24)
    start_dt = end_dt - timedelta(days=30)

    print(f"\nDescargando klines para {symbol}...")
    klines = download_all_klines([symbol], start_dt, end_dt)
    if symbol not in klines:
        sys.exit(f"FATAL: sin datos para {symbol}")

    sym_prep = {}
    for tf in ("1h", "4h", "1d", "1w"):
        df = klines[symbol].get(tf)
        if df is not None:
            sym_prep[tf] = precompute_indicators(df, cfg)

    df_4h = sym_prep.get("4h")
    if df_4h is None:
        sys.exit("FATAL: sin datos 4h")

    bar_idx = find_idx_at_or_before(df_4h, ts_ms)
    if bar_idx < 40:
        sys.exit(f"FATAL: bar_idx={bar_idx} insuficiente (necesita ≥ 40 barras 4h)")

    bar_ts    = int(df_4h["close_time"].iat[bar_idx])
    bar_human = datetime.fromtimestamp(bar_ts / 1000, tz=timezone.utc).isoformat()
    print(f"  Bar 4h: idx={bar_idx}, close_time={bar_human}")

    tf_data = get_tf_data_at(sym_prep, bar_idx, cfg)
    if tf_data is None:
        sys.exit("FATAL: datos insuficientes (necesita 1h, 4h y 1d)")

    # ── Features por TF ──────────────────────────────────────────────────────
    print("\n─── FEATURES POR TIMEFRAME ─────────────────────────────────────")
    FIELDS = [
        "price", "ema_trend_up", "atr_pct", "vol_ratio", "vol_growth",
        "strong_close", "candle_body_pct", "close_change_curr", "width_curr", "width_expansion",
        "recent_max", "near_recent_max", "breakout", "breakout_distance",
        "recent_long_ok", "obv_slope", "obv_rising", "cvd_ratio", "cvd_bullish",
        "not_near_resistance", "dist_to_res", "major_struct_ok", "major_struct_dist",
        "bars_since_major_max",
        "hold_recent_break", "hold_kept_zone", "hold_pullback_ok", "hold_strong",
        "bars_since_break", "riding_gain", "riding_above_zone", "riding_vol_ok",
        "riding_bars_since", "fading_reversal",
    ]
    for tf_name in ("1h", "4h", "1d"):
        d = tf_data[tf_name]
        print(f"\n  [{tf_name}]")
        for field in FIELDS:
            val = d.get(field)
            if val is None:
                continue
            if isinstance(val, float):
                print(f"    {field:<34} {val:>+.4f}")
            else:
                print(f"    {field:<34} {val}")

    # ── Pre-gates (1d) ────────────────────────────────────────────────────────
    print("\n─── PRE-GATES (1d) ─────────────────────────────────────────────")
    tf_1d    = tf_data["1d"]
    atr_thr  = tf_1d.get("atr_threshold", cfg.g("indicators", "ATR_MIN_PCT"))
    gates = [
        ("ema_trend_up (1d)",                              tf_1d.get("ema_trend_up")),
        ("not_near_resistance (1d)",                       tf_1d.get("not_near_resistance")),
        (f"atr_pct >= {atr_thr:.3f} — actual {tf_1d.get('atr_pct', 0):.3f}", tf_1d.get("atr_pct", 0) >= atr_thr),
        ("major_struct_ok (1d)",                           tf_1d.get("major_struct_ok", True)),
    ]
    all_pass = True
    for name, val in gates:
        mark = "✓" if val else "✗"
        print(f"  {mark} {name:<50} {val}")
        if not val:
            all_pass = False

    # ── Clasificación ─────────────────────────────────────────────────────────
    print("\n─── CLASIFICACIÓN ───────────────────────────────────────────────")
    alert, reason, _prox = classify_explain(symbol, tf_data, cfg)
    if alert is not None:
        print(f"  ✓ SEÑAL: {alert['label']}  score={alert['score']}  bucket={alert['bucket']}")
        for r in alert.get("reasons", []):
            print(f"    • {r}")
    else:
        if all_pass:
            print(f"  ✗ Sin señal — pasó pre-gates pero ningún detector disparó")
            _diagnose_signals(tf_data, cfg)
        else:
            print(f"  ✗ Sin señal — bloqueado en pre-gate: {reason}")


def _diagnose_signals(tf_data, cfg):
    tf_1h = tf_data["1h"]
    tf_4h = tf_data["4h"]
    tf_1d = tf_data["1d"]

    print("\n  ─ Diagnóstico por señal:")

    def show_checks(label, checks):
        fails = [k for k, v in checks.items() if not v]
        status = "✗" if fails else "✓"
        print(f"  {status} {label}")
        if fails:
            for k, v in checks.items():
                mark = "  ✓" if v else "  ✗"
                print(f"    {mark} {k}")
        else:
            print(f"    (todos los checks pasan — ¿debería haber disparado?)")

    # PREBREAK
    show_checks("PREBREAK", {
        "near_recent_max":
            bool(tf_1h.get("near_recent_max")),
        f"BB_width <= {cfg.g('prebreak','PREBREAK_BB_WIDTH_MAX')} (actual {tf_1h.get('width_curr',0):.4f})":
            tf_1h.get("width_curr", 9) <= cfg.g("prebreak", "PREBREAK_BB_WIDTH_MAX"),
        f"vol_ratio >= {cfg.g('prebreak','PREBREAK_MIN_VOL_RATIO')} (actual {tf_1h.get('vol_ratio',0):.2f})":
            tf_1h.get("vol_ratio", 0) >= cfg.g("prebreak", "PREBREAK_MIN_VOL_RATIO"),
        f"vol_growth >= {cfg.g('prebreak','PREBREAK_VOLUME_GROWTH_MIN')} (actual {tf_1h.get('vol_growth',0):.2f})":
            tf_1h.get("vol_growth", 0) >= cfg.g("prebreak", "PREBREAK_VOLUME_GROWTH_MIN"),
    })

    # BREAKOUT
    require_obv = cfg.g("breakout", "BREAKOUT_REQUIRE_OBV_NON_NEGATIVE", default=True)
    show_checks("BREAKOUT", {
        f"breakout (price > recent_max × {1+cfg.g('breakout','BREAKOUT_BUFFER'):.3f})":
            bool(tf_4h.get("breakout")),
        f"vol_ratio_4h >= {cfg.g('breakout','BREAKOUT_MIN_VOL_RATIO')} (actual {tf_4h.get('vol_ratio',0):.2f})":
            tf_4h.get("vol_ratio", 0) >= cfg.g("breakout", "BREAKOUT_MIN_VOL_RATIO"),
        f"distance <= {cfg.g('breakout','BREAKOUT_MAX_EXTENDED')} (actual {tf_4h.get('breakout_distance',0):.4f})":
            tf_4h.get("breakout_distance", 9) <= cfg.g("breakout", "BREAKOUT_MAX_EXTENDED"),
        f"BB_exp >= {cfg.g('breakout','BREAKOUT_BB_EXPANSION_MIN')} (actual {tf_4h.get('width_expansion',0):.4f})":
            tf_4h.get("width_expansion", -9) >= cfg.g("breakout", "BREAKOUT_BB_EXPANSION_MIN"),
        f"strong_close_4h (actual pos={tf_4h.get('_close_pos',0):.2f})":
            bool(tf_4h.get("strong_close")),
        f"body >= {cfg.g('breakout','BREAKOUT_MIN_BODY_PCT')} (actual {tf_4h.get('candle_body_pct',0):.2f})":
            tf_4h.get("candle_body_pct", 0) >= cfg.g("breakout", "BREAKOUT_MIN_BODY_PCT"),
        f"1h_vol >= {cfg.g('breakout','BREAKOUT_1H_MIN_VOL_RATIO', default=1.0)} (actual {tf_1h.get('vol_ratio',0):.2f})":
            tf_1h.get("vol_ratio", 0) >= cfg.g("breakout", "BREAKOUT_1H_MIN_VOL_RATIO", default=1.0),
        "1h strong_close":
            bool(tf_1h.get("strong_close")),
        "obv_slope >= 0":
            (not require_obv) or tf_4h.get("obv_slope", 0) >= 0,
    })

    # RIDING
    rg    = tf_4h.get("riding_gain") or 0.0
    min_g = cfg.g("riding", "RIDING_MIN_GAIN")
    max_g = cfg.g("riding", "RIDING_MAX_GAIN")
    max_fade = cfg.g("riding", "RIDING_MAX_FADE_FROM_HIGH", default=0.025)
    show_checks("RIDING", {
        "riding_above_zone":
            bool(tf_4h.get("riding_above_zone")),
        "riding_vol_ok":
            bool(tf_4h.get("riding_vol_ok")),
        f"gain in [{min_g:.3f}, {max_g:.3f}] (actual {rg:.4f})":
            min_g <= rg <= max_g,
        f"riding_bars_since >= 1 (actual {tf_4h.get('riding_bars_since')})":
            tf_4h.get("riding_bars_since") is not None and tf_4h["riding_bars_since"] >= 1,
        "not in active breakout":
            not bool(tf_4h.get("breakout")),
        "ema_trend_up (1d) [si RIDING_EMA_MUST_TREND]":
            (not cfg.g("riding", "RIDING_EMA_MUST_TREND", default=True)) or bool(tf_1d.get("ema_trend_up")),
        f"fade_ok >= -{max_fade} (actual {tf_4h.get('fading_reversal',0):.4f})":
            (tf_4h.get("fading_reversal") or 0.0) >= -max_fade,
    })

    # HOLD
    show_checks("HOLD", {
        "hold_recent_break": bool(tf_4h.get("hold_recent_break")),
        "hold_kept_zone":    bool(tf_4h.get("hold_kept_zone")),
        "hold_pullback_ok":  bool(tf_4h.get("hold_pullback_ok")),
        "hold_strong":       bool(tf_4h.get("hold_strong")),
    })


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Auditoría de cobertura del screener swing")
    sub = parser.add_subparsers(dest="cmd", required=True)

    p_cr = sub.add_parser("catch-rate", help="Catch-rate real vs universo de movers (4h bars)")
    p_cr.add_argument("--days",      type=int, default=28,  help="Días a analizar (default 28)")
    p_cr.add_argument("--max-pairs", type=int, default=200, help="Máximo de pares (default 200)")
    p_cr.add_argument("--quick",     action="store_true",
                      help="Modo dev rápido: max-pairs=50, days=14")
    p_cr.add_argument("--pre-pump-window", type=int, default=PRE_PUMP_WINDOW,
                      help=f"Barras 4h antes del pump a revisar (default {PRE_PUMP_WINDOW} = 168h / 7d)")

    p_di = sub.add_parser("distributions", help="Distribución empírica de indicadores 4h/1d")
    p_di.add_argument("--days",      type=int, default=28,  help="Días a analizar (default 28)")
    p_di.add_argument("--max-pairs", type=int, default=200, help="Máximo de pares (default 200)")

    p_pm = sub.add_parser("post-mortem", help="Replay analyze+classify para símbolo+timestamp")
    p_pm.add_argument("symbol",    help="Símbolo ej. BTCUSDT")
    p_pm.add_argument("timestamp", help="ISO 8601 ej. 2025-01-15T14:30:00  o  ms Unix")

    parser.add_argument(
        "--config", default=str(CONFIG_PATH),
        help="Path a config.json (default: swing/config.json)",
    )

    args = parser.parse_args()
    cfg  = Config(Path(args.config))

    if args.cmd == "catch-rate" and getattr(args, "quick", False):
        args.max_pairs = min(args.max_pairs, 50)
        args.days      = min(args.days, 14)
        print(f"  [quick] max-pairs={args.max_pairs}, days={args.days}")

    if args.cmd == "catch-rate":
        cmd_catch_rate(args, cfg)
    elif args.cmd == "distributions":
        cmd_distributions(args, cfg)
    elif args.cmd == "post-mortem":
        cmd_post_mortem(args, cfg)


if __name__ == "__main__":
    main()
