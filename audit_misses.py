"""
audit_misses.py — Auditoría de cobertura del screener.

Subcomandos:
  catch-rate             Mide catch-rate real vs universo de movers
  post-mortem SYM TS     Replay de analyze+classify para símbolo+timestamp
  false-positives        Analiza outcomes_dump.json: alertas que no se cumplieron
  lateness               Mide cuánto tarde entran los BREAKOUTs (chasing)

Uso:
  python audit_misses.py catch-rate [--days 14] [--max-pairs 400]
  python audit_misses.py post-mortem BTCUSDT 2025-01-15T14:30:00
  python audit_misses.py post-mortem BTCUSDT 1705327800000
  python audit_misses.py false-positives [--dump outcomes_dump.json] [--days 14]
  python audit_misses.py lateness [--dump outcomes_dump.json] [--days 30]
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
    find_idx_at_or_before,
    classify,
)

CONFIG_PATH   = Path(__file__).parent / "config.json"
AUDIT_CACHE   = Path(__file__).parent / ".audit_cache"
AUDIT_RESULTS = Path(__file__).parent / ".audit_results"
AUDIT_CACHE.mkdir(exist_ok=True)
AUDIT_RESULTS.mkdir(exist_ok=True)

# Umbrales de "mover" acordados: (etiqueta, umbral_ganancia, ventana_barras_15m)
MOVER_THRESHOLDS = [
    ("5pct_4h",   0.05,  16),   # ≥5%  en 4h  (16 × 15m)
    ("10pct_24h", 0.10,  96),   # ≥10% en 24h (96 × 15m)
    ("15pct_24h", 0.15,  96),   # ≥15% en 24h
]

# Ventana de verificación: cuántas barras 15m ANTES del pump revisamos
# [-PRE_PUMP_WINDOW .. 0]: si alguna da alerta, el screener "pescó" el move
PRE_PUMP_WINDOW = 4


# ── Clasificación con diagnóstico ─────────────────────────────────────────────

def _get_signal_fail_reason(tf5, tf15, cfg):
    """Devuelve razón específica cuando todos los pre-gates pasaron pero ninguna señal disparó.
    Evalúa las gates de cada señal en orden, elige la que más estuvo a punto de pasar
    (mayor cantidad de checks superados) y devuelve '<prefijo>:<primera_gate_fallida>'.
    Formato ejemplos: 'bo:bb_exp', 'pre:bb_width', 'ri:gain_range', 'ho:recent_break'."""
    require_obv = cfg.g("breakout", "BREAKOUT_REQUIRE_OBV_NON_NEGATIVE", default=True)
    min_g = cfg.g("riding", "RIDING_MIN_GAIN", default=0.005)
    max_g = cfg.g("riding", "RIDING_MAX_GAIN", default=0.15)

    signals = {
        "pre": [
            ("near_max",   bool(tf5.get("near_recent_max"))),
            ("bb_width",   tf5.get("width_curr", 9) <= cfg.g("prebreak", "PREBREAK_BB_WIDTH_MAX")),
            ("vol_ratio",  tf5.get("vol_ratio", 0) >= cfg.g("prebreak", "PREBREAK_MIN_VOL_RATIO")),
            ("vol_growth", tf5.get("vol_growth", 0) >= cfg.g("prebreak", "PREBREAK_VOLUME_GROWTH_MIN")),
        ],
        "bo": [
            ("breakout",   bool(tf15.get("breakout"))),
            ("vol_ratio",  tf15.get("vol_ratio", 0) >= cfg.g("breakout", "BREAKOUT_MIN_VOL_RATIO")),
            ("distance",   tf15.get("breakout_distance", 9) <= cfg.g("breakout", "BREAKOUT_MAX_EXTENDED")),
            ("bb_exp",     tf15.get("width_expansion", -9) >= cfg.g("breakout", "BREAKOUT_BB_EXPANSION_MIN")),
            ("strong_close", bool(tf15.get("strong_close"))),
            ("body",       tf15.get("candle_body_pct", 0) >= cfg.g("breakout", "BREAKOUT_MIN_BODY_PCT")),
            ("5m_vol",     tf5.get("vol_ratio", 0) >= cfg.g("breakout", "BREAKOUT_5M_MIN_VOL_RATIO")),
            ("5m_close",   bool(tf5.get("strong_close"))),
            ("obv_nonneg", (not require_obv) or tf15.get("obv_slope", 0) >= 0),
        ],
        "ri": [
            ("above_zone", bool(tf15.get("riding_above_zone"))),
            ("vol_ok",     bool(tf15.get("riding_vol_ok"))),
            ("gain_range", min_g <= (tf15.get("riding_gain") or 0) <= max_g),
            ("bars_since", tf15.get("riding_bars_since") is not None and tf15.get("riding_bars_since", 0) >= 1),
            ("no_breakout", not bool(tf15.get("breakout"))),
        ],
        "ho": [
            ("recent_break", bool(tf15.get("hold_recent_break"))),
            ("kept_zone",    bool(tf15.get("hold_kept_zone"))),
            ("pullback_ok",  bool(tf15.get("hold_pullback_ok"))),
            ("strong",       bool(tf15.get("hold_strong"))),
        ],
    }

    best_prefix = "all"
    best_fail   = "signals"
    best_passes = -1

    for prefix, checks in signals.items():
        passes = sum(v for _, v in checks)
        first_fail = next((k for k, v in checks if not v), None)
        if first_fail is None:
            return f"{prefix}:cooldown_or_active"
        if passes > best_passes:
            best_passes = passes
            best_prefix = prefix
            best_fail   = first_fail

    return f"{best_prefix}:{best_fail}"


def classify_explain(symbol, tf_data, cfg):
    """Llama classify() y si devuelve None explica la primera causa.
    Devuelve (alert_o_None, razon_str_o_None)."""
    tf5  = tf_data.get("5m")  or {}
    tf15 = tf_data.get("15m") or {}
    tf1h = tf_data.get("1h")  or {}
    if not tf5 or not tf15 or not tf1h:
        return None, "data_missing"

    # classify() re-deriva obv_rising/cvd_bullish internamente; lo hacemos aquí
    # también para el chequeo de pre-gates sin depender del orden de llamadas.
    obv_min = cfg.g("indicators", "OBV_RISING_MIN", default=0.05)
    cvd_min = cfg.g("indicators", "CVD_BULLISH_MIN", default=0.05)
    tf15 = dict(tf15)
    tf15["obv_rising"]  = tf15.get("obv_slope",  0) >= obv_min
    tf15["cvd_bullish"] = tf15.get("cvd_ratio", 0)  >= cvd_min
    tf_data = {**tf_data, "15m": tf15}

    if not tf1h.get("ema_trend_up"):
        return None, "pre_ema_trend"
    if not tf1h.get("not_near_resistance"):
        return None, "pre_resist"
    if tf1h.get("atr_pct", 0) < cfg.g("indicators", "ATR_MIN_PCT"):
        return None, "pre_atr"
    if not tf1h.get("major_struct_ok", True):
        return None, "pre_major_struct"

    result = classify(symbol, tf_data, cfg)
    if result is not None:
        return result, None
    return None, _get_signal_fail_reason(tf5, tf15, cfg)


# ── Detección de pump-starts ──────────────────────────────────────────────────

def find_pump_starts(df_15m, threshold, window_bars, start_ms, end_ms):
    """Devuelve lista de (bar_idx, gain_pct) donde empieza un pump >= threshold.
    Deduplicado: tras un pump_start, no cuenta otro dentro de window_bars barras."""
    close = df_15m["close"].values.astype(float)
    high  = df_15m["high"].values.astype(float)
    cts   = df_15m["close_time"].values.astype(np.int64)
    n = len(close)

    # forward max vectorizado: fwd_max[t] = max(high[t+1 .. t+window_bars])
    # Invirtiendo la serie y aplicando rolling max de window_bars con min_periods completo
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


# ── Análisis de un bar en los 3 TFs ──────────────────────────────────────────

def get_tf_data_at(prepared_sym, bar_idx_15m, cfg):
    """Construye tf_data para un símbolo en bar_idx_15m.
    Mapea el timestamp 15m al bar correcto de 5m y 1h."""
    df_15m = prepared_sym.get("15m")
    df_5m  = prepared_sym.get("5m")
    df_1h  = prepared_sym.get("1h")
    if df_15m is None or df_5m is None or df_1h is None:
        return None
    if bar_idx_15m >= len(df_15m):
        return None

    ts_ms = int(df_15m["close_time"].iat[bar_idx_15m])
    idx_5m = find_idx_at_or_before(df_5m, ts_ms)
    idx_1h = find_idx_at_or_before(df_1h, ts_ms)

    if idx_5m < 80 or idx_1h < 80 or bar_idx_15m < 80:
        return None

    r5  = analyze_at_index(df_5m,  idx_5m,       cfg)
    r15 = analyze_at_index(df_15m, bar_idx_15m,  cfg)
    r1h = analyze_at_index(df_1h,  idx_1h,       cfg)

    if r5 is None or r15 is None or r1h is None:
        return None
    return {"5m": r5, "15m": r15, "1h": r1h}


# ── Subcomando: catch-rate ────────────────────────────────────────────────────

def cmd_catch_rate(args, cfg):
    end_dt   = datetime.now(timezone.utc).replace(minute=0, second=0, microsecond=0)
    start_dt = end_dt - timedelta(days=args.days)
    start_ms = int(start_dt.timestamp() * 1000)
    end_ms   = int(end_dt.timestamp() * 1000)

    print(f"\n=== CATCH-RATE AUDIT — últimos {args.days} días ===")
    print(f"  Período: {start_dt.date()} → {end_dt.date()}")

    # 1. Universo
    min_vol = cfg.g("general", "MIN_QUOTE_VOLUME")
    print(f"\n[1/5] Obteniendo universo (vol >= {min_vol/1e6:.0f}M USDT)...")
    symbols = get_top_pairs_today(min_vol, top_n=args.max_pairs)
    print(f"  {len(symbols)} pares activos")

    # 2. Klines
    print(f"\n[2/5] Descargando klines ({len(symbols)} pares)...")
    klines = download_all_klines(symbols, start_dt, end_dt)
    print(f"  {len(klines)} pares con datos completos")

    # 3. Precomputar indicadores
    print(f"\n[3/5] Precomputando indicadores...")
    prepared = {}
    for sym, tfs in klines.items():
        entry = {}
        for tf in ("5m", "15m", "1h"):
            df = tfs.get(tf)
            if df is not None:
                entry[tf] = precompute_indicators(df, cfg)
        if len(entry) == 3:
            prepared[sym] = entry
    print(f"  {len(prepared)} pares listos")

    # 4. Detectar movers (ground truth)
    print(f"\n[4/5] Detectando pump-starts...")
    mover_events = {}
    for label, threshold, window in MOVER_THRESHOLDS:
        events = []
        for sym, sym_prep in prepared.items():
            df_15m = sym_prep.get("15m")
            if df_15m is None:
                continue
            starts = find_pump_starts(df_15m, threshold, window, start_ms, end_ms)
            for bar_t, gain_pct in starts:
                events.append((sym, bar_t, gain_pct))
        mover_events[label] = events
        print(f"  {label}: {len(events)} pump-starts detectados")

    # 5. Pasada del screener
    print(f"\n[5/5] Evaluando cobertura del screener...")
    results = {}
    for label, threshold, window in MOVER_THRESHOLDS:
        events = mover_events[label]
        caught = []
        missed_list = []
        reasons = Counter()

        for sym, pump_t, gain_pct in events:
            sym_prep = prepared.get(sym)
            if sym_prep is None:
                reasons["data_missing"] += 1
                missed_list.append({"symbol": sym, "gain_pct": round(gain_pct, 2),
                                    "reason": "data_missing", "ts": ""})
                continue

            # Revisar ventana [-PRE_PUMP_WINDOW .. 0] antes del pump
            caught_this = False
            last_reason = "data_missing"
            for offset in range(-PRE_PUMP_WINDOW, 1):
                bar = pump_t + offset
                if bar < 0:
                    continue
                tf_data = get_tf_data_at(sym_prep, bar, cfg)
                if tf_data is None:
                    last_reason = "data_missing"
                    continue
                alert, reason = classify_explain(sym, tf_data, cfg)
                if alert is not None:
                    caught_this = True
                    break
                last_reason = reason or "unknown"

            df_15m = sym_prep["15m"]
            ts_ms_pump = int(df_15m["close_time"].iat[pump_t])
            ts_iso = datetime.fromtimestamp(ts_ms_pump / 1000, tz=timezone.utc).isoformat()

            if caught_this:
                caught.append((sym, pump_t, gain_pct))
            else:
                reasons[last_reason] += 1
                missed_list.append({"symbol": sym, "ts": ts_iso,
                                    "gain_pct": round(gain_pct, 2), "reason": last_reason})

        total = len(events)
        catch_rate = len(caught) * 100 / total if total else 0.0
        missed_list.sort(key=lambda x: x["gain_pct"], reverse=True)

        results[label] = {
            "total_movers":  total,
            "caught":        len(caught),
            "missed":        len(missed_list),
            "catch_rate":    round(catch_rate, 1),
            "miss_reasons":  dict(reasons),
            "top_misses":    missed_list[:20],
        }

    # ── Reporte stdout ───────────────────────────────────────────────────────
    print()
    print("═" * 70)
    print(" RESULTADO — CATCH RATE")
    print("═" * 70)
    for label, _, _ in MOVER_THRESHOLDS:
        r = results[label]
        print(f"\n  [{label}]")
        print(f"    Total movers  : {r['total_movers']}")
        print(f"    Pescados      : {r['caught']}  ({r['catch_rate']:.1f}%)")
        print(f"    Perdidos      : {r['missed']}")
        if r["miss_reasons"]:
            total_miss = sum(r["miss_reasons"].values())
            print(f"    Causas de miss:")
            for reason, n in sorted(r["miss_reasons"].items(), key=lambda x: -x[1]):
                pct = n * 100 / total_miss if total_miss else 0
                print(f"      {reason:<30} {n:>4}  ({pct:.0f}%)")
        if r["top_misses"]:
            print(f"\n    Top 10 misses más grandes:")
            print(f"      {'#':>3} {'Symbol':<14} {'Gain':>8}  {'Razón':<28} Timestamp")
            for i, m in enumerate(r["top_misses"][:10], 1):
                ts = m.get("ts", "")[:16]
                print(f"      {i:>3} {m['symbol']:<14} {m['gain_pct']:>+7.1f}%  "
                      f"{m['reason']:<28} {ts}")

    # ── Exportar JSON ────────────────────────────────────────────────────────
    ts_str = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M")
    out_path = AUDIT_RESULTS / f"catch_rate_{ts_str}.json"
    out = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "period_days":  args.days,
        "thresholds":   results,
    }
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(out, f, ensure_ascii=False, indent=2)
    print(f"\n  Resultado guardado en: {out_path}")
    print(f"  Post-mortem de un miss: python audit_misses.py post-mortem SYMBOL TIMESTAMP")


# ── Subcomando: post-mortem ───────────────────────────────────────────────────

def cmd_post_mortem(args, cfg):
    symbol = args.symbol.upper()

    # Parsear timestamp (ISO o ms)
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
    print(f"\n=== POST-MORTEM: {symbol} @ {ts_human} ===")

    end_dt   = datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc) + timedelta(hours=6)
    start_dt = end_dt - timedelta(days=10)

    print(f"\nDescargando klines para {symbol}...")
    klines = download_all_klines([symbol], start_dt, end_dt)
    if symbol not in klines:
        sys.exit(f"FATAL: sin datos para {symbol}")

    sym_prep = {}
    for tf in ("5m", "15m", "1h"):
        df = klines[symbol].get(tf)
        if df is not None:
            sym_prep[tf] = precompute_indicators(df, cfg)

    df_15m = sym_prep.get("15m")
    if df_15m is None:
        sys.exit("FATAL: sin datos 15m")

    bar_idx = find_idx_at_or_before(df_15m, ts_ms)
    if bar_idx < 80:
        sys.exit(f"FATAL: bar_idx={bar_idx} insuficiente (necesita ≥ 80 barras)")

    bar_ts = int(df_15m["close_time"].iat[bar_idx])
    bar_human = datetime.fromtimestamp(bar_ts / 1000, tz=timezone.utc).isoformat()
    print(f"  Bar 15m: idx={bar_idx}, close_time={bar_human}")

    tf_data = get_tf_data_at(sym_prep, bar_idx, cfg)
    if tf_data is None:
        sys.exit("FATAL: datos insuficientes para analizar (necesita 5m y 1h también)")

    # ── Features crudas ──────────────────────────────────────────────────────
    print("\n─── FEATURES POR TIMEFRAME ─────────────────────────────────────")
    FIELDS = [
        "price", "ema_trend_up", "atr_pct", "vol_ratio", "vol_growth",
        "strong_close", "candle_body_pct", "width_curr", "width_expansion",
        "recent_max", "near_recent_max", "breakout", "breakout_distance",
        "recent_long_ok", "obv_slope", "obv_rising", "cvd_ratio", "cvd_bullish",
        "not_near_resistance", "dist_to_res", "major_struct_ok", "major_struct_dist",
        "hold_recent_break", "hold_kept_zone", "hold_pullback_ok", "hold_strong",
        "bars_since_break", "riding_gain", "riding_above_zone", "riding_vol_ok",
        "riding_bars_since",
    ]
    for tf_name in ("5m", "15m", "1h"):
        d = tf_data[tf_name]
        print(f"\n  [{tf_name}]")
        for field in FIELDS:
            val = d.get(field)
            if val is None:
                continue
            if isinstance(val, float):
                print(f"    {field:<32} {val:>+.4f}")
            else:
                print(f"    {field:<32} {val}")

    # ── Pre-gates ────────────────────────────────────────────────────────────
    print("\n─── PRE-GATES (1h) ─────────────────────────────────────────────")
    tf1h = tf_data["1h"]
    atr_min = cfg.g("indicators", "ATR_MIN_PCT")
    gates = [
        ("ema_trend_up",          tf1h.get("ema_trend_up")),
        ("not_near_resistance",   tf1h.get("not_near_resistance")),
        (f"atr_pct >= {atr_min}", tf1h.get("atr_pct", 0) >= atr_min),
        ("major_struct_ok",       tf1h.get("major_struct_ok", True)),
    ]
    all_pass = True
    for name, val in gates:
        mark = "✓" if val else "✗"
        print(f"  {mark} {name:<40} {val}")
        if not val:
            all_pass = False

    # ── Clasificación ────────────────────────────────────────────────────────
    print("\n─── CLASIFICACIÓN ──────────────────────────────────────────────")
    alert, reason = classify_explain(symbol, tf_data, cfg)
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
    """Imprime qué condición le faltó a cada señal para disparar."""
    tf5  = tf_data["5m"]
    tf15 = tf_data["15m"]

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
            print(f"    (todos los checks pasan — debería haber disparado?)")

    # PREBREAK
    show_checks("PREBREAK", {
        "near_recent_max":
            bool(tf5.get("near_recent_max")),
        f"BB_width <= {cfg.g('prebreak','PREBREAK_BB_WIDTH_MAX')} "
        f"(actual {tf5.get('width_curr',0):.4f})":
            tf5.get("width_curr", 9) <= cfg.g("prebreak", "PREBREAK_BB_WIDTH_MAX"),
        f"vol_ratio >= {cfg.g('prebreak','PREBREAK_MIN_VOL_RATIO')} "
        f"(actual {tf5.get('vol_ratio',0):.2f})":
            tf5.get("vol_ratio", 0) >= cfg.g("prebreak", "PREBREAK_MIN_VOL_RATIO"),
        f"vol_growth >= {cfg.g('prebreak','PREBREAK_VOLUME_GROWTH_MIN')} "
        f"(actual {tf5.get('vol_growth',0):.2f})":
            tf5.get("vol_growth", 0) >= cfg.g("prebreak", "PREBREAK_VOLUME_GROWTH_MIN"),
    })

    # BREAKOUT
    require_obv = cfg.g("breakout", "BREAKOUT_REQUIRE_OBV_NON_NEGATIVE", default=True)
    show_checks("BREAKOUT", {
        f"breakout (price > recent_max × {1+cfg.g('breakout','BREAKOUT_BUFFER'):.3f})":
            bool(tf15.get("breakout")),
        f"vol_ratio >= {cfg.g('breakout','BREAKOUT_MIN_VOL_RATIO')} "
        f"(actual {tf15.get('vol_ratio',0):.2f})":
            tf15.get("vol_ratio", 0) >= cfg.g("breakout", "BREAKOUT_MIN_VOL_RATIO"),
        f"distance <= {cfg.g('breakout','BREAKOUT_MAX_EXTENDED')} "
        f"(actual {tf15.get('breakout_distance',0):.4f})":
            tf15.get("breakout_distance", 9) <= cfg.g("breakout", "BREAKOUT_MAX_EXTENDED"),
        f"BB_exp >= {cfg.g('breakout','BREAKOUT_BB_EXPANSION_MIN')} "
        f"(actual {tf15.get('width_expansion',0):.4f})":
            tf15.get("width_expansion", -9) >= cfg.g("breakout", "BREAKOUT_BB_EXPANSION_MIN"),
        f"strong_close 15m (actual pos={tf15.get('_close_pos',0):.2f})":
            bool(tf15.get("strong_close")),
        f"body >= {cfg.g('breakout','BREAKOUT_MIN_BODY_PCT')} "
        f"(actual {tf15.get('candle_body_pct',0):.2f})":
            tf15.get("candle_body_pct", 0) >= cfg.g("breakout", "BREAKOUT_MIN_BODY_PCT"),
        f"5m vol >= {cfg.g('breakout','BREAKOUT_5M_MIN_VOL_RATIO')} "
        f"(actual {tf5.get('vol_ratio',0):.2f})":
            tf5.get("vol_ratio", 0) >= cfg.g("breakout", "BREAKOUT_5M_MIN_VOL_RATIO"),
        f"5m strong_close":
            bool(tf5.get("strong_close")),
        "obv_slope >= 0":
            (not require_obv) or tf15.get("obv_slope", 0) >= 0,
    })

    # RIDING
    riding_gain = tf15.get("riding_gain") or 0.0
    min_g = cfg.g("riding", "RIDING_MIN_GAIN")
    max_g = cfg.g("riding", "RIDING_MAX_GAIN")
    show_checks("RIDING", {
        "riding_above_zone":
            bool(tf15.get("riding_above_zone")),
        "riding_vol_ok":
            bool(tf15.get("riding_vol_ok")),
        f"gain in [{min_g:.3f}, {max_g:.3f}] (actual {riding_gain:.4f})":
            min_g <= riding_gain <= max_g,
        f"riding_bars_since >= 1 (actual {tf15.get('riding_bars_since')})":
            tf15.get("riding_bars_since") is not None
            and tf15["riding_bars_since"] >= 1,
        "not in active breakout":
            not bool(tf15.get("breakout")),
    })

    # HOLD
    show_checks("HOLD", {
        "hold_recent_break":  bool(tf15.get("hold_recent_break")),
        "hold_kept_zone":     bool(tf15.get("hold_kept_zone")),
        "hold_pullback_ok":   bool(tf15.get("hold_pullback_ok")),
        "hold_strong":        bool(tf15.get("hold_strong")),
    })


# ── Subcomando: false-positives ───────────────────────────────────────────────

def cmd_false_positives(args, cfg):
    dump_path = Path(args.dump)
    if not dump_path.exists():
        sys.exit(
            f"FATAL: {dump_path} no encontrado.\n"
            f"  Primero corrí:  python dump_outcomes.py\n"
            f"  (requiere SUPABASE_KEY en el entorno)"
        )

    with open(dump_path, "r", encoding="utf-8") as f:
        rows = json.load(f)

    cutoff = datetime.now(timezone.utc) - timedelta(days=args.days)
    rows = [
        r for r in rows
        if r.get("outcomes_complete") and r.get("alerted_at")
        and datetime.fromisoformat(r["alerted_at"].replace("Z", "+00:00")) >= cutoff
    ]

    if not rows:
        print(f"  Sin outcomes completos en los últimos {args.days} días.")
        return

    print(f"\n=== FALSE POSITIVES — últimos {args.days} días ===")
    print(f"  {len(rows)} alertas con outcome completo")

    def pct(row, field):
        ep = row.get("entry_price")
        fp = row.get(field)
        if ep and fp and float(ep) > 0:
            return (float(fp) / float(ep) - 1) * 100
        return None

    # ── Por signal_type × bucket ─────────────────────────────────────────────
    print("\n─── Por signal_type × bucket ──────────────────────────────────")
    groups = defaultdict(list)
    for r in rows:
        groups[(r.get("signal_type", "?"), r.get("bucket", "?"))].append(r)

    print(f"  {'Tipo':<12} {'Bucket':<8} {'N':>4}  "
          f"{'max24h avg':>10}  {'dd avg':>8}  {'no-move <2%':>11}")
    for (st, bk), items in sorted(groups.items()):
        maxs = [m for m in (pct(r, "max_high_24h") for r in items) if m is not None]
        dds  = [m for m in (pct(r, "min_low_24h")  for r in items) if m is not None]
        no_move = sum(1 for m in maxs if m < 2.0)
        avg_max  = sum(maxs) / len(maxs) if maxs else 0
        avg_dd   = sum(dds)  / len(dds)  if dds  else 0
        no_pct   = no_move * 100 / len(maxs) if maxs else 0
        print(f"  {st:<12} {bk:<8} {len(items):>4}  "
              f"{avg_max:>+9.2f}%  {avg_dd:>+7.2f}%  {no_pct:>10.0f}%")

    # ── Top 20 peores ────────────────────────────────────────────────────────
    print("\n─── Top 20 peores (drawdown sin upside) ───────────────────────")
    worst = []
    for r in rows:
        mx = pct(r, "max_high_24h")
        dd = pct(r, "min_low_24h")
        if mx is not None and dd is not None:
            badness = -dd - max(0.0, mx - 2.0)
            worst.append((badness, mx, dd, r))
    worst.sort(key=lambda x: x[0], reverse=True)

    print(f"  {'#':>3} {'Symbol':<14} {'Type':<10} {'Score':>5} "
          f"{'max24h':>8} {'dd':>8}  alerted_at")
    for i, (_, mx, dd, r) in enumerate(worst[:20], 1):
        alerted = (r.get("alerted_at") or "")[:16]
        print(f"  {i:>3} {r.get('symbol','?'):<14} {r.get('signal_type','?'):<10} "
              f"{r.get('score',0):>5} {mx:>+7.2f}% {dd:>+7.2f}%  {alerted}")

    # ── OBV/CVD análisis ─────────────────────────────────────────────────────
    print("\n─── OBV / CVD: buenos (≥5%) vs malos (<2%) ────────────────────")
    good = [r for r in rows if (pct(r, "max_high_24h") or 0)    >= 5.0]
    bad  = [r for r in rows if (pct(r, "max_high_24h") or 100.0) < 2.0]
    print(f"  Buenos (max_24h ≥ 5%): {len(good)}    Malos (max_24h < 2%): {len(bad)}")
    for label, subset in [("Buenos", good), ("Malos", bad)]:
        obvs = [float(r["obv_slope"]) for r in subset if r.get("obv_slope") is not None]
        cvds = [float(r["cvd_ratio"]) for r in subset if r.get("cvd_ratio") is not None]
        if obvs:
            obv_avg = sum(obvs) / len(obvs)
            cvd_avg = sum(cvds) / len(cvds) if cvds else float("nan")
            print(f"  {label:<8} — OBV slope avg: {obv_avg:+.3f}  CVD ratio avg: {cvd_avg:+.3f}  (n={len(obvs)})")


# ── Subcomando: lateness ──────────────────────────────────────────────────────

def cmd_lateness(args, cfg):
    """Mide cuánto tarde entran las señales BREAKOUT respecto al nivel de ruptura.
    Para cada alerta usa entry_price y ref_price (guardados en outcomes) para calcular
    lateness_pct = (entry_price / ref_price - 1) - BREAKOUT_BUFFER.
    Si lateness_pct > 2% la señal llegó tarde (chasing). Si < 0.5% fue temprana."""
    dump_path = Path(args.dump)
    if not dump_path.exists():
        sys.exit(
            f"FATAL: {dump_path} no encontrado.\n"
            f"  Primero corrí:  python dump_outcomes.py"
        )

    with open(dump_path, "r", encoding="utf-8") as f:
        rows = json.load(f)

    cutoff = datetime.now(timezone.utc) - timedelta(days=args.days)
    bo_rows = [
        r for r in rows
        if r.get("signal_type") == "BREAKOUT"
        and r.get("entry_price") and r.get("ref_price")
        and r.get("alerted_at")
        and datetime.fromisoformat(r["alerted_at"].replace("Z", "+00:00")) >= cutoff
    ]

    if not bo_rows:
        print(f"  Sin BREAKOUTs con entry_price+ref_price completos en los últimos {args.days} días.")
        return

    buffer = cfg.g("breakout", "BREAKOUT_BUFFER", default=0.003)

    entries = []
    for r in bo_rows:
        ep = float(r["entry_price"])
        rp = float(r["ref_price"])
        if rp > 0:
            lateness = (ep / rp - 1) - buffer
            entries.append({
                "symbol":      r.get("symbol", "?"),
                "alerted_at":  (r.get("alerted_at") or "")[:16],
                "bucket":      r.get("bucket", "?"),
                "score":       r.get("score", 0),
                "entry_price": ep,
                "ref_price":   rp,
                "lateness_pct": lateness * 100,
            })

    if not entries:
        return

    vals = sorted(x["lateness_pct"] for x in entries)
    n    = len(vals)
    med  = vals[n // 2]
    p75  = vals[min(int(n * 0.75), n - 1)]
    p90  = vals[min(int(n * 0.90), n - 1)]
    early = sum(1 for v in vals if v <= 0.5)
    late  = sum(1 for v in vals if v > 2.0)

    print(f"\n=== LATENESS — BREAKOUT últimos {args.days} días ===")
    print(f"  N={n}   mediana={med:+.2f}%   p75={p75:+.2f}%   p90={p90:+.2f}%")
    print(f"  Tempranas (<=0.5% sobre nivel): {early:>4}  ({early*100/n:.0f}%)")
    print(f"  Tardías   (> 2%  sobre nivel):  {late:>4}  ({late*100/n:.0f}%)")
    print(f"\n  Interpretación:")
    print(f"    mediana > 2%  → chasing estructural (bajar gates de confirmación)")
    print(f"    mediana < 1%  → entrada ajustada    (no urge cambiar)")

    # Por bucket
    print("\n─── Por bucket ──────────────────────────────────────────────")
    by_bucket = defaultdict(list)
    for x in entries:
        by_bucket[x["bucket"]].append(x["lateness_pct"])
    for bk in ("BEST", "STRONG", "WATCH"):
        vs = sorted(by_bucket.get(bk, []))
        if vs:
            med_bk = vs[len(vs) // 2]
            late_bk = sum(1 for v in vs if v > 2.0)
            print(f"  {bk:<6}  n={len(vs):>3}  mediana={med_bk:+.2f}%  tardías={late_bk} ({late_bk*100/len(vs):.0f}%)")

    # Top 15 más tardías
    entries.sort(key=lambda x: x["lateness_pct"], reverse=True)
    print("\n─── Top 15 más tardías ──────────────────────────────────────")
    print(f"  {'#':>3} {'Symbol':<14} {'Bucket':<8} {'Score':>5} {'Lateness':>10}  alerted_at")
    for i, x in enumerate(entries[:15], 1):
        print(f"  {i:>3} {x['symbol']:<14} {x['bucket']:<8} {x['score']:>5} "
              f"{x['lateness_pct']:>+9.2f}%  {x['alerted_at']}")

    # Exportar JSON
    ts_str   = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M")
    out_path = AUDIT_RESULTS / f"lateness_{ts_str}.json"
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump({
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "period_days":  args.days,
            "n":            n,
            "median_pct":   round(med, 3),
            "p75_pct":      round(p75, 3),
            "p90_pct":      round(p90, 3),
            "early_count":  early,
            "late_count":   late,
            "entries":      entries,
        }, f, ensure_ascii=False, indent=2)
    print(f"\n  Resultado guardado en: {out_path}")


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Auditoría de cobertura del screener")
    sub = parser.add_subparsers(dest="cmd", required=True)

    p_cr = sub.add_parser("catch-rate", help="Catch-rate real vs universo de movers")
    p_cr.add_argument("--days",      type=int, default=7,   help="Días a analizar (default 7)")
    p_cr.add_argument("--max-pairs", type=int, default=400, help="Máximo de pares (default 400)")

    p_pm = sub.add_parser("post-mortem", help="Replay analyze+classify para símbolo+timestamp")
    p_pm.add_argument("symbol",    help="Símbolo ej. BTCUSDT")
    p_pm.add_argument("timestamp", help="ISO 8601 ej. 2025-01-15T14:30:00  o  ms Unix")

    p_fp = sub.add_parser("false-positives", help="Alertas de outcomes que no se cumplieron")
    p_fp.add_argument("--dump", default="outcomes_dump.json",
                      help="Path a outcomes_dump.json (default: outcomes_dump.json)")
    p_fp.add_argument("--days", type=int, default=7, help="Últimos N días (default 7)")

    p_la = sub.add_parser("lateness", help="Cuánto tarde entran los BREAKOUTs (chasing)")
    p_la.add_argument("--dump", default="outcomes_dump.json",
                      help="Path a outcomes_dump.json (default: outcomes_dump.json)")
    p_la.add_argument("--days", type=int, default=30, help="Últimos N días (default 30)")

    parser.add_argument(
        "--config", default=str(CONFIG_PATH),
        help="Path a config.json a usar (default: config.json). Permite A/B entre variantes.",
    )

    args = parser.parse_args()
    cfg  = Config(Path(args.config))

    if args.cmd == "catch-rate":
        cmd_catch_rate(args, cfg)
    elif args.cmd == "post-mortem":
        cmd_post_mortem(args, cfg)
    elif args.cmd == "false-positives":
        cmd_false_positives(args, cfg)
    elif args.cmd == "lateness":
        cmd_lateness(args, cfg)


if __name__ == "__main__":
    main()
