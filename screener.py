"""
Binance Spot USDT Crypto Screener — TODOS los pares, paralelo, MULTI-TIMEFRAME
Indicadores activos: BB width expansion + volume spike + price up (combo)
Indicadores comentados: RSI, MACD, EMA crossover, BB breakout, BB squeeze, Volumen spike standalone
Alertas vía Telegram
"""

import os
import requests
import pandas as pd
import ta
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

# ── Configuración ──────────────────────────────────────────────────────────────
TELEGRAM_TOKEN   = os.environ["TELEGRAM_TOKEN"]
TELEGRAM_CHAT_ID = os.environ["TELEGRAM_CHAT_ID"]

INTERVALS    = ["1m", "15m", "1h"]
LIMIT        = 100
TOP_N        = 9999
MAX_WORKERS  = 20

# ── Filtro de liquidez ────────────────────────────────────────────────────────
MIN_QUOTE_VOLUME = 100000

# ── BB Squeeze ────────────────────────────────────────────────────────────────
BB_WIDTH_MIN     = 0.02

# ── BB Width Expansion + Volume + Price Up (combo) ───────────────────────────
BB_EXPANSION_MIN = 0.095
BB_EXPANSION_PCT = 0.03
BB_WIDTH_MAX     = 5.0
EXP_VOL_NORMAL   = 2.0
EXP_VOL_FUERTE   = 5.0
EXP_VOL_EXTREMO  = 10.0

# ── RSI ───────────────────────────────────────────────────────────────────────
RSI_OVERSOLD     = 30
RSI_OVERBOUGHT   = 70
RSI_WINDOW       = 14

# ── MACD ─────────────────────────────────────────────────────────────────────
MACD_FAST        = 12
MACD_SLOW        = 26
MACD_SIGNAL      = 9

# ── EMA Crossover ─────────────────────────────────────────────────────────────
EMA_FAST         = 9
EMA_SLOW         = 21

# ── Vol Spike standalone ──────────────────────────────────────────────────────
VOL_NORMAL       = 3
VOL_FUERTE       = 5.0
VOL_EXTREMO      = 10.0


# ── Datos ─────────────────────────────────────────────────────────────────────
def get_active_usdt_symbols():
    """Devuelve el conjunto de símbolos USDT con status TRADING en Binance."""
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
        timeout=10
    )
    r.raise_for_status()
    df = pd.DataFrame(r.json(), columns=[
        "open_time", "open", "high", "low", "close", "volume",
        "close_time", "quote_vol", "trades", "taker_buy_base", "taker_buy_quote", "ignore"
    ])
    for col in ["open", "high", "low", "close", "volume"]:
        df[col] = df[col].astype(float)
    return df


# ── Análisis ──────────────────────────────────────────────────────────────────
def analyze(symbol, interval):
    try:
        df = get_klines(symbol, interval)
    except Exception:
        return symbol, interval, None, 0

    if len(df) < 21:
        return symbol, interval, None, 0

    signals = []
    close  = df["close"]
    open_  = df["open"]
    volume = df["volume"]

    # ── RSI ───────────────────────────────────────────────────────────────────
    # rsi_val = ta.momentum.RSIIndicator(close, window=RSI_WINDOW).rsi().iloc[-1]
    # if rsi_val <= RSI_OVERSOLD:
    #     signals.append(f"📉 RSI={rsi_val:.1f} (sobreventa)")
    # elif rsi_val >= RSI_OVERBOUGHT:
    #     signals.append(f"📈 RSI={rsi_val:.1f} (sobrecompra)")

    # ── MACD crossover ────────────────────────────────────────────────────────
    # macd_ind  = ta.trend.MACD(close, window_slow=MACD_SLOW, window_fast=MACD_FAST, window_sign=MACD_SIGNAL)
    # macd_line = macd_ind.macd()
    # sig_line  = macd_ind.macd_signal()
    # if macd_line.iloc[-2] < sig_line.iloc[-2] and macd_line.iloc[-1] > sig_line.iloc[-1]:
    #     signals.append("⚡ MACD crossover alcista")
    # elif macd_line.iloc[-2] > sig_line.iloc[-2] and macd_line.iloc[-1] < sig_line.iloc[-1]:
    #     signals.append("⚡ MACD crossover bajista")

    # ── EMA crossover ─────────────────────────────────────────────────────────
    # ema_fast = ta.trend.EMAIndicator(close, window=EMA_FAST).ema_indicator()
    # ema_slow = ta.trend.EMAIndicator(close, window=EMA_SLOW).ema_indicator()
    # if ema_fast.iloc[-2] < ema_slow.iloc[-2] and ema_fast.iloc[-1] > ema_slow.iloc[-1]:
    #     signals.append(f"🔀 EMA{EMA_FAST} cruzó arriba EMA{EMA_SLOW} (alcista)")
    # elif ema_fast.iloc[-2] > ema_slow.iloc[-2] and ema_fast.iloc[-1] < ema_slow.iloc[-1]:
    #     signals.append(f"🔀 EMA{EMA_FAST} cruzó abajo EMA{EMA_SLOW} (bajista)")

    # ── Bollinger Bands ───────────────────────────────────────────────────────
    bb     = ta.volatility.BollingerBands(close, window=20, window_dev=2)
    hband  = bb.bollinger_hband()
    lband  = bb.bollinger_lband()
    mavg   = bb.bollinger_mavg()

    mid_curr = mavg.iloc[-1]
    mid_prev = mavg.iloc[-2]
    price    = close.iloc[-1]

    width_curr = (hband.iloc[-1] - lband.iloc[-1]) / mid_curr if mid_curr != 0 else 0
    width_prev = (hband.iloc[-2] - lband.iloc[-2]) / mid_prev if mid_prev != 0 else 0

    # ── Volumen base (para combo y standalone) ───────────────────────────────
    vol_mean  = volume.iloc[-21:-1].mean()
    vol_curr  = volume.iloc[-1]
    vol_ratio = vol_curr / vol_mean if vol_mean > 0 else 0

    # ── BB breakout ───────────────────────────────────────────────────────────
    # if price > hband.iloc[-1]:
    #     signals.append(f"🔥 BB breakout arriba (close={price:.4f} > upper={hband.iloc[-1]:.4f})")
    # elif price < lband.iloc[-1]:
    #     signals.append(f"🔥 BB breakout abajo (close={price:.4f} < lower={lband.iloc[-1]:.4f})")

    # ── BB squeeze ────────────────────────────────────────────────────────────
    # if width_curr <= BB_WIDTH_MIN:
    #     signals.append(f"🤏 BB squeeze (width={width_curr:.2%}) — movimiento fuerte próximo")

    # ── BB Width Expansion + Volume Spike + Price Up (combo) ✅ ACTIVO ───────
    price_up      = close.iloc[-1] > open_.iloc[-1]
    width_delta   = width_curr - width_prev
    width_pct_chg = width_delta / width_prev if width_prev > 0 else 0
    expansion_ok  = width_delta >= BB_EXPANSION_MIN or width_pct_chg >= BB_EXPANSION_PCT

    if vol_ratio >= EXP_VOL_EXTREMO:
        exp_vol_label = "🔴 vol extremo"
    elif vol_ratio >= EXP_VOL_FUERTE:
        exp_vol_label = "🟡 vol fuerte"
    elif vol_ratio >= EXP_VOL_NORMAL:
        exp_vol_label = "🟢 vol normal"
    else:
        exp_vol_label = None

    if expansion_ok and exp_vol_label and price_up and width_curr < BB_WIDTH_MAX:
        signals.append(
            f"{exp_vol_label} {vol_ratio:.1f}x | BB expansion "
            f"{width_prev:.2%} → {width_curr:.2%} (+{width_pct_chg:.0%})"
        )

    # ── Vol Spike standalone (sin requerir BB expansion ni precio) ───────────
    # if vol_ratio >= VOL_EXTREMO:
    #     signals.append(f"🔴 vol extremo standalone {vol_ratio:.1f}x promedio")
    # elif vol_ratio >= VOL_FUERTE:
    #     signals.append(f"🟡 vol fuerte standalone {vol_ratio:.1f}x promedio")
    # elif vol_ratio >= VOL_NORMAL:
    #     signals.append(f"🟢 vol normal standalone {vol_ratio:.1f}x promedio")

    return symbol, interval, (signals if signals else None), vol_ratio


# ── Telegram ──────────────────────────────────────────────────────────────────
def send_telegram(text):
    requests.post(
        f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage",
        json={"chat_id": TELEGRAM_CHAT_ID, "text": text, "parse_mode": "Markdown", "disable_web_page_preview": True},
        timeout=10
    ).raise_for_status()


def binance_link(symbol):
    """Convierte DASHUSDT → enlace Markdown a Binance spot."""
    pair = symbol[:-4] + "_USDT"  # DASHUSDT → DASH_USDT
    url  = f"https://www.binance.com/en/trade/{pair}?type=spot"
    return f"[🔗]({url})"


# ── Ordenar timeframes de menor a mayor (siempre) ────────────────────────────
_TF_ORDER = {"1m": 0, "3m": 1, "5m": 2, "15m": 3, "30m": 4, "1h": 5, "2h": 6, "4h": 7, "6h": 8, "8h": 9, "12h": 10, "1d": 11, "3d": 12, "1w": 13}

def sort_intervals(intervals):
    return sorted(intervals, key=lambda tf: _TF_ORDER.get(tf, 99))


# ── Main ──────────────────────────────────────────────────────────────────────
def main():
    now = datetime.utcnow().strftime("%Y-%m-%d %H:%M UTC")
    pairs = get_all_usdt_pairs()

    # Ordenar siempre de menor a mayor timeframe
    intervals_sorted = sort_intervals(INTERVALS)
    tf_label = " + ".join(intervals_sorted)
    print(f"[{now}] Escaneando {len(pairs)} pares USDT ({tf_label}) con {MAX_WORKERS} workers...")

    # Construir todas las tareas: (símbolo, timeframe)
    tasks = [(sym, tf) for sym in pairs for tf in intervals_sorted]

    # Agrupar resultados por timeframe → símbolo
    results = {tf: {} for tf in intervals_sorted}

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(analyze, sym, tf): (sym, tf) for sym, tf in tasks}
        for future in as_completed(futures):
            symbol, interval, sigs, vol_ratio = future.result()
            results[interval][symbol] = (sigs, vol_ratio)
            if sigs:
                print(f"  ✅ {symbol} [{interval}] vol={vol_ratio:.1f}x: {sigs}")

    # Contar señales totales por TF para el header
    signals_by_tf = {}
    for tf in intervals_sorted:
        raw = [(sym, results[tf][sym]) for sym in pairs if results[tf].get(sym) and results[tf][sym][0]]
        # Ordenar de mayor a menor vol_ratio
        raw.sort(key=lambda x: x[1][1], reverse=True)
        signals_by_tf[tf] = [(sym, sigs) for sym, (sigs, _) in raw]

    total_signals = sum(len(v) for v in signals_by_tf.values())

    if total_signals == 0:
        print("Sin señales en este scan. No se envía nada a Telegram.")
        return

    # ── Mensaje separador de inicio de run ───────────────────────────────────
    bar = "━" * 24
    tf_counts = "  ".join(f"{tf}: {len(signals_by_tf[tf])}" for tf in intervals_sorted)
    run_header = (
        f"{bar}\n"
        f"🟣  NUEVO SCAN  \u2022  {now}\n"
        f"     {tf_label}  \u2022  {len(pairs)} pares\n"
        f"     {tf_counts}\n"
        f"{bar}"
    )
    send_telegram(run_header)

    # ── Enviar resultados agrupados por timeframe, de menor a mayor ──────────
    for tf in intervals_sorted:
        all_signals = signals_by_tf[tf]

        if not all_signals:
            print(f"Sin señales en {tf}.")
            continue

        # Header de sección por TF
        tf_header = f"📊 [{tf}]  {len(all_signals)} señales\n{'─' * 20}\n\n"
        current = tf_header

        for symbol, sigs in all_signals:
            block = f"▶ {symbol} {binance_link(symbol)}\n" + "\n".join(f"  {s}" for s in sigs) + "\n\n"
            if len(current) + len(block) > 4000:
                send_telegram(current)
                current = block
            else:
                current += block

        if current.strip():
            send_telegram(current)

    print(f"\nTotal señales: {total_signals} / {len(pairs)} pares × {len(intervals_sorted)} timeframes ({tf_label})")
    print("✅ Mensajes enviados a Telegram.")


if __name__ == "__main__":
    main()
