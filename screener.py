"""
Binance Spot USDT Crypto Screener — TODOS los pares, paralelo
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

INTERVAL     = "1h"
LIMIT        = 100
TOP_N        = 9999
MAX_WORKERS  = 20

# ── BB Squeeze ────────────────────────────────────────────────────────────────
BB_WIDTH_MIN        = 0.1

# ── BB Width Expansion + Volume + Price (combo) ──────────────────────────────
BB_EXPANSION_MIN    = 0.095
BB_EXPANSION_PCT    = 0.03
VOLUME_MULT         = 2.0
# (price up se detecta automáticamente: close > open en la vela actual)

# ── Indicadores comentados ────────────────────────────────────────────────────
# RSI_OVERSOLD   = 30
# RSI_OVERBOUGHT = 70


# ── Datos ─────────────────────────────────────────────────────────────────────
def get_all_usdt_pairs(n=TOP_N):
    r = requests.get("https://data-api.binance.vision/api/v3/ticker/24hr", timeout=15)
    r.raise_for_status()
    pairs = [
        x for x in r.json()
        if x["symbol"].endswith("USDT")
        and x["symbol"].encode("ascii", errors="ignore").decode() == x["symbol"]
        and float(x["quoteVolume"]) > 0
    ]
    pairs.sort(key=lambda x: float(x["quoteVolume"]), reverse=True)
    return [x["symbol"] for x in pairs[:n]]


def get_klines(symbol):
    r = requests.get(
        "https://data-api.binance.vision/api/v3/klines",
        params={"symbol": symbol, "interval": INTERVAL, "limit": LIMIT},
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
def analyze(symbol):
    try:
        df = get_klines(symbol)
    except Exception:
        return symbol, None

    signals = []
    close  = df["close"]
    open_  = df["open"]
    volume = df["volume"]

    # ── RSI ───────────────────────────────────────────────────────────────────
    # rsi_val = ta.momentum.RSIIndicator(close, window=14).rsi().iloc[-1]
    # if rsi_val <= RSI_OVERSOLD:
    #     signals.append(f"📉 RSI={rsi_val:.1f} (sobreventa)")
    # elif rsi_val >= RSI_OVERBOUGHT:
    #     signals.append(f"📈 RSI={rsi_val:.1f} (sobrecompra)")

    # ── MACD crossover ────────────────────────────────────────────────────────
    # macd_ind  = ta.trend.MACD(close, window_slow=26, window_fast=12, window_sign=9)
    # macd_line = macd_ind.macd()
    # sig_line  = macd_ind.macd_signal()
    # if macd_line.iloc[-2] < sig_line.iloc[-2] and macd_line.iloc[-1] > sig_line.iloc[-1]:
    # signals.append("⚡ MACD crossover alcista")
    # elif macd_line.iloc[-2] > sig_line.iloc[-2] and macd_line.iloc[-1] < sig_line.iloc[-1]:
    # signals.append("⚡ MACD crossover bajista")

    # ── EMA 9/21 crossover ────────────────────────────────────────────────────
    # ema9  = ta.trend.EMAIndicator(close, window=9).ema_indicator()
    # ema21 = ta.trend.EMAIndicator(close, window=21).ema_indicator()
    # if ema9.iloc[-2] < ema21.iloc[-2] and ema9.iloc[-1] > ema21.iloc[-1]:
    # signals.append("🔀 EMA9 cruzó arriba EMA21 (alcista)")
    # elif ema9.iloc[-2] > ema21.iloc[-2] and ema9.iloc[-1] < ema21.iloc[-1]:
    # signals.append("🔀 EMA9 cruzó abajo EMA21 (bajista)")

    # ── Bollinger Bands ───────────────────────────────────────────────────────
    bb     = ta.volatility.BollingerBands(close, window=20, window_dev=2)
    hband  = bb.bollinger_hband()
    lband  = bb.bollinger_lband()
    mavg   = bb.bollinger_mavg()

    mid_curr  = mavg.iloc[-1]
    mid_prev  = mavg.iloc[-2]
    price     = close.iloc[-1]

    width_curr = (hband.iloc[-1] - lband.iloc[-1]) / mid_curr if mid_curr != 0 else 0
    width_prev = (hband.iloc[-2] - lband.iloc[-2]) / mid_prev if mid_prev != 0 else 0

    # ── BB breakout ───────────────────────────────────────────────────────────
    # if price > hband.iloc[-1]:
    #     signals.append(f"🔥 BB breakout arriba (close={price:.4f} > upper={hband.iloc[-1]:.4f})")
    # elif price < lband.iloc[-1]:
    #     signals.append(f"🔥 BB breakout abajo (close={price:.4f} < lower={lband.iloc[-1]:.4f})")

    # ── BB squeeze ──────────────────────────────────────────────────
    if width_curr <= BB_WIDTH_MIN:
        signals.append(f"🤏 BB squeeze (width={width_curr:.2%}) — movimiento fuerte próximo")

    # ── BB Width Expansion + Volume Spike + Price Up (combo) ✅ ACTIVO ───────
    width_delta    = width_curr - width_prev
    width_pct_chg  = width_delta / width_prev if width_prev > 0 else 0
    vol_mean       = volume.iloc[-21:-1].mean()
    vol_curr       = volume.iloc[-1]
    vol_spike      = vol_mean > 0 and vol_curr > vol_mean * VOLUME_MULT
    price_up       = close.iloc[-1] > open_.iloc[-1]

    expansion_ok   = width_delta >= BB_EXPANSION_MIN or width_pct_chg >= BB_EXPANSION_PCT

    if expansion_ok and vol_spike and price_up:
        signals.append(
            f"🚀 BB expansion + vol + precio sube\n"
            f"     width {width_prev:.2%} → {width_curr:.2%} "
            f"(+{width_pct_chg:.0%}) | vol {vol_curr/vol_mean:.1f}x"
        )

    #     signals.append(f"🚀 Volumen spike {vol_curr/vol_mean:.1f}x promedio")
    # if vol_mean > 0 and vol_curr > vol_mean * VOLUME_MULT:
    #     signals.append(f"🚀 Volumen spike {vol_curr/vol_mean:.1f}x promedio")

    return symbol, (signals if signals else None)


# ── Telegram ──────────────────────────────────────────────────────────────────
def send_telegram(text):
    requests.post(
        f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage",
        json={"chat_id": TELEGRAM_CHAT_ID, "text": text},
        timeout=10
    ).raise_for_status()


# ── Main ──────────────────────────────────────────────────────────────────────
def main():
    now = datetime.utcnow().strftime("%Y-%m-%d %H:%M UTC")
    pairs = get_all_usdt_pairs()
    print(f"[{now}] Escaneando {len(pairs)} pares USDT ({INTERVAL}) con {MAX_WORKERS} workers...")

    results = {}

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(analyze, sym): sym for sym in pairs}
        for future in as_completed(futures):
            symbol, sigs = future.result()
            results[symbol] = sigs
            if sigs:
                print(f"  ✅ {symbol}: {sigs}")

    all_signals = [(sym, results[sym]) for sym in pairs if results.get(sym)]
    print(f"\nTotal señales: {len(all_signals)} / {len(pairs)} pares")

    if not all_signals:
        print("Sin señales en este scan.")
        return

    header  = f"📡 Screener | {now}\n{len(all_signals)} señales en {len(pairs)} pares ({INTERVAL})\n\n"
    current = header

    for symbol, sigs in all_signals:
        block = f"▶ {symbol}\n" + "\n".join(f"  {s}" for s in sigs) + "\n\n"
        if len(current) + len(block) > 4000:
            send_telegram(current)
            current = block
        else:
            current += block

    if current.strip():
        send_telegram(current)

    print("✅ Mensajes enviados a Telegram.")


if __name__ == "__main__":
    main()
