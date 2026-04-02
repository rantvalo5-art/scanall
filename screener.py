"""
Binance Spot USDT Crypto Screener — TODOS los pares, paralelo
Indicadores activos: BB squeeze (width <= 10%)
Indicadores comentados: RSI, MACD, EMA crossover, BB breakout, Volumen spike
Alertas via Telegram
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

INTERVAL     = "1h"    # Timeframe
LIMIT        = 100     # Velas a traer por símbolo
TOP_N        = 9999    # 9999 = todos los pares USDT
MAX_WORKERS  = 20      # Requests en paralelo

# RSI_OVERSOLD   = 30
# RSI_OVERBOUGHT = 70
BB_WIDTH_MIN   = 0.02   # Squeeze si width <= 10%
# VOLUME_MULT    = 2.5


# ── Datos ──────────────────────────────────────────────────────────────────────
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
        "open_time","open","high","low","close","volume",
        "close_time","quote_vol","trades","taker_buy_base","taker_buy_quote","ignore"
    ])
    for col in ["open","high","low","close","volume"]:
        df[col] = df[col].astype(float)
    return df


# ── Análisis ───────────────────────────────────────────────────────────────────
def analyze(symbol):
    try:
        df = get_klines(symbol)
    except Exception:
        return symbol, None

    signals = []
    close  = df["close"]
    # volume = df["volume"]

    # ── RSI ──────────────────────────────────────────────────────────────────
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
    #     signals.append("⚡ MACD crossover alcista")
    # elif macd_line.iloc[-2] > sig_line.iloc[-2] and macd_line.iloc[-1] < sig_line.iloc[-1]:
    #     signals.append("⚡ MACD crossover bajista")

    # ── EMA 9/21 crossover ────────────────────────────────────────────────────
    # ema9  = ta.trend.EMAIndicator(close, window=9).ema_indicator()
    # ema21 = ta.trend.EMAIndicator(close, window=21).ema_indicator()
    # if ema9.iloc[-2] < ema21.iloc[-2] and ema9.iloc[-1] > ema21.iloc[-1]:
    #     signals.append("🔀 EMA9 cruzó arriba EMA21 (alcista)")
    # elif ema9.iloc[-2] > ema21.iloc[-2] and ema9.iloc[-1] < ema21.iloc[-1]:
    #     signals.append("🔀 EMA9 cruzó abajo EMA21 (bajista)")

    # ── Bollinger Bands ───────────────────────────────────────────────────────
    bb    = ta.volatility.BollingerBands(close, window=20, window_dev=2)
    upper = bb.bollinger_hband().iloc[-1]
    lower = bb.bollinger_lband().iloc[-1]
    mid   = bb.bollinger_mavg().iloc[-1]
    # price = close.iloc[-1]
    width = (upper - lower) / mid if mid != 0 else 0

    # BB breakout
    # if price > upper:
    #     signals.append(f"🔥 BB breakout arriba (close={price:.4f} > upper={upper:.4f})")
    # elif price < lower:
    #     signals.append(f"🔥 BB breakout abajo (close={price:.4f} < lower={lower:.4f})")

    # BB squeeze ✅ ACTIVO
    if width <= BB_WIDTH_MIN:
        signals.append(f"🤏 BB squeeze (width={width:.2%}) — movimiento fuerte próximo")

    # ── Volumen spike ─────────────────────────────────────────────────────────
    # vol_mean = volume.iloc[-21:-1].mean()
    # vol_curr = volume.iloc[-1]
    # if vol_mean > 0 and vol_curr > vol_mean * VOLUME_MULT:
    #     signals.append(f"🚀 Volumen spike {vol_curr/vol_mean:.1f}x promedio")

    return symbol, (signals if signals else None)


# ── Telegram ───────────────────────────────────────────────────────────────────
def send_telegram(text):
    requests.post(
        f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage",
        json={"chat_id": TELEGRAM_CHAT_ID, "text": text},
        timeout=10
    ).raise_for_status()


# ── Main ───────────────────────────────────────────────────────────────────────
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

    # Orden por volumen (top primero)
    all_signals = [(sym, results[sym]) for sym in pairs if results.get(sym)]

    print(f"\nTotal señales: {len(all_signals)} / {len(pairs)} pares")

    if not all_signals:
        print("Sin señales en este scan.")
        return

    header  = f"🤏 BB Squeeze | {now}\n{len(all_signals)} pares en squeeze ({INTERVAL})\n\n"
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

    print(f"✅ Mensajes enviados a Telegram.")


if __name__ == "__main__":
    main()
