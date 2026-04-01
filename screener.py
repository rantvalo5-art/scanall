"""
Binance Spot USDT Crypto Screener
Indicadores: RSI, MACD, EMA crossover, Bollinger Bands (breakout + width), Volumen spike
Alertas via Telegram
"""

import os
import requests
import pandas as pd
import ta
from datetime import datetime

# ── Configuración ──────────────────────────────────────────────────────────────
TELEGRAM_TOKEN   = os.environ["TELEGRAM_TOKEN"]
TELEGRAM_CHAT_ID = os.environ["TELEGRAM_CHAT_ID"]

INTERVAL      = "15m"   # Timeframe
LIMIT         = 100     # Velas a traer por símbolo
TOP_N         = 80      # Top pares USDT por volumen 24h

RSI_OVERSOLD   = 30
RSI_OVERBOUGHT = 70
BB_WIDTH_MIN   = 0.02   # Squeeze si width < 2%
VOLUME_MULT    = 2.5    # Spike si volumen > 2.5x promedio 20 velas


# ── Datos ──────────────────────────────────────────────────────────────────────
def get_top_usdt_pairs(n=TOP_N):
    r = requests.get("https://data-api.binance.vision/api/v3/ticker/24hr", timeout=10)
    r.raise_for_status()
    pairs = [x for x in r.json() if x["symbol"].endswith("USDT") and float(x["quoteVolume"]) > 0]
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
        return None

    signals = []
    close  = df["close"]
    volume = df["volume"]

    # RSI
    rsi = ta.momentum.RSIIndicator(close, window=14).rsi()
    rsi_val = rsi.iloc[-1]
    if rsi_val <= RSI_OVERSOLD:
        signals.append(f"📉 RSI={rsi_val:.1f} (sobreventa)")
    elif rsi_val >= RSI_OVERBOUGHT:
        signals.append(f"📈 RSI={rsi_val:.1f} (sobrecompra)")

    # MACD crossover
    macd_ind  = ta.trend.MACD(close, window_slow=26, window_fast=12, window_sign=9)
    macd_line = macd_ind.macd()
    sig_line  = macd_ind.macd_signal()
    if macd_line.iloc[-2] < sig_line.iloc[-2] and macd_line.iloc[-1] > sig_line.iloc[-1]:
        signals.append("⚡ MACD crossover alcista")
    elif macd_line.iloc[-2] > sig_line.iloc[-2] and macd_line.iloc[-1] < sig_line.iloc[-1]:
        signals.append("⚡ MACD crossover bajista")

    # EMA 9/21 crossover
    ema9  = ta.trend.EMAIndicator(close, window=9).ema_indicator()
    ema21 = ta.trend.EMAIndicator(close, window=21).ema_indicator()
    if ema9.iloc[-2] < ema21.iloc[-2] and ema9.iloc[-1] > ema21.iloc[-1]:
        signals.append("🔀 EMA9 cruzó arriba EMA21 (alcista)")
    elif ema9.iloc[-2] > ema21.iloc[-2] and ema9.iloc[-1] < ema21.iloc[-1]:
        signals.append("🔀 EMA9 cruzó abajo EMA21 (bajista)")

    # Bollinger Bands
    bb     = ta.volatility.BollingerBands(close, window=20, window_dev=2)
    upper  = bb.bollinger_hband().iloc[-1]
    lower  = bb.bollinger_lband().iloc[-1]
    mid    = bb.bollinger_mavg().iloc[-1]
    price  = close.iloc[-1]
    width  = (upper - lower) / mid if mid != 0 else 0

    if price > upper:
        signals.append(f"🔥 BB breakout arriba (close={price:.4f} > upper={upper:.4f})")
    elif price < lower:
        signals.append(f"🔥 BB breakout abajo (close={price:.4f} < lower={lower:.4f})")

    if width < BB_WIDTH_MIN:
        signals.append(f"🤏 BB squeeze (width={width:.2%}) — posible movimiento fuerte")

    # Volumen spike
    vol_mean = volume.iloc[-21:-1].mean()
    vol_curr = volume.iloc[-1]
    if vol_mean > 0 and vol_curr > vol_mean * VOLUME_MULT:
        signals.append(f"🚀 Volumen spike {vol_curr/vol_mean:.1f}x promedio")

    return signals if signals else None


# ── Telegram ───────────────────────────────────────────────────────────────────
def send_telegram(text):
    # Sin parse_mode HTML para evitar errores con caracteres especiales
    requests.post(
        f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage",
        json={"chat_id": TELEGRAM_CHAT_ID, "text": text},
        timeout=10
    ).raise_for_status()


def safe_symbol(symbol):
    # Filtrar solo ASCII — evita que pares con nombres chinos rompan el mensaje
    return symbol.encode("ascii", errors="ignore").decode()


# ── Main ───────────────────────────────────────────────────────────────────────
def main():
    now = datetime.utcnow().strftime("%Y-%m-%d %H:%M UTC")
    print(f"[{now}] Escaneando top {TOP_N} pares USDT ({INTERVAL})...")

    pairs       = get_top_usdt_pairs()
    all_signals = []

    for symbol in pairs:
        sigs = analyze(symbol)
        if sigs:
            all_signals.append((symbol, sigs))
            print(f"  ✅ {symbol}: {sigs}")
        else:
            print(f"  — {symbol}")

    if not all_signals:
        print("Sin señales en este scan.")
        return

    # Armar y enviar mensajes (máx 4096 chars por mensaje de Telegram)
    header  = f"🔍 Crypto Screener | {now}\n\n"
    current = header

    for symbol, sigs in all_signals:
        clean = safe_symbol(symbol)
        if not clean:
            continue
        block = f"▶ {clean}\n" + "\n".join(f"  {s}" for s in sigs) + "\n\n"
        if len(current) + len(block) > 4000:
            send_telegram(current)
            current = block
        else:
            current += block

    if current.strip():
        send_telegram(current)

    print(f"\n✅ {len(all_signals)} señales enviadas a Telegram.")


if __name__ == "__main__":
    main()
