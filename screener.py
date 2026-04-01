"""
Binance Spot USDT Crypto Screener
Indicadores: RSI, MACD, EMA crossover, Bollinger Bands (breakout + width), Volumen spike
Alertas via Telegram
"""

import os
import requests
import pandas as pd
import pandas_ta as ta
from datetime import datetime

# ── Configuración ──────────────────────────────────────────────────────────────
TELEGRAM_TOKEN = os.environ["TELEGRAM_TOKEN"]
TELEGRAM_CHAT_ID = os.environ["TELEGRAM_CHAT_ID"]

INTERVAL = "15m"          # Timeframe de las velas
LIMIT    = 100            # Cantidad de velas a traer por símbolo

# Thresholds
RSI_OVERSOLD    = 30
RSI_OVERBOUGHT  = 70
BB_WIDTH_MIN    = 0.02    # BB width mínima para considerar squeeze (< 2%)
VOLUME_MULT     = 2.5     # Spike si volumen actual > 2.5x promedio de últimas 20 velas

# Cuántos pares USDT del top querés scanear (por volumen 24h)
TOP_N = 80


# ── Funciones de datos ─────────────────────────────────────────────────────────
def get_top_usdt_pairs(n=TOP_N):
    url = "https://api.binance.com/api/v3/ticker/24hr"
    r = requests.get(url, timeout=10)
    r.raise_for_status()
    data = r.json()
    usdt = [x for x in data if x["symbol"].endswith("USDT") and float(x["quoteVolume"]) > 0]
    usdt.sort(key=lambda x: float(x["quoteVolume"]), reverse=True)
    return [x["symbol"] for x in usdt[:n]]


def get_klines(symbol, interval=INTERVAL, limit=LIMIT):
    url = "https://api.binance.com/api/v3/klines"
    params = {"symbol": symbol, "interval": interval, "limit": limit}
    r = requests.get(url, params=params, timeout=10)
    r.raise_for_status()
    raw = r.json()
    df = pd.DataFrame(raw, columns=[
        "open_time","open","high","low","close","volume",
        "close_time","quote_vol","trades","taker_buy_base",
        "taker_buy_quote","ignore"
    ])
    for col in ["open","high","low","close","volume"]:
        df[col] = df[col].astype(float)
    return df


# ── Análisis de indicadores ────────────────────────────────────────────────────
def analyze(symbol):
    try:
        df = get_klines(symbol)
    except Exception as e:
        return None

    signals = []

    # RSI
    rsi_series = ta.rsi(df["close"], length=14)
    rsi = rsi_series.iloc[-1]
    if rsi <= RSI_OVERSOLD:
        signals.append(f"📉 RSI={rsi:.1f} (sobreventa)")
    elif rsi >= RSI_OVERBOUGHT:
        signals.append(f"📈 RSI={rsi:.1f} (sobrecompra)")

    # MACD crossover
    macd_df = ta.macd(df["close"], fast=12, slow=26, signal=9)
    if macd_df is not None and len(macd_df.columns) >= 3:
        macd_col   = macd_df.columns[0]  # MACD_12_26_9
        signal_col = macd_df.columns[2]  # MACDs_12_26_9
        macd_prev   = macd_df[macd_col].iloc[-2]
        signal_prev = macd_df[signal_col].iloc[-2]
        macd_curr   = macd_df[macd_col].iloc[-1]
        signal_curr = macd_df[signal_col].iloc[-1]
        if macd_prev < signal_prev and macd_curr > signal_curr:
            signals.append("⚡ MACD crossover alcista")
        elif macd_prev > signal_prev and macd_curr < signal_curr:
            signals.append("⚡ MACD crossover bajista")

    # EMA crossover (9 / 21)
    ema9  = ta.ema(df["close"], length=9)
    ema21 = ta.ema(df["close"], length=21)
    if ema9 is not None and ema21 is not None:
        if ema9.iloc[-2] < ema21.iloc[-2] and ema9.iloc[-1] > ema21.iloc[-1]:
            signals.append(f"🔀 EMA9 cruzó arriba EMA21 (alcista)")
        elif ema9.iloc[-2] > ema21.iloc[-2] and ema9.iloc[-1] < ema21.iloc[-1]:
            signals.append(f"🔀 EMA9 cruzó abajo EMA21 (bajista)")

    # Bollinger Bands breakout + width
    bb = ta.bbands(df["close"], length=20, std=2)
    if bb is not None:
        upper_col = [c for c in bb.columns if "BBU" in c][0]
        lower_col = [c for c in bb.columns if "BBL" in c][0]
        mid_col   = [c for c in bb.columns if "BBM" in c][0]
        upper = bb[upper_col].iloc[-1]
        lower = bb[lower_col].iloc[-1]
        mid   = bb[mid_col].iloc[-1]
        close = df["close"].iloc[-1]
        bb_width = (upper - lower) / mid if mid != 0 else 0

        if close > upper:
            signals.append(f"🔥 BB breakout arriba (close={close:.4f} > upper={upper:.4f})")
        elif close < lower:
            signals.append(f"🔥 BB breakout abajo (close={close:.4f} < lower={lower:.4f})")

        if bb_width < BB_WIDTH_MIN:
            signals.append(f"🤏 BB squeeze (width={bb_width:.3%}) — posible movimiento fuerte próximo")

    # Volumen spike
    vol_mean = df["volume"].iloc[-21:-1].mean()
    vol_curr = df["volume"].iloc[-1]
    if vol_mean > 0 and vol_curr > vol_mean * VOLUME_MULT:
        signals.append(f"🚀 Volumen spike {vol_curr/vol_mean:.1f}x promedio")

    return signals if signals else None


# ── Telegram ───────────────────────────────────────────────────────────────────
def send_telegram(message):
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    payload = {
        "chat_id": TELEGRAM_CHAT_ID,
        "text": message,
        "parse_mode": "HTML"
    }
    r = requests.post(url, json=payload, timeout=10)
    r.raise_for_status()


# ── Main ───────────────────────────────────────────────────────────────────────
def main():
    now = datetime.utcnow().strftime("%Y-%m-%d %H:%M UTC")
    print(f"[{now}] Iniciando scan de top {TOP_N} pares USDT en Binance Spot ({INTERVAL})...")

    pairs = get_top_usdt_pairs(TOP_N)
    print(f"Pares a scanear: {len(pairs)}")

    all_signals = []

    for symbol in pairs:
        signals = analyze(symbol)
        if signals:
            all_signals.append((symbol, signals))
            print(f"  ✅ {symbol}: {signals}")
        else:
            print(f"  — {symbol}: sin señal")

    if all_signals:
        lines = [f"🔍 <b>Crypto Screener</b> | {now}\n"]
        for symbol, sigs in all_signals:
            lines.append(f"<b>{symbol}</b>")
            for s in sigs:
                lines.append(f"  {s}")
            lines.append("")
        message = "\n".join(lines)

        # Telegram tiene límite de 4096 chars por mensaje, dividir si es necesario
        if len(message) <= 4096:
            send_telegram(message)
        else:
            chunks = []
            current = f"🔍 <b>Crypto Screener</b> | {now}\n\n"
            for symbol, sigs in all_signals:
                block = f"<b>{symbol}</b>\n" + "\n".join(f"  {s}" for s in sigs) + "\n\n"
                if len(current) + len(block) > 4000:
                    chunks.append(current)
                    current = block
                else:
                    current += block
            if current:
                chunks.append(current)
            for chunk in chunks:
                send_telegram(chunk)

        print(f"\n✅ {len(all_signals)} señales enviadas a Telegram.")
    else:
        print("\n— Sin señales en este scan.")


if __name__ == "__main__":
    main()
