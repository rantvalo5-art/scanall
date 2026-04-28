import os
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed
import requests
import pandas as pd

# ── CONFIG ─────────────────────────────────────────────
TELEGRAM_TOKEN = os.environ["TELEGRAM_TOKEN"]
TELEGRAM_CHAT_ID = os.environ["TELEGRAM_CHAT_ID"]

INTERVAL = "15m"
LIMIT = 120
MAX_WORKERS = 20
TOP_N = 100
TOP_ALERTS = 2

MIN_QUOTE_VOLUME = 300000

# CORE LOGIC
MIN_VOL_STRONG = 10
MIN_VOL_BEST = 20
BB_EXPANSION_MIN = 1.15   # 15% expansión
MAX_EXTENSION = 0.04      # 4%

# memoria simple (repetición)
recent_symbols = {}

# ── TELEGRAM ───────────────────────────────────────────
def send(msg):
    requests.post(
        f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage",
        json={"chat_id": TELEGRAM_CHAT_ID, "text": msg},
        timeout=10
    )

# ── DATA ───────────────────────────────────────────────
def get_pairs():
    r = requests.get("https://data-api.binance.vision/api/v3/ticker/24hr")
    data = r.json()
    pairs = [
        x["symbol"] for x in data
        if x["symbol"].endswith("USDT")
        and float(x["quoteVolume"]) > MIN_QUOTE_VOLUME
    ]
    return pairs[:TOP_N]

def get_klines(symbol):
    r = requests.get(
        "https://data-api.binance.vision/api/v3/klines",
        params={"symbol": symbol, "interval": INTERVAL, "limit": LIMIT}
    )
    df = pd.DataFrame(r.json(), columns=[
        "t","o","h","l","c","v","ct","q","n","tb","tq","i"
    ])
    for col in ["o","h","l","c","v"]:
        df[col] = df[col].astype(float)
    return df

# ── CORE DETECTION ─────────────────────────────────────
def analyze(symbol):
    try:
        df = get_klines(symbol)
    except:
        return None

    close = df["c"]
    open_ = df["o"]
    volume = df["v"]

    # BB
    ma = close.rolling(20).mean()
    std = close.rolling(20).std()
    upper = ma + 2*std
    lower = ma - 2*std

    width = (upper - lower) / ma
    width_prev = width.shift(1)

    bb_expansion = width.iloc[-1] > width_prev.iloc[-1] * BB_EXPANSION_MIN

    # precio
    price_up = close.iloc[-1] > open_.iloc[-1] and close.iloc[-1] > close.iloc[-2]

    # volumen
    vol_mean = volume.iloc[-20:-1].mean()
    vol_ratio = volume.iloc[-1] / vol_mean if vol_mean else 0

    # extensión
    recent_max = close.iloc[-15:-1].max()
    move = (close.iloc[-1] - recent_max) / recent_max if recent_max else 0

    too_extended = move > MAX_EXTENSION

    if not (bb_expansion and price_up and vol_ratio >= MIN_VOL_STRONG):
        return None

    # repetición
    repeat_bonus = 0
    if symbol in recent_symbols:
        repeat_bonus = 2

    # score simple
    score = 0
    score += 2  # bb
    score += 2  # price
    score += 2 if vol_ratio >= MIN_VOL_STRONG else 0
    score += 2 if vol_ratio >= MIN_VOL_BEST else 0
    score += 1 if not too_extended else -1
    score += repeat_bonus

    label = "BEST" if vol_ratio >= MIN_VOL_BEST else "STRONG"

    return {
        "symbol": symbol,
        "price": close.iloc[-1],
        "vol": vol_ratio,
        "move": move,
        "score": score,
        "label": label
    }

# ── FORMAT ─────────────────────────────────────────────
def fmt(a):
    emoji = "👑" if a["label"] == "BEST" else "💪"
    return f"""{emoji} {a['label']} EXPLOSION

{a['symbol']}
Price: {a['price']:.4f}
Vol: {a['vol']:.1f}x
Move: {a['move']*100:.2f}%
"""

# ── EXPORT ─────────────────────────────────────────────
def log(a):
    with open("alerts_log.csv","a") as f:
        f.write(f"{datetime.utcnow()},{a['symbol']},{a['price']},{a['vol']},{a['move']},{a['label']}\n")

# ── MAIN ───────────────────────────────────────────────
def main():
    pairs = get_pairs()
    results = []

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
        futures = [ex.submit(analyze, p) for p in pairs]
        for f in as_completed(futures):
            r = f.result()
            if r:
                results.append(r)

    if not results:
        print("no signals")
        return

    # ordenar
    results = sorted(results, key=lambda x: x["score"], reverse=True)

    top = results[:TOP_ALERTS]

    # guardar repetición
    for a in top:
        recent_symbols[a["symbol"]] = datetime.utcnow()

    # enviar
    msg = "🚀 TOP EXPLOSIONS\n\n"
    for a in top:
        msg += fmt(a) + "\n"
        log(a)

    send(msg)

if __name__ == "__main__":
    main()
