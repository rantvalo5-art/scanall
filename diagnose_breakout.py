import requests, pandas as pd, ta

SYMBOL = "OSMOUSDT"
LIMIT = 180
EMA_SLOW = 21
RECENT_LOOKBACK = 15
BREAKOUT_BUFFER = 0.003
ATR_MIN_PCT = 2.4          # tu config actual
STRONG_CLOSE_MIN = 0.70
BREAKOUT_MIN_VOL_RATIO = 2.0
BREAKOUT_MAX_EXTENDED = 0.04
BREAKOUT_BB_EXPANSION_MIN = 0.12
BREAKOUT_MIN_BODY_PCT = 0.5
BREAKOUT_5M_MIN_VOL_RATIO = 1.0
ONE_H_RESIST_LOOKBACK = 24
ONE_H_RESIST_BUFFER = 0.015

def analyze_tf(interval):
    r = requests.get("https://data-api.binance.vision/api/v3/klines", params={
        "symbol": SYMBOL, "interval": interval, "limit": LIMIT
    }).json()
    df = pd.DataFrame(r, columns=["open_time","open","high","low","close","volume",
                                   "close_time","quote_vol","trades","taker_buy_base","taker_buy_quote","ignore"])
    for col in ["open","high","low","close","volume","taker_buy_base"]:
        df[col] = df[col].astype(float)
    last_close = int(df["close_time"].iloc[-1])
    now_ms = int(pd.Timestamp.now('UTC').timestamp() * 1000)
    if now_ms < last_close:
        df = df.iloc[:-1]
    return df

# Obtener 5m, 15m, 1h
df5 = analyze_tf("5m")
df15 = analyze_tf("15m")
df1h = analyze_tf("1h")

def compute_indicators(df, lookback=RECENT_LOOKBACK):
    close = df["close"]
    high = df["high"]
    low = df["low"]
    volume = df["volume"]
    price = close.iloc[-1]

    ema = ta.trend.EMAIndicator(close, window=EMA_SLOW).ema_indicator()
    ema_up = price > ema.iloc[-1] and ema.iloc[-1] > ema.iloc[-4]

    atr = ta.volatility.AverageTrueRange(high, low, close, window=14).average_true_range().iloc[-1]
    atr_pct = (atr / price * 100) if price else 0

    recent_max = high.iloc[-(lookback+2):-2].max()
    breakout = recent_max > 0 and price > recent_max * (1 + BREAKOUT_BUFFER)

    vol_mean = volume.iloc[-21:-1].mean()
    vol_ratio = (volume.iloc[-1] / vol_mean) if vol_mean else 0

    bb = ta.volatility.BollingerBands(close, window=20, window_dev=2)
    hband = bb.bollinger_hband()
    lband = bb.bollinger_lband()
    mavg = bb.bollinger_mavg()
    w_curr = ((hband.iloc[-1] - lband.iloc[-1]) / mavg.iloc[-1]) if mavg.iloc[-1] else 0
    w_prev = ((hband.iloc[-2] - lband.iloc[-2]) / mavg.iloc[-2]) if mavg.iloc[-2] else 0
    bb_exp = (w_curr / w_prev - 1) if w_prev else 0

    close_pos = (price - low.iloc[-1]) / max(high.iloc[-1] - low.iloc[-1], 1e-12)
    strong = close_pos >= STRONG_CLOSE_MIN

    candle_range = max(high.iloc[-1] - low.iloc[-1], 1e-12)
    body_pct = abs(close.iloc[-1] - df["open"].iloc[-1]) / candle_range

    one_h_resist = high.iloc[-(ONE_H_RESIST_LOOKBACK+2):-2].max()
    dist_to_res = (one_h_resist - price) / price if price else 0

    return {
        "price": price,
        "ema_up": ema_up,
        "atr_pct": atr_pct,
        "breakout": breakout,
        "recent_max": recent_max,
        "vol_ratio": vol_ratio,
        "bb_exp": bb_exp,
        "strong_close": strong,
        "body_pct": body_pct,
        "dist_to_res": dist_to_res,
        "close_pos": close_pos,
    }

# Calcular para los tres TFs
ind15 = compute_indicators(df15)
ind5 = compute_indicators(df5, lookback=RECENT_LOOKBACK)
ind1h = compute_indicators(df1h, lookback=ONE_H_RESIST_LOOKBACK)

# Mostrar chequeos
print("=== 1h ===")
print(f"EMA trend up: {ind1h['ema_up']}")
print(f"ATR %: {ind1h['atr_pct']:.2f}% (mínimo {ATR_MIN_PCT}%) -> {'OK' if ind1h['atr_pct'] >= ATR_MIN_PCT else 'FALLA'}")
print(f"Distancia a resistencia 24h: {ind1h['dist_to_res']:.2%} (buffer {ONE_H_RESIST_BUFFER}) -> {'OK' if ind1h['dist_to_res'] > ONE_H_RESIST_BUFFER or ind15['breakout'] else 'FALLA'}")

print("\n=== 15m ===")
print(f"¿Breakout? {ind15['breakout']} (recent_max={ind15['recent_max']:.6f})")
print(f"Vol ratio: {ind15['vol_ratio']:.2f} (mínimo {BREAKOUT_MIN_VOL_RATIO}) -> {'OK' if ind15['vol_ratio'] >= BREAKOUT_MIN_VOL_RATIO else 'FALLA'}")
print(f"BB expansión: {ind15['bb_exp']:.2%} (mínimo {BREAKOUT_BB_EXPANSION_MIN}) -> {'OK' if ind15['bb_exp'] >= BREAKOUT_BB_EXPANSION_MIN else 'FALLA'}")
print(f"Strong close: {ind15['strong_close']} (posición={ind15['close_pos']:.2f}) -> {'OK' if ind15['strong_close'] else 'FALLA'}")
print(f"Candle body %: {ind15['body_pct']:.2f} (mínimo {BREAKOUT_MIN_BODY_PCT}) -> {'OK' if ind15['body_pct'] >= BREAKOUT_MIN_BODY_PCT else 'FALLA'}")

print("\n=== 5m ===")
print(f"Vol ratio: {ind5['vol_ratio']:.2f} (mínimo {BREAKOUT_5M_MIN_VOL_RATIO}) -> {'OK' if ind5['vol_ratio'] >= BREAKOUT_5M_MIN_VOL_RATIO else 'FALLA'}")
print(f"Strong close: {ind5['strong_close']} (posición={ind5['close_pos']:.2f}) -> {'OK' if ind5['strong_close'] else 'FALLA'}")

# Conclusión
checks = [
    ind1h['ema_up'],
    ind1h['atr_pct'] >= ATR_MIN_PCT,
    ind15['breakout'],
    ind15['vol_ratio'] >= BREAKOUT_MIN_VOL_RATIO,
    ind15['bb_exp'] >= BREAKOUT_BB_EXPANSION_MIN,
    ind15['strong_close'],
    ind15['body_pct'] >= BREAKOUT_MIN_BODY_PCT,
    ind5['vol_ratio'] >= BREAKOUT_5M_MIN_VOL_RATIO,
    ind5['strong_close'],
]
print("\n=== RESULTADO ===")
if all(checks):
    print("Todas las condiciones OK. La señal BREAKOUT debería haberse generado.")
else:
    print("Falló al menos un filtro. Revisá los marcados como FALLA.")