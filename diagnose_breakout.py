import requests, pandas as pd, ta

SYMBOL = "RADUSDT"
INTERVAL = "15m"
LIMIT = 180
EMA_SLOW = 21
RECENT_LOOKBACK = 15
BREAKOUT_BUFFER = 0.003
ATR_MIN_PCT = 2.4          # ← poné aquí el valor actual de tu config
OBV_SLOPE_LOOKBACK = 10
OBV_RISING_MIN = 0.024
CVD_LOOKBACK = 10
CVD_BULLISH_MIN = 0.024
STRONG_CLOSE_MIN = 0.70
BREAKOUT_MIN_VOL_RATIO = 2.0
BREAKOUT_MAX_EXTENDED = 0.04
BREAKOUT_BB_EXPANSION_MIN = 0.12

# 1. Descargar klines
r = requests.get("https://data-api.binance.vision/api/v3/klines", params={
    "symbol": SYMBOL, "interval": INTERVAL, "limit": LIMIT
}).json()
df = pd.DataFrame(r, columns=["open_time","open","high","low","close","volume",
                               "close_time","quote_vol","trades","taker_buy_base","taker_buy_quote","ignore"])
for col in ["open","high","low","close","volume","taker_buy_base"]:
    df[col] = df[col].astype(float)

# Recortar vela en formación (misma lógica del screener)
last_close_time = int(df["close_time"].iloc[-1])
now_ms = int(pd.Timestamp.utcnow().timestamp() * 1000)
if now_ms < last_close_time:
    df = df.iloc[:-1]

if len(df) < 80:
    print("Datos insuficientes")
    exit()

close = df["close"]
high = df["high"]
low = df["low"]
volume = df["volume"]
price = close.iloc[-1]

# Indicadores
ema_slow = ta.trend.EMAIndicator(close, window=EMA_SLOW).ema_indicator()
ema_trend_up = price > ema_slow.iloc[-1] and ema_slow.iloc[-1] > ema_slow.iloc[-4]

atr = ta.volatility.AverageTrueRange(high, low, close, window=14).average_true_range().iloc[-1]
atr_pct = (atr / price * 100) if price else 0

recent_max = high.iloc[-(RECENT_LOOKBACK+2):-2].max()
breakout = recent_max > 0 and price > recent_max * (1 + BREAKOUT_BUFFER)

vol_mean = volume.iloc[-21:-1].mean()
vol_ratio = (volume.iloc[-1] / vol_mean) if vol_mean else 0

bb = ta.volatility.BollingerBands(close, window=20, window_dev=2)
hband = bb.bollinger_hband()
lband = bb.bollinger_lband()
mavg = bb.bollinger_mavg()
width_curr = ((hband.iloc[-1] - lband.iloc[-1]) / mavg.iloc[-1]) if mavg.iloc[-1] else 0
width_prev = ((hband.iloc[-2] - lband.iloc[-2]) / mavg.iloc[-2]) if mavg.iloc[-2] else 0
width_expansion = (width_curr / width_prev - 1) if width_prev else 0

close_pos = (price - low.iloc[-1]) / max(high.iloc[-1] - low.iloc[-1], 1e-12)
strong_close = close_pos >= STRONG_CLOSE_MIN

candle_range = max(high.iloc[-1] - low.iloc[-1], 1e-12)
candle_body_pct = abs(close.iloc[-1] - df["open"].iloc[-1]) / candle_range

try:
    obv = ta.volume.OnBalanceVolumeIndicator(close, volume).on_balance_volume()
    obv_now = obv.iloc[-1]
    obv_ref = obv.iloc[-OBV_SLOPE_LOOKBACK]
    obv_slope = (obv_now - obv_ref) / abs(obv_now) if abs(obv_now) > 1e-12 else 0
except:
    obv_slope = 0

try:
    taker_buy = df["taker_buy_base"].astype(float)
    delta = 2 * taker_buy - volume
    cvd = delta.cumsum()
    cvd_now = cvd.iloc[-1]
    cvd_ref = cvd.iloc[-CVD_LOOKBACK]
    vol_window = volume.iloc[-CVD_LOOKBACK:].sum()
    cvd_ratio = (cvd_now - cvd_ref) / vol_window if vol_window else 0
except:
    cvd_ratio = 0

# Imprimir chequeos
print(f"Precio: {price:.6f}")
print(f"¿Breakout? {breakout} (recent_max={recent_max:.6f}, buffer={BREAKOUT_BUFFER})")
print(f"ATR 14 (1h): {atr:.6f} -> {atr_pct:.2f}%  (mínimo requerido: {ATR_MIN_PCT}%) -> {'OK' if atr_pct >= ATR_MIN_PCT else 'FALLA'}")
print(f"EMA 1h trend up: {ema_trend_up}")
print(f"Vol ratio 15m: {vol_ratio:.2f} (mínimo {BREAKOUT_MIN_VOL_RATIO}) -> {'OK' if vol_ratio >= BREAKOUT_MIN_VOL_RATIO else 'FALLA'}")
print(f"BB expansión: {width_expansion:.2%} (mínimo {BREAKOUT_BB_EXPANSION_MIN}) -> {'OK' if width_expansion >= BREAKOUT_BB_EXPANSION_MIN else 'FALLA'}")
print(f"Strong close 15m: {strong_close} (posición={close_pos:.2f})")
print(f"Candle body %: {candle_body_pct:.2f} (mínimo {0.5}) -> {'OK' if candle_body_pct >= 0.5 else 'FALLA'}")
print(f"OBV slope: {obv_slope:.4f} (mínimo {OBV_RISING_MIN} para bonus, no obligatorio)")
print(f"CVD ratio: {cvd_ratio:.4f} (mínimo {CVD_BULLISH_MIN} para bonus, no obligatorio)")