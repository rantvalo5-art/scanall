import requests, pandas as pd, ta

SYMBOL = "OSMOUSDT"
LIMIT = 180
MAJOR_STRUCT_LOOKBACK = 60
MAJOR_STRUCT_MAX_DIST = 0.03

r = requests.get("https://data-api.binance.vision/api/v3/klines", params={
    "symbol": SYMBOL, "interval": "1h", "limit": LIMIT
}).json()
df = pd.DataFrame(r, columns=["open_time","open","high","low","close","volume",
                               "close_time","quote_vol","trades","taker_buy_base","taker_buy_quote","ignore"])
for col in ["open","high","low","close"]:
    df[col] = df[col].astype(float)

# Recortar vela en formación
last_close = int(df["close_time"].iloc[-1])
now_ms = int(pd.Timestamp.now('UTC').timestamp() * 1000)
if now_ms < last_close:
    df = df.iloc[:-1]

high = df["high"]
price = df["close"].iloc[-1]

major_max = high.iloc[-(MAJOR_STRUCT_LOOKBACK+2):-2].max()
major_dist = (major_max - price) / price if price > 0 else 0.0
ok = major_dist <= MAJOR_STRUCT_MAX_DIST

print(f"Precio actual: {price:.6f}")
print(f"Máximo 60 velas (1h) atrás: {major_max:.6f}")
print(f"Distancia al máximo mayor: {major_dist:.2%}")
print(f"Límite permitido: {MAJOR_STRUCT_MAX_DIST:.2%}")
print(f"¿major_struct_ok? {'SÍ' if ok else 'NO (bloqueado)'}")