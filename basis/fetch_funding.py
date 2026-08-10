"""
Fase 1 — descarga y cachea el funding historico de los perps USDT de Binance.

Fuente: fapi.binance.com (/fapi/v1/fundingRate). OJO: geo-bloqueado desde runners de
GitHub y Cloudflare (451/403) — corre solo desde la PC local / VPS en jurisdiccion
permitida. Ver HANDOFF_BASIS.md seccion 6.

Que hace:
  1. Lista perps USDT PERPETUAL en TRADING (+ fundingInfo: intervalo e cap/floor).
  2. Resuelve la pata spot: mismo simbolo, o des-escalando el prefijo 1000/1000000
     (1000PEPEUSDT perp <-> PEPEUSDT spot; solo cambia la unidad, el hedge es valido).
  3. Baja el funding de la ventana FIJA de config.json y lo cachea en .funding_cache/.

Cache: un CSV por simbolo, idempotente. Re-correr no re-descarga lo ya cacheado
salvo --refresh.
"""
import json
import os
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone

import pandas as pd
import requests

HERE = os.path.dirname(os.path.abspath(__file__))
CACHE_DIR = os.path.join(HERE, ".funding_cache")
FAPI = "https://fapi.binance.com"
SPOT_API = "https://api.binance.com"


def load_config(path=None):
    with open(path or os.path.join(HERE, "config.json"), encoding="utf-8") as f:
        return json.load(f)


def _get(url, params=None, tries=4):
    """GET con reintento y backoff. Devuelve None si no se pudo."""
    for i in range(tries):
        try:
            r = requests.get(url, params=params, timeout=25)
            if r.status_code == 200:
                return r.json()
            if r.status_code in (418, 429):          # rate limit
                time.sleep(2 ** i)
                continue
            if r.status_code in (403, 451):
                print(f"  GEO-BLOCK ({r.status_code}) en {url} — ver HANDOFF seccion 6")
                return None
            time.sleep(0.5 * (i + 1))
        except Exception:
            time.sleep(0.5 * (i + 1))
    return None


def to_ms(date_str):
    return int(datetime.strptime(date_str, "%Y-%m-%d")
               .replace(tzinfo=timezone.utc).timestamp() * 1000)


# ---------------------------------------------------------------- universo

def list_perps():
    """{symbol: fundingIntervalHours} de los perps USDT en TRADING."""
    ei = _get(f"{FAPI}/fapi/v1/exchangeInfo")
    if not ei:
        return {}
    perps = [s["symbol"] for s in ei["symbols"]
             if s.get("contractType") == "PERPETUAL"
             and s.get("quoteAsset") == "USDT"
             and s.get("status") == "TRADING"]
    # El intervalo NO siempre es 8h: hay simbolos a 4h y a 1h. Leerlo, no asumirlo.
    info = _get(f"{FAPI}/fapi/v1/fundingInfo") or []
    hours = {r["symbol"]: int(r.get("fundingIntervalHours", 8)) for r in info}
    return {s: hours.get(s, 8) for s in perps}


def list_spot():
    ei = _get(f"{SPOT_API}/api/v3/exchangeInfo")
    if not ei:
        return set()
    return {s["symbol"] for s in ei["symbols"]
            if s.get("quoteAsset") == "USDT"
            and s.get("status") == "TRADING"
            and s.get("isSpotTradingAllowed")}


def resolve_spot_leg(perp, spot_set):
    """Perp -> par spot con el que se hedgea, o None.
    1000PEPEUSDT se cubre con PEPEUSDT: solo cambia la escala de la unidad."""
    if perp in spot_set:
        return perp
    base = perp[:-4]
    for pref in ("1000000", "1000"):
        if base.startswith(pref):
            cand = base[len(pref):] + "USDT"
            if cand in spot_set:
                return cand
    return None


# ---------------------------------------------------------------- funding

def _cache_path(symbol, start_ms, end_ms):
    return os.path.join(CACHE_DIR, f"{symbol}_{start_ms}_{end_ms}.csv")


def fetch_funding(symbol, start_ms, end_ms):
    """Todo el funding settled en [start, end). Paginado (limit 1000)."""
    rows, cursor = [], start_ms
    while cursor < end_ms:
        data = _get(f"{FAPI}/fapi/v1/fundingRate",
                    {"symbol": symbol, "startTime": cursor,
                     "endTime": end_ms, "limit": 1000})
        if not data:
            break
        rows.extend(data)
        last = int(data[-1]["fundingTime"])
        if len(data) < 1000 or last <= cursor:
            break
        cursor = last + 1
    if not rows:
        return None
    df = pd.DataFrame([{"funding_time": int(r["fundingTime"]),
                        "funding_rate": float(r["fundingRate"])} for r in rows])
    return (df.drop_duplicates("funding_time")
              .sort_values("funding_time").reset_index(drop=True))


def get_funding(symbol, start_ms, end_ms, refresh=False):
    os.makedirs(CACHE_DIR, exist_ok=True)
    path = _cache_path(symbol, start_ms, end_ms)
    if os.path.exists(path) and not refresh:
        try:
            return pd.read_csv(path)
        except Exception:
            pass
    df = fetch_funding(symbol, start_ms, end_ms)
    if df is not None and not df.empty:
        df.to_csv(path, index=False)
    return df


def build_universe(cfg, refresh=False, limit=None):
    """Descarga el funding de todos los perps con pata spot. Devuelve
    (dict symbol->df, dict meta) y escribe universe.json."""
    w = cfg["window"]
    start_ms, end_ms = to_ms(w["start_utc"]), to_ms(w["end_utc"])
    print(f"Ventana FIJA: {w['start_utc']} -> {w['end_utc']}")

    perps = list_perps()
    spot_set = list_spot()
    if not perps or not spot_set:
        print("FATAL: no se pudo listar el universo (geo-block?)")
        sys.exit(1)
    print(f"  perps USDT TRADING: {len(perps)} | spot USDT TRADING: {len(spot_set)}")

    require_spot = cfg["universe"].get("REQUIRE_SPOT_LEG", True)
    meta = {}
    for p, hrs in perps.items():
        leg = resolve_spot_leg(p, spot_set)
        if leg is None and require_spot:
            continue
        meta[p] = {"spot": leg, "interval_h": hrs}
    print(f"  con pata spot: {len(meta)} ({100*len(meta)//max(len(perps),1)}%)")

    targets = sorted(meta)[:limit] if limit else sorted(meta)
    data, done = {}, [0]

    def work(sym):
        df = get_funding(sym, start_ms, end_ms, refresh)
        done[0] += 1
        if done[0] % 50 == 0:
            print(f"    {done[0]}/{len(targets)}...")
        return sym, df

    t0 = time.time()
    with ThreadPoolExecutor(max_workers=8) as ex:
        for sym, df in ex.map(work, targets):
            if df is not None and not df.empty:
                data[sym] = df

    min_days = cfg["universe"].get("MIN_HISTORY_DAYS", 180)
    kept = {}
    for sym, df in data.items():
        span_d = (df["funding_time"].iloc[-1] - df["funding_time"].iloc[0]) / 86400000
        if span_d >= min_days:
            kept[sym] = df
            meta[sym]["n_settlements"] = int(len(df))
            meta[sym]["span_days"] = round(float(span_d), 1)

    print(f"  bajados: {len(data)} | con >={min_days}d de historia: {len(kept)}"
          f" | {time.time()-t0:.0f}s")

    with open(os.path.join(HERE, "universe.json"), "w", encoding="utf-8") as f:
        json.dump({s: meta[s] for s in kept}, f, indent=1)
    return kept, {s: meta[s] for s in kept}


if __name__ == "__main__":
    cfg = load_config()
    lim = None
    if "--limit" in sys.argv:
        lim = int(sys.argv[sys.argv.index("--limit") + 1])
    build_universe(cfg, refresh="--refresh" in sys.argv, limit=lim)
