"""
CORRIDA 8 — paso 1 de la direccion 2.1: existe el instrumento?

Regla de parada, textual de banco/PREREGISTRO_OPCIONES.md, escrita ANTES de mirar:

    (A) INSTRUMENTO: >= 3 subyacentes que NO sean BTC ni ETH, en cualquiera de los
        tres venues, con volumen de opciones 24h >= USD 1.000.000 de nocional Y
        open interest >= USD 5.000.000 de nocional.

    (B) COSTO: para esos subyacentes, la mediana del spread ATM relativo
        (ask-bid)/mid del vencimiento mas cercano a 30 dias con >= 7 dias de vida,
        <= 30%.  Se lee como MEDIO spread (vender al bid y llevar a vencimiento
        cruza una sola vez) = <= 15% de la prima, que es el orden del premio total
        que BTC llego a pagar en su mejor regimen.

Si cualquiera falla -> CERRADO, y no se mide ninguna prima.

Unidades: TODO a nocional del subyacente en USD. Los tres venues cotizan la prima en
monedas distintas (BTC, USDC, USDT) y la cantidad en unidades del subyacente o en
contratos. El spread RELATIVO es adimensional, asi que no lo afecta; el volumen y el
OI si, y por eso se convierte explicitamente venue por venue.

    $env:PYTHONIOENCODING = "utf-8"
    py -3.13 -u viabilidad.py
"""
import json
import os
import sys
import time
from datetime import datetime, timezone

import numpy as np
import pandas as pd
import requests

HERE = os.path.dirname(os.path.abspath(__file__))
SNAPS = os.path.join(HERE, ".snapshots")

# --- los umbrales del preregistro, fijados antes de mirar -------------------
VOL_MIN_USD = 1_000_000      # nocional 24h
OI_MIN_USD = 5_000_000       # nocional
SPREAD_MAX = 0.30            # (ask-bid)/mid en el ATM
DIAS_OBJ = 30                # vencimiento objetivo
DIAS_MIN = 7                 # vida minima para no medir el spread de un vencimiento muerto

MAYORES = {"BTC", "ETH"}     # ya cerrados en iv_rv.py
NO_CRIPTO = {"XAU", "XAG", "EUR", "USD"}   # se reportan aparte (regla de clase de activo)

S = requests.Session()
S.headers.update({"User-Agent": "Mozilla/5.0"})


def _get(url, params=None, intentos=3):
    for i in range(intentos):
        try:
            r = S.get(url, params=params, timeout=45)
            r.raise_for_status()
            return r.json()
        except Exception as e:
            if i == intentos - 1:
                print(f"    ! fallo {url}: {type(e).__name__} {str(e)[:90]}")
                return None
            time.sleep(1.5 * (i + 1))


def _rel_spread(bid, ask):
    """Spread relativo (ask-bid)/mid. None si el libro no tiene dos lados."""
    if bid is None or ask is None:
        return None
    try:
        bid, ask = float(bid), float(ask)
    except (TypeError, ValueError):
        return None
    if not (bid > 0 and ask > 0 and ask >= bid):
        return None
    mid = (bid + ask) / 2
    return (ask - bid) / mid if mid > 0 else None


# =========================================================================
# DERIBIT
# =========================================================================
def deribit():
    """volume y open_interest vienen en UNIDADES DEL SUBYACENTE (no en contratos):
    verificado contra volume_notional, que es la prima en USD. Nocional = cantidad
    x underlying_price."""
    print("\n[deribit] listando instrumentos...")
    ins = _get("https://www.deribit.com/api/v2/public/get_instruments",
               {"currency": "any", "kind": "option", "expired": "false"})
    if not ins or "result" not in ins:
        return pd.DataFrame()
    meta = {d["instrument_name"]: d for d in ins["result"]}
    print(f"          {len(meta)} instrumentos vivos")

    filas = []
    for cur in ("BTC", "ETH", "USDC"):
        bs = _get("https://www.deribit.com/api/v2/public/get_book_summary_by_currency",
                  {"currency": cur, "kind": "option"})
        for d in (bs or {}).get("result", []):
            m = meta.get(d["instrument_name"])
            if not m:
                continue
            px = d.get("underlying_price")
            if not px:
                continue
            filas.append({
                "venue": "deribit", "subyac": m["base_currency"], "instr": d["instrument_name"],
                "vence": m["expiration_timestamp"], "strike": m["strike"],
                "tipo": m["option_type"], "spot": float(px),
                "vol_usd": float(d.get("volume") or 0) * float(px),
                "oi_usd": float(d.get("open_interest") or 0) * float(px),
                "bid": d.get("bid_price"), "ask": d.get("ask_price"),
            })
    return pd.DataFrame(filas)


# =========================================================================
# BYBIT
# =========================================================================
def bybit():
    """volume24h y openInterest en unidades del subyacente; underlyingPrice en USD.
    Primas en USDT (lineales)."""
    print("\n[bybit]   barriendo base coins...")
    cands = ("BTC ETH SOL XRP DOGE BNB LTC ADA AVAX LINK TON TRX HYPE PEPE SUI APT "
             "ARB OP NEAR DOT MATIC SHIB WLD XAU").split()
    filas = []
    for b in cands:
        r = _get("https://api.bybit.com/v5/market/tickers",
                 {"category": "option", "baseCoin": b})
        lst = (r or {}).get("result", {}).get("list", []) or []
        if not lst:
            continue
        print(f"          {b}: {len(lst)} instrumentos")
        for d in lst:
            try:
                # SOL-4SEP26-88-C-USDT
                partes = d["symbol"].split("-")
                vence = pd.Timestamp(partes[1], tz="UTC")
                strike = float(partes[2])
                tipo = "call" if partes[3] == "C" else "put"
                px = float(d.get("underlyingPrice") or 0)
            except Exception:
                continue
            if px <= 0:
                continue
            filas.append({
                "venue": "bybit", "subyac": b, "instr": d["symbol"],
                "vence": int(vence.timestamp() * 1000), "strike": strike,
                "tipo": tipo, "spot": px,
                "vol_usd": float(d.get("volume24h") or 0) * px,
                "oi_usd": float(d.get("openInterest") or 0) * px,
                "bid": d.get("bid1Price"), "ask": d.get("ask1Price"),
            })
    return pd.DataFrame(filas)


# =========================================================================
# OKX
# =========================================================================
def okx():
    """GOTCHA medido: el tamano de contrato de OKX es ctVal x ctMult, NO ctVal solo.
    ctVal viene 1 y ctMult 0,01 (BTC) o 0,1 (SOL): usar ctVal solo infla el nocional
    100x y daba USD 628.000 millones/dia en BTC, seis veces Deribit, que es imposible.
    Se usan directamente las cantidades ya expresadas en unidades del subyacente
    (volCcy24h y oiCcy), que es lo que ctVal x ctMult reproduce: verificado, vol24h
    24582 x 0,01 = 245,82 = volCcy24h. Precio del subyacente = index."""
    print("\n[okx]     listando tickers...")
    tk = _get("https://www.okx.com/api/v5/market/tickers", {"instType": "OPTION"})
    datos = (tk or {}).get("data", []) or []
    fams = sorted({"-".join(d["instId"].split("-")[:2]) for d in datos})
    print(f"          {len(datos)} instrumentos, familias: {fams}")

    meta, oi_ct, idx = {}, {}, {}
    for f in fams:
        ins = _get("https://www.okx.com/api/v5/public/instruments",
                   {"instType": "OPTION", "instFamily": f})
        for d in (ins or {}).get("data", []):
            meta[d["instId"]] = d
        oi = _get("https://www.okx.com/api/v5/public/open-interest",
                  {"instType": "OPTION", "instFamily": f})
        for d in (oi or {}).get("data", []):
            oi_ct[d["instId"]] = float(d.get("oiCcy") or 0)   # ya en unidades del subyacente
        uly = f.replace("_UM", "")
        it = _get("https://www.okx.com/api/v5/market/index-tickers", {"instId": uly})
        px = (it or {}).get("data", [{}])
        idx[f] = float(px[0].get("idxPx")) if px and px[0].get("idxPx") else 0.0

    filas = []
    for d in datos:
        m = meta.get(d["instId"])
        if not m:
            continue
        fam = "-".join(d["instId"].split("-")[:2])
        px = idx.get(fam, 0.0)
        if px <= 0:
            continue
        filas.append({
            "venue": "okx", "subyac": m["instFamily"].split("-")[0], "instr": d["instId"],
            "vence": int(m["expTime"]), "strike": float(m["stk"]),
            "tipo": "call" if m["optType"] == "C" else "put", "spot": px,
            "vol_usd": float(d.get("volCcy24h") or 0) * px,
            "oi_usd": oi_ct.get(d["instId"], 0.0) * px,
            "bid": d.get("bidPx"), "ask": d.get("askPx"),
        })
    return pd.DataFrame(filas)


# =========================================================================
def spread_atm(g, ahora_ms):
    """Spread ATM del vencimiento mas cercano a 30d con >= 7 dias de vida.

    Devuelve (spread_relativo, dias, n_dos_lados, n_total_venc, strike).
    El ATM es el strike con |strike - spot| minimo; se toma la MEDIANA del call y
    el put de ese strike (los dos son el mismo ATM y deberian cotizar parecido).
    """
    dias = (g.vence - ahora_ms) / 86_400_000
    vivos = g[dias >= DIAS_MIN]
    if vivos.empty:
        return None, None, 0, 0, None
    d2 = (vivos.vence - ahora_ms) / 86_400_000
    obj = vivos.vence.iloc[(d2 - DIAS_OBJ).abs().argmin()]
    v = vivos[vivos.vence == obj]
    dd = (obj - ahora_ms) / 86_400_000

    dos_lados = sum(1 for _, r in v.iterrows() if _rel_spread(r.bid, r.ask) is not None)
    spot = v.spot.median()
    k = v.strike.iloc[(v.strike - spot).abs().argmin()]
    atm = v[v.strike == k]
    sp = [s for s in (_rel_spread(r.bid, r.ask) for _, r in atm.iterrows()) if s is not None]
    return (float(np.median(sp)) if sp else None), dd, dos_lados, len(v), k


def main():
    os.makedirs(SNAPS, exist_ok=True)
    ahora = datetime.now(timezone.utc)
    ahora_ms = int(ahora.timestamp() * 1000)

    print("=" * 92)
    print("CORRIDA 8 — VIABILIDAD: existe el instrumento para vender volatilidad en alts?")
    print(f"foto tomada {ahora:%Y-%m-%d %H:%M UTC}")
    print("=" * 92)
    print(f"regla de parada preregistrada:  (A) >= 3 subyacentes NO BTC/ETH con "
          f"vol24h >= ${VOL_MIN_USD/1e6:.0f}M y OI >= ${OI_MIN_USD/1e6:.0f}M")
    print(f"                                (B) spread ATM relativo <= {SPREAD_MAX:.0%} "
          f"(= {SPREAD_MAX/2:.0%} de la prima al vender al bid)")

    df = pd.concat([deribit(), bybit(), okx()], ignore_index=True)
    if df.empty:
        print("\nNo se pudo bajar nada. Sin datos no hay veredicto.")
        return 2

    df.to_csv(os.path.join(SNAPS, f"opciones_{ahora:%Y%m%d_%H%M}.csv"), index=False)

    filas = []
    for (venue, sub), g in df.groupby(["venue", "subyac"]):
        sp, dd, dos, ntot, k = spread_atm(g, ahora_ms)
        filas.append({
            "venue": venue, "subyac": sub, "n": len(g),
            "vol_usd": g.vol_usd.sum(), "oi_usd": g.oi_usd.sum(),
            "spread": sp, "venc_d": dd, "strike": k,
            "dos_lados": dos, "n_venc": ntot,
        })
    t = pd.DataFrame(filas).sort_values("oi_usd", ascending=False)

    print(f"\n{'='*92}\nTODOS LOS SUBYACENTES CON OPCIONES LISTADAS "
          f"(nocional del subyacente, USD)\n{'='*92}")
    print(f"{'venue':<9}{'sub':<7}{'n':>6}{'vol 24h':>14}{'open int':>14}"
          f"{'ATM spread':>12}{'venc':>7}{'2 lados':>10}")
    for _, r in t.iterrows():
        marca = "  <- BTC/ETH" if r.subyac in MAYORES else ("  <- no cripto" if r.subyac in NO_CRIPTO else "")
        sp = f"{r.spread:11.1%}" if r.spread is not None and not pd.isna(r.spread) else "        s/d"
        vd = f"{r.venc_d:6.0f}d" if r.venc_d is not None and not pd.isna(r.venc_d) else "     -"
        print(f"{r.venue:<9}{r.subyac:<7}{r.n:>6}{r.vol_usd:>14,.0f}{r.oi_usd:>14,.0f}"
              f"{sp}{vd}{r.dos_lados:>6}/{r.n_venc:<4}{marca}")

    # --- COMPUERTA A -----------------------------------------------------
    cand = t[(~t.subyac.isin(MAYORES)) & (~t.subyac.isin(NO_CRIPTO))]
    pasa_a = cand[(cand.vol_usd >= VOL_MIN_USD) & (cand.oi_usd >= OI_MIN_USD)]
    subs_a = sorted(set(pasa_a.subyac))

    print(f"\n{'='*92}\nCOMPUERTA (A) — INSTRUMENTO\n{'='*92}")
    print(f"  subyacentes alt distintos con opciones listadas: "
          f"{len(set(cand.subyac))}  {sorted(set(cand.subyac))}")
    print(f"  pares venue-subyacente que cumplen vol Y OI: {len(pasa_a)}")
    print(f"  SUBYACENTES distintos que cumplen: {len(subs_a)}  {subs_a}")
    ok_a = len(subs_a) >= 3
    print(f"  -> (A) {'PASA' if ok_a else 'FALLA'}  (hacen falta 3)")

    # --- COMPUERTA B -----------------------------------------------------
    print(f"\n{'='*92}\nCOMPUERTA (B) — COSTO\n{'='*92}")
    if not ok_a:
        print("  no se evalua: (A) ya cerro la corrida.")
        ok_b = False
    else:
        med = {}
        for s in subs_a:
            sp = pasa_a[pasa_a.subyac == s].spread.dropna()
            med[s] = float(sp.median()) if len(sp) else None
            v = f"{med[s]:.1%}" if med[s] is not None else "sin cotizacion de dos lados"
            estado = "" if med[s] is None else ("  ok" if med[s] <= SPREAD_MAX else "  NO cruza")
            print(f"  {s:<8} spread ATM relativo {v:>28}"
                  f"   -> media prima {'' if med[s] is None else f'{med[s]/2:.1%}'}{estado}")
        buenos = [s for s, v in med.items() if v is not None and v <= SPREAD_MAX]
        ok_b = len(buenos) >= 3
        print(f"\n  subyacentes que cumplen (A) Y (B): {len(buenos)}  {sorted(buenos)}")
        print(f"  -> (B) {'PASA' if ok_b else 'FALLA'}  (hacen falta 3)")

    print(f"\n{'='*92}\nVEREDICTO\n{'='*92}")
    if ok_a and ok_b:
        print("  ABIERTA. La regla preregistrada se cumple: hay instrumento.")
        print("  PASO SIGUIENTE OBLIGATORIO antes de medir prima alguna: consistencia.")
        print("  Repetir esta foto en >= 3 dias distintos y verificar que los mismos")
        print("  subyacentes siguen cumpliendo (A). Un solo dia no prueba 'consistente'.")
    else:
        print("  CERRADA. La regla de parada, escrita antes de mirar, dispara.")
        print("  No se mide la prima. No se busca un cuarto venue. No se baja el umbral.")
    print("=" * 92)
    return 0


if __name__ == "__main__":
    sys.exit(main())
