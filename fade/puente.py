"""
PUENTE — evalua las alertas del REPLAY con la misma vara que las alertas en vivo.

Las compuertas B1/B2/B3 y los numeros de referencia estan en fade/PUENTE.md, escrito
antes de correr. Este script solo las aplica; no decide nada.

    py -3.13 puente.py                      # usa fade/puente.json
    py -3.13 puente.py --alertas otro.json
"""
import argparse
import json
import os
import sys

import numpy as np
import pandas as pd

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
import evaluar as ev  # noqa: E402

# --- referencia EN VIVO (preexistente, ver PUENTE.md) -----------------------
VIVO = {"4h": 0.550, "24h": 1.435}
IC_4H = (0.14, 2.04)          # IC95 semanal en vivo, a 4h
DIA_VIVO = 26.6               # alertas de extension por dia en vivo
DESDE = "2026-06-26"          # mismo arranque que la ventana medida


def cargar(path):
    """El --out del backtest es {cfg_stem: [alert_record, ...]}."""
    raw = json.load(open(path, encoding="utf-8"))
    filas = raw["main"] if isinstance(raw, dict) and "main" in raw else \
        [a for v in raw.values() for a in v]
    d = pd.DataFrame(filas)
    d = d[d.signal_type.isin(ev.EXTENSION)].copy()
    d["alerted_at"] = pd.to_datetime(d["alerted_at"], utc=True, format="mixed")
    d = d[d.alerted_at >= pd.Timestamp(DESDE, tz="UTC")]
    # evaluar() re-parsea alerted_at desde string
    d["alerted_at"] = d["alerted_at"].astype(str)
    return d


def medir(d, horizonte):
    """Repite el calculo de evaluar() y devuelve los numeros, no solo el veredicto."""
    x = d.copy()
    x["alerted_at"] = pd.to_datetime(x["alerted_at"], utc=True, format="mixed")
    x["week"] = x["alerted_at"].dt.tz_localize(None).dt.to_period("W")
    x = x[x.symbol.isin(ev.con_perp())]
    x["f"] = -(x[f"price_{horizonte}"] / x["price_15m"] - 1) - ev.COSTO
    x = x.dropna(subset=["f"])
    ap = x.groupby("symbol").f.sum().sort_values()
    p, ic = ev.p_semanas(x)
    return {
        "n": len(x),
        "media": 100 * x.f.mean(),
        "sin3": 100 * x[~x.symbol.isin(ap.tail(3).index)].f.mean(),
        "sin_peor": 100 * x[x.symbol != ap.index[0]].f.mean(),
        "p": p, "ic": (100 * ic[0], 100 * ic[1]),
    }


if __name__ == "__main__":
    ap_ = argparse.ArgumentParser()
    ap_.add_argument("--alertas", default=os.path.join(HERE, "puente.json"))
    a = ap_.parse_args()

    d = cargar(a.alertas)
    t0 = pd.to_datetime(d.alerted_at).min()
    t1 = pd.to_datetime(d.alerted_at).max()
    dias = max((t1 - t0).total_seconds() / 86400, 1e-9)
    por_dia = len(d) / dias
    tipos = d.signal_type.value_counts().to_dict()

    print("=" * 78)
    print("PUENTE — replay del backtest contra los 51 dias medidos en vivo")
    print("=" * 78)
    print(f"{len(d)} alertas de extension  |  {tipos}")
    print(f"ventana {str(t0)[:10]} -> {str(t1)[:10]}  ({dias:.1f} dias, {por_dia:.1f}/dia)")

    # la tabla completa de compuertas, igual que en vivo
    for h in ("4h", "24h"):
        ev.evaluar(d, h, "price_15m", True, ev.COSTO)

    m4 = medir(d, "4h")
    m24 = medir(d, "24h")

    print("\n" + "=" * 78)
    print("COMPUERTAS DEL PUENTE (PUENTE.md, escritas antes de correr)")
    print("=" * 78)
    print(f"\n  replay 4h  {m4['media']:+.3f}%   vivo {VIVO['4h']:+.3f}%"
          f"   IC95 vivo [{IC_4H[0]:+.2f}, {IC_4H[1]:+.2f}]")
    print(f"  replay 24h {m24['media']:+.3f}%   vivo {VIVO['24h']:+.3f}%")

    b1 = m4["media"] > 0 and IC_4H[0] <= m4["media"] <= IC_4H[1]
    b2 = (0.5 * DIA_VIVO <= por_dia <= 2 * DIA_VIVO) and len(tipos) == 2
    b3 = m4["media"] > 0 and m4["sin3"] > 0 and m4["sin_peor"] > 0

    print(f"\n  B1  media 4h > 0 y dentro del IC95 vivo      "
          f"{m4['media']:+.3f}%          {'OK' if b1 else 'FALLA'}")
    print(f"  B2  tasa 0,5x-2x vivo (13-53/dia), 2 tipos   "
          f"{por_dia:.1f}/dia, {len(tipos)} tipos   {'OK' if b2 else 'FALLA'}")
    print(f"  B3  (a) media, (b) sin top-3, (d) sin peor   "
          f"{m4['media']:+.2f}/{m4['sin3']:+.2f}/{m4['sin_peor']:+.2f}   "
          f"{'OK' if b3 else 'FALLA'}")

    print("\n" + "-" * 78)
    if b1 and b2 and b3:
        print("  PUENTE OK -> habilitado el paso 2 (ventanas alcistas W1/W2 de PUENTE.md)")
    elif not b1:
        print("  B1 FALLA -> STOP. El replay no reemplaza a las alertas en vivo.")
        print("  4.7 vuelve al calendario de octubre. NO retocar para que pase.")
    else:
        print("  PARCIAL -> el paso 2 se reporta como direccional, no como compuertas.")
    print("-" * 78)
