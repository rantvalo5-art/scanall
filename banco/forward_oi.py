"""
FORWARD TEST de la regla del PREREGISTRO_OI — sobre SEMANAS NUEVAS.

Lo unico que le faltaba a la regla: el test aprobado uso monedas nuevas pero LAS MISMAS
SEMANAS. Esto corre la misma regla, sin cambiarle nada, sobre semanas posteriores a la
ventana de descubrimiento (2021-08-01 -> 2026-08-01).

    py -3.13 forward_oi.py                     # desde 2026-08-01 hasta hoy
    py -3.13 forward_oi.py --desde 2026-08-01 --hasta 2026-12-01

SE NIEGA A DAR VEREDICTO con menos de 8 semanas. No es una molestia: mirar un resultado
subpotenciado y despues volver a mirar cuando hay mas datos es multiplicidad encubierta,
y este repo ya se comio esa. Con pocas semanas solo chequea que la plomeria ande.
"""
import argparse
from datetime import datetime, timezone

import numpy as np
import pandas as pd

from funding import acumulado, bajar
from klines import klines, load_panel, to_ms
from lote import features
from metricas import feat_metricas, load_metrics
from primer_toque import tabla, winrate_necesario

SEMANAS_MIN = 8
TGT = STP = 8.0

ap = argparse.ArgumentParser()
ap.add_argument("--desde", default="2026-08-01")
ap.add_argument("--hasta", default=datetime.now(timezone.utc).strftime("%Y-%m-%d"))
ap.add_argument("--pares", type=int, default=100)
a = ap.parse_args()

print(f"FORWARD TEST — {a.desde} -> {a.hasta}")
panel = load_panel(a.desde, a.hasta, n=a.pares, pin="metricas100", min_bars=200)
T = tabla(panel, target=TGT, stop=STP, horizonte_d=7, paso_h=4, verbose=False)
F = features(panel, T, verbose=False)
M = load_metrics(list(panel.keys()), a.desde, a.hasta, verbose=False)
G = feat_metricas(M, T, verbose=False)

btc = klines("BTCUSDT", to_ms(a.desde), to_ms(a.hasta), "1h")[["t", "c"]].copy()
btc["ema"] = btc["c"].ewm(span=168, adjust=False).mean()
btc["bajista"] = btc["c"] < btc["ema"]
reg = T[["t"]].merge(btc[["t", "bajista"]], on="t", how="left")["bajista"]
reg.index = T.index
mask = (G.oi_z < -2).fillna(False) & reg.fillna(False)

S = T[mask & T.resuelto]
semanas = S.semana.nunique() if len(S) else 0
print(f"\nentradas {len(T):,} | BTC bajista {100*reg.fillna(False).mean():.1f}% "
      f"| senales resueltas {len(S):,} | semanas distintas {semanas}")

if semanas < SEMANAS_MIN:
    print(f"\n{'='*78}")
    print(f"SIN VEREDICTO — hacen falta {SEMANAS_MIN} semanas y hay {semanas}.")
    print("La plomeria anda; volver a correr mas adelante. NO mirar el numero ahora:")
    print("mirar subpotenciado y despues re-mirar es multiplicidad encubierta.")
    print("=" * 78)
    raise SystemExit(0)

FUND = {s: bajar(s, to_ms(a.desde), to_ms(a.hasta)) for s in sorted(S.sym.unique())}
f = np.array([acumulado(FUND.get(r.sym), int(r.t), int(r.velas)) for r in S.itertuples()])
costo = 0.20 - 100 * f.mean()
nec = winrate_necesario(TGT, STP, costo)
wr = 100 * (S.res < 0).mean()      # short gana cuando el largo pierde

por_sem = S.groupby("semana").res.apply(lambda s: 100 * (s < 0).mean())
ap_ = S.groupby("sym").res.apply(lambda s: (s < 0).mean())
sin3 = S[~S.sym.isin(ap_.sort_values().tail(3).index)]

print(f"\n{'='*78}")
print(f"VEREDICTO — {semanas} semanas nuevas")
print("=" * 78)
print(f"  funding medio      {100*f.mean():+.4f}%   costo real {costo:.3f}%   "
      f"umbral {nec:.2f}%")
print(f"  win rate           {wr:.2f}%   margen {wr-nec:+.2f} pp")
print(f"  sin top-3 monedas  {100*(sin3.res<0).mean():.2f}%   "
      f"margen {100*(sin3.res<0).mean()-nec:+.2f} pp")
print(f"  semanas arriba     {100*(por_sem > nec).mean():.0f}%")
print(f"  {'AGUANTA' if wr > nec and (por_sem > nec).mean() >= 0.60 else 'NO AGUANTA'}")
