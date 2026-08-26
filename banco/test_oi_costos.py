"""Cuanto costo aguanta la regla antes de romperse. Igual que hizo 4.7 con sus 3 niveles."""
import numpy as np
import pandas as pd

from funding import acumulado, bajar
from klines import klines, load_panel, to_ms
from lote import features, lote
from metricas import feat_metricas, load_metrics
from primer_toque import tabla, winrate_necesario

INICIO, FIN = "2021-08-01", "2026-08-01"
INI_MS, FIN_MS = to_ms(INICIO), to_ms(FIN)

panel = load_panel(INICIO, FIN, n=60, pin="oos_oi", verbose=False)
T = tabla(panel, target=8, stop=8, horizonte_d=7, paso_h=4, verbose=False)
F = features(panel, T, verbose=False)
M = load_metrics(list(panel.keys()), INICIO, FIN, verbose=False)
G = feat_metricas(M, T, verbose=False)
btc = klines("BTCUSDT", INI_MS, FIN_MS, "1h")[["t", "c"]].copy()
btc["ema"] = btc["c"].ewm(span=168, adjust=False).mean()
btc["bajista"] = btc["c"] < btc["ema"]
reg = T[["t"]].merge(btc[["t", "bajista"]], on="t", how="left")["bajista"]
reg.index = T.index
mask = (G.oi_z < -2).fillna(False) & reg.fillna(False)

S = T[mask]
FUND = {s: bajar(s, INI_MS, FIN_MS) for s in sorted(S.sym.unique())}
f = np.array([acumulado(FUND.get(r.sym), int(r.t), int(r.velas)) for r in S.itertuples()])
fund_pp = 100 * f.mean()

Tc = T.copy()
Tc["res"] = -Tc["res"]
Tc.attrs.update(T.attrs)

print(f"funding medio (a favor del short): {fund_pp:+.4f} pp\n")
print(f"{'escenario':38s} {'costo':>8s} {'umbral':>8s} {'win%':>7s} {'margen':>8s}  veredicto")
print("-" * 100)
esc = [
    ("comision perp 0,10% (sin slippage)", 0.10),
    ("banco 0,20% (el del preregistro)",   0.20),
    ("+ slippage 0,30% (supuesto de 4.7)", 0.50),
    ("+ slippage 0,60% (alts finas)",      0.80),
    ("estres 1,20%",                       1.20),
]
for nombre, bruto in esc:
    c = bruto - fund_pp
    D = lote(Tc, {"regla": mask}, costo=c, mostrar=False)
    r = D.iloc[0]
    print(f"{nombre:38s} {bruto:7.2f}% {winrate_necesario(8,8,c):7.2f}% "
          f"{r.wr:7.2f} {r.margen:+8.2f}  {r.veredicto}")

# donde se rompe exactamente
D = lote(Tc, {"regla": mask}, costo=0.20, mostrar=False)
wr = D.iloc[0].wr
lo, hi = 0.0, 8.0
for _ in range(60):
    mid = (lo + hi) / 2
    if winrate_necesario(8, 8, mid) < wr:
        lo = mid
    else:
        hi = mid
print(f"\ncosto BRUTO de equilibrio (incluyendo el funding a favor): {lo + fund_pp:.2f}%")
