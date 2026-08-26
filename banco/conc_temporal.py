"""Trampa de concentracion, pero en el eje TIEMPO. El repo la chequeo siempre por simbolo."""
import numpy as np, pandas as pd
from klines import klines, load_panel, to_ms
from metricas import feat_metricas, load_metrics
from primer_toque import tabla, winrate_necesario

INICIO, FIN = "2021-08-01", "2026-08-01"
NEC = winrate_necesario(8.0, 8.0, 0.181)
panel = load_panel(INICIO, FIN, n=60, pin="oos_oi")
T = tabla(panel, target=8, stop=8, horizonte_d=7, paso_h=4, verbose=False)
M = load_metrics(list(panel.keys()), INICIO, FIN, verbose=False)
G = feat_metricas(M, T, verbose=False)
btc = klines("BTCUSDT", to_ms(INICIO), to_ms(FIN), "1h")[["t","c"]].copy()
btc["ema"] = btc["c"].ewm(span=168, adjust=False).mean(); btc["bajista"] = btc["c"] < btc["ema"]
reg = T[["t"]].merge(btc[["t","bajista"]], on="t", how="left")["bajista"]; reg.index = T.index
S = T[(G.oi_z<-2).fillna(False) & reg.fillna(False) & T.resuelto].copy()
S["gana"] = S.res < 0

wr = lambda d: 100*d.gana.mean() if len(d) else np.nan
print(f"umbral necesario {NEC:.2f}%\n")
print(f"{'':28} {'trades':>7} {'win rate':>9} {'margen':>8}")
print(f"{'TODO':28} {len(S):>7,} {wr(S):>8.2f}% {wr(S)-NEC:>+7.2f}pp")

ap = S.groupby("semana").gana.apply(lambda s: (s.sum() - (~s).sum())).sort_values(ascending=False)
for k in (1, 2, 3, 5, 10):
    d = S[~S.semana.isin(ap.head(k).index)]
    print(f"{'sin las top-'+str(k)+' semanas':28} {len(d):>7,} {wr(d):>8.2f}% {wr(d)-NEC:>+7.2f}pp")

aps = S.groupby("sym").gana.apply(lambda s: (s.sum() - (~s).sum())).sort_values(ascending=False)
for k in (1, 3):
    d = S[~S.sym.isin(aps.head(k).index)]
    print(f"{'sin los top-'+str(k)+' simbolos':28} {len(d):>7,} {wr(d):>8.2f}% {wr(d)-NEC:>+7.2f}pp   <- la que SI se corrio")

print(f"\ntop-5 semanas por aporte neto (de {S.semana.nunique()} semanas con senal):")
for s in ap.head(5).index:
    g = S[S.semana==s]
    print(f"   {s}   n={len(g):>3}  wr={wr(g):>6.2f}%  aporte neto +{ap[s]}")
print(f"\nesas 5 semanas son el {100*5/S.semana.nunique():.1f}% de las semanas y "
      f"{100*len(S[S.semana.isin(ap.head(5).index)])/len(S):.0f}% de los trades")
