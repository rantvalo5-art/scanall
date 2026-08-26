"""
AUDITORIA: los criterios 3 y 7 del preregistro se calcularon SOLO sobre semanas con
>=20 senales (lote.py:232 y lote.py:157). Cuanto depende el veredicto de ese 20?

Esto NO afloja una compuerta: la aprieta. Chequea si una compuerta fue inadvertidamente
indulgente. Aflojar despues de ver numeros esta prohibido; auditar no.
"""
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
btc["ema"] = btc["c"].ewm(span=168, adjust=False).mean()
btc["bajista"] = btc["c"] < btc["ema"]
reg = T[["t"]].merge(btc[["t","bajista"]], on="t", how="left")["bajista"]; reg.index = T.index
S = T[(G.oi_z<-2).fillna(False) & reg.fillna(False) & T.resuelto].copy()
S["gana"] = S.res < 0
sem_all = S.groupby("semana").gana.agg(wr=lambda s:100*s.mean(), n="size")

rng = np.random.default_rng(0)
def p_bloques(wr):
    k = len(wr)
    if k < 8: return 1.0
    m = np.array([rng.choice(wr, k, replace=True).mean() for _ in range(4000)])
    return float((m <= NEC).mean())

print(f"umbral necesario {NEC:.2f}%  |  pooled TODAS las semanas {100*S.gana.mean():.2f}%\n")
print(f"{'filtro':>10} {'semanas':>8} {'trades':>8} {'%trades':>8} {'WR pooled':>10} "
      f"{'WR sem medio':>13} {'crit.7 %sem':>12} {'crit.3 p':>9} {'veredicto':>12}")
for umbral in (1, 3, 5, 10, 15, 20, 30):
    sub = sem_all[sem_all.n >= umbral]
    if not len(sub): continue
    tr = S[S.semana.isin(sub.index)]
    c7 = (sub.wr > NEC).mean()
    c3 = p_bloques(sub.wr.to_numpy())
    ok = (c3 <= 0.10) and (c7 >= 0.60)
    print(f"  n>={umbral:<6} {len(sub):>8} {sub.n.sum():>8,} {100*sub.n.sum()/len(S):>7.0f}% "
          f"{100*tr.gana.mean():>9.2f}% {sub.wr.mean():>12.2f}% {100*c7:>11.0f}% "
          f"{c3:>9.4f} {'PASA' if ok else 'FALLA':>12}")

print("\n\nlas semanas que el filtro n>=20 DESCARTA:")
exc = sem_all[sem_all.n < 20]; tr = S[S.semana.isin(exc.index)]
print(f"  {len(exc)} semanas ({100*len(exc)/len(sem_all):.0f}%), {exc.n.sum():,} trades "
      f"({100*exc.n.sum()/len(S):.0f}% del total), win rate pooled {100*tr.gana.mean():.2f}%")
print(f"  -> por debajo del umbral {NEC:.2f}%: la mitad mas grande de la muestra pierde.")
