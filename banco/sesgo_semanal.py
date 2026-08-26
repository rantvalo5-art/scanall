"""El filtro n>=20 de _p_bloques: es neutral o selecciona las semanas buenas?"""
import numpy as np, pandas as pd
from klines import klines, load_panel, to_ms
from metricas import feat_metricas, load_metrics
from primer_toque import tabla, winrate_necesario

INICIO, FIN = "2021-08-01", "2026-08-01"
NEC = winrate_necesario(8.0, 8.0, 0.181)

def senales(n, pin):
    panel = load_panel(INICIO, FIN, n=n, pin=pin)
    T = tabla(panel, target=8, stop=8, horizonte_d=7, paso_h=4, verbose=False)
    M = load_metrics(list(panel.keys()), INICIO, FIN, verbose=False)
    G = feat_metricas(M, T, verbose=False)
    btc = klines("BTCUSDT", to_ms(INICIO), to_ms(FIN), "1h")[["t","c"]].copy()
    btc["ema"] = btc["c"].ewm(span=168, adjust=False).mean()
    btc["bajista"] = btc["c"] < btc["ema"]
    reg = T[["t"]].merge(btc[["t","bajista"]], on="t", how="left")["bajista"]
    reg.index = T.index
    S = T[(G.oi_z<-2).fillna(False) & reg.fillna(False) & T.resuelto].copy()
    S["gana"] = S.res < 0
    return S

for etiqueta, n, pin in [("OOS del preregistro (54 pares)", 60, "oos_oi"),
                         ("universo del FORWARD (100 pares)", 100, "metricas100")]:
    S = senales(n, pin)
    sem = S.groupby("semana").gana.agg(wr=lambda s:100*s.mean(), n="size")
    inc, exc = sem[sem.n>=20], sem[sem.n<20]
    print(f"\n{'='*74}\n{etiqueta}\n{'='*74}")
    print(f"senales {len(S):,} | semanas con senal {len(sem)} | win rate POOLED {100*S.gana.mean():.2f}%")
    print(f"\n  semanas QUE CUENTAN (n>=20): {len(inc):>3} ({100*len(inc)/len(sem):>2.0f}%)  "
          f"trades {inc.n.sum():>5,}  win rate pooled {100*S[S.semana.isin(inc.index)].gana.mean():.2f}%")
    print(f"  semanas DESCARTADAS (n<20) : {len(exc):>3} ({100*len(exc)/len(sem):>2.0f}%)  "
          f"trades {exc.n.sum():>5,}  win rate pooled {100*S[S.semana.isin(exc.index)].gana.mean():.2f}%")
    print(f"  umbral necesario: {NEC:.2f}%")
    # ritmo: cada cuantas semanas de calendario aparece una semana util
    tot = pd.period_range(S.semana.min(), S.semana.max(), freq="W").size
    print(f"\n  calendario cubierto: {tot} semanas -> 1 semana util cada "
          f"{tot/max(len(inc),1):.1f} semanas de calendario")
    print(f"  => 8 semanas utiles ~ {8*tot/max(len(inc),1):.0f} semanas de calendario "
          f"({8*tot/max(len(inc),1)/4.35:.1f} meses)")
