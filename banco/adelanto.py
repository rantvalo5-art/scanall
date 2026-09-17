"""¿P5 detecta LA MISMA oportunidad antes, o cosas distintas? (stat corregido)"""
import glob
import numpy as np
import alertas as al

P = al.cargar(sorted(glob.glob("../bt_P5_*.json")))
P = P[(P["signal_type"] == "PREBREAK") & (P["t"] <= 1785000000000)]
B = al.cargar(sorted(glob.glob("../bt_rk_*.json")))

ad, nuevas = [], 0
for r in P.itertuples():
    c = B[(B["symbol"] == r.symbol) & (B["t"] - r.t).abs().le(24 * al.HORA)]
    if len(c):
        d = (c["t"].to_numpy() - r.t) / al.HORA
        ad.append(float(d[np.abs(d).argmin()]))
    else:
        nuevas += 1
ad = np.array(ad)
n = len(P)
print(f"PREBREAK de P5: {n:,}")
print(f"  sin contraparte en base (+-24h) = oportunidad nueva : {nuevas:,} ({100*nuevas/n:.0f}%)")
print(f"  con contraparte                                     : {len(ad):,} ({100*len(ad)/n:.0f}%)")
print(f"\n  delta a la contraparte MAS CERCANA (negativo = la base llego antes):")
print(f"    mediana {np.median(ad):+.1f}h   media {ad.mean():+.1f}h")
print(f"    P5 llega ANTES que la base : {100*(ad>0).mean():.0f}% de los casos")
print(f"    P5 llega DESPUES           : {100*(ad<0).mean():.0f}%")
