"""
EL RIESGO DE COLA DEL SHORT — lo unico grande que quedaba sin medir.

La triple barrera supone que se sale EXACTO en -8%. Para un short eso es optimista: la
vela que dispara el stop puede haber ido MUCHO mas arriba de +8% dentro de esa hora, y
ahi el stop no te llena en +8%.

Se mide sobre los MISMOS trades del test aprobado, tres modelos de salida:
  ideal   : se sale en +8% clavado (lo que supone el banco)
  al cierre: no llegaste a salir en el spike, saliste al cierre de esa hora
  al maximo: saliste en el peor tick de esa hora (catastrofico, cota inferior)

Y en vez de win rate se calcula EXPECTATIVA en %, que es lo que la cola puede romper.
"""
import numpy as np
import pandas as pd

from funding import acumulado, bajar
from klines import klines, load_panel, to_ms
from lote import features
from metricas import feat_metricas, load_metrics
from primer_toque import tabla

INICIO, FIN = "2021-08-01", "2026-08-01"
INI_MS, FIN_MS = to_ms(INICIO), to_ms(FIN)
TGT = STP = 8.0
HOR, PASO = 7 * 24, 4

panel = load_panel(INICIO, FIN, n=60, pin="oos_oi", verbose=False)
T = tabla(panel, target=TGT, stop=STP, horizonte_d=7, paso_h=PASO, verbose=False)
F = features(panel, T, verbose=False)
M = load_metrics(list(panel.keys()), INICIO, FIN, verbose=False)
G = feat_metricas(M, T, verbose=False)
btc = klines("BTCUSDT", INI_MS, FIN_MS, "1h")[["t", "c"]].copy()
btc["ema"] = btc["c"].ewm(span=168, adjust=False).mean()
btc["bajista"] = btc["c"] < btc["ema"]
reg = T[["t"]].merge(btc[["t", "bajista"]], on="t", how="left")["bajista"]
reg.index = T.index
mask = (G.oi_z < -2).fillna(False) & reg.fillna(False)
quiero = set(zip(T.sym[mask], T.t[mask]))

# --- re-caminar las entradas guardando la vela que resuelve --------------------
filas = []
up_f, dn_f = 1 + TGT / 100, 1 - STP / 100
for sym, df in panel.items():
    c = df["c"].to_numpy(float); h = df["h"].to_numpy(float)
    l = df["l"].to_numpy(float); t = df["t"].to_numpy()
    for i in range(0, len(c) - HOR, PASO):
        if (sym, t[i]) not in quiero:
            continue
        e = c[i]
        sh, sl = h[i+1:i+1+HOR], l[i+1:i+1+HOR]
        hu = np.flatnonzero(sh >= e * up_f)
        hd = np.flatnonzero(sl <= e * dn_f)
        iu = hu[0] if hu.size else np.inf
        idn = hd[0] if hd.size else np.inf
        if iu == np.inf and idn == np.inf:            # timeout: se cierra al final
            j = i + HOR
            filas.append((sym, t[i], "timeout", HOR, 100*(e/c[j]-1), 100*(e/c[j]-1)))
        elif iu < idn:                                # subio 8% -> el SHORT PIERDE
            j = i + 1 + int(iu)
            filas.append((sym, t[i], "perdida", int(iu)+1,
                          -100*(h[j]/e - 1),           # salida en el MAXIMO
                          -100*(max(c[j], e*up_f)/e - 1)))   # salida al CIERRE
        else:                                         # bajo 8% -> el SHORT GANA
            filas.append((sym, t[i], "ganada", int(idn)+1, STP, STP))

R = pd.DataFrame(filas, columns=["sym", "t", "res", "velas", "peor", "cierre"])
R["ideal"] = np.where(R.res == "perdida", -STP, np.where(R.res == "ganada", STP, R.peor))
R["cierre"] = np.where(R.res == "ganada", STP, R.cierre)

FUND = {s: bajar(s, INI_MS, FIN_MS) for s in sorted(R.sym.unique())}
R["fund"] = [100*acumulado(FUND.get(r.sym), int(r.t), int(r.velas)) for r in R.itertuples()]

perd = R[R.res == "perdida"]
exc = -perd.peor            # cuanto subio de verdad, en %
print(f"trades {len(R):,} | ganadas {100*(R.res=='ganada').mean():.1f}% "
      f"| perdidas {100*(R.res=='perdida').mean():.1f}% "
      f"| timeout {100*(R.res=='timeout').mean():.1f}%")
print(f"\n--- la vela que dispara el stop, en las {len(perd):,} perdidas ---")
print(f"  el modelo supone que subio          : {STP:.1f}%")
print(f"  subio de verdad (mediana)           : {exc.median():.2f}%")
print(f"  media                               : {exc.mean():.2f}%")
print(f"  p90 / p99                           : {np.percentile(exc,90):.2f}% / "
      f"{np.percentile(exc,99):.2f}%")
print(f"  peor caso                           : {exc.max():.2f}%")
for u in (15, 20, 30, 50):
    print(f"  velas que pasaron +{u}%              : {100*(exc>u).mean():5.2f}%  "
          f"({int((exc>u).sum())} trades)")

print(f"\n--- expectativa por trade, con funding y 0,20% de comision ---")
for nombre, col in (("ideal   (sale clavado en -8%)", "ideal"),
                    ("al cierre (no sale en el spike)", "cierre"),
                    ("al maximo (peor tick, cota inferior)", "peor")):
    ev = (R[col] + R.fund).mean() - 0.20
    print(f"  {nombre:38s} {ev:+.3f}% por trade")
