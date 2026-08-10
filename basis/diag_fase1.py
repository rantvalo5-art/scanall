"""
Diagnosticos de la Fase 1. Verifican el resultado antes de creerlo.

1. SANITY  — funding de BTC/ETH y majors: si el signo esta mal, se ve aca.
2. REGIMEN — mediana mensual entre simbolos: la ventana es bear? hubo meses buenos?
3. CONCENTRACION del subgrupo 'funding alto' — cuantos simbolos y semanas distintas
   lo componen. Es el unico hilo que quedo vivo y es exactamente donde vive la trampa
   (ver [[project-swing-trampa-concentracion]]).
4. DISTRIBUCION del trailing usado como senal.
"""
import numpy as np
import pandas as pd

from fetch_funding import load_config, build_universe, to_ms
from phase1 import (build_panel, days_to_breakeven, round_trip_cost_pct,
                    capital_factor, MS_DAY)

cfg = load_config()
data, meta = build_universe(cfg)
panel = build_panel(data, cfg)
cost = round_trip_cost_pct(cfg)
capf = capital_factor(cfg)

print("\n" + "=" * 74)
print("1. SANITY — funding acumulado por simbolo (bruto, sobre notional)")
print("=" * 74)
for sym in ("BTCUSDT", "ETHUSDT", "BNBUSDT", "SOLUSDT", "XRPUSDT", "DOGEUSDT", "ADAUSDT"):
    if sym not in panel:
        continue
    t, cs = panel[sym]
    span = (t[-1] - t[0]) / MS_DAY
    tot = cs[-1] * 100
    pos = int((np.diff(cs) > 0).sum())
    n = len(t)
    print(f"  {sym:10s} n={n:5d}  span={span:5.0f}d  acum={tot:+7.2f}%  "
          f"APY={tot/span*365:+7.2f}%  periodos positivos={100*pos/n:4.1f}%")

print("\n" + "=" * 74)
print("2. REGIMEN — mediana ENTRE SIMBOLOS del funding mensual (APY bruto)")
print("=" * 74)
rows = []
for sym, (t, cs) in panel.items():
    # csum lleva un 0 al frente -> np.diff(cs) tiene la misma longitud que t
    df = pd.DataFrame({"t": t, "r": np.diff(cs)})
    df["m"] = pd.to_datetime(df["t"], unit="ms", utc=True).dt.strftime("%Y-%m")
    # los dias salen de los timestamps: el intervalo NO siempre es 8h
    g = df.groupby("m").agg(s=("r", "sum"), n=("r", "count"),
                            t0=("t", "min"), t1=("t", "max"))
    for m, r in g.iterrows():
        days = max((r["t1"] - r["t0"]) / MS_DAY, 1.0)
        if r["n"] < 10:                      # mes parcial (listing/borde): no comparable
            continue
        rows.append({"m": m, "sym": sym, "apy": r["s"] * 100 * 365 / days})
mo = pd.DataFrame(rows)
tab = mo.groupby("m")["apy"].agg(["median", "mean", "count"])
tab["pct_pos"] = mo.groupby("m")["apy"].apply(lambda s: (s > 0).mean() * 100)
print(f"  {'mes':>9} {'mediana':>9} {'media':>9} {'%simb>0':>9} {'n':>5}")
for m, r in tab.iterrows():
    print(f"  {m:>9} {r['median']:+8.2f}% {r['mean']:+8.2f}% {r['pct_pos']:8.1f}% {int(r['count']):5d}")

print("\n" + "=" * 74)
print("3. CONCENTRACION del subgrupo 'FUNDING ALTO'")
print("=" * 74)
be = days_to_breakeven(cfg=cfg, panel=panel)
hi = be[be["is_high"]].copy()
hi["week"] = pd.to_datetime(hi["entry_ms"], unit="ms", utc=True).dt.strftime("%G-W%V")
hi["month"] = pd.to_datetime(hi["entry_ms"], unit="ms", utc=True).dt.strftime("%Y-%m")
print(f"  n entradas 'altas' : {len(hi):,} de {len(be):,} ({100*len(hi)/len(be):.2f}%)")
print(f"  simbolos distintos : {hi['symbol'].nunique()} de {len(panel)}")
print(f"  semanas distintas  : {hi['week'].nunique()} de 52")
print(f"  meses distintos    : {hi['month'].nunique()}")
top = hi["symbol"].value_counts()
print(f"\n  top simbolos (share de las entradas altas):")
for s, c in top.head(8).items():
    print(f"    {s:16s} {c:4d}  ({100*c/len(hi):4.1f}%)")
print(f"  top-1 concentra {100*top.iloc[0]/len(hi):.1f}% | "
      f"top-3 {100*top.head(3).sum()/len(hi):.1f}% | top-5 {100*top.head(5).sum()/len(hi):.1f}%")
tw = hi["week"].value_counts()
print(f"  top-3 SEMANAS concentran {100*tw.head(3).sum()/len(hi):.1f}% de las entradas altas")

print("\n  break-even del subgrupo, sacando los top aportantes:")
for k in (0, 1, 3, 5):
    drop = set(top.head(k).index) if k else set()
    sub = hi[~hi["symbol"].isin(drop)]
    if sub.empty:
        continue
    cov10 = ((sub["be_days"] > 0) & (sub["be_days"] <= 10)).mean() * 100
    med = sub.loc[sub["be_days"] > 0, "be_days"].median() if (sub["be_days"] > 0).any() else float("nan")
    print(f"    sin top-{k}: n={len(sub):4d}  cubre<=10d {cov10:5.1f}%  "
          f"mediana BE {med:4.1f}d  acum45d(mediana) {sub['cum_at_max'].median():+.3f}%")

print("\n  por simbolo (mediana del acumulado a 45d), top-10 por n:")
for s, c in top.head(10).items():
    sub = hi[hi["symbol"] == s]
    print(f"    {s:16s} n={c:4d}  acum45d mediana {sub['cum_at_max'].median():+7.3f}%  "
          f"semanas={sub['week'].nunique()}")

print("\n" + "=" * 74)
print("4. DISTRIBUCION del trailing 3d anualizado (la senal)")
print("=" * 74)
q = be["trail_apy"].quantile([.01, .05, .25, .5, .75, .95, .99])
for k, v in q.items():
    print(f"  p{int(k*100):02d}  {v:+9.2f}%")
for thr in (5, 10, 20, 30, 50, 100):
    m = be["trail_apy"] > thr
    if m.sum() == 0:
        continue
    sub = be[m]
    cov10 = ((sub["be_days"] > 0) & (sub["be_days"] <= 10)).mean() * 100
    print(f"  umbral >{thr:4d}%: n={int(m.sum()):6d}  simbolos={sub['symbol'].nunique():3d}  "
          f"cubre<=10d {cov10:5.1f}%  acum45d mediana {sub['cum_at_max'].median():+.3f}%")
