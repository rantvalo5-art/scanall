"""
DIAGNOSTICO DEL DAY TRADER POR REGIMEN — corta las alertas reales en dos tramos
(bear / bull) y muestra el retorno neto por horizonte, senal, concentracion y dia.

POR QUE EXISTE. El veredicto del day trader (pierde en todo horizonte, peor que al
azar) se midio sobre 51 dias de UN SOLO regimen bear. El 2026-08-17 arranco un tramo
alcista de BTC, que es el regimen que faltaba. Este script re-corre el descriptivo
sobre el archivo de disco, cortando donde uno quiera.

    py -3.13 dt_diag_regimen.py
    py -3.13 dt_diag_regimen.py --desde 2026-08-17

OJO — ESTE SCRIPT NO ALCANZA PARA CONCLUIR NADA. Un promedio positivo en un tramo
alcista es, por defecto, beta del mercado: comprar cualquier cosa daba positivo. Para
saber si el bot eligio bien el MOMENTO hay que correr `dt_diag_pareado.py`, que
compara cada alerta contra la misma moneda a una hora al azar de la misma ventana.
La primera vez que se corrio esto (08-22), el tramo alcista daba +4,03% a 24h y el
dardo pareado daba +5,10%.
"""
import argparse
import os

import numpy as np
import pandas as pd

HERE = os.path.dirname(os.path.abspath(__file__))
CSV = os.path.join(HERE, "archivo_outcomes", "daytrader_outcomes.csv")
COSTO = 0.0020          # 0,20% ida y vuelta spot taker, sin slippage: optimista
HORIZONTES = ["15m", "1h", "4h", "24h"]


def cargar(csv):
    D = pd.read_csv(csv, low_memory=False)
    D["t"] = pd.to_datetime(D["alerted_at"], utc=True, format="mixed")
    return D.sort_values("t").reset_index(drop=True)


def corte_madurez(D, col="price_24h", umbral=0.90):
    """Ultimo instante con forward COMPLETO.

    `update_outcomes.py` no corre cada 15 min como dice el cron del workflow: en la
    practica llena una vez por dia (~04:00 UTC) con ~2 dias de rezago. Las alertas
    recientes tienen el precio en NaN. Incluirlas sesga y no poco: dentro de un dia a
    medio llenar, las que ya tienen precio son las mas VIEJAS, asi que se cuela una
    seleccion por hora del dia. Se camina hacia atras hasta el ultimo dia con
    cobertura >= umbral y se corta al final de ese dia.
    """
    cov = D.groupby(D.t.dt.date)[col].apply(lambda c: c.notna().mean())
    dias = sorted(cov.index)
    i = len(dias) - 1
    while i >= 0 and cov[dias[i]] < umbral:
        i -= 1
    if i < 0:
        raise SystemExit("ningun dia con forward completo: correr archivar_outcomes.py")
    return pd.Timestamp(dias[i], tz="UTC") + pd.Timedelta("1D"), cov


def ret(d, h, fill, costo):
    return (d[f"price_{h}"] / d[fill] - 1) - costo


def tabla(d, titulo, fill, costo):
    print(f"### {titulo}   (fill={fill})")
    print("          n      15m       1h       4h      24h  |  med_4h  med_24h  | %pos_24h")
    for reg in ["BEAR", "BULL"]:
        g = d[d.reg == reg]
        if len(g) < 20:
            continue
        r = {h: ret(g, h, fill, costo).dropna() for h in HORIZONTES}
        print(f"  {reg:5s}{len(g):5d}   " + " ".join(f"{100*r[h].mean():+8.3f}" for h in HORIZONTES)
              + f"  | {100*r['4h'].median():+7.3f} {100*r['24h'].median():+7.3f}"
                f"  | {100*(r['24h']>0).mean():5.1f}%")
    print()


def btc_contexto(ini, fin):
    """El tramo alcista hay que verlo, no asumirlo."""
    try:
        from banco.klines import klines
    except Exception:
        return
    s = int((ini - pd.Timedelta("2D")).timestamp() * 1000)
    e = int((fin + pd.Timedelta("1D")).timestamp() * 1000)
    k = klines("BTCUSDT", s, e, "1d")
    if k is None or k.empty:
        return
    k = k.copy()
    k["d"] = pd.to_datetime(k.t, unit="ms", utc=True).dt.date
    k["c"] = k.c.astype(float)
    base = float(k.c.iloc[0])
    print("### BTC en la ventana (cierre diario, % desde el primer dia)")
    for _, r in k.iterrows():
        print(f"  {r.d}  {r.c:>10,.0f}  {100*(r.c/base-1):+6.2f}%")
    print()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--csv", default=CSV)
    ap.add_argument("--desde", default="2026-08-17", help="corte de regimen (ISO)")
    ap.add_argument("--costo", type=float, default=COSTO)
    a = ap.parse_args()

    D = cargar(a.csv)
    corte, cov = corte_madurez(D)
    print("=" * 84)
    print("DAY TRADER POR REGIMEN")
    print("=" * 84)
    print(f"archivo   {a.csv}")
    print(f"crudo     {len(D):,} filas | {D.t.min():%Y-%m-%d %H:%M} -> {D.t.max():%Y-%m-%d %H:%M} UTC"
          f" | {D.symbol.nunique()} simbolos")
    print(f"madurez   corte en {corte:%Y-%m-%d %H:%M} UTC (se descartan "
          f"{(D.t >= corte).sum():,} filas sin forward de 24h)")
    print("          cobertura 24h por dia (ultimos 5): "
          + "  ".join(f"{d} {100*cov[d]:.0f}%" for d in sorted(cov.index)[-5:]))

    CRUDO = D.copy()          # la TASA de alertas no depende de la madurez del forward
    D = D[D.t < corte].copy()
    cut = pd.Timestamp(a.desde, tz="UTC")
    D["reg"] = np.where(D.t >= cut, "BULL", "BEAR")
    if (D.reg == "BULL").sum() < 20:
        raise SystemExit(f"casi no hay filas maduras despues de {a.desde}: nada que mirar")
    bull = D[D.reg == "BULL"]
    n_dias = (bull.t.max() - cut).total_seconds() / 86400
    print(f"corte     {a.desde}  ->  BEAR {(D.reg=='BEAR').sum():,} | "
          f"BULL {len(bull):,} en {n_dias:.2f} dias ({bull.symbol.nunique()} simbolos)")
    print(f"costo     {100*a.costo:.2f}% ida y vuelta, sin slippage\n")

    btc_contexto(cut, CRUDO.t.max())

    print("### alertas por dia — sobre el CRUDO, incluidos los dias sin forward todavia.")
    print("    La tasa es un dato en si: en la aceleracion del rally se disparo x4,5.")
    ult = CRUDO.t.max().normalize() - pd.Timedelta("9D")   # dias ENTEROS, no 9x24h
    for d_, g in CRUDO[CRUDO.t >= ult].groupby(CRUDO.t.dt.date):
        h = (g.t.max() - pd.Timestamp(d_, tz="UTC")).total_seconds() / 3600 + 0.1
        parcial = "  (dia parcial)" if h < 23 else ""
        print(f"    {d_}  {len(g):4d} alertas{parcial}")
    for reg, g in CRUDO.assign(reg=np.where(CRUDO.t >= cut, "BULL", "BEAR")).groupby("reg"):
        d = (g.t.max() - g.t.min()).total_seconds() / 86400
        print(f"  {reg}: {len(g)/max(d, 1e-9):.1f}/dia sobre {len(g):,} alertas")
    print("\n### composicion por senal (% de cada tramo)")
    print((pd.crosstab(D.signal_type, D.reg, normalize="columns") * 100).round(1).to_string())
    print()

    activas = D.signal_type != "FADING"
    tabla(D, "TODAS las senales (FADING incluido; esta APAGADO en produccion)", "entry_price", a.costo)
    tabla(D[activas], "SOLO activas en produccion (sin FADING)", "entry_price", a.costo)
    tabla(D[activas], "SOLO activas, fill realista 15m despues de la alerta", "price_15m", a.costo)

    print("### por senal — 24h neta desde entry_price")
    piv = D.assign(r=ret(D, "24h", "entry_price", a.costo)).pivot_table(
        index="signal_type", columns="reg", values="r", aggfunc=["mean", "median", "size"])
    esc = np.where(np.isin(piv.columns.get_level_values(0), ["mean", "median"]), 100, 1)
    print((piv * esc).round(2).to_string())

    print("\n### concentracion (activas, 24h) — todo promedio positivo se rechequea sin el top-3")
    for reg in ["BEAR", "BULL"]:
        g = D[activas & (D.reg == reg)].copy()
        g["r"] = ret(g, "24h", "entry_price", a.costo)
        g = g.dropna(subset=["r"])
        if len(g) < 20:
            continue
        ap_ = g.groupby("symbol").r.sum().sort_values()
        sin3 = g[~g.symbol.isin(ap_.tail(3).index)].r
        print(f"  {reg}: media {100*g.r.mean():+7.3f}%   sin top-3 {100*sin3.mean():+7.3f}%   "
              f"top-3 {list(ap_.tail(3).index)}   n={len(g)}")

    print("\n### por dia (activas, 24h neta) — mirar la dispersion antes de creerle a la media")
    for d_, g in D[activas].groupby(D.t.dt.date):
        r = ret(g, "24h", "entry_price", a.costo).dropna()
        if len(r) < 10:
            continue
        marca = " <-- BULL" if pd.Timestamp(d_, tz="UTC") >= cut else ""
        print(f"  {d_}  n={len(r):4d}  media {100*r.mean():+7.3f}%  mediana {100*r.median():+7.3f}%{marca}")

    print("\n" + "=" * 84)
    print("FALTA EL CONTROL: correr `py -3.13 dt_diag_pareado.py --desde " + a.desde + "`.")
    print("Sin el, un numero positivo en tramo alcista no distingue habilidad de beta.")
    print("=" * 84)


if __name__ == "__main__":
    main()
