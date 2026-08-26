"""
CONTROL PAREADO DEL DAY TRADER — cada alerta contra la MISMA moneda a una hora al
azar de la MISMA ventana. Es el unico control que separa "eligio bien el momento" de
"subio todo el mercado".

POR QUE ESTE CONTROL Y NO OTRO. La linea base de azar global (cualquier moneda,
cualquier hora) mezcla tres cosas: la seleccion de moneda, el timing y la deriva del
mercado. El screener solo elige dos, y en un tramo alcista la deriva domina todo. Al
sortear horas del MISMO simbolo dentro de la MISMA ventana, la deriva y la eleccion de
moneda se cancelan por construccion y queda el timing solo, que es lo unico que el bot
decide. Ademas es inmune al sesgo de universo del backtest local.

    py -3.13 dt_diag_pareado.py --desde 2026-08-17
    py -3.13 dt_diag_pareado.py --desde 2026-06-26 --sorteos 20 --out pareado.csv

LO QUE NO HACE: no da veredicto con menos de 4 semanas. La unidad independiente de
este repo es la SEMANA, no la alerta — remuestrear alertas da un IC que no cruza cero
cuando el correcto si lo cruza, y asi ya murieron dos hipotesis hermanas. Con pocas
semanas imprime la dispersion diaria y se calla.

Primera corrida (2026-08-22, tramo alcista 08-17 -> 08-20, 204 alertas activas):
alerta +4,03% a 24h contra dardo +5,41% = **-1,38pp**, ganandole al dardo solo el
32,4% de las veces. El +4% era beta entero. A 4h, -0,38pp.

OJO CON LA SEMILLA: esa misma corrida con --sorteos 10 (el metodo del item 4.1) daba
-1,07pp o -1,49pp segun la semilla. Sobre un efecto de ~1,4pp eso es un tercio del
numero saliendo del generador de azar. Por eso el default es exacto.
"""
import argparse
import os

import numpy as np
import pandas as pd

from dt_diag_regimen import COSTO, cargar, corte_madurez

HERE = os.path.dirname(os.path.abspath(__file__))
CSV = os.path.join(HERE, "archivo_outcomes", "daytrader_outcomes.csv")
HORIZONTES = {"4h": pd.Timedelta("4h"), "24h": pd.Timedelta("24h")}
RNG = np.random.default_rng(7)


def velas(syms, ini, fin, margen=pd.Timedelta("2D")):
    """1h por simbolo, cacheadas en banco/.kline_cache. Devuelve {sym: Serie[cierre]}."""
    from banco.klines import klines
    s_ms = int((ini - margen).timestamp() * 1000)
    e_ms = int((fin + margen).timestamp() * 1000)
    px = {}
    for i, s in enumerate(syms):
        k = klines(s, s_ms, e_ms, "1h")
        if k is not None and len(k) > 24:
            k = k.copy()
            k["ts"] = pd.to_datetime(k["t"], unit="ms", utc=True)
            px[s] = k.set_index("ts")["c"].astype(float)
        if (i + 1) % 50 == 0:
            print(f"    velas {i+1}/{len(syms)}", flush=True)
    return px


def fwd(serie, t0, h, costo):
    p0, p1 = serie.asof(t0), serie.asof(t0 + h)
    if pd.isna(p0) or pd.isna(p1) or not p0:
        return np.nan
    return (p1 / p0 - 1) - costo


def p_semanas(d, col="delta", reps=8000):
    """IC y p con LA SEMANA como unidad: cada semana pesa igual, se remuestrean enteras.

    Poolear las alertas de cada bloque hace pesar mas a las semanas con mas alertas y
    subestima la variabilidad. Devuelve None si no hay al menos 4 semanas.
    """
    wm = np.array([g[col].mean() for _, g in d.groupby("week", sort=True)])
    if len(wm) < 4:
        return None
    m = np.array([RNG.choice(wm, len(wm), replace=True).mean() for _ in range(reps)])
    return float((m <= 0).mean()), tuple(np.percentile(m, [2.5, 97.5])), len(wm)


def bloque(d, titulo):
    if len(d) < 20:
        return
    print(f"### {titulo}")
    print("   h       n     alerta      dardo      DELTA    %le_gana   delta_mediana")
    for h in HORIZONTES:
        g = d[d.h == h]
        if len(g) < 20:
            continue
        print(f"  {h:4s} {len(g):6d}  {100*g.alerta.mean():+8.3f}%  {100*g.base.mean():+8.3f}%  "
              f"{100*g.delta.mean():+8.2f}pp   {100*(g.delta>0).mean():5.1f}%   "
              f"{100*g.delta.median():+8.2f}pp")
    print()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--csv", default=CSV)
    ap.add_argument("--desde", default="2026-08-17")
    ap.add_argument("--hasta", default=None, help="por defecto, el corte de madurez")
    ap.add_argument("--sorteos", type=int, default=0,
                    help="0 = EXACTO (promedia todas las horas). >0 = Monte Carlo, "
                         "que es como se hizo en el item 4.1 pero depende de la semilla")
    ap.add_argument("--costo", type=float, default=COSTO)
    ap.add_argument("--out", default=None, help="CSV con un renglon por par alerta/dardo")
    a = ap.parse_args()

    D = cargar(a.csv)
    corte, _ = corte_madurez(D)
    ini = pd.Timestamp(a.desde, tz="UTC")
    fin = pd.Timestamp(a.hasta, tz="UTC") if a.hasta else corte
    B = D[(D.t >= ini) & (D.t < fin)].copy()
    if B.empty:
        raise SystemExit("ventana vacia")

    print("=" * 84)
    print("CONTROL PAREADO — mismo simbolo, hora al azar, misma ventana")
    print("=" * 84)
    print(f"ventana   {ini:%Y-%m-%d %H:%M} -> {fin:%Y-%m-%d %H:%M} UTC "
          f"({(fin-ini).total_seconds()/86400:.2f} dias)")
    modo = "EXACTO (todas las horas)" if a.sorteos <= 0 else f"{a.sorteos} sorteos por alerta"
    print(f"alertas   {len(B):,} | {B.symbol.nunique()} simbolos | dardo: {modo}")
    print(f"costo     {100*a.costo:.2f}% ida y vuelta\n")

    px = velas(sorted(B.symbol.unique()), ini, fin)
    print(f"  con velas: {len(px)}/{B.symbol.nunique()} simbolos\n")

    # Las horas candidatas son el MISMO rango que las alertas: el forward de ambos se
    # va mas alla del fin de ventana, y eso esta bien mientras sea simetrico.
    horas = pd.date_range(ini, fin - pd.Timedelta("1h"), freq="1h", tz="UTC")

    # El dardo depende SOLO del simbolo y del horizonte, no de la alerta: se calcula
    # una vez por simbolo. Con --sorteos 0 se promedian TODAS las horas de la ventana,
    # que es el limite del Monte Carlo y no depende de la semilla; con 10 sorteos el
    # delta se movia ~0,4pp de una semilla a otra, sobre un efecto de ~1,5pp.
    dardo = {}
    for sym, serie in px.items():
        hs = horas if a.sorteos <= 0 else [pd.Timestamp(d) for d in
                                           RNG.choice(horas, a.sorteos, replace=True)]
        for lbl, h in HORIZONTES.items():
            v = np.nanmean([fwd(serie, d, h, a.costo) for d in hs])
            if not np.isnan(v):
                dardo[(sym, lbl)] = v

    rows = []
    for _, x in B.iterrows():
        if x.symbol not in px or pd.isna(x.entry_price):
            continue
        for lbl, h in HORIZONTES.items():
            real = x[f"price_{lbl}"]
            base = dardo.get((x.symbol, lbl))
            if pd.isna(real) or base is None:
                continue
            r = (real / x.entry_price - 1) - a.costo
            rows.append(dict(sym=x.symbol, t=x.t, dia=x.t.date(), sig=x.signal_type,
                             bucket=x.bucket, h=lbl, alerta=r, base=base, delta=r - base))
    R = pd.DataFrame(rows)
    if R.empty:
        raise SystemExit("no se pudo construir ningun par")
    R["week"] = R.t.dt.tz_localize(None).dt.to_period("W")
    print(f"pares construidos: {len(R):,}\n")

    bloque(R, "TODAS las senales")
    bloque(R[R.sig != "FADING"], "SOLO activas en produccion (sin FADING)")

    print("### por senal — delta = alerta menos dardo")
    print("  senal        n_4h   delta_4h    n_24h  delta_24h   mediana_24h")
    for s, g in R.groupby("sig"):
        g4, g24 = g[g.h == "4h"], g[g.h == "24h"]
        if len(g24) < 10:
            continue
        print(f"  {s:11s} {len(g4):5d}  {100*g4.delta.mean():+8.2f}pp  {len(g24):6d}  "
              f"{100*g24.delta.mean():+8.2f}pp   {100*g24.delta.median():+8.2f}pp")

    g = R[(R.h == "24h") & (R.sig != "FADING")]
    if len(g) >= 20:
        ap_ = g.groupby("sym").delta.sum().sort_values()
        sin3 = g[~g.sym.isin(ap_.tail(3).index)].delta
        print("\n### robustez del delta 24h (activas)")
        print(f"  media {100*g.delta.mean():+.2f}pp | sin top-3 {100*sin3.mean():+.2f}pp "
              f"(top-3 {list(ap_.tail(3).index)})")
        r = p_semanas(g)
        if r is None:
            print(f"  SIN VEREDICTO: {g.week.nunique()} semana(s) / {g.dia.nunique()} dias. "
                  f"La unidad de este repo es la SEMANA y hacen falta >=4.")
            print("  dispersion diaria (es el ruido contra el que compite la media):")
            for d_, gg in g.groupby("dia"):
                print(f"    {d_}  n={len(gg):4d}  delta {100*gg.delta.mean():+8.2f}pp")
        else:
            p, ic, k = r
            print(f"  semanas={k}  p={p:.4f}  IC95 [{100*ic[0]:+.2f}, {100*ic[1]:+.2f}]pp")
            for w, gg in g.groupby("week"):
                print(f"    {w}  n={len(gg):4d}  delta {100*gg.delta.mean():+8.2f}pp")

    if a.out:
        p = a.out if os.path.isabs(a.out) else os.path.join(HERE, a.out)
        R.to_csv(p, index=False)
        print(f"\nguardado {p}")


if __name__ == "__main__":
    main()
