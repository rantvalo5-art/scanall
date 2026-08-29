"""
CORRIDA 12 — patrones de GRAFICO. La decima familia, la unica que el repo nunca midio.

Preregistro con la regla de parada: `PREREGISTRO_GRAFICOS.md`, con los parametros del
detector fijados ANTES de estimar un solo efecto.

    py -3.13 -u correr_graficos.py --tf 1h    # primaria
    py -3.13 -u correr_graficos.py --tf 1d

La compuerta de potencia (corrida 11, `potencia_graficos.py`) ya dijo que se puede medir:
la frontera esta en 0,20% de tasa de disparo a 1d y 0,05% a 1h, y los cinco patrones
disparan arriba de eso a 1h. Asi que si esto da cero, es un CERO MEDIDO, no un
"no se pudo" — y esa distincion es la que las corridas 8 y 9 dejaron escrito que hay que
hacer explicita.

Reusa el estimador de `correr_velas.py` tal cual, que es lo que el handoff dice que hay
que hacer: control POR BARRA, bloques semanales, sin_top3, FDR sobre el lote entero.
Lo unico distinto son las MASCARAS.
"""
import argparse
import json
import os
import sys
import time

import numpy as np
import pandas as pd

import graficos
from correr_velas import (COSTOS, FUERA, INICIO, FIN, PARAMS, Q_FDR, SEM_MIN,
                          N_MIN_BARRAS, evaluar)
from klines import CACHE, load_panel
from ranking import MIN_SYMS, _bh


def tablero(panel, tf, horizonte, semilla=0):
    """Una fila por (simbolo, barra) con el retorno futuro normalizado, mas las mascaras.

    Identico a `tablero_eventos` de la corrida 7 salvo el detector: aca son patrones de
    grafico, y ademas se calculan LOS CONTROLES que dependen de la serie (ruptura simple
    y pivotes barajados), que no se pueden armar despues porque son por simbolo.
    """
    P = PARAMS[tf]
    rng = np.random.default_rng(semilla)
    piezas = []
    for sym, df in panel.items():
        c = df["c"].to_numpy(float)
        h = df["h"].to_numpy(float)
        l = df["l"].to_numpy(float)
        n = len(c)
        if n < P["warmup"] + horizonte + 1:
            continue
        atr = pd.Series((h - l) / c).rolling(P["atr"]).mean()
        base = atr.rolling(P["base"], min_periods=P["base"] // 4).median().to_numpy()
        ret = np.full(n, np.nan)
        ret[:n - horizonte] = c[horizonte:] / c[:n - horizonte] - 1.0

        d = pd.DataFrame({"sym": sym, "t": df["t"].to_numpy(),
                          "atr_base": base, "ret": ret})
        for k, m in graficos.patrones(df).items():
            d[k] = m.to_numpy()
        for k, m in graficos.patrones_barajados(df, rng).items():
            d[f"BARAJADO {k}"] = m.to_numpy()

        # CTRL ruptura simple: sin pivotes, sin tolerancias, sin estructura.
        # `shift(1)` para que el extremo sea el de las barras ANTERIORES a la ruptura.
        piso = pd.Series(l).rolling(graficos.MAX_SEP).min().shift(1).to_numpy()
        techo = pd.Series(h).rolling(graficos.MAX_SEP).max().shift(1).to_numpy()
        cprev = np.concatenate([[np.nan], c[:-1]])
        d["CTRL ruptura abajo"] = (c < piso) & (cprev >= piso)
        d["CTRL ruptura arriba"] = (c > techo) & (cprev <= techo)

        d.loc[d.index[:P["warmup"]], "atr_base"] = np.nan
        piezas.append(d)

    TB = pd.concat(piezas, ignore_index=True)
    TB = TB[TB["ret"].notna() & TB["atr_base"].gt(0)]
    TB["y"] = TB["ret"] / TB["atr_base"]
    TB["dt"] = pd.to_datetime(TB["t"], unit="ms", utc=True)
    TB["semana"] = TB["dt"].dt.strftime("%G-W%V")
    vivos = TB.groupby("t")["y"].transform("count")
    TB = TB[vivos >= MIN_SYMS].reset_index(drop=True)
    print(f"  tablero {tf} H={horizonte}: {len(TB):,} filas | {TB['t'].nunique():,} barras "
          f"| {TB['sym'].nunique()} pares | {TB['semana'].nunique()} semanas")
    return TB


def brazos(TB, semilla=0):
    B = {}
    for k in graficos.NOMBRES:
        B[k] = TB[k].astype(bool)
        B[f"BARAJADO {k}"] = TB[f"BARAJADO {k}"].astype(bool)
    B["CTRL ruptura abajo"] = TB["CTRL ruptura abajo"].astype(bool)
    B["CTRL ruptura arriba"] = TB["CTRL ruptura arriba"].astype(bool)
    rng = np.random.default_rng(semilla)
    tasa = float(np.mean([TB[k].mean() for k in graficos.NOMBRES]))
    for i in range(3):
        B[f"CONTROL azar {i+1}"] = pd.Series(rng.random(len(TB)) < tasa, index=TB.index)
    return B


def main():
    ap = argparse.ArgumentParser(description="Banco — corrida 12: patrones de grafico")
    ap.add_argument("--tf", default="1h", choices=["1d", "1h"])
    ap.add_argument("--workers", type=int, default=12)
    ap.add_argument("--out", default=None)
    a = ap.parse_args()

    with open(os.path.join(CACHE, "universo_base200.json"), encoding="utf-8") as f:
        syms = [s for s in json.load(f) if s not in FUERA]
    print(f"universo: {len(syms)} pares (base200 menos {len(FUERA)} que no son cripto)")
    mb = 400 if a.tf == "1d" else 8000
    panel = load_panel(INICIO, FIN, tf=a.tf, full=True, workers=a.workers,
                       syms=syms, min_bars=mb)
    if not panel:
        print("FATAL: panel vacio")
        sys.exit(1)

    t0 = time.time()
    filas, tasas = [], None
    for H in PARAMS[a.tf]["horizontes"]:
        TB = tablero(panel, a.tf, H)
        B = brazos(TB)
        if tasas is None:
            tasas = {k: float(TB[k].mean()) for k in graficos.NOMBRES}
            print(f"\n  tasas de disparo a {a.tf} (la compuerta de la corrida 11 pedia "
                  f">= {'0,20%' if a.tf == '1d' else '0,05%'}):")
            for k, v in sorted(tasas.items(), key=lambda x: -x[1]):
                print(f"    {k:<14}{v:>9.3%}{int(TB[k].sum()):>10,} disparos")
            print()
        for costo in COSTOS:
            for nom, m in B.items():
                objetivos = ("largo", "corto")
                for obj in objetivos:
                    r = evaluar(TB, m, nom, obj, costo)
                    r.update(tf=a.tf, horizonte=H, costo=costo)
                    filas.append(r)
            print(f"    H={H} costo {costo:.2f} listo ({time.time()-t0:.0f}s)", flush=True)

    D = pd.DataFrame(filas)
    vivas = D["exceso"].notna()
    D["fdr_ok"] = False
    if vivas.any():
        D.loc[vivas, "fdr_ok"] = _bh(D.loc[vivas, "p"].to_numpy(), Q_FDR)

    # El MDE crece con el HORIZONTE: uno solo para todos seria muy laxo en el corto y
    # muy estricto en el largo. Se calcula por horizonte, siempre con la nula real.
    ctrl = D[D["patron"].str.startswith("CONTROL azar") & D["exceso"].notna()]
    MDE = {}
    print("\nMDE del azar (80% de potencia), POR HORIZONTE:")
    for H, g in ctrl.groupby("horizonte"):
        MDE[H] = 2.80 * float(g["sd_sem"].median()) / np.sqrt(float(g["semanas"].median()))
        print(f"  H={int(H):<3} +-{MDE[H]:.4f} ATR")
    mde = float(np.median(list(MDE.values())))

    # DIRECCION DECLARADA: solo cuenta el objetivo que el patron afirma
    def declarada(r):
        d = graficos.DIRECCION.get(r["patron"].replace("BARAJADO ", ""))
        return d is None or d == r["objetivo"]

    D["declarada"] = D.apply(declarada, axis=1)

    # el mejor barajado y la mejor ruptura simple, para la compuerta de estructura
    baraj = (D[D["patron"].str.startswith("BARAJADO")]
             .groupby(["horizonte", "costo", "objetivo"])["exceso"].max())
    rupt = (D[D["patron"].str.startswith("CTRL ruptura")]
            .groupby(["horizonte", "costo", "objetivo"])["exceso"].max())

    def sobrevive(r):
        if r["patron"].startswith(("CONTROL", "CTRL", "BARAJADO")):
            return False
        if not r["declarada"] or pd.isna(r["exceso"]):
            return False
        k = (r["horizonte"], r["costo"], r["objetivo"])
        return bool(r["exceso"] > MDE.get(r["horizonte"], mde)
                    and r["fdr_ok"] and r["exceso"] > 0
                    and np.sign(r["crudo"]) == np.sign(r["exceso"])
                    and not pd.isna(r["sin_top3"])
                    and np.sign(r["sin_top3"]) == np.sign(r["exceso"])
                    and r["exceso"] > baraj.get(k, -np.inf)
                    and r["exceso"] > rupt.get(k, -np.inf))

    D["sobrevive"] = D.apply(sobrevive, axis=1)

    out = a.out or f"rank_graficos_{a.tf}.csv"
    D.sort_values("exceso", ascending=False).to_csv(out, index=False)

    print("\n" + "=" * 92)
    print(f"CORRIDA 12 — PATRONES DE GRAFICO ({a.tf})")
    print("=" * 92)
    reales = D[~D["patron"].str.startswith(("CONTROL", "CTRL", "BARAJADO"))]
    print(f"  brazos totales {len(D)}   de patron real {len(reales)}   "
          f"de los cuales en la direccion declarada {int(reales['declarada'].sum())}")
    print(f"  no se pudo medir: {int(D['exceso'].isna().sum())} brazos "
          f"(< {N_MIN_BARRAS} disparos o < {SEM_MIN} semanas)")
    print(f"  SOBREVIVEN: {int(D['sobrevive'].sum())}")

    print(f"\n  los 12 mejores brazos de patron real, en la direccion declarada:")
    print(f"  {'patron':<16}{'obj':<7}{'H':>3}{'cost':>6}{'disp':>8}{'sem':>6}"
          f"{'exceso':>9}{'MDE(H)':>9}{'sin_top3':>10}{'p':>9}{'FDR':>5}")
    top = reales[reales.declarada & reales.exceso.notna()].nlargest(12, "exceso")
    for _, r in top.iterrows():
        print(f"  {r['patron']:<16}{r['objetivo']:<7}{int(r['horizonte']):>3}"
              f"{r['costo']:>6.2f}{int(r['disparos']):>8,}{int(r['semanas']):>6}"
              f"{r['exceso']:>9.4f}{MDE.get(r['horizonte'], mde):>9.4f}"
              f"{r['sin_top3']:>10.4f}{r['p']:>9.4f}"
              f"{'si' if r['fdr_ok'] else 'no':>5}")

    print(f"\n  LOS CONTROLES (el mejor de cada tipo, por objetivo):")
    for pref in ("BARAJADO", "CTRL ruptura", "CONTROL azar"):
        g = D[D["patron"].str.startswith(pref) & D["exceso"].notna()]
        if len(g):
            b = g.nlargest(1, "exceso").iloc[0]
            print(f"    {pref:<14} mejor exceso {b['exceso']:+.4f}  "
                  f"({b['patron']}, {b['objetivo']}, H={int(b['horizonte'])})")

    print("\n" + "=" * 92)
    if int(D["sobrevive"].sum()) == 0:
        print("  VEREDICTO: CERO. Y es un CERO MEDIDO, no un 'no se pudo medir':")
        print("  la compuerta de la corrida 11 establecio la potencia ANTES, y el MDE")
        print("  de esta corrida lo confirma con la nula real: "
              + ", ".join(f"H={int(h)} +-{v:.4f}" for h, v in sorted(MDE.items())))
    else:
        print(f"  {int(D['sobrevive'].sum())} brazos sobreviven TODAS las compuertas.")
        print("  Revisar uno por uno antes de creerlo.")
    print(f"  -> {out}   ({time.time()-t0:.0f}s)")
    print("=" * 92)
    return 0


if __name__ == "__main__":
    sys.exit(main())
