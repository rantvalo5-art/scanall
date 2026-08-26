"""
TEST UNLOCKS — desbloqueos como shock de oferta. Ver `PREREGISTRO_UNLOCKS.md`.

DISENO. No se arma una tabla de eventos aparte: se genera la tabla de primer toque NORMAL
sobre las monedas que tienen calendario de vesting, y se **marca** la entrada que cae justo
despues de cada desbloqueo. Asi `wr_pareado()` compara cada evento contra entradas de la
MISMA moneda en otros momentos, sin escribir un control nuevo — y ese control es la
compuerta primaria del preregistro, porque las monedas con vesting son sistematicamente
distintas (alts nuevas, FDV alto, float bajo) y sin el se estaria midiendo la muestra, no
el desbloqueo.

    py -3.13 test_unlocks.py --pilot     # pocos pares, para validar el cableado
    py -3.13 test_unlocks.py             # corrida 1: buckets de dosis (NO adjudicable)
    py -3.13 test_unlocks.py --tendencia # corrida 2: dosis-respuesta continua
"""
import argparse
import json
import os

import numpy as np
import pandas as pd

from klines import CACHE, load_panel
from lote import lote
from primer_toque import tabla, winrate_necesario
from unlocks import tabla_eventos

HERE = os.path.dirname(os.path.abspath(__file__))
MS_H = 3600000

# Buckets de dosis — declarados en el preregistro, no se tocan.
BUCKETS = [(0.005, 0.01), (0.01, 0.02), (0.02, 0.05), (0.05, 0.10), (0.10, 1e9)]
CATS = ["insiders", "privateSale", "noncirculating", "ecosystem", "farming", "airdrop"]
PCT_MIN = 0.005          # "grande" = >= 0,5% del circulante


def eventos_agregados(verbose=True):
    """Unlocks cliff, sumados por (simbolo, dia).

    Si tres categorias desbloquean el mismo dia es UN shock, no tres: contarlos por
    separado lo triplicaria en la muestra y en la concentracion.
    """
    E = tabla_eventos(verbose=verbose)
    E = E[(E.tipo == "cliff") & E.pct.notna() & (E.tokens > 0)].copy()
    E["dia"] = E.fecha.dt.floor("D")

    g = E.groupby(["sym", "dia"], as_index=False).agg(
        tokens=("tokens", "sum"), circulante=("circulante", "first"),
        cat=("cat", lambda s: s.iloc[np.argmax(E.loc[s.index, "tokens"].to_numpy())]),
        n_cat=("cat", "size"))
    g["pct"] = g.tokens / g.circulante.replace(0, np.nan)
    g["t"] = (g.dia.astype("int64") // 10**6)
    if verbose:
        print(f"\neventos agregados por (sym,dia): {len(g):,}  "
              f"(de {len(E):,} filas crudas)")
    return g.dropna(subset=["pct"])


def pin_universo(syms, nombre="unlocks"):
    """load_panel() solo acepta universos pineados; se escribe el pin a mano."""
    os.makedirs(CACHE, exist_ok=True)
    p = os.path.join(CACHE, f"universo_{nombre}.json")
    json.dump(sorted(syms), open(p, "w", encoding="utf-8"))
    return nombre


def marcar(T, G, ventana_h=12):
    """Para cada entrada de T, el desbloqueo que ocurrio en (t - ventana_h, t].

    El paso de `tabla()` es 12h, asi que cada evento marca a lo sumo UNA entrada: la
    primera que se puede tomar despues del desbloqueo.
    """
    out = pd.Series(np.nan, index=T.index)
    cat = pd.Series(None, index=T.index, dtype=object)
    w = ventana_h * MS_H
    for sym, ev in G.groupby("sym"):
        m = T.sym == sym
        if not m.any():
            continue
        te = np.sort(ev.t.to_numpy())
        orden = np.argsort(ev.t.to_numpy())
        pc = ev.pct.to_numpy()[orden]
        cc = ev.cat.to_numpy()[orden]
        tt = T.t[m].to_numpy()
        j = np.searchsorted(te, tt, side="right") - 1      # ultimo evento <= t
        ok = (j >= 0) & (tt - te[np.clip(j, 0, None)] < w)
        vals = np.where(ok, pc[np.clip(j, 0, None)], np.nan)
        cats = np.where(ok, cc[np.clip(j, 0, None)], None)
        out.loc[m] = vals
        cat.loc[m] = cats
    return out, cat


def hipotesis(T, pct, cat):
    """Los buckets de dosis y los cortes por categoria del preregistro."""
    H = {}
    grande = pct >= PCT_MIN
    H["desbloqueo >=0,5% (todos)"] = grande
    for lo, hi in BUCKETS:
        et = f"{lo:.1%}-{hi:.0%}" if hi < 1 else f">={lo:.0%}"
        H[f"dosis {et}"] = (pct >= lo) & (pct < hi)
    for c in CATS:
        m = grande & (cat == c)
        if m.sum() >= 50:
            H[f"cat {c} (>=0,5%)"] = m
    return H


# ==================================================================== tendencia
# Corrida 2. Ver `PREREGISTRO_UNLOCKS_2.md` — que NO es ciego, y lo dice en el encabezado.
#
# La corrida 1 murio de planificacion: la regla se apoyaba en el bucket >=10%, que
# post-join quedo con n=143. Cortar 1.040 eventos en cinco buckets deja ~200 por bucket.
# Un test de tendencia estima UN parametro con toda la muestra, que es la unica salida
# que no pasa por re-cortar los buckets (prohibido).

WINSOR = (1, 99)         # percentiles; preregistrado, no se toca despues de ver beta
PERM_REPS = 3000
BOOT_REPS = 2000


def _beta(y, x):
    """Pendiente OLS de y sobre x. Se imprime x100 = pp de win rate por decada."""
    xc = x - x.mean()
    d = float((xc * xc).sum())
    return float((xc * (y - y.mean())).sum() / d) if d > 0 else np.nan


def preparar(T, G, ventana=12, corte=PCT_MIN, dias=0):
    """La tabla de eventos lista para regresar: y pareado por simbolo, x = log10(dosis).

    `y_i = win_i - p_base(sym_i)`, con p_base sacado de las entradas de esa MISMA moneda
    que no son evento. Es el control pareado del preregistro 1 hecho por observacion en
    vez de por agregado: sin el, esto mide "las alts con vesting bajan", que es un hecho
    de la muestra y no del desbloqueo.

    `dias` corre los eventos en el tiempo — el placebo (-30) cae donde no hubo desbloqueo.
    """
    g = G[G.pct >= corte].copy()
    if dias:
        g["t"] = g["t"] + dias * 24 * MS_H
    pct, cat = marcar(T, g, ventana)

    R = T[T["resuelto"]].copy()
    R["pct"] = pct.reindex(R.index)
    R["cat"] = cat.reindex(R.index)
    R["win"] = (R["res"] > 0).astype(float)
    ev = R["pct"].notna()

    # la linea base EXCLUYE los eventos: si no, el control se contamina con lo medido
    base = R[~ev].groupby("sym")["win"].mean()
    E = R[ev].copy()
    E["y"] = E["win"] - E["sym"].map(base)
    E = E.dropna(subset=["y"])

    xr = np.log10(E["pct"].to_numpy())
    lo, hi = np.percentile(xr, WINSOR)
    E["x_crudo"] = xr
    E["x"] = np.clip(xr, lo, hi)
    E["q"] = E["dt"].dt.year.astype(str) + "Q" + E["dt"].dt.quarter.astype(str)
    E["era"] = np.where(E["dt"].dt.year <= 2023, "2021-2023", "2024-2026")
    E.attrs.update(winsor=(10 ** lo, 10 ** hi),
                   n_fuera=int(((xr < lo) | (xr > hi)).sum()))
    return E


def _p_permutacion(E, obs, reps=PERM_REPS, seed=0):
    """p two-sided permutando la DOSIS dentro de cada simbolo.

    Rompe dosis->resultado conservando el nivel de cada moneda y su estructura temporal.
    Es la nula correcta para una pregunta de dosis; el p de OLS supone entradas
    independientes y aca se solapan y hay hasta 56 eventos del mismo par.
    """
    rng = np.random.default_rng(seed)
    y, x, s = E["y"].to_numpy(), E["x"].to_numpy(), E["sym"].to_numpy()
    grupos = [np.flatnonzero(s == k) for k in np.unique(s)]
    nulo = np.empty(reps)
    for r in range(reps):
        xp = x.copy()
        for ii in grupos:
            xp[ii] = rng.permutation(x[ii])
        nulo[r] = _beta(y, xp)
    return float((np.abs(nulo) >= abs(obs)).mean()), nulo


def _ic_bootstrap(E, col, reps=BOOT_REPS, seed=1):
    """IC 95% remuestreando CLUSTERES enteros de `col` (simbolo o trimestre).

    El de simbolos ataca directo lo que mato la corrida 1: si el efecto vive en pocos
    nombres, este IC cruza cero. El trimestral reemplaza a la compuerta semanal de
    lote.py, inaplicable a eventos esparcidos (SEM_N_MIN=20 vs ~3,5 eventos/semana).
    """
    rng = np.random.default_rng(seed)
    y, x, k = E["y"].to_numpy(), E["x"].to_numpy(), E[col].to_numpy()
    idx = {v: np.flatnonzero(k == v) for v in np.unique(k)}
    claves = np.array(sorted(idx))
    out = np.empty(reps)
    for r in range(reps):
        ii = np.concatenate([idx[v]
                             for v in rng.choice(claves, len(claves), replace=True)])
        out[r] = _beta(y[ii], x[ii])
    return float(np.percentile(out, 2.5)), float(np.percentile(out, 97.5)), len(claves)


def _spearman(y, x):
    ry = pd.Series(y).rank().to_numpy()
    rx = pd.Series(x).rank().to_numpy()
    return float(np.corrcoef(ry, rx)[0, 1])


def tendencia(T, G, ventana=12, corte=PCT_MIN, etq="PRIMARIO", nec=None, dias=0,
              detalle=True):
    """El test del preregistro 2. Devuelve el dict que consume `veredicto()`."""
    E = preparar(T, G, ventana, corte, dias)
    lo, hi = E.attrs["winsor"]
    y, x = E["y"].to_numpy(), E["x"].to_numpy()
    b = _beta(y, x)
    rango = float(E.x.max() - E.x.min())

    print("\n" + "=" * 84)
    print(f"{etq}   corte >= {corte:.1%}   n={len(E):,}   simbolos={E.sym.nunique()}   "
          f"trimestres={E.q.nunique()}")
    print("=" * 84)
    print(f"  winsor [{lo:.2%}, {hi:.2%}] -> {E.attrs['n_fuera']} eventos recortados")
    print(f"\n  PENDIENTE   beta = {b*100:+.2f}pp de win rate por decada de dosis")
    print(f"              rango util {rango:.2f} decadas -> "
          f"{b*100*rango:+.1f}pp de punta a punta")

    p, nulo = _p_permutacion(E, b)
    print(f"\n  p permutacion intra-simbolo (two-sided)   {p:.4f}"
          f"        [sd nula {nulo.std()*100:.2f}pp]")
    ic_s = _ic_bootstrap(E, "sym", seed=1)
    ic_q = _ic_bootstrap(E, "q", seed=2)
    print(f"  IC95 bootstrap de simbolos ({ic_s[2]:>3} clusteres)   "
          f"[{ic_s[0]*100:+.2f} , {ic_s[1]*100:+.2f}]pp")
    print(f"  IC95 bootstrap trimestral  ({ic_q[2]:>3} clusteres)   "
          f"[{ic_q[0]*100:+.2f} , {ic_q[1]*100:+.2f}]pp")

    # concentracion: el modo de muerte de la corrida 1
    top = E.groupby("sym").size().sort_values(ascending=False)
    s3 = E[~E.sym.isin(top.head(3).index)]
    s1 = E[E.sym != top.index[0]]
    b3 = _beta(s3.y.to_numpy(), s3.x.to_numpy())
    b1 = _beta(s1.y.to_numpy(), s1.x.to_numpy())
    print(f"\n  sin top-3 ({', '.join(top.head(3).index)} = {top.head(3).sum()} ev)"
          f"   beta = {b3*100:+.2f}pp")
    print(f"  sin top-1 ({top.index[0]})   beta = {b1*100:+.2f}pp")

    eras = {}
    for k, g in E.groupby("era"):
        eras[k] = _beta(g.y.to_numpy(), g.x.to_numpy())
        print(f"  era {k} (n={len(g):,})   beta = {eras[k]*100:+.2f}pp")

    if detalle:
        print("\n  --- secundarios (declarados en la seccion 7, NO deciden) ---")
        print(f"  beta sin winsorizar        {_beta(y, E.x_crudo.to_numpy())*100:+.2f}pp")
        print(f"  Spearman(y, dosis)         {_spearman(y, E.x_crudo.to_numpy()):+.4f}")
        for c in CATS:
            g = E[E.cat == c]
            if len(g) >= 50:
                print(f"  cat {c:<15} n={len(g):4d}   beta = "
                      f"{_beta(g.y.to_numpy(), g.x.to_numpy())*100:+.2f}pp")

    if nec is not None:
        d = E[E.x >= E.x.quantile(0.9)]
        wr = d.win.mean() * 100
        print(f"\n  operable? decil superior de dosis (n={len(d)}, >= "
              f"{10**float(d.x.min()):.1%} del circulante): win rate {wr:.2f}% "
              f"vs necesario {nec:.2f}% -> {'CRUZA' if wr > nec else 'NO cruza'}")

    return dict(E=E, beta=b, p=p, ic_sym=ic_s[:2], ic_q=ic_q[:2],
                b_sin3=b3, b_sin1=b1, eras=eras)


def veredicto(res, plac):
    """La regla de parada del preregistro 2 (seccion 5), aplicada literalmente."""
    b = res["beta"]
    ok = {
        "1. p permutacion two-sided < 0,05": res["p"] < 0.05,
        "2. IC95 bootstrap de simbolos no contiene 0":
            res["ic_sym"][0] * res["ic_sym"][1] > 0,
        "3. IC95 bootstrap trimestral no contiene 0":
            res["ic_q"][0] * res["ic_q"][1] > 0,
        "4. el signo aguanta sin top-3 y sin top-1":
            np.sign(res["b_sin3"]) == np.sign(b) and np.sign(res["b_sin1"]) == np.sign(b),
        "5. el signo coincide en las dos epocas":
            len(res["eras"]) == 2 and len(set(np.sign(list(res["eras"].values())))) == 1,
        "6. el placebo -30d queda por debajo del real":
            abs(plac["beta"]) < abs(b),
    }
    print("\n" + "#" * 84)
    print("# REGLA DE PARADA — PREREGISTRO_UNLOCKS_2.md, seccion 5")
    print("#" * 84)
    for k, v in ok.items():
        print(f"  [{'OK' if v else '--'}]   {k}")
    vivo = all(ok.values())
    print(f"\n  ==> {'SOBREVIVE' if vivo else 'FAMILIA CERRADA'}")
    if not vivo:
        print("      Y esta vez NO es 'subpotenciado': con un MDE de 6,6pp por decada")
        print("      (seccion 6, calculado ANTES) el test podia ver un efecto de ese")
        print("      tamano o mayor. La corrida 1 no podia; esta si.")
    return vivo


def main():
    ap = argparse.ArgumentParser(description="Banco — unlocks como shock de oferta")
    ap.add_argument("--inicio", default="2021-01-01")
    ap.add_argument("--fin", default="2026-08-01")
    ap.add_argument("--target", type=float, default=8)
    ap.add_argument("--stop", type=float, default=8)
    ap.add_argument("--horizonte", type=int, default=14)   # preregistrado
    ap.add_argument("--paso", type=int, default=12)
    ap.add_argument("--ventana", type=int, default=12, help="horas post-evento")
    ap.add_argument("--pilot", action="store_true")
    ap.add_argument("--tendencia", action="store_true",
                    help="corrida 2: dosis-respuesta continua (PREREGISTRO_UNLOCKS_2.md)")
    ap.add_argument("--out", default=None)
    a = ap.parse_args()

    G_full = eventos_agregados()          # sin cortar: el secundario >=0,1% lo necesita
    G = G_full[G_full.pct >= PCT_MIN]
    syms = sorted(G.sym.unique())
    if a.pilot:
        syms = syms[:12]
        G = G[G.sym.isin(syms)]
    print(f"\n{len(G):,} eventos grandes sobre {len(syms)} pares")

    pin = pin_universo(syms, "unlocks_pilot" if a.pilot else "unlocks")
    panel = load_panel(a.inicio, a.fin, n=len(syms), pin=pin, min_bars=1500)
    if not panel:
        print("FATAL: panel vacio")
        return

    T = tabla(panel, a.target, a.stop, a.horizonte, a.paso)
    pct, cat = marcar(T, G, a.ventana)
    n_marc = int(pct.notna().sum())
    print(f"\n{len(T):,} entradas  |  {n_marc:,} marcadas por un desbloqueo")
    if n_marc < 100:
        print("FATAL: muy pocas entradas marcadas; revisar alineacion")
        return

    if a.tendencia:
        nec = winrate_necesario(a.target, a.stop)
        print(f"\nwin rate necesario: {nec:.2f}%")
        # el corte primario es el MISMO de la corrida 1; el ampliado es secundario y
        # se corre siempre, pase lo que pase, para que no se pueda elegir despues
        res = tendencia(T, G_full, a.ventana, PCT_MIN, "PRIMARIO", nec=nec)
        plac = tendencia(T, G_full, a.ventana, PCT_MIN,
                         "PLACEBO -30d (control negativo)", dias=-30, detalle=False)
        tendencia(T, G_full, a.ventana, 0.001,
                  "SECUNDARIO >=0,1% (NO decide)", detalle=False)
        veredicto(res, plac)
        if a.out:
            res["E"].to_csv(a.out, index=False)
            print(f"\neventos -> {a.out}")
        return

    H = hipotesis(T, pct, cat)
    print(f"{len(H)} hipotesis (dosis + categoria)\n")

    print("#" * 100)
    print("# CORTO — vender en el desbloqueo (la direccion que predice la hipotesis)")
    print("#" * 100)
    Tc = T.copy()
    Tc["res"] = -Tc["res"]
    Tc.attrs.update(T.attrs)
    D2 = lote(Tc, H)

    print("\n" + "#" * 100)
    print("# LARGO — el lado contrario, para no repetir el error de funding")
    print("#" * 100)
    D1 = lote(T, H)

    print("\n" + "=" * 100)
    print("ESCALERA DE DOSIS (la prediccion fuerte del preregistro)")
    print("=" * 100)
    def _vp(D, k):
        """vs_pareado de una hipotesis, o None si quedo subpotenciada (lote() no
        devuelve la columna cuando ninguna hipotesis llega a n>=200)."""
        if "vs_pareado" not in D.columns:
            return None
        r = D[D.hipotesis == k]
        if r.empty or pd.isna(r.vs_pareado.iloc[0]):
            return None
        return float(r.vs_pareado.iloc[0])

    print(f"{'bucket':<16}{'n':>7}{'corto vs par':>15}{'largo vs par':>15}")
    for lo, hi in BUCKETS:
        et = f"{lo:.1%}-{hi:.0%}" if hi < 1 else f">={lo:.0%}"
        k = f"dosis {et}"
        r2 = D2[D2.hipotesis == k]
        if r2.empty:
            continue
        n = int(r2.n.iloc[0])
        c, l = _vp(D2, k), _vp(D1, k)
        sc = f"{c:>+12.2f}pp" if c is not None else f"{'subpot.':>14}"
        sl = f"{l:>+12.2f}pp" if l is not None else f"{'subpot.':>14}"
        print(f"{et:<16}{n:>7,}{sc:>15}{sl:>15}")
    print("\nla regla de parada pide: bucket >=10% le gana al pareado en CORTO,")
    print("Y la escalera monotona en >=4 de 5. Un bucket suelto no cuenta.")

    if a.out:
        D2["lado"], D1["lado"] = "corto", "largo"
        pd.concat([D2, D1], ignore_index=True).to_csv(a.out, index=False)
        print(f"\ntabla -> {a.out}")


if __name__ == "__main__":
    main()
