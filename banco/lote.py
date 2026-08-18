"""
LOTE — probar muchas hipotesis de una, con la correccion que eso exige.

Por que existe: probar una idea por sesion es lento Y deshonesto. Lento porque
cada idea cuesta rehacer todo. Deshonesto porque una hipotesis mirada sola
siempre parece confirmatoria — el ojo ya recorrio veinte tablas antes de elegir
cual reportar. Este repo ya se comio esa: un spread de +6,6pp con p~1% que era
look-elsewhere sobre una tabla de ~30 celdas.

La solucion es la misma para los dos problemas: correr el lote ENTERO y
corregir por multiplicidad. Si de 40 hipotesis ninguna sobrevive, eso se sabe en
una corrida en vez de en 40 sesiones.

    py -3.13 lote.py                        # bateria estandar
    py -3.13 lote.py --cruces               # + cruces de a pares (mucho mas grande)
    py -3.13 lote.py --pares 300 --q 0.05   # mas universo, mas exigente

Para probar TUS hipotesis:

    from klines import load_panel
    from primer_toque import tabla
    from lote import features, lote

    panel = load_panel("2025-08-01", "2026-08-01", n=200)
    T = tabla(panel, target=8, stop=8, horizonte_d=30)
    F = features(panel, T)
    lote(T, {"mi idea": F.roc_168 > 0.3, "otra": F.dd_168 < -0.4})

Las compuertas estan CABLEADAS, no son sugerencias. El veredicto por default es
CERRADA: la carga de la prueba la tiene la senal, no el escepticismo.
"""
import argparse
import math
import sys

import numpy as np
import pandas as pd

from klines import load_panel
from primer_toque import COSTO_PCT, tabla, winrate_necesario

# ------------------------------------------------------------------ compuertas
# Cambiarlas es una decision de diseno, no un ajuste para que pase algo.
N_MIN = 200          # menos que esto es SUBPOTENCIADO, que no es lo mismo que refutado
Q_FDR = 0.10         # tasa de falsos descubrimientos tolerada en el lote
SEM_MIN = 0.60       # fraccion de semanas que tiene que estar arriba del umbral
TOP_N = 3            # cuantos simbolos se sacan en el chequeo de concentracion


# ------------------------------------------------------------------ features
def _feat_simbolo(df):
    """Features de un par, calculadas SOLO con pasado (posicion i mira hasta i)."""
    c = df["c"].to_numpy(float)
    h = df["h"].to_numpy(float)
    l = df["l"].to_numpy(float)
    n = len(c)
    out = {}

    def roc(k):
        r = np.full(n, np.nan)
        if n > k:
            r[k:] = c[k:] / c[:-k] - 1.0
        return r

    out["roc_24"] = roc(24)
    out["roc_72"] = roc(72)
    out["roc_168"] = roc(168)
    out["roc_720"] = roc(720)

    lr = np.diff(np.log(c), prepend=np.log(c[0]))
    s = pd.Series(lr)
    out["vol_24"] = s.rolling(24).std().to_numpy()
    out["vol_168"] = s.rolling(168).std().to_numpy()
    # compresion: volatilidad reciente contra la de fondo. <1 = comprimido.
    with np.errstate(invalid="ignore", divide="ignore"):
        out["compresion"] = out["vol_24"] / out["vol_168"]
        out["atr_24"] = (pd.Series(h - l).rolling(24).mean().to_numpy() / c)

    hi168 = pd.Series(h).rolling(168).max().to_numpy()
    lo168 = pd.Series(l).rolling(168).min().to_numpy()
    hi720 = pd.Series(h).rolling(720).max().to_numpy()
    with np.errstate(invalid="ignore", divide="ignore"):
        out["dd_168"] = c / hi168 - 1.0          # 0 = en maximos, negativo = abajo
        out["dd_720"] = c / hi720 - 1.0
        out["pos_168"] = (c - lo168) / (hi168 - lo168)   # donde esta dentro del rango
        out["rango_168"] = (hi168 - lo168) / c           # amplitud del rango
    return out


def features(panel, T, verbose=True):
    """Una fila por entrada de T, alineada por (sym, t). Todo mira solo al pasado.

    Incluye dos features de MERCADO (mediana del universo), porque el regimen
    domina el resultado y conviene poder gatearlo aunque ya se sepa que no
    generaliza — ver la familia de regimen, cerrada.
    """
    piezas = []
    for k, (sym, df) in enumerate(panel.items(), 1):
        f = _feat_simbolo(df)
        d = pd.DataFrame(f)
        d.insert(0, "t", df["t"].to_numpy())
        d.insert(0, "sym", sym)
        piezas.append(d)
        if verbose and k % 50 == 0:
            print(f"  features {k}/{len(panel)}...", flush=True)
    FULL = pd.concat(piezas, ignore_index=True)

    # mercado = mediana del universo, hora a hora
    mkt = FULL.groupby("t")[["roc_168", "vol_168"]].median()
    mkt.columns = ["mkt_168", "mkt_vol_168"]
    FULL = FULL.merge(mkt, left_on="t", right_index=True, how="left")
    FULL["rs_168"] = FULL["roc_168"] - FULL["mkt_168"]   # fuerza relativa

    F = T[["sym", "t"]].merge(FULL, on=["sym", "t"], how="left")
    F.index = T.index
    return F.drop(columns=["sym", "t"])


# ------------------------------------------------------------------ estadistica
def _p_binomial(k, n, p0):
    """p-valor de una cola suponiendo entradas INDEPENDIENTES.

    OJO: aca esa suposicion es falsa. Las entradas se solapan (una cada 12h con
    horizonte de 30d = ~60 trades vivos a la vez) y el regimen esta
    autocorrelacionado, asi que el n efectivo es mucho menor que el n contado.
    Este p-valor esta SIEMPRE inflado. Se reporta como referencia; el que manda
    es `_p_bloques`.
    """
    if n <= 0:
        return 1.0
    mu, sd = n * p0, math.sqrt(n * p0 * (1 - p0))
    if sd == 0:
        return 1.0
    z = (k - 0.5 - mu) / sd
    return 0.5 * math.erfc(z / math.sqrt(2))


def _p_bloques(S, nec, bloque=3, reps=2000, seed=0):
    """p-valor remuestreando SEMANAS ENTERAS, cada una pesando igual.

    La semana es la unidad independiente: las entradas se solapan (una cada 12h
    con horizonte de 30d = ~60 trades vivos a la vez) y el regimen esta
    autocorrelacionado.

    OJO — una version anterior remuestreaba bloques de semanas pero POOLEABA las
    entradas de cada bloque. Eso hace que las semanas con mas entradas pesen mas
    y, si hay pocas semanas, subestima la variabilidad. En `fade/evaluar.py`
    (8 semanas) la diferencia dio vuelta un veredicto: IC [+0,17, +2,32] contra
    el correcto [-0,52, +3,30]. Aca hay ~52 semanas y el sesgo es menor, pero se
    corrige igual: cada semana entra con su propio win rate, pesando uno.

    Una senal de timing de mercado suele tener p ingenuo ~0 y p de semanas alto:
    esa brecha es el autoengano que se quiere evitar.
    """
    wr = np.array([(g["res"] > 0).mean() * 100
                   for _, g in S.groupby("semana", sort=True)
                   if len(g) >= 20])
    k = len(wr)
    if k < 8:
        return 1.0
    rng = np.random.default_rng(seed)
    m = np.array([rng.choice(wr, k, replace=True).mean() for _ in range(reps)])
    return float((m <= nec).mean())


def _bh(ps, q):
    """Benjamini-Hochberg. Devuelve el vector de bool 'sobrevive la correccion'."""
    m = len(ps)
    orden = np.argsort(ps)
    ok = np.zeros(m, dtype=bool)
    corte = 0
    for rank, i in enumerate(orden, 1):
        if ps[i] <= rank / m * q:
            corte = rank
    for rank, i in enumerate(orden, 1):
        if rank <= corte:
            ok[i] = True
    return ok


def wr_pareado(T, mascara):
    """Win rate de la linea base RESTRINGIDA A LOS MISMOS SIMBOLOS y ponderada
    igual que la senal. Aisla el TIMING de la seleccion de moneda.

    Es el control que faltaba: si una senal solo elige los pares que iban a andar
    bien igual, contra la linea base global parece genial y contra esta, cero.
    """
    R = T[T["resuelto"]]
    sel = R[mascara.reindex(R.index, fill_value=False)]
    if sel.empty:
        return float("nan")
    peso = sel.groupby("sym").size()
    wr_sym = R[R["sym"].isin(peso.index)].groupby("sym")["res"].apply(lambda s: (s > 0).mean())
    comun = peso.index.intersection(wr_sym.index)
    if not len(comun):
        return float("nan")
    return float((wr_sym[comun] * peso[comun]).sum() / peso[comun].sum() * 100)


# ------------------------------------------------------------------ el lote
def _una(T, mascara, nombre, nec, costo):
    R = T[T["resuelto"]]
    m = mascara.reindex(R.index, fill_value=False).fillna(False).astype(bool)
    S = R[m]
    n = len(S)
    fila = {"hipotesis": nombre, "n": n}
    if n < N_MIN:
        fila.update(wr=np.nan, margen=np.nan, p=1.0, veredicto="POCA MUESTRA")
        return fila

    wins = int((S["res"] > 0).sum())
    wr = wins / n * 100
    fila["wr"] = wr
    fila["margen"] = wr - nec
    fila["p_indep"] = _p_binomial(wins, n, nec / 100)   # inflado, referencia
    fila["p"] = _p_bloques(S, nec) if wr > nec else 1.0  # el que decide

    # compuerta de seleccion-de-moneda
    fila["wr_pareado"] = wr_pareado(T, mascara)
    fila["vs_pareado"] = wr - fila["wr_pareado"]

    # compuerta de concentracion: sacar los TOP_N pares que mas aportan
    aporte = S.groupby("sym")["res"].sum().sort_values(ascending=False)
    sin = S[~S["sym"].isin(aporte.head(TOP_N).index)]
    fila["margen_sin_top3"] = ((sin["res"] > 0).mean() * 100 - nec) if len(sin) else np.nan
    # y sacar el UNICO mejor par (test de cola: un solo nombre no puede sostener nada)
    sin1 = S[S["sym"] != aporte.index[0]] if len(aporte) else S
    fila["margen_sin_top1"] = ((sin1["res"] > 0).mean() * 100 - nec) if len(sin1) else np.nan

    # compuerta semanal
    sem = S.groupby("semana")["res"].agg(wr=lambda s: (s > 0).mean() * 100, n="size")
    sem = sem[sem["n"] >= 20]
    fila["semanas"] = len(sem)
    fila["sem_ok"] = float((sem["wr"] > nec).mean()) if len(sem) else np.nan
    return fila


def lote(T, hipotesis, costo=COSTO_PCT, q=Q_FDR, mostrar=True):
    """Corre TODAS las hipotesis y aplica las compuertas. Devuelve el DataFrame.

    `hipotesis`: {nombre: mascara booleana alineada al indice de T}.
    """
    tgt, stp = T.attrs["target"], T.attrs["stop"]
    nec = winrate_necesario(tgt, stp, costo)
    base = _una(T, pd.Series(True, index=T.index), "LINEA BASE", nec, costo)

    filas = [_una(T, pd.Series(m, index=T.index) if not isinstance(m, pd.Series) else m,
                  k, nec, costo)
             for k, m in hipotesis.items()]
    D = pd.DataFrame(filas)

    # correccion por multiplicidad SOLO entre las que tienen muestra suficiente
    vivas = D.veredicto.isna() if "veredicto" in D else pd.Series(True, index=D.index)
    D["fdr_ok"] = False
    if vivas.any():
        D.loc[vivas, "fdr_ok"] = _bh(D.loc[vivas, "p"].to_numpy(), q)

    def veredicto(r):
        if r.get("veredicto") == "POCA MUESTRA":
            return f"POCA MUESTRA (n<{N_MIN})"
        if not (r["margen"] > 0):
            return "no cruza el umbral"
        if not r["fdr_ok"]:
            return f"muere en la correccion (FDR q={q})"
        if not (r["vs_pareado"] > 0):
            return "es seleccion de moneda, no timing"
        if not (r["margen_sin_top3"] > 0):
            return f"concentracion: se cae sin el top-{TOP_N}"
        if not (r["margen_sin_top1"] > 0):
            return "un solo par la sostiene"
        if not (r["sem_ok"] >= SEM_MIN):
            return f"inconsistente por semana ({100*r['sem_ok']:.0f}% arriba)"
        return "SOBREVIVE"

    D["veredicto"] = D.apply(veredicto, axis=1)
    D = D.sort_values("margen", ascending=False, na_position="last").reset_index(drop=True)

    if mostrar:
        print("\n" + "=" * 100)
        print(f"LOTE — {len(D)} hipotesis  |  umbral {nec:.2f}%  |  "
              f"linea base {base.get('wr', float('nan')):.2f}% (n={base['n']:,})")
        print("=" * 100)
        print(f"{'hipotesis':26s} {'n':>7s} {'win%':>7s} {'margen':>8s} {'vs par':>7s} "
              f"{'sin3':>7s} {'sem':>5s} {'p_ind':>7s} {'p_blq':>7s}  veredicto")
        print("-" * 108)
        for _, r in D.iterrows():
            f = lambda v, d=2: ("  --  " if pd.isna(v) else f"{v:+.{d}f}")  # noqa: E731
            print(f"{r.hipotesis[:26]:26s} {r.n:7,d} "
                  f"{'  --  ' if pd.isna(r.get('wr')) else f'{r.wr:7.2f}'} "
                  f"{f(r.get('margen')):>8s} {f(r.get('vs_pareado'), 1):>7s} "
                  f"{f(r.get('margen_sin_top3'), 1):>7s} "
                  f"{'  -- ' if pd.isna(r.get('sem_ok')) else f'{100*r.sem_ok:4.0f}%'} "
                  f"{r.get('p_indep', float('nan')):7.4f} {r.p:7.4f}  {r.veredicto}")
        print("-" * 100)
        viven = (D.veredicto == "SOBREVIVE").sum()
        pocas = D.veredicto.str.startswith("POCA").sum()
        print(f"SOBREVIVEN {viven} de {len(D)}   "
              f"(subpotenciadas {pocas}, que NO es lo mismo que refutadas)")
        if viven == 0:
            print("\nNinguna cruza. Ese es un resultado, no un fracaso: cierra la familia\n"
                  "entera en una corrida. Lo que NO se puede hacer es aflojar una compuerta\n"
                  "y volver a mirar — eso es exactamente como se fabrica un falso positivo.")
        else:
            print("\nOJO: 'SOBREVIVE' significa 'todavia no la pude matar EN ESTA VENTANA'.\n"
                  "Antes de creerle: repetir en una ventana distinta que no hayas mirado.")
    return D


# ------------------------------------------------------------------ bateria
def hipotesis_estandar(F, cruces=False):
    """Colas de cada feature. Cada una agarra ~20% de las entradas.

    Usar quintiles y no umbrales a dedo evita el sesgo de elegir el corte que
    mejor queda — que es otra forma del mismo look-elsewhere.
    """
    H = {}
    cols = [c for c in F.columns if F[c].notna().sum() > 1000]
    for c in cols:
        q20, q80 = F[c].quantile([0.20, 0.80])
        H[f"{c} alto"] = F[c] >= q80
        H[f"{c} bajo"] = F[c] <= q20
    if cruces:
        base = dict(H)
        nombres = list(base)
        for i, a in enumerate(nombres):
            for b in nombres[i + 1:]:
                if a.rsplit(" ", 1)[0] == b.rsplit(" ", 1)[0]:
                    continue          # no cruzar una feature consigo misma
                H[f"{a} + {b}"] = base[a] & base[b]
    return H


def main():
    ap = argparse.ArgumentParser(description="Banco — lote de hipotesis con correccion")
    ap.add_argument("--target", type=float, default=8)
    ap.add_argument("--stop", type=float, default=8)
    ap.add_argument("--horizonte", type=int, default=30)
    ap.add_argument("--paso", type=int, default=12)
    ap.add_argument("--pares", type=int, default=200)
    ap.add_argument("--inicio", default="2025-08-01")
    ap.add_argument("--fin", default="2026-08-01")
    ap.add_argument("--costo", type=float, default=COSTO_PCT)
    ap.add_argument("--q", type=float, default=Q_FDR, help="tasa de falsos descubrimientos")
    ap.add_argument("--cruces", action="store_true", help="agregar cruces de a pares")
    ap.add_argument("--out", default=None, help="guardar la tabla a csv")
    ap.add_argument("--pin", default="base200",
                    help="congela el universo en disco para que la corrida sea reproducible")
    a = ap.parse_args()

    panel = load_panel(a.inicio, a.fin, n=a.pares, pin=a.pin)
    if not panel:
        print("FATAL: no se pudo cargar el panel"); sys.exit(1)

    T = tabla(panel, a.target, a.stop, a.horizonte, a.paso)
    F = features(panel, T)
    H = hipotesis_estandar(F, cruces=a.cruces)
    print(f"\n{len(T):,} entradas  |  {F.shape[1]} features  |  {len(H)} hipotesis")

    D = lote(T, H, costo=a.costo, q=a.q)
    if a.out:
        D.to_csv(a.out, index=False)
        print(f"\ntabla -> {a.out}")


if __name__ == "__main__":
    main()
