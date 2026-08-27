"""
RANKING — evaluar un ranking TRANSVERSAL por barra, no una mascara absoluta.

Por que existe: todo el resto del banco prueba FILTROS. Una mascara booleana sobre
cuantiles pooled del panel-anio (`F.roc_168 >= q80`), evaluada por win rate contra el
umbral. Eso tiene tres defectos que NO son estadisticos sino de diseno, y que este
archivo elimina por construccion:

1. UN CORTE POOLED ES MEDIO SELECTOR DE TIEMPO. `roc_168 alto` en una semana alcista
   selecciona el universo entero. El n efectivo colapsa a semanas y despues `p_bloques`
   lo mata — pero lo que murio fue el diseno, no necesariamente la idea. Rankear DENTRO
   de cada barra separa la seccion cruzada del regimen.

2. LA ACTIVIDAD CORRELACIONA CON EL RESULTADO. Un filtro dispara en rafagas, y ahi vive
   la trampa de SEM_N_MIN: en `PREREGISTRO_RANKING.md` descartaba el 66% de las semanas
   y el 41% de los trades, y lo que descartaba era la parte que perdia. Con top-k por
   barra CADA BARRA APORTA EXACTAMENTE k POSICIONES, asi que ese modo de falla no puede
   ocurrir. No es una compuerta aflojada: es un modo de falla eliminado.

3. EL CONTROL NUNCA FUE POR BARRA. `wr_pareado` de `lote.py` aparea por SIMBOLO. El
   termino de mercado —−0,316 ATR, el 34% de la perdida medida en la seccion 9 de
   `PREREGISTRO_RANKING.md`— nunca se neutralizo en un test de ranking. Aca el
   estadistico es top-k MENOS el universo de esa misma hora, asi que se va solo.

Y las barras NO SE SOLAPAN (`paso >= horizonte`), a diferencia del resto del repo, donde
hay ~60 trades vivos a la vez inflando el n aparente.

El preregistro con la regla de parada esta en `PREREGISTRO_TRANSVERSAL.md` y se escribio
ANTES que este archivo. La seccion 7 declara la fuga: `atr_24` no es un brazo ciego.

    py -3.13 ranking.py --nula              # SOLO los controles al azar -> MDE
    py -3.13 ranking.py                     # el lote entero
    py -3.13 ranking.py --k 20 --horizonte 24 --out rank_transversal.csv

Para probar TU ranking:

    from ranking import tablero, evaluar, lote_rankings
    TB = tablero(panel, paso=24, horizonte=24)
    evaluar(TB, TB.mi_score, "mi idea", objetivo="largo")
"""
import argparse
import sys

import numpy as np
import pandas as pd

from klines import load_panel
from lote import _bh, _feat_simbolo
from primer_toque import COSTO_PCT

# ------------------------------------------------------------------ compuertas
# Cambiarlas es una decision de diseno, no un ajuste para que pase algo.
MIN_SYMS = 30        # una barra con menos pares no tiene seccion cruzada
SEM_N_MIN = 8        # semanas minimas para que el bootstrap de bloques signifique algo
SEM_OK = 0.60        # fraccion de semanas con spread > 0
TOP_N = 3            # simbolos que se sacan en el chequeo de concentracion
Q_FDR = 0.10
REPS = 2000
WARMUP = 720         # roc_720 necesita 30d de historia; igual que movers.py

# OJO: aca NO hay SEM_N_MIN de actividad, y es a proposito. Con k constante por barra,
# todas las semanas tienen la misma cantidad de posiciones (salvo huecos de datos), asi
# que filtrar por actividad no puede filtrar por resultado. Ese era el agujero.


# ------------------------------------------------------------------ el tablero
def tablero(panel, paso=24, horizonte=24, verbose=True):
    """Una fila por (simbolo, barra): features del PASADO + resultado del FUTURO.

    Entrada al cierre de la barra `i`. `_feat_simbolo` calcula en la posicion `i`
    mirando solo hasta `i`, asi que no hay lookahead: se decide con la vela cerrada y
    se entra a ese precio.

    `paso >= horizonte` para que las barras NO se solapen. Es la diferencia con el resto
    del repo, donde una entrada cada 12h con horizonte de 30d deja ~60 trades vivos a la
    vez y el n contado no es el n efectivo.
    """
    if paso < horizonte:
        raise ValueError(
            f"paso={paso} < horizonte={horizonte}: las barras se solaparian y el n "
            f"efectivo dejaria de ser el contado. Subi el paso o bajá el horizonte.")

    H = horizonte
    piezas = []
    for k, (sym, df) in enumerate(panel.items(), 1):
        c = df["c"].to_numpy(float)
        h = df["h"].to_numpy(float)
        l = df["l"].to_numpy(float)
        t = df["t"].to_numpy()
        n = len(c)
        if n < WARMUP + H + 1:
            continue

        f = _feat_simbolo(df)
        idx = np.arange(WARMUP, n - H, paso)
        if not len(idx):
            continue

        entrada = c[idx]
        # camino futuro en (i, i+H]: la barra de entrada NO cuenta
        runup = np.array([h[i + 1:i + H + 1].max() for i in idx]) / entrada - 1.0
        caida = np.array([l[i + 1:i + H + 1].min() for i in idx]) / entrada - 1.0
        ret = c[idx + H] / entrada - 1.0

        d = pd.DataFrame({k2: v[idx] for k2, v in f.items()})
        d.insert(0, "t", t[idx])
        d.insert(0, "sym", sym)
        d["ret"] = ret
        d["runup"] = runup
        d["caida"] = caida
        piezas.append(d)
        if verbose and k % 50 == 0:
            print(f"  tablero {k}/{len(panel)}...", flush=True)

    if not piezas:
        raise RuntimeError("panel vacio o demasiado corto para el warmup pedido")
    TB = pd.concat(piezas, ignore_index=True)

    # normalizacion por ATR del propio simbolo. La seccion 8B de PREREGISTRO_RANKING
    # midio que ~87% de cualquier efecto crudo es ESCALA: los brazos "ganadores" se
    # movian 40-45% menos y con deriva base negativa eso acerca la mediana a cero por
    # mecanica, sin ninguna ventaja. En unidades de ATR ese artefacto no existe.
    atr = TB["atr_24"].replace(0, np.nan)
    TB["y_largo"] = TB["ret"] / atr
    TB["y_corto"] = -TB["ret"] / atr
    TB["y_magnitud"] = (TB["runup"] - TB["caida"]) / atr

    # ...y la version CRUDA de cada objetivo, sin dividir por nada.
    #
    # OJO, esto NO es opcional: el denominador de arriba ES UNA DE LAS FEATURES QUE SE
    # RANKEAN. Rankear por `atr_24` contra un objetivo dividido por `atr_24` es circular,
    # y en la primera corrida (2026-08-27) eso dio vuelta el signo de TODOS los brazos de
    # magnitud: `dd_168` elegia nombres con 0,60x el ATR del universo y salia +0,669
    # normalizado contra -0,029 crudo (falso positivo), y `atr_24` elegia 1,88x y salia
    # -0,976 normalizado contra +0,059 crudo (falso negativo). Por eso `_spread_semanal`
    # computa las dos y `lote_rankings` marca ARTEFACTO DE ESCALA cualquier brazo cuyo
    # signo dependa de la normalizacion.
    TB["y_largo_crudo"] = TB["ret"]
    TB["y_corto_crudo"] = -TB["ret"]
    TB["y_magnitud_crudo"] = TB["runup"] - TB["caida"]

    # mercado = mediana del universo en esa barra. OJO: `rs_168 = roc_168 - mkt_168` NO
    # se agrega como score, porque restar una constante DE LA BARRA no cambia el orden
    # DENTRO de la barra: transversalmente, fuerza relativa y momentum son el MISMO
    # ranking. (Se ve en movers_estudio.csv: `roc_168 alto` y `rs_168 alto` tienen
    # numeros identicos hasta el ultimo decimal.)
    TB["dt"] = pd.to_datetime(TB["t"], unit="ms", utc=True)
    TB["semana"] = TB["dt"].dt.strftime("%G-W%V")

    # barras sin seccion cruzada suficiente
    vivos = TB.groupby("t")["y_largo"].transform("count")
    TB = TB[vivos >= MIN_SYMS].reset_index(drop=True)

    if verbose:
        print(f"\ntablero: {len(TB):,} filas | {TB['t'].nunique():,} barras | "
              f"{TB['sym'].nunique()} pares | {TB['semana'].nunique()} semanas | "
              f"paso {paso}h horizonte {horizonte}h (sin solape)")
    return TB


# ------------------------------------------------------------------ scores
def _z(s):
    """z-score DENTRO de la barra. ddof=0 explicito: es una normalizacion de la
    seccion cruzada completa, no una estimacion de una poblacion mas grande."""
    sd = s.std(ddof=0)
    return (s - s.mean()) / sd if sd and np.isfinite(sd) else s * 0.0


def scores(TB, neutralizar="roc_24"):
    """{nombre: serie} con los rankings a probar.

    Incluye la version RESIDUALIZADA de cada uno contra el momentum reciente. Ese es
    el punto: todos los rankings de este repo —el score de 15 puntos, la banda ATR,
    roc12, roc_*/rs_*/ext— comparten UN eje, "ya se movio", y por eso todos terminan
    comprando el techo (+3,12 ATR de corrida previa, seccion 9). Residualizar contra
    `roc_24` saca ese eje y deja ver si abajo queda algo.

    La residualizacion es una regresion transversal POR BARRA: con ambos lados
    z-scoreados dentro de la barra, beta = correlacion de esa barra.
    """
    crudos = ["atr_24", "vol_24", "vol_168", "rango_168",
              "roc_24", "roc_72", "roc_168", "roc_720",
              "compresion", "dd_168", "dd_720", "pos_168"]
    crudos = [c for c in crudos if c in TB.columns]

    S = {}
    for c in crudos:
        S[c] = TB[c]

    zb = TB.groupby("t")[neutralizar].transform(_z)
    for c in crudos:
        if c == neutralizar:
            continue
        za = TB.groupby("t")[c].transform(_z)
        beta = (za * zb).groupby(TB["t"]).transform("mean")
        S[f"{c} ~ sin {neutralizar}"] = za - beta * zb

    return S


def controles(TB, n=3, seed=0):
    """Rankings al azar, mismo k y mismas barras. Es la pieza central del test: si
    ningun score se separa de estos, la pregunta esta contestada sin que nada tenga
    que 'sobrevivir'."""
    rng = np.random.default_rng(seed)
    return {f"CONTROL azar {i+1}": pd.Series(rng.random(len(TB)), index=TB.index)
            for i in range(n)}


# ------------------------------------------------------------------ estadistica
def _spread_semanal(TB, score, k, y, costo):
    """Spread top-k contra el universo de LA MISMA BARRA, agregado por semana.

    Devuelve (spread semanal, aporte por simbolo, spread semanal CRUDO, ratio de ATR).

    El costo se aplica SOLO a la pata top-k (el universo es un benchmark, no una
    posicion) y se convierte a unidades de ATR con el atr_24 mediano de los
    seleccionados de esa barra.
    """
    y_crudo = f"{y}_crudo"
    D = TB[["t", "sym", "semana", "atr_24", y, y_crudo]].copy()
    D["score"] = score.to_numpy()
    D = D[D["score"].notna() & D[y].notna() & D["atr_24"].gt(0)]
    if D.empty:
        return None, None, None

    # top-k por barra
    D = D.sort_values(["t", "score"], ascending=[True, False], kind="mergesort")
    D["_rk"] = D.groupby("t").cumcount()
    sel = D["_rk"] < k

    uni = D.groupby("t")[y].mean()
    top = D[sel].groupby("t")[y].mean()
    atr_sel = D[sel].groupby("t")["atr_24"].median()
    if costo:
        top = top - (costo / 100.0) / atr_sel

    # el mismo spread SIN normalizar, para detectar artefactos de escala
    uni_c = D.groupby("t")[y_crudo].mean()
    top_c = D[sel].groupby("t")[y_crudo].mean()
    if costo:
        top_c = top_c - costo / 100.0
    crudo = (top_c - uni_c).dropna()

    # cuanto mas (o menos) volatil es lo que elige este ranking que el universo
    ratio = float((atr_sel / D.groupby("t")["atr_24"].median()).mean())

    por_barra = (top - uni).dropna()
    if por_barra.empty:
        return None, None, None

    # aporte de cada simbolo al spread (para el chequeo de concentracion)
    S = D[sel].copy()
    S["exceso"] = S[y].to_numpy() - uni.reindex(S["t"]).to_numpy()
    aporte = S.groupby("sym")["exceso"].sum().sort_values(ascending=False)

    sem_de = D.groupby("t")["semana"].first()
    sem = por_barra.groupby(sem_de.reindex(por_barra.index)).mean()
    sem_crudo = crudo.groupby(sem_de.reindex(crudo.index)).mean()
    return sem, aporte, sem_crudo, ratio


def _p_bloques(sem, reps=REPS, seed=0):
    """p-valor remuestreando SEMANAS ENTERAS, cada una pesando uno.

    Misma logica que `lote._p_bloques`, pero aca el estadistico es el spread medio y la
    nula es spread <= 0. No hace falta filtrar semanas flacas: con k constante por barra
    todas las semanas tienen practicamente la misma cantidad de posiciones.
    """
    k = len(sem)
    if k < SEM_N_MIN:
        return 1.0
    rng = np.random.default_rng(seed)
    v = sem.to_numpy()
    m = np.array([rng.choice(v, k, replace=True).mean() for _ in range(reps)])
    return float((m <= 0).mean())


def evaluar(TB, score, nombre, objetivo="largo", k=20, costo=COSTO_PCT):
    """Una fila con todas las compuertas aplicadas."""
    y = f"y_{objetivo}"
    # la magnitud no es una posicion: no se le descuenta costo (ver preregistro §3.7)
    c = 0.0 if objetivo == "magnitud" else costo

    sem, aporte, sem_crudo, ratio = _spread_semanal(TB, score, k, y, c)
    fila = {"ranking": nombre, "objetivo": objetivo}
    if sem is None or len(sem) < SEM_N_MIN:
        fila.update(semanas=0 if sem is None else len(sem), spread=np.nan, p=1.0,
                    veredicto=f"POCAS SEMANAS (<{SEM_N_MIN})")
        return fila

    fila["semanas"] = int(len(sem))
    fila["spread"] = float(sem.mean())
    fila["sd_sem"] = float(sem.std(ddof=1))
    fila["sem_ok"] = float((sem > 0).mean())
    fila["p"] = _p_bloques(sem) if sem.mean() > 0 else 1.0
    fila["spread_crudo"] = float(sem_crudo.mean())
    fila["atr_ratio"] = ratio          # <1 = elige nombres mas quietos que el universo

    # concentracion: recomputar SIN los simbolos que mas aportan (no pueden ser elegidos)
    for etiq, cuantos in (("sin_top3", TOP_N), ("sin_top1", 1)):
        fuera = set(aporte.head(cuantos).index)
        sub = TB[~TB["sym"].isin(fuera)]
        s2, _, _, _ = _spread_semanal(sub, score[sub.index], k, y, c)
        fila[etiq] = float(s2.mean()) if s2 is not None and len(s2) else np.nan

    return fila


def lote_rankings(TB, rankings, k=20, objetivos=("largo", "corto", "magnitud"),
                  costo=COSTO_PCT, q=Q_FDR, mde=None, mostrar=True):
    """Corre TODOS los rankings x TODOS los objetivos y aplica las compuertas.

    La correccion por multiplicidad va sobre el lote ENTERO —todos los brazos y los
    tres objetivos juntos—, no una familia por vez.
    """
    filas = [evaluar(TB, s, n, objetivo=o, k=k, costo=costo)
             for o in objetivos for n, s in rankings.items()]
    D = pd.DataFrame(filas)

    vivas = ~D["veredicto"].notna() if "veredicto" in D else pd.Series(True, index=D.index)
    D["fdr_ok"] = False
    if vivas.any():
        D.loc[vivas, "fdr_ok"] = _bh(D.loc[vivas, "p"].to_numpy(), q)

    # referencia de los controles al azar, por objetivo
    ctrl = (D[D["ranking"].str.startswith("CONTROL")]
            .groupby("objetivo")["spread"].median().to_dict())

    def veredicto(r):
        if isinstance(r.get("veredicto"), str):
            return r["veredicto"]
        if r["ranking"].startswith("CONTROL"):
            return "control"
        if not (r["spread"] > 0):
            return "spread <= 0"
        # el signo no puede depender de la normalizacion (ver el comentario de `tablero`)
        if not (r["spread_crudo"] > 0):
            return f"ARTEFACTO DE ESCALA (atr_ratio {r['atr_ratio']:.2f})"
        if mde is not None and abs(r["spread"] - ctrl.get(r["objetivo"], 0.0)) < mde:
            return f"dentro del MDE del azar (±{mde:.3f})"
        if not r["fdr_ok"]:
            return f"muere en la correccion (FDR q={q})"
        if not (r["sin_top3"] > 0):
            return f"concentracion: se cae sin el top-{TOP_N}"
        if not (r["sin_top1"] > 0):
            return "un solo par lo sostiene"
        if not (r["sem_ok"] >= SEM_OK):
            return f"inconsistente por semana ({100*r['sem_ok']:.0f}% arriba)"
        return "SOBREVIVE"

    D["veredicto"] = D.apply(veredicto, axis=1)
    D = D.sort_values(["objetivo", "spread"], ascending=[True, False],
                      na_position="last").reset_index(drop=True)

    if mostrar:
        print("\n" + "=" * 104)
        print(f"RANKING TRANSVERSAL — {len(D)} brazos | top-k={k} | costo {costo:.2f}%"
              f"{f' | MDE ±{mde:.3f} ATR' if mde is not None else ''}")
        print("  spread = media semanal de (top-k − universo de la MISMA barra), en ATR")
        print("=" * 104)
        for o in objetivos:
            sub = D[D["objetivo"] == o]
            if sub.empty:
                continue
            print(f"\n--- objetivo: {o.upper()} "
                  f"(control al azar = {ctrl.get(o, float('nan')):+.4f}) ---")
            print(f"{'ranking':30s} {'sem':>4s} {'spread':>9s} {'crudo':>8s} "
                  f"{'atrR':>5s} {'sin3':>8s} {'sem>0':>6s} {'p':>7s}  veredicto")
            print("-" * 104)
            for _, r in sub.iterrows():
                f = lambda v: ("   --   " if pd.isna(v) else f"{v:+.4f}")  # noqa: E731
                print(f"{r.ranking[:30]:30s} {int(r.semanas):4d} {f(r.spread):>9s} "
                      f"{f(r.get('spread_crudo')):>8s} "
                      f"{r.get('atr_ratio', float('nan')):5.2f} "
                      f"{f(r.get('sin_top3')):>8s} "
                      f"{'  -- ' if pd.isna(r.get('sem_ok')) else f'{100*r.sem_ok:4.0f}%'} "
                      f"{r.p:7.4f}  {r.veredicto}")
        print("-" * 104)
        viven = (D.veredicto == "SOBREVIVE").sum()
        print(f"\nSOBREVIVEN {viven} de {(D.veredicto != 'control').sum()} brazos")
        if viven == 0:
            print(
                "\nNinguno cruza. Eso CIERRA la familia del ranking transversal en una\n"
                "corrida — y es un resultado distinto del de `lote.py`, porque este diseno\n"
                "no tiene la trampa de SEM_N_MIN ni el termino de mercado adentro.\n"
                "Lo que NO se puede hacer es aflojar una compuerta y volver a mirar.")
        else:
            print("\nOJO: 'SOBREVIVE' = 'no lo pude matar EN ESTA VENTANA', que es un bear\n"
                  "brutal. La seccion 6 del preregistro es obligatoria: reserva OOS\n"
                  "2024-08 -> 2025-08, nunca mirada. Si ahi cambia de signo, era el regimen.")
    return D


# ------------------------------------------------------------------ MDE
def mde_del_azar(TB, k=20, objetivos=("largo", "corto", "magnitud"), n=8, costo=COSTO_PCT):
    """MDE al 80% de potencia, estimado con la NULA REAL (rankings al azar).

    Regla del handoff: contar el n post-join y calcular el MDE ANTES de estimar nada.
    Convierte un 'no se pudo medir' en 'no esta' — o al reves, y eso es justamente lo
    que hay que saber antes de mirar un brazo real.
    """
    C = controles(TB, n=n, seed=12345)
    filas = [evaluar(TB, s, nm, objetivo=o, k=k, costo=costo)
             for o in objetivos for nm, s in C.items()]
    D = pd.DataFrame(filas)
    out = {}
    print("\n" + "=" * 72)
    print(f"NULA — {n} rankings al azar x {len(objetivos)} objetivos, top-k={k}")
    print("=" * 72)
    for o in objetivos:
        sub = D[D["objetivo"] == o]
        sd = float(sub["sd_sem"].median())
        sem = int(sub["semanas"].median())
        se = sd / np.sqrt(sem)
        m = 2.80 * se          # (1,96 + 0,84) * error estandar = 80% de potencia
        out[o] = m
        print(f"  {o:10s} semanas {sem:3d} | sd semanal {sd:.4f} | "
              f"spread del azar {sub['spread'].median():+.4f} | **MDE ±{m:.4f} ATR**")
    print("\nUn efecto mas chico que su MDE no se puede distinguir del azar con esta\n"
          "ventana. Si nada supera el MDE, el veredicto es 'no se pudo medir', que NO\n"
          "es lo mismo que 'no esta'.")
    return out


# ------------------------------------------------------------------ main
def main():
    ap = argparse.ArgumentParser(description="Banco — ranking transversal por barra")
    ap.add_argument("--k", type=int, default=20, help="cuantos simbolos por barra")
    ap.add_argument("--paso", type=int, default=24, help="horas entre rebalanceos")
    ap.add_argument("--horizonte", type=int, default=24, help="horas de tenencia")
    ap.add_argument("--pares", type=int, default=200)
    ap.add_argument("--inicio", default="2025-08-01")
    ap.add_argument("--fin", default="2026-08-01")
    ap.add_argument("--costo", type=float, default=COSTO_PCT)
    ap.add_argument("--q", type=float, default=Q_FDR)
    ap.add_argument("--pin", default="base200")
    ap.add_argument("--nula", action="store_true",
                    help="SOLO los controles al azar: calcula el MDE y sale")
    ap.add_argument("--out", default=None)
    a = ap.parse_args()

    panel = load_panel(a.inicio, a.fin, n=a.pares, pin=a.pin)
    if not panel:
        print("FATAL: no se pudo cargar el panel"); sys.exit(1)

    TB = tablero(panel, paso=a.paso, horizonte=a.horizonte)

    if a.nula:
        mde_del_azar(TB, k=a.k, costo=a.costo)
        print("\nAhora si: correr sin --nula para el lote real.")
        return

    mde = mde_del_azar(TB, k=a.k, costo=a.costo)
    R = {**scores(TB), **controles(TB)}
    print(f"\n{len(R)} rankings x 3 objetivos = {len(R)*3} brazos")

    D = lote_rankings(TB, R, k=a.k, costo=a.costo, q=a.q,
                      mde=float(np.median(list(mde.values()))))
    if a.out:
        D.to_csv(a.out, index=False)
        print(f"\ntabla -> {a.out}")


if __name__ == "__main__":
    main()
