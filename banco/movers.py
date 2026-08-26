"""
MOVERS — que tenian en comun, ANTES, los coins que despues mas recorrido hicieron.

Esta es la pregunta que mas facil se contesta mal en este repo. Tres errores conocidos
y como los esquiva este archivo:

1. CONDICIONAR AL REVES. Mirar el top-20 del mes y ver que tenian en comun da
   P(feature | mover). Lo que se opera es P(mover | feature), y difieren por la tasa
   base. Aca la etiqueta se define hacia ADELANTE — features en t0, recorrido en
   [t0, t0+30d] — y toda tabla lleva la tasa base al lado.

2. EL MAXIMO DE N SORTEOS. "Siempre hay una que sube 30%" aparece IGUAL barajando los
   datos: es el maximo de ~200 sorteos. Por eso (a) la etiqueta es el decil superior
   DENTRO de cada ventana, lo que saca el mes alcista de encima, y (b) hay un null de
   permutacion que se queda con el MAXIMO lift sobre todas las features — la
   distribucion look-elsewhere, que es contra la que hay que comparar cuando mirás 30.

3. LA COLA SIMETRICA. Ya esta medido en este repo que lo que predice movidas grandes
   las predice para los dos lados. Cada feature reporta lift ARRIBA y lift ABAJO. Si el
   ratio es ~1 la feature predice volatilidad, no direccion, y eso no se cobra sin
   convexidad (ver: dos puntas, descartado).

    py -3.13 movers.py --foto                 # los ultimos 30 dias, descriptivo
    py -3.13 movers.py --estudio              # ~43 ventanas rodantes + null
    py -3.13 movers.py                        # las dos
"""
import argparse
import sys
from datetime import datetime, timedelta, timezone

import numpy as np
import pandas as pd

from klines import load_panel
from lote import features

MS_H = 3600000
MS_D = 86400000
BASE = 0.10          # decil superior: la tasa base es 10% por construccion
TOP_N = 3            # simbolos que se sacan en el chequeo de concentracion
WARMUP_H = 720       # roc_720 necesita 30d de historia antes de t0
MIN_SYMS = 30        # una ventana con menos pares no tiene decil transversal, se descarta


# ------------------------------------------------------------------ recorrido
def recorrido(panel, t0_ms, dias=30):
    """Recorrido futuro de cada par desde t0. Entrada = ultima vela CERRADA (offset -1).

    Devuelve [sym, t, entrada, runup, caida, ret]:
      runup = max(high) en [t0, t0+dias] / entrada - 1   (lo que se podia agarrar)
      caida = min(low)  en [t0, t0+dias] / entrada - 1   (la otra cola)
      ret   = close al final / entrada - 1               (lo que se cobra sin gestion)

    runup y caida son estadisticos de CAMINO; `ret` no. Hacen falta los tres: una
    feature puede ensanchar el camino sin mover el retorno, que es justo lo que pasa.
    """
    H = dias * 24
    filas = []
    for sym, df in panel.items():
        t = df["t"].to_numpy()
        i = int(np.searchsorted(t, t0_ms, side="left"))
        if i < WARMUP_H or i + H > len(t):
            continue
        entrada = float(df["c"].to_numpy()[i - 1])
        if not np.isfinite(entrada) or entrada <= 0:
            continue
        h = df["h"].to_numpy()[i:i + H]
        l = df["l"].to_numpy()[i:i + H]
        cierre = float(df["c"].to_numpy()[i + H - 1])
        filas.append((sym, int(t[i - 1]), entrada,
                      float(h.max()) / entrada - 1.0,
                      float(l.min()) / entrada - 1.0,
                      cierre / entrada - 1.0))
    return pd.DataFrame(filas, columns=["sym", "t", "entrada", "runup", "caida", "ret"])


def _ventanas(panel, dias, paso_d):
    """Los t0 posibles: dejan WARMUP_H de historia atras y `dias` de futuro adelante."""
    t_min = min(int(df["t"].iloc[0]) for df in panel.values())
    t_max = max(int(df["t"].iloc[-1]) for df in panel.values())
    ini = t_min + WARMUP_H * MS_H
    fin = t_max - dias * MS_D
    return list(range(ini, fin + 1, paso_d * MS_D))


# ------------------------------------------------------------------ foto
def foto(panel, t0_ms, dias=30, top=20):
    """Descriptivo de UNA ventana. Es n=1: no distingue patron de casualidad."""
    R = recorrido(panel, t0_ms, dias)
    if R.empty:
        print("FATAL: ningun par cubre la ventana")
        return None
    R = R.sort_values("runup", ascending=False).reset_index(drop=True)

    d0 = datetime.fromtimestamp(t0_ms / 1000, timezone.utc).strftime("%Y-%m-%d")
    d1 = datetime.fromtimestamp((t0_ms + dias * MS_D) / 1000, timezone.utc).strftime("%Y-%m-%d")
    print("\n" + "=" * 74)
    print(f"FOTO  {d0} -> {d1}  ({dias}d, {len(R)} pares)")
    print("=" * 74)
    print(f"mediana del universo: runup {R.runup.median():+7.1%}   "
          f"caida {R.caida.median():+7.1%}")

    F = features(panel, R[["sym", "t"]], verbose=False)
    F.index = R.index
    cols = [c for c in F.columns if F[c].notna().sum() > len(F) * 0.5]

    print(f"\n--- top {top} por recorrido al alza ---")
    print(f"{'par':<14}{'runup':>9}{'caida':>9}   {'roc_168':>9}{'dd_168':>9}"
          f"{'compres':>9}{'atr_24':>9}{'rs_168':>9}")
    for i in range(min(top, len(R))):
        f = F.iloc[i]
        print(f"{R.sym[i]:<14}{R.runup[i]:>+8.1%}{R.caida[i]:>+9.1%}   "
              f"{f.get('roc_168', np.nan):>+9.1%}{f.get('dd_168', np.nan):>+9.1%}"
              f"{f.get('compresion', np.nan):>9.2f}{f.get('atr_24', np.nan):>9.2%}"
              f"{f.get('rs_168', np.nan):>+9.1%}")

    # el punto de la foto: comparar el top contra TODO el universo, no leerlo solo
    k = max(5, len(R) // 10)
    print(f"\n--- top decil (n={k}) vs universo, feature por feature ---")
    print("si las dos columnas son parecidas, el 'patron' es la tasa base")
    print(f"{'feature':<14}{'top decil':>12}{'universo':>12}{'brecha':>10}")
    for c in cols:
        a, b = F[c].iloc[:k].median(), F[c].median()
        if not (np.isfinite(a) and np.isfinite(b)):
            continue
        print(f"{c:<14}{a:>12.3f}{b:>12.3f}{a - b:>+10.3f}")

    peor = R.sort_values("caida").head(k)
    print(f"\n--- control de cola simetrica: las {k} que MAS cayeron ---")
    print(f"{'feature':<14}{'top runup':>12}{'top caida':>12}")
    for c in cols:
        a = F[c].iloc[:k].median()
        b = F.loc[peor.index, c].median()
        if not (np.isfinite(a) and np.isfinite(b)):
            continue
        print(f"{c:<14}{a:>12.3f}{b:>12.3f}")
    print("\ncolumnas parecidas = la feature marca volatilidad, no direccion")
    return R


# ------------------------------------------------------------------ estudio
def _rank_intra(s, g):
    """Percentil de la feature DENTRO de su ventana (0..1). Evita umbrales a dedo y
    evita que el drift del mercado haga de feature."""
    return s.groupby(g).rank(pct=True, na_option="keep")


def _obs(panel, dias, paso_d):
    """Las observaciones (sym, ventana) con features en t0 y etiquetas de las DOS colas.

    Las etiquetas son CROSS-SECTIONAL: decil superior de recorrido dentro de la misma
    ventana. Eso saca el beta del mes de la ecuacion — en un mes alcista sube todo, y
    eso no es un patron de la moneda."""
    vs = _ventanas(panel, dias, paso_d)
    print(f"{len(vs)} ventanas de {dias}d cada {paso_d}d")

    piezas = []
    for k, t0 in enumerate(vs, 1):
        R = recorrido(panel, t0, dias)
        if len(R) < MIN_SYMS:
            continue
        R["v"] = k
        piezas.append(R)
        if k % 10 == 0:
            print(f"  ventana {k}/{len(vs)}...", flush=True)
    if not piezas:
        print("FATAL: sin ventanas utiles")
        return None, None, None
    R = pd.concat(piezas, ignore_index=True)

    F = features(panel, R[["sym", "t"]], verbose=False)
    F.index = R.index
    cols = [c for c in F.columns if F[c].notna().sum() > len(F) * 0.5]

    # etiquetas: decil de cada cola, dentro de la ventana
    R["p_up"] = _rank_intra(R.runup, R.v)
    R["p_dn"] = _rank_intra(-R.caida, R.v)
    R["gana"] = R.p_up >= 0.90
    R["cae"] = R.p_dn >= 0.90
    R["dir"] = (R.runup + R.caida) > 0      # subio mas de lo que bajo
    return R, F, cols


def _null_max(R, mascaras, etiqueta, base, nperm, seed):
    """Distribucion look-elsewhere: permutar la etiqueta DENTRO de cada ventana y
    quedarse con el MAXIMO lift del lote. Es contra esto que hay que comparar cuando
    mirás N features, no contra 1."""
    rng = np.random.default_rng(seed)
    y = R[etiqueta].to_numpy()
    vidx = R.v.to_numpy()
    orden = np.argsort(vidx, kind="stable")
    cortes = np.unique(vidx[orden], return_index=True)[1][1:]
    grupos = np.split(orden, cortes)
    maxs = np.empty(nperm)
    for i in range(nperm):
        g = y.copy()
        for idx in grupos:
            g[idx] = rng.permutation(g[idx])
        maxs[i] = max(g[m].mean() for m in mascaras) / base
    return maxs


def estudio(panel, dias=30, paso_d=7, nperm=300, seed=0):
    """Que separa a los top movers del resto del universo."""
    print("\n" + "=" * 74)
    print("ESTUDIO — universo completo")
    print("=" * 74)
    R, F, cols = _obs(panel, dias, paso_d)
    if R is None:
        return None

    # la tasa base OBSERVADA, no la nominal: con pocos pares por ventana el corte del
    # decil cae entre simbolos y da 11,4% en vez de 10%. El lift se divide por esta.
    base = float(R.gana.mean())
    print(f"\n{len(R):,} obs  |  {len(cols)} features  |  {R.v.nunique()} ventanas")
    print(f"tasa base = {base:.1%} (nominal {BASE:.0%})")

    filas, mascaras = [], []
    for c in cols:
        pr = _rank_intra(F[c], R.v)
        for lado, m in (("alto", pr >= 0.80), ("bajo", pr <= 0.20)):
            m = m.fillna(False)
            n = int(m.sum())
            if n < 200:
                continue
            mascaras.append(m.to_numpy())
            up, dn = R.gana[m].mean(), R.cae[m].mean()
            top3 = R.sym[m & R.gana].value_counts().head(TOP_N).index
            sin_top = m & ~R.sym.isin(top3)
            por_v = R[m].groupby("v").gana.mean()
            filas.append(dict(
                feature=f"{c} {lado}", n=n,
                lift=up / base, p_up=up, p_dn=dn,
                simetria=up / dn if dn > 0 else np.inf,
                lift_sin_top3=(R.gana[sin_top].mean() / base) if sin_top.sum() > 100 else np.nan,
                ventanas_ok=(por_v > base).mean(), n_ventanas=len(por_v)))
    if not filas:
        print(f"\nninguna feature llega a n>=200 (obs={len(R):,}). "
              "Correr con mas pares o mas ventanas.")
        return None
    D = pd.DataFrame(filas).sort_values("lift", ascending=False).reset_index(drop=True)

    maxs = _null_max(R, mascaras, "gana", base, nperm, seed)
    p95, p99 = np.percentile(maxs, [95, 99])

    print(f"\n--- null de permutacion ({nperm} barajadas, {len(mascaras)} features) ---")
    print(f"maximo lift esperado por AZAR mirando {len(mascaras)} features:")
    print(f"  mediana {np.median(maxs):.2f}x   p95 {p95:.2f}x   p99 {p99:.2f}x")
    print("  -> un lift por debajo del p95 NO es un hallazgo, es el maximo de N sorteos")

    print(f"\n--- ranking (lift = P(top decil | feature) / {base:.1%}) ---")
    print(f"{'feature':<24}{'n':>8}{'lift':>7}{'sinTop3':>9}{'simetr':>8}"
          f"{'vent.OK':>9}  veredicto")
    for _, r in D.iterrows():
        if r.lift < p95:
            v = "dentro del azar"
        elif not (r.lift_sin_top3 > 1.0):
            v = f"concentracion (top-{TOP_N})"
        elif r.simetria < 1.2:
            v = "volatilidad, no direccion"
        elif r.ventanas_ok < 0.60:
            v = "vive en pocas ventanas"
        else:
            v = "SOBREVIVE"
        st = f"{r.lift_sin_top3:.2f}x" if np.isfinite(r.lift_sin_top3) else "  -  "
        print(f"{r.feature:<24}{r.n:>8,}{r.lift:>6.2f}x{st:>9}"
              f"{r.simetria:>8.2f}{r.ventanas_ok:>8.0%}  {v}")
    D.attrs["p95"] = p95
    return D


# ------------------------------------------------------------------ condicional
def condicional(panel, dias=30, paso_d=7, nperm=300, seed=0,
                bucket="atr_24", lado="alto", corte=0.80):
    """DENTRO del bucket de alta volatilidad, ¿algo separa la cola de arriba de la de abajo?

    El estudio general encontro que la volatilidad previa marca las DOS colas, y mas la
    de abajo (17,3% arriba contra 24,0% abajo). Detectar no era el cuello. La pregunta
    que queda es direccional y esta es su forma limpia: se fija el bucket y se busca
    adentro. Tres etiquetas, porque las tres pueden fallar por separado:
      gana = decil superior de recorrido al alza  (del universo entero, no del bucket)
      cae  = decil inferior de caida
      dir  = subio mas de lo que bajo  (usa TODAS las obs del bucket, no solo las colas)

    Las features se rankean DENTRO del bucket y de la ventana. Rankearlas contra el
    universo entero seria confundir: la alta volatilidad ya correlaciona con |roc| alto,
    asi que el quintil global no seria una particion del bucket.
    """
    print("\n" + "=" * 74)
    print(f"CONDICIONAL — dentro del bucket `{bucket} {lado}` (quintil {corte:.0%})")
    print("=" * 74)
    R, F, cols = _obs(panel, dias, paso_d)
    if R is None:
        return None

    pr_b = _rank_intra(F[bucket], R.v)
    mb = (pr_b >= corte) if lado == "alto" else (pr_b <= 1 - corte)
    mb = mb.fillna(False)
    Rb, Fb = R[mb].copy(), F[mb]
    if len(Rb) < 500:
        print(f"FATAL: bucket con solo {len(Rb)} obs")
        return None

    b_up, b_dn, b_dir = Rb.gana.mean(), Rb.cae.mean(), Rb.dir.mean()
    b_med = Rb.ret.median()
    print(f"\nbucket: {len(Rb):,} obs  |  {Rb.v.nunique()} ventanas  |  {Rb.sym.nunique()} simbolos")
    print(f"linea base DEL BUCKET:  arriba {b_up:.1%}   abajo {b_dn:.1%}   "
          f"asimetria {b_up / b_dn:.2f}   subio>bajo {b_dir:.1%}")
    print(f"  retorno a {dias}d: mediana {b_med:+.2%}   media {Rb.ret.mean():+.2%}")
    print(f"(universo entero: arriba {R.gana.mean():.1%}  abajo {R.cae.mean():.1%}  "
          f"subio>bajo {R.dir.mean():.1%}  ret mediana {R.ret.median():+.2%}  "
          f"media {R.ret.mean():+.2%})")
    print("\npara que esto sirva, alguna fila tiene que llevar la asimetria por encima")
    print("de 1,00 — o sea dar vuelta el signo del bucket, no solo mejorarlo un poco.")

    filas, mascaras = [], []
    for c in cols:
        pr = _rank_intra(Fb[c], Rb.v)
        for l2, m in (("alto", pr >= 0.80), ("bajo", pr <= 0.20)):
            m = m.fillna(False)
            n = int(m.sum())
            if n < 200:
                continue
            mascaras.append(m.to_numpy())
            up, dn, dr = Rb.gana[m].mean(), Rb.cae[m].mean(), Rb.dir[m].mean()
            top3 = Rb.sym[m & Rb.gana].value_counts().head(TOP_N).index
            sin_top = m & ~Rb.sym.isin(top3)
            por_v = Rb[m].groupby("v").dir.mean()
            filas.append(dict(
                feature=f"{c} {l2}", n=n, p_up=up, p_dn=dn, p_dir=dr,
                lift_up=up / b_up, lift_dir=dr / b_dir,
                asimetria=up / dn if dn > 0 else np.inf,
                ret_med=Rb.ret[m].median(), ret_media=Rb.ret[m].mean(),
                dir_sin_top3=(Rb.dir[sin_top].mean() / b_dir) if sin_top.sum() > 100 else np.nan,
                ventanas_ok=(por_v > b_dir).mean()))
    if not filas:
        print(f"\nninguna feature llega a n>=200 dentro del bucket ({len(Rb):,} obs)")
        return None
    D = pd.DataFrame(filas).sort_values("asimetria", ascending=False).reset_index(drop=True)

    # el null va sobre `dir`, que es la etiqueta direccional y la que usa todas las obs
    maxs = _null_max(Rb, mascaras, "dir", b_dir, nperm, seed)
    p95 = float(np.percentile(maxs, 95))
    print(f"\n--- null de permutacion ({nperm} barajadas, {len(mascaras)} features) ---")
    print(f"max lift de `subio>bajo` por AZAR: mediana {np.median(maxs):.2f}x  "
          f"p95 {p95:.2f}x  p99 {np.percentile(maxs, 99):.2f}x")

    print(f"\n--- ordenado por asimetria (la del bucket es {b_up / b_dn:.2f}) ---")
    print(f"{'feature':<24}{'n':>7}{'arriba':>8}{'abajo':>8}{'asim':>7}"
          f"{'sub>baj':>9}{'retMed':>9}{'sinTop3':>9}{'vOK':>6}  veredicto")
    for _, r in D.iterrows():
        if r.asimetria <= 1.0:
            v = "sigue apuntando abajo"
        elif r.lift_dir < p95:
            v = "asimetria ok pero dir dentro del azar"
        elif not (r.dir_sin_top3 > 1.0):
            v = f"concentracion (top-{TOP_N})"
        elif r.ventanas_ok < 0.60:
            v = "vive en pocas ventanas"
        else:
            v = "SOBREVIVE"
        st = f"{r.dir_sin_top3:.2f}x" if np.isfinite(r.dir_sin_top3) else "  -  "
        print(f"{r.feature:<24}{r.n:>7,}{r.p_up:>8.1%}{r.p_dn:>8.1%}{r.asimetria:>7.2f}"
              f"{r.p_dir:>9.1%}{r.ret_med:>+9.2%}{st:>9}{r.ventanas_ok:>6.0%}  {v}")
    D.attrs["p95"] = p95
    return D


# ------------------------------------------------------------------ escalera
def escalera(panel, dias=30, paso_d=7, feat="atr_24"):
    """La escalera completa de quintiles, no solo los extremos.

    Comparar "muy volatil" contra "muy calmo" esconde la forma del medio: puede ser
    monotona (mas calmo = siempre mejor) o tener un optimo adentro. Con los extremos
    solos las dos se ven igual.

    OJO con la barrera. `tabla()` usa +-8% FIJO, y 8% es MUY LEJOS para una moneda
    calma y MUY CERCA para una volatil. Asi que esta escalera no compara lo mismo en
    cada escalon: mide, en parte, cuanto tarda cada grupo en llegar a una distancia
    fija. Por eso van tambien mediana y media de retorno, que no dependen de barrera.
    """
    print("\n" + "=" * 74)
    print(f"ESCALERA por quintil de `{feat}`")
    print("=" * 74)
    R, F, cols = _obs(panel, dias, paso_d)
    if R is None:
        return None

    q = _rank_intra(F[feat], R.v)
    et = ["Q1 mas calmo", "Q2", "Q3", "Q4", "Q5 mas volatil"]
    R = R.assign(qb=pd.cut(q, [0, .2, .4, .6, .8, 1.0], labels=et, include_lowest=True))

    print(f"\nuniverso: arriba {R.gana.mean():.1%}  abajo {R.cae.mean():.1%}  "
          f"ret mediana {R.ret.median():+.2%}  media {R.ret.mean():+.2%}")
    print(f"\n{'quintil':<17}{'n':>7}{'arriba':>9}{'abajo':>8}{'ratio':>7}"
          f"{'retMed':>9}{'retMedia':>10}{'|ret|med':>10}{'ret/riesgo':>12}")
    filas = []
    for e in et:
        s = R[R.qb == e]
        if s.empty:
            continue
        up, dn = s.gana.mean(), s.cae.mean()
        med, mea = s.ret.median(), s.ret.mean()
        # riesgo realizado del grupo: cuanto se mueve tipicamente, en valor absoluto.
        # Sin dividir por esto, "menos volatil pierde menos" es trivialmente cierto.
        rie = s.ret.abs().median()
        filas.append(dict(quintil=e, n=len(s), p_up=up, p_dn=dn,
                          ratio=up / dn if dn > 0 else np.nan,
                          ret_med=med, ret_media=mea, riesgo=rie,
                          ret_riesgo=mea / rie if rie > 0 else np.nan))
        print(f"{e:<17}{len(s):>7,}{up:>9.1%}{dn:>8.1%}"
              f"{(up / dn if dn > 0 else np.nan):>7.2f}{med:>+9.2%}{mea:>+10.2%}"
              f"{rie:>10.2%}{(mea / rie if rie > 0 else np.nan):>+12.3f}")

    print("\nLa columna que decide es la ultima: retorno por unidad de movimiento.")
    print("Bajar volatilidad baja la perdida Y la ganancia. Si `ret/riesgo` no mejora,")
    print("no ganaste nada que no consiguieras poniendo menos plata en lo mismo.")
    return pd.DataFrame(filas)


def main():
    ap = argparse.ArgumentParser(description="Banco — patrones de los coins que mas recorren")
    ap.add_argument("--foto", action="store_true")
    ap.add_argument("--estudio", action="store_true")
    ap.add_argument("--condicional", action="store_true",
                    help="buscar direccion DENTRO del bucket de alta volatilidad")
    ap.add_argument("--escalera", action="store_true",
                    help="los 5 quintiles de volatilidad, con retorno por unidad de riesgo")
    ap.add_argument("--bucket", default="atr_24", help="feature que define el bucket")
    ap.add_argument("--bucket-lado", default="alto", choices=["alto", "bajo"])
    ap.add_argument("--dias", type=int, default=30, help="largo de la ventana de recorrido")
    ap.add_argument("--paso", type=int, default=7, help="cada cuantos dias arranca una ventana")
    ap.add_argument("--pares", type=int, default=200)
    ap.add_argument("--inicio", default="2025-08-01")
    ap.add_argument("--fin", default="2026-08-01")
    ap.add_argument("--hoy", default=None, help="fin de la foto (default: hoy UTC)")
    ap.add_argument("--perm", type=int, default=300)
    ap.add_argument("--pin", default="base200")
    ap.add_argument("--out", default=None)
    a = ap.parse_args()
    if not (a.foto or a.estudio or a.condicional or a.escalera):
        a.foto = a.estudio = True

    if a.foto:
        fin = a.hoy or datetime.now(timezone.utc).strftime("%Y-%m-%d")
        ini = (datetime.strptime(fin, "%Y-%m-%d") - timedelta(days=a.dias + 45)
               ).strftime("%Y-%m-%d")
        p = load_panel(ini, fin, n=a.pares, min_bars=(a.dias + 38) * 24, pin=a.pin)
        if not p:
            print("FATAL: panel vacio para la foto")
            sys.exit(1)
        t_max = max(int(df["t"].iloc[-1]) for df in p.values())
        foto(p, t_max - a.dias * MS_D, a.dias)

    if a.estudio or a.condicional or a.escalera:
        p = load_panel(a.inicio, a.fin, n=a.pares, pin=a.pin)
        if not p:
            print("FATAL: panel vacio")
            sys.exit(1)
        D = None
        if a.estudio:
            D = estudio(p, a.dias, a.paso, a.perm)
        if a.escalera:
            D = escalera(p, a.dias, a.paso, feat=a.bucket)
        if a.condicional:
            D = condicional(p, a.dias, a.paso, a.perm,
                            bucket=a.bucket, lado=a.bucket_lado)
        if D is not None and a.out:
            D.to_csv(a.out, index=False)
            print(f"\ntabla -> {a.out}")


if __name__ == "__main__":
    main()
