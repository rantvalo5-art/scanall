"""
GATE DE MEDIANA — las mismas compuertas de `lote.py`, pero sobre la MEDIANA.

Por que existe: la corrida de `PREREGISTRO_RANKING.md` gateo win rate y reporto
mediana SIN gatear. El unico efecto grande que aparecio —las alertas sobre cosas
quietas baten a las extendidas por 4-5pp de mediana a 24h— quedo sin adjudicar. Eso
es un agujero, no un hallazgo: un numero grande que nunca paso una compuerta es
exactamente la forma que tiene un falso positivo antes de que lo maten.

Diferencias de diseno contra `lote.py`, todas forzadas por la metrica:

1. **El estadistico es una diferencia PAREADA DENTRO DE LA SEMANA**:
   `dif_w = mediana(brazo en la semana w) - mediana(TODAS las alertas de w)`.
   Restar la mediana de la misma semana neutraliza el regimen sin tener que
   detectarlo — la familia de regimen esta cerrada justamente porque no se puede
   detectar ([[project-swing-regimen-familia-agotada]]).

2. **El bootstrap remuestrea SEMANAS**, cada una pesando uno. Es la unidad
   independiente: las alertas se solapan y el regimen esta autocorrelacionado.

3. **`n_min` se BARRE, no se fija.** En la corrida anterior un sobreviviente vivia
   entero de `SEM_N_MIN=20`: contaba 10 semanas y tiraba 19, y lo que tiraba era lo
   que perdia. Aca el barrido es parte del resultado, no una auditoria posterior.

4. **Concentracion por conteo, no por aporte.** En win rate el aporte de un simbolo
   es la suma de sus resultados; en mediana no hay tal cosa, asi que se sacan los
   simbolos con MAS alertas (los que mas pueden mover el cuantil).
"""
import numpy as np
import pandas as pd

from lote import N_MIN, _bh

REPS = 2000
SEM_MIN_MEDIANA = 0.60      # fraccion de semanas con dif > 0
TOP_N = 3


def _difs_semanales(S, ret, semana, n_min):
    """[dif_w] — mediana del brazo menos mediana de TODAS las alertas, semana a semana."""
    difs = []
    for w, idx in S.groupby(semana.loc[S.index]).groups.items():
        sel = ret.loc[idx].dropna()
        if len(sel) < n_min:
            continue
        todas = ret[semana == w].dropna()
        if todas.empty:
            continue
        difs.append(float(sel.median() - todas.median()))
    return np.array(difs)


def _p_bloques_mediana(difs, reps=REPS, seed=0):
    """p de una cola remuestreando semanas enteras. H0: la diferencia es <= 0."""
    k = len(difs)
    if k < 8:
        return 1.0, k
    rng = np.random.default_rng(seed)
    m = np.array([rng.choice(difs, k, replace=True).mean() for _ in range(reps)])
    return float((m <= 0).mean()), k


def _una(A, T, ret, semana, mascara, nombre, n_min):
    m = mascara.reindex(T.index, fill_value=False).fillna(False).astype(bool)
    m &= ret.notna()
    S = T[m]
    n = len(S)
    fila = {"hipotesis": nombre, "n": n}
    if n < N_MIN:
        fila.update(mediana=np.nan, vs_base=np.nan, p=1.0, veredicto="POCA MUESTRA")
        return fila

    r = ret[m]
    base = ret.dropna()
    fila["mediana"] = float(r.median())
    fila["vs_base"] = float(r.median() - base.median())

    # dardo pareado: mismos simbolos, mismos pesos, cualquier momento
    sym = A.loc[T["aid"].to_numpy(), "symbol"].to_numpy()
    sym = pd.Series(sym, index=T.index)
    peso = sym[m].value_counts()
    med_sym = base.groupby(sym.loc[base.index]).median()
    comun = peso.index.intersection(med_sym.index)
    par = float((med_sym[comun] * peso[comun]).sum() / peso[comun].sum()) if len(comun) else np.nan
    fila["vs_pareado"] = float(r.median() - par)

    # concentracion: sacar los simbolos con MAS alertas
    for k in (1, TOP_N):
        fuera = peso.head(k).index
        sin = r[~sym[m].isin(fuera).to_numpy()]
        fila[f"vs_base_sin_top{k}"] = (float(sin.median() - base.median())
                                       if len(sin) else np.nan)

    difs = _difs_semanales(S, ret, semana, n_min)
    fila["p"], fila["semanas"] = _p_bloques_mediana(difs)
    fila["semanas_todas"] = int(semana.loc[S.index].nunique())
    fila["sem_ok"] = float((difs > 0).mean()) if len(difs) else np.nan
    fila["dif_semanal"] = float(difs.mean()) if len(difs) else np.nan
    return fila


def lote_mediana(A, T, H, ret, costo, q=0.10, n_min=5, mostrar=True, titulo=""):
    """Corre todas las hipotesis contra la MEDIANA y aplica las compuertas."""
    semana = T["semana"]
    filas = [_una(A, T, ret, semana,
                  m if isinstance(m, pd.Series) else pd.Series(m, index=T.index),
                  k, n_min)
             for k, m in H.items()]
    D = pd.DataFrame(filas)

    vivas = D["veredicto"].isna() if "veredicto" in D else pd.Series(True, index=D.index)
    D["fdr_ok"] = False
    if vivas.any():
        D.loc[vivas, "fdr_ok"] = _bh(D.loc[vivas, "p"].to_numpy(), q)

    def veredicto(r):
        if r.get("veredicto") == "POCA MUESTRA":
            return f"POCA MUESTRA (n<{N_MIN})"
        if not (r["vs_base"] > 0):
            return "no supera la linea base"
        if not r["fdr_ok"]:
            return f"muere en la correccion (FDR q={q})"
        if not (r["vs_pareado"] > 0):
            return "es seleccion de moneda, no timing"
        if not (r["vs_base_sin_top3"] > 0):
            return f"concentracion: se cae sin el top-{TOP_N}"
        if not (r["vs_base_sin_top1"] > 0):
            return "un solo par la sostiene"
        if not (r["sem_ok"] >= SEM_MIN_MEDIANA):
            return f"inconsistente por semana ({100*r['sem_ok']:.0f}%)"
        return "SOBREVIVE"

    D["veredicto"] = D.apply(veredicto, axis=1)
    D = D.sort_values("vs_base", ascending=False, na_position="last").reset_index(drop=True)

    if mostrar:
        base = ret.dropna()
        print("\n" + "=" * 108)
        print(f"GATE DE MEDIANA {titulo} — {len(D)} hipotesis  |  costo {costo:.2f}%  |  "
              f"linea base mediana {base.median():+.2f}%  media {base.mean():+.2f}%")
        print(f"  (n_min={n_min} alertas para que una semana cuente)")
        print("=" * 108)
        print(f"{'hipotesis':30s} {'n':>6s} {'medi':>7s} {'vsbase':>7s} {'vspar':>7s} "
              f"{'sin3':>7s} {'sem':>5s} {'sems':>5s} {'p':>7s}  veredicto")
        print("-" * 116)
        for _, r in D.head(20).iterrows():
            f = lambda v: ("  --  " if pd.isna(v) else f"{v:+.2f}")  # noqa: E731
            print(f"{str(r.hipotesis)[:30]:30s} {r.n:6,d} "
                  f"{f(r.get('mediana')):>7s} {f(r.get('vs_base')):>7s} "
                  f"{f(r.get('vs_pareado')):>7s} {f(r.get('vs_base_sin_top3')):>7s} "
                  f"{'  -- ' if pd.isna(r.get('sem_ok')) else f'{100*r.sem_ok:4.0f}%'} "
                  f"{0 if pd.isna(r.get('semanas')) else int(r['semanas']):5d} "
                  f"{r.p:7.4f}  {r.veredicto}")
        print("-" * 108)
        viven = int((D.veredicto == "SOBREVIVE").sum())
        print(f"SOBREVIVEN {viven} de {len(D)}")
    return D


def barrido_nmin(A, T, H, ret, nombre, valores=(20, 15, 10, 5, 3, 1)):
    """El veredicto de UN brazo segun donde se ponga el filtro de actividad semanal.

    Es la leccion de la corrida anterior convertida en herramienta: un brazo cuyo
    veredicto depende de `n_min` no es un hallazgo, es el filtro.
    """
    semana = T["semana"]
    m = H[nombre]
    m = (m if isinstance(m, pd.Series) else pd.Series(m, index=T.index))
    m = m.reindex(T.index, fill_value=False).fillna(False).astype(bool) & ret.notna()
    S = T[m]
    print(f"\n  barrido de n_min — {nombre}")
    print(f"    {'n_min':>6s} {'semanas':>8s} {'dif media':>10s} {'p':>8s}")
    for nm in valores:
        difs = _difs_semanales(S, ret, semana, nm)
        p, k = _p_bloques_mediana(difs)
        d = difs.mean() if len(difs) else float("nan")
        print(f"    {nm:6d} {k:8d} {d:+10.2f} {p:8.4f}  "
              f"{'sobrevive' if p < 0.10 else 'muere'}")
