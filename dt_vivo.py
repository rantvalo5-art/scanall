"""
DT VIVO — medir un sistema EN PRODUCCION con el metodo del banco.

    py -3.13 -u dt_vivo.py --sistema daytrader   # outcomes_dump.json
    py -3.13 -u dt_vivo.py --sistema swing       # screener_outcomes_dump.json

Las dos tablas NO son la misma y el docstring de `screener.py:320` miente al respecto:
el daytrader escribe a `daytrader_outcomes` (screener.py:354) y el swing a
`screener_outcomes` (swing/screener.py:339).

OJO CON EL SWING: su filler solo calcula hasta 24h (`swing/update_outcomes.py:158-161`),
que son los horizontes del DAYTRADER. Un sistema que opera en 1h/4h/1d/1w no se juega en
24h, asi que lo que se mide del swing es si la entrada se va en contra enseguida — NO su
tesis. Para medir eso haria falta instrumentar horizontes de dias.


Por que existe: los analizadores que ya estan (`dt_analyze.py`, `analyze_bests.py`,
`find_my_edge.py`) son DESCRIPTIVOS y son de antes del banco. Cuentan win rate y ordenan
buckets. La leccion de las 16 corridas es que eso alcanza para engañarse: con alertas
solapadas el n contado no es el n efectivo, y un win rate lindo sobre 14.000 filas puede
ser ruido de 9 semanas. Aca se aplican las mismas reglas que el banco:

  1. LA COMPUERTA PRIMERO. n en SEMANAS y MDE antes de mirar el efecto.
  2. El p que decide es el de BLOQUES por semana, no el binomial.
  3. El costo va en las MISMAS unidades que el ruido.

    $env:PYTHONIOENCODING = "utf-8"
    py -3.13 -u dt_vivo.py

QUE SE MIDE Y QUE NO, que es la mitad del trabajo (taxonomia de `screener.py` §1):

    PRE-BREAK   5m cerca de romper                    -> ENTRADA
    BREAKOUT    15m rompe maximo con volumen          -> ENTRADA
    EXPLOSION   (no esta en el header, si en el dato)  -> ENTRADA
    RIDING      "breakout que sigue subiendo,
                 SE REPITE CADA RUN"                  -> NO es entrada nueva
    HOLD        rompe y sostiene la zona              -> continuacion
    FADING      "avisa una vez para SALIR"            -> es una SALIDA

FADING es el 75% de las filas (11.004 de 14.736). Medir el retorno desde una alerta de
salida como si fuera un trade no significa nada, y es el error que hace que el registro
parezca 5x mas grande de lo que es. RIDING y HOLD repiten sobre la MISMA posicion, asi
que suman filas y no suman trades.

El screener es LONG ONLY: no hay una sola rama short en `screener.py`. Todos los retornos
se leen en largo.
"""
import argparse
import json
import sys

import numpy as np
import pandas as pd

COSTO_PCT = 0.20          # ida y vuelta, el mismo de banco/primer_toque.py

# Taxonomia por sistema. Es la mitad del trabajo: contar una SALIDA o una
# CONTINUACION como si fuera un trade nuevo es lo que hace que el registro del
# daytrader parezca 10x mas grande de lo que es.
SISTEMAS = {
    "daytrader": {
        "fuente": "outcomes_dump.json",
        "entradas": ("BREAKOUT", "EXPLOSION", "PREBREAK"),
        "salidas": ("FADING",),
    },
    # swing/screener.py:326 guarda `history_tf` en la columna `signal_type`, y el
    # nombre de la variable engana: sus VALORES son nombres de senal, no timeframes
    # (se ve en swing/screener.py:551, `if alert["history_tf"] not in ("RIDING",
    # "HOLD")`). El swing no tiene senal de salida: no hay FADING.
    "swing": {
        "fuente": "screener_outcomes_dump.json",
        "entradas": ("BREAKOUT", "PREBREAK", "COILING"),
        "salidas": (),
    },
}
HORIZONTES = ("15m", "1h", "4h", "24h")
DEDUP_H = 24              # dos alertas del mismo par dentro de esto son EL MISMO trade
Z = 2.80                  # 1,96 (alfa 0,05 dos colas) + 0,84 (80% de potencia)
REPS = 5000
SEM_MIN = 8               # semanas minimas para que el bootstrap signifique algo


def cargar(fuente):
    with open(fuente, encoding="utf-8") as f:
        d = json.load(f)
    if isinstance(d, dict):                      # volcados viejos venian {"main": [...]}
        d = next((v for v in d.values() if isinstance(v, list) and v), [])
    D = pd.DataFrame(d)
    D["dt"] = pd.to_datetime(D["alerted_at"], format="ISO8601", utc=True)
    D["semana"] = D["dt"].dt.strftime("%G-W%V")
    return D.sort_values("dt").reset_index(drop=True)


def deduplicar(D, horas=DEDUP_H):
    """Una entrada por (par, senal) cada `horas`. Sin esto el n aparente se infla.

    No es una compuerta aflojada: es que dos alertas del mismo par a media hora de
    distancia NO son dos observaciones independientes del mismo fenomeno, son la misma
    posicion avisada dos veces.
    """
    D = D.sort_values("dt")
    keep = []
    ultimo = {}
    for i, r in zip(D.index, D.itertuples()):
        k = (r.symbol, r.signal_type)
        if k not in ultimo or (r.dt - ultimo[k]).total_seconds() >= horas * 3600:
            keep.append(i)
            ultimo[k] = r.dt
    return D.loc[keep]


def retornos(D):
    """Retorno NETO por horizonte, en % del precio de entrada. Long only."""
    e = D["entry_price"].astype(float)
    out = {}
    for h in HORIZONTES:
        c = f"price_{h}"
        out[f"r_{h}"] = (D[c].astype(float) / e - 1.0) * 100.0 - COSTO_PCT
    # mejor y peor salida disponibles en la ventana de 4h (bruto, sin costo:
    # son referencias de recorrido, no posiciones que se cobren)
    out["mfe_4h"] = (D["max_high_4h"].astype(float) / e - 1.0) * 100.0
    out["mae_4h"] = (D["min_low_4h"].astype(float) / e - 1.0) * 100.0
    return D.assign(**out)


def por_semana(D, col):
    """Media por semana. Cada semana pesa UNO, sin importar cuantas alertas trajo."""
    return D.dropna(subset=[col]).groupby("semana")[col].mean()


def boot_p(sem, reps=REPS, seed=0):
    """p de BLOQUES: remuestrea SEMANAS enteras. Nula = media <= 0."""
    k = len(sem)
    if k < SEM_MIN:
        return float("nan")
    rng = np.random.default_rng(seed)
    v = sem.to_numpy()
    return float((np.array([rng.choice(v, k, True).mean()
                            for _ in range(reps)]) <= 0).mean())


def compuerta(D):
    """(C) La potencia, ANTES de mirar el efecto. Devuelve el MDE por horizonte."""
    print("\n" + "=" * 84)
    print("(C) LA COMPUERTA — n y sigma primero. Todavia NO se mira ningun retorno.")
    print("=" * 84)
    print(f"  MDE = {Z} * sigma_semanal / sqrt(n_semanas)   [% por trade, neto]")
    print(f"\n  {'horizonte':>10}{'alertas':>9}{'semanas':>9}{'sigma sem':>11}"
          f"{'MDE % / trade':>15}")
    mde = {}
    for h in HORIZONTES:
        s = por_semana(D, f"r_{h}")
        m = Z * float(s.std(ddof=1)) / np.sqrt(len(s)) if len(s) > 1 else np.inf
        mde[h] = m
        print(f"  {h:>10}{int(D[f'r_{h}'].notna().sum()):>9}{len(s):>9}"
              f"{s.std(ddof=1):>11.2f}{m:>15.2f}")
    print(f"\n  costo descontado: {COSTO_PCT:.2f}% ida y vuelta")
    print("  Un efecto mas chico que su MDE no se distingue del azar con este registro.")
    return mde


def efecto(D, mde):
    print("\n" + "=" * 84)
    print("EL EFECTO — recien ahora")
    print("=" * 84)
    print(f"  {'horizonte':>10}{'media sem':>11}{'mediana':>10}{'sem>0':>8}"
          f"{'p bloques':>11}{'MDE':>8}   veredicto")
    filas = []
    for h in HORIZONTES:
        s = por_semana(D, f"r_{h}")
        med = float(s.mean())
        p = boot_p(s)
        ok = abs(med) > mde[h]      # |efecto| contra el MDE: un negativo tambien se mide
        filas.append({"horizonte": h, "media_semanal": med,
                      "mediana_alerta": float(D[f"r_{h}"].median()),
                      "semanas_pos": float((s > 0).mean()), "p_bloques": p,
                      "mde": mde[h], "supera_mde": ok})
        v = ("dentro del MDE — NO se distingue de cero" if not ok else
             "POSITIVO, supera el MDE" if med > 0 else
             "NEGATIVO, supera el MDE")
        print(f"  {h:>10}{med:>+11.2f}{D[f'r_{h}'].median():>+10.2f}"
              f"{(s > 0).mean():>7.0%}{p:>11.4f}{mde[h]:>8.2f}   {v}")
    return pd.DataFrame(filas)


def recorrido(D):
    """MFE/MAE: donde esta el problema, en la ENTRADA o en la SALIDA."""
    print("\n" + "=" * 84)
    print("RECORRIDO A 4h — la entrada o la salida?")
    print("=" * 84)
    mfe, mae, r4 = D["mfe_4h"].median(), D["mae_4h"].median(), D["r_4h"].median()
    print(f"  mejor salida disponible (MFE mediano)   {mfe:+.2f}%")
    print(f"  peor momento          (MAE mediano)     {mae:+.2f}%")
    print(f"  lo que se cobra al cierre de 4h         {r4:+.2f}%  (neto de costo)")
    print(f"  asimetria MFE/|MAE|                     {mfe / abs(mae):.2f}"
          f"   (1,00 = simetrico = una moneda)")
    if mfe <= COSTO_PCT:
        print(f"\n  --> Ni siquiera hay {COSTO_PCT:.2f}% disponible en el mejor momento:")
        print("      el problema es la ENTRADA, no hay movimiento que cobrar.")
    else:
        print(f"\n  --> Movimiento HAY ({mfe:+.2f}% disponible). Pero la caida disponible")
        print(f"      es {mae:+.2f}%, casi igual de grande: la alerta agarra VOLATILIDAD,")
        print("      no direccion. Es 'forma, no expectativa', que HANDOFF_CIERRE ya")
        print("      documento cinco veces en el banco.")
        print("\n  OJO — lo que este dato NO puede decir: si el MFE llega ANTES o DESPUES")
        print("  que el MAE. Un trailing solo sirve si lo favorable viene primero, y con")
        print("  el max/min de la ventana eso es INDECIDIBLE. Haria falta el instante de")
        print("  cada extremo, que no se esta guardando. Sin eso, 'poner un trailing' es")
        print("  una hipotesis para preregistrar, no una conclusion de esta medicion.")


def ordena_el_score(D):
    """El score ordena el resultado? Correlacion de rangos DENTRO de cada semana."""
    print("\n" + "=" * 84)
    print("EL SCORE ORDENA? — correlacion de rangos por semana (Spearman)")
    print("=" * 84)
    for h in ("4h", "24h"):
        rs = []
        for _, g in D.dropna(subset=[f"r_{h}", "score"]).groupby("semana"):
            if len(g) >= 10 and g["score"].nunique() > 1:
                rs.append(g["score"].corr(g[f"r_{h}"], method="spearman"))
        if len(rs) < SEM_MIN:
            print(f"  {h:>4}  solo {len(rs)} semanas utiles: no alcanza")
            continue
        s = pd.Series(rs)
        p = boot_p(s)
        print(f"  {h:>4}  rho medio {s.mean():+.4f}  |  {len(rs)} semanas  |  "
              f"{(s > 0).mean():.0%} positivas  |  p bloques {p:.4f}"
              f"   {'ORDENA' if p < 0.05 else 'no ordena'}")
    print("\n  por bucket (retorno neto a 4h, media por semana):")
    for b, g in D.groupby("bucket"):
        s = por_semana(g, "r_4h")
        if len(s) >= SEM_MIN:
            print(f"    {b:>8}  {len(g):5} alertas  {s.mean():+7.2f}%  "
                  f"({(s > 0).mean():.0%} de semanas arriba)")


def main():
    ap = argparse.ArgumentParser(description="Medir un sistema en vivo")
    ap.add_argument("--sistema", choices=sorted(SISTEMAS), default="daytrader")
    ap.add_argument("--fuente", default=None, help="override del JSON de entrada")
    a = ap.parse_args()
    cfg = SISTEMAS[a.sistema]
    ENTRADAS, SALIDAS = cfg["entradas"], cfg["salidas"]

    D = cargar(a.fuente or cfg["fuente"])
    print("=" * 84)
    print(f"{a.sistema.upper()} EN VIVO — el registro real, con el metodo del banco")
    print("=" * 84)
    print(f"  {len(D):,} filas  |  {D.dt.min():%Y-%m-%d} -> {D.dt.max():%Y-%m-%d}  |  "
          f"{D.semana.nunique()} semanas")
    print("\n  filas por tipo de senal:")
    for s, n in D.signal_type.value_counts().items():
        rol = ("ENTRADA" if s in ENTRADAS else
               "SALIDA — no es un trade" if s in SALIDAS else
               "continuacion — no es entrada nueva")
        print(f"    {s:>10} {n:6}   {rol}")

    E = D[D.signal_type.isin(ENTRADAS)]
    print(f"\n  -> entradas: {len(E):,} de {len(D):,} filas")
    E = deduplicar(E)
    print(f"  -> tras deduplicar (una por par+senal cada {DEDUP_H}h): {len(E):,}")
    if E.semana.nunique() < SEM_MIN:
        sys.exit(f"\nSolo {E.semana.nunique()} semanas: por debajo del minimo de {SEM_MIN}.")
    E = retornos(E)

    mde = compuerta(E)
    R = efecto(E, mde)
    recorrido(E)
    ordena_el_score(E)

    R.to_csv(f"dt_vivo_{a.sistema}.csv", index=False)
    print("\n" + "=" * 84)
    print(f"  {E.semana.nunique()} semanas independientes. -> dt_vivo_{a.sistema}.csv")
    print("=" * 84)
    return 0


if __name__ == "__main__":
    sys.exit(main())
