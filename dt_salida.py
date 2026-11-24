"""
DT SALIDA — el chequeo de PREREGISTRO_SALIDA.md: el MFE llega ANTES que el MAE?

    python dt_salida.py                       # outcomes_dump.json (lo baja dump_outcomes.py)
    python dt_salida.py --prueba              # SOLO datos sinteticos en memoria, sin Supabase
    python dt_salida.py --prueba sesgo        # un caso sintetico: moneda | sesgo | ruido

En Actions: workflow 'dt_salida' -> Run workflow. Baja la tabla con `dump_outcomes.py` y
corre esto; el resultado queda en el log del run, no se commitea nada.

Hace lo de §3 y §4 del preregistro y NADA mas (§5):

  1. CANDADO: antes del 2026-11-24 UTC no corre (§5: no se escribe ni se mira antes).
  2. Universo y deduplicacion de `dt_vivo.py`, IMPORTADOS de ahi, no copiados:
     `signal_type` en BREAKOUT/EXPLOSION/PREBREAK, una por (par, senal) cada 24h.
  3. Ventana: la semana de la instrumentacion (2026-09-01, martes, 2026-W36) NO cuenta
     (§2). Desde el primer lunes completo posterior, 2026-09-07 00:00 UTC, y solo semanas
     ISO (lunes a domingo, `%G-W%V`, como `dt_vivo.cargar`) COMPLETAS antes de la fecha:
     hasta 2026-11-23 00:00 UTC exclusivo. Son 2026-W37..2026-W47 = 11 semanas, no las 12
     que dice §4 (12 semanas desde el 09-01 incluyen la W36 descartada). Se declara en la
     salida y no se espera ni se mueve la fecha.
  4. `mfe_min_4h` y `mae_min_4h` no nulos. NO se rellena hacia atras (§5). Se reporta
     cuantas quedaron afuera por nulos.
  5. Empates (mismo minuto) se descartan y se reportan (§3).
  6. P = fraccion con mfe_min_4h < mae_min_4h, DENTRO de cada semana; cada semana pesa uno.
  7. (P) POTENCIA primero: n, sigma_semanal(P), MDE = 2,8 * sigma / sqrt(n). Si MDE > 0,05
     -> NO SE PUDO MEDIR (por n o por sigma) y P no se interpreta.
     Recien con MDE <= 0,05: media semanal de P, bootstrap por bloques de semanas y el
     veredicto contra (C) P > 0,55, sin redondear. P < 0,50 NO se lee como short (§5).

DECISIONES QUE EL PREREGISTRO NO FIJA — todas tomadas del lado conservador:

  a. Orden de los filtros: universo -> deduplicacion sobre TODO el historial -> ventana
     -> nulos -> empates. Deduplicar antes de la ventana es lo que hace `dt_vivo.py` (y
     evita contar como entrada nueva una repeticion de una alerta de la W36); deduplicar
     antes de los nulos evita que tirar una fila nula "libere" a su repeticion como
     entrada.
  b. El p del bootstrap se calcula contra 0,55 (el umbral de (C)), NO contra 0,50.
     Nula: media semanal de P <= 0,55. Contra 0,50 seria mas facil pasar.
  c. Alfa: una cola al 0,025, que es lo que corresponde al 1,96 del 2,8 del MDE (dos
     colas al 0,05). Pasa (C) solo si la media semanal > 0,55 ESTRICTO **y** p < 0,025.
     Media > 0,55 con p >= 0,025 es NO PASA.
  d. Repeticiones del bootstrap: se reusa `dt_vivo.boot_p` tal cual (REPS = 5.000,
     semilla 0, minimo 8 semanas). El pedido decia 2.000 "como el resto del repo", pero
     el repo usa 5.000 (`dt_vivo.py`, `swing/medir_7d.py`, `opciones/medir_prima.py`).
     Mas repeticiones solo reducen el ruido de Monte Carlo; no mueven el p a favor.
  e. n o sigma (regla de la corrida 9): si el MDE no pasa, se compara sigma_semanal con
     el a priori de §4 (0,06). Si sigma <= 0,06, fallo por n y se reabre esperando las
     semanas que pide la cuenta. Si sigma > 0,06, fallo por sigma y NO se reabre
     esperando. Atribuirlo a sigma ante la duda es lo que cierra la puerta a "esperar un
     poco mas" hasta que de.
  f. Con menos de 8 semanas con P (`dt_vivo.SEM_MIN`) el bootstrap no significa nada:
     NO SE PUDO MEDIR por n, en la compuerta de potencia, antes de mirar P.
  g. Semanas con pocas entradas validas pesan uno igual (§3); no se fija un minimo por
     semana porque seria un parametro libre. Una semana con 0 validas no tiene P y queda
     afuera; se reporta.
"""
import argparse
import os
import sys
from datetime import datetime, timezone

import numpy as np
import pandas as pd

# Universo, deduplicacion y bootstrap de bloques: los MISMOS de dt_vivo.py (§3).
from dt_vivo import SISTEMAS, Z, SEM_MIN, REPS, cargar, deduplicar, boot_p

FECHA_MIRAR = datetime(2026, 11, 24, tzinfo=timezone.utc)      # §4 "La fecha"
INSTRUMENTACION = pd.Timestamp("2026-09-01", tz="UTC")           # §2 / §4
DESDE = pd.Timestamp("2026-09-07", tz="UTC")   # primer lunes completo posterior (§2)
HASTA = pd.Timestamp("2026-11-23", tz="UTC")   # exclusivo: lunes de la semana en curso
SEMANAS_PREREGISTRO = 12                        # lo que dice §4
UMBRAL_C = 0.55                                 # (C), fijo. No se redondea ni se afloja
MDE_MAX = 0.05                                  # (P)
SIGMA_A_PRIORI = 0.06                           # la cuenta de §4
ALFA_UNA_COLA = 0.025                           # decision (c) del docstring
ENTRADAS = SISTEMAS["daytrader"]["entradas"]    # BREAKOUT, EXPLOSION, PREBREAK
COLS = ("mfe_min_4h", "mae_min_4h")


def candado():
    hoy = datetime.now(timezone.utc)
    if hoy < FECHA_MIRAR:
        sys.exit(f"CANDADO: hoy es {hoy:%Y-%m-%d} UTC, antes del {FECHA_MIRAR:%Y-%m-%d}.\n"
                 "PREREGISTRO_SALIDA.md §5 no autoriza correr dt_salida.py ni mirar "
                 "mfe_min_4h mientras se acumula. Es la regla que ahorro las corridas "
                 "8, 9, 13 y 14.")


def semanas_esperadas():
    return pd.date_range(DESDE, HASTA - pd.Timedelta(days=1), freq="7D") \
             .strftime("%G-W%V").tolist()


def construir(D):
    """§3: universo -> dedup -> ventana -> nulos -> empates. Devuelve (E, conteos)."""
    c = {"filas": len(D)}
    E = D[D.signal_type.isin(ENTRADAS)]
    c["universo"] = len(E)
    E = deduplicar(E)
    c["dedup"] = len(E)
    E = E[(E.dt >= DESDE) & (E.dt < HASTA)]
    c["ventana"] = len(E)
    faltan = [k for k in COLS if k not in E.columns]
    if faltan:
        sys.exit(f"FATAL: no existen las columnas {faltan}. El SQL de §2 no se corrio "
                 "o el volcado es viejo: el reloj de §4 no arranco.")
    nulas = E[list(COLS)].isna().any(axis=1)
    c["nulas"] = int(nulas.sum())
    c["nulas_sem"] = E[nulas].groupby("semana").size()
    E = E[~nulas].copy()
    E["mfe_min_4h"] = E["mfe_min_4h"].astype(float)
    E["mae_min_4h"] = E["mae_min_4h"].astype(float)
    emp = E["mfe_min_4h"] == E["mae_min_4h"]
    c["empates"] = int(emp.sum())
    E = E[~emp].copy()
    c["validas"] = len(E)
    E["mfe_primero"] = (E["mfe_min_4h"] < E["mae_min_4h"]).astype(float)
    return E, c


def medir(D):
    E, c = construir(D)
    esperadas = semanas_esperadas()
    print("=" * 84)
    print("PREREGISTRO_SALIDA — el MFE llega ANTES que el MAE?   P = frac(mfe_min < mae_min)")
    print("=" * 84)
    print(f"  ventana: {DESDE:%Y-%m-%d} -> {HASTA:%Y-%m-%d} (exclusivo), semanas ISO completas")
    print(f"  la semana de la instrumentacion ({INSTRUMENTACION:%Y-%m-%d}, "
          f"{INSTRUMENTACION:%G-W%V}) NO cuenta (§2)")
    print(f"  OJO: hay {len(esperadas)} semanas completas, no las {SEMANAS_PREREGISTRO} "
          "que menciona §4.")
    print("       No se espera ni se mueve la fecha; la compuerta de potencia decide.")
    print(f"\n  {c['filas']:>7,} filas en la tabla")
    print(f"  {c['universo']:>7,} en el universo {'/'.join(ENTRADAS)}")
    print(f"  {c['dedup']:>7,} tras deduplicar (una por par+senal cada 24h, dt_vivo.py)")
    print(f"  {c['ventana']:>7,} dentro de la ventana")
    print(f"  {c['nulas']:>7,} afuera por mfe_min_4h / mae_min_4h nulos (NO se rellenan, §5)")
    print(f"  {c['empates']:>7,} empates (mismo minuto), descartados (§3)")
    print(f"  {c['validas']:>7,} entradas validas")

    # ---- (P) POTENCIA, antes de mirar P --------------------------------------------
    print("\n" + "=" * 84)
    print("(P) POTENCIA — n y sigma primero. Todavia NO se mira P.")
    print("=" * 84)
    n_sem = E.groupby("semana").size()
    print(f"  {'semana':>10}{'entradas':>10}{'nulas':>8}")
    for s in esperadas:
        print(f"  {s:>10}{int(n_sem.get(s, 0)):>10}{int(c['nulas_sem'].get(s, 0)):>8}")
    otras = sorted(set(n_sem.index) - set(esperadas))
    if otras:                                       # no deberia pasar nunca
        sys.exit(f"FATAL: semanas fuera de la ventana: {otras}")
    sin = [s for s in esperadas if n_sem.get(s, 0) == 0]
    if sin:
        print(f"\n  semanas sin ninguna entrada valida (sin P, quedan afuera): {sin}")

    P = E.groupby("semana")["mfe_primero"].mean()   # cada semana pesa UNO
    k = len(P)
    if k < 2:
        print(f"\n  n_semanas = {k}: no hay sigma.")
        print("\n  >>> NO SE PUDO MEDIR — falla por n.")
        return {"veredicto": "NO SE PUDO MEDIR", "falla": "n", "n_semanas": k}
    sig = float(P.std(ddof=1))
    mde = Z * sig / np.sqrt(k)
    print(f"\n  n_semanas            {k}")
    print(f"  entradas por semana  media {n_sem.mean():.1f}  |  min {n_sem.min()}  |  "
          f"max {n_sem.max()}")
    print(f"  sigma_semanal(P)     {sig:.4f}   (a priori §4: {SIGMA_A_PRIORI:.2f})")
    print(f"  MDE = {Z} * {sig:.4f} / sqrt({k}) = {mde:.4f}   (tope {MDE_MAX:.2f})")

    if not mde <= MDE_MAX:
        n_nec = int(np.ceil((Z * sig / MDE_MAX) ** 2))
        if sig <= SIGMA_A_PRIORI:
            falla = "n"
            motivo = (f"sigma {sig:.4f} <= {SIGMA_A_PRIORI:.2f} (a priori): falta n. "
                      f"Con esta sigma harian falta {n_nec} semanas. Se reabre ESPERANDO.")
        else:
            falla = "sigma"
            motivo = (f"sigma {sig:.4f} > {SIGMA_A_PRIORI:.2f} (a priori): el ruido entre "
                      f"semanas es mas grande que lo previsto (pediria {n_nec} semanas). "
                      "NO se reabre esperando (regla de la corrida 9).")
        print(f"\n  >>> NO SE PUDO MEDIR — falla por {falla.upper()}.")
        print(f"      {motivo}")
        print("      P no se mira ni se interpreta.")
        return {"veredicto": "NO SE PUDO MEDIR", "falla": falla, "n_semanas": k,
                "sigma": sig, "mde": mde}
    if k < SEM_MIN:
        print(f"\n  >>> NO SE PUDO MEDIR — falla por N: {k} semanas, el bootstrap de bloques")
        print(f"      pide al menos {SEM_MIN} (dt_vivo.SEM_MIN). Se reabre ESPERANDO.")
        print("      P no se mira ni se interpreta.")
        return {"veredicto": "NO SE PUDO MEDIR", "falla": "n", "n_semanas": k,
                "sigma": sig, "mde": mde}
    print(f"\n  -> MDE {mde:.4f} <= {MDE_MAX:.2f}: se puede medir. Recien ahora se mira P.")

    # ---- (C) EL EFECTO ---------------------------------------------------------------
    print("\n" + "=" * 84)
    print("(C) EL EFECTO — recien ahora")
    print("=" * 84)
    media = float(P.mean())
    # Nula: media semanal de P <= 0,55. boot_p de dt_vivo prueba "media <= 0", asi que se
    # le pasa P - 0,55: es el mismo remuestreo de semanas enteras.
    p = boot_p(P - UMBRAL_C, reps=REPS)
    print(f"  {'semana':>10}{'P':>8}")
    for s, v in P.items():
        print(f"  {s:>10}{v:>8.4f}")
    print(f"\n  media semanal de P            {media:.4f}")
    print(f"  semanas con P > {UMBRAL_C:.2f}          {(P > UMBRAL_C).mean():.0%}")
    print(f"  p bloques (nula P <= {UMBRAL_C:.2f})    {p:.4f}   "
          f"({REPS:,} repeticiones, una cola, alfa {ALFA_UNA_COLA})")

    if media > UMBRAL_C and p < ALFA_UNA_COLA:
        ver = "PASA (C)"
        print(f"\n  >>> {ver}: P > {UMBRAL_C:.2f} y se distingue de {UMBRAL_C:.2f}.")
        print("      NO es un negocio todavia (§6): es permiso para escribir OTRO")
        print("      preregistro, el de la regla de salida, con sus propias compuertas.")
        print("      Esto no autoriza barrer objetivos, stops ni ventanas (§5).")
    else:
        ver = "NO PASA (C)"
        print(f"\n  >>> {ver}: se necesitaba media > {UMBRAL_C:.2f} y p < {ALFA_UNA_COLA}.")
        print("      Ninguna regla de salida construida sobre este orden paga el costo:")
        print("      la direccion se CIERRA (§4). El umbral no se afloja (§5).")
        if media < 0.50:
            print(f"      P = {media:.4f} < 0,50: NO se reinterpreta como senal short. Seria")
            print("      otra hipotesis y necesita su propio preregistro (§3, §5).")
    return {"veredicto": ver, "n_semanas": k, "sigma": sig, "mde": mde,
            "media": media, "p": p}


# ---- --prueba: datos sinteticos en memoria, nunca Supabase -----------------------------
CASOS = {
    #          P medio, sd entre semanas, entradas por semana
    "moneda": (0.50, 0.00, 139),    # esperado: pasa la potencia y NO PASA (C)
    "sesgo":  (0.60, 0.01, 139),    # esperado: pasa la potencia y PASA (C)
    "ruido":  (0.60, 0.15, 139),    # esperado: NO SE PUDO MEDIR por sigma
}


def sintetico(p_medio, sd_sem, por_semana, seed=1):
    """Filas con la forma de `daytrader_outcomes`, con trampas a proposito: FADING/RIDING
    fuera del universo, repeticiones dentro de 24h, nulos, empates y filas en la W36 y la
    W48, que tienen que quedar afuera."""
    rng = np.random.default_rng(seed)
    filas = []
    lunes = pd.date_range("2026-08-31", "2026-11-23", freq="7D", tz="UTC")  # W36..W48
    for lu in lunes:
        p_w = float(np.clip(rng.normal(p_medio, sd_sem), 0, 1))
        for j in range(por_semana):
            t = lu + pd.Timedelta(minutes=int(rng.integers(0, 7 * 24 * 60)))
            sym = f"S{j:03d}USDT"          # un par distinto por entrada: no se deduplican
            a, b = sorted(rng.choice(241, 2, replace=False))
            mfe, mae = (a, b) if rng.random() < p_w else (b, a)
            if rng.random() < 0.03:
                mae = mfe                                     # empate
            if rng.random() < 0.05:
                mfe = None                                    # nulo
            st = ENTRADAS[j % 3]
            filas.append(dict(symbol=sym, signal_type=st, alerted_at=t.isoformat(),
                              mfe_min_4h=mfe, mae_min_4h=mae))
            if rng.random() < 0.2:   # repeticion a <24h: el dedup la tiene que tirar
                filas.append(dict(symbol=sym, signal_type=st,
                                  alerted_at=(t + pd.Timedelta(hours=2)).isoformat(),
                                  mfe_min_4h=0, mae_min_4h=240))
            if rng.random() < 0.5:   # FADING/RIDING: fuera del universo
                filas.append(dict(symbol=sym, signal_type=("FADING", "RIDING")[j % 2],
                                  alerted_at=t.isoformat(), mfe_min_4h=0, mae_min_4h=240))
    # misma preparacion que dt_vivo.cargar, sin pasar por un archivo
    D = pd.DataFrame(filas)
    D["dt"] = pd.to_datetime(D["alerted_at"], format="ISO8601", utc=True)
    D["semana"] = D["dt"].dt.strftime("%G-W%V")
    D["mfe_min_4h"] = D["mfe_min_4h"].astype("Float64")
    return D.sort_values("dt").reset_index(drop=True)


def main():
    ap = argparse.ArgumentParser(description="El chequeo de PREREGISTRO_SALIDA.md")
    ap.add_argument("--fuente", default=SISTEMAS["daytrader"]["fuente"],
                    help="volcado JSON de daytrader_outcomes (dump_outcomes.py)")
    ap.add_argument("--prueba", nargs="?", const="todos",
                    choices=sorted(CASOS) + ["todos"],
                    help="solo datos sinteticos en memoria, sin Supabase")
    a = ap.parse_args()
    candado()

    if a.prueba:
        casos = sorted(CASOS) if a.prueba == "todos" else [a.prueba]
        for nombre in casos:
            p_m, sd, n = CASOS[nombre]
            print("\n" + "#" * 84)
            print(f"# PRUEBA SINTETICA '{nombre}': P medio {p_m}, sd semanal {sd}, "
                  f"{n} entradas/semana. NO son datos reales.")
            print("#" * 84)
            medir(sintetico(p_m, sd, n))
        return 0

    if not os.path.exists(a.fuente):
        sys.exit(f"FATAL: no existe {a.fuente}. Bajalo con: python dump_outcomes.py")
    medir(cargar(a.fuente))
    return 0


if __name__ == "__main__":
    sys.exit(main())
