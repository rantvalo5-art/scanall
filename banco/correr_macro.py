"""
CORRIDA 15 — macro / cross-asset como condicionamiento del panel cripto.

Preregistro con la regla de parada: `PREREGISTRO_MACRO.md`, escrito ANTES de evaluar un
solo brazo. Lo unico que se corrio antes fue `macro.py --cobertura`, que cuenta filas y
mide desfase y no mira ningun resultado.

    $env:PYTHONIOENCODING = "utf-8"
    py -3.13 -u correr_macro.py

ORDEN OBLIGATORIO, del preregistro §4:

  (P) LA PREMISA   sd_transversal(beta_F) / se(beta_F) > 1,5, o el factor sale del lote.
                   Rankear betas que no se distinguen entre si es rankear ruido de
                   estimacion, y eso es falso AUNQUE EL MDE DIERA LINDO.

  (C) POTENCIA     no se re-pregunta: el MDE de `ranking.py` sale de la NULA, y la nula
                   no sabe con que score se rankea. Es el mismo numero de la corrida 13.
                   Lo que se verifica es (1) no perder semanas por alineacion y (2) el
                   piso absoluto de 45%/ano bruto.

Y la reserva OOS 2024-08-01 -> 2025-08-01 se EXCLUYE de la ventana (preregistro §3): es
lo unico que queda para confirmar cualquier cosa que sobreviva.
"""
import sys
import time

import numpy as np
import pandas as pd

import macro
from klines import load_panel
from ranking import (COSTO_PCT, Q_FDR, _z, controles, evaluar, lote_rankings,
                     tablero)

INICIO, FIN = "2021-08-01", "2026-08-01"
OOS_INI, OOS_FIN = "2024-08-01", "2025-08-01"     # la reserva, se saca
H = 168                    # paso = horizonte, sin solape
K = 20
N_CTRL = 8
Z = 2.80                   # 1,96 + 0,84
HORAS_ANO = 8760
NEUTRAL = "roc_168"        # preregistro §3: la barra es de 168h
P_MIN = 1.5                # umbral de la compuerta (P)
MDE_BRUTO_MAX = 45.0       # %/ano — el piso absoluto de (C), preregistro §4
COB_MIN = 0.90             # semanas de un brazo macro / semanas de la nula


def residualizar(TB, s, neutral=NEUTRAL):
    """Regresion transversal POR BARRA contra el momentum del propio horizonte.

    Misma cuenta que `ranking.scores`: con los dos lados z-scoreados dentro de la barra,
    la beta de esa barra ES la correlacion de esa barra.
    """
    zb = TB.groupby("t")[neutral].transform(_z)
    za = s.groupby(TB["t"]).transform(_z)
    beta = (za * zb).groupby(TB["t"]).transform("mean")
    return za - beta * zb


# signo que la estructura de mercado ya conoce, para la calibracion
ESPERADO = {"NDX": (+1, "activo de riesgo"), "DXY": (-1, "dolar arriba = risk-off"),
            "ORO": (+1, "activo real"), "T10": (-1, "tasas arriba castigan duracion"),
            "VIX": (-1, "vol arriba = risk-off")}


def calibracion(TB, panel, M):
    """El aparato contra estructura de mercado CONOCIDA, antes de creerle nada.

    Regla del repo: un numero que contradice una estructura conocida es un bug hasta que
    se demuestre lo contrario (delato un error de unidades de 100x en OKX). Si la beta de
    cripto al Nasdaq no sale positiva y grande, o la del dolar no sale negativa, lo que
    esta roto es la alineacion y no el mercado.

    NO es una hipotesis: es contemporanea y descriptiva. No mira ningun retorno futuro.
    """
    print(f"\n  {'factor':>7}{'beta med.':>11}{'% beta>0':>10}{'corr BTC':>10}"
          f"   esperado")
    PD = macro.panel_diario(panel)
    r = M.index[M.index.isin(PD.index)]
    RC = np.log(PD.reindex(r)["BTCUSDT"]).diff()
    RF = np.log(M.reindex(r)).diff()
    ok = 0
    for f, (signo, motivo) in ESPERADO.items():
        b = float(TB[f"beta_{f}"].median())
        pos = float((TB[f"beta_{f}"] > 0).mean())
        c = float(RC.corr(RF[f]))
        bien = np.sign(b) == signo
        ok += bien
        print(f"  {f:>7}{b:>+11.3f}{pos:>9.1%}{c:>+10.3f}   "
              f"{'+' if signo > 0 else '-'} {motivo}{'' if bien else '   <-- NO'}")
    print(f"\n  {ok} de {len(ESPERADO)} factores reproducen el signo conocido.")
    return ok


def nula_anual(TB, k=K, costo=COSTO_PCT):
    """MDE de la nula real, en ATR y en % anualizado (misma cuenta que corrida 13).

    Los TRES objetivos, y no solo largo/corto como en `horizonte_largo.py`: `y_magnitud`
    = (runup - caida)/atr no esta en la misma escala que `y_largo` = ret/atr, asi que un
    solo MDE escalar deja la compuerta de magnitud inerte. Se devuelve uno por objetivo.

    El % anualizado solo tiene sentido en largo/corto: magnitud no es una posicion (no
    paga costo y no es un retorno), asi que ahi va NaN a proposito.
    """
    C = controles(TB, n=N_CTRL, seed=12345)
    objetivos = ("largo", "corto", "magnitud")
    filas = [evaluar(TB, s, nm, objetivo=o, k=k, costo=costo)
             for o in objetivos for nm, s in C.items()]
    D = pd.DataFrame(filas)
    atr = float(TB["atr_base"].median()) * 100.0
    vueltas = HORAS_ANO / H
    out = []
    for o in objetivos:
        sub = D[D["objetivo"] == o]
        sd = float(sub["sd_sem"].median())
        sem = float(sub["semanas"].median())
        m_atr = Z * sd / np.sqrt(sem)
        out.append({"objetivo": o, "semanas": sem, "sd_sem": sd, "mde_atr": m_atr,
                    "mde_anual": m_atr * atr * vueltas if o != "magnitud" else np.nan})
    return pd.DataFrame(out), atr, vueltas


def main():
    t0 = time.time()
    print("=" * 96)
    print("CORRIDA 15 — MACRO / CROSS-ASSET SOBRE EL PANEL TRANSVERSAL")
    print("=" * 96)
    print(f"ventana {INICIO} -> {FIN}   paso = horizonte = {H}h (sin solape)   top-k={K}")
    print(f"reserva OOS {OOS_INI} -> {OOS_FIN}: EXCLUIDA (preregistro §3)")

    # ------------------------------------------------------------------ datos
    print("\n--- series macro ---")
    M = macro.cargar()

    panel = load_panel(INICIO, FIN, n=200, pin="base200", full=True)
    if not panel:
        print("FATAL: no se pudo cargar el panel")
        return 1

    TB = tablero(panel, paso=H, horizonte=H)

    # ---- la reserva OOS sale ANTES de cualquier cuenta -----------------
    n_antes = TB["t"].nunique()
    dt = pd.to_datetime(TB["t"], unit="ms", utc=True)
    dentro = (dt >= pd.Timestamp(OOS_INI, tz="UTC")) & (dt < pd.Timestamp(OOS_FIN, tz="UTC"))
    TB = TB[~dentro].reset_index(drop=True)
    n_desp = TB["t"].nunique()
    print(f"\nreserva OOS: {n_antes - n_desp} barras descartadas -> quedan {n_desp} "
          f"({TB['semana'].nunique()} semanas, {len(TB):,} filas)")

    print(f"\n--- betas moviles ({macro.VENTANA_BETA} ruedas) ---")
    BETA, SE, RF = macro.betas(panel, M)
    print("\n--- alineacion ---")
    TB, diag = macro.alinear(TB, BETA, SE, RF)

    print("\n" + "=" * 96)
    print("CALIBRACION — el aparato contra estructura de mercado CONOCIDA")
    print("=" * 96)
    calibracion(TB, panel, M)

    # ------------------------------------------------------- (P) LA PREMISA
    print("\n" + "=" * 96)
    print("(P) LA PREMISA — hay seccion cruzada de betas, o se rankea ruido de estimacion")
    print("=" * 96)
    print(f"  criterio preregistrado: mediana por barra de "
          f"sd_transversal(beta) / se(beta) > {P_MIN}")
    print(f"\n  {'factor':>7}{'sd_transv':>12}{'se(beta)':>11}{'razon':>9}"
          f"{'sd_verdadera':>15}   veredicto")
    vivos = []
    for f in macro.SERIES:
        sd_t = TB.groupby("t")[f"beta_{f}"].std(ddof=0)
        se_t = TB.groupby("t")[f"se_{f}"].median()
        sd, se = float(sd_t.median()), float(se_t.median())
        razon = sd / se if se else np.inf
        # varianza observada = verdadera + estimacion
        verdadera = np.sqrt(max(sd**2 - se**2, 0.0))
        pasa = razon > P_MIN
        if pasa:
            vivos.append(f)
        print(f"  {f:>7}{sd:>12.4f}{se:>11.4f}{razon:>9.2f}{verdadera:>15.4f}"
              f"   {'PASA' if pasa else 'NO PASA — sale del lote'}")

    if not vivos:
        print("\n  Ningun factor tiene seccion cruzada por encima del ruido de estimacion.")
        print("  El diseno entero es falso y la corrida se CIERRA aca, sin mirar el MDE.")
        return 0
    print(f"\n  --> {len(vivos)} de {len(macro.SERIES)} factores entran al lote: "
          f"{', '.join(vivos)}")

    # ---------------------------------------------------------- (C) POTENCIA
    print("\n" + "=" * 96)
    print("(C) POTENCIA — heredada de la corrida 13; lo que se verifica es no PERDERLA")
    print("=" * 96)
    N, atr, vueltas = nula_anual(TB)
    costo_anual = COSTO_PCT * vueltas
    MDE = dict(zip(N["objetivo"], N["mde_atr"]))          # uno por objetivo
    dir_ = N[N["objetivo"] != "magnitud"]
    mde_atr = float(dir_["mde_atr"].median())
    mde_anual = float(dir_["mde_anual"].median())
    bruto = mde_anual + costo_anual
    sem_nula = float(N["semanas"].median())
    print(f"  ATR base mediano {atr:.2f}%   {vueltas:.1f} vueltas/ano   "
          f"costo {costo_anual:.2f} %/ano")
    for _, r in N.iterrows():
        an = "" if np.isnan(r.mde_anual) else f" = {r.mde_anual:.1f} %/ano neto"
        print(f"  nula {r.objetivo:9} {r.semanas:.0f} semanas | "
              f"MDE {r.mde_atr:.4f} ATR{an}")
    print(f"\n  EFECTO BRUTO DETECTABLE: {bruto:.1f} %/ano"
          f"   (corrida 13 con las 255 barras: 32,4)")
    print(f"  piso absoluto preregistrado: {MDE_BRUTO_MAX:.0f} %/ano")
    if bruto > MDE_BRUTO_MAX:
        print(f"\n  --> NO PASA. Se aborta como 'no se pudo medir' (preregistro §4).")
        return 0
    print(f"  --> PASA (por {MDE_BRUTO_MAX - bruto:.1f} pp de margen)")

    # ------------------------------------------------------------- el lote
    R = {}
    for f in vivos:
        R[f"beta_{f}"] = TB[f"beta_{f}"]
        R[f"beta_{f} x d_{f}"] = TB[f"beta_{f}"] * TB[f"d_{f}"]
    for nm in list(R):
        R[f"{nm} ~ sin {NEUTRAL}"] = residualizar(TB, R[nm])
    for nm in list(R):
        R[f"{nm} [bajo]"] = -R[nm]

    n_macro = len(R)
    # controles que PUEDEN ganar (regla del repo) + la nula real
    R["CTRL roc_168 [bajo]"] = -TB["roc_168"]        # el fade, la unica familia viva
    R["CTRL atr_24"] = TB["atr_24"]
    R["CTRL roc_24"] = TB["roc_24"]
    R.update(controles(TB, n=3, seed=7))

    print(f"\n{n_macro} scores macro + {len(R) - n_macro} controles"
          f" -> {len(R) * 3} brazos x 3 objetivos")

    D = lote_rankings(TB, R, k=K, costo=COSTO_PCT, q=Q_FDR, mde=MDE)

    # ------------------------------------------------ (C.1) no perder semanas
    print("\n" + "=" * 96)
    print("(C.1) COBERTURA — las semanas de los brazos macro contra las de la nula")
    print("=" * 96)
    es_macro = ~D["ranking"].str.startswith(("CTRL", "CONTROL"))
    sem_macro = float(D.loc[es_macro, "semanas"].median())
    razon = sem_macro / sem_nula if sem_nula else 0.0
    print(f"  nula {sem_nula:.0f} semanas | macro (mediana) {sem_macro:.0f} | "
          f"razon {razon:.3f}   umbral {COB_MIN}")
    print(f"  --> {'PASA' if razon >= COB_MIN else 'NO PASA: el diseno pierde barras '
                                                   'por alineacion'}")

    D.to_csv("rank_macro.csv", index=False)

    # ---------------------------------------------------------- veredicto
    viven = D[(D.veredicto == "SOBREVIVE") & es_macro]
    print("\n" + "=" * 96)
    print("VEREDICTO")
    print("=" * 96)
    if viven.empty:
        print(f"  0 de {int(es_macro.sum())} brazos macro sobreviven.")
        print(f"\n  El cierre esta ACOTADO, y decirlo entero es parte del resultado:")
        print(f"  >> NO hay un efecto macro transversal mayor a {bruto:.0f} %/ano BRUTO")
        print(f"     en 187 pares, {n_desp} barras de 168h y {TB['semana'].nunique()} "
              f"semanas (con la reserva OOS afuera).")
        print("  NO esta establecido que no haya uno mas chico: eso pide anios, no otro")
        print("  estimador. Es la misma resolucion a la que estan cerradas las otras nueve")
        print("  familias (HANDOFF_FUENTES_NUEVAS.md §1).")
        print("\n  La reserva OOS 2024-08 -> 2025-08 NO se toco y sigue virgen.")
    else:
        print(f"  {len(viven)} brazos macro sobreviven las seis compuertas:")
        for _, r in viven.iterrows():
            print(f"    {r.objetivo:9} {r.ranking:38} spread {r.spread:+.4f} "
                  f"p {r.p:.4f}")
        print("\n  NO se les cree todavia. Preregistro §5: van derecho a la reserva OOS")
        print(f"  {OOS_INI} -> {OOS_FIN}, que quedo afuera de la ventana para esto.")

    print(f"\n  ({time.time()-t0:.0f}s)  -> rank_macro.csv")
    print("=" * 96)
    return 0


if __name__ == "__main__":
    sys.exit(main())
