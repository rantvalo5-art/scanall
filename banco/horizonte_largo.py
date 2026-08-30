"""
CORRIDA 13 — compuerta de potencia en horizontes LARGOS (> 1 semana).

Preregistro con la regla de parada: `PREREGISTRO_HORIZONTE_LARGO.md`, escrito antes de
correr una sola configuracion.

    py -3.13 -u horizonte_largo.py

El ultimo hueco declarado del mapa: la corrida 4 cerro el ranking transversal "a
horizontes de 4h a 7d". Mas alla de una semana no se probo nada.

DOS FUERZAS OPUESTAS, y por eso hay que medir en vez de suponer:
  - a favor: el termino de costo del harness es (costo/100)/atr_base, y atr_base es la
    mediana movil de 30d del ATR de 24h, que NO depende del horizonte. El costo por
    rebalanceo es constante, pero se paga 365 veces al ano a 24h y 12 veces a 30d.
  - en contra: con paso = horizonte (sin solape), 5 anos dan ~260 rebalanceos a 7d, ~60
    a 30d y ~20 a 90d.

LA UNIDAD: % ANUALIZADO, no ATR por tenencia. Comparar un MDE en "ATR por tenencia"
entre 24h y 90d no significa nada porque la misma unidad mide cosas distintas.

    % por rebalanceo = spread_ATR x atr_base_mediana(%)
    % anualizado     = % por rebalanceo x (8760 / horizonte)

168h entra como CALIBRACION, no como hipotesis: es el horizonte mas largo donde la
corrida 4 SI concluyo. Si a 168h el MDE anualizado tampoco baja de 10%/ano, el problema
es el umbral y no los horizontes largos — el mismo truco que uso BTC en la corrida 8.
"""
import sys
import time

import numpy as np
import pandas as pd

from klines import load_panel
from ranking import COSTO_PCT, controles, evaluar, tablero

INICIO, FIN = "2021-08-01", "2026-08-01"
HORIZONTES = (168, 720, 2160)      # 7d (calibracion), 30d, 90d
MDE_MAX_ANUAL = 10.0               # %/ano — el umbral preregistrado
K = 20
N_CTRL = 8
Z = 2.80                           # 1,96 + 0,84
HORAS_ANO = 8760


def nula(TB, horizonte, k=K, costo=COSTO_PCT):
    """MDE con la nula real (rankings al azar), convertido a % anualizado."""
    C = controles(TB, n=N_CTRL, seed=12345)
    filas = [evaluar(TB, s, nm, objetivo=o, k=k, costo=costo)
             for o in ("largo", "corto") for nm, s in C.items()]
    D = pd.DataFrame(filas)
    atr = float(TB["atr_base"].median()) * 100.0    # ATR base en % del precio
    vueltas = HORAS_ANO / horizonte
    out = []
    for o in ("largo", "corto"):
        sub = D[D["objetivo"] == o]
        sd = float(sub["sd_sem"].median())
        sem = float(sub["semanas"].median())
        mde_atr = Z * sd / np.sqrt(sem)
        out.append({"objetivo": o, "semanas": sem, "sd_sem": sd,
                    "mde_atr": mde_atr,
                    "mde_anual": mde_atr * atr * vueltas})
    return pd.DataFrame(out), atr, vueltas


def main():
    print("=" * 92)
    print("CORRIDA 13 — COMPUERTA DE POTENCIA EN HORIZONTES LARGOS")
    print("=" * 92)
    print(f"ventana {INICIO} -> {FIN}   paso = horizonte (sin solape)   top-k={K}")
    print(f"regla preregistrada: MDE anualizado <= {MDE_MAX_ANUAL:.0f}%/ano, o ese")
    print("                     horizonte es 'no se pudo medir'")
    print("168h es CALIBRACION: es donde la corrida 4 SI concluyo (0 de 4.140)\n")

    panel = load_panel(INICIO, FIN, n=200, pin="base200", full=True)
    if not panel:
        print("FATAL: no se pudo cargar el panel")
        sys.exit(1)

    t0 = time.time()
    filas = []
    for H in HORIZONTES:
        print(f"\n--- horizonte {H}h ({H/24:.0f}d) ---", flush=True)
        TB = tablero(panel, paso=H, horizonte=H)
        R, atr, vueltas = nula(TB, H)
        for _, r in R.iterrows():
            filas.append({"horizonte": H, **r.to_dict(), "atr_base_pct": atr,
                          "vueltas_ano": vueltas,
                          "costo_anual": COSTO_PCT * vueltas,
                          "barras": TB["t"].nunique()})
        print(f"    ATR base mediano {atr:.2f}%   {vueltas:.1f} vueltas/ano   "
              f"({time.time()-t0:.0f}s)")

    D = pd.DataFrame(filas)
    D.to_csv("horizonte_largo.csv", index=False)

    print(f"\n{'='*92}\nMDE POR HORIZONTE, EN % ANUALIZADO\n{'='*92}")
    print(f"  {'horiz':>7}{'barras':>8}{'semanas':>9}{'MDE ATR':>10}"
          f"{'MDE %/ano':>12}{'costo %/ano':>13}{'BRUTO nec.':>13}{'':>6}")
    for H in HORIZONTES:
        g = D[D.horizonte == H]
        m = float(g.mde_anual.median())
        marca = "  ok" if m <= MDE_MAX_ANUAL else "  NO"
        cal = "  <- calibracion" if H == 168 else ""
        bruto = m + float(g.costo_anual.iloc[0])
        print(f"  {H:>6}h{int(g.barras.iloc[0]):>8}{g.semanas.median():>9.0f}"
              f"{g.mde_atr.median():>10.4f}"
              f"{m:>12.1f}{g.costo_anual.iloc[0]:>13.2f}{bruto:>13.1f}{marca}{cal}")

    D["bruto"] = D.mde_anual + D.costo_anual
    br = D.groupby("horizonte").bruto.median()
    print("\n  BRUTO nec. = el efecto ANTES de costos que haria falta para que el NETO")
    print("  sea detectable. Es la comparacion que decide si alargar el horizonte sirve:")
    print("  el costo baja, pero la precision tambien.")
    print(f"  rango sobre los tres horizontes: {br.min():.1f}% a {br.max():.1f}%/ano"
          f"   (dispersion {100*(br.max()/br.min()-1):.0f}%)")

    print(f"\n{'='*92}\nVEREDICTO\n{'='*92}")
    cal = float(D[D.horizonte == 168].mde_anual.median())
    largos = [H for H in HORIZONTES if H > 168
              and float(D[D.horizonte == H].mde_anual.median()) <= MDE_MAX_ANUAL]

    if cal > MDE_MAX_ANUAL:
        print(f"  OJO: la CALIBRACION tambien falla ({cal:.1f}%/ano a 168h, donde la")
        print("  corrida 4 SI concluyo con 4.140 brazos). Eso no dice que los horizontes")
        print("  largos sean especiales: dice que el umbral de 10%/ano es mas exigente que")
        print("  el que uso la corrida 4, que decidia en ATR por tenencia y no anualizado.")
        print("  Se reporta y NO se afloja el umbral. La comparacion entre horizontes")
        print("  sigue siendo valida porque todos estan en la misma unidad.")
        print()

    if largos:
        print(f"  horizontes largos MEDIBLES: {largos}")
        print("  -> el hueco sigue abierto ahi. Corresponde correr el lote real,")
        print("     con su preregistro, solo en esos horizontes.")
    else:
        print("  NINGUN horizonte largo llega al umbral.")
        print("  Veredicto: NO SE PUDO MEDIR. El hueco del mapa se cierra asi, y")
        print("  la distincion importa: no es que no haya efecto a 30d o 90d, es que")
        print("  con 5 anos de historia no se puede saber. Se reabre con mas historia,")
        print("  no con otro estimador.")

    print(f"\n  ({time.time()-t0:.0f}s)  -> horizonte_largo.csv")
    print("=" * 92)
    return 0


if __name__ == "__main__":
    sys.exit(main())
