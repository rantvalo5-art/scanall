"""Cuanto hay que esperar DE VERDAD para que el forward test decida?

El `SEM_MIN = 8` de `radar/medir.py` salio de copiar el umbral de `lote.py`, que existia
por otra razon: alla las entradas se SOLAPAN (una cada 12h con horizonte de 30d, ~60
trades vivos a la vez) y el n efectivo es una fraccion del contado. Aca las barras NO se
solapan por diseno (paso = horizonte = 4h), asi que el argumento no se traslada solo.

Esto lo calcula en vez de heredarlo:

  1. la autocorrelacion real del spread por barra -> n EFECTIVO, no el contado
  2. cuantas barras hacen falta para 80% de potencia contra el efecto medido
  3. lo mismo si el efecto real es la MITAD, que es lo normal

    py -3.13 -u cuanto_esperar.py
"""
import numpy as np
import pandas as pd

import ranking as R
from klines import load_panel

H, K = 4, 8
CORRIDAS_DIA = 24 / H
Z = 2.80          # (1,96 + 0,84): 80% de potencia, alfa 0,05 a dos colas


def main():
    panel = load_panel("2021-08-01", "2026-08-01", n=46, pin="deriv46", full=True)
    TB = R.tablero(panel, paso=H, horizonte=H, verbose=False)

    D = TB[["t", "semana", "y_largo", "y_magnitud", "atr_base"]].copy()
    D["s"] = TB["n_surge"].to_numpy()
    D = D[D.s.notna() & D.y_magnitud.notna() & D.atr_base.gt(0)]
    D = D.sort_values(["t", "s"], ascending=[True, False], kind="mergesort")
    sel = D.groupby("t").cumcount() < K

    uni = D.groupby("t")["y_magnitud"].mean()
    top = D[sel].groupby("t")["y_magnitud"].mean()
    sp = (top - uni).dropna().sort_index()          # spread POR BARRA

    n = len(sp)
    mu, sd = float(sp.mean()), float(sp.std(ddof=1))
    print(f"\nbarras: {n:,}   spread medio {mu:+.4f}   sd por barra {sd:.4f}")

    # autocorrelacion: cuanto se parece una barra a la siguiente
    print("\nautocorrelacion del spread por barra:")
    rhos = []
    for lag in (1, 2, 3, 6, 12, 42):
        r = float(sp.autocorr(lag))
        rhos.append(r)
        etq = {1: "(4h)", 2: "(8h)", 3: "(12h)", 6: "(1 dia)", 12: "(2 dias)",
               42: "(1 semana)"}[lag]
        print(f"  lag {lag:>2} {etq:<11} {r:+.3f}")

    # factor de inflacion de varianza por autocorrelacion (Newey-West simple):
    # n_efectivo = n / (1 + 2*sum(rho_k)) sobre los lags que importan
    suma = sum(max(r, 0) for r in rhos[:5])
    factor = 1 + 2 * suma
    print(f"\nfactor de inflacion 1+2*sum(rho) = {factor:.2f}"
          f"  ->  n efectivo = n / {factor:.2f}")

    print("\n" + "=" * 72)
    print("CUANTAS BARRAS HACEN FALTA PARA 80% DE POTENCIA")
    print("=" * 72)
    print(f"{'si el efecto real es':<26}{'barras':>10}{'corridas':>11}{'DIAS':>8}"
          f"{'semanas':>9}")
    print("-" * 72)
    for etq, ef in (("lo medido (x1,0)", mu), ("la mitad (x0,5)", mu * 0.5),
                    ("un tercio (x0,33)", mu / 3), ("un cuarto (x0,25)", mu * 0.25)):
        n_req = (Z * sd / ef) ** 2 * factor
        dias = n_req / CORRIDAS_DIA
        print(f"{etq:<26}{n_req:>10,.0f}{n_req:>11,.0f}{dias:>8.1f}{dias/7:>9.1f}")

    print("\nOJO: esto es potencia para distinguir el efecto de CERO. No reemplaza")
    print("mirar si el efecto se sostiene en el tiempo — para eso hacen falta")
    print("suficientes semanas distintas, no solo suficientes barras.")

    # cuantas semanas distintas hacen falta para ver consistencia
    sem = sp.groupby(D.groupby("t")["semana"].first().reindex(sp.index)).mean()
    print(f"\ncontrol: con {len(sem)} semanas historicas, "
          f"{100*(sem > 0).mean():.0f}% dieron spread > 0")
    print(f"la sd ENTRE semanas es {sem.std(ddof=1):.4f}, o sea que para el promedio")
    print(f"semanal hacen falta {(Z*sem.std(ddof=1)/mu)**2:.1f} semanas por esa via.")


if __name__ == "__main__":
    main()
