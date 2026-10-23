"""MEDIR EL SWING A 7D — `swing/PREREGISTRO_7D.md` §3 y §4, tal cual.

    py -3.13 dump_outcomes.py --tabla screener_outcomes     # dump fresco
    py -3.13 -u swing/medir_7d.py

Reusa `cargar` y `deduplicar` de `dt_vivo.py` para que el universo sea IDENTICO al de
`dt_vivo.py --sistema swing`, como pide §3. Lo unico propio es lo que `dt_vivo.py` no
tiene: el horizonte de 7d y el bloque de 14 DIAS (a 7d los bloques semanales se solapan y
el bootstrap regala significancia, §3).

El orden importa y es el del preregistro: la compuerta de potencia (C) se imprime y decide
ANTES de mirar `r_7d`. Si falla, se dice por cual de las dos (n o sigma) y se corta.
"""
import os
import sys

import numpy as np

RAIZ = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, RAIZ)
from dt_vivo import cargar, deduplicar, SISTEMAS, COSTO_PCT, Z  # noqa: E402

BLOQUE_D = 14     # el doble del horizonte (§3)
N_OBJ = 10        # bloques objetivo = 140 dias de registro (§3)
MDE_MAX = 6.0     # arriba de esto, "no se pudo medir" (§4)
REPS = 5000


def medias(X):
    """Media por bloque. Cada bloque pesa UNO."""
    return X.groupby("bloque").r_7d.mean()


def main():
    D = cargar(os.path.join(RAIZ, SISTEMAS["swing"]["fuente"]))
    D = D[D.signal_type.isin(SISTEMAS["swing"]["entradas"])]
    D = deduplicar(D)
    D["r_7d"] = (D.price_7d.astype(float) / D.entry_price.astype(float) - 1) * 100 - COSTO_PCT
    R = D.dropna(subset=["r_7d"]).copy()
    t0 = R.dt.min().normalize()
    R["bloque"] = ((R.dt - t0).dt.days // BLOQUE_D).astype(int)

    b = medias(R)
    print(f"universo dedup: {len(D)} alertas, {len(R)} con price_7d resuelto")
    print(f"ventana {R.dt.min():%Y-%m-%d} -> {R.dt.max():%Y-%m-%d}  |  "
          f"{len(b)} bloques de {BLOQUE_D} d")
    print(f"alertas por bloque: {R.groupby('bloque').size().tolist()}")

    # (C) POTENCIA — antes de mirar r_7d
    sd = float(b.std(ddof=1))
    mde = Z * sd / np.sqrt(len(b))
    print(f"\n(C) sigma por bloque {sd:.2f}  |  MDE {mde:.2f}% (umbral {MDE_MAX}%, a priori 4,4%)")
    print(f"    n bloques {len(b)} (objetivo {N_OBJ})")
    falla_n, falla_s = len(b) < N_OBJ, mde > MDE_MAX
    if falla_n or falla_s:
        mde_obj = Z * sd / np.sqrt(N_OBJ)
        print(f"    ---> NO SE PUDO MEDIR  (por n: {falla_n}; por sigma: {falla_s};"
              f" MDE con {N_OBJ} bloques seria {mde_obj:.2f}%)")
        return
    print("    (C) pasa")

    rng = np.random.default_rng(0)
    v = b.to_numpy()
    p = float((np.array([rng.choice(v, len(v), True).mean()
                         for _ in range(REPS)]) <= 0).mean())
    mejor_par = R.groupby("symbol").r_7d.sum().idxmax()
    sin_par = medias(R[R.symbol != mejor_par]).mean()
    sin_bloque = b.drop(b.idxmax()).mean()
    pos = (b > 0).mean()
    print(f"\n media por bloque {b.mean():+.3f}%   (MDE {mde:.2f}%)")
    print(f" 1. bootstrap de bloques p = {p:.4f}            {'OK' if p < 0.05 else 'FALLA'}")
    print(f" 2. sin el mejor bloque {sin_bloque:+.3f}%  "
          f"sin el mejor par ({mejor_par}) {sin_par:+.3f}%")
    print(f" 3. bloques positivos {pos:.0%}  (>=60%)          {'OK' if pos >= .6 else 'FALLA'}")
    print(f" 4. signo sin el mejor par                     {'OK' if sin_par > 0 else 'FALLA'}")
    print(f" efecto supera MDE: {'SI' if abs(b.mean()) > mde else 'NO'}")


if __name__ == "__main__":
    main()
