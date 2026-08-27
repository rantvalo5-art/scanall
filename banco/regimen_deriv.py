"""Corte por REGIMEN de los sobrevivientes de magnitud (corrida 3).

Un resultado pooled sobre 5 anios puede estar sostenido por un solo tramo. La regla
del preregistro exige el corte para cualquier direccional; aca se aplica igual a los de
magnitud, porque un detector de movimiento que solo funciona en bear no sirve.

    py -3.13 -u regimen_deriv.py
"""
import numpy as np
import pandas as pd

import ranking as R
from klines import load_panel
from metricas import feat_metricas, load_metrics

TRAMOS = [
    ("2021-08 bull tardio", "2021-08-01", "2021-11-15"),
    ("2021-11 a 2022-11 bear", "2021-11-15", "2022-11-30"),
    ("2022-12 a 2024-03 recup", "2022-11-30", "2024-03-15"),
    ("2024-03 a 2025-08 lateral", "2024-03-15", "2025-08-01"),
    ("2025-08 a 2026-08 bear", "2025-08-01", "2026-08-01"),
]

BRAZOS = ["oi_rel_168", "oi_chg_24h", "n_surge", "turnover", "roc_168", "atr_24",
          "tt_cuentas_pct [bajo]", "CONTROL azar 1"]


def main():
    panel = load_panel("2021-08-01", "2026-08-01", n=46, pin="deriv46", full=True)
    TB = R.tablero(panel, paso=24, horizonte=24)
    M = load_metrics(list(panel), "2021-08-01", "2026-08-01", verbose=False)
    TB = pd.concat([TB, feat_metricas(M, TB[["sym", "t"]], verbose=False)], axis=1)

    S = R.scores(TB)
    S.update(R.controles(TB))

    lim = [(n, pd.Timestamp(a, tz="UTC").value // 10**6,
            pd.Timestamp(b, tz="UTC").value // 10**6) for n, a, b in TRAMOS]

    print(f"\n{'brazo':26s}" + "".join(f"{n.split(' ',1)[0]:>13s}" for n, _, _ in lim)
          + f"{'TODO':>13s}")
    print("-" * (26 + 13 * (len(lim) + 1)))
    for b in BRAZOS:
        if b not in S:
            continue
        fila = f"{b[:26]:26s}"
        for _, a, z in lim:
            m = (TB["t"] >= a) & (TB["t"] < z)
            sub = TB[m]
            if sub["t"].nunique() < 30:
                fila += f"{'--':>13s}"
                continue
            sem, _, _, _ = R._spread_semanal(sub, S[b][sub.index], 8, "y_magnitud", 0.0)
            fila += f"{'--':>13s}" if sem is None or len(sem) < 8 else f"{sem.mean():>+13.3f}"
        sem, _, _, _ = R._spread_semanal(TB, S[b], 8, "y_magnitud", 0.0)
        fila += f"{sem.mean():>+13.3f}"
        print(fila)

    print("\nSpread de magnitud (top-8 menos universo de la misma barra), en ATR base.")
    print("Un brazo que cambia de signo entre tramos esta sostenido por un regimen.")


if __name__ == "__main__":
    main()
