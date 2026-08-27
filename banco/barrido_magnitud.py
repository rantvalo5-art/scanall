"""A que horizonte sirve el radar? El barrido de MAGNITUD que faltaba.

La corrida 4 barrio horizonte y k, pero SOLO para los objetivos direccionales. La
magnitud —lo unico que sobrevivio— se midio a un solo horizonte: 24h. Asi que no se
sabia si el radar sirve a 4h (day trading), a 24h, o a 7 dias (swing).

Esto lo contesta. Mismo diseno y mismas compuertas que `ranking.py`; se reportan tambien
los controles al azar por configuracion, porque a horizontes largos la dispersion crece
y el "spread positivo" solo significa algo contra su propia nula.

    py -3.13 -u barrido_magnitud.py
"""
import time

import numpy as np
import pandas as pd

import ranking as R
from klines import load_panel
from metricas import feat_metricas, load_metrics

HORIZONTES = [4, 8, 24, 72, 168]
KS = [3, 8, 16]
BRAZOS = ["n_surge", "turnover", "oi_rel_168", "atr_24", "roc_168"]


def main():
    t0 = time.time()
    panel = load_panel("2021-08-01", "2026-08-01", n=46, pin="deriv46", full=True)
    M = load_metrics(list(panel), "2021-08-01", "2026-08-01", verbose=False)

    filas = []
    for h in HORIZONTES:
        TB = R.tablero(panel, paso=h, horizonte=h, verbose=False)
        TB = pd.concat([TB, feat_metricas(M, TB[["sym", "t"]], verbose=False)], axis=1)
        S = {**R.scores(TB, ambas=False), **R.controles(TB, n=5)}
        print(f"h={h:3d}h | {len(TB):,} filas | {TB['t'].nunique():,} barras | "
              f"{TB['semana'].nunique()} semanas", flush=True)
        for k in KS:
            for nm in BRAZOS + [c for c in S if c.startswith("CONTROL")]:
                if nm not in S:
                    continue
                sem, _, sem_crudo, ratios = R._spread_semanal(TB, S[nm], k,
                                                              "y_magnitud", 0.0)
                if sem is None or len(sem) < R.SEM_N_MIN:
                    continue
                filas.append(dict(h=h, k=k, ranking=nm, semanas=len(sem),
                                  spread=float(sem.mean()),
                                  crudo=float(sem_crudo.mean()),
                                  sem_ok=float((sem > 0).mean()),
                                  p=R._p_bloques(sem) if sem.mean() > 0 else 1.0))
    D = pd.DataFrame(filas)
    D["ctrl"] = D.ranking.str.startswith("CONTROL")
    D.to_csv("barrido_magnitud.csv", index=False)

    print("\n" + "=" * 84)
    print("SPREAD DE MAGNITUD por horizonte y k (ATR base). "
          "'azar' = mediana de 5 controles")
    print("=" * 84)
    for k in KS:
        print(f"\n--- k={k} ---")
        print(f"{'ranking':14s}" + "".join(f"{str(h) + 'h':>12s}" for h in HORIZONTES))
        print("-" * (14 + 12 * len(HORIZONTES)))
        for nm in BRAZOS:
            fila = f"{nm:14s}"
            for h in HORIZONTES:
                r = D[(D.h == h) & (D.k == k) & (D.ranking == nm)]
                fila += f"{r.spread.iloc[0]:>+12.3f}" if len(r) else f"{'--':>12s}"
            print(fila)
        fila = f"{'azar':14s}"
        for h in HORIZONTES:
            r = D[(D.h == h) & (D.k == k) & D.ctrl]
            fila += f"{r.spread.median():>+12.3f}" if len(r) else f"{'--':>12s}"
        print(fila)
        # cuanto le saca al azar: es lo unico comparable ENTRE horizontes
        fila = f"{'n_surge-azar':14s}"
        for h in HORIZONTES:
            a = D[(D.h == h) & (D.k == k) & (D.ranking == "n_surge")]
            c = D[(D.h == h) & (D.k == k) & D.ctrl]
            fila += (f"{a.spread.iloc[0] - c.spread.median():>+12.3f}"
                     if len(a) and len(c) else f"{'--':>12s}")
        print(fila)

    print(f"\n--- consistencia de n_surge (% de semanas con spread > 0) ---")
    print(f"{'k':>4s}" + "".join(f"{str(h) + 'h':>10s}" for h in HORIZONTES))
    for k in KS:
        fila = f"{k:>4d}"
        for h in HORIZONTES:
            r = D[(D.h == h) & (D.k == k) & (D.ranking == "n_surge")]
            fila += f"{100*r.sem_ok.iloc[0]:>9.0f}%" if len(r) else f"{'--':>10s}"
        print(fila)
    print(f"\n{time.time() - t0:.0f}s -> barrido_magnitud.csv")


if __name__ == "__main__":
    main()
