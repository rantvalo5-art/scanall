"""Auditoria del unico candidato direccional del barrido: `tt_pos ~ sin roc_24` en CORTO.

Tres pruebas, las tres preregistradas:

  1. CONCENTRACION (regla 4 de la corrida 3). El nivel crudo funciona y su version
     comparable entre monedas (`tt_pos_pct`) no. Eso es el sintoma de estar rankeando
     IDENTIDAD DE MONEDA: si el top-3 son siempre los mismos tres nombres, no hay senal,
     hay tres monedas que bajaron.
  2. REGIMEN (regla 5.2 de la corrida 3). No puede cambiar de signo entre tramos.
  3. sin_top3 / sin_top1, que el barrido no computo para los no-candidatos.

    py -3.13 -u auditar_ttpos.py
"""
import numpy as np
import pandas as pd

import ranking as R
from klines import load_panel
from metricas import feat_metricas, load_metrics

H, K, OBJ = 168, 3, "corto"
BRAZO = "tt_pos ~ sin roc_24"

TRAMOS = [
    ("2021-11 a 2022-11 bear", "2021-11-15", "2022-11-30"),
    ("2022-12 a 2024-03 bull", "2022-11-30", "2024-03-15"),
    ("2024-03 a 2025-08 lateral", "2024-03-15", "2025-08-01"),
    ("2025-08 a 2026-08 bear", "2025-08-01", "2026-08-01"),
]


def main():
    panel = load_panel("2021-08-01", "2026-08-01", n=46, pin="deriv46", full=True)
    M = load_metrics(list(panel), "2021-08-01", "2026-08-01", verbose=False)
    TB = R.tablero(panel, paso=H, horizonte=H, verbose=False)
    TB = pd.concat([TB, feat_metricas(M, TB[["sym", "t"]], verbose=False)], axis=1)
    S = {**R.scores(TB), **R.controles(TB)}
    s = S[BRAZO]
    y = f"y_{OBJ}"

    # ---------------------------------------------------------------- 1. quien entra
    D = TB[["t", "sym", y]].copy()
    D["s"] = s.to_numpy()
    D = D[D["s"].notna() & D[y].notna()]
    D = D.sort_values(["t", "s"], ascending=[True, False], kind="mergesort")
    sel = D[D.groupby("t").cumcount() < K]
    conteo = sel["sym"].value_counts()
    barras = sel["t"].nunique()

    print("=" * 76)
    print(f"AUDITORIA — {BRAZO} | {OBJ} | h={H}h k={K}")
    print("=" * 76)
    print(f"\n1. CONCENTRACION — quien entra al top-{K}")
    print(f"   barras: {barras}   selecciones: {len(sel)}   "
          f"monedas distintas: {conteo.size} de {TB['sym'].nunique()}")
    print(f"   las 5 mas elegidas (cuota de las {len(sel)} selecciones):")
    for sym, n in conteo.head(5).items():
        print(f"     {sym:12s} {n:4d}  {100*n/len(sel):5.1f}%")
    print(f"   cuota del top-3 de monedas: {100*conteo.head(3).sum()/len(sel):.1f}%")
    print(f"   (un ranking sano reparte: con {TB['sym'].nunique()} monedas y k={K}, "
          f"lo esperable por moneda es {100*K/TB['sym'].nunique():.1f}%)")

    # ---------------------------------------------------------------- 2. sin top-n
    print(f"\n2. SIN LOS SIMBOLOS QUE MAS APORTAN")
    sem, aporte, _, _ = R._spread_semanal(TB, s, K, y, R.COSTO_PCT)
    print(f"   completo                  {sem.mean():+.4f}   ({len(sem)} semanas)")
    for n in (1, 3):
        fuera = set(aporte.head(n).index)
        sub = TB[~TB["sym"].isin(fuera)]
        s2, _, _, _ = R._spread_semanal(sub, s[sub.index], K, y, R.COSTO_PCT)
        v = s2.mean() if s2 is not None and len(s2) else float("nan")
        print(f"   sin top-{n} ({', '.join(sorted(fuera))[:34]:34s}) {v:+.4f}")

    # ---------------------------------------------------------------- 3. regimen
    print(f"\n3. CORTE POR REGIMEN (regla 5.2 — no puede cambiar de signo)")
    ctrl = S["CONTROL azar 1"]
    for nom, a, b in TRAMOS:
        m = (TB["t"] >= pd.Timestamp(a, tz="UTC").value // 10**6) & \
            (TB["t"] < pd.Timestamp(b, tz="UTC").value // 10**6)
        sub = TB[m]
        r, c = float("nan"), float("nan")
        s3, _, _, _ = R._spread_semanal(sub, s[sub.index], K, y, R.COSTO_PCT)
        if s3 is not None and len(s3) >= 8:
            r = s3.mean()
        s4, _, _, _ = R._spread_semanal(sub, ctrl[sub.index], K, y, R.COSTO_PCT)
        if s4 is not None and len(s4) >= 8:
            c = s4.mean()
        print(f"   {nom:28s} brazo {r:+.4f}   control {c:+.4f}")


if __name__ == "__main__":
    main()
