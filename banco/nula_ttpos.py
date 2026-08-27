"""La prueba que decide sobre `tt_pos ~ sin roc_24`: donde cae contra 200 dados.

La auditoria anterior comparo el brazo contra UN ranking al azar. Con k=3 y ~50 semanas
por tramo, un solo sorteo no es una linea base: el propio control se movia de +0,82 en el
bull a -0,95 en el bear. Aca se sortean 200 rankings al azar POR TRAMO y se mira en que
percentil de esa distribucion cae el brazo.

Ademas, la nula correcta para look-elsewhere en este repo es por DESPLAZAMIENTO CIRCULAR,
no barajando: barajar destruye la autocorrelacion y hace la nula demasiado facil. Aca se
usan las dos y se reportan las dos.

    py -3.13 -u nula_ttpos.py
"""
import numpy as np
import pandas as pd

import ranking as R
from klines import load_panel
from metricas import feat_metricas, load_metrics

H, K, OBJ, BRAZO = 168, 3, "corto", "tt_pos ~ sin roc_24"
REPS = 200

TRAMOS = [
    ("2021-11 a 2022-11 bear", "2021-11-15", "2022-11-30"),
    ("2022-12 a 2024-03 bull", "2022-11-30", "2024-03-15"),
    ("2024-03 a 2025-08 lateral", "2024-03-15", "2025-08-01"),
    ("2025-08 a 2026-08 bear", "2025-08-01", "2026-08-01"),
    ("TODO", "2021-08-01", "2026-08-01"),
]


def circular(s, TB, corr):
    """Nula por DESPLAZAMIENTO CIRCULAR: se rota la serie de cada simbolo en el tiempo.
    Conserva la autocorrelacion del score y rompe solo su alineacion con el futuro."""
    out = np.empty(len(s))
    for sym, idx in TB.groupby("sym", sort=False).indices.items():
        v = s.to_numpy()[idx]
        out[idx] = np.roll(v, corr % max(len(v), 1))
    return pd.Series(out, index=s.index)


def spread(TB, s, sub_idx=None):
    T = TB if sub_idx is None else TB[sub_idx]
    sem, _, _, _ = R._spread_semanal(T, s[T.index], K, f"y_{OBJ}", R.COSTO_PCT)
    if sem is None or len(sem) < 8:
        return np.nan
    return float(sem.mean())


def main():
    panel = load_panel("2021-08-01", "2026-08-01", n=46, pin="deriv46", full=True)
    M = load_metrics(list(panel), "2021-08-01", "2026-08-01", verbose=False)
    TB = R.tablero(panel, paso=H, horizonte=H, verbose=False)
    TB = pd.concat([TB, feat_metricas(M, TB[["sym", "t"]], verbose=False)], axis=1)
    S = R.scores(TB)
    s = S[BRAZO]

    cob = TB.groupby(pd.to_datetime(TB.t, unit="ms", utc=True).dt.year)["tt_pos"] \
            .apply(lambda x: x.notna().mean())
    print("cobertura de tt_pos por anio:")
    print("  " + "  ".join(f"{y}:{v:.0%}" for y, v in cob.items()))

    rng = np.random.default_rng(0)
    print(f"\n{'tramo':28s}{'brazo':>9s}{'nula media':>12s}{'p95':>9s}"
          f"{'percentil':>11s}{'p circular':>12s}")
    print("-" * 81)

    for nom, a, b in TRAMOS:
        m = ((TB["t"] >= pd.Timestamp(a, tz="UTC").value // 10**6) &
             (TB["t"] < pd.Timestamp(b, tz="UTC").value // 10**6))
        obs = spread(TB, s, m)
        if not np.isfinite(obs):
            print(f"{nom:28s}{'--':>9s}   (menos de 8 semanas con datos)")
            continue

        # nula 1: rankings al azar
        nula = np.array([spread(TB, pd.Series(rng.random(len(TB)), index=TB.index), m)
                         for _ in range(REPS)])
        nula = nula[np.isfinite(nula)]
        pct = float((nula < obs).mean())

        # nula 2: desplazamiento circular del PROPIO score
        nc = np.array([spread(TB, circular(s, TB, int(c)), m)
                       for c in rng.integers(5, 200, size=60)])
        nc = nc[np.isfinite(nc)]
        p_circ = float((nc >= obs).mean()) if len(nc) else np.nan

        print(f"{nom:28s}{obs:>+9.3f}{nula.mean():>+12.3f}"
              f"{np.percentile(nula, 95):>+9.3f}{100*pct:>10.1f}%{p_circ:>12.3f}")

    print("\npercentil = donde cae el brazo dentro de 200 rankings al azar del mismo")
    print("tamano y periodo. p circular = fraccion de 60 desplazamientos del PROPIO")
    print("score que igualan o superan al brazo (la nula que conserva autocorrelacion).")


if __name__ == "__main__":
    main()
