"""
COSTOS REALES - fase 0 del item 4.3 (la cola iliquida).

Por que existe. Todo el banco corre con `COSTO_PCT = 0.20` fijo: 0,10% de fee taker
por lado, SIN spread y SIN slippage. En el top-200 eso es casi cierto (el spread de
BTCUSDT son ~1-2 bps). En la cola iliquida es falso, y falso EN LA DIRECCION QUE
IMPORTA: las features que mas prometen ahi (Amihud, Roll, ac1) son literalmente
medidas de iliquidez, o sea que cualquier "hallazgo" con costo fijo seria un
artefacto de contabilidad. Sin esto, el item 4.3 no se puede leer.

Que estima. El costo de un round trip taker:

    costo = 2*fee + spread + impacto(tamano)

- `fee`  : 0,10% por lado. Es dato, no estimacion.
- `spread`: se estima de OHLC con DOS estimadores independientes:
    * **Corwin-Schultz (2012)**: usa el high-low de dos barras consecutivas. El
      high-low de una barra mezcla spread y volatilidad; el de dos barras permite
      separarlos porque la volatilidad escala con el tiempo y el spread no.
    * **Roll (1984)**: 2*sqrt(-cov(r_t, r_t-1)). Solo definido cuando la
      autocovarianza es negativa (el rebote bid-ask); si es positiva, no da.
  Se usan los dos A PROPOSITO: son independientes en su derivacion, asi que si
  coinciden en el ranking es evidencia de que miden spread y no ruido.
- `impacto`: Amihud, |retorno| por dolar operado, escalado al tamano de la orden.

    py -3.13 -u costos.py                  # tabla del top-200 (validacion)
    py -3.13 -u costos.py --pares 600      # incluye la cola
"""
import argparse

import numpy as np
import pandas as pd

from klines import load_panel

FEE_LADO = 0.10        # % por lado, taker spot en Binance
K_CS = 3 - 2 * np.sqrt(2)


def corwin_schultz(h, l):
    """Spread proporcional (en %) estimado a la Corwin-Schultz, por barra.

    beta  = suma de los (log high-low)^2 de DOS barras consecutivas
    gamma = (log del rango de las dos barras juntas)^2
    alpha = (sqrt(2 beta) - sqrt(beta))/(3-2sqrt2) - sqrt(gamma/(3-2sqrt2))
    S     = 2 (e^alpha - 1)/(1 + e^alpha)

    Los negativos se truncan a 0: el estimador es ruidoso barra a barra y da
    negativos cuando la volatilidad domina. Truncar sesga hacia ARRIBA, que es el
    lado conservador para un costo.
    """
    h = np.asarray(h, dtype=float)
    l = np.asarray(l, dtype=float)
    with np.errstate(invalid="ignore", divide="ignore"):
        hl = np.log(h / l) ** 2
        beta = hl[:-1] + hl[1:]
        h2 = np.maximum(h[:-1], h[1:])
        l2 = np.minimum(l[:-1], l[1:])
        gamma = np.log(h2 / l2) ** 2
        alpha = (np.sqrt(2 * beta) - np.sqrt(beta)) / K_CS - np.sqrt(gamma / K_CS)
        S = 2 * (np.exp(alpha) - 1) / (1 + np.exp(alpha))
    S = np.where(np.isfinite(S), S, np.nan)
    return np.clip(S, 0, None) * 100          # en %


def roll_spread(c):
    """Spread proporcional (en %) a la Roll: 2*sqrt(-cov(r_t, r_t-1)).

    Solo definido si la autocovarianza es NEGATIVA (el rebote entre bid y ask).
    Si es positiva el estimador no existe -> NaN, no 0. Devolver 0 seria decir
    'no hay spread', que es una afirmacion muy distinta de 'no se puede medir'.
    """
    c = np.asarray(c, dtype=float)
    r = np.diff(np.log(c))
    if len(r) < 3:
        return np.nan
    cov = np.cov(r[1:], r[:-1])[0, 1]
    if not np.isfinite(cov) or cov >= 0:
        return np.nan
    return 2 * np.sqrt(-cov) * 100


def amihud(c, qv):
    """Iliquidez de Amihud: |retorno| por dolar operado. Unidades: % por USD."""
    c = np.asarray(c, dtype=float)
    qv = np.asarray(qv, dtype=float)
    r = np.abs(np.diff(np.log(c))) * 100
    q = qv[1:]
    m = (q > 0) & np.isfinite(r)
    if m.sum() < 10:
        return np.nan
    return float(np.median(r[m] / q[m]))


def resumen(panel, orden_usd=1000.0):
    """Una fila por simbolo con las tres medidas + el costo total estimado."""
    filas = []
    for s, df in panel.items():
        cs = corwin_schultz(df["h"].to_numpy(), df["l"].to_numpy())
        cs_med = float(np.nanmedian(cs)) if np.isfinite(cs).any() else np.nan
        rl = roll_spread(df["c"].to_numpy())
        am = amihud(df["c"].to_numpy(), df["qv"].to_numpy()) if "qv" in df else np.nan
        qv_med = float(df["qv"].median()) if "qv" in df else np.nan
        # impacto de una orden de `orden_usd`: Amihud es %/USD
        imp = am * orden_usd if np.isfinite(am) else np.nan
        filas.append(dict(sym=s, qv_med=qv_med, cs=cs_med, roll=rl,
                          amihud=am, impacto=imp,
                          costo=2 * FEE_LADO + (cs_med if np.isfinite(cs_med) else 0)
                                + (imp if np.isfinite(imp) else 0)))
    R = pd.DataFrame(filas).set_index("sym")
    R["rank_vol"] = R.qv_med.rank(ascending=False)
    return R.sort_values("rank_vol")


def validar(R):
    """Los tres chequeos que deciden si este modelo se puede usar."""
    print("\n" + "=" * 68)
    print("VALIDACION DEL MODELO DE COSTOS")
    print("=" * 68)

    ok = True
    # 1. los muy liquidos tienen que dar spread chico
    for s in ("BTCUSDT", "ETHUSDT"):
        if s in R.index:
            v = R.loc[s, "cs"]
            bien = v < 0.10
            ok &= bien
            print(f"  1. {s:9s} spread CS = {v*100:6.1f} bps   "
                  f"{'ok (<10 bps)' if bien else 'SOSPECHOSO'}")

    # 2. el spread tiene que crecer al bajar en el ranking de volumen
    q = pd.qcut(R.rank_vol, 4, labels=["Q1 (mas liquido)", "Q2", "Q3", "Q4 (menos)"])
    t = R.groupby(q, observed=True)[["cs", "roll", "impacto", "costo"]].median()
    print("\n  2. monotonia por cuartil de volumen (mediana, en %):")
    print(t.to_string(float_format=lambda x: f"{x:8.4f}"))
    mono = t["cs"].is_monotonic_increasing
    ok &= mono
    print(f"     spread CS monotono creciente: {'SI' if mono else 'NO'}")

    # 3. los dos estimadores independientes tienen que coincidir en el ranking
    m = R[["cs", "roll"]].dropna()
    if len(m) > 10:
        rho = m["cs"].corr(m["roll"], method="spearman")
        bien = rho > 0.3
        ok &= bien
        print(f"\n  3. Spearman(CS, Roll) = {rho:+.3f} sobre {len(m)} pares   "
              f"{'ok (>0.3)' if bien else 'NO COINCIDEN'}")
        print(f"     Roll definido en {len(m)}/{len(R)} pares "
              f"({100*len(m)/len(R):.0f}%; NaN = autocovarianza positiva)")

    print(f"\n  => modelo {'USABLE' if ok else 'NO USABLE todavia'}")
    return ok


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--pares", type=int, default=200)
    ap.add_argument("--pin", default="base200")
    ap.add_argument("--orden", type=float, default=1000.0,
                    help="tamano de orden en USD para el impacto")
    ap.add_argument("--csv", default="costos.csv")
    a = ap.parse_args()

    P = load_panel("2025-08-01", "2026-08-01", n=a.pares, tf="1h",
                   pin=a.pin, full=True)
    R = resumen(P, orden_usd=a.orden)
    print(f"\n{len(R)} pares. Costo round-trip taker estimado, orden de "
          f"${a.orden:,.0f}:\n")
    print(f"  fee fijo            : {2*FEE_LADO:.3f}%  (lo que usa hoy el banco)")
    print(f"  + spread (mediana)  : {R.cs.median():.3f}%")
    print(f"  + impacto (mediana) : {R.impacto.median():.3f}%")
    print(f"  = costo   (mediana) : {R.costo.median():.3f}%")
    print(f"\n  el banco asume 0,200% -> subestima por "
          f"{R.costo.median()-0.20:.3f} pp en la MEDIANA")
    print(f"  peor decil: {R.costo.quantile(0.90):.3f}%")
    validar(R)
    R.to_csv(a.csv)
    print(f"\n-> {a.csv}")


if __name__ == "__main__":
    main()
