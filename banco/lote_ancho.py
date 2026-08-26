"""
LOTE ANCHO — las familias que el banco nunca pudo probar porque el loader las tiraba.

`klines()` se quedaba con [t,h,l,c] y descartaba apertura, volumen, quote volume,
numero de trades y volumen taker comprador. O sea: el banco que mato 450+ hipotesis de
precio nunca midio una sola feature de volumen ni de forma de vela — justo aquello sobre
lo que esta construido el screener en vivo (OBV/CVD, "volumen creciente", body strength).
`klines(..., full=True)` lo destraba.

Cuatro familias, ninguna probada antes:

  A. VOLUMEN Y FORMA. Volumen relativo, numero de trades, tamano medio de trade,
     cuerpo/mecha, efficiency ratio, volumen por unidad de movimiento. Y sobre todo el
     **ratio taker comprador** (vb/v): la unica feature DIRECCIONAL que hay en una vela.
     Importa porque todo lo que murio en este repo murio por no tener direccion — las
     features de precio ensanchan las dos colas por igual.

  B. TRANSVERSAL. Beta y correlacion a BTC, momentum idiosincratico (roc menos la parte
     explicada por BTC), dispersion del universo, breadth, rank de turnover. Todo lo
     probado hasta ahora es serie de tiempo por simbolo; `rs_168` resta la mediana, que
     no es lo mismo que sacar beta. El efecto tamano/liquidez no se miro nunca.

  C. CICLO DE VIDA. Edad del par en Binance (primera vela disponible), distancia al
     maximo de toda su historia, si esta en maximos historicos.

  D. SHORT. Las mismas hipotesis con el signo dado vuelta. La cola de abajo ya midio
     mas fuerte que la de arriba (2,33x contra 1,68x en `movers.py`).

Se corren DOS lotes (largo y corto) sobre la misma familia de hipotesis, asi que el q
va partido al medio (0,05 cada uno) para que la correccion cubra el total.

    py -3.13 lote_ancho.py --pares 40      # piloto
    py -3.13 lote_ancho.py                 # corrida completa (baja el cache ancho)
"""
import argparse

import numpy as np
import pandas as pd

from klines import load_panel
from lote import lote
from primer_toque import tabla

BTC = "BTCUSDT"


# ------------------------------------------------------------------ A + C
def _feat_simbolo(df):
    """Features de un par. La fila i mira solo hasta i inclusive."""
    o, h, l, c = (df[k].to_numpy(float) for k in "ohlc")
    v, qv, n, vb = (df[k].to_numpy(float) for k in ("v", "qv", "n", "vb"))
    t = df["t"].to_numpy()
    N = len(c)
    out = {}
    S = lambda a: pd.Series(a)                                    # noqa: E731

    def rel(a, k=168):
        """Cuanto se sale de su propia media movil. Escala-libre entre monedas."""
        m = S(a).rolling(k).mean()
        return (S(a) / m.replace(0, np.nan) - 1.0).to_numpy()

    def zs(a, k=168):
        s = S(a)
        return ((s - s.rolling(k).mean()) / s.rolling(k).std().replace(0, np.nan)).to_numpy()

    # --- A. volumen -----------------------------------------------------------
    out["vol_rel"] = rel(v)
    out["vol_z"] = zs(v)
    out["trades_rel"] = rel(n)
    with np.errstate(divide="ignore", invalid="ignore"):
        tam = np.where(n > 0, v / n, np.nan)      # tamano medio de trade
    out["tam_trade_rel"] = rel(tam)
    out["turnover"] = np.log10(S(qv).rolling(168).mean().clip(lower=1).to_numpy())

    # --- A. flujo agresor (lo unico direccional de una vela) -------------------
    with np.errstate(divide="ignore", invalid="ignore"):
        tk = np.where(v > 0, vb / v, np.nan)
    out["taker"] = tk
    out["taker_24"] = (S(vb).rolling(24).sum()
                       / S(v).rolling(24).sum().replace(0, np.nan)).to_numpy()
    out["taker_z"] = zs(tk)

    # --- A. forma de vela -----------------------------------------------------
    rng = h - l
    with np.errstate(divide="ignore", invalid="ignore"):
        out["cuerpo"] = np.where(rng > 0, (c - o) / rng, np.nan)
        out["mecha_sup"] = np.where(rng > 0, (h - np.maximum(o, c)) / rng, np.nan)
        out["mecha_inf"] = np.where(rng > 0, (np.minimum(o, c) - l) / rng, np.nan)
        # cuerpo neto de 24h: cuanto del rango recorrido quedo como movimiento
        cam = S(np.abs(np.diff(c, prepend=c[0]))).rolling(24).sum().to_numpy()
        neto = np.full(N, np.nan)
        if N > 24:
            neto[24:] = np.abs(c[24:] - c[:-24])
        out["efic_24"] = np.where(cam > 0, neto / cam, np.nan)
        # volumen que hizo falta por unidad de movimiento: absorcion / liquidez
        atr = S(rng).rolling(24).mean().to_numpy() / c
        out["vol_por_mov"] = np.where(atr > 0, S(qv).rolling(24).mean().to_numpy()
                                      / (atr * 1e6), np.nan)

    # --- C. ciclo de vida -----------------------------------------------------
    out["edad_d"] = (t - t[0]) / 86400000.0
    cummax = S(c).cummax().to_numpy()
    out["dd_ath"] = c / cummax - 1.0
    out["en_ath"] = (c >= cummax * 0.99).astype(float)
    return out


def features_ancho(panel, T, verbose=True):
    """Una fila por entrada de T, alineada por (sym, t). Solo familias NUEVAS."""
    piezas = []
    for k, (sym, df) in enumerate(panel.items(), 1):
        d = pd.DataFrame(_feat_simbolo(df))
        d.insert(0, "t", df["t"].to_numpy())
        d.insert(0, "sym", sym)
        piezas.append(d)
        if verbose and k % 50 == 0:
            print(f"  features {k}/{len(panel)}...", flush=True)
    FULL = pd.concat(piezas, ignore_index=True)

    # --- B. transversal -------------------------------------------------------
    # log-returns por simbolo, para beta/correlacion contra BTC
    lr = []
    for sym, df in panel.items():
        c = df["c"].to_numpy(float)
        lr.append(pd.DataFrame({"sym": sym, "t": df["t"].to_numpy(),
                                "lr": np.diff(np.log(c), prepend=np.log(c[0]))}))
    LR = pd.concat(lr, ignore_index=True)
    if BTC not in panel:
        print(f"  OJO: {BTC} no esta en el panel; beta/corr quedan en NaN")
        LR["blr"] = np.nan
    else:
        b = LR[LR.sym == BTC][["t", "lr"]].rename(columns={"lr": "blr"})
        LR = LR.merge(b, on="t", how="left")

    g = LR.groupby("sym", sort=False)
    cov = g.apply(lambda d: d.lr.rolling(168).cov(d.blr), include_groups=False)
    var = g.apply(lambda d: d.blr.rolling(168).var(), include_groups=False)
    cor = g.apply(lambda d: d.lr.rolling(168).corr(d.blr), include_groups=False)
    LR["beta_btc"] = (cov / var.replace(0, np.nan)).to_numpy()
    LR["corr_btc"] = cor.to_numpy()
    LR["r168"] = g.lr.transform(lambda s: s.rolling(168).sum())
    LR["b168"] = g.blr.transform(lambda s: s.rolling(168).sum())
    # momentum idiosincratico: lo que se movio MAS ALLA de su beta a BTC
    LR["idio_168"] = LR.r168 - LR.beta_btc * LR.b168
    FULL = FULL.merge(LR[["sym", "t", "beta_btc", "corr_btc", "idio_168"]],
                      on=["sym", "t"], how="left")

    # de mercado: dispersion transversal y breadth, hora a hora
    d24 = LR.groupby("sym", sort=False).lr.transform(lambda s: s.rolling(24).sum())
    LR["r24"] = d24
    mkt = LR.groupby("t").agg(disp=("r24", "std"), breadth=("r24", lambda s: (s > 0).mean()))
    FULL = FULL.merge(mkt, left_on="t", right_index=True, how="left")
    # rank de turnover dentro del universo: el efecto tamano, nunca mirado aca
    FULL["rank_turnover"] = FULL.groupby("t").turnover.rank(pct=True)

    F = T[["sym", "t"]].merge(FULL, on=["sym", "t"], how="left")
    F.index = T.index
    return F.drop(columns=["sym", "t"])


# ------------------------------------------------------------------ hipotesis
def hipotesis(F):
    """Colas de cada feature por quintil. Sin umbrales a dedo: elegir el corte
    despues de ver los numeros es otra forma del mismo look-elsewhere."""
    H = {}
    for c in F.columns:
        s = F[c]
        if s.notna().sum() < 1000 or s.nunique() < 5:
            continue
        q20, q80 = s.quantile([0.20, 0.80])
        if not np.isfinite(q20) or not np.isfinite(q80) or q20 == q80:
            continue
        H[f"{c} alto"] = s >= q80
        H[f"{c} bajo"] = s <= q20
    # `en_ath` es binaria: el quintil no la parte, va como esta
    if "en_ath" in F:
        H["en maximos historicos"] = F.en_ath > 0.5
    return H


def main():
    ap = argparse.ArgumentParser(description="Banco — lote de las familias nunca probadas")
    ap.add_argument("--target", type=float, default=8)
    ap.add_argument("--stop", type=float, default=8)
    ap.add_argument("--horizonte", type=int, default=30)
    ap.add_argument("--paso", type=int, default=12)
    ap.add_argument("--pares", type=int, default=200)
    ap.add_argument("--inicio", default="2025-08-01")
    ap.add_argument("--fin", default="2026-08-01")
    ap.add_argument("--q", type=float, default=0.05,
                    help="q por lote; son dos lotes (largo y corto), asi que 0,05+0,05=0,10")
    ap.add_argument("--pin", default="base200")
    ap.add_argument("--out", default=None)
    a = ap.parse_args()

    panel = load_panel(a.inicio, a.fin, n=a.pares, pin=a.pin, full=True)
    if not panel:
        print("FATAL: panel vacio")
        return
    T = tabla(panel, a.target, a.stop, a.horizonte, a.paso)
    F = features_ancho(panel, T)
    H = hipotesis(F)
    print(f"\n{len(T):,} entradas  |  {F.shape[1]} features nuevas  |  {len(H)} hipotesis")
    print(f"familias: A volumen/forma, B transversal, C ciclo de vida  x  largo y corto")

    print("\n" + "#" * 100)
    print("# LARGO — comprar en la senal")
    print("#" * 100)
    D1 = lote(T, H, q=a.q)

    Tc = T.copy()
    Tc["res"] = -Tc["res"]
    Tc.attrs.update(T.attrs)
    print("\n" + "#" * 100)
    print("# CORTO — vender en la senal (mismo evento, signo dado vuelta)")
    print("#" * 100)
    D2 = lote(Tc, H, q=a.q)

    if a.out:
        D1["lado"], D2["lado"] = "largo", "corto"
        pd.concat([D1, D2], ignore_index=True).to_csv(a.out, index=False)
        print(f"\ntabla -> {a.out}")


if __name__ == "__main__":
    main()
