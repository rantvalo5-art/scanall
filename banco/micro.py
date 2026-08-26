"""
MICROESTRUCTURA INTRA-VELA — la FORMA del camino dentro de la hora, no el agregado.

Ver `PREREGISTRO_MICRO.md`.

Por que esta familia y no otra. `lote_ancho.py` ya mato volumen y forma de vela a
resolucion HORARIA (0 de 86): cuerpo, mechas, taker, efficiency ratio de 24h, volumen por
unidad de movimiento. Todo eso se calcula del OHLCV de 1h. Lo que NO se puede calcular de
una vela horaria es **como se recorrio esa hora**: si el movimiento fue un salto o una
deriva, si el volumen entro en una rafaga o parejo, cuantas veces se dio vuelta el precio,
y cuanto de la varianza a 5m sobrevive a agregarla a 1h.

Eso es informacion distinta, y sobre todo es informacion de LIQUIDEZ (Roll, Amihud,
autocorrelacion de retornos), no de precio. El techo condicional y el techo oraculo dicen
que en el precio no queda nada; la liquidez no sale del precio.

**El riesgo obvio, y por eso el test es condicional.** Casi todas estas features
correlacionan con volatilidad, y en este repo ya esta medido tres veces que la volatilidad
ensancha las DOS colas por igual (`project-movers-asimetria-volatilidad`,
`project-swing-cola-simetrica`). Descubrir "las volatiles se mueven mas" por cuarta vez no
es un hallazgo. Por eso cada hipotesis se corre tambien **dentro del quintil de `atr_24`**:
la pregunta no es si la forma del camino predice, sino si predice **mas alla de la
volatilidad**.

    py -3.13 -u micro.py --pares 40      # piloto (valida el cableado y cuenta el n)
    py -3.13 -u micro.py                 # la corrida del preregistro
"""
import argparse
import os

import numpy as np
import pandas as pd

from klines import load_panel
from lote import lote
from primer_toque import tabla

HERE = os.path.dirname(os.path.abspath(__file__))
MS_H = 3600000
POR_HORA = 12            # velas de 5m en una hora
VENT = 24                # ventana de suavizado, en horas


def _por_hora(df5):
    """Features de la forma del camino de CADA hora, calculadas desde velas de 5m.

    Devuelve un frame indexado por el timestamp de la hora (el mismo `t` que usa el panel
    horario), asi que se une por (sym, t) sin re-alinear nada.

    Todo lo de aca mira SOLO velas cerradas de esa hora: la fila de la hora H describe lo
    que paso DENTRO de H, y el join la deja disponible para una entrada tomada al cierre
    de H (offset -1, la convencion del repo). No hay lookahead.
    """
    t = df5["t"].to_numpy()
    o, h, l, c = (df5[k].to_numpy(float) for k in "ohlc")
    v, qv, nt, vb = (df5[k].to_numpy(float) for k in ("v", "qv", "n", "vb"))

    hora = (t // MS_H) * MS_H
    lr = np.diff(np.log(np.clip(c, 1e-12, None)), prepend=np.log(max(c[0], 1e-12)))
    lr[0] = 0.0

    d = pd.DataFrame({"hora": hora, "lr": lr, "alr": np.abs(lr), "r2": lr * lr,
                      "v": v, "qv": qv, "n": nt, "vb": vb,
                      "rng": h - l, "c": c, "o": o})
    # la posicion de cada vela dentro de su hora: 0..11
    d["k"] = d.groupby("hora").cumcount()
    with np.errstate(divide="ignore", invalid="ignore"):
        d["tk"] = np.where(d.v > 0, d.vb / d.v, np.nan)
        sg = np.sign(d.lr.to_numpy())
        # OJO: nada de groupby.apply con lambdas aca. Con 8.760 grupos por par eso tarda
        # ~270s POR PAR (medido) y son 15h para 200 pares. Todo lo de abajo se arma como
        # columna primero y se agrega con una reduccion vectorizada.
        d["v2"] = d.v * d.v
        d["k_alr"] = d.k * d.alr
        d["tk_up"] = (d.tk > 0.5).astype(float)
        d["amh"] = np.where(d.qv > 0, d.alr / d.qv, np.nan)
        vuelta = np.empty(len(d))
        vuelta[0] = np.nan
        vuelta[1:] = (sg[1:] * sg[:-1] < 0).astype(float)
        vuelta[d.k.to_numpy() == 0] = np.nan     # el cruce con la hora previa no cuenta
        d["vuelta"] = vuelta

    g = d.groupby("hora")
    A = g.agg(nb=("lr", "size"),
              camino=("alr", "sum"),      # distancia recorrida
              sumr=("lr", "sum"),         # desplazamiento neto (log)
              sumr2=("r2", "sum"),        # varianza realizada a 5m
              v_h=("v", "sum"), qv_h=("qv", "sum"), n_h=("n", "sum"),
              rng5=("rng", "sum"),        # suma de rangos de 5m
              sv2=("v2", "sum"), sk_alr=("k_alr", "sum"),
              tk_sd=("tk", "std"), tk_frac=("tk_up", "mean"),
              cambios=("vuelta", "mean"), amihud=("amh", "mean"),
              hi=("c", "max"), lo=("c", "min"))
    A = A[A.nb >= POR_HORA - 2]          # horas con huecos de datos no describen nada

    with np.errstate(divide="ignore", invalid="ignore"):
        A["hhi"] = A.sv2 / A.v_h.replace(0, np.nan) ** 2
        A["centro"] = A.sk_alr / A.camino.replace(0, np.nan)
        A["amihud"] = A.amihud * 1e6
        # 1. EFICIENCIA: cuanto del camino quedo como movimiento. 1 = linea recta,
        #    ~0 = ida y vuelta. `lote_ancho` midio esto sobre 24 velas HORARIAS
        #    (`efic_24`); aca es dentro de UNA hora, que es otro objeto.
        A["efic_h"] = np.abs(A.sumr) / A.camino.replace(0, np.nan)
        # 2. VARIANCE RATIO: varianza del agregado sobre la suma de varianzas de 5m.
        #    <1 = reversion a la media / rebote entre puntas; >1 = tendencia intra-hora.
        A["vr_h"] = (A.sumr ** 2) / A.sumr2.replace(0, np.nan)
        # 3. CHOP: cuantas veces se recorrio el rango de la hora.
        A["chop"] = A.rng5 / (A.hi - A.lo).replace(0, np.nan)
        # 4. tamano medio de trade dentro de la hora
        A["tam"] = A.qv_h / A.n_h.replace(0, np.nan)
        # 5. AMIHUD a 5m (ya calculado arriba): |retorno| por dolar operado. Es impacto /
        #    iliquidez medido en la escala en la que el impacto ocurre, no el ratio de los
        #    promedios de 24h que ya midio (y mato) `lote_ancho.vol_por_mov`.
    A["hhi"] = A.hhi * POR_HORA          # 1 = volumen parejo, 12 = todo en una vela
    return A.drop(columns=["nb", "sumr", "sumr2", "rng5", "hi", "lo", "qv_h", "n_h",
                           "sv2", "sk_alr"])


def _rodantes(A):
    """Version suavizada a 24h + las que solo existen como ventana (Roll, autocorr).

    Un valor de UNA hora es ruidoso; la media rodante describe el REGIMEN de
    microestructura del par. Se prueban las dos y la correccion por multiplicidad paga
    el costo de haberlas probado.
    """
    out = pd.DataFrame(index=A.index)
    for c in ("efic_h", "vr_h", "chop", "hhi", "cambios", "centro", "tk_sd",
              "tk_frac", "tam", "amihud"):
        out[f"{c}_24"] = A[c].rolling(VENT, min_periods=VENT // 2).mean()
    return out


def _roll(df5):
    """Autocorrelacion lag-1 de retornos de 5m y spread implicito de Roll, por hora.

    `ac1 < 0` es la firma de la microestructura ilíquida (rebote entre bid y ask).
    Roll (1984): spread efectivo = 2*sqrt(-cov(r_t, r_t-1)) cuando esa cov es negativa.
    Ninguna de las dos se puede calcular con velas de 1h: a esa escala el rebote ya se
    promedio y desaparecio.
    """
    t = df5["t"].to_numpy()
    c = df5["c"].to_numpy(float)
    lr = pd.Series(np.diff(np.log(np.clip(c, 1e-12, None)), prepend=np.log(max(c[0], 1e-12))))
    lr.iloc[0] = 0.0
    w = VENT * POR_HORA                       # 24h de velas de 5m = 288
    cov = lr.rolling(w).cov(lr.shift(1))
    var = lr.rolling(w).var()
    ac1 = (cov / var.replace(0, np.nan)).to_numpy()
    roll = 2.0 * np.sqrt(np.clip(-cov.to_numpy(), 0, None)) * 100     # en %

    d = pd.DataFrame({"hora": (t // MS_H) * MS_H, "ac1_5m": ac1, "roll_sp": roll})
    return d.groupby("hora").last()           # el valor vigente al cierre de la hora


def features_micro(panel5, verbose=True):
    """Una fila por (sym, hora). Todo mira solo velas cerradas de esa hora o anteriores."""
    piezas = []
    for k, (sym, df5) in enumerate(panel5.items(), 1):
        if len(df5) < VENT * POR_HORA * 2:
            continue
        A = _por_hora(df5)
        A = A.join(_rodantes(A)).join(_roll(df5), how="left")
        A.insert(0, "t", A.index)
        A.insert(0, "sym", sym)
        piezas.append(A.reset_index(drop=True))
        if verbose and k % 25 == 0:
            print(f"  micro {k}/{len(panel5)}...", flush=True)
    return pd.concat(piezas, ignore_index=True)


def _atr24(panel):
    """El control de volatilidad. Es la feature que YA se sabe que funciona (y que
    ensancha las dos colas por igual), asi que sin ella el test no dice nada nuevo."""
    piezas = []
    for sym, df in panel.items():
        h, l, c = (df[k].to_numpy(float) for k in "hlc")
        a = pd.Series(h - l).rolling(24).mean().to_numpy() / c
        piezas.append(pd.DataFrame({"sym": sym, "t": df["t"].to_numpy(), "atr_24": a}))
    return pd.concat(piezas, ignore_index=True)


def hipotesis(F, atr, q=0.20, n_min=2000):
    """Colas por quintil, y las MISMAS colas condicionadas al quintil de volatilidad.

    La version condicional es la que contesta la pregunta del preregistro: se toma el
    quintil de la feature DENTRO de cada quintil de `atr_24`, asi que la mascara tiene la
    misma mezcla de volatilidad que la linea base y no puede ganar por ser volatil.
    """
    H = {}
    vq = pd.qcut(atr.rank(method="first"), 5, labels=False, duplicates="drop")
    for c in F.columns:
        s = F[c]
        if s.notna().sum() < n_min or s.nunique() < 5:
            continue
        lo, hi = s.quantile([q, 1 - q])
        if not np.isfinite(lo) or not np.isfinite(hi) or lo == hi:
            continue
        H[f"{c} alto"] = s >= hi
        H[f"{c} bajo"] = s <= lo
        # dentro de cada quintil de vol, el quintil propio de la feature
        r = s.groupby(vq).rank(pct=True)
        H[f"{c} alto | vol"] = r >= 1 - q
        H[f"{c} bajo | vol"] = r <= q
    return H


def nula(T, F, atr, q, reps=5, seed=0):
    """Cuantas hipotesis cruzan las seis compuertas cuando NO hay senal.

    El null de look-elsewhere: 192 hipotesis contra seis compuertas igual dejan pasar
    algo por azar, y sin saber cuanto no se puede leer el resultado real. `movers.py` uso
    la misma idea.

    **No es una permutacion plana.** Barajar las filas destruiria la autocorrelacion de
    los resultados (el regimen dura semanas) y la de las features, y eso hace la nula
    demasiado facil: todo cruzaria menos de lo que corresponde. Aca se DESPLAZA
    CIRCULARMENTE la matriz de features dentro de cada simbolo, con un corrimiento al
    azar por simbolo. Cada feature conserva su propia estructura temporal y cada moneda
    su secuencia de resultados; lo unico que se rompe es la alineacion entre las dos,
    que es exactamente la hipotesis nula.
    """
    rng = np.random.default_rng(seed)
    bloques = [np.flatnonzero((T["sym"] == s).to_numpy()) for s in T["sym"].unique()]
    out = []
    for r in range(reps):
        idx = np.arange(len(T))
        for ii in bloques:
            if len(ii) > 1:
                idx[ii] = np.roll(ii, int(rng.integers(1, len(ii))))
        Fp = F.iloc[idx].set_axis(F.index)
        ap_ = atr.iloc[idx].set_axis(atr.index)
        H = hipotesis(Fp, ap_)
        n1 = int((lote(T, H, q=q, mostrar=False).veredicto == "SOBREVIVE").sum())
        Tc = T.copy()
        Tc["res"] = -Tc["res"]
        Tc.attrs.update(T.attrs)
        n2 = int((lote(Tc, H, q=q, mostrar=False).veredicto == "SOBREVIVE").sum())
        out.append(n1 + n2)
        print(f"  nula {r+1}/{reps}: {n1} largo + {n2} corto = {n1+n2} "
              f"sobrevivientes de {2*len(H)}", flush=True)
    a = np.array(out)
    print(f"\n  NULA: media {a.mean():.2f}  max {a.max()}  "
          f"-> el resultado real tiene que superar {a.max()}")
    return a


def main():
    ap = argparse.ArgumentParser(description="Banco — microestructura intra-vela")
    ap.add_argument("--inicio", default="2025-08-01")
    ap.add_argument("--fin", default="2026-08-01")
    ap.add_argument("--pares", type=int, default=200)
    ap.add_argument("--target", type=float, default=8)
    ap.add_argument("--stop", type=float, default=8)
    ap.add_argument("--horizonte", type=int, default=30)
    ap.add_argument("--paso", type=int, default=12)
    ap.add_argument("--workers", type=int, default=12)
    ap.add_argument("--q", type=float, default=0.05, help="FDR por lado (dos lados)")
    ap.add_argument("--solo-conteo", action="store_true",
                    help="cuenta el n post-join y sale, sin mirar un solo resultado")
    ap.add_argument("--nula", type=int, default=0,
                    help="calibra el look-elsewhere con N desplazamientos y sale")
    ap.add_argument("--out", default=None)
    a = ap.parse_args()

    panel = load_panel(a.inicio, a.fin, n=a.pares, tf="1h", pin="base200", min_bars=2000)
    panel5 = load_panel(a.inicio, a.fin, n=a.pares, tf="5m", pin="base200",
                        min_bars=VENT * POR_HORA * 2, full=True, workers=a.workers)
    comun = [s for s in panel if s in panel5]
    print(f"\npares con 1h y 5m: {len(comun)}")
    panel = {s: panel[s] for s in comun}
    panel5 = {s: panel5[s] for s in comun}

    T = tabla(panel, a.target, a.stop, a.horizonte, a.paso)
    M = features_micro(panel5)
    A = _atr24(panel)
    print(f"filas micro: {len(M):,}  horas cubiertas")

    F = T[["sym", "t"]].merge(M, on=["sym", "t"], how="left")
    F = F.merge(A, on=["sym", "t"], how="left")
    F.index = T.index
    atr = F["atr_24"]
    F = F.drop(columns=["sym", "t", "atr_24"])

    R = T["resuelto"]
    print(f"\nentradas {len(T):,}  resueltas {int(R.sum()):,}")
    print("cobertura por feature (post-join, sobre las resueltas):")
    for c in F.columns:
        ok = int((F[c].notna() & R).sum())
        print(f"  {c:<14} {ok:>7,}  ({ok/int(R.sum())*100:5.1f}%)")
    print(f"  {'atr_24 (control)':<14} {int((atr.notna() & R).sum()):>7,}")
    if a.solo_conteo:
        return
    if a.nula:
        print(f"\nCALIBRACION DE LA NULA ({a.nula} desplazamientos circulares)\n")
        nula(T, F, atr, a.q, reps=a.nula)
        return

    H = hipotesis(F, atr)
    print(f"\n{len(H)} hipotesis ({len(F.columns)} features x 2 colas x "
          f"2 versiones: cruda y condicional a volatilidad)\n")

    print("#" * 100)
    print("# LARGO")
    print("#" * 100)
    D1 = lote(T, H, q=a.q)

    print("\n" + "#" * 100)
    print("# CORTO — la cola de abajo midio mas fuerte que la de arriba en movers.py")
    print("#" * 100)
    Tc = T.copy()
    Tc["res"] = -Tc["res"]
    Tc.attrs.update(T.attrs)
    D2 = lote(Tc, H, q=a.q)

    if a.out:
        D1["lado"], D2["lado"] = "largo", "corto"
        pd.concat([D1, D2], ignore_index=True).to_csv(a.out, index=False)
        print(f"\ntabla -> {a.out}")


if __name__ == "__main__":
    main()
