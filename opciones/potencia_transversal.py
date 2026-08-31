"""
CORRIDA 14 — compuertas (C) y (P): se puede MEDIR la prima de vol DE COSTADO?

Regla de parada, textual de `banco/PREREGISTRO_VOL_TRANSVERSAL.md` §4, escrita antes
de calcular un solo numero transversal:

    (C) POTENCIA: si el MDE de la cartera transversal es > 10%/ano, se declara "no se
        pudo medir" y la direccion se CIERRA. Mismo umbral que la corrida 8. Y hay que
        decir cual de las dos fallo: por n se reabre esperando, por sigma no.

    (P) LA PREMISA: sigma(cartera top-bottom) < sigma(pnl de un nombre promedio). Si
        el diferencing no bajo la varianza, el factor comun no se removio, el
        argumento entero es falso y se cierra ahi — AUNQUE EL MDE DIERA LINDO.

ESTE SCRIPT NO MIRA LA PRIMA. Calcula n y sigma. El signo y el tamano se miran despues
y solo si las dos compuertas pasan, igual que `potencia.py` en la corrida 8.

    $env:PYTHONIOENCODING = "utf-8"
    py -3.13 -u potencia_transversal.py
"""
import os
import sys

import numpy as np
import pandas as pd
import requests

HERE = os.path.dirname(os.path.abspath(__file__))
IVDIR = os.path.join(HERE, "iv_diaria")
CACHE = os.path.join(HERE, ".dvol_cache")

DIAS = 30                 # horizonte del indice de implicita y de la straddle
MDE_MAX = 10.0            # %/ano — el umbral preregistrado (mismo que la corrida 8)
MIN_MONEDAS = 3           # monedas con dato para que la barra cuente
Z = 2.8                   # 1,96 (alfa 0,05 dos colas) + 0,84 (80% de potencia)

# `data-api.binance.vision` es el mirror publico de solo-lectura: no devuelve 451 y
# anda igual desde esta maquina que desde un runner (ver el comentario de radar.py).
SPOT = "https://data-api.binance.vision"

S = requests.Session()
S.headers.update({"User-Agent": "Mozilla/5.0"})


# ---------------------------------------------------------------- datos
def implicitas():
    """Series diarias de implicita, UNA SOLA FUENTE (bybit) por el preregistro §2.

    El DVOL de Deribit no entra en la seccion cruzada aunque sea mas largo: mezclar
    dos indices con metodologias distintas mete una diferencia ENTRE NOMBRES que es de
    metodo y no de mercado, y el ranking la leeria como senal.
    """
    out = {}
    for f in sorted(os.listdir(IVDIR)):
        if not f.startswith("bybit_") or not f.endswith(".csv"):
            continue
        m = f[len("bybit_"):-len(".csv")]
        d = pd.read_csv(os.path.join(IVDIR, f), parse_dates=["fecha"])
        out[m] = d.set_index("fecha")["iv"].sort_index()
    return out


def cierres(sym):
    """Cierres diarios de Binance spot, cacheados. Mismo estimador que iv_rv.py."""
    os.makedirs(CACHE, exist_ok=True)
    p = os.path.join(CACHE, f"px_{sym}.csv")
    if os.path.exists(p):
        return pd.read_csv(p, parse_dates=["fecha"]).set_index("fecha")["c"].sort_index()
    filas = []
    cur = int(pd.Timestamp("2024-01-01", tz="UTC").timestamp() * 1000)
    fin = int(pd.Timestamp.utcnow().timestamp() * 1000)
    while cur < fin:
        r = S.get(f"{SPOT}/api/v3/klines",
                  params={"symbol": sym, "interval": "1d", "startTime": cur,
                          "endTime": fin, "limit": 1000}, timeout=30)
        if r.status_code != 200:
            print(f"    ! binance {sym}: {r.status_code}")
            break
        d = r.json()
        if not d:
            break
        filas.extend(d)
        nuevo = int(d[-1][0])
        if len(d) < 1000 or nuevo <= cur:
            break
        cur = nuevo + 1
    if not filas:
        return pd.Series(dtype=float, name="c")
    s = pd.DataFrame([{"t": int(x[0]), "c": float(x[4])} for x in filas]).drop_duplicates("t")
    s["fecha"] = pd.to_datetime(s["t"], unit="ms", utc=True).dt.tz_localize(None).dt.normalize()
    s = s.groupby("fecha")["c"].last()
    s.to_frame().reset_index().to_csv(p, index=False)
    return s


def panel(moneda, iv):
    """Arma la tabla diaria de una moneda: senal observable + resultado futuro.

    senal    ratio = IV(t) / RV_PASADA(t)   <- solo mira hacia atras
    result.  pnl   = 0,7979 * (IV - RV_FUTURA)/100 * sqrt(30/365) * 100  [% del spot]

    OJO: el `ratio` de iv_rv.py usa rv FUTURA. Alla es un diagnostico ex-post y esta
    bien; aca seria lookahead. Son cosas distintas y por eso no se reusa.
    """
    px = cierres(f"{moneda}USDT")
    if px.empty:
        return None
    lr = np.log(px).diff()
    rv_pas = lr.rolling(DIAS).std() * np.sqrt(365) * 100
    rv_fut = lr.rolling(DIAS).std().shift(-DIAS) * np.sqrt(365) * 100

    d = pd.DataFrame({"iv": iv, "rv_pas": rv_pas, "rv_fut": rv_fut}).dropna()
    if d.empty:
        return None
    d["ratio"] = d.iv / d.rv_pas
    d["pnl"] = 0.7979 * (d.iv - d.rv_fut) / 100 * np.sqrt(DIAS / 365) * 100
    return d


def barras_mensuales(paneles):
    """Una fila por mes NO SOLAPADO y por moneda: el primer dia del mes con dato."""
    filas = []
    for m, d in paneles.items():
        g = d.groupby(d.index.to_period("M")).head(1)
        for f, r in g.iterrows():
            filas.append({"mes": f.to_period("M"), "fecha": f, "moneda": m,
                          "ratio": r["ratio"], "pnl": r["pnl"]})
    return pd.DataFrame(filas)


# ---------------------------------------------------------------- la cartera
def cartera(B):
    """top-bottom por barra, y la version con pesos por rank centrados (secundaria)."""
    filas = []
    for mes, g in B.groupby("mes"):
        if len(g) < MIN_MONEDAS:
            continue
        g = g.sort_values("ratio")
        tb = g.iloc[-1]["pnl"] - g.iloc[0]["pnl"]
        # pesos por rank centrados: suma cero, escala tal que |peso| suma 1 por lado
        r = g["ratio"].rank()
        w = r - r.mean()
        w = w / np.abs(w).sum() * 2 if np.abs(w).sum() else w
        filas.append({"mes": mes, "n_monedas": len(g), "tb": tb,
                      "rank": float((w * g["pnl"]).sum()),
                      "monedas": ",".join(sorted(g["moneda"]))})
    return pd.DataFrame(filas)


def mde(sigma, n):
    return Z * sigma / np.sqrt(n) * 12 if n > 0 else np.inf


def calibracion():
    """Preregistro §5: la misma cuenta sobre una serie con efecto CONOCIDO.

    BTC solo, con el DVOL largo de Deribit (2021->). Si el aparato no reproduce lo que
    `iv_rv.py` ya midio —+20,96%/ano bruto sobre 5,3 anios— el problema es el codigo y
    no el mercado, y un numero nuevo no se puede interpretar.

    Se reporta ademas el MDE con las ULTIMAS 18 barras, que es la ventana de la
    seccion cruzada: la corrida 8 midio 27,1%/ano ahi y ese es el numero a igualar.
    """
    p = os.path.join(IVDIR, "deribit_BTC.csv")
    if not os.path.exists(p):
        print("  (sin deribit_BTC.csv: no se puede calibrar)")
        return
    iv = pd.read_csv(p, parse_dates=["fecha"]).set_index("fecha")["iv"].sort_index()
    d = panel("BTC", iv)
    if d is None or d.empty:
        print("  (panel de BTC vacio)")
        return
    m = d.groupby(d.index.to_period("M")).head(1)          # mensual no solapado
    bruto = 12 * m["pnl"].mean()
    s18, n18 = m["pnl"].tail(18).std(ddof=1), 18
    print(f"  BTC/DVOL  {m.index.min():%Y-%m} -> {m.index.max():%Y-%m}  "
          f"({len(m)} meses no solapados)")
    print(f"  bruto {bruto:+.2f} %/ano        <- iv_rv.py midio +20,96 sobre 5,3 anios")
    print(f"  MDE con las ultimas 18 barras: {mde(s18, n18):.1f} %/ano"
          f"   <- la corrida 8 midio 27,1")


def main():
    print("=" * 78)
    print("CORRIDA 14 — compuertas (C) potencia y (P) premisa")
    print("Este script NO mira la prima. Calcula n y sigma.")
    print("=" * 78)

    print("\n--- CALIBRACION (preregistro §5): el aparato contra un efecto conocido ---")
    calibracion()

    ivs = implicitas()
    print(f"\nimplicitas encontradas: {', '.join(sorted(ivs))}")

    paneles = {}
    for m, iv in sorted(ivs.items()):
        d = panel(m, iv)
        if d is None or d.empty:
            print(f"  {m:6} sin panel (falta spot o no solapa)")
            continue
        paneles[m] = d
        print(f"  {m:6} {len(d):4} dias   {d.index.min():%Y-%m-%d} -> {d.index.max():%Y-%m-%d}")

    if len(paneles) < MIN_MONEDAS:
        sys.exit(f"\nmenos de {MIN_MONEDAS} monedas con panel: no hay seccion cruzada.")

    B = barras_mensuales(paneles)
    C = cartera(B)
    if C.empty:
        sys.exit("\nninguna barra mensual con suficientes monedas.")

    print(f"\n--- barras mensuales NO SOLAPADAS: {len(C)} ---")
    print(f"  {'mes':>8s} {'monedas':>8s}   nombres")
    for _, r in C.iterrows():
        print(f"  {str(r['mes']):>8s} {r['n_monedas']:>8d}   {r['monedas']}")

    # ---- sigmas: la cartera contra los nombres sueltos --------------------
    n = len(C)
    sig_tb = float(C["tb"].std(ddof=1))
    sig_rk = float(C["rank"].std(ddof=1))

    # sigma de un nombre suelto, sobre las MISMAS barras (si no, no es comparable)
    meses_ok = set(C["mes"])
    Bm = B[B["mes"].isin(meses_ok)]
    sig_por_moneda = Bm.groupby("moneda")["pnl"].std(ddof=1).dropna()
    sig_nombre = float(sig_por_moneda.mean())

    # rho media entre pares, para comparar contra el +0,92 de la corrida 8
    W = Bm.pivot_table(index="mes", columns="moneda", values="pnl")
    R = W.corr()
    pares = [R.iloc[i, j] for i in range(len(R)) for j in range(i + 1, len(R))
             if not np.isnan(R.iloc[i, j])]
    rho = float(np.mean(pares)) if pares else float("nan")

    print(f"\n--- sigma mensual (% del spot) ---")
    for m, s in sig_por_moneda.items():
        print(f"  {m:6} {s:6.2f}")
    print(f"  {'PROM':6} {sig_nombre:6.2f}   <- un nombre suelto")
    print(f"  {'top-bot':6} {sig_tb:6.2f}   <- la cartera")
    print(f"  {'rank':6} {sig_rk:6.2f}   (secundaria)")
    print(f"\n  rho media entre pares de nombres: {rho:+.3f}"
          f"   (corrida 8 midio +0,92 en el P&L mensual)")

    # ---- (P) LA PREMISA ---------------------------------------------------
    print("\n" + "=" * 78)
    print("(P) LA PREMISA — el diferencing tiene que bajar la varianza")
    print("=" * 78)
    reduccion = sig_tb / sig_nombre if sig_nombre else np.inf
    print(f"  sigma(cartera) / sigma(nombre) = {reduccion:.3f}")
    print(f"  esperado si rho={rho:+.2f} y las sigmas fueran iguales: "
          f"{np.sqrt(max(2 * (1 - rho), 0)):.3f}")
    p_pasa = sig_tb < sig_nombre
    print(f"  --> {'PASA' if p_pasa else 'NO PASA'}: la cartera "
          f"{'baja' if p_pasa else 'NO baja'} la varianza respecto de un nombre suelto.")
    if not p_pasa:
        print("\n  El factor comun no se removio. El argumento entero de la corrida 14")
        print("  es falso y se cierra aca, sin mirar el MDE ni la prima.")

    # ---- (C) POTENCIA -----------------------------------------------------
    print("\n" + "=" * 78)
    print("(C) POTENCIA — MDE = 2,8 * sigma / sqrt(n) * 12")
    print("=" * 78)
    m_tb, m_rk = mde(sig_tb, n), mde(sig_rk, n)
    print(f"  n = {n} meses no solapados")
    print(f"  top-bottom   sigma {sig_tb:5.2f}   MDE {m_tb:6.1f} %/ano")
    print(f"  rank         sigma {sig_rk:5.2f}   MDE {m_rk:6.1f} %/ano")
    print(f"  umbral preregistrado: {MDE_MAX:.1f} %/ano")

    c_pasa = m_tb <= MDE_MAX
    print(f"\n  --> {'PASA' if c_pasa else 'NO PASA'}")
    if not c_pasa:
        # cual de las dos fallo — regla de la corrida 9
        n_hace_falta = (Z * sig_tb * 12 / MDE_MAX) ** 2
        sig_hace_falta = MDE_MAX * np.sqrt(n) / (Z * 12)
        print(f"      con esta sigma harian falta {n_hace_falta:.0f} meses "
              f"({n_hace_falta / 12:.1f} anios) — faltan {n_hace_falta - n:.0f}")
        print(f"      con este n haria falta una sigma de {sig_hace_falta:.2f} "
              f"(la medida es {sig_tb:.2f})")
        if n_hace_falta - n <= 24:
            print("      FALLA POR n: se reabre esperando. El colector diario "
                  "(PR #28) es lo que la destapa.")
        else:
            print("      FALLA POR SIGMA mas que por n: no se arregla esperando "
                  "un par de meses.")

    print("\n" + "=" * 78)
    if p_pasa and c_pasa:
        print("LAS DOS COMPUERTAS PASAN. Recien ahora se puede mirar la prima:")
        print("  correr el paso 3 del preregistro (bootstrap por meses, sin el mejor")
        print("  mes, sin el mejor par, >=60% de meses, dos costos).")
    else:
        print("NO SE PUDO MEDIR. La direccion se CIERRA por el preregistro §4.")
        print("Anotar el veredicto en banco/PREREGISTRO_VOL_TRANSVERSAL.md y decir")
        print("cual de las dos compuertas fallo.")
    print("=" * 78)

    return 0 if (p_pasa and c_pasa) else 0   # el veredicto es el texto, no el exit code


if __name__ == "__main__":
    sys.exit(main())
