"""
MACRO — series de AFUERA del sistema cripto, alineadas al panel del banco.

Por que existe: las catorce corridas midieron doce familias, y **todas** son patrones en
precio, volumen, derivados u on-chain de cripto. `HANDOFF_FUENTES_NUEVAS.md` §2 verifico
con grep que macro / cross-asset no aparece en ninguna parte del arbol: los unicos hits de
oro son `PAXG`/`XAUT` siendo EXCLUIDOS del universo. Esto es una fuente nueva, no una
feature mas sobre la misma ventana de precios.

La familia "regimen" que ya se cerro eran **7 detectores INTERNOS a cripto** (BTC sobre su
EMA, breadth, vol de mercado). Condicionar por una variable de afuera es otra cosa.

    py -3.13 -u macro.py --cobertura      # baja las series y reporta cobertura y desfase

EL DATO. Yahoo Finance, endpoint publico de charts, sin dependencias nuevas (no hay
`yfinance` ni `pandas_datareader` en esta maquina, y FRED da timeout desde aca; Stooq
devuelve un desafio de JavaScript). Cinco factores, uno por eje economico:

    NDX   ^NDX       Nasdaq 100          activo de riesgo
    DXY   DX-Y.NYB   indice dolar ICE    dolar
    ORO   GC=F       futuro COMEX        activo real
    T10   ^TNX       rendimiento 10a     tasas
    VIX   ^VIX       volatilidad SPX     aversion al riesgo

No entra el 2 anios: un factor por eje, y ^TNX ya ocupa el de tasas. Meter 2a y 10a es
meter dos versiones de la misma variable y pagarlas dos veces en el FDR.

EL DESFASE, que es donde vive el lookahead (`HANDOFF_FUENTES_NUEVAS.md` §4.2, punto 2):

  - El cierre de la fecha D se declara disponible recien a las **00:00 UTC de D+1**. El
    cash de EEUU cierra 20:00-21:00 UTC, asi que la regla es conservadora por >=3h.
  - Despues se rellena **HACIA ADELANTE** sobre la grilla de barras. Rellenar hacia
    adelante es informacion de ayer y esta bien. Rellenar hacia atras es lookahead.
  - Los retornos del factor se calculan entre **dias de rueda**, no de calendario, y los
    retornos de cripto se miden sobre **esos mismos intervalos**. Si no, el viernes->lunes
    de cripto (3 dias) se comparara contra el viernes->lunes del Nasdaq (1 rueda) y la
    beta seria un artefacto de calendario.

LA GRILLA. Con `paso=168` el tablero pone las barras cada 168h desde el primer dato del
panel. Para la ventana 2021-08-01 -> 2026-08-01 eso es **domingo 00:00 UTC**, todas. No es
un problema, es lo mejor que podia pasar: cada barra ve la semana de rueda ENTERA y ya
cerrada (viernes), con dos dias de margen, y el desfase es el mismo en todas las barras.
"""
import argparse
import os
import sys
import time

import numpy as np
import pandas as pd
import requests

HERE = os.path.dirname(os.path.abspath(__file__))
CACHE = os.path.join(HERE, ".macro_cache")

# nombre corto -> ticker de Yahoo
SERIES = {
    "NDX": "^NDX",
    "DXY": "DX-Y.NYB",
    "ORO": "GC=F",
    "T10": "^TNX",
    "VIX": "^VIX",
}

VENTANA_BETA = 90       # ruedas para la regresion movil (~4,5 meses)
MIN_BETA = 60           # ruedas minimas para que la beta exista
DIAS_RUEDA_SENAL = 5    # el retorno del factor que condiciona = la semana de rueda

S = requests.Session()
S.headers.update({"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                                "AppleWebKit/537.36"})


# ---------------------------------------------------------------- descarga
def bajar(nombre, ticker, inicio="2020-06-01", fin="2026-09-01"):
    """Cierres diarios de un factor, cacheados en disco. Devuelve Series indexada por
    fecha (naive, normalizada) con el cierre de esa rueda."""
    os.makedirs(CACHE, exist_ok=True)
    p = os.path.join(CACHE, f"{nombre}.csv")
    if os.path.exists(p):
        d = pd.read_csv(p, parse_dates=["fecha"])
        return d.set_index("fecha")["c"].sort_index()

    p1 = int(pd.Timestamp(inicio, tz="UTC").timestamp())
    p2 = int(pd.Timestamp(fin, tz="UTC").timestamp())
    r = S.get(f"https://query1.finance.yahoo.com/v8/finance/chart/{ticker}",
              params={"period1": p1, "period2": p2, "interval": "1d"}, timeout=30)
    if r.status_code != 200:
        raise RuntimeError(f"{nombre} ({ticker}): HTTP {r.status_code}")
    res = r.json()["chart"]["result"][0]
    ts = res["timestamp"]
    cl = res["indicators"]["quote"][0]["close"]
    d = pd.DataFrame({"t": ts, "c": cl}).dropna()
    # El timestamp de Yahoo es la APERTURA de la rueda en hora del mercado. Lo unico
    # que se usa de el es la FECHA de rueda; la hora de disponibilidad la fija `alinear`.
    d["fecha"] = pd.to_datetime(d["t"], unit="s", utc=True).dt.tz_convert(
        "America/New_York").dt.tz_localize(None).dt.normalize()
    s = d.groupby("fecha")["c"].last().sort_index()
    s.to_frame().reset_index().to_csv(p, index=False)
    return s


def cargar(verbose=True):
    """DataFrame diario con una columna por factor, en fechas de rueda."""
    out = {}
    for nombre, tk in SERIES.items():
        s = bajar(nombre, tk)
        out[nombre] = s
        if verbose:
            print(f"  {nombre:4} {tk:10} {len(s):5} ruedas  "
                  f"{s.index.min():%Y-%m-%d} -> {s.index.max():%Y-%m-%d}")
    return pd.DataFrame(out).sort_index()


# ---------------------------------------------------------------- alineacion
def disponible_utc(fechas):
    """Instante (ms UTC) en que el cierre de cada rueda se declara disponible: 00:00 UTC
    del dia siguiente. Conservador por >=3h contra el cierre real del cash de EEUU."""
    return ((pd.DatetimeIndex(fechas) + pd.Timedelta(days=1))
            .tz_localize("UTC").astype("int64") // 10**6)


def panel_diario(panel):
    """Cierre diario (UTC) de cada par del panel horario. DataFrame fecha x simbolo."""
    cols = {}
    for sym, df in panel.items():
        t = pd.to_datetime(df["t"].to_numpy(), unit="ms", utc=True)
        s = pd.Series(df["c"].to_numpy(float), index=t)
        cols[sym] = s.groupby(s.index.tz_localize(None).normalize()).last()
    return pd.DataFrame(cols).sort_index()


def betas(panel, M, ventana=VENTANA_BETA, min_obs=MIN_BETA, verbose=True):
    """Beta movil de cada par contra cada factor, en la grilla de RUEDAS.

    Devuelve (BETA, SE, RF) donde:
      BETA[f]  DataFrame fecha_rueda x simbolo con la beta estimada CON DATOS HASTA
               ESA RUEDA INCLUSIVE (la disponibilidad la aplica `alinear`)
      SE[f]    el error estandar de esa misma beta (lo necesita la compuerta (P))
      RF       DataFrame fecha_rueda x factor con el log-retorno de la rueda

    Los dos lados se miden sobre LOS MISMOS INTERVALOS (rueda a rueda), no sobre dias de
    calendario: el cierre de cripto se reindexa a las fechas de rueda antes de diferenciar.
    Sin eso, el salto viernes->lunes de cripto (3 dias) se compararia contra una sola
    rueda del Nasdaq y la beta mediria calendario.
    """
    PD = panel_diario(panel)
    ruedas = M.index[M.index.isin(PD.index)]
    if verbose:
        print(f"  ruedas con cripto y macro: {len(ruedas)}  "
              f"{ruedas.min():%Y-%m-%d} -> {ruedas.max():%Y-%m-%d}")

    RC = np.log(PD.reindex(ruedas)).diff()          # cripto, rueda a rueda
    RF = np.log(M.reindex(ruedas)).diff()           # factor, rueda a rueda
    # ^TNX y ^VIX son NIVELES de una tasa/volatilidad, no precios. El log-retorno igual
    # es la variacion relativa y es lo comparable entre factores; se deja explicito
    # porque "el retorno del VIX" no significa lo mismo que "el retorno del Nasdaq".

    BETA, SE = {}, {}
    for f in M.columns:
        x = RF[f]
        vx = x.rolling(ventana, min_periods=min_obs).var(ddof=1)
        mx = x.rolling(ventana, min_periods=min_obs).mean()
        b, se = {}, {}
        for sym in RC.columns:
            y = RC[sym]
            # cov movil con la identidad E[xy]-E[x]E[y]; sin groupby.apply (regla del repo)
            n = y.notna().rolling(ventana, min_periods=min_obs).sum()
            cxy = (x * y).rolling(ventana, min_periods=min_obs).mean() - mx * y.rolling(
                ventana, min_periods=min_obs).mean()
            cxy = cxy * n / (n - 1)
            beta = cxy / vx
            # error estandar de la pendiente: sd(residuo) / (sd(x) * sqrt(n))
            vy = y.rolling(ventana, min_periods=min_obs).var(ddof=1)
            resid = (vy - beta * cxy).clip(lower=0)
            se[sym] = np.sqrt(resid / (n - 2)) / np.sqrt(vx * n)
            b[sym] = beta
        BETA[f] = pd.DataFrame(b)
        SE[f] = pd.DataFrame(se)
        if verbose:
            cob = BETA[f].notna().mean().mean()
            print(f"  beta vs {f:4}  cobertura {cob:5.1%}")
    return BETA, SE, RF


def alinear(TB, BETA, SE, RF, factores=None, verbose=True):
    """Pega a cada fila (sym, t) del tablero la beta y el retorno del factor que YA
    ESTABAN DISPONIBLES en `t`. Devuelve (TB con columnas nuevas, diagnostico).

    Columnas agregadas, por factor F:
        beta_F    exposicion del par al factor  (transversal: varia entre pares)
        se_F      error estandar de esa beta    (solo diagnostico, no es un score)
        d_F       retorno del factor en las ultimas 5 ruedas (constante DENTRO de la
                  barra: por si solo no es un ranking, solo sirve como interaccion)
        stale_F   dias de calendario entre el ultimo cierre usado y la barra
    """
    factores = list(factores or BETA)
    TB = TB.copy()
    diag = {}

    # instante de disponibilidad de cada rueda, y el retorno acumulado de 5 ruedas
    ruedas = RF.index
    disp = pd.Series(disponible_utc(ruedas), index=ruedas)
    D5 = RF.rolling(DIAS_RUEDA_SENAL).sum()

    # para cada barra del tablero, cual es la ULTIMA rueda disponible (busqueda binaria)
    barras = np.sort(TB["t"].unique())
    pos = np.searchsorted(disp.to_numpy(), barras, side="right") - 1
    ok = pos >= 0
    rueda_de_barra = pd.Series(pd.NaT, index=barras)
    rueda_de_barra[ok] = ruedas[pos[ok]]
    ret_barra = TB["t"].map(rueda_de_barra)

    dt_barra = pd.to_datetime(TB["t"], unit="ms", utc=True).dt.tz_localize(None)
    for f in factores:
        B, E = BETA[f], SE[f]
        idx = pd.MultiIndex.from_arrays([ret_barra, TB["sym"]])
        TB[f"beta_{f}"] = B.stack(future_stack=True).reindex(idx).to_numpy()
        TB[f"se_{f}"] = E.stack(future_stack=True).reindex(idx).to_numpy()
        TB[f"d_{f}"] = ret_barra.map(D5[f]).to_numpy()
        TB[f"stale_{f}"] = (dt_barra - ret_barra).dt.days.to_numpy()
        diag[f] = {
            "cobertura_beta": float(TB[f"beta_{f}"].notna().mean()),
            "stale_mediano": float(TB[f"stale_{f}"].median()),
            "stale_p95": float(TB[f"stale_{f}"].quantile(0.95)),
            "barras_stale_gt5": float((TB[f"stale_{f}"] > 5).mean()),
        }
        if verbose:
            d = diag[f]
            print(f"  {f:4} cobertura {d['cobertura_beta']:5.1%} | desfase mediano "
                  f"{d['stale_mediano']:.0f}d p95 {d['stale_p95']:.0f}d | "
                  f"barras con hueco >5d {d['barras_stale_gt5']:.2%}")
    return TB, diag


# ---------------------------------------------------------------- main
def main():
    ap = argparse.ArgumentParser(description="Banco — factores macro externos")
    ap.add_argument("--cobertura", action="store_true",
                    help="bajar las series y reportar cobertura contra el panel")
    ap.add_argument("--inicio", default="2021-08-01")
    ap.add_argument("--fin", default="2026-08-01")
    ap.add_argument("--pares", type=int, default=200)
    ap.add_argument("--pin", default="base200")
    a = ap.parse_args()

    print("=" * 84)
    print("MACRO — descarga y alineacion. NO mide ninguna hipotesis.")
    print("=" * 84)

    print("\n--- series (Yahoo, cacheadas) ---")
    M = cargar()

    if not a.cobertura:
        print("\n(sin --cobertura no hay nada mas que hacer)")
        return 0

    from klines import load_panel
    from ranking import tablero

    t0 = time.time()
    panel = load_panel(a.inicio, a.fin, n=a.pares, pin=a.pin, full=True)
    if not panel:
        print("FATAL: no se pudo cargar el panel")
        return 1

    TB = tablero(panel, paso=168, horizonte=168)
    print(f"\n--- betas moviles ({VENTANA_BETA} ruedas) ---")
    BETA, SE, RF = betas(panel, M)
    print("\n--- alineacion contra la grilla de barras ---")
    TB, diag = alinear(TB, BETA, SE, RF)

    dt = pd.to_datetime(TB["t"].unique(), unit="ms", utc=True)
    print(f"\n  barras: {len(dt)}  |  dias de la semana: "
          f"{sorted(set(dt.day_name()))}")
    print(f"  {time.time()-t0:.0f}s")
    return 0


if __name__ == "__main__":
    sys.exit(main())
