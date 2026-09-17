"""
CORRIDA 16 — compuerta (C): se puede MEDIR el efecto de los flujos de ETF spot?

Regla de parada, textual de `banco/PREREGISTRO_ETF.md` §4, escrita antes de calcular un
solo sigma:

    (C) POTENCIA: si el MDE del retorno neto ANUALIZADO supera los 10%/ano, ese diseno se
        declara "no se pudo medir". Si fallan los TRES disenos, la direccion se CIERRA.
        Mismo umbral que las corridas 8, 13 y 14. Y hay que decir cual de las dos fallo:
        por n se reabre esperando, por sigma no.

    (P) LA PREMISA: mediana(|flujo neto diario|) / volumen spot del mismo dia > 1%. Si los
        flujos no son grandes contra el mercado que dicen mover, el argumento de "son
        compras y ventas REALES" es falso y se cierra ahi — AUNQUE EL MDE DIERA LINDO.

ESTE SCRIPT NO MIRA NINGUN RETORNO CONDICIONADO AL FLUJO. Calcula n y sigma. Si imprimiera
uno, dejaria de ser una compuerta.

EL HALLAZGO QUE HACE QUE ESTO CUESTE MEDIA TARDE: (C) no necesita los datos de flujo. La
sigma del P&L de una posicion +-1 sobre BTC ES la sigma de BTC — no depende de cual sea la
senal. Y n es un hecho de calendario. Los flujos solo hacen falta para (P).

    $env:PYTHONIOENCODING = "utf-8"
    py -3.13 -u potencia_etf.py

GOTCHA de esta maquina: no hay `lxml` ni `html5lib` ni `bs4`, asi que `pandas.read_html`
revienta. La tabla de Farside se parsea con `html.parser` de la stdlib. No se instala nada.
"""
import argparse
import os
import sys
import time
from html.parser import HTMLParser

import numpy as np
import pandas as pd
import requests

from klines import load_panel
from ranking import COSTO_PCT, controles, evaluar, tablero

HERE = os.path.dirname(os.path.abspath(__file__))
CACHE = os.path.join(HERE, ".etf_cache")

INICIO, FIN = "2021-08-01", "2026-08-01"
ETF_BTC_D0 = "2024-01-11"          # primera fila de la tabla de Farside
H = 168                            # la barra: la semana, sin solape
K = 20
N_CTRL = 8
Z = 2.80                           # 1,96 (alfa 0,05 dos colas) + 0,84 (80% de potencia)
SEM_ANO = 52.0
HORAS_ANO = 8760
MDE_MAX = 10.0                     # %/ano — el umbral preregistrado
PREMISA_MIN = 0.01                 # 1% del volumen spot del mismo dia

FUENTES = {"BTC": "https://farside.co.uk/bitcoin-etf-flow-all-data/",
           "ETH": "https://farside.co.uk/ethereum-etf-flow-all-data/"}

S = requests.Session()
S.headers.update({"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                                "AppleWebKit/537.36"})


# ------------------------------------------------------------------ el parser
class _Tabla(HTMLParser):
    """Extrae las filas de la primera <table class="etf">. Stdlib, sin dependencias.

    La tabla de Farside tiene `<div>` y `<span>` adentro de cada `<td>` y algun `<tr>`
    sin cerrar, asi que se acumula el texto por celda y se abre fila nueva en cada `<tr>`
    en vez de confiar en el cierre.
    """

    def __init__(self):
        super().__init__()
        self.filas = []
        self._fila = None
        self._celda = None
        self._en = False
        self._prof = 0
        self._nivel = None

    def handle_starttag(self, tag, attrs):
        if tag == "table":
            self._prof += 1
            if not self._en and dict(attrs).get("class") == "etf":
                self._en, self._nivel = True, self._prof
        if not self._en:
            return
        if tag == "tr":
            self._fila = []
        elif tag in ("td", "th"):
            self._celda = []

    def handle_endtag(self, tag):
        if tag == "table":
            if self._en and self._prof == self._nivel:
                self._en = False
            self._prof -= 1
            return
        if not self._en:
            return
        if tag in ("td", "th") and self._celda is not None:
            if self._fila is not None:
                self._fila.append("".join(self._celda).strip())
            self._celda = None
        elif tag == "tr":
            if self._fila:
                self.filas.append(self._fila)
            self._fila = None

    def handle_data(self, data):
        if self._celda is not None:
            self._celda.append(data)


def _num(s):
    """Un valor de la tabla -> float. Farside usa '-' para cero y parentesis para negativo."""
    s = s.strip().replace(",", "").replace("$", "").replace("\xa0", "")
    if s in ("", "-", "--"):
        return 0.0
    neg = s.startswith("(") and s.endswith(")")
    if neg:
        s = s[1:-1]
    try:
        v = float(s)
    except ValueError:
        return np.nan
    return -v if neg else v


def bajar_farside(cual, refrescar=False):
    """Serie diaria del flujo neto TOTAL, en millones de USD. Cacheada en disco.

    OJO (preregistro §2): esta tabla SE REVISA HACIA ATRAS. Lo que se baja hoy no es lo
    que se veia ese dia. Sirve para la premisa (una magnitud tipica) y NO serviria para
    un backtest sin resolver el point-in-time primero.
    """
    os.makedirs(CACHE, exist_ok=True)
    p = os.path.join(CACHE, f"{cual}.csv")
    if os.path.exists(p) and not refrescar:
        d = pd.read_csv(p, parse_dates=["fecha"])
        return d.set_index("fecha")["flujo"].sort_index()

    r = S.get(FUENTES[cual], timeout=40)
    if r.status_code != 200:
        raise RuntimeError(f"farside {cual}: HTTP {r.status_code}")
    T = _Tabla()
    T.feed(r.text)
    if not T.filas:
        raise RuntimeError(f"farside {cual}: no se pudo parsear ninguna fila")

    cab = T.filas[0]
    try:
        i_tot = [c.strip().lower() for c in cab].index("total")
    except ValueError:
        raise RuntimeError(f"farside {cual}: no hay columna 'Total' en {cab}")

    filas = []
    for f in T.filas[1:]:
        if len(f) <= i_tot:
            continue
        fecha = pd.to_datetime(f[0].strip(), format="%d %b %Y", errors="coerce")
        if pd.isna(fecha):          # 'Total', 'Average', 'Maximum', ...
            continue
        v = _num(f[i_tot])
        if not np.isnan(v):
            filas.append({"fecha": fecha, "flujo": v})
    if not filas:
        raise RuntimeError(f"farside {cual}: 0 filas con fecha valida")

    s = pd.DataFrame(filas).drop_duplicates("fecha").set_index("fecha")["flujo"].sort_index()
    s.to_frame().reset_index().to_csv(p, index=False)
    return s


# ------------------------------------------------------------------ potencia
def mde_anual(sigma_sem, n, vueltas=SEM_ANO):
    """MDE al 80% de potencia, anualizado. `sigma_sem` en % por barra semanal."""
    return Z * sigma_sem / np.sqrt(n) * vueltas if n > 0 else np.inf


def falla_por(sigma_sem, n, vueltas=SEM_ANO, tope=MDE_MAX):
    """Cual de las dos fallo (regla de la corrida 9). Devuelve (n_necesario, sigma_nec)."""
    n_nec = (Z * sigma_sem * vueltas / tope) ** 2
    s_nec = tope * np.sqrt(n) / (Z * vueltas)
    return n_nec, s_nec


def ret_semanal(df, t0_ms, desde=None):
    """Retornos de barras de 168h SIN SOLAPE, en la misma grilla que `ranking.tablero`."""
    t = df["t"].to_numpy()
    c = df["c"].to_numpy(float)
    en = np.flatnonzero((t - t0_ms) % (H * 3600000) == 0)
    en = en[en < len(c) - H]
    if desde is not None:
        en = en[t[en] >= desde]
    if len(en) < 2:
        return pd.Series(dtype=float)
    return pd.Series(c[en + H] / c[en] - 1.0,
                     index=pd.to_datetime(t[en], unit="ms", utc=True)) * 100.0


def nula_transversal(TB, k=K, costo=COSTO_PCT):
    """sigma semanal y n de la NULA REAL del harness, y el MDE anualizado.

    Es la sigma del diseno C: la de un spread top-k contra el universo de la misma barra,
    que es MUCHO mas baja que la de una posicion direccional pelada. Por eso C es el
    diseno mas favorable y el que decide.
    """
    C = controles(TB, n=N_CTRL, seed=12345)
    filas = [evaluar(TB, s, nm, objetivo=o, k=k, costo=costo)
             for o in ("largo", "corto") for nm, s in C.items()]
    D = pd.DataFrame(filas)
    sd = float(D["sd_sem"].median())            # en ATR
    n = float(D["semanas"].median())
    atr = float(TB["atr_base"].median()) * 100.0
    vueltas = HORAS_ANO / H
    return sd, n, atr, mde_anual(sd * atr, n, vueltas), COSTO_PCT * vueltas


# ------------------------------------------------------------------ main
def main():
    ap = argparse.ArgumentParser(description="Corrida 16 — compuerta de potencia de ETF")
    ap.add_argument("--refrescar", action="store_true", help="re-bajar Farside")
    a = ap.parse_args()

    t0 = time.time()
    print("=" * 92)
    print("CORRIDA 16 — COMPUERTA DE POTENCIA DE LOS FLUJOS DE ETF SPOT")
    print("Este script NO mira ningun retorno condicionado al flujo. Calcula n y sigma.")
    print("=" * 92)

    # ---------------------------------------------------------------- flujos
    print("\n--- flujos (Farside, parser stdlib, cacheado) ---")
    F = {}
    for cual in FUENTES:
        try:
            s = bajar_farside(cual, a.refrescar)
            F[cual] = s
            print(f"  {cual}  {len(s):4} dias  {s.index.min():%Y-%m-%d} -> "
                  f"{s.index.max():%Y-%m-%d}  |  acumulado {s.sum()/1000:+8.1f} mil M USD")
        except Exception as e:
            print(f"  {cual}  FALLO: {type(e).__name__} {e}")
    if "BTC" not in F:
        print("\nSin la tabla de BTC no se puede correr (P). (C) no la necesita, pero el")
        print("preregistro pide las dos compuertas.")
        return 1

    # ---------------------------------------------------------------- panel
    panel = load_panel(INICIO, FIN, n=200, pin="base200", full=True)
    if not panel:
        print("FATAL: no se pudo cargar el panel")
        return 1
    t0_ms = min(int(df["t"].iloc[0]) for df in panel.values())
    d0 = int(pd.Timestamp(ETF_BTC_D0, tz="UTC").timestamp() * 1000)

    # ------------------------------------------------------- (P) LA PREMISA
    print("\n" + "=" * 92)
    print("(P) LA PREMISA — los flujos son grandes contra el mercado que dicen mover?")
    print("=" * 92)
    print(f"  criterio preregistrado: mediana(|flujo diario|) / volumen spot > "
          f"{PREMISA_MIN:.0%}")
    print("  el denominador es SOLO Binance spot, o sea que la fraccion sale MAS GRANDE")
    print("  de lo que es: la premisa recibe el beneficio de la duda (preregistro §4).")

    bt = panel["BTCUSDT"]
    qv = pd.Series(bt["qv"].to_numpy(float),
                   index=pd.to_datetime(bt["t"].to_numpy(), unit="ms", utc=True))
    qv_d = qv.groupby(qv.index.tz_localize(None).normalize()).sum()

    print(f"\n  {'activo':>7}{'|flujo| med.':>15}{'vol spot med.':>16}"
          f"{'fraccion':>11}   veredicto")
    p_pasa = False
    prem = {}
    for cual, s in F.items():
        sym = f"{cual}USDT"
        if sym not in panel:
            continue
        b = panel[sym]
        q = pd.Series(b["qv"].to_numpy(float),
                      index=pd.to_datetime(b["t"].to_numpy(), unit="ms", utc=True))
        q_d = q.groupby(q.index.tz_localize(None).normalize()).sum()
        j = pd.DataFrame({"flujo": s.abs() * 1e6, "vol": q_d}).dropna()
        if j.empty:
            continue
        frac = float((j["flujo"] / j["vol"]).median())
        prem[cual] = frac
        ok = frac > PREMISA_MIN
        p_pasa = p_pasa or ok
        print(f"  {cual:>7}{j['flujo'].median()/1e6:>12.1f} M{j['vol'].median()/1e9:>13.2f} B"
              f"{frac:>11.2%}   {'PASA' if ok else 'NO PASA'}")

    print(f"\n  --> {'PASA' if p_pasa else 'NO PASA'}: los flujos "
          f"{'son' if p_pasa else 'NO son'} una fraccion material del volumen spot.")

    # ------------------------------------------------------- (C) POTENCIA
    print("\n" + "=" * 92)
    print("(C) POTENCIA — MDE = 2,8 * sigma_semanal / sqrt(n) * 52")
    print("=" * 92)

    TB = tablero(panel, paso=H, horizonte=H)
    dt = pd.to_datetime(TB["t"], unit="ms", utc=True)
    TB_etf = TB[dt >= pd.Timestamp(ETF_BTC_D0, tz="UTC")].reset_index(drop=True)

    # --- calibracion: el mismo aparato contra un numero YA PUBLICADO ---
    print("\n--- CALIBRACION (preregistro §4): reproducir la corrida 13 ---")
    sd_c, n_c, atr_c, mde_c, costo_c = nula_transversal(TB)
    print(f"  panel completo: {n_c:.0f} barras de 168h | sd {sd_c:.4f} ATR | "
          f"ATR base {atr_c:.2f}%")
    print(f"  MDE {mde_c:.1f} %/ano neto    <- la corrida 13 midio 22,0 con 255 barras")
    print(f"                                  (la corrida 15 midio 25,1 con 203)")

    filas = []

    # CADA DISENO ARRANCA CUANDO EXISTE EL FLUJO QUE NECESITA, no cuando arranca el de
    # BTC. Los ETF de ETH se lanzaron en julio de 2024, asi que darle a un diseno con
    # pata de ETH las 26 semanas de enero a julio seria regalarle un n que no existe.
    def _d0(*cuales):
        return max(int(F[c].index.min().tz_localize("UTC").timestamp() * 1000)
                   for c in cuales)

    # --- A: timing directo, posicion +-1 sobre el activo ---
    for cual in ("BTC", "ETH"):
        sym = f"{cual}USDT"
        if sym not in panel or cual not in F:
            continue
        r = ret_semanal(panel[sym], t0_ms, desde=_d0(cual))
        if len(r) < 3:
            continue
        sg, n = float(r.std(ddof=1)), len(r)
        filas.append({"diseno": f"A  timing {cual} (+-1)", "n_semanas": n,
                      "desde": f"{F[cual].index.min():%Y-%m-%d}",
                      "sigma_sem_pct": sg, "mde_anual": mde_anual(sg, n),
                      "costo_anual": COSTO_PCT * SEM_ANO})

    # --- B: long-short BTC vs ETH (necesita LAS DOS series de flujo) ---
    if {"BTCUSDT", "ETHUSDT"} <= set(panel) and {"BTC", "ETH"} <= set(F):
        db = _d0("BTC", "ETH")
        rb = ret_semanal(panel["BTCUSDT"], t0_ms, desde=db)
        re_ = ret_semanal(panel["ETHUSDT"], t0_ms, desde=db)
        d = (rb - re_).dropna()
        if len(d) > 3:
            sg = float(d.std(ddof=1))
            filas.append({"diseno": "B  long-short BTC/ETH", "n_semanas": len(d),
                          "desde": f"{pd.Timestamp(db, unit='ms'):%Y-%m-%d}",
                          "sigma_sem_pct": sg, "mde_anual": mde_anual(sg, len(d)),
                          "costo_anual": 2 * COSTO_PCT * SEM_ANO,
                          "rho": float(rb.corr(re_))})

    # --- C: el flujo condicionando el panel transversal ---
    # Condiciona por el flujo de BTC, que es la serie mas larga: arranca 2024-01-11.
    sd_e, n_e, atr_e, mde_e, costo_e = nula_transversal(TB_etf)
    print(f"  ventana ETF: {n_e:.0f} barras | sd {sd_e:.4f} ATR | ATR base {atr_e:.2f}%")
    filas.append({"diseno": "C  transversal condicionado", "n_semanas": n_e,
                  "desde": ETF_BTC_D0, "sigma_sem_pct": sd_e * atr_e,
                  "mde_anual": mde_e, "costo_anual": costo_e})

    D = pd.DataFrame(filas)
    D["bruto"] = D["mde_anual"] + D["costo_anual"]
    D["pasa"] = D["mde_anual"] <= MDE_MAX

    print(f"\n  desde {ETF_BTC_D0} (lanzamiento de los ETF de BTC)")
    print(f"\n  {'diseno':>30}{'desde':>12}{'n sem':>7}{'sigma sem':>11}{'MDE %/ano':>12}"
          f"{'costo':>8}{'BRUTO nec.':>12}{'':>6}")
    for _, r in D.iterrows():
        print(f"  {r['diseno']:>30}{r['desde']:>12}{int(r.n_semanas):>7}{r.sigma_sem_pct:>11.2f}"
              f"{r.mde_anual:>12.1f}{r.costo_anual:>8.2f}{r.bruto:>12.1f}"
              f"{'   ok' if r.pasa else '   NO'}")
    print(f"\n  umbral preregistrado: MDE <= {MDE_MAX:.0f} %/ano")

    print("\n  cual de las dos falla (regla de la corrida 9):")
    for _, r in D.iterrows():
        if r.pasa:
            continue
        vueltas = SEM_ANO if r["diseno"].startswith(("A", "B")) else HORAS_ANO / H
        n_nec, s_nec = falla_por(r.sigma_sem_pct, r.n_semanas, vueltas)
        anos = (n_nec - r.n_semanas) / vueltas
        cual = "n" if (n_nec - r.n_semanas) <= 2 * vueltas else "SIGMA"
        print(f"    {r['diseno']:>30}  harian falta {n_nec:,.0f} barras "
              f"({anos:,.0f} anos mas) o una sigma de {s_nec:.3f} "
              f"(la medida es {r.sigma_sem_pct:.2f})  -> falla por {cual}")

    D.to_csv("potencia_etf.csv", index=False)

    # ---------------------------------------------------------- veredicto
    print("\n" + "=" * 92)
    print("VEREDICTO")
    print("=" * 92)
    if D["pasa"].any():
        print("  Algun diseno cruza el umbral. NO se mide nada en esta corrida:")
        print("  la medicion necesita un preregistro propio que ademas resuelva el")
        print("  look-ahead de revision (preregistro §2) y la reserva OOS 2024-08/2025-08,")
        print("  que cae justo en el medio de la historia de los ETF.")
    else:
        mejor = D.loc[D["mde_anual"].idxmin()]
        print(f"  NO SE PUDO MEDIR con ninguno de los {len(D)} disenos.")
        print(f"  El mas favorable ({mejor['diseno'].strip()}) queda en "
              f"{mejor.mde_anual:.1f} %/ano contra un umbral de {MDE_MAX:.0f}: "
              f"falla por {mejor.mde_anual / MDE_MAX:.0f}x.")
        print("\n  La direccion se CIERRA por el preregistro §4. Y la distincion importa:")
        print("  no es que los flujos de ETF no sirvan, es que con la historia que")
        print("  existe no se puede saber — y como falla por SIGMA, no se arregla")
        print("  esperando. Anotarlo en banco/PREREGISTRO_ETF.md.")
    if not p_pasa:
        print("\n  Y ademas (P) NO PASA: el mecanismo declarado tampoco esta.")

    print(f"\n  ({time.time()-t0:.0f}s)  -> potencia_etf.csv")
    print("=" * 92)
    return 0


if __name__ == "__main__":
    sys.exit(main())
