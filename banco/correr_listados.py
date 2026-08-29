"""
CORRIDA 9 — eventos de listado en Binance.

Preregistro con la regla de parada: `PREREGISTRO_LISTADOS.md`, escrito ANTES de contar
un solo evento.

    py -3.13 -u correr_listados.py --nula     # SOLO supervivencia, n post-join y MDE

El `--nula` primero no es opcional: la regla del handoff es contar el n post-join y
calcular el MDE ANTES de estimar nada. Es lo que convirtio unlocks y la cola iliquida en
"no esta" en vez de "no se pudo", y lo que cerro la corrida 8 en una tarde.

LA FUENTE. El handoff asumia que habia que scrapear el blog de anuncios. No hace falta:
**la primera vela de un par ES el momento del listado**, exacta y sin depender de que un
HTML no cambie.

LA TRAMPA, y es la que puede cerrar esto solo. `exchangeInfo` sin filtrar parece devolver
solo lo que cotiza hoy, y si la muestra fueran solo los listados que sobrevivieron, el
drift post-listado saldria positivo POR CONSTRUCCION. No es asi: exchangeInfo devuelve los
deslistados con `status == "BREAK"`. Contarlos como vivos —que es lo que pasa si uno filtra
por nombre y no por estado— es exactamente el error que fabrica el falso positivo.
"""
import argparse
import os
import re
import sys
import time
from concurrent.futures import ThreadPoolExecutor

import numpy as np
import pandas as pd
import requests

HERE = os.path.dirname(os.path.abspath(__file__))
CACHE = os.path.join(HERE, ".listados_cache")
SPOT = "https://api.binance.com"

HORIZONTES = (1, 3, 7, 30)        # dias, preregistrados; no se agrega un quinto
COSTOS = (0.20, 0.50)             # %
MDE_MAX = 0.10                    # ATR — el umbral preregistrado
FRAC_MUERTOS_MIN = 0.05           # si menos que esto son deslistados, la fuente sesga
Z = 2.8                           # 1,96 + 0,84
ATR_N = 14                        # ventana del ATR de mercado
MIN_VIVOS = 30                    # simbolos vivos minimos para que una barra sea control

# clase de activo, no solo volumen (regla de metodo de la corrida 5)
FUERA_EXACTOS = {
    "USDCUSDT", "TUSDUSDT", "BUSDUSDT", "FDUSDUSDT", "USDPUSDT", "DAIUSDT", "PAXUSDT",
    "USDSUSDT", "USDSBUSDT", "SUSDUSDT", "USTUSDT", "EURUSDT", "GBPUSDT", "AUDUSDT",
    "EURIUSDT", "USD1USDT", "RLUSDUSDT", "XUSDUSDT", "BFUSDUSDT", "USDEUSDT",
    "PAXGUSDT", "XAUTUSDT", "WBTCUSDT", "WBETHUSDT", "BETHUSDT", "STETHUSDT",
}
# acciones tokenizadas y tokens apalancados
FUERA_PATRON = re.compile(r"(?:UP|DOWN|BULL|BEAR)USDT$|^[A-Z]{2,5}BUSDT$")

S = requests.Session()
S.headers.update({"User-Agent": "Mozilla/5.0"})


def _get(url, params=None, tries=5):
    for i in range(tries):
        try:
            r = S.get(url, params=params, timeout=30)
            if r.status_code == 200:
                return r.json()
            if r.status_code in (418, 429):
                time.sleep(2 ** i)
                continue
            time.sleep(0.4 * (i + 1))
        except Exception:
            time.sleep(0.4 * (i + 1))
    return None


def simbolos():
    """Todos los pares USDT que EXISTIERON, con su estado.

    GOTCHA que decide la corrida: filtrar exchangeInfo por nombre da 734 pares y los
    da todos por vivos. El estado dice otra cosa: 485 TRADING y 249 BREAK. Los BREAK
    son los deslistados, y son justo los que no pueden faltar.
    """
    ei = _get(f"{SPOT}/api/v3/exchangeInfo")
    if not ei:
        return pd.DataFrame()
    filas = [{"sym": d["symbol"], "base": d["baseAsset"], "estado": d["status"]}
             for d in ei["symbols"] if d["symbol"].endswith("USDT")]
    d = pd.DataFrame(filas)
    d["fuera"] = d.sym.isin(FUERA_EXACTOS) | d.sym.str.contains(FUERA_PATRON)
    return d


def velas(sym):
    """Historia diaria completa del par, desde su primera vela. Cacheada."""
    os.makedirs(CACHE, exist_ok=True)
    p = os.path.join(CACHE, f"{sym}_1d.csv")
    if os.path.exists(p):
        try:
            return pd.read_csv(p, parse_dates=["fecha"]).set_index("fecha")
        except Exception:
            pass
    filas, cur = [], 0
    while True:
        d = _get(f"{SPOT}/api/v3/klines",
                 {"symbol": sym, "interval": "1d", "startTime": cur, "limit": 1000})
        if not d:
            break
        filas.extend(d)
        nuevo = int(d[-1][0])
        if len(d) < 1000 or nuevo <= cur:
            break
        cur = nuevo + 1
    if not filas:
        return pd.DataFrame()
    x = pd.DataFrame([{"t": int(k[0]), "o": float(k[1]), "h": float(k[2]),
                       "l": float(k[3]), "c": float(k[4]), "q": float(k[7])}
                      for k in filas]).drop_duplicates("t")
    x["fecha"] = pd.to_datetime(x["t"], unit="ms", utc=True).dt.tz_localize(None).dt.normalize()
    x = x.set_index("fecha").drop(columns="t").sort_index()
    x.reset_index().to_csv(p, index=False)
    return x


def bajar(syms, workers=12):
    out = {}
    with ThreadPoolExecutor(max_workers=workers) as ex:
        for sym, v in zip(syms, ex.map(velas, syms)):
            if len(v):
                out[sym] = v
    return out


def preparar(workers=12):
    D = simbolos()
    if D.empty:
        return None, None
    print(f"pares USDT que existieron: {len(D)}   "
          f"TRADING {(D.estado=='TRADING').sum()}   BREAK {(D.estado=='BREAK').sum()}")
    print(f"excluidos por CLASE DE ACTIVO (stables/FX/oro/apalancados/acciones): "
          f"{int(D.fuera.sum())}")
    D = D[~D.fuera].reset_index(drop=True)

    print(f"bajando velas diarias de {len(D)} pares...", flush=True)
    V = bajar(list(D.sym), workers)
    print(f"  con serie: {len(V)}")

    D = D[D.sym.isin(V)].copy()
    D["listado"] = [V[s].index[0] for s in D.sym]
    D["ultima"] = [V[s].index[-1] for s in D.sym]
    D["barras"] = [len(V[s]) for s in D.sym]
    return D, V


def _atr_mercado(V, fechas):
    """ATR de MERCADO en % del precio: mediana, sobre los simbolos vivos en cada fecha,
    del rango verdadero medio de ATR_N dias.

    Por que el del mercado y no el del simbolo: un par recien listado NO TIENE historia
    para calcular su propio ATR, y usar la posterior seria mirar el futuro. El del
    mercado esta disponible en el instante del evento y es comparable en el tiempo.
    """
    ser = []
    for s, v in V.items():
        tr = pd.concat([(v.h - v.l),
                        (v.h - v.c.shift()).abs(),
                        (v.l - v.c.shift()).abs()], axis=1).max(axis=1)
        ser.append((tr.rolling(ATR_N).mean() / v.c * 100).rename(s))
    A = pd.concat(ser, axis=1).reindex(fechas)
    return A.median(axis=1, skipna=True), A.notna().sum(axis=1)


def n_post_join(D, V):
    """El conteo que el handoff exige ANTES de estimar nada."""
    fechas = pd.date_range(min(D.listado), max(D.ultima), freq="D")
    atr, vivos = _atr_mercado(V, fechas)

    E = D.copy()
    E["semana"] = E.listado.dt.to_period("W").astype(str)
    E["vivos"] = vivos.reindex(E.listado).values
    E["atr_mkt"] = atr.reindex(E.listado).values
    # un evento solo sirve si hay universo de control y si le siguen datos
    E["util"] = (E.vivos >= MIN_VIVOS) & E.atr_mkt.notna() & \
                (E.barras > max(HORIZONTES) + 1)

    print("\n" + "=" * 78)
    print("SUPERVIVENCIA — la condicion que puede cerrar esto sola")
    print("=" * 78)
    m = (E.estado == "BREAK").mean()
    print(f"  eventos totales                {len(E)}")
    print(f"  de simbolos hoy DESLISTADOS    {(E.estado=='BREAK').sum()}  ({m:.1%})")
    print(f"  umbral preregistrado           {FRAC_MUERTOS_MIN:.0%}")
    print(f"  -> {'PASA' if m >= FRAC_MUERTOS_MIN else 'FALLA: la fuente sesga, se cierra'}")

    U = E[E.util]
    print("\n" + "=" * 78)
    print("n POST-JOIN — se cuenta ANTES de estimar (regla del handoff)")
    print("=" * 78)
    print(f"  eventos                        {len(E)}")
    print(f"  eventos UTILES                 {len(U)}   "
          f"(universo >= {MIN_VIVOS} vivos y >= {max(HORIZONTES)+1} barras propias)")
    print(f"  ventana                        {U.listado.min():%Y-%m} -> "
          f"{U.listado.max():%Y-%m}")
    print(f"  SEMANAS con listado (el n independiente) {U.semana.nunique()}")
    porsem = U.groupby("semana").size()
    print(f"  listados por semana: mediana {porsem.median():.0f} | "
          f"max {porsem.max()} | semanas con 1 solo {int((porsem==1).sum())}")
    print("\n  comparador — unlocks murio con 1.040 eventos y MDE 6,6 pp/decada;")
    print("  la corrida 3 (derivados) tenia 46 pares y 251 semanas, MDE +-0,062 ATR;")
    print("  la corrida 6 (on-chain) concluyo con 41 activos, 257 semanas, +-0,065 ATR.")
    return U, atr, vivos


def mde(U, V, atr, vivos):
    """MDE con la dispersion REAL, no una supuesta. Unidad: ATR de mercado.

    exceso(evento, h) = retorno del simbolo - MEDIANA del universo vivo, misma ventana,
    dividido por el ATR de mercado. El control es POR BARRA: los listados se agrupan en
    el tiempo (Binance lista mas en mercado alcista) y aparear por simbolo dejaria el
    termino de mercado adentro.
    """
    cierres = pd.concat([v.c.rename(s) for s, v in V.items()], axis=1).sort_index()
    print("\n" + "=" * 78)
    print("MDE DEL AZAR — antes de estimar el efecto (regla del handoff)")
    print("=" * 78)
    print(f"  unidad: ATR de mercado ({ATR_N}d, mediana del universo vivo)")
    print(f"  n independiente: SEMANAS con al menos un listado")
    print(f"  MDE = {Z} x sigma_semanal / sqrt(n_semanas)     umbral {MDE_MAX} ATR\n")
    print(f"  {'h':>4}{'eventos':>9}{'semanas':>9}{'sigma sem':>11}{'MDE (ATR)':>12}"
          f"{'MDE (%)':>10}{'veredicto':>20}")

    res = []
    for h in HORIZONTES:
        filas = []
        for _, e in U.iterrows():
            t0 = e.listado
            t1 = t0 + pd.Timedelta(days=h)
            if t0 not in cierres.index or t1 not in cierres.index:
                continue
            p0, p1 = cierres.loc[t0], cierres.loc[t1]
            # universo de control: vivos en las DOS puntas, sin el propio evento
            ok = p0.notna() & p1.notna()
            ok[e.sym] = False
            if ok.sum() < MIN_VIVOS:
                continue
            r_mkt = np.median((p1[ok] / p0[ok] - 1) * 100)
            if pd.isna(cierres.loc[t0, e.sym]) or pd.isna(cierres.loc[t1, e.sym]):
                continue
            r_sym = (cierres.loc[t1, e.sym] / cierres.loc[t0, e.sym] - 1) * 100
            a = e.atr_mkt
            if not a or not np.isfinite(a) or a <= 0:
                continue
            filas.append({"semana": e.semana, "pct": r_sym - r_mkt,
                          "exc": (r_sym - r_mkt) / a, "atr": a})
        if not filas:
            print(f"  {h:>4}{0:>9}{0:>9}{'-':>11}{'inf':>12}{'-':>10}{'sin datos':>20}")
            continue
        F = pd.DataFrame(filas)
        sem = F.groupby("semana")["exc"].mean()
        n = len(sem)
        sig = sem.std()
        m = Z * sig / np.sqrt(n) if n > 1 else np.inf
        ver = "medible" if m <= MDE_MAX else "NO SE PUDO MEDIR"
        semp = F.groupby("semana")["pct"].mean()
        mp = Z * semp.std() / np.sqrt(len(semp))
        print(f"  {h:>4}{len(F):>9}{n:>9}{sig:>10.3f}{m:>12.3f}{mp:>9.1f}%{ver:>20}")
        res.append({"h": h, "n_ev": len(F), "n_sem": n, "sigma": sig, "mde": m,
                    "mde_pct": mp, "sig_pct": semp.std(), "atr_med": F.atr.median(),
                    "sig_ev": F.pct.std()})
    return pd.DataFrame(res)


def main():
    ap = argparse.ArgumentParser(description="Banco — corrida 9: listados")
    ap.add_argument("--workers", type=int, default=12)
    ap.add_argument("--nula", action="store_true",
                    help="SOLO supervivencia, n post-join y MDE, y sale")
    a = ap.parse_args()

    t0 = time.time()
    D, V = preparar(workers=a.workers)
    if D is None:
        print("FATAL: no se pudo preparar el panel")
        sys.exit(1)
    U, atr, vivos = n_post_join(D, V)
    R = mde(U, V, atr, vivos)

    print("\n" + "=" * 78)
    if len(R):
        print(f"  ATR de mercado mediano en los eventos: {R.atr_med.median():.2f}%/dia")
        print("  La columna MDE(%) es la MISMA barra sin normalizar: sirve para juzgar si")
        print("  el umbral en ATR es justo, y para compararla contra el costo de una")
        print(f"  vuelta ({COSTOS[0]:.2f}% y {COSTOS[1]:.2f}%).")
    if len(R):
        print()
        print("  CUANTA HISTORIA HARIA FALTA. Un efecto vale la pena si supera 2x el costo")
        print(f"  de una vuelta, o sea ~{2*COSTOS[1]:.1f}% de exceso. n = (2,8 x sigma_sem / objetivo)^2")
        print(f"  {'h':>4}{'sigma/evento':>14}{'sigma/semana':>14}{'semanas nec.':>14}"
              f"{'ANOS':>9}{'hay':>7}")
        for _, r in R.iterrows():
            nec = (Z * r.sig_pct / (2 * COSTOS[1])) ** 2
            print(f"  {int(r.h):>4}{r.sig_ev:>13.1f}%{r.sig_pct:>13.1f}%{nec:>14,.0f}"
                  f"{nec/52:>9,.0f}{int(r.n_sem):>7}")
    print("VEREDICTO DE LA COMPUERTA DE POTENCIA")
    print("=" * 78)
    ok = R[R.mde <= MDE_MAX] if len(R) else R
    if len(ok):
        print(f"  horizontes medibles: {list(ok.h)}  -> se puede correr el lote.")
    else:
        print("  NINGUN horizonte llega al MDE preregistrado.")
        print("  Veredicto: NO SE PUDO MEDIR. Se cierra sin estimar el efecto.")
    print(f"\n({time.time()-t0:.0f}s)")

    if a.nula:
        print("\nCon esto se decide si vale la pena correr el lote. Recien despues,")
        print("y con el preregistro escrito, correr sin --nula.")
        return
    print("\n(el lote todavia no esta implementado: depende de esta compuerta)")


if __name__ == "__main__":
    sys.exit(main())
