"""
CORRIDA 10 — cuanto dura una dislocacion entre venues?

Preregistro con la regla de parada: `PREREGISTRO_DISLOCACION.md`, escrito ANTES de tomar
una sola muestra.

    py -3.13 -u dislocacion.py --recolectar --horas 24    # muestrea y escribe a disco
    py -3.13 -u dislocacion.py --analizar                 # lee lo juntado y decide

Regla de parada, textual:
    Si la mediana de duracion de las dislocaciones que superan el costo (filo ejecutable
    > 20 bps) es menor a 2 segundos, el negocio es de latencia y se CIERRA.
    Hacen falta >= 30 episodios para afirmar una mediana.

LA METRICA. El filo EJECUTABLE, no la diferencia de medios:

    filo(A->B) = (bid_B - ask_A) / mid * 10.000   [bps]

Un arbitraje no cruza contra el medio: compra al ask de un venue y vende al bid del otro.
La diferencia de medios cuenta la mitad de cada spread como ganancia cuando es costo. Se
reporta igual la metrica del handoff (|mid_A - mid_B|) para que el cambio sea auditable.

Una sola llamada por venue trae TODOS los simbolos: 3 requests por muestra sin importar
cuantos pares, y los pares de un mismo venue quedan sincronizados entre si.
"""
import argparse
import json
import os
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone

import numpy as np
import pandas as pd
import requests

HERE = os.path.dirname(os.path.abspath(__file__))
DATOS = os.path.join(HERE, ".dislocacion")

# --- todo esto esta fijado en el preregistro, antes de muestrear ---------------
# los 7 del preregistro: el ANALISIS PRIMARIO es sobre estos
PARES = ["BTCUSDT", "ETHUSDT", "DOGEUSDT", "LTCUSDT", "INJUSDT", "ALGOUSDT", "AGLDUSDT"]
# 23 mas, agregados en la enmienda del preregistro DESPUES del piloto y ANTES de la
# corrida larga. Una llamada por venue trae todos los simbolos, asi que sumarlos no
# cuesta un request mas: solo pueden hacer MAS FACIL encontrar el efecto, nunca cerrarlo.
PARES_EXTRA = ["SOLUSDT", "SUIUSDT", "ADAUSDT", "ONDOUSDT", "ROBOUSDT", "DOTUSDT",
               "SHIBUSDT", "MASKUSDT", "BONKUSDT", "PENDLEUSDT", "PEOPLEUSDT", "APEUSDT",
               "XTZUSDT", "GMXUSDT", "THETAUSDT", "SKYUSDT", "LINEAUSDT", "ENJUSDT",
               "GMTUSDT", "MANAUSDT", "NOTUSDT", "ZRXUSDT", "RPLUSDT"]
TODOS = PARES + PARES_EXTRA
UMBRALES = (0, 10, 20, 30)      # bps sobre el filo ejecutable
UMBRAL_BASE = 20                # dos taker al tramo base = 0,10% x 2
UMBRAL_MEDIOS = (40, 60)        # la metrica del handoff, como referencia
DUR_MIN = 2.0                   # segundos — la regla de parada
EPISODIOS_MIN = 30              # menos que esto y no se afirma una mediana
SIZE_MIN_USD = 1000             # nocional minimo en el tope, las DOS patas
SKEW_MAX_MS = 1000              # si las 3 respuestas abarcan mas, se tira la muestra
PERIODO = 0.5                   # segundos entre muestras

VENUES = ("binance", "okx", "bybit")


def _sesion():
    s = requests.Session()
    s.headers.update({"User-Agent": "Mozilla/5.0"})
    return s


SES = {v: _sesion() for v in VENUES}


def _binance(objetivo):
    r = SES["binance"].get("https://api.binance.com/api/v3/ticker/bookTicker", timeout=8)
    r.raise_for_status()
    out = {}
    for d in r.json():
        s = d["symbol"]
        if s in objetivo:
            out[s] = (float(d["bidPrice"]), float(d["bidQty"]),
                      float(d["askPrice"]), float(d["askQty"]))
    return out, None          # bookTicker NO trae timestamp de servidor


def _okx(objetivo):
    r = SES["okx"].get("https://www.okx.com/api/v5/market/tickers",
                       params={"instType": "SPOT"}, timeout=8)
    r.raise_for_status()
    j = r.json()
    out, ts = {}, None
    for d in j.get("data", []):
        s = d["instId"].replace("-", "")
        if s in objetivo and d.get("bidPx") and d.get("askPx"):
            out[s] = (float(d["bidPx"]), float(d["bidSz"]),
                      float(d["askPx"]), float(d["askSz"]))
            ts = int(d["ts"])
    return out, ts


def _bybit(objetivo):
    r = SES["bybit"].get("https://api.bybit.com/v5/market/tickers",
                         params={"category": "spot"}, timeout=8)
    r.raise_for_status()
    j = r.json()
    out = {}
    for d in j.get("result", {}).get("list", []):
        s = d["symbol"]
        if s in objetivo and d.get("bid1Price") and d.get("ask1Price"):
            out[s] = (float(d["bid1Price"]), float(d["bid1Size"]),
                      float(d["ask1Price"]), float(d["ask1Size"]))
    return out, int(j["time"]) if j.get("time") else None


FETCH = {"binance": _binance, "okx": _okx, "bybit": _bybit}


def una_muestra(objetivo, pool):
    """Los tres venues EN PARALELO. Devuelve libros + el instante local de cada respuesta."""
    def tirar(v):
        t0 = time.time()
        try:
            libro, ts = FETCH[v](objetivo)
        except Exception:
            return v, None, None, None
        return v, libro, ts, (t0 + time.time()) / 2      # instante medio de la llamada

    filas = list(pool.map(tirar, VENUES))
    libros = {v: lb for v, lb, _, _ in filas}
    tsrv = {v: ts for v, _, ts, _ in filas}
    tloc = {v: tl for v, _, _, tl in filas if tl is not None}
    return libros, tsrv, tloc


def recolectar(horas, periodo=PERIODO):
    os.makedirs(DATOS, exist_ok=True)
    obj = set(TODOS)
    ini = time.time()
    fin = ini + horas * 3600
    p = os.path.join(DATOS, f"muestras_{datetime.now(timezone.utc):%Y%m%d_%H%M}.csv")
    print(f"recolectando {horas}h a {1/periodo:.0f} Hz -> {os.path.basename(p)}")
    print(f"pares: {len(TODOS)} ({len(PARES)} primarios + {len(PARES_EXTRA)} extra)")

    n, tirado, buff = 0, 0, []
    with ThreadPoolExecutor(max_workers=3) as pool, open(p, "w", encoding="utf-8") as f:
        f.write("t,par,venue,bid,bidq,ask,askq,skew_ms\n")
        while time.time() < fin:
            t0 = time.time()
            libros, tsrv, tloc = una_muestra(obj, pool)
            if len(tloc) == 3:
                skew = (max(tloc.values()) - min(tloc.values())) * 1000
                if skew <= SKEW_MAX_MS:
                    t = min(tloc.values())
                    for v in VENUES:
                        lb = libros.get(v) or {}
                        for par, (b, bq, a, aq) in lb.items():
                            buff.append(f"{t:.3f},{par},{v},{b},{bq},{a},{aq},{skew:.0f}\n")
                    n += 1
                else:
                    tirado += 1
            else:
                tirado += 1
            if len(buff) > 2000:
                f.writelines(buff)
                f.flush()
                buff = []
            if n and n % 600 == 0:
                print(f"  {n:,} muestras   {(time.time()-ini)/3600:.2f}h   "
                      f"tiradas {tirado}", flush=True)
            dormir = periodo - (time.time() - t0)
            if dormir > 0:
                time.sleep(dormir)
        f.writelines(buff)
    print(f"listo: {n:,} muestras utiles, {tirado} tiradas -> {p}")
    return p


# =========================================================================
def cargar():
    if not os.path.isdir(DATOS):
        return pd.DataFrame()
    fs = [os.path.join(DATOS, x) for x in os.listdir(DATOS) if x.endswith(".csv")]
    if not fs:
        return pd.DataFrame()
    D = pd.concat([pd.read_csv(x) for x in fs], ignore_index=True)
    return D.drop_duplicates(["t", "par", "venue"])


def filos(D, size_min=SIZE_MIN_USD):
    """Filo ejecutable de cada par ordenado de venues, muestra por muestra.

    Solo cuentan las muestras donde los TRES venues cotizaron el par con los dos lados
    (si falta uno, el par pudo estar halted en ese venue y la muestra no es comparable).
    """
    W = D.pivot_table(index=["t", "par"], columns="venue",
                      values=["bid", "bidq", "ask", "askq"])
    W = W.dropna()
    filas = []
    for A in VENUES:
        for B in VENUES:
            if A == B:
                continue
            ask_A, bid_B = W[("ask", A)], W[("bid", B)]
            mid = (W[("bid", A)] + W[("ask", A)] + W[("bid", B)] + W[("ask", B)]) / 4
            filo = (bid_B - ask_A) / mid * 1e4
            # nocional en el tope de las dos patas
            nA = W[("askq", A)] * ask_A
            nB = W[("bidq", B)] * bid_B
            dif_medios = ((W[("bid", B)] + W[("ask", B)]) / 2
                          - (W[("bid", A)] + W[("ask", A)]) / 2).abs() / mid * 1e4
            filas.append(pd.DataFrame({
                "t": W.index.get_level_values("t"),
                "par": W.index.get_level_values("par"),
                "ruta": f"{A}->{B}", "filo": filo.values,
                "medios": dif_medios.values,
                "size_ok": ((nA >= size_min) & (nB >= size_min)).values,
            }))
    return pd.concat(filas, ignore_index=True)


def _periodo_real(F):
    """El intervalo EFECTIVO entre muestras, medido del dato.

    Fijarlo en PERIODO seria mentir: el RTT de los tres venues manda, y en esta maquina
    la frecuencia efectiva es ~1 Hz, no los 2 Hz pedidos. Un episodio de una sola muestra
    dura un intervalo REAL, no el nominal.
    """
    t = np.sort(F["t"].unique())
    if len(t) < 3:
        return PERIODO
    d = np.diff(t)
    return float(np.median(d[d > 0])) if (d > 0).any() else PERIODO


def episodios(F, col, umbral, periodo=None, exigir_size=True):
    """Rachas consecutivas por encima del umbral. Devuelve duraciones en segundos."""
    if periodo is None:
        periodo = _periodo_real(F)
    dur = []
    sel = F[F.size_ok] if exigir_size else F
    for (par, ruta), g in sel.groupby(["par", "ruta"], sort=False):
        g = g.sort_values("t")
        arriba = (g[col] > umbral).values
        if not arriba.any():
            continue
        t = g["t"].values
        i = 0
        while i < len(arriba):
            if not arriba[i]:
                i += 1
                continue
            j = i
            while j + 1 < len(arriba) and arriba[j + 1] and \
                    (t[j + 1] - t[j]) < periodo * 3:      # corta si hubo un hueco
                j += 1
            dur.append({"par": par, "ruta": ruta, "n": j - i + 1,
                        "seg": (t[j] - t[i]) + periodo})
            i = j + 1
    return pd.DataFrame(dur)


def analizar():
    D = cargar()
    if D.empty:
        print("no hay muestras todavia. Correr con --recolectar primero.")
        return 1
    D["fecha"] = pd.to_datetime(D.t, unit="s", utc=True)
    horas = (D.t.max() - D.t.min()) / 3600
    nm = D.t.nunique()
    print("=" * 92)
    print("CORRIDA 10 — DISLOCACION ENTRE VENUES")
    print("=" * 92)
    print(f"  muestras {nm:,}   ventana {horas:.2f} h   "
          f"{D.fecha.min():%Y-%m-%d %H:%M} -> {D.fecha.max():%H:%M} UTC")
    print(f"  frecuencia efectiva {nm/max(horas*3600,1):.2f} Hz   "
          f"skew mediano entre venues {D.skew_ms.median():.0f} ms "
          f"(p90 {D.skew_ms.quantile(.9):.0f})")
    print(f"  regla: mediana de duracion < {DUR_MIN}s con filo > {UMBRAL_BASE} bps -> CIERRA")

    F = filos(D)
    per = _periodo_real(F)
    print(f"\n  observaciones par-ruta: {len(F):,}   con tamano >= "
          f"${SIZE_MIN_USD:,} en las dos patas: {F.size_ok.mean():.1%}")
    print(f"  intervalo efectivo entre muestras: {per:.2f}s "
          f"(nominal {PERIODO}s; manda el RTT de los tres venues)")

    print(f"\n{'='*92}\nEL FILO EJECUTABLE — distribucion (bps)\n{'='*92}")
    print("  PRIMARIOS (los 7 del preregistro)")
    print(f"  {'par':<10}{'p50':>9}{'p90':>9}{'p99':>9}{'max':>9}"
          f"{'>0 bps':>9}{'>20 bps':>10}{'con size':>10}")
    for par in PARES:
        g = F[F.par == par]
        if g.empty:
            print(f"  {par:<10}{'sin datos':>58}")
            continue
        gs = g[g.size_ok]
        print(f"  {par:<10}{g.filo.quantile(.5):>9.1f}{g.filo.quantile(.9):>9.1f}"
              f"{g.filo.quantile(.99):>9.1f}{g.filo.max():>9.1f}"
              f"{(g.filo>0).mean():>8.2%}{(g.filo>UMBRAL_BASE).mean():>10.3%}"
              f"{g.size_ok.mean():>10.1%}")

    extra = [p for p in PARES_EXTRA if (F.par == p).any()]
    if extra:
        ge = F[F.par.isin(extra)]
        print(f"\n  EXTRA ({len(extra)} pares, enmienda post-piloto) — agregado:")
        print(f"  {'':<10}{ge.filo.quantile(.5):>9.1f}{ge.filo.quantile(.9):>9.1f}"
              f"{ge.filo.quantile(.99):>9.1f}{ge.filo.max():>9.1f}"
              f"{(ge.filo>0).mean():>8.2%}{(ge.filo>UMBRAL_BASE).mean():>10.3%}"
              f"{ge.size_ok.mean():>10.1%}")
        peor = ge.groupby("par").filo.max().sort_values(ascending=False)
        print("  los 5 pares con el filo maximo mas alto: " +
              ", ".join(f"{k} {v:.1f}bps" for k, v in peor.head(5).items()))

    print(f"\n{'='*92}\nDURACION DE LOS EPISODIOS\n{'='*92}")
    print(f"  {'umbral':>8}{'episodios':>11}{'mediana s':>11}{'p90 s':>9}"
          f"{'1 sola muestra':>16}{'mediana sin esas':>18}")
    res = []
    for u in UMBRALES:
        E = episodios(F, "filo", u, periodo=per)
        if E.empty:
            print(f"  {u:>6}bps{0:>11}{'-':>11}{'-':>9}{'-':>16}{'-':>18}")
            res.append({"umbral": u, "n": 0, "med": np.nan})
            continue
        multi = E[E.n > 1]
        m = E.seg.median()
        print(f"  {u:>6}bps{len(E):>11}{m:>11.2f}{E.seg.quantile(.9):>9.2f}"
              f"{(E.n==1).mean():>15.1%}"
              f"{(multi.seg.median() if len(multi) else float('nan')):>18.2f}")
        res.append({"umbral": u, "n": len(E), "med": m,
                    "med_multi": multi.seg.median() if len(multi) else np.nan})
    R = pd.DataFrame(res)

    print(f"\n{'='*92}\nLA METRICA DEL HANDOFF (|mid_A - mid_B|), como referencia\n{'='*92}")
    print(f"  {'umbral':>8}{'episodios':>11}{'mediana s':>11}{'p90 s':>9}")
    for u in UMBRAL_MEDIOS:
        E = episodios(F, "medios", u, periodo=per)
        if E.empty:
            print(f"  {u:>6}bps{0:>11}{'-':>11}{'-':>9}")
        else:
            print(f"  {u:>6}bps{len(E):>11}{E.seg.median():>11.2f}"
                  f"{E.seg.quantile(.9):>9.2f}")
    print("  (mide de mas: cuenta la mitad de cada spread como ganancia cuando es costo)")

    print(f"\n{'='*92}\nPATRON POR LIQUIDEZ — la direccion declarada antes de medir\n{'='*92}")
    print("  la hipotesis predice MAS episodios y MAS largos cuanto mas fino el par")
    print(f"  {'par':<10}{'episodios >20bps':>18}{'mediana s':>11}")
    for par in PARES:
        E = episodios(F[F.par == par], "filo", UMBRAL_BASE, periodo=per)
        print(f"  {par:<10}{len(E):>18}"
              f"{(E.seg.median() if len(E) else float('nan')):>11.2f}")
    if extra:
        Ee = episodios(F[F.par.isin(extra)], "filo", UMBRAL_BASE, periodo=per)
        print(f"  {'(los 23 extra)':<10}{len(Ee):>18}"
              f"{(Ee.seg.median() if len(Ee) else float('nan')):>11.2f}")

    print(f"\n{'='*92}\nVEREDICTO\n{'='*92}")
    fila = R[R.umbral == UMBRAL_BASE].iloc[0]
    if horas < 24:
        print(f"  PILOTO ({horas:.2f} h). El preregistro exige >= 24 h corridas.")
        print("  Esto valida la caneria y adelanta la forma del resultado. NO es el veredicto.")
    if fila.n < EPISODIOS_MIN:
        print(f"  episodios con filo > {UMBRAL_BASE} bps: {int(fila.n)} "
              f"(hacen falta {EPISODIOS_MIN} para afirmar una mediana)")
        print(f"  Sobre {nm:,} muestras en {horas:.2f} h, la oportunidad que supera el costo")
        print("  practicamente NO APARECE. Eso no es 'no se pudo medir': es la respuesta,")
        print("  y es un cierre mas fuerte que el de duracion.")
    elif fila.med < DUR_MIN:
        print(f"  mediana {fila.med:.2f}s < {DUR_MIN}s  ->  CERRADA.")
        print("  El negocio es de latencia. No hay version lenta.")
    else:
        print(f"  mediana {fila.med:.2f}s >= {DUR_MIN}s con {int(fila.n)} episodios  ->  ABIERTA.")
        print("  Verificar el patron por liquidez antes de festejar.")
    print("=" * 92)
    return 0


def main():
    ap = argparse.ArgumentParser(description="Banco — corrida 10: dislocacion entre venues")
    ap.add_argument("--recolectar", action="store_true")
    ap.add_argument("--analizar", action="store_true")
    ap.add_argument("--horas", type=float, default=24.0)
    ap.add_argument("--periodo", type=float, default=PERIODO)
    a = ap.parse_args()
    if a.recolectar:
        recolectar(a.horas, a.periodo)
    if a.analizar or not a.recolectar:
        return analizar()
    return 0


if __name__ == "__main__":
    sys.exit(main())
