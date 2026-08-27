"""
RADAR — que monedas se van a MOVER en las proximas 24h. No dice para donde.

Por que existe, y por que es tan chico. El screener de la raiz tiene 2.111 lineas y 233
parametros de configuracion, y se midio que:

  - entra +3,12 ATR TARDE (el precio ya subio 3 dias de movimiento normal antes de la
    alerta, y despues baja -0,94),
  - su score de 15 puntos no ordena mejor que una mascara al azar (0 de 735 brazos),
  - elegir la moneda aporta +0,012 ATR, o sea CERO: el 60% de la perdida es el MOMENTO.

Y despues se midieron 4.140 formas de predecir la DIRECCION —precio, flujo de ordenes y
posicionamiento de futuros, las dos direcciones, de 4h a 7 dias, tres niveles de
selectividad, cinco anios, cuatro regimenes— y sobrevivieron CERO.

Lo unico que sobrevivio fue predecir la MAGNITUD. Este archivo hace solo eso.

    py -3.13 -u radar.py                  # el top-8 a la salida estandar
    py -3.13 -u radar.py --k 15 --json    # mas nombres, formato json
    py -3.13 -u radar.py --telegram       # ademas lo manda (requiere las env vars)

Lo que NO hace, a proposito: no dice comprar, no dice vender, no puntua de 0 a 15, no
tiene buckets BEST/STRONG/WATCH, y no tiene un archivo de configuracion con 233
perillas. Todo eso se midio y no compraba nada.
"""
import argparse
import os
import sys
import time
from concurrent.futures import ThreadPoolExecutor

import numpy as np
import pandas as pd
import requests

SPOT = "https://api.binance.com"
FAPI = "https://fapi.binance.com"

# ---------------------------------------------------------------- lo medido
# `banco/PREREGISTRO_TRANSVERSAL.md`, corrida 3, panel de 46 pares 2021-10 -> 2026-07,
# 251 semanas, top-8 contra el universo de la MISMA barra:
#
#   n_surge      spread +1,008 ATR base | 97% de 251 semanas | p 0,0000
#   oi_rel_168   spread +1,108 ATR base | 97% de 201 semanas | p 0,0000
#   turnover     spread +0,953 ATR base | 96% de 251 semanas | p 0,0000
#
# En crudo (que es lo que se siente): las 8 elegidas por `n_surge` recorren 7,30% en 24h
# contra 6,33% del universo (1,15x), y la elegida supera la mediana de su barra el 61,3%
# de las veces contra 49,5% de linea base.
#
# ES MODESTO Y HAY QUE DECIRLO: "se mueve ~15% mas que la tipica", no "se mueve el doble".
CAL_MULTIPLO = 1.15
CAL_TASA = 0.613
CAL_TASA_BASE = 0.495
# Camino de 24h en unidades del ATR base, MEDIDO (mediana sobre las 251 semanas):
#   top-8 por n_surge  5.41      universo  4.83
# Se usa el medido y no una cuenta propia. La primera version de este archivo hacia
# `atr_base * 24` —multiplicar un ATR HORARIO por 24 horas— y daba disparates como
# "BICOUSDT se mueve 104% por dia". La volatilidad no escala lineal con el tiempo, y
# ademas `atr_base` ya es un promedio de rangos horarios, no un rango diario.
CAL_CAMINO_ATR = 5.41

# Se rankea por UNA feature, no por una combinacion. Medido en `banco/combo.py`: combinar
# oi+n_surge+turnover gana +0,0077 contra un MDE de +-0,078, o sea ruido. Tres features
# correlacionadas no son tres features.
#
# Y se elige `n_surge` sobre `oi_rel_168` —que mide 0,10 mejor, dentro del MDE— porque
# sale de los mismos klines: no necesita que el par tenga perpetuo (el ~20% no lo tiene)
# ni una request extra por simbolo. `oi_rel_168` se muestra al lado, informativo.
VENTANA = 168        # horas de la mediana movil de referencia
BARRAS = 1000        # klines por simbolo (41 dias; hacen falta 720 para atr_base)
ATR_BASE_H = 720     # mediana movil de 30d del ATR propio


def _get(url, params=None, intentos=3):
    for i in range(intentos):
        try:
            r = requests.get(url, params=params, timeout=20)
            if r.status_code == 200:
                return r.json()
            if r.status_code in (418, 429):
                time.sleep(2 ** i)
        except Exception:
            time.sleep(1 + i)
    return None


def universo(n, vol_min):
    """Top n pares USDT spot por volumen de 24h. Es el ranking de HOY: los deslistados
    no estan, y eso sesga hacia mejor (mismo sesgo que todo el banco)."""
    d = _get(f"{SPOT}/api/v3/ticker/24hr") or []
    filas = [(x["symbol"], float(x["quoteVolume"])) for x in d
             if x["symbol"].endswith("USDT")
             and not x["symbol"].endswith(("UPUSDT", "DOWNUSDT"))
             and float(x["quoteVolume"]) >= vol_min]
    filas.sort(key=lambda r: -r[1])
    return [s for s, _ in filas[:n]]


def features(sym):
    """n_surge, turnover y el ATR base de un par. Solo velas CERRADAS."""
    d = _get(f"{SPOT}/api/v3/klines",
             {"symbol": sym, "interval": "1h", "limit": BARRAS})
    if not d or len(d) < ATR_BASE_H + 2:
        return None
    # la ultima vela esta EN CURSO: se descarta. Usarla es mirar el futuro a medias y
    # ademas hace que el ranking cambie solo por el minuto en que corras el script.
    d = d[:-1]
    h = np.array([float(r[2]) for r in d])
    l = np.array([float(r[3]) for r in d])
    c = np.array([float(r[4]) for r in d])
    qv = np.array([float(r[7]) for r in d])
    nt = np.array([float(r[8]) for r in d])

    atr = pd.Series((h - l) / c).rolling(24).mean()
    atr_base = float(atr.rolling(ATR_BASE_H).median().iloc[-1])
    med_n = float(pd.Series(nt).rolling(VENTANA).median().iloc[-1])
    med_q = float(pd.Series(qv).rolling(VENTANA).median().iloc[-1])
    if not (atr_base > 0 and med_n > 0 and med_q > 0):
        return None
    return dict(sym=sym, n_surge=nt[-1] / med_n, turnover=qv[-1] / med_q,
                atr_base=atr_base, precio=c[-1], qv_24h=float(qv[-24:].sum()))


def oi_rel(sym):
    """OI actual contra su media de 168h. La API tiene un muro de ~20 dias, que alcanza.
    Devuelve None si el par no tiene perpetuo (el ~20% no lo tiene)."""
    for p in (sym, f"1000{sym}", f"1000000{sym}"):
        d = _get(f"{FAPI}/futures/data/openInterestHist",
                 {"symbol": p, "period": "1h", "limit": 200})
        if d and len(d) >= VENTANA:
            v = np.array([float(x["sumOpenInterestValue"]) for x in d])
            m = v[-VENTANA:].mean()
            if m > 0:
                return float(v[-1] / m - 1.0)
    return None


def escanear(n_pares, k, vol_min, min_atr=0.0, workers=12):
    syms = universo(n_pares, vol_min)
    print(f"universo: {len(syms)} pares USDT (vol 24h >= ${vol_min:,.0f})",
          file=sys.stderr, flush=True)
    with ThreadPoolExecutor(workers) as ex:
        filas = [f for f in ex.map(features, syms) if f]
    if len(filas) < 30:
        print(f"FATAL: solo {len(filas)} pares con datos suficientes; "
              f"sin seccion cruzada no hay ranking", file=sys.stderr)
        sys.exit(1)

    F = pd.DataFrame(filas)
    # PISO DE VOLATILIDAD — APAGADO POR DEFAULT, y NO ESTA MEDIDO.
    #
    # El ranking es RELATIVO: una moneda quieta que se activa sigue siendo quieta.
    # SUNUSDT entra al top-8 con 4,8x de actividad y recorre 1,1% en 24h. Si lo que
    # queres es movimiento ABSOLUTO, este filtro ayuda — pero el spread validado se
    # midio SIN el, asi que activarlo te saca de lo medido. Es una perilla, y las
    # perillas son exactamente lo que hundio al screener viejo: usala sabiendo eso.
    if min_atr > 0:
        antes = len(F)
        F = F[F["atr_base"] >= min_atr]
        print(f"piso de volatilidad {100*min_atr:.2f}%/h: {antes} -> {len(F)} pares "
              f"(NO MEDIDO)", file=sys.stderr)
        if len(F) < k:
            print("OJO: el piso deja menos pares que k", file=sys.stderr)

    # RANKING TRANSVERSAL: la posicion de cada moneda es contra las OTRAS de este mismo
    # momento, no contra un umbral fijo. Un umbral absoluto mezcla "que moneda es" con
    # "que hora es"; el rank dentro de la barra separa las dos cosas.
    F = F.sort_values("n_surge", ascending=False).reset_index(drop=True)
    F["rank"] = np.arange(1, len(F) + 1)
    F["en_top"] = F["rank"] <= k
    F["universo"] = len(F)

    # OI solo para el top-k: son k requests en vez de 200, y es informativo, no el eje.
    top = F[F.en_top]
    with ThreadPoolExecutor(6) as ex:
        vals = list(ex.map(oi_rel, top["sym"]))
    F["oi_rel_168"] = np.nan
    F.loc[top.index, "oi_rel_168"] = vals

    # el camino esperado no es una prediccion nueva: es el ATR base del propio par por
    # el multiplo MEDIDO del top-8. Si el par no se mueve, esto no lo hace moverse.
    F["camino_24h_est"] = F["atr_base"] * CAL_CAMINO_ATR
    return F


SUPABASE_URL = "https://ecgdswroygkfckkaguxp.supabase.co"
TABLA = "radar_runs"


def guardar(F):
    """Guarda el UNIVERSO ENTERO de esta corrida en Supabase.

    Se guardan las ~200 filas y no solo el top-k, y no es por prolijidad: el
    estadistico validado es `media(top-k) - media(UNIVERSO DE LA MISMA BARRA)`. Sin las
    filas del universo el forward test no se puede calcular, y guardarlas despues es
    imposible porque el universo de hoy no es el de manana.

    `precio` es la referencia de entrada: con eso y `run_at` se reconstruye el camino
    real despues, sin tener que guardar ningun resultado.
    """
    key = os.environ.get("SUPABASE_KEY")
    if not key:
        print("(sin SUPABASE_KEY: no se guardo)", file=sys.stderr)
        return
    ahora = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    filas = [{"run_at": ahora, "symbol": r.sym, "rank": int(r["rank"]),
              "en_top": bool(r.en_top), "universo": int(r.universo),
              "n_surge": round(float(r.n_surge), 4),
              "turnover": round(float(r.turnover), 4),
              "atr_base": round(float(r.atr_base), 6),
              "precio": float(r.precio),
              "oi_rel_168": (None if pd.isna(r.oi_rel_168)
                             else round(float(r.oi_rel_168), 4))}
             for _, r in F.iterrows()]
    h = {"apikey": key, "Authorization": f"Bearer {key}",
         "Content-Type": "application/json", "Prefer": "return=minimal"}
    ok = 0
    for i in range(0, len(filas), 500):
        try:
            r = requests.post(f"{SUPABASE_URL}/rest/v1/{TABLA}", headers=h,
                              json=filas[i:i + 500], timeout=20)
            r.raise_for_status()
            ok += len(filas[i:i + 500])
        except Exception as e:
            print(f"Supabase error: {e}", file=sys.stderr)
            if "r" in dir() and getattr(r, "text", ""):
                print(f"  {r.text[:300]}", file=sys.stderr)
            return
    print(f"supabase: {ok} filas guardadas en {TABLA} ({ahora})", file=sys.stderr)


def texto(F_top, k):
    F = F_top.reset_index(drop=True)
    out = [f"RADAR — {k} monedas con mas probabilidad de MOVERSE (24h)",
           f"{time.strftime('%Y-%m-%d %H:%M UTC', time.gmtime())}",
           "",
           "NO dice direccion. Solo magnitud. Medido: las elegidas recorren "
           f"{CAL_MULTIPLO:.2f}x lo que",
           f"la moneda tipica, y superan la mediana el {100*CAL_TASA:.0f}% de las veces "
           f"(base {100*CAL_TASA_BASE:.0f}%).",
           ""]
    out.append(f"{'#':>2} {'par':<14}{'actividad':>10}{'volumen':>9}"
               f"{'OI 7d':>8}{'ATR/h':>8}{'recorrido 24h':>14}")
    out.append("-" * 69)
    for i, r in F.iterrows():
        oi = "  n/d" if pd.isna(r.oi_rel_168) else f"{100*r.oi_rel_168:+5.0f}%"
        out.append(f"{i+1:>2} {r.sym:<14}{r.n_surge:>9.1f}x{r.turnover:>8.1f}x"
                   f"{oi:>8}{100*r.atr_base:>7.2f}%{100*r.camino_24h_est:>13.1f}%")
    out += ["",
            "actividad = operaciones de la ultima hora contra su mediana de 7 dias",
            "            (es el eje del ranking: lo unico validado)",
            "OI 7d     = open interest contra su media de 7 dias (informativo)",
            "ATR/h     = rango horario tipico del par",
            f"recorrido = distancia total esperada en 24h (maximo menos minimo),",
            f"            = ATR/h x {CAL_CAMINO_ATR} (mediana medida del top-8).",
            "            OJO: es camino RECORRIDO, no ganancia. Puede ser todo hacia abajo."]
    return "\n".join(out)


def main():
    ap = argparse.ArgumentParser(description="RADAR — magnitud, no direccion")
    ap.add_argument("--k", type=int, default=8, help="cuantas monedas listar")
    ap.add_argument("--pares", type=int, default=200)
    ap.add_argument("--vol-min", type=float, default=5e6,
                    help="volumen minimo 24h en USDT")
    ap.add_argument("--min-atr", type=float, default=0.0,
                    help="piso de ATR horario (ej 0.01 = 1%%/h). NO MEDIDO: el spread "
                         "validado no lo usa. Apagado por default.")
    ap.add_argument("--json", action="store_true")
    ap.add_argument("--supabase", action="store_true",
                    help="guardar el UNIVERSO ENTERO en radar_runs (requiere "
                         "SUPABASE_KEY). Es lo que habilita el forward test.")
    ap.add_argument("--telegram", action="store_true",
                    help="enviar ademas por Telegram (TELEGRAM_TOKEN y CHAT_ID)")
    a = ap.parse_args()

    F = escanear(a.pares, a.k, a.vol_min, a.min_atr)
    TOP = F[F.en_top]

    if a.supabase:
        guardar(F)

    if a.json:
        print(TOP.to_json(orient="records", indent=2))
        return
    t = texto(TOP, a.k)
    print(t)

    if a.telegram:
        tok, chat = os.environ.get("TELEGRAM_TOKEN"), os.environ.get("TELEGRAM_CHAT_ID")
        if not tok or not chat:
            print("\n(sin TELEGRAM_TOKEN/TELEGRAM_CHAT_ID: no se envio)", file=sys.stderr)
            return
        r = requests.post(f"https://api.telegram.org/bot{tok}/sendMessage",
                          json={"chat_id": chat, "text": f"```\n{t}\n```",
                                "parse_mode": "Markdown"}, timeout=20)
        print(f"\n(telegram: HTTP {r.status_code})", file=sys.stderr)


if __name__ == "__main__":
    main()
