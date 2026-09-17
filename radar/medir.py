"""MEDIR — el forward test del radar, contra datos que nadie miro al construirlo.

Todo lo que promete `radar.py` se midio sobre historia (2021-10 -> 2026-07). Esto lo
mide sobre lo que el radar dijo EN VIVO: lee `radar_runs` de Supabase, reconstruye lo que
efectivamente paso con velas posteriores, y compara contra los numeros preregistrados.

    py -3.13 -u medir.py                # todo lo que haya
    py -3.13 -u medir.py --desde 2026-09-01

Lo que se compara, y esta fijado ANTES de que existan datos:

    spread     +0,511 ATR base   (el numero medido sobre 251 semanas, a 4h)
    multiplo    1,21x            (camino del top-8 / camino del universo)
    tasa        62,6%            (veces que la elegida supera la mediana de su barra)
    linea base  49,5%

REGLA DE PARADA, escrita ahora:

  - No hay un numero fijo de semanas. Se compara el efecto observado contra el MDE
    ACTUAL —lo mas chico que se puede distinguir de cero con los datos que hay— y el
    script lo calcula en cada corrida. Un "no se pudo medir" NO es "no esta".
  - Si el observado supera el MDE actual y es positivo -> replico.
  - Si el observado es NEGATIVO y su magnitud supera el MDE -> no replico, se apaga.
  - Si cae dentro del MDE -> todavia no alcanza. Seguir juntando.
  - No se toca `n_surge` ni `k` por lo que salga aca. Ajustar el screener con el
    resultado del forward test convierte el out-of-sample en in-sample y no queda
    ninguna ventana limpia.

POR QUE NO SON 8 SEMANAS FIJAS. El `SEM_MIN = 8` original salio de copiar el umbral de
`banco/lote.py`, que existia por otra razon: alla las entradas SE SOLAPAN (una cada 12h
con horizonte de 30d, ~60 trades vivos a la vez) y el n efectivo es una fraccion del
contado. Aca las barras no se solapan por diseno (paso = horizonte), asi que el argumento
no se traslada. `banco/cuanto_esperar.py` lo calculo con la autocorrelacion real del
spread (0,449 a un lag, factor de inflacion 4,24):

    si el efecto real es    dias     semanas
    lo medido (x1,0)          12         1,7
    la mitad (x0,5)           48         6,8
    un tercio (x0,33)        107        15,3
    un cuarto (x0,25)        191        27,3

Refutar es mucho mas rapido que confirmar: si el efecto es tan grande como se midio, se
ve en dos semanas. Por eso conviene mirar temprano y seguido, no esperar sentado.
"""
import argparse
import os
import sys
import time
from concurrent.futures import ThreadPoolExecutor

import numpy as np
import pandas as pd
import requests

SUPABASE_URL = "https://ecgdswroygkfckkaguxp.supabase.co"
SPOT = "https://api.binance.com"
TABLA = "radar_runs"
H = 4                       # horizonte, en horas — el mismo que se valido
                            # (`banco/horizonte_util.py`: 4h gana en multiplo,
                            #  tasa y t contra 8/24/72/168h)

# EL UNIVERSO SOBRE EL QUE SE PREREGISTRO EL +0,511: los 46 pares con perpetuo desde 2021
# que `banco/` congelo como pin `deriv46`. Va escrito ACA y no leido de
# `banco/.kline_cache/`, que no esta en git: el numero contra el que se compara y el
# universo sobre el que se midio tienen que viajar juntos o la comparacion miente.
#
# Importa porque el radar en produccion NO mira este universo: mira el ranking de volumen
# de hoy, ~70 pares que rotan (137 simbolos distintos en los primeros 8 dias, y solo 35
# presentes en las 24 barras). El 79% de los elegidos en vivo cae FUERA de estos 46, en la
# cola nueva e iliquida donde el README dice explicitamente que la calibracion NO se puede
# extrapolar y donde los costos reales son 1,5x a 6,3x lo que asume el banco.
#
# Medido sin restringir, el spread en vivo da +2,25 —el 441% de lo preregistrado— y eso no
# es una replica espectacular, es una comparacion invalida: n_surge llega a 153x en un par
# recien listado, algo que 46 perpetuos establecidos no pueden producir por construccion.
# Restringido a los 46 y re-rankeando el top-8 adentro, da +0,83. Ese es el numero que le
# corresponde al +0,511.
UNIVERSO_PRE = {"AAVEUSDT", "ACEUSDT", "ADAUSDT", "APTUSDT", "ARBUSDT", "AVAXUSDT",
                "BCHUSDT", "BICOUSDT", "BNBUSDT", "BONKUSDT", "BTCUSDT", "CAKEUSDT",
                "CRVUSDT", "DASHUSDT", "DOGEUSDT", "DOTUSDT", "ETHUSDT", "FETUSDT",
                "FILUSDT", "GALAUSDT", "HBARUSDT", "ICPUSDT", "INJUSDT", "LDOUSDT",
                "LINKUSDT", "LTCUSDT", "NEARUSDT", "ONDOUSDT", "ONGUSDT", "ONTUSDT",
                "OPUSDT", "ORDIUSDT", "PENDLEUSDT", "PEOPLEUSDT", "PEPEUSDT",
                "PYTHUSDT", "SHIBUSDT", "SOLUSDT", "SUIUSDT", "TRXUSDT", "UNIUSDT",
                "WIFUSDT", "WLDUSDT", "XLMUSDT", "XRPUSDT", "ZECUSDT"}

PRE_SPREAD, PRE_MULT, PRE_TASA, PRE_BASE = 0.511, 1.21, 0.626, 0.495
K = 8              # el top-k sobre el que se preregistro. Si cambias `--k` en el
                   # cron, el +0,511 deja de ser la linea base y hay que
                   # preregistrar de nuevo: no alcanza con tocar esto.
Z = 2.80          # (1,96 + 0,84): 80% de potencia, alfa 0,05 a dos colas
N_MIN_BARRAS = 20  # abajo de esto el sd propio no es estimable, ni se intenta
FACTOR_HIST = 4.24  # 1+2*sum(rho) medido sobre las 251 semanas de historia
                    # (`banco/cuanto_esperar.py`). Es el piso mientras el dato propio
                    # sea demasiado corto para estimarlo por su cuenta.


def factor_inflacion(sp):
    """Inflacion de varianza por autocorrelacion: n_efectivo = n / factor.

    Las barras no se solapan, pero el spread SI se parece a si mismo de una barra a la
    siguiente (+0,449 en historia), asi que contar n barras como n datos independientes
    exagera la precision. Se estima con el dato propio sobre los lags que el largo
    acumulado banca —hace falta n >= 4*lag para que rho signifique algo— y se toma el
    MAYOR entre eso y el factor historico.

    El max() es a proposito y es asimetrico: subestimar el factor achica el MDE y hace
    declarar replica antes de tiempo. Con 251 semanas atras diciendo 4,24, creerle a un
    rho estimado sobre 25 barras seria el mismo error de heredar un numero sin mirarlo,
    nada mas que al reves.
    """
    rhos = []
    for lag in (1, 2, 3, 6, 12):
        if len(sp) < 4 * lag:
            break
        r = sp.autocorr(lag)
        rhos.append(max(float(r), 0.0) if pd.notna(r) else 0.0)
    return max(1 + 2 * sum(rhos), FACTOR_HIST)


def bajar(desde):
    key = os.environ.get("SUPABASE_KEY")
    if not key:
        print("FATAL: falta SUPABASE_KEY", file=sys.stderr); sys.exit(1)
    h = {"apikey": key, "Authorization": f"Bearer {key}"}
    filas, off = [], 0
    while True:
        r = requests.get(f"{SUPABASE_URL}/rest/v1/{TABLA}", headers=h, timeout=30,
                         params={"select": "*", "run_at": f"gte.{desde}",
                                 "order": "run_at.asc", "limit": 1000,
                                 "offset": off})
        r.raise_for_status()
        d = r.json()
        filas += d
        if len(d) < 1000:
            break
        off += 1000
    return pd.DataFrame(filas)


def camino(args):
    """Camino real (maximo - minimo) en las H horas POSTERIORES a la corrida."""
    sym, t_ms, precio = args
    d = None
    for _ in range(3):
        try:
            r = requests.get(f"{SPOT}/api/v3/klines", timeout=20,
                             params={"symbol": sym, "interval": "1h",
                                     "startTime": t_ms, "limit": H + 2})
            if r.status_code == 200:
                d = r.json()
                break
        except Exception:
            time.sleep(1)
    if not d or len(d) < H:
        return np.nan
    hi = max(float(x[2]) for x in d[:H])
    lo = min(float(x[3]) for x in d[:H])
    return (hi - lo) / precio


def main():
    ap = argparse.ArgumentParser(description="Forward test del radar")
    ap.add_argument("--desde", default="2020-01-01")
    ap.add_argument("--out", default=None)
    a = ap.parse_args()

    D = bajar(a.desde)
    if D.empty:
        print("todavia no hay corridas guardadas."); return
    D["run_at"] = pd.to_datetime(D["run_at"], utc=True, format="mixed")
    D["t_ms"] = D["run_at"].astype("int64") // 10**6

    # DE-SOLAPAR. Dos corridas separadas por menos de H horas comparten futuro, asi que
    # contarlas como dos observaciones infla el n aparente — el defecto que este repo
    # arrastra en todos lados por contar entradas solapadas como independientes. Pasa por
    # tres vias: corridas de prueba, el cron disparado dos veces, y corridas manuales
    # mezcladas con las automaticas. Se camina hacia adelante quedandose con la primera
    # de cada grupo. No se borra nada de la tabla: se filtra al medir.
    ts = sorted(pd.Series(D["run_at"].unique()))
    quedan, ult = [], None
    for t in ts:
        if ult is None or (t - ult) >= pd.Timedelta(hours=H):
            quedan.append(t)
            ult = t
    if len(quedan) < len(ts):
        print(f"de-solape: {len(ts)} corridas -> {len(quedan)} "
              f"({len(ts) - len(quedan)} descartadas por estar a menos de {H}h "
              f"de la anterior)")
    D = D[D["run_at"].isin(quedan)]

    # solo corridas con horizonte COMPLETO: truncar sesgaria hacia lo que ya se movio
    corte = pd.Timestamp.utcnow() - pd.Timedelta(hours=H + 1)
    D = D[D["run_at"] <= corte]
    if D.empty:
        print(f"hay corridas, pero ninguna cumplio todavia las {H}h de horizonte.")
        return

    print(f"corridas: {D.run_at.nunique()} | filas: {len(D):,} | "
          f"{D.run_at.min():%Y-%m-%d} -> {D.run_at.max():%Y-%m-%d}", flush=True)

    with ThreadPoolExecutor(12) as ex:
        D["camino"] = list(ex.map(camino, zip(D.symbol, D.t_ms, D.precio)))
    D = D[D["camino"].notna() & (D["atr_base"] > 0)]
    D["y"] = D["camino"] / D["atr_base"]

    D["semana"] = D["run_at"].dt.strftime("%G-W%V")

    def estadisticos(d, col):
        """spread por barra, multiplo y tasa — el estadistico validado, top-k menos el
        universo DE LA MISMA CORRIDA."""
        g = d.groupby("run_at")
        sp = g.apply(lambda x: x.loc[x[col], "y"].mean() - x["y"].mean(),
                     include_groups=False).dropna()
        top = d[d[col]]
        med = g["camino"].median()
        j = top.join(med.rename("m"), on="run_at")
        jb = d.join(med.rename("m"), on="run_at")
        sem = sp.groupby(g["semana"].first().reindex(sp.index)).mean()
        return dict(sp=sp, sem=sem,
                    mult=top["camino"].median() / d["camino"].median(),
                    tasa=float((j["camino"] > j["m"]).mean()),
                    base=float((jb["camino"] > jb["m"]).mean()),
                    pares=g.size().mean())

    # LO QUE SE COMPARA CONTRA EL PREREGISTRO: los 46 de `deriv46`, con el top-8
    # RE-RANKEADO adentro. Restringir no es elegir el resultado que gusta —de hecho baja
    # el spread de +2,25 a +0,83—: es la unica forma de que la comparacion sea la misma
    # pregunta. El +0,511 nunca fue una prediccion sobre la cola iliquida.
    P = D[D["symbol"].isin(UNIVERSO_PRE)].copy()
    if P.empty:
        print()
        print("FATAL: ninguna fila cae en el universo de calibracion (`deriv46`).",
              file=sys.stderr)
        print("Sin interseccion no hay contra que comparar, y el descriptivo solo",
              file=sys.stderr)
        print("concluye nada. Revisar el filtro de volumen de radar.py.", file=sys.stderr)
        sys.exit(1)
    P = P.sort_values(["run_at", "n_surge"], ascending=[True, False], kind="mergesort")
    P["top_pre"] = P.groupby("run_at").cumcount() < K
    R = estadisticos(P, "top_pre")
    spread, sem = R["sp"], R["sem"]

    # LO QUE EL RADAR HACE DE VERDAD: el universo desplegado, sin linea base
    # preregistrada. Descriptivo, no concluye nada.
    V = estadisticos(D, "en_top")
    fuera = float((~D.loc[D.en_top, "symbol"].isin(UNIVERSO_PRE)).mean())

    print("\n" + "=" * 64)
    print("CONTRA EL PREREGISTRO: los 46 pares de `deriv46`, top-8 re-rankeado")
    print(f"{'':22s}{'medido antes':>16s}{'EN VIVO':>14s}")
    print("=" * 64)
    print(f"{'spread (ATR base)':22s}{PRE_SPREAD:>+16.3f}{sem.mean():>+14.3f}")
    print(f"{'multiplo de camino':22s}{PRE_MULT:>15.2f}x{R['mult']:>13.2f}x")
    print(f"{'tasa de acierto':22s}{100*PRE_TASA:>15.1f}%{100*R['tasa']:>13.1f}%")
    print(f"{'linea base':22s}{100*PRE_BASE:>15.1f}%{100*R['base']:>13.1f}%")
    print(f"{'pares por barra':22s}{46:>16d}{R['pares']:>14.0f}")
    print(f"{'semanas':22s}{251:>16d}{len(sem):>14d}")
    print(f"{'barras > 0':22s}{'':>16s}{100*(spread > 0).mean():>13.0f}%")

    print("\n" + "-" * 64)
    print("DESCRIPTIVO: el universo desplegado, el que el radar mira de verdad.")
    print("NO tiene linea base preregistrada, asi que NO concluye nada.")
    print(f"  {R['pares']:.0f} pares por barra contra {V['pares']:.0f}   |   "
          f"{100*fuera:.0f}% de los elegidos cae FUERA de los 46")
    print(f"  spread {V['sem'].mean():+.3f}   multiplo {V['mult']:.2f}x   "
          f"tasa {100*V['tasa']:.1f}%")
    print("  Es mucho mas grande, y no es una buena noticia: pasa en la cola nueva e")
    print("  iliquida, donde los costos reales son 1,5x a 6,3x lo que asume el banco.")

    print("\n" + "-" * 64)
    # El veredicto se decide contra el MDE calculado con ESTE dato, no contra un numero
    # de semanas fijado de antemano. La unidad con potencia es la BARRA: las semanas se
    # muestran arriba para mirar consistencia, no para cortar.
    n_b = len(spread)
    obs = float(spread.mean())
    if n_b < N_MIN_BARRAS:
        print(f"TODAVIA NO ALCANZA: {n_b} barras, de {N_MIN_BARRAS} minimas para que el")
        print("sd propio sea estimable. Sin sd no hay MDE, y sin MDE no se afirma nada.")
    else:
        factor = factor_inflacion(spread)
        sd = float(spread.std(ddof=1))
        mde = Z * sd / np.sqrt(n_b / factor)
        print(f"observado {obs:+.3f} por barra   MDE actual +-{mde:.3f}   "
              f"({n_b} barras / {factor:.2f} = {n_b / factor:.1f} efectivas)")
        print()
        if obs > mde:
            print(f"REPLICO: {obs:+.3f} supera el MDE, y es el "
                  f"{100 * obs / PRE_SPREAD:.0f}% del {PRE_SPREAD:+.3f} preregistrado.")
            if obs < PRE_SPREAD:
                print("Si dio cerca de la mitad es lo normal y sigue siendo una replica:")
                print("la primera medicion exagera, porque se encontro mirando.")
            else:
                print("Dio MAS grande que lo preregistrado, y eso no es mejor noticia que")
                print("dar igual: lo esperable era menos. Con el MDE apenas superado")
                print(f"({obs:+.3f} contra +-{mde:.3f}) el intervalo todavia abarca el")
                print("preregistrado, asi que no hay nada que explicar todavia. Si se")
                print("sostiene arriba con mas barras, ahi si hay que preguntar por que.")
            print("Accion: NO tocar nada del radar (HANDOFF_FORWARD_TEST.md, seccion 6).")
        elif obs < -mde:
            print(f"NO REPLICO: {obs:+.3f}, negativo y fuera del MDE.")
            print("Por la regla de parada el radar se apaga: comentar el bloque")
            print("`schedule:` de .github/workflows/radar.yml y commitear.")
        else:
            print(f"TODAVIA NO ALCANZA: {obs:+.3f} cae dentro de +-{mde:.3f}.")
            print("Esto NO es 'no esta', es 'no se pudo medir'. Son cosas distintas, y")
            print("confundirlas ya cerro mal dos familias en este repo.")
            # A la cadencia REAL medida, no a la que pide el cron: GitHub saltea
            # corridas y las atrasa, asi que el ritmo nominal miente.
            dias = (D.run_at.max() - D.run_at.min()).total_seconds() / 86400
            por_dia = n_b / dias if dias > 0 else float("nan")
            print()
            print(f"{'para decidir sobre un efecto de':<34}{'barras':>8}{'FALTAN':>9}")
            for etq, ef in (("lo medido (x1,0)", 1.0), ("la mitad (x0,5)", 0.5),
                            ("un tercio (x0,33)", 1 / 3), ("un cuarto (x0,25)", 0.25)):
                n_req = (Z * sd / (PRE_SPREAD * ef)) ** 2 * factor
                falta = max(n_req - n_b, 0) / por_dia
                print(f"{etq:<34}{n_req:>8,.0f}{falta:>8.0f}d")
            print(f"\n(a la cadencia REAL medida, {por_dia:.1f} barras utiles por dia)")
    print("\nNO ajustar `n_surge` ni `k` por este resultado: eso convierte el")
    print("out-of-sample en in-sample y no queda ninguna ventana limpia.")

    if a.out:
        D.to_csv(a.out, index=False)
        print(f"\ndetalle -> {a.out}")


if __name__ == "__main__":
    main()
