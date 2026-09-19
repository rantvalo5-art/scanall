"""
Guarda lo que COSTABA el straddle corto cada vez que el radar mira. Hoy n = 0.

EL AGUJERO QUE TAPA, dicho sin vueltas. El unico hallazgo vivo del repo es que se puede
predecir MAGNITUD (38 rankings sobreviven los cuatro regimenes; el radar acaba de
replicar en vivo). Y no se puede cobrar: magnitud no dice direccion, y el instrumento
que paga por movimiento sin direccion es un STRADDLE. Lo que falta para cerrar ese
circuito no es otra familia de predictores — es el PRECIO del straddle en el momento en
que el radar dispara, que es justo el dato que se evapora cada 4 horas.

Medido el 2026-09-19 sobre 23 dias de `radar_runs` (752 elegidos, 94 barras):

    universo desplegado (169 simbolos)    3 de 752 = 0,4% tienen opciones listadas
    universo de calibracion (los 46)     75 de 752 = 10,0%

El 10% es exactamente la tasa base (5 de 46), o sea que el radar NO elige preferentemente
nombres con opciones — pero 75 eventos en 23 dias sobre BTC/ETH/SOL/XRP/DOGE es una tasa
medible. El 0,4% del universo desplegado es, ademas, un segundo argumento independiente
—y mas barato que el de los costos— de por que esa cola no es un negocio: no tiene
instrumento.

LA CADENCIA NO ES CAPRICHO. Corre cada 2h para aparearse con las barras del radar
(`.github/workflows/radar.yml`, que intenta cada 2h y de-solapa a 4h al medir). Una foto
diaria no sirve: la pregunta es que costaba la opcion EN ESA BARRA, no ese dia.

LO QUE ESTO **NO** ES. No es una hipotesis lista para contestar. Es el dato que hace
falta para poder preguntar, dentro de meses, si cuando el radar dice "esto se va a mover
mas" se mueve mas de lo que la opcion YA cobraba. La compuerta de potencia y la regla de
cuando se puede mirar estan en `banco/PREREGISTRO_SKEW.md`, escritas antes de la primera
fila. Y vale la advertencia de siempre: esto NO autoriza a tocar el radar. Esta corriendo
un forward test preregistrado.

    py -3.13 -u prima_radar.py            # esto corre el cron
    py -3.13 -u prima_radar.py --ver      # imprime y NO escribe

Sale a `opciones/prima_radar/<MONEDA>.csv`, una fila por corrida y venue.
"""
import argparse
import os
import sys

import pandas as pd

import cadena

HERE = os.path.dirname(os.path.abspath(__file__))
SALIDA = os.path.join(HERE, "prima_radar")

COLS = ["ts", "moneda", "venue", "vence", "dias", "iv_atm", "straddle_pct",
        "spread_iv", "iv_atm_30d", "subyacente", "n"]

# La ventana del vencimiento CORTO. El horizonte del radar es 4h, asi que lo que
# interesa es lo mas pegado a hoy que exista; el piso de 0,08 dias (~2h) evita el
# vencimiento que expira dentro de minutos, donde el mark deja de ser un precio.
CORTO_MIN, CORTO_MAX = 0.08, 5.0


def foto(C, venue):
    """IV ATM y prima del straddle del vencimiento mas corto, mas la IV a 30d de contexto.

    El straddle sale en % del subyacente —(call + put) / spot— que es la unidad en la
    que se compara contra el camino que mide el radar. Solo Bybit sirve `markPrice`;
    en OKX la columna queda vacia a proposito y no se inventa con un Black-Scholes
    propio: un precio estimado por mi no es lo que se paga.
    """
    V = C[C.venue == venue]
    if V.empty:
        return None
    W = V[(V.dias >= CORTO_MIN) & (V.dias <= CORTO_MAX)]
    if W.empty:
        return None
    vence = W.sort_values("dias").iloc[0]["vence"]
    E = W[W.vence == vence]
    call = cadena._cerca(E[E.tipo == "C"], +0.50, tol=0.15)
    put = cadena._cerca(E[E.tipo == "P"], -0.50, tol=0.15)
    if call is None or put is None:
        return None

    spot = float(E["subyacente"].median())
    prima = float(call.mark + put.mark)
    # El ancho del libro EN PUNTOS DE IV, promediado entre las dos patas. Se guarda
    # ahora y no se estima despues: es lo que hay que descontarle al resultado para
    # que sea ejecutable, y en el momento de medir ya no va a existir. La corrida 8
    # midio que cruzar cuesta 1-2% de la prima; esto permite cobrarlo por barra en vez
    # de aplicar esa constante a todo.
    anchos = [float(x.iv_ask - x.iv_bid) for x in (call, put)
              if x.iv_ask == x.iv_ask and x.iv_bid == x.iv_bid and x.iv_bid > 0]
    largo = cadena.rr25(C, venue)           # la misma cadena, vencimiento ~30d
    return dict(vence=pd.Timestamp(vence).strftime("%Y-%m-%d"),
                dias=round(float(E["dias"].iloc[0]), 3),
                iv_atm=round(float((call.iv + put.iv) / 2), 4),
                spread_iv=round(sum(anchos) / len(anchos), 4) if anchos else float("nan"),
                straddle_pct=round(100 * prima / spot, 4) if prima == prima and spot > 0
                else float("nan"),
                iv_atm_30d=largo["iv_atm"] if largo else float("nan"),
                subyacente=round(spot, 6), n=int(len(E)))


def guardar(moneda, filas):
    """Append idempotente por (ts, venue). Lo viejo no se pisa."""
    os.makedirs(SALIDA, exist_ok=True)
    p = os.path.join(SALIDA, f"{moneda}.csv")
    nuevo = pd.DataFrame(filas, columns=COLS)
    viejo = pd.read_csv(p, parse_dates=["ts"]) if os.path.exists(p) else None
    todo = nuevo if viejo is None or viejo.empty else \
        pd.concat([viejo, nuevo], ignore_index=True)
    todo["ts"] = pd.to_datetime(todo["ts"])
    todo = todo.drop_duplicates(["ts", "venue"], keep="first").sort_values(["ts", "venue"])
    todo.to_csv(p, index=False, date_format="%Y-%m-%dT%H:%M:%SZ")
    return len(todo) - (0 if viejo is None else len(viejo)), len(todo)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--ver", action="store_true", help="imprime sin escribir")
    a = ap.parse_args()

    ahora = pd.Timestamp.now(tz="UTC")
    # Al minuto, sin segundos: el apareo con las barras del radar se hace por cercania
    # de tiempo, no por igualdad exacta, y los segundos solo ensucian el CSV.
    ts = ahora.floor("min").tz_localize(None)
    print(f"prima_radar — {ts:%Y-%m-%d %H:%M} UTC"
          f"{'  (solo mirar, no escribe)' if a.ver else ''}\n", flush=True)

    nuevas, fotos = 0, 0
    for m in cadena.MONEDAS:
        C = cadena.bajar(m, ahora)
        if C.empty:
            print(f"  {m:5} sin cadena", flush=True)
            continue
        filas = []
        for v in ("bybit", "okx"):
            f = foto(C, v)
            if f is None:
                continue
            fotos += 1
            filas.append(dict(ts=ts, moneda=m, venue=v, **f))
            sp = f["straddle_pct"]
            print(f"  {m:5} {v:6} {f['vence']} ({f['dias']:5.2f}d)  "
                  f"iv_atm {f['iv_atm']:6.2f}%   straddle "
                  f"{('%6.2f%%' % sp) if sp == sp else '   n/d'}   "
                  f"spread_iv {f['spread_iv']:5.2f}   iv_30d {f['iv_atm_30d']:6.2f}%", flush=True)
        if filas and not a.ver:
            n, _ = guardar(m, filas)
            nuevas += n

    print(f"\n{fotos} fotos, {nuevas} filas nuevas", flush=True)

    # Mismo criterio que `juntar_skew.py`: quedar en verde sin guardar nada es la forma
    # en que esto se muere sin que nadie se entere.
    if fotos == 0:
        print("\nFATAL: ninguna moneda devolvio un vencimiento corto utilizable.",
              file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
