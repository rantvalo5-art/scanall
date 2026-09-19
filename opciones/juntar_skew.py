"""
Guarda el SKEW (risk reversal 25 delta) todos los dias. Hoy n = 0.

POR QUE EXISTE. `HANDOFF_FUENTES_NUEVAS.md` §4.1 verifico con grep que el skew no esta
en ninguna parte del repo: se midio el NIVEL de implicita (IV/RV, corrida 8), nunca la
INCLINACION. Y es otra clase de variable. Las doce familias medidas son, todas, patrones
en precios pasados; el risk reversal es el precio que el mercado paga HOY por protegerse
en una direccion — una cotizacion de asimetria, no un patron inferido. En FX y en
acciones es *la* variable canonica de posicionamiento direccional.

POR QUE NO SE PUEDE ESPERAR A NECESITARLO. Chequeado contra Deribit el 2026-08-31:
`get_instruments(expired=true)` devuelve 56 instrumentos de UN solo vencimiento, y pedir
velas de un instrumento vencido da `status: no_data`. **No hay historia gratis.** La
cadena viva esta completa en los tres venues y la vencida no existe: lo que no se guarda
hoy no se puede comprar, bajar ni reconstruir manana.

LO QUE ESTO **NO** ES. No es una hipotesis que se vaya a poder contestar pronto. Hoy
n = 0; dentro de un anio n = 12 meses, que es EXACTAMENTE donde murio la corrida 8 (18
meses de implicita para SOL, MDE 39%/anio contra un umbral de 10%). O sea que esto no es
medible antes de ~3-4 anios, y esta escrito para que nadie se ilusione en marzo. Se
compra opcionalidad, no se prueba nada. La regla de cuando SI se puede mirar esta en
`banco/PREREGISTRO_SKEW.md`, escrita antes de que exista la primera fila.

    py -3.13 -u juntar_skew.py            # esto corre el cron, una vez por dia
    py -3.13 -u juntar_skew.py --ver      # imprime y NO escribe (para mirar sin ensuciar)

Sale a `opciones/skew_diario/<venue>_<MONEDA>.csv`, una fila por dia, append idempotente
por fecha: correrlo dos veces el mismo dia no duplica nada y NO pisa lo ya guardado.
"""
import argparse
import os
import sys

import pandas as pd

import cadena

HERE = os.path.dirname(os.path.abspath(__file__))
SALIDA = os.path.join(HERE, "skew_diario")

COLS = ["fecha", "moneda", "venue", "vence", "dias", "rr25", "mariposa25", "iv_atm",
        "iv_put25", "iv_call25", "delta_put", "delta_call", "oi_calls", "oi_puts",
        "n", "subyacente"]


def guardar(venue, moneda, fila):
    """Append idempotente por fecha. Lo viejo NUNCA se pisa.

    Mismo criterio que `juntar_iv.py`: un recalculo retroactivo del venue no deberia
    poder reescribir una fila que ya se uso para medir.
    """
    os.makedirs(SALIDA, exist_ok=True)
    p = os.path.join(SALIDA, f"{venue}_{moneda}.csv")
    nuevo = pd.DataFrame([fila], columns=COLS)
    viejo = pd.read_csv(p, parse_dates=["fecha"]) if os.path.exists(p) else None
    todo = nuevo if viejo is None or viejo.empty else \
        pd.concat([viejo, nuevo], ignore_index=True)
    todo["fecha"] = pd.to_datetime(todo["fecha"])
    todo = todo.drop_duplicates("fecha", keep="first").sort_values("fecha")
    todo.to_csv(p, index=False, date_format="%Y-%m-%d")
    return len(todo) - (0 if viejo is None else len(viejo)), len(todo)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--ver", action="store_true", help="imprime sin escribir")
    a = ap.parse_args()

    ahora = pd.Timestamp.now(tz="UTC")
    hoy = ahora.normalize().tz_localize(None)
    print(f"skew_diario — {ahora:%Y-%m-%d %H:%M} UTC"
          f"{'  (solo mirar, no escribe)' if a.ver else ''}\n", flush=True)

    nuevas, medidas = 0, 0
    for m in cadena.MONEDAS:
        C = cadena.bajar(m, ahora)
        if C.empty:
            print(f"  {m:5} sin cadena en ningun venue", flush=True)
            continue
        for v in ("bybit", "okx"):
            r = cadena.rr25(C, v)
            if r is None:
                continue
            medidas += 1
            fila = dict(fecha=hoy, moneda=m, **{k: r[k] for k in COLS if k in r})
            print(f"  {m:5} {v:6} {r['vence']} ({r['dias']:5.1f}d, n={r['n']:3})  "
                  f"rr25 {r['rr25']:+6.2f}   mariposa {r['mariposa25']:+5.2f}   "
                  f"iv_atm {r['iv_atm']:6.2f}%", flush=True)
            if not a.ver:
                n, tot = guardar(v, m, fila)
                nuevas += n

    print(f"\n{medidas} series medidas, {nuevas} filas nuevas", flush=True)

    # Que falle FUERTE si deja de juntar. La forma en que un colector se muere sin que
    # nadie se entere es quedar en verde durante meses mientras no guarda nada: la
    # corrida 8 ya se comio una version de esto con `quoteCoin`, que devolvia
    # `SUCCESS` con la lista vacia. Un venue caido no alcanza para matar la corrida;
    # los dos a la vez, si.
    if medidas == 0:
        print("\nFATAL: ninguna moneda devolvio una cadena utilizable en ningun venue.",
              file=sys.stderr)
        print("No es un dia flojo: es el endpoint cambiado o el parser roto.",
              file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
