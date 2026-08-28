"""
FUTUROS — la capa que le falta al banco para probar el PERPETUO como instrumento.

`klines.py` ya sabe traer velas de `fapi` (`mercado="fut"`). Lo que falta —y es lo que
hace que un perp NO sea "spot mas barato"— es el **funding**: cada 8h el perp liquida la
diferencia contra el indice, y a 24h de tenencia eso son 3 pagos. En este repo ya hay un
precedente de lo que pasa si se ignora (`funding.py`: hasta 0,63% en 7 dias, TRES veces
la comision que el banco contaba).

Signo, que se equivoca facil (copiado de `funding.py` para no tener que ir a buscarlo):

    funding > 0  -> el perp cotiza sobre el indice, los LARGOS pagan a los cortos
    funding < 0  -> al reves

O sea que el retorno de una posicion LARGA en perp es `ret - carry`, y el de una CORTA
`-(ret - carry)`. La antisimetria se mantiene, que es lo que deja usar el mismo test de
`ranking.py` para las dos direcciones.

    py -3.13 futuros.py     # self-test
"""
import numpy as np
import pandas as pd

from funding import bajar
from klines import to_ms

HORA_MS = 3_600_000


def _serie(sym, ini_ms, fin_ms):
    """Funding de un simbolo como (tiempos, suma acumulada). None si no hay perp.

    `bajar` prueba las variantes `1000X`, asi que sirve tanto si `sym` viene con el
    nombre de spot (`PEPEUSDT`) como si ya viene con el de perp (`1000PEPEUSDT`): el
    primer intento es el simbolo tal cual.
    """
    d = bajar(sym, ini_ms, fin_ms)
    if d is None or d.empty:
        return None
    t = d["t"].to_numpy(np.int64)
    # cumsum con un cero adelante: asi `acum[hi] - acum[lo]` es la suma de [lo, hi)
    acum = np.concatenate([[0.0], np.cumsum(d["rate"].to_numpy(float))])
    return t, acum


def _suma(serie, desde_ms, hasta_ms):
    """Suma de los fundings pagados en (desde, hasta]. Vectorizado sobre las entradas."""
    t, acum = serie
    lo = np.searchsorted(t, desde_ms, "right")
    hi = np.searchsorted(t, hasta_ms, "right")
    return acum[hi] - acum[lo]


def carry(claves, ini, fin, horizonte=24, verbose=True):
    """Funding alineado al tablero de `ranking.py`.

    `claves`: DataFrame con las columnas `sym` y `t` (el tablero mismo sirve).
    Devuelve un DataFrame con el mismo indice y dos columnas:

      `carry`      — funding pagado DURANTE la tenencia, o sea en (t+1h, t+(H+1)h].
                     Esa es la ventana real: `tablero()` entra al CIERRE de la barra `t`
                     (que abre en `t` y cierra en `t+1h`) y sale al cierre de `t+H`.
      `carry_acum` — funding de las 24h ANTERIORES a la entrada, o sea en (t+1h-24h,
                     t+1h]. Es pasado, no hay lookahead, y es el unico score que existe
                     en perp y no en spot.

    Los simbolos sin perp quedan en NaN, no en cero: cero seria afirmar que no pagaron
    funding, y lo que pasa es que no se sabe (y `_spread_semanal` ya descarta los NaN).
    """
    ini_ms, fin_ms = to_ms(ini), to_ms(fin)
    out = pd.DataFrame(index=claves.index,
                       data={"carry": np.nan, "carry_acum": np.nan})
    sin_perp = []
    for sym, idx in claves.groupby("sym").groups.items():
        s = _serie(sym, ini_ms, fin_ms)
        if s is None:
            sin_perp.append(sym)
            continue
        t0 = claves.loc[idx, "t"].to_numpy(np.int64) + HORA_MS      # cierre de la barra
        out.loc[idx, "carry"] = _suma(s, t0, t0 + horizonte * HORA_MS)
        out.loc[idx, "carry_acum"] = _suma(s, t0 - 24 * HORA_MS, t0)
    if verbose:
        cob = out["carry"].notna().mean()
        print(f"funding: {claves['sym'].nunique() - len(sin_perp)} pares con perp | "
              f"cobertura de filas {cob:.1%}"
              + (f" | SIN PERP: {len(sin_perp)}" if sin_perp else ""))
    return out


def aplicar(TB, C):
    """Mete el funding en el RETORNO del tablero y recalcula los objetivos direccionales.

    El funding NO es un costo de transaccion: es parte del retorno del activo, asi que le
    entra a LAS DOS PATAS (top-k y universo), no solo a la seleccionada. Por eso se
    corrige `ret` y se rederivan `y_largo`/`y_corto` en vez de tocar el termino de costo
    de `_spread_semanal`.

    `magnitud` NO se toca: no es una posicion y `runup - caida` es camino de precio.
    """
    TB = TB.copy()
    TB["carry"] = C["carry"].to_numpy()
    TB["carry_acum"] = C["carry_acum"].to_numpy()
    TB["ret_bruto"] = TB["ret"]
    # sin dato de funding no se puede afirmar el retorno del perp -> la fila muere
    TB["ret"] = TB["ret"] - TB["carry"]
    atr = TB["atr_base"].replace(0, np.nan)
    TB["y_largo"] = TB["ret"] / atr
    TB["y_corto"] = -TB["ret"] / atr
    TB["y_largo_crudo"] = TB["ret"]
    TB["y_corto_crudo"] = -TB["ret"]
    return TB


if __name__ == "__main__":
    claves = pd.DataFrame({"sym": ["BTCUSDT"] * 3 + ["1000PEPEUSDT"] * 3,
                           "t": [to_ms("2026-01-05") + i * 24 * HORA_MS
                                 for i in range(3)] * 2})
    C = carry(claves, "2025-08-01", "2026-08-01", horizonte=24)
    print(pd.concat([claves, C], axis=1).to_string(index=False))
    print("\n(3 pagos por dia: un carry de +0,0003 = 0,03% que el largo paga en 24h)")
