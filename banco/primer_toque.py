"""
BANCO DE PRUEBAS — mide si una senal candidata sirve para operar, en minutos.

Metodo: triple barrera / primer toque. Se entra en una vela y se camina hacia adelante
hasta tocar el target (+t%) o el stop (-s%). Lo que toque primero define el trade.

La unica pregunta que importa:

    win rate necesario = (stop + costo) / (target + stop)

El win rate NO es una habilidad, es una perilla: lo fija la relacion target/stop.
Con +8%/-16% sacas 64% de aciertos sin ninguna senal, y perdes plata igual. Por eso
todo se compara contra el umbral, nunca contra 50%.

Ojo con la intuicion de la moneda al aire: con barreras simetricas en %, un activo SIN
direccion no da 50% sino ~52% (para recuperarte de -8% necesitas +8,7%, o sea que la
barrera de abajo esta mas lejos en escala log). El banco calcula ese numero solo.

Uso:
    py -3.13 primer_toque.py                      # linea base
    py -3.13 primer_toque.py --target 10 --stop 5
    py -3.13 primer_toque.py --regimen            # + corte por mes y deteccion

Para probar TU senal, ver `evaluar_senal()` y el ejemplo al pie del README.
"""
import argparse
import sys

import numpy as np
import pandas as pd

from klines import load_panel, to_ms

INICIO = "2025-08-01"
FIN = "2026-08-01"
# OJO: 0,20% es SOLO el fee taker (0,10% por lado). NO incluye spread ni slippage,
# y por eso es OPTIMISTA. Se midio el costo real caminando el libro (`libro.py`,
# 2026-08-26, 480 pares USDT spot vivos, round trip completo):
#
#   banda          orden $1k   orden $10k     win rate necesario (target/stop 8%)
#   rank 1-50        0,230%      0,279%          51,44%  /  51,74%
#   rank 51-200      0,339%      0,597%          52,12%  /  53,73%
#   rank 201-400     0,441%      0,994%          52,76%  /  56,21%
#   rank 401-600     0,524%      1,261%          53,28%  /  57,88%
#
# O sea entre 1,5x y 6,3x este numero. Se DEJA en 0,20 a proposito: cambiarlo
# rompe la comparabilidad con todo lo ya medido, y como todas las familias
# cerradas se cerraron contra un costo demasiado BARATO, subirlo solo las mata
# mas — ningun negativo previo se da vuelta. Pero para cualquier resultado
# POSITIVO, o para cualquier cosa fuera del top-200, usar la tabla de arriba:
# con 0,20 el umbral esta ~0,7 pp demasiado bajo.
COSTO_PCT = 0.20
MS_DIA = 86400000


# ---------------------------------------------------------------- umbrales

def winrate_necesario(target, stop, costo=COSTO_PCT):
    """El win rate por debajo del cual se pierde plata."""
    return (stop + costo) / (target + stop) * 100


def winrate_sin_direccion(target, stop):
    """Lo que daria un activo sin deriva. NO es 50%: las barreras simetricas en %
    no son simetricas en log, y la de abajo queda mas lejos."""
    d_up = np.log(1 + target / 100)
    d_dn = -np.log(1 - stop / 100)
    return d_dn / (d_up + d_dn) * 100


# ---------------------------------------------------------------- motor

def _entradas(df, target, stop, horizonte_h, paso_h):
    """[(t_entrada, resultado, velas_hasta_resolver)]. resultado: 1 gana, -1 pierde,
    0 se acabo el tiempo. Empate en la misma vela -> conservador, cuenta como perdida."""
    c = df["c"].to_numpy()
    h = df["h"].to_numpy()
    l = df["l"].to_numpy()
    t = df["t"].to_numpy()
    n = len(c)
    up, dn = 1 + target / 100, 1 - stop / 100
    out = []
    for i in range(0, n - horizonte_h, paso_h):
        e = c[i]
        j0, j1 = i + 1, i + 1 + horizonte_h
        sh, sl = h[j0:j1], l[j0:j1]
        hu = np.flatnonzero(sh >= e * up)
        hd = np.flatnonzero(sl <= e * dn)
        iu = hu[0] if hu.size else np.inf
        idn = hd[0] if hd.size else np.inf
        if iu == np.inf and idn == np.inf:
            out.append((t[i], 0, horizonte_h))
        elif iu < idn:
            out.append((t[i], 1, int(iu) + 1))
        else:
            out.append((t[i], -1, int(idn) + 1))
    return out


def tabla(panel, target=8, stop=8, horizonte_d=30, paso_h=12, verbose=True):
    """Una fila por (par, entrada). Es la base de todo lo demas."""
    hor = horizonte_d * 24
    filas = []
    for k, (sym, df) in enumerate(panel.items(), 1):
        for t, res, velas in _entradas(df, target, stop, hor, paso_h):
            filas.append((sym, t, res, velas))
        if verbose and k % 50 == 0:
            print(f"  primer toque {k}/{len(panel)}...", flush=True)
    T = pd.DataFrame(filas, columns=["sym", "t", "res", "velas"])
    T["dt"] = pd.to_datetime(T["t"], unit="ms", utc=True)
    T["semana"] = T["dt"].dt.strftime("%G-W%V")
    T["mes"] = T["dt"].dt.strftime("%Y-%m")
    T["resuelto"] = T["res"] != 0
    T.attrs.update(target=target, stop=stop, horizonte_d=horizonte_d)
    return T


# ---------------------------------------------------------------- evaluacion

def evaluar(T, etiqueta="linea base", costo=COSTO_PCT, mostrar=True):
    """Aplica la disciplina completa: umbral, mediana por par, mediana por semana y
    chequeo de concentracion. Devuelve dict con los numeros."""
    tgt, stp = T.attrs["target"], T.attrs["stop"]
    nec = winrate_necesario(tgt, stp, costo)
    R = T[T["resuelto"]]
    if R.empty:
        return {}
    wr = (R["res"] > 0).mean() * 100
    ev = (R["res"] > 0).mean() * (tgt - costo) - (R["res"] < 0).mean() * (stp + costo)

    por_par = R.groupby("sym")["res"].apply(lambda s: (s > 0).mean() * 100)
    por_sem = R.groupby("semana")["res"].apply(lambda s: (s > 0).mean() * 100)

    # concentracion: sacar los pares que mas aportan (ver trampa BANKUSDT en el README)
    aporte = R.groupby("sym")["res"].sum().sort_values(ascending=False)
    sin3 = R[~R["sym"].isin(aporte.head(3).index)]
    wr_sin3 = (sin3["res"] > 0).mean() * 100 if not sin3.empty else float("nan")

    out = dict(etiqueta=etiqueta, n=int(len(R)), win_rate=wr, necesario=nec,
               margen=wr - nec, ev_pct=ev, mediana_por_par=float(por_par.median()),
               mediana_por_semana=float(por_sem.median()), win_rate_sin_top3=wr_sin3,
               pct_timeout=float((T["res"] == 0).mean() * 100))
    if mostrar:
        ok = "GANA" if wr > nec else "pierde"
        print(f"\n  {etiqueta}  (n={len(R):,})")
        print(f"    win rate {wr:6.2f}%   necesario {nec:6.2f}%   "
              f"margen {wr-nec:+6.2f}pp   -> {ok}")
        print(f"    EV por trade {ev:+.3f}%  |  sin resolver {out['pct_timeout']:.1f}%")
        print(f"    mediana por par {por_par.median():6.2f}%  |  "
              f"mediana por semana {por_sem.median():6.2f}%  |  "
              f"sin top-3 {wr_sin3:6.2f}%")
    return out


def evaluar_senal(T, mascara, etiqueta="senal", costo=COSTO_PCT):
    """LA funcion del banco: tu senal suma o no?

    `mascara` es un booleano por fila de T (True = la senal dispara en esa entrada).
    Compara el win rate con senal contra la linea base. Si no separa varios puntos
    porcentuales POR ENCIMA del umbral, la senal no sirve — por interesante que suene.
    """
    base = evaluar(T, "linea base (todas las entradas)", costo)
    con = evaluar(T[mascara], f"{etiqueta} (dispara)", costo)
    sin = evaluar(T[~mascara], f"{etiqueta} (no dispara)", costo)
    if base and con:
        print(f"\n  APORTE DE LA SENAL: {con['win_rate'] - base['win_rate']:+.2f}pp "
              f"sobre la linea base")
        if sin:
            print(f"  separacion dispara/no-dispara: "
                  f"{con['win_rate'] - sin['win_rate']:+.2f}pp")
        print(f"  {'CRUZA el umbral' if con['margen'] > 0 else 'NO cruza el umbral'} "
              f"(margen {con['margen']:+.2f}pp)")
        print("  Aportar pp NO alcanza: lo que decide es cruzar el umbral.")
    return base, con, sin


# ---------------------------------------------------------------- regimen

def regimen(T, panel, costo=COSTO_PCT):
    """El momento decide mas que la seleccion. Aca se ve cuanto, y si se puede
    anticipar con un detector que SOLO mira el pasado."""
    tgt, stp = T.attrs["target"], T.attrs["stop"]
    nec = winrate_necesario(tgt, stp, costo)
    R = T[T["resuelto"]]

    print("\n" + "=" * 74)
    print("REGIMEN — mes a mes")
    print("=" * 74)
    mm = R.groupby("mes")["res"].agg(wr=lambda s: (s > 0).mean() * 100, n="size")
    for m, r in mm.iterrows():
        marca = "  ARRIBA" if r["wr"] > nec else ""
        print(f"  {m}  {r['wr']:6.2f}%  {'#' * int(max(0, r['wr']-40) * 1.4)}{marca}")

    sem = R.groupby("semana")["res"].agg(wr=lambda s: (s > 0).mean() * 100, n="size")
    sem = sem[sem["n"] >= 200]
    arriba = (sem["wr"] > nec).to_numpy()
    cambios = int((arriba[1:] != arriba[:-1]).sum())
    print(f"\n  semanas medidas {len(sem)} | arriba del umbral "
          f"{arriba.sum()} ({arriba.mean()*100:.0f}%) | CAMBIOS de regimen {cambios}")

    print("\n" + "=" * 74)
    print("DETECCION EN TIEMPO REAL — el detector solo puede mirar el pasado")
    print("=" * 74)
    idx = {}
    for sym, df in panel.items():
        s = pd.Series(np.log(df["c"].to_numpy()), index=df["t"].to_numpy())
        idx[sym] = s - s.iloc[0]
    mkt = pd.DataFrame(idx).sort_index().median(axis=1)
    mkt.index = pd.to_datetime(mkt.index, unit="ms", utc=True)

    Rs = R.sort_values("dt")
    for N in (14, 30, 60):
        trail = (mkt - mkt.shift(N * 24)) * 100
        tr = trail.reindex(Rs["dt"], method="ffill").to_numpy()
        sub = Rs.assign(trail=tr).dropna(subset=["trail"])
        hi, lo = sub[sub["trail"] > 0], sub[sub["trail"] <= 0]
        wr_hi = (hi["res"] > 0).mean() * 100 if len(hi) else float("nan")
        wr_lo = (lo["res"] > 0).mean() * 100 if len(lo) else float("nan")
        print(f"  mercado ultimos {N:2d}d  subiendo {wr_hi:6.2f}%  "
              f"bajando {wr_lo:6.2f}%  separacion {wr_hi-wr_lo:+6.2f}pp  "
              f"(umbral {nec:.2f}%)")
    print("\n  Si el signo de la separacion cambia segun el lookback, es ruido.")


# ---------------------------------------------------------------- CLI

def main():
    ap = argparse.ArgumentParser(description="Banco de pruebas — primer toque")
    ap.add_argument("--target", type=float, default=8)
    ap.add_argument("--stop", type=float, default=8)
    ap.add_argument("--horizonte", type=int, default=30, help="dias de limite")
    ap.add_argument("--paso", type=int, default=12, help="horas entre entradas")
    ap.add_argument("--pares", type=int, default=200)
    ap.add_argument("--inicio", default=INICIO)
    ap.add_argument("--fin", default=FIN)
    ap.add_argument("--costo", type=float, default=COSTO_PCT)
    ap.add_argument("--regimen", action="store_true")
    a = ap.parse_args()

    panel = load_panel(a.inicio, a.fin, n=a.pares)
    if not panel:
        print("FATAL: no se pudo cargar el panel"); sys.exit(1)

    print(f"\nbarrera +{a.target}% / -{a.stop}%  |  limite {a.horizonte}d  |  "
          f"costo {a.costo:.2f}%")
    print(f"  win rate NECESARIO      {winrate_necesario(a.target, a.stop, a.costo):6.2f}%")
    print(f"  win rate SIN DIRECCION  {winrate_sin_direccion(a.target, a.stop):6.2f}%"
          f"   <- lo que da un activo que no va a ningun lado")

    T = tabla(panel, a.target, a.stop, a.horizonte, a.paso)
    print(f"\n{len(T):,} entradas | mediana de dias hasta resolver "
          f"{T.loc[T['resuelto'], 'velas'].median()/24:.1f}d")
    evaluar(T, "LINEA BASE (entrada al azar)", a.costo)

    if a.regimen:
        regimen(T, panel, a.costo)


if __name__ == "__main__":
    main()
