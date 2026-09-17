"""
ALERTAS — el score compuesto, ¿ordena mejor que UN indicador?

La pregunta no es "¿hay senal?" (eso ya se cerro: [[project-swing-techo-condicional]]
no encontro informacion condicional con 36 features y modelos libres). La pregunta es
mas barata y no esta contestada: **dentro del stream de alertas que el bot ya emite**,
el score de 15 puntos —base + bonos escalonados + penalizaciones, ~200 lineas de
config— ¿separa mejor que ordenar por un solo indicador, o que ordenar al azar?

Si empata con el azar, el score no compra nada y colapsarlo es una mejora de robustez:
menos perillas que sobreajustar, backtest mas parecido al vivo.

El motor NO es nuevo: se arma una tabla con el mismo esquema que `primer_toque.tabla`
(sym, t, res, velas, semana, resuelto) pero anclada en ALERTAS en vez de en la grilla
de 12h, y se la pasa a `lote.lote()` con sus seis compuertas cableadas.

    py -3.13 alertas.py --alertas ../bt_base.json --solo-conteo
    py -3.13 alertas.py --alertas ../bt_base.json --horizonte 48
    py -3.13 alertas.py --alertas ../bt_base.json --horizonte 48 --cruces

ADVERTENCIAS QUE VAN EN TODO RESULTADO DE ACA:

1. El stream es de REPLAY, no vivo. [[project-replay-no-reemplaza-vivo]]: el backtest
   genera 1/5 a 1/3 de las alertas reales y el nivel medio se da vuelta. Pero aca la
   comparacion es RELATIVA y todos los brazos viven en el mismo stream, asi que el
   sesgo es de modo comun. Lo que NO se puede leer de esta corrida es el nivel.
2. El camino de primer toque se recorre con velas de 1h y la alerta cae en grilla de
   15m: se pierde hasta 1h de camino. Es identico para todos los brazos.
3. Costo: `--costo` default 0.20% es el supuesto del repo. El medido en el libro es
   1,5x a 6,3x eso ([[project-costos-reales-libro]]). Correr tambien con --costo 0.50.
"""
import argparse
import json
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed

import numpy as np
import pandas as pd

from klines import klines
from lote import N_MIN, SEM_N_MIN, _feat_simbolo, lote
from primer_toque import COSTO_PCT, winrate_necesario

HORA = 3_600_000
DIA = 86_400_000
ATRAS_H = 780          # cubre las ventanas de 720 barras de _feat_simbolo


# ------------------------------------------------------------------ carga
def _uno_json(path):
    with open(path, encoding="utf-8") as f:
        d = json.load(f)
    rows = d["main"] if isinstance(d, dict) and "main" in d else d
    if isinstance(rows, dict):
        rows = next(v for v in rows.values() if isinstance(v, list))
    return rows


def cargar(paths):
    """Registros de alerta de una o varias corridas de backtest.py (--out).

    Acepta varios archivos porque la ventana larga se corre en trozos: un solo
    proceso de 3+ horas es fragil y si muere no deja nada. Los trozos son
    contiguos y no se solapan, pero se deduplica igual por (symbol, alerted_at,
    signal_type) por si dos ventanas se pisan en el borde.
    """
    if isinstance(paths, str):
        paths = [paths]
    rows = []
    for p in paths:
        r = _uno_json(p)
        print(f"  {p}: {len(r):,} alertas")
        rows.extend(r)
    A = pd.DataFrame(rows)
    antes = len(A)
    A = A.drop_duplicates(["symbol", "alerted_at", "signal_type"])
    if len(A) < antes:
        print(f"  deduplicadas {antes - len(A):,} en los bordes de los trozos")
    A["dt"] = pd.to_datetime(A["alerted_at"], utc=True)
    A["t"] = A["dt"].astype("int64") // 10**6
    return A.sort_values("t").reset_index(drop=True)


def bajar(A, horizonte_h, workers=8, verbose=True):
    """Panel 1h de los simbolos que alertaron. Ventana redondeada a dia para que la
    clave de cache sea estable entre corridas."""
    syms = sorted(A["symbol"].unique())
    t0 = ((int(A["t"].min()) - ATRAS_H * HORA) // DIA) * DIA
    t1 = ((int(A["t"].max()) + (horizonte_h + 4) * HORA) // DIA + 1) * DIA
    if verbose:
        print(f"panel: {len(syms)} pares 1h | "
              f"{pd.to_datetime(t0, unit='ms')} -> {pd.to_datetime(t1, unit='ms')}")
    panel, hechos = {}, [0]

    def _uno(s):
        try:
            return s, klines(s, t0, t1, "1h")
        except Exception as ex:
            print(f"  {s}: {type(ex).__name__} {ex}", flush=True)
            return s, None

    with ThreadPoolExecutor(workers) as ex:
        for f in as_completed([ex.submit(_uno, s) for s in syms]):
            s, df = f.result()
            if df is not None and len(df) > ATRAS_H:
                panel[s] = df
            hechos[0] += 1
            if verbose and hechos[0] % 50 == 0:
                print(f"  {hechos[0]}/{len(syms)}", flush=True)
    if verbose:
        print(f"  {len(panel)} pares con historia suficiente")
    return panel


# ------------------------------------------------------------------ tabla
def tabla_alertas(A, panel, target, stop, horizonte_h, verbose=True):
    """Una fila por alerta. Primer toque desde `entry_price` de la alerta.

    El camino empieza en la primera vela de 1h que ABRE en o despues de la alerta:
    nunca se mira la vela en la que la alerta ocurrio. Si el horizonte no entra
    completo en los datos, la alerta se descarta (no se trunca: truncar sesga hacia
    'no resuelto' justo al final de la ventana).
    """
    up, dn = 1 + target / 100, 1 - stop / 100
    filas = []
    for r in A.itertuples():
        df = panel.get(r.symbol)
        if df is None:
            continue
        t = df["t"].to_numpy()
        j0 = int(np.searchsorted(t, r.t, side="left"))
        j1 = j0 + horizonte_h
        if j1 > len(t):
            continue
        e = float(r.entry_price)
        sh = df["h"].to_numpy()[j0:j1]
        sl = df["l"].to_numpy()[j0:j1]
        hu = np.flatnonzero(sh >= e * up)
        hd = np.flatnonzero(sl <= e * dn)
        iu = hu[0] if hu.size else np.inf
        idn = hd[0] if hd.size else np.inf
        if iu == np.inf and idn == np.inf:
            res, velas = 0, horizonte_h
        elif iu < idn:                      # empate en la misma vela -> perdida
            res, velas = 1, int(iu) + 1
        else:
            res, velas = -1, int(idn) + 1
        filas.append((r.symbol, int(r.t), res, velas, int(r.Index)))

    T = pd.DataFrame(filas, columns=["sym", "t", "res", "velas", "aid"])
    T["dt"] = pd.to_datetime(T["t"], unit="ms", utc=True)
    T["semana"] = T["dt"].dt.strftime("%G-W%V")
    T["mes"] = T["dt"].dt.strftime("%Y-%m")
    T["resuelto"] = T["res"] != 0
    T.attrs.update(target=target, stop=stop, horizonte_d=horizonte_h / 24)
    if verbose:
        print(f"tabla: {len(T):,} alertas con horizonte completo "
              f"(de {len(A):,}) | resueltas {T['resuelto'].mean()*100:.1f}%")
    return T.reset_index(drop=True)


# ------------------------------------------------------------------ features
def features_alertas(A, T, panel, verbose=True):
    """Features al momento de la alerta. Todo mira a la ULTIMA VELA CERRADA de 1h,
    igual que el motor de produccion (offset -1)."""
    piezas = []
    for k, (sym, df) in enumerate(panel.items(), 1):
        d = pd.DataFrame(_feat_simbolo(df))
        d.insert(0, "t", df["t"].to_numpy())
        d.insert(0, "sym", sym)
        piezas.append(d)
        if verbose and k % 100 == 0:
            print(f"  features {k}/{len(panel)}...", flush=True)
    FULL = pd.concat(piezas, ignore_index=True)

    mkt = FULL.groupby("t")[["roc_168", "vol_168"]].median()
    mkt.columns = ["mkt_168", "mkt_vol_168"]
    FULL = FULL.merge(mkt, left_on="t", right_index=True, how="left")
    FULL["rs_168"] = FULL["roc_168"] - FULL["mkt_168"]

    key = pd.DataFrame({"sym": T["sym"].to_numpy(),
                        "t": (T["t"].to_numpy() // HORA) * HORA - HORA})
    F = key.merge(FULL, on=["sym", "t"], how="left").drop(columns=["sym", "t"])
    F.index = T.index

    a = A.loc[T["aid"].to_numpy()].reset_index(drop=True)
    F["score"] = pd.to_numeric(a["score"], errors="coerce").to_numpy()
    F["obv_slope"] = pd.to_numeric(a["obv_slope"], errors="coerce").to_numpy()
    F["cvd_ratio"] = pd.to_numeric(a["cvd_ratio"], errors="coerce").to_numpy()
    with np.errstate(invalid="ignore", divide="ignore"):
        F["ext"] = (a["entry_price"].to_numpy(float)
                    / a["ref_price"].to_numpy(float) - 1.0)   # cuanto ya extendio
    return F


# ------------------------------------------------------------------ hipotesis
def hipotesis(F, A, T, cruces=False, qs=0.20, qp=0.33, seed=0):
    """Colas por cuantil, nunca umbrales a dedo (eso es look-elsewhere disfrazado).

    Simples usan quintiles (~20%). Los cruces usan terciles (~33%) porque el cruce de
    dos quintiles cae a ~4% de la muestra y no llega a N_MIN.
    """
    a = A.loc[T["aid"].to_numpy()].reset_index(drop=True)
    a.index = T.index
    H = {}

    # cobertura RELATIVA: el umbral absoluto tiene que salir de N_MIN, no de aca.
    # Una feature que existe en el 90% de las filas es utilizable aunque la muestra
    # sea chica; si la cola no llega a N_MIN, `lote` la marca POCA MUESTRA sola.
    cols = [c for c in F.columns
            if F[c].notna().sum() >= 0.90 * len(F) and F[c].nunique(dropna=True) > 5]
    for c in cols:
        lo, hi = F[c].quantile([qs, 1 - qs])
        H[f"{c} alto"] = F[c] >= hi
        H[f"{c} bajo"] = F[c] <= lo

    # ---- el incumbente, con sus propios cortes de produccion
    H["INC score>=13 BEST"] = a["score"] >= 13
    H["INC score>=11 STRONG"] = a["score"] >= 11
    for st in sorted(a["signal_type"].dropna().unique()):
        H[f"INC tipo {st}"] = a["signal_type"] == st
    if "candle_status" in a:
        H["INC vela cerrada"] = a["candle_status"] == "closed"
    for flag in ("htf_1h_up", "htf_4h_up"):
        if flag in a:
            H[f"INC {flag}"] = a[flag].fillna(False).astype(bool)

    # ---- control nulo: si el score no le gana a esto, no compra nada
    rng = np.random.default_rng(seed)
    for i in range(3):
        H[f"CONTROL azar {i+1}"] = pd.Series(rng.random(len(T)) < qs, index=T.index)

    if cruces:
        ter = {}
        for c in cols:
            lo, hi = F[c].quantile([qp, 1 - qp])
            ter[f"{c} A"] = F[c] >= hi
            ter[f"{c} B"] = F[c] <= lo
        nombres = list(ter)
        for i, x in enumerate(nombres):
            for y in nombres[i + 1:]:
                if x.rsplit(" ", 1)[0] == y.rsplit(" ", 1)[0]:
                    continue
                H[f"{x} + {y}"] = ter[x] & ter[y]
    return H


# ------------------------------------------------------------------ medianas
def medianas(A, T, H, costo, top=25, mostrar=True):
    """Segunda metrica: mediana del retorno a 24h, neta de costo.

    El win rate depende de donde se pongan las barreras; la mediana no. Y la MEDIA
    esta descartada de antemano — este repo ya la vio secuestrada por un solo par dos
    veces ([[project-swing-trampa-concentracion]]). Se reporta igual, al lado, para
    que se vea la brecha.
    """
    a = A.loc[T["aid"].to_numpy()].reset_index(drop=True)
    a.index = T.index
    if "price_24h" not in a:
        return None
    ret = (a["price_24h"].astype(float) / a["entry_price"].astype(float) - 1) * 100 - costo
    ok = ret.notna()
    base_med, base_mea = ret[ok].median(), ret[ok].mean()

    filas = []
    for nombre, m in H.items():
        m = m.reindex(T.index, fill_value=False).fillna(False).astype(bool) & ok
        n = int(m.sum())
        if n < N_MIN:
            continue
        r = ret[m]
        # dardo pareado: mismos simbolos, mismos pesos, cualquier momento
        peso = a.loc[m, "symbol"].value_counts()
        med_sym = ret[ok].groupby(a.loc[ok, "symbol"]).median()
        comun = peso.index.intersection(med_sym.index)
        par = (float((med_sym[comun] * peso[comun]).sum() / peso[comun].sum())
               if len(comun) else np.nan)
        filas.append(dict(hipotesis=nombre, n=n, mediana=float(r.median()),
                          media=float(r.mean()), vs_base=float(r.median()) - base_med,
                          vs_pareado=float(r.median()) - par))
    if not filas:
        return None
    D = pd.DataFrame(filas).sort_values("mediana", ascending=False)
    if not mostrar:
        return D

    print("\n" + "=" * 92)
    print(f"MEDIANA DEL RETORNO A 24h, neta de {costo:.2f}%  |  "
          f"linea base mediana {base_med:+.2f}%  media {base_mea:+.2f}%")
    print("=" * 92)
    print(f"{'hipotesis':34s} {'n':>7s} {'mediana':>9s} {'vs base':>9s} "
          f"{'vs dardo':>9s} {'media':>9s}")
    print("-" * 92)
    vista = pd.concat([D.head(top), D.tail(5)]).drop_duplicates("hipotesis")
    for _, r in vista.iterrows():
        print(f"{r.hipotesis[:34]:34s} {r.n:7,d} {r.mediana:+9.2f} "
              f"{r.vs_base:+9.2f} {r.vs_pareado:+9.2f} {r.media:+9.2f}")
    print("-" * 92)
    print("Leer la columna 'vs dardo': aisla el MOMENTO de la eleccion de moneda.")
    return D


# ------------------------------------------------------------------ conteo
def conteo(A, T, F, H, target, stop, costo):
    """Contar ANTES de decidir. Es la regla que salio del error de unlocks."""
    nec = winrate_necesario(target, stop, costo)
    R = T[T["resuelto"]]
    sem = T.groupby("semana").size()
    sem_ok = int((sem >= SEM_N_MIN).sum())
    wr = (R["res"] > 0).mean() * 100 if len(R) else float("nan")
    print("\n" + "=" * 78)
    print("CONTEO — potencia disponible")
    print("=" * 78)
    print(f"  alertas totales            {len(A):,}")
    print(f"  con horizonte completo     {len(T):,}")
    print(f"  resueltas (tocan barrera)  {len(R):,}  ({len(R)/max(len(T),1)*100:.1f}%)")
    print(f"  win rate linea base        {wr:.2f}%   "
          f"necesario {nec:.2f}%   margen {wr-nec:+.2f}pp")
    print(f"  semanas                    {len(sem)}  "
          f"(con >={SEM_N_MIN} alertas: {sem_ok})")
    print(f"  alertas/semana  mediana    {sem.median():.0f}")
    print(f"  features con cobertura     "
          f"{int(F.notna().sum().ge(0.90 * len(F)).sum())} de {F.shape[1]}")
    print(f"  hipotesis                  {len(H)}")
    n20 = int(len(R) * 0.20)
    n11 = int(len(R) * 0.33 * 0.33)

    def mde(n):
        return 1.96 * np.sqrt(0.25 / n) * 100 if n > 0 else float("nan")

    print(f"\n  n de una cola del 20%      ~{n20:,}   "
          f"{'OK' if n20 >= N_MIN else f'POR DEBAJO de N_MIN={N_MIN}'}"
          f"   MDE ~{mde(n20):.1f}pp")
    print(f"  n de un cruce de terciles  ~{n11:,}   "
          f"{'OK' if n11 >= N_MIN else f'POR DEBAJO de N_MIN={N_MIN}'}"
          f"   MDE ~{mde(n11):.1f}pp")
    if sem_ok < 8:
        print(f"\n  AVISO: con {sem_ok} semanas utiles el p de bloques devuelve 1.0 "
              f"por diseno.\n  Esta corrida NO puede adjudicar nada: es plomeria.")
    print("=" * 78)


# ------------------------------------------------------------------ main
def main():
    ap = argparse.ArgumentParser(description="Banco — ordenar DENTRO de las alertas")
    ap.add_argument("--alertas", required=True, nargs="+",
                    help="uno o mas json de backtest.py --out (trozos contiguos)")
    ap.add_argument("--target", type=float, default=8)
    ap.add_argument("--stop", type=float, default=8)
    ap.add_argument("--horizonte", type=int, default=48, help="horas")
    ap.add_argument("--costo", type=float, default=COSTO_PCT)
    ap.add_argument("--q", type=float, default=0.10)
    ap.add_argument("--sem-n-min", type=int, default=SEM_N_MIN)
    ap.add_argument("--cruces", action="store_true")
    ap.add_argument("--solo-conteo", action="store_true")
    ap.add_argument("--humo", action="store_true",
                    help="ejercita TODO el pipeline sin imprimir un solo resultado por "
                         "brazo. Para probar plomeria sin quemar el preregistro: mirar "
                         "resultados de una muestra chica contamina la corrida real si "
                         "la ventana se solapa (paso una vez, ver PREREGISTRO_RANKING).")
    ap.add_argument("--workers", type=int, default=8)
    ap.add_argument("--out", default=None)
    a = ap.parse_args()

    A = cargar(a.alertas)
    print(f"{len(A):,} alertas  |  {A['dt'].min()} -> {A['dt'].max()}")
    panel = bajar(A, a.horizonte, workers=a.workers)
    if not panel:
        print("FATAL: panel vacio")
        sys.exit(1)

    T = tabla_alertas(A, panel, a.target, a.stop, a.horizonte)
    if T.empty:
        print("FATAL: ninguna alerta con horizonte completo")
        sys.exit(1)
    F = features_alertas(A, T, panel)
    H = hipotesis(F, A, T, cruces=a.cruces)

    conteo(A, T, F, H, a.target, a.stop, a.costo)
    if a.solo_conteo:
        return

    D = lote(T, H, costo=a.costo, q=a.q, sem_n_min=a.sem_n_min, mostrar=not a.humo)
    M = medianas(A, T, H, a.costo, mostrar=not a.humo)
    if a.humo:
        print(f"\nHUMO OK — {len(D)} brazos evaluados, "
              f"{0 if M is None else len(M)} con n suficiente para mediana. "
              f"Resultados NO impresos a proposito.")
    if a.out:
        D.to_csv(a.out, index=False)
        if M is not None:
            M.to_csv(a.out.replace(".csv", "_mediana.csv"), index=False)
        print(f"\ntabla -> {a.out}")


if __name__ == "__main__":
    main()
