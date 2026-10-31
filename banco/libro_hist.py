"""
LIBRO HISTORICO — la profundidad del libro de los perpetuos como fuente de features.
Es la seccion 7 de `PREREGISTRO_LIBRO.md`, y nada mas: el diseno esta fijado ahi.

EL DATO. `data.binance.vision/data/futures/um/daily/bookDepth`, desde el 2023-01-01.
Cada 30 s, la profundidad ACUMULADA de cada lado a 0,2/1/2/3/4/5 % del precio, en
contratos (`depth`) y en USD (`notional`). Porcentaje negativo = bid, positivo = ask.
Se usa el `notional`, que es comparable entre monedas.

LO QUE SE GUARDA. Los snapshots crudos no: ~60.000 archivos y 20-30 GB. Cada dia se
agrega a horario en el momento y se tira. Por hora queda el ULTIMO snapshot (lo que se
sabe cuando la vela cierra, igual que `metricas.py`), a 1 % y 5 % (el 0,2 % no existe en
los archivos antes de 2026; ver la enmienda del preregistro), mas cuantos snapshots hubo y cuantos segundos antes del
cierre cayo el ultimo, para poder auditar la cobertura.

EL CACHE ES POR PAR Y POR MES, no por rango como en `metricas.py`. La descarga entera es
una corrida de horas; si se corta (la notebook suspende, se cae la red), se retoma donde
quedo en vez de empezar de cero. Un mes con un dia que fallo por red (no por 404) NO se
cachea, asi la proxima corrida lo vuelve a pedir.

LA RESERVA NO SE BAJA. `PREREGISTRO_LIBRO.md` §2 guarda 2026-08-01 -> 2026-10-31 fuera
de muestra. `load_libro` corta en `FIN_PRINCIPAL` y se niega a pasarlo sin `reserva=True`,
que solo corresponde si algo sobrevive a la ventana principal.

    py -3.13 -u libro_hist.py --solo-conteo        # baja (o lee del cache) y audita
    py -3.13 -u libro_hist.py --solo-conteo --pares BTCUSDT,PEPEUSDT --hasta 2023-02-01
"""
import argparse
import io
import json
import os
import sys
import time
import zipfile
from concurrent.futures import ThreadPoolExecutor

import numpy as np
import pandas as pd
import requests

HERE = os.path.dirname(os.path.abspath(__file__))
from klines import CACHE as KCACHE  # noqa: E402
from metricas import perp_de  # noqa: E402

CACHE = os.path.join(HERE, ".libro_cache")
BASE = "https://data.binance.vision/data/futures/um/daily/bookDepth"
MS_H = 3_600_000
INICIO = "2023-01-01"           # el primer archivo que existe (2022-12-25 da 404)
FIN_PRINCIPAL = "2026-08-01"    # exclusivo: la reserva arranca aca
# Las profundidades del preregistro MENOS el 0,2 %: ese nivel no existe en los archivos
# hasta 2026 (entre el 01-ene y el 01-mar; antes solo vienen 1-5 %). Enmienda del
# 2026-10-31 en PREREGISTRO_LIBRO.md, escrita antes de calcular nada.
PROF = (1.0, 5.0)
COLS = [f"{lado}{str(x).replace('.', '')}" for x in PROF for lado in ("b", "a")]
# -> b10, a10, b50, a50


def deriv46():
    """Los 46 del pin `deriv46`, el mismo universo que `ranking.py --pin deriv46` y que
    `radar/medir.py` (UNIVERSO_PRE; verificado identico el 2026-10-31).

    Se lee el pin congelado y NO se usa `klines._universo_fijo`: si el archivo falta,
    esa funcion lo regenera con el ranking de volumen DE HOY, que es otro universo.
    """
    p = os.path.join(KCACHE, "universo_deriv46.json")
    if not os.path.exists(p):
        sys.exit(f"FATAL: falta {p}. Correr desde el repo que tiene el cache de klines; "
                 "regenerarlo daria otro universo.")
    with open(p, encoding="utf-8") as f:
        return sorted(json.load(f))


# ------------------------------------------------------------------ descarga
def _dia(perp, fecha, intentos=3):
    """(DataFrame horario, estado). estado: 'ok', 'no_existe' (404) o 'fallo' (red)."""
    url = f"{BASE}/{perp}/{perp}-bookDepth-{fecha}.zip"
    for i in range(intentos):
        try:
            r = requests.get(url, timeout=60)
            if r.status_code == 404:
                return None, "no_existe"
            if r.status_code != 200:
                time.sleep(2 ** i)
                continue
            with zipfile.ZipFile(io.BytesIO(r.content)) as z:
                with z.open(z.namelist()[0]) as f:
                    d = pd.read_csv(f)
            h = _a_horario(d)
            if h.empty and len(d):
                # bajo bien y no quedo nada: el formato cambio. Que se note, y que no
                # se cachee como si el dia no tuviera dato (paso con el 0,2 %).
                print(f"  FORMATO: {perp} {fecha} tiene {len(d)} filas y 0 snapshots "
                      f"completos — niveles {sorted(d['percentage'].unique())}",
                      file=sys.stderr, flush=True)
                return None, "fallo"
            return h, "ok"
        except Exception:
            time.sleep(2 ** i)
    return None, "fallo"


def _a_horario(d):
    """Snapshots de 30 s -> una fila por hora con el ULTIMO snapshot de esa hora."""
    # los archivos viejos traen el porcentaje como entero (`-5`), los nuevos como
    # `-5.00`: se pasa a float antes de nombrar, o `1` y `1.0` serian columnas distintas
    pct = d["percentage"].astype(float)
    d = d[pct.abs().round(2).isin(PROF)]
    pct = pct[d.index]
    ts = pd.to_datetime(d["timestamp"], utc=True, format="mixed").astype("int64") // 10**6
    d = d.assign(ts=ts.to_numpy(),
                 col=np.where(pct < 0, "b", "a")
                 + pct.abs().round(2).map(lambda x: str(float(x)).replace(".", "")))
    W = d.pivot_table(index="ts", columns="col", values="notional", aggfunc="last")
    W = W.reindex(columns=COLS)
    W = W.dropna(how="any")                       # snapshot incompleto: no existe
    W["t"] = (W.index // MS_H) * MS_H
    W["ts"] = W.index
    g = W.groupby("t")
    out = g[COLS].last()
    out["n_snap"] = g.size()
    # segundos entre el ultimo snapshot y el cierre de la vela: auditoria de frescura
    out["seg_al_cierre"] = (out.index + MS_H - g["ts"].last()) / 1000
    return out.reset_index()


def _mes(perp, fechas, workers):
    with ThreadPoolExecutor(workers) as ex:
        res = list(ex.map(lambda f: _dia(perp, f), fechas))
    fallos = [f for f, (_, e) in zip(fechas, res) if e == "fallo"]
    dias = [d for d, e in res if e == "ok" and d is not None and not d.empty]
    return (pd.concat(dias, ignore_index=True) if dias else None), fallos


def _resolver_perp(spot):
    """El perp que existe para este spot (prefijo 1000 en los de precio chico)."""
    for muestra in ("2024-06-01", "2025-06-01", "2026-06-01"):
        for p in perp_de(spot):
            r = requests.head(f"{BASE}/{p}/{p}-bookDepth-{muestra}.zip", timeout=30)
            if r.status_code == 200:
                return p
    return None


def frame_simbolo(spot, inicio=INICIO, fin=FIN_PRINCIPAL, workers=16, verbose=True):
    """DataFrame horario [t, b10, a10, b50, a50, n_snap, seg_al_cierre]."""
    os.makedirs(CACHE, exist_ok=True)
    perp = None
    piezas, faltan = [], 0
    meses = pd.period_range(inicio, pd.Timestamp(fin) - pd.Timedelta(days=1), freq="M")
    for m in meses:
        p = os.path.join(CACHE, f"{spot}_{m}.pkl")
        if os.path.exists(p):
            d = pd.read_pickle(p)
            if not d.empty:
                piezas.append(d)
            continue
        if perp is None:
            perp = _resolver_perp(spot)
            if perp is None:
                if verbose:
                    print(f"  {spot}: sin perpetuo con bookDepth", flush=True)
                return None
        fechas = [x.strftime("%Y-%m-%d") for x in
                  pd.date_range(max(m.start_time, pd.Timestamp(inicio)),
                                min(m.end_time, pd.Timestamp(fin) - pd.Timedelta(days=1)),
                                freq="D")]
        d, fallos = _mes(perp, fechas, workers)
        d = d if d is not None else pd.DataFrame(columns=["t"] + COLS)
        if fallos:
            faltan += len(fallos)
            if verbose:
                print(f"  {spot} {m}: {len(fallos)} dias fallaron (red o formato) — NO se "
                      f"cachea el mes, se re-pide la proxima vez", flush=True)
        else:
            d.to_pickle(p)
        if not d.empty:
            piezas.append(d)
    if not piezas:
        return None
    out = pd.concat(piezas, ignore_index=True).drop_duplicates("t").sort_values("t")
    out = out[(out["t"] >= pd.Timestamp(inicio, tz="UTC").value // 10**6)
              & (out["t"] < pd.Timestamp(fin, tz="UTC").value // 10**6)]
    out.attrs["dias_fallados"] = faltan
    return out.reset_index(drop=True)


def load_libro(syms, inicio=INICIO, fin=FIN_PRINCIPAL, workers=16, reserva=False,
               verbose=True):
    """{spot: DataFrame horario}. Los pares sin bookDepth quedan afuera del dict."""
    if pd.Timestamp(fin) > pd.Timestamp(FIN_PRINCIPAL) and not reserva:
        raise ValueError(f"fin={fin} entra en la RESERVA (desde {FIN_PRINCIPAL}). "
                         "PREREGISTRO_LIBRO §2: solo con reserva=True y solo si algo "
                         "sobrevivio a la ventana principal.")
    L = {}
    for k, s in enumerate(syms, 1):
        if verbose:
            print(f"libro {k}/{len(syms)} {s}...", flush=True)
        f = frame_simbolo(s, inicio, fin, workers=workers, verbose=verbose)
        if f is not None and not f.empty:
            L[s] = f
    if verbose:
        print(f"libro: {len(L)}/{len(syms)} pares con bookDepth")
    return L


# ------------------------------------------------------------------ features
FEATS = ["imb_1", "imb_5", "imb_1_m24", "imb_1_chg4", "imb_1_pct",
         "prof_1_rel", "prof_1_pct"]


def _feat(d):
    """Las 7 de PREREGISTRO_LIBRO §2 (8 menos `imb_02`, por la enmienda). Todo mira solo al pasado: la fila i usa hasta i.

    Se re-indexa a la grilla horaria COMPLETA antes de cualquier rolling: con huecos, un
    `rolling(24)` sobre filas no son 24 horas. Las horas sin snapshot quedan NaN y no se
    rellenan (las ventanas piden un minimo de horas presentes).
    """
    t = np.arange(int(d["t"].min()), int(d["t"].max()) + MS_H, MS_H)
    g = d.set_index("t").reindex(t)

    def imb(b, a):
        return (g[b] - g[a]) / (g[b] + g[a])

    out = pd.DataFrame(index=t)
    out["imb_1"] = imb("b10", "a10")
    out["imb_5"] = imb("b50", "a50")
    out["imb_1_m24"] = out["imb_1"].rolling(24, min_periods=18).mean()
    out["imb_1_chg4"] = out["imb_1"] - out["imb_1"].shift(4)
    out["imb_1_pct"] = out["imb_1"].rolling(720, min_periods=360).rank(pct=True)
    prof = g["b10"] + g["a10"]
    out["prof_1_rel"] = prof / prof.rolling(168, min_periods=84).median()
    out["prof_1_pct"] = prof.rolling(720, min_periods=360).rank(pct=True)
    return out.rename_axis("t").reset_index()


def feat_libro(L, T, verbose=True):
    """Una fila por entrada de T, alineada por (sym, t). Index = T.index.
    Misma firma que `metricas.feat_metricas`."""
    piezas = []
    for k, (sym, d) in enumerate(L.items(), 1):
        f = _feat(d)
        f.insert(0, "sym", sym)
        piezas.append(f)
        if verbose and k % 10 == 0:
            print(f"  feat libro {k}/{len(L)}...", flush=True)
    FULL = pd.concat(piezas, ignore_index=True)
    F = T[["sym", "t"]].merge(FULL, on=["sym", "t"], how="left")
    F.index = T.index
    return F.drop(columns=["sym", "t"])


# ------------------------------------------------------------------ auditoria
def conteo(L, inicio, fin):
    """§7: cobertura por par y por mes, y frescura del ultimo snapshot. Ningun retorno."""
    h_tot = (pd.Timestamp(fin) - pd.Timestamp(inicio)) // pd.Timedelta(hours=1)
    print("\n" + "=" * 78)
    print("COBERTURA — horas con snapshot completo / horas desde el primer archivo")
    print("=" * 78)
    print(f"{'par':<14}{'desde':>12}{'horas':>8}{'cob':>7}{'meses<90%':>11}"
          f"{'seg_al_cierre p50/p95':>24}{'fallos':>8}")
    flojos = []
    for s, d in sorted(L.items()):
        t0 = pd.to_datetime(d["t"].min(), unit="ms", utc=True)
        esp = (pd.Timestamp(fin, tz="UTC") - t0) // pd.Timedelta(hours=1)
        cob = len(d) / max(esp, 1)
        mes = pd.to_datetime(d["t"], unit="ms", utc=True).dt.tz_localize(None).dt.to_period("M")
        por_mes = mes.value_counts()
        horas_mes = {m: m.days_in_month * 24 for m in por_mes.index}
        bajos = sum(1 for m, n in por_mes.items() if n / horas_mes[m] < 0.90
                    and m != por_mes.index.min())
        sc = d["seg_al_cierre"].astype(float)
        print(f"{s:<14}{t0:%Y-%m-%d}{len(d):>8}{100*cob:>6.1f}%{bajos:>11}"
              f"{sc.median():>14.0f}/{sc.quantile(.95):>5.0f}"
              f"{d.attrs.get('dias_fallados', 0):>8}")
        if cob < 0.90:
            flojos.append(s)
    print(f"\n{len(L)} pares | horas de la ventana: {h_tot}")
    if flojos:
        print(f"HUECOS DE MAS DEL 10% (se declaran, no se rellenan): {', '.join(flojos)}")
    else:
        print("ningun par con huecos de mas del 10%.")


def main():
    ap = argparse.ArgumentParser(description="bookDepth historico -> panel horario")
    ap.add_argument("--solo-conteo", action="store_true",
                    help="bajar/leer y auditar cobertura. No calcula features ni mira "
                         "ningun retorno.")
    ap.add_argument("--pares", default=None, help="lista separada por comas (default: los 46)")
    ap.add_argument("--desde", default=INICIO)
    ap.add_argument("--hasta", default=FIN_PRINCIPAL, help="exclusivo")
    ap.add_argument("--workers", type=int, default=16)
    a = ap.parse_args()

    syms = a.pares.split(",") if a.pares else deriv46()
    L = load_libro(syms, a.desde, a.hasta, workers=a.workers)
    if a.solo_conteo:
        conteo(L, a.desde, a.hasta)


if __name__ == "__main__":
    main()
