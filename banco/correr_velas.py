"""
CORRIDA 7 — patrones de velas japonesas.

Preregistro con la regla de parada: `PREREGISTRO_VELAS.md`, escrito ANTES de calcular un
solo patron.

Un patron es un EVENTO, no un ranking, asi que el top-k de `ranking.py` no aplica. Pero su
ESTIMADOR si, y es el que hace falta:

    exceso(t) = media(y | simbolos donde disparo en t) - media(y | universo de t)
    semana(w) = media de exceso(t) sobre las barras de w
    estadistico = media de semana(w), cada semana pesando UNO

Eso neutraliza el TERMINO DE MERCADO, que en un test de patrones es el sesgo principal:
los patrones alcistas disparan mas en dias alcistas del mercado entero. `lote.py` aparea
por SIMBOLO y por eso nunca lo neutralizo.

Las barras se solapan (un patron dispara cuando dispara, no en una grilla), y eso es
deliberado: usar la grilla sin solape de `ranking.py` tiraria ~95% de los eventos. La
unidad independiente sigue siendo LA SEMANA, y el bootstrap de bloques semanales es lo
que da la inferencia. El solape infla el conteo dentro de la semana, no el estadistico
semanal.

    py -3.13 -u correr_velas.py --tf 1d
    py -3.13 -u correr_velas.py --tf 1h
"""
import argparse
import json
import os
import sys
import time

import numpy as np
import pandas as pd

import velas
from klines import CACHE, load_panel
from ranking import MIN_SYMS, SEM_OK, TOP_N, _bh, _p_bloques

INICIO, FIN = "2021-08-01", "2026-08-01"
COSTOS = (0.20, 0.50)
Q_FDR = 0.10
N_MIN_BARRAS = 200     # menos que esto -> "no se pudo medir", no "no esta"
SEM_MIN = 20

# las 16 de `base200` que no son cripto (regla de metodo de la corrida 5)
FUERA = {"USD1USDT", "RLUSDUSDT", "EURIUSDT", "FDUSDUSDT", "EURUSDT", "BFUSDUSDT",
         "XUSDUSDT", "QQQBUSDT", "SPCXBUSDT", "SPYBUSDT", "SNDKBUSDT", "CRCLBUSDT",
         "SKHYBUSDT", "AAPLBUSDT", "SNXXBUSDT", "NVDABUSDT", "USDCUSDT", "WBTCUSDT",
         "PAXGUSDT", "XAUTUSDT", "TUSDUSDT"}

# periodo de ATR y de su linea base, por resolucion. Los dos son "30 dias" en las dos.
PARAMS = {"1h": dict(atr=24, base=720, warmup=720, horizontes=(4, 24)),
          "1d": dict(atr=14, base=30, warmup=60, horizontes=(1, 3, 5))}


def tablero_eventos(panel, tf, horizonte):
    """Una fila por (simbolo, barra) con el retorno futuro normalizado. TODAS las barras."""
    P = PARAMS[tf]
    piezas = []
    for sym, df in panel.items():
        c = df["c"].to_numpy(float)
        h = df["h"].to_numpy(float)
        l = df["l"].to_numpy(float)
        n = len(c)
        if n < P["warmup"] + horizonte + 1:
            continue
        atr = (pd.Series((h - l) / c).rolling(P["atr"]).mean())
        base = atr.rolling(P["base"], min_periods=P["base"] // 4).median().to_numpy()
        ret = np.full(n, np.nan)
        ret[:n - horizonte] = c[horizonte:] / c[:n - horizonte] - 1.0

        d = pd.DataFrame({"sym": sym, "t": df["t"].to_numpy(),
                          "atr_base": base, "ret": ret})
        for k, m in velas.patrones(df).items():
            d[k] = m
        bajo, alto = velas.contexto(df)
        d["ctx_bajo"], d["ctx_alto"] = bajo, alto
        cu = np.abs(df["c"].to_numpy(float) - df["o"].to_numpy(float))
        rg = np.where(h - l > 0, h - l, np.nan)
        d["cuerpo_rel"] = cu / rg
        # invalidar el warmup SIN romper el dtype de las columnas booleanas: alcanza
        # con tirar `atr_base`, porque el filtro de abajo descarta esas filas enteras.
        d.loc[d.index[:P["warmup"]], "atr_base"] = np.nan
        piezas.append(d)

    TB = pd.concat(piezas, ignore_index=True)
    TB = TB[TB["ret"].notna() & TB["atr_base"].gt(0)]
    TB["y"] = TB["ret"] / TB["atr_base"]
    TB["dt"] = pd.to_datetime(TB["t"], unit="ms", utc=True)
    TB["semana"] = TB["dt"].dt.strftime("%G-W%V")
    vivos = TB.groupby("t")["y"].transform("count")
    TB = TB[vivos >= MIN_SYMS].reset_index(drop=True)
    print(f"  tablero {tf} H={horizonte}: {len(TB):,} filas | {TB['t'].nunique():,} barras "
          f"| {TB['sym'].nunique()} pares | {TB['semana'].nunique()} semanas")
    return TB


def _exceso(TB, mask, objetivo, costo):
    """Exceso semanal de las barras donde disparo, contra el universo de la MISMA barra."""
    signo = 1.0 if objetivo == "largo" else -1.0
    D = TB[["t", "sym", "semana", "atr_base", "y"]].copy()
    D["y"] = signo * D["y"]
    D["fire"] = mask.to_numpy()
    uni = D.groupby("t")["y"].mean()
    F = D[D["fire"]]
    if F.empty:
        return None, None, None, 0
    top = F.groupby("t")["y"].mean()
    if costo:
        top = top - (costo / 100.0) / F.groupby("t")["atr_base"].median()
    por_barra = (top - uni.reindex(top.index)).dropna()
    if por_barra.empty:
        return None, None, None, 0

    # aporte por simbolo, para el chequeo de concentracion
    E = F.copy()
    E["exceso"] = E["y"].to_numpy() - uni.reindex(E["t"]).to_numpy()
    aporte = E.groupby("sym")["exceso"].sum().sort_values(ascending=False)

    sem_de = D.groupby("t")["semana"].first()
    sem = por_barra.groupby(sem_de.reindex(por_barra.index)).mean()
    # version cruda (sin normalizar por ATR), para el chequeo de artefacto de escala
    Dc = TB[["t", "semana", "ret"]].copy()
    Dc["ret"] = signo * Dc["ret"]
    Dc["fire"] = mask.to_numpy()
    uc = Dc.groupby("t")["ret"].mean()
    tc = Dc[Dc["fire"]].groupby("t")["ret"].mean()
    if costo:
        tc = tc - costo / 100.0
    crudo = (tc - uc.reindex(tc.index)).dropna()
    sem_c = crudo.groupby(sem_de.reindex(crudo.index)).mean()
    return sem, sem_c, aporte, int(F.shape[0])


def evaluar(TB, mask, nombre, objetivo, costo):
    fila = {"patron": nombre, "objetivo": objetivo}
    sem, sem_c, aporte, n = _exceso(TB, mask, objetivo, costo)
    fila["disparos"] = n
    if sem is None or n < N_MIN_BARRAS or len(sem) < SEM_MIN:
        fila.update(semanas=0 if sem is None else len(sem), exceso=np.nan, p=1.0,
                    veredicto=f"NO SE PUDO MEDIR (n={n}, sem={0 if sem is None else len(sem)})")
        return fila
    fila["semanas"] = int(len(sem))
    fila["exceso"] = float(sem.mean())
    fila["sd_sem"] = float(sem.std(ddof=1))
    fila["sem_ok"] = float((sem > 0).mean())
    fila["p"] = _p_bloques(sem) if sem.mean() > 0 else 1.0
    fila["crudo"] = float(sem_c.mean())
    for etiq, cuantos in (("sin_top3", TOP_N), ("sin_top1", 1)):
        fuera = set(aporte.head(cuantos).index)
        sub = TB["sym"].isin(fuera)
        s2, _, _, _ = _exceso(TB[~sub], mask[~sub], objetivo, costo)
        fila[etiq] = float(s2.mean()) if s2 is not None and len(s2) else np.nan
    return fila


def brazos(TB, semilla=0):
    """{nombre: mascara}. Los patrones x contexto, mas los controles del preregistro."""
    B = {}
    for k in velas.DIRECCION:
        m = TB[k].astype(bool)
        B[k] = m
        # contexto: reversion pide tendencia contraria, continuacion pide a favor
        d = velas.DIRECCION[k]
        if k in velas.REVERSION or d is None:
            ctx = TB["ctx_bajo"] if d != "corto" else TB["ctx_alto"]
        else:
            ctx = TB["ctx_alto"] if d == "largo" else TB["ctx_bajo"]
        B[f"{k} +ctx"] = m & ctx.astype(bool)
    # CONTROLES declarados en §3.5
    B["CTRL contexto solo (baja previa)"] = TB["ctx_bajo"].astype(bool)
    B["CTRL contexto solo (suba previa)"] = TB["ctx_alto"].astype(bool)
    B["CTRL cuerpo q80"] = TB["cuerpo_rel"] >= TB["cuerpo_rel"].quantile(0.80)
    rng = np.random.default_rng(semilla)
    tasa = float(np.mean([TB[k].mean() for k in velas.DIRECCION]))
    for i in range(3):
        B[f"CONTROL azar {i+1}"] = pd.Series(
            rng.random(len(TB)) < tasa, index=TB.index)
    return B


def main():
    ap = argparse.ArgumentParser(description="Banco — corrida 7: patrones de velas")
    ap.add_argument("--tf", default="1d", choices=["1d", "1h"])
    ap.add_argument("--workers", type=int, default=12)
    ap.add_argument("--out", default=None)
    a = ap.parse_args()

    with open(os.path.join(CACHE, "universo_base200.json"), encoding="utf-8") as f:
        syms = [s for s in json.load(f) if s not in FUERA]
    print(f"universo: {len(syms)} pares (base200 menos {len(FUERA)} que no son cripto)")

    mb = 400 if a.tf == "1d" else 8000
    panel = load_panel(INICIO, FIN, tf=a.tf, full=True, workers=a.workers,
                       syms=syms, min_bars=mb)
    if not panel:
        print("FATAL: panel vacio"); sys.exit(1)

    t0 = time.time()
    filas = []
    for H in PARAMS[a.tf]["horizontes"]:
        TB = tablero_eventos(panel, a.tf, H)
        B = brazos(TB)
        for costo in COSTOS:
            for nom, m in B.items():
                for obj in ("largo", "corto"):
                    r = evaluar(TB, m, nom, obj, costo)
                    r.update(tf=a.tf, horizonte=H, costo=costo)
                    filas.append(r)
            print(f"    H={H} costo {costo:.2f} listo ({time.time()-t0:.0f}s)", flush=True)

    D = pd.DataFrame(filas)
    # FDR sobre el LOTE ENTERO: todos los patrones x contextos x direcciones x horizontes
    vivas = ~D["veredicto"].notna() if "veredicto" in D else pd.Series(True, index=D.index)
    D["fdr_ok"] = False
    if vivas.any():
        D.loc[vivas, "fdr_ok"] = _bh(D.loc[vivas, "p"].to_numpy(), Q_FDR)

    ctrl = D[D["patron"].str.startswith("CONTROL")]
    mde = 2.80 * float(ctrl["sd_sem"].median()) / np.sqrt(float(ctrl["semanas"].median()))
    print(f"\nMDE del azar (80% de potencia): ±{mde:.4f} ATR")

    def veredicto(r):
        if isinstance(r.get("veredicto"), str):
            return r["veredicto"]
        if r["patron"].startswith("CONTROL"):
            return "control"
        if not (r["exceso"] > 0):
            return "exceso <= 0"
        if not (r["crudo"] > 0):
            return "ARTEFACTO DE ESCALA"
        if abs(r["exceso"]) < mde:
            return f"dentro del MDE del azar (±{mde:.3f})"
        if not r["fdr_ok"]:
            return f"muere en la correccion (FDR q={Q_FDR})"
        if not (r["sin_top3"] > 0):
            return f"concentracion: se cae sin el top-{TOP_N}"
        if not (r["sin_top1"] > 0):
            return "un solo par lo sostiene"
        if not (r["sem_ok"] >= SEM_OK):
            return f"inconsistente por semana ({100*r['sem_ok']:.0f}%)"
        return "SOBREVIVE"

    D["veredicto"] = D.apply(veredicto, axis=1)
    out = a.out or f"velas_{a.tf}.csv"
    D.to_csv(out, index=False)

    print("\n" + "=" * 100)
    print(f"PATRONES DE VELAS — {a.tf} | {len(D)} brazos | control POR BARRA")
    print("=" * 100)
    for (H, c), g in D.groupby(["horizonte", "costo"]):
        g = g[~g["patron"].str.startswith("CONTROL")]
        viv = (g.veredicto == "SOBREVIVE").sum()
        print(f"  H={H:<3d} costo {c:.2f}: {viv:2d} sobreviven de {len(g)} | "
              f"exceso>0 en {(g.exceso > 0).sum():3d} | mejor {g.exceso.max():+.4f}")
    viven = D[D.veredicto == "SOBREVIVE"]
    print(f"\nSOBREVIVEN {len(viven)} de {(~D.patron.str.startswith('CONTROL')).sum()}")
    if len(viven):
        print(viven[["patron", "objetivo", "horizonte", "costo", "disparos",
                     "exceso", "sin_top3", "sem_ok", "p"]].to_string(index=False))
    print(f"\n-> {out} | {time.time()-t0:.0f}s")


if __name__ == "__main__":
    main()
