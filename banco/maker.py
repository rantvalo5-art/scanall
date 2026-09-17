"""
MAKER - seleccion adversa y spread realizado. Item 4.1 de `HANDOFF_SIGUIENTE.md`.

`libro.py` midio cuanto cuesta CRUZAR el spread. Este mide lo contrario: cuanto
cobraria quien lo pone. Poner ordenes limite invierte el signo del spread, y es la
unica forma NO DIRECCIONAL y CON MECANISMO que le queda al repo (ver seccion 2.2 del
handoff: diez familias direccionales en cero, y lo unico que funciono fue vender
volatilidad, que es no direccional).

Lo que lo mata es la SELECCION ADVERSA: al maker lo llenan cuando el otro tiene razon.
La metrica que decide es el SPREAD REALIZADO, para cada fill:

    d     = +1 si el agresor compro (`m == false`, contra el ask)
            -1 si el agresor vendio (`m == true`, contra el bid)
    maker = el lado opuesto, o sea posicion `-d`

    RS(D) = s/2 - d*(mid(t+D) - mid(t))   =   d * (p - mid(t+D))

Las dos formas son identicas. Se computa la SEGUNDA a proposito: no necesita el mid en
el instante del fill, que es justo donde un mid reconstruido de trades esta mas sesgado
(el lado que acaba de imprimir esta fresco y el otro rancio). Solo necesita el precio
del fill, su lado, y un mid FUTURO evaluado en un instante que NO esta condicionado a
un trade — ahi la rancidez es simetrica.

La headline esta BALANCEADA POR LADO: promedio de la media sobre fills de compra del
maker y la media sobre fills de venta. Una deriva del periodo entra con signo opuesto en
cada lado, asi que balancear la cancela. El pooleado se reporta al lado como
diagnostico, no como veredicto — este repo ya se engano varias veces confundiendo
deriva del periodo con captura de spread.

`RS` es BRUTO. La vara es el fee de maker, que es un dato conocido y no se estima. La
regla de parada exacta esta en `PREREGISTRO_MAKER.md`, escrita antes de bajar nada.

    py -3.13 -u maker.py                    # corrida completa
    py -3.13 -u maker.py --solo-conteo      # n y MDE, sin estimar (paso previo)
"""
import argparse
import json
import os
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone

import numpy as np
import pandas as pd

from klines import SPOT, _get, to_ms
from libro import universo_por_volumen

HERE = os.path.dirname(os.path.abspath(__file__))
CACHE = os.path.join(HERE, ".aggtrades_cache")

# fees de maker de Binance VIP 0, % por lado. DATO, no estimacion.
FEE = {"spot": 0.1000, "spot_bnb": 0.0750, "futuros": 0.0200}
FEE_DECIDE = "spot_bnb"          # el mas favorable de los dos spot: si no alcanza ahi...

DELTAS = [1, 10, 60, 300]        # segundos
D_DECIDE = 60                    # el unico que decide; el resto es perfil de decaimiento
COLA_S = 310                     # cola extra por ventana, para poder evaluar D=300

# 5 posiciones fijas dentro de cada banda de `libro.py`. La ultima banda es 401-485 y no
# 401-600 como decia el preregistro: el universo USDT spot VIVO tiene 485 pares, no 600
# (`libro.py --top 600` devuelve lo que hay). Ajuste mecanico hecho ANTES de ver un solo
# RS; las tres primeras bandas quedan exactamente como estaban preregistradas.
BANDAS = [("1-50", [1, 10, 20, 35, 50]),
          ("51-200", [60, 90, 120, 150, 190]),
          ("201-400", [210, 250, 300, 350, 395]),
          ("401-485", [410, 430, 450, 465, 480])]

# pares donde el activo base es otra stablecoin: estan pegados, el spread es un tick y no
# hay seleccion adversa que medir. No se sacan de la seleccion (las posiciones son fijas y
# cambiarlas despues de ver quien salio seria elegir), pero se marcan y la mediana por
# banda se reporta con y sin ellos.
ESTABLES = {"USDC", "USD1", "RLUSD", "FDUSD", "TUSD", "BUSD", "DAI", "USDP", "PYUSD",
            "EURI", "USDE", "XUSD", "AEUR", "EUR", "GBP", "TRY", "BRL", "ARS", "JPY"}


# --------------------------------------------------------------------------- universo

def seleccionar(pin="maker20", top=600):
    """20 pares, 5 por banda, en posiciones fijas dentro de cada banda.

    Se congela en disco: el ranking de volumen es en vivo y dos corridas separadas por
    horas devuelven listas distintas (ver `klines._universo_fijo`).
    """
    os.makedirs(CACHE, exist_ok=True)
    path = os.path.join(CACHE, f"seleccion_{pin}.json")
    if os.path.exists(path):
        with open(path, encoding="utf-8") as f:
            return pd.DataFrame(json.load(f))
    # min_usd=0: la cola iliquida es justo donde el market making podria pagar, asi que
    # no se recorta por volumen. `libro.py` usa 50k por default y por eso su rank difiere
    # un poco del de aca en la cola.
    U = universo_por_volumen(top, min_usd=0)
    filas = []
    for banda, ranks in BANDAS:
        for r in ranks:
            if r <= len(U):
                fila = U.iloc[r - 1]
                sym = fila["sym"]
                filas.append(dict(sym=sym, rank=int(fila["rank"]),
                                  qv24=float(fila["qv24"]), banda=banda,
                                  estable=sym[:-4] in ESTABLES))
    S = pd.DataFrame(filas)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(S.to_dict("records"), f, indent=1)
    return S


def ventanas(inicio, fin, por_dia=3, largo_min=10, seed=7):
    """Instantes de arranque pseudoaleatorios, `por_dia` por cada dia UTC de la ventana.

    No hace falta la serie contigua: hace falta n y bloques independientes. El dia es
    el bloque (14 dias = 14 bloques), que es la unidad que decide en este repo.
    """
    rng = np.random.default_rng(seed)
    d0 = datetime.strptime(inicio, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    d1 = datetime.strptime(fin, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    out = []
    dia = d0
    while dia < d1:
        # el ultimo arranque tiene que dejar entrar la ventana + la cola dentro del dia
        tope = 86400 - largo_min * 60 - COLA_S
        for off in np.sort(rng.integers(0, tope, por_dia)):
            t0 = int((dia.timestamp() + int(off)) * 1000)
            out.append((dia.strftime("%Y-%m-%d"), t0,
                        t0 + largo_min * 60 * 1000,
                        t0 + (largo_min * 60 + COLA_S) * 1000))
        dia += timedelta(days=1)
    return out


# --------------------------------------------------------------------------- descarga

def _bajar(sym, t0, t1):
    """aggTrades de [t0, t1) paginando por fromId. Devuelve dict de arrays o None."""
    filas, cursor_id = [], None
    while True:
        if cursor_id is None:
            # el endpoint exige rango < 1h; nuestras ventanas son de ~15 min
            d = _get(f"{SPOT}/api/v3/aggTrades",
                     {"symbol": sym, "startTime": t0, "endTime": t1, "limit": 1000})
        else:
            d = _get(f"{SPOT}/api/v3/aggTrades",
                     {"symbol": sym, "fromId": cursor_id, "limit": 1000})
        if d is None:
            return None
        if not d:
            break
        crudo = len(d)
        d = [r for r in d if t0 <= r["T"] < t1]
        filas.extend(d)
        if crudo < 1000 or not d:
            break                       # la pagina no se lleno: el rango se agoto
        cursor_id = d[-1]["a"] + 1
    if not filas:
        return dict(T=np.zeros(0, "int64"), p=np.zeros(0), q=np.zeros(0),
                    m=np.zeros(0, bool), f=np.zeros(0, "int64"), l=np.zeros(0, "int64"))
    filas = {r["a"]: r for r in filas}
    filas = [filas[k] for k in sorted(filas)]
    return dict(T=np.array([r["T"] for r in filas], "int64"),
                p=np.array([float(r["p"]) for r in filas]),
                q=np.array([float(r["q"]) for r in filas]),
                m=np.array([bool(r["m"]) for r in filas]),
                f=np.array([r["f"] for r in filas], "int64"),
                l=np.array([r["l"] for r in filas], "int64"))


def ventana_cacheada(sym, t0, t_fin):
    os.makedirs(CACHE, exist_ok=True)
    path = os.path.join(CACHE, f"{sym}_{t0}_{t_fin}.npz")
    if os.path.exists(path):
        try:
            z = np.load(path)
            return {k: z[k] for k in ("T", "p", "q", "m", "f", "l")}
        except Exception:
            pass
    d = _bajar(sym, t0, t_fin)
    if d is None:
        return None
    np.savez_compressed(path, **d)
    return d


# ------------------------------------------------------------------------- reconstruir

def _ffill(vals, mask):
    """Ultimo `vals[i]` con `mask[i]` verdadero, hacia adelante. NaN antes del primero."""
    n = len(vals)
    idx = np.where(mask, np.arange(n), -1)
    np.maximum.accumulate(idx, out=idx)
    out = np.full(n, np.nan)
    ok = idx >= 0
    out[ok] = vals[idx[ok]]
    return out


def _primer_del_barrido(T, f, l, d):
    """True para el primer aggTrade de cada barrido de un mismo agresor.

    Una orden taker que come varios niveles imprime varios aggTrades con ids de trade
    CONSECUTIVOS, mismo lado y mismo milisegundo. El maker que esta en la mejor punta
    solo se lleva el primero; los profundos son los mas adversamente seleccionados. La
    variante que se queda con el primero es la FAVORABLE al maker, por eso se reporta
    junto con la version completa y las dos tienen que dar el mismo veredicto.
    """
    n = len(T)
    if n == 0:
        return np.zeros(0, bool)
    cont = np.zeros(n, bool)
    cont[1:] = (f[1:] == l[:-1] + 1) & (d[1:] == d[:-1]) & (T[1:] == T[:-1])
    return ~cont


def procesar(dat, t_fin_fills):
    """Una ventana -> DataFrame de fills con RS para cada delta y cada estimador de mid."""
    T, p, q, m = dat["T"], dat["p"], dat["q"], dat["m"]
    n = len(T)
    if n < 10:
        return None
    d = np.where(m, -1.0, 1.0)                 # m=True: agresor vendedor -> d=-1

    bid = _ffill(p, m)                         # ultima impresion contra el bid
    ask = _ffill(p, ~m)                        # ultima impresion contra el ask
    mid_bi = (bid + ask) / 2.0                 # estimador 1: midpoint de las dos puntas
    mid_ul = p                                 # estimador 2: ultimo precio transado

    # mid PREVIO al fill, solo para el spread efectivo (diagnostico, no decide)
    mid_pre = np.full(n, np.nan)
    mid_pre[1:] = mid_bi[:-1]

    fills = np.where(T <= t_fin_fills)[0]
    if len(fills) == 0:
        return None
    primero = _primer_del_barrido(T, dat["f"], dat["l"], d)

    out = dict(T=T[fills], p=p[fills], q=q[fills], d=d[fills],
               usd=p[fills] * q[fills], primero=primero[fills])
    with np.errstate(invalid="ignore", divide="ignore"):
        out["s_eff"] = 2 * d[fills] * (p[fills] - mid_pre[fills]) / mid_pre[fills] * 100

    t_ultimo = T[-1]
    for D in DELTAS:
        obj = T[fills] + D * 1000
        j = np.searchsorted(T, obj, side="right") - 1
        vale = (obj <= t_ultimo) & (j >= 0)
        for nom, mid in (("bi", mid_bi), ("ul", mid_ul)):
            ref = np.where(vale, mid[np.clip(j, 0, n - 1)], np.nan)
            with np.errstate(invalid="ignore", divide="ignore"):
                out[f"rs{D}_{nom}"] = d[fills] * (p[fills] - ref) / ref * 100
    return pd.DataFrame(out)


# ------------------------------------------------------------------------- agregacion

def _bal(g, col):
    """Media balanceada por lado: promedio de la media de compras y la de ventas."""
    v = g[col].to_numpy()
    lado = g["d"].to_numpy()
    ok = np.isfinite(v)
    if not ok.any():
        return np.nan
    v, lado = v[ok], lado[ok]
    a, b = v[lado < 0], v[lado > 0]           # maker compra (d=-1) / maker vende (d=+1)
    if len(a) == 0 or len(b) == 0:
        return np.nan
    return (a.mean() + b.mean()) / 2.0


def _bal_usd(g, col):
    v, lado, w = g[col].to_numpy(), g["d"].to_numpy(), g["usd"].to_numpy()
    ok = np.isfinite(v) & np.isfinite(w) & (w > 0)
    if not ok.any():
        return np.nan
    v, lado, w = v[ok], lado[ok], w[ok]
    out = []
    for sel in (lado < 0, lado > 0):
        if not sel.any() or w[sel].sum() <= 0:
            return np.nan
        out.append(float(np.average(v[sel], weights=w[sel])))
    return sum(out) / 2.0


def por_dia(F, col):
    """Una fila por (sym, dia) con RS balanceado. El dia es el BLOQUE."""
    filas = []
    for (s, dia), g in F.groupby(["sym", "dia"], sort=True):
        filas.append(dict(sym=s, dia=dia, n=len(g),
                          rs=_bal(g, col), rs_usd=_bal_usd(g, col)))
    return pd.DataFrame(filas)


def mde(x, alfa_z=2.80):
    """Minimo efecto detectable al 80% de potencia sobre la media de los bloques."""
    x = np.asarray([v for v in x if np.isfinite(v)])
    if len(x) < 3:
        return np.nan
    return float(alfa_z * x.std(ddof=1) / np.sqrt(len(x)))


def p_bloques(D, dias, fee, reps=4000, seed=0):
    """p de que la MEDIANA sobre pares del RS balanceado supere el fee.

    Remuestrea DIAS ENTEROS (14 bloques), los mismos para todos los pares en cada rep,
    asi la correlacion entre pares se conserva. Cada dia pesa uno: poolear los fills
    haria que los dias activos pesen mas y subestima la variabilidad (el error que en
    `fade/evaluar.py` dio vuelta un veredicto).
    """
    piv = D.pivot_table(index="dia", columns="sym", values="rs", aggfunc="mean")
    piv = piv.reindex(dias)
    if piv.shape[0] < 8:
        return 1.0
    A = piv.to_numpy()
    rng = np.random.default_rng(seed)
    k = A.shape[0]
    med = np.empty(reps)
    for i in range(reps):
        sel = rng.integers(0, k, k)
        med[i] = np.nanmedian(np.nanmean(A[sel], axis=0))
    return float((med <= fee).mean())


# ------------------------------------------------------------- diagnostico de la cola

def cola_nivel1(sym, reps=3, espera=8):
    """Notional en USD parado en la MEJOR punta, mediana de varios snapshots.

    Es el numero que falta para saber si un fill es alcanzable. `RS` mide cuanto paga
    un fill; esto mide cuantos fills hay. Cuando el spread esta clavado en UN TICK
    —caso tipico de las monedas de precio chico, donde el tick vale 0,1% o mas— la cola
    es enorme y el maker que llega ultimo no cobra ese spread: lo cobran los que ya
    estaban. Ahi el unico fill que le toca es el que atraviesa el nivel, o sea
    seleccion adversa pura.
    """
    bids, asks, spr = [], [], []
    for i in range(reps):
        d = _get(f"{SPOT}/api/v3/depth", {"symbol": sym, "limit": 5})
        if d and d.get("bids") and d.get("asks"):
            b, a = d["bids"][0], d["asks"][0]
            pb, pa = float(b[0]), float(a[0])
            bids.append(pb * float(b[1]))
            asks.append(pa * float(a[1]))
            spr.append((pa - pb) / ((pa + pb) / 2) * 100)
        if i < reps - 1:
            time.sleep(espera)
    if not bids:
        return None
    return dict(sym=sym, q1_bid=float(np.median(bids)), q1_ask=float(np.median(asks)),
                spread_libro=float(np.median(spr)))


def ticks(syms):
    """tickSize / precio, en %. El spread NO puede ser mas angosto que un tick.

    En las monedas de precio chico el tick vale 0,1% o mas, asi que el spread queda
    CLAVADO en un tick y se ve ancho sin que el par sea iliquido. Es la explicacion
    candidata de los unicos pares donde `RS` cruza el fee.
    """
    ei = _get(f"{SPOT}/api/v3/exchangeInfo")
    tk = _get(f"{SPOT}/api/v3/ticker/price")
    if not ei or not tk:
        return {}
    px = {d["symbol"]: float(d["price"]) for d in tk}
    out = {}
    for d in ei["symbols"]:
        if d["symbol"] not in syms:
            continue
        for f in d["filters"]:
            if f["filterType"] == "PRICE_FILTER":
                t, p0 = float(f["tickSize"]), px.get(d["symbol"], 0)
                if t > 0 and p0 > 0:
                    out[d["symbol"]] = t / p0 * 100
    return out


def diagnostico_cola(F, S, min_muestreados):
    """Cruza el spread reconstruido con el del libro y estima la rotacion de la cola."""
    print()
    print("=" * 92)
    print("DIAGNOSTICO DE LA COLA: cuantos fills hay, no cuanto paga cada fill")
    print("=" * 92)
    filas = []
    with ThreadPoolExecutor(6) as ex:
        for f in as_completed([ex.submit(cola_nivel1, s) for s in S.sym]):
            r = f.result()
            if r:
                filas.append(r)
    Q = pd.DataFrame(filas).set_index("sym")

    TK = ticks(set(S.sym))
    RS_ = pd.read_csv("maker.csv").set_index("sym")
    print(f"{'par':14s} {'banda':9s} {'tick %':>8s} {'spr libro':>10s} {'spr eff':>9s} "
          f"{'spr/tick':>9s} {'cola L1 $':>11s} {'flujo $/dia':>12s} {'rotac/dia':>10s} "
          f"{'techo $/dia':>11s}")
    out = []
    for r in S.itertuples():
        g = F[F.sym == r.sym]
        if r.sym not in Q.index or not len(g):
            continue
        q = Q.loc[r.sym]
        cola = (q.q1_bid + q.q1_ask) / 2
        # los fills muestreados son `min_muestreados` minutos por dia; se extrapola a 1440
        flujo = float(g["usd"].sum()) / g["dia"].nunique() / min_muestreados * 1440
        rot = flujo / cola / 2 if cola > 0 else np.nan   # /2: el flujo se parte en 2 puntas
        s_eff = float(g["s_eff"].median())
        tick = TK.get(r.sym, np.nan)
        # techo de capacidad: el neto por fill CONTRA EL FEE, cobrado sobre el 100% del
        # flujo del dia. Es un techo imposible (nadie es el maker de todos los trades),
        # y sirve para eso: si el techo ya es chico, el par no da ni en el mejor caso.
        neto = (float(RS_.loc[r.sym, "rs"]) - FEE[FEE_DECIDE]) / 100 if r.sym in RS_.index else np.nan
        techo = neto * flujo
        out.append(dict(sym=r.sym, banda=r.banda, tick=tick, spread_libro=q.spread_libro,
                        s_eff=s_eff, cola=cola, flujo=flujo, rotacion=rot,
                        neto_pct=neto * 100, techo=techo))
        print(f"{r.sym:14s} {r.banda:9s} {tick:7.4f}% {q.spread_libro:9.4f}% "
              f"{s_eff:8.4f}% {(s_eff/tick if tick else np.nan):9.2f} "
              f"{cola:11,.0f} {flujo:12,.0f} {rot:10,.1f} {techo:+11,.0f}")
    D = pd.DataFrame(out)
    D.to_csv("maker_cola.csv", index=False)

    print()
    print("  validacion del spread (el libro es de HOY, los trades de la ventana):")
    ok = D[(D.spread_libro > 0) & (D.s_eff > 0)]
    if len(ok):
        rel = (ok.s_eff / ok.spread_libro).median()
        print(f"    spread efectivo / spread cotizado, mediana = {rel:.2f}x  "
              f"({'coherente: el efectivo entra dentro del cotizado' if 0.3 <= rel <= 1.3 else 'INCOHERENTE - revisar la reconstruccion'})")
    print()
    print("  rotacion = veces por dia que se da vuelta la cola de la mejor punta.")
    print("    < 1 significa que una orden nueva al final de la cola practicamente no se")
    print("    llena por agotamiento; el unico fill que le toca es el que atraviesa el")
    print("    nivel, que es seleccion adversa pura.")
    print(f"    pares con rotacion < 1: {int((D.rotacion < 1).sum())}/{len(D)}   "
          f"< 5: {int((D.rotacion < 5).sum())}/{len(D)}")
    print()
    print("  el tick como explicacion: spread/tick ~ 1 significa clavado en el tick.")
    clav = D[(D.s_eff / D.tick) < 2.5]
    print(f"    pares con el spread clavado en <2,5 ticks: {len(clav)}/{len(D)}  "
          f"-> neto mediano {clav.neto_pct.median():+.4f} pp")
    otros = D[(D.s_eff / D.tick) >= 2.5]
    print(f"    el resto:                                  {len(otros)}/{len(D)}  "
          f"-> neto mediano {otros.neto_pct.median():+.4f} pp")
    print()
    print(f"  techo de capacidad (100% del flujo, imposible): "
          f"suma de los pares con techo>0 = ${D[D.techo > 0].techo.sum():,.0f}/dia "
          f"repartido en {int((D.techo > 0).sum())} pares")
    return D


# ------------------------------------------------------------------------------- main

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--inicio", default="2026-08-11")
    ap.add_argument("--fin", default="2026-08-25")
    ap.add_argument("--por-dia", type=int, default=3)
    ap.add_argument("--largo-min", type=int, default=10)
    ap.add_argument("--workers", type=int, default=6)
    ap.add_argument("--cola", action="store_true",
                    help="diagnostico: cola de nivel 1 del libro vs flujo, y validacion "
                         "del spread reconstruido contra `/api/v3/depth`")
    ap.add_argument("--solo-conteo", action="store_true",
                    help="baja y cuenta n + MDE, sin estimar RS (el paso previo)")
    ap.add_argument("--csv", default="maker.csv")
    a = ap.parse_args()

    S = seleccionar()
    V = ventanas(a.inicio, a.fin, a.por_dia, a.largo_min)
    dias = sorted({v[0] for v in V})
    print(f"pares: {len(S)} | ventana FIJA {a.inicio} -> {a.fin} ({len(dias)} dias)")
    print(f"ventanas: {len(V)} por par ({a.por_dia}/dia x {a.largo_min} min "
          f"+ {COLA_S}s de cola) = {len(S)*len(V)} descargas")
    print(S.to_string(index=False,
                      formatters={"qv24": lambda x: f"{x:,.0f}"}), flush=True)

    tareas = [(r.sym, dia, t0, t_core, t_fin)
              for r in S.itertuples() for (dia, t0, t_core, t_fin) in V]
    print(f"\nbajando {len(tareas)} ventanas ({a.workers} workers)...", flush=True)

    partes, t0w, hechas, vacias = [], time.time(), [0], [0]

    def _una(t):
        sym, dia, ini, core, fin = t
        dat = ventana_cacheada(sym, ini, fin)
        if dat is None or len(dat["T"]) < 10:
            return sym, dia, None
        F = procesar(dat, core)
        return sym, dia, F

    with ThreadPoolExecutor(a.workers) as ex:
        for fut in as_completed([ex.submit(_una, t) for t in tareas]):
            sym, dia, F = fut.result()
            hechas[0] += 1
            if F is None or not len(F):
                vacias[0] += 1
            else:
                F["sym"], F["dia"] = sym, dia
                partes.append(F)
            if hechas[0] % 100 == 0:
                print(f"  {hechas[0]}/{len(tareas)}  ({time.time()-t0w:.0f}s, "
                      f"{vacias[0]} vacias)", flush=True)

    if not partes:
        print("\nsin datos: no se bajo ni un fill.")
        return
    F = pd.concat(partes, ignore_index=True)
    F = F.join(S.set_index("sym")[["rank", "banda"]], on="sym")
    print(f"\n{len(F):,} fills | {vacias[0]}/{len(tareas)} ventanas vacias "
          f"| {time.time()-t0w:.0f}s")

    # ---------------------------------------------------- CONTEO Y MDE, antes de estimar
    col = f"rs{D_DECIDE}_bi"
    D = por_dia(F, col)
    fee = FEE[FEE_DECIDE]
    print("\n" + "=" * 92)
    print(f"CONTEO POST-JOIN Y MDE (antes de estimar) | fee que decide: "
          f"{FEE_DECIDE} = {fee:.4f}%")
    print("=" * 92)
    print(f"{'par':14s} {'banda':9s} {'fills':>9s} {'dias':>5s} {'fills/dia':>10s} "
          f"{'MDE 80%':>9s}  potencia")
    conteo = []
    for r in S.itertuples():
        g = D[D.sym == r.sym]
        nf = int(F[F.sym == r.sym].shape[0])
        m = mde(g["rs"].to_numpy()) if len(g) else np.nan
        # un par decide algo solo si el MDE es menor a la distancia tipica al fee
        tiene = np.isfinite(m) and m < fee
        conteo.append(dict(sym=r.sym, banda=r.banda, fills=nf, dias=len(g),
                           mde=m, potencia=bool(tiene)))
        print(f"{r.sym:14s} {r.banda:9s} {nf:9,d} {len(g):5d} "
              f"{(nf/max(len(g),1)):10,.0f} {m:9.4f}  "
              f"{'si' if tiene else 'NO (no puede decidir)'}")
    C = pd.DataFrame(conteo)
    n_pot = int(C.potencia.sum())
    print(f"\n  pares con potencia para distinguirse del fee: {n_pot}/{len(C)}")
    if n_pot < len(C) // 2:
        print("  OJO: menos de la mitad tiene potencia. Un resultado global aca es "
              "'no se pudo medir', no 'no esta'.")
    if a.solo_conteo:
        C.to_csv("maker_conteo.csv", index=False)
        print("\n-> maker_conteo.csv  (corrida de conteo, sin estimar)")
        return

    # -------------------------------------------------------------- spread reconstruido
    print("\n" + "=" * 92)
    print("VALIDACION: spread efectivo reconstruido de los trades (vs `libro.py`)")
    print("=" * 92)
    sp = F.groupby("banda", observed=True)["s_eff"].median()
    for b, _ in BANDAS:
        if b in sp.index:
            print(f"  {b:9s} spread efectivo mediano {sp[b]:7.4f}%   "
                  f"(medio-spread {sp[b]/2:7.4f}%)")

    # ------------------------------------------------------------------ RS por par/banda
    print("\n" + "=" * 92)
    print(f"SPREAD REALIZADO RS({D_DECIDE}s) BALANCEADO POR LADO, mid bipunta")
    print("=" * 92)
    print(f"{'par':14s} {'banda':9s} {'s/2':>8s} {'RS':>9s} {'sel.adv':>9s} "
          f"{'RS-fee':>9s} {'RS_usd':>9s} {'dias+':>7s}")
    filas = []
    for r in S.itertuples():
        g = D[D.sym == r.sym]
        if not len(g):
            continue
        rs = float(np.nanmean(g["rs"])) if g["rs"].notna().any() else np.nan
        rsu = float(np.nanmean(g["rs_usd"])) if g["rs_usd"].notna().any() else np.nan
        half = float(F[F.sym == r.sym]["s_eff"].median()) / 2
        arriba = float((g["rs"] > fee).mean() * 100)
        filas.append(dict(sym=r.sym, banda=r.banda, rank=r.rank, half=half, rs=rs,
                          adv=half - rs, margen=rs - fee, rs_usd=rsu, dias_arriba=arriba))
        print(f"{r.sym:14s} {r.banda:9s} {half:8.4f} {rs:9.4f} {half-rs:9.4f} "
              f"{rs-fee:+9.4f} {rsu:9.4f} {arriba:6.0f}%")
    R = pd.DataFrame(filas)
    R.to_csv(a.csv, index=False)

    print("\n  por banda (mediana de los pares):")
    print(f"  {'banda':9s} {'s/2':>8s} {'RS':>9s} {'sel.adv':>9s} "
          + "  ".join(f"{'RS-'+k:>12s}" for k in FEE))
    for b, _ in BANDAS:
        g = R[R.banda == b]
        if not len(g):
            continue
        print(f"  {b:9s} {g.half.median():8.4f} {g.rs.median():9.4f} "
              f"{g.adv.median():9.4f} "
              + "  ".join(f"{g.rs.median()-v:+12.4f}" for v in FEE.values()))

    # ------------------------------------------------------------ perfil de decaimiento
    print("\n  perfil de decaimiento (RS mediano sobre pares, por horizonte):")
    print(f"  {'banda':9s}" + "".join(f"{'D='+str(x)+'s':>11s}" for x in DELTAS))
    for b, _ in BANDAS:
        syms = S[S.banda == b].sym.tolist()
        fila = []
        for Dl in DELTAS:
            Dd = por_dia(F[F.sym.isin(syms)], f"rs{Dl}_bi")
            v = Dd.groupby("sym")["rs"].mean()
            fila.append(float(v.median()) if len(v) else np.nan)
        print(f"  {b:9s}" + "".join(f"{x:11.4f}" for x in fila))

    # ---------------------------------------------------------------- REGLA DE PARADA
    print("\n" + "=" * 92)
    print("REGLA DE PARADA (`PREREGISTRO_MAKER.md`)")
    print("=" * 92)
    med = float(R.rs.median())
    print(f"  mediana sobre los {len(R)} pares de RS({D_DECIDE}s) = {med:.4f}%")
    for k, v in FEE.items():
        print(f"    - fee {k:9s} {v:.4f}%  ->  margen {med-v:+.4f} pp   "
              f"{'CRUZA' if med - v > 0 else 'no cruza'}")

    # sin_top3 / sin_top1
    ord_ = R.sort_values("rs", ascending=False)
    for k in (1, 3):
        m2 = float(ord_.iloc[k:]["rs"].median())
        print(f"  sin_top{k}: mediana {m2:.4f}%  -> margen vs {FEE_DECIDE} "
              f"{m2-fee:+.4f} pp")

    # robustez: el otro estimador de mid, y solo el primer fill del barrido
    print("\n  robustez (la conclusion tiene que aguantar las cuatro):")
    variantes = [("mid bipunta, todos los fills", F, f"rs{D_DECIDE}_bi"),
                 ("mid ultimo precio, todos", F, f"rs{D_DECIDE}_ul"),
                 ("mid bipunta, 1er fill del barrido", F[F.primero], f"rs{D_DECIDE}_bi"),
                 ("mid ultimo, 1er fill del barrido", F[F.primero], f"rs{D_DECIDE}_ul")]
    for nom, Fv, cv in variantes:
        Dv = por_dia(Fv, cv)
        v = Dv.groupby("sym")["rs"].mean()
        mv = float(v.median()) if len(v) else np.nan
        print(f"    {nom:36s} mediana {mv:8.4f}%  margen {mv-fee:+8.4f} pp  "
              f"{'CRUZA' if mv - fee > 0 else 'no cruza'}")

    # pooleado (diagnostico de deriva)
    pool = F.groupby("sym").apply(
        lambda g: float(np.nanmean(g[col])), include_groups=False)
    lados = F.groupby(["sym", "d"]).apply(
        lambda g: float(np.nanmean(g[col])), include_groups=False).unstack()
    print(f"\n  diagnostico de deriva: pooleado (sin balancear) mediana "
          f"{float(pool.median()):.4f}% vs balanceado {med:.4f}%")
    if -1.0 in lados.columns and 1.0 in lados.columns:
        print(f"    RS mediano de fills de COMPRA del maker: "
              f"{float(lados[-1.0].median()):.4f}%")
        print(f"    RS mediano de fills de VENTA  del maker: "
              f"{float(lados[1.0].median()):.4f}%")

    if a.cola:
        diagnostico_cola(F, S, a.por_dia * a.largo_min)

    p = p_bloques(D, dias, fee)
    print(f"\n  p de bloques por dia ({len(dias)} bloques, la mediana supera el fee): "
          f"{p:.4f}")

    cruza = (med - fee > 0) and all(
        (por_dia(Fv, cv).groupby("sym")["rs"].mean().median() - fee) > 0
        for _, Fv, cv in variantes) and p < 0.05
    print("\n  VEREDICTO spot: " + ("VIVO - revisar a mano" if cruza else "CERRADO"))
    med_f = med - FEE["futuros"]
    print(f"  VEREDICTO futuros (referencia): {'margen ' if med_f > 0 else ''}"
          f"{med_f:+.4f} pp -> "
          + ("hay margen aritmetico; el paso siguiente es RE-MEDIR sobre aggTrades de "
             "FUTUROS, no concluir" if med_f > 0 else "CERRADO tambien"))
    print(f"\n-> {a.csv}")


if __name__ == "__main__":
    main()
