"""
MEDIR LA PRIMA DEL RADAR — `banco/PREREGISTRO_SKEW.md` §3 y §4, en un comando.

    py -3.13 -u opciones/medir_prima.py

La pregunta, fijada el 2026-09-19 antes de la primera fila:

    Cuando el radar pone un nombre en el top-8, ¿ese nombre se mueve MAS de lo que la
    opcion ya cobraba?

LA GUARDA VA PRIMERO. Hasta que haya N_MIN disparos apareados (§4: n = 200, de-solapado
por barra) el script cuenta y nada mas: cuantos eventos, cuantos con foto, a que ritmo y
para cuando se estima la fecha. NO baja ni un precio posterior a la barra. Correrlo antes
de tiempo no mira nada, que es lo que permite correrlo seguido para vigilar el apareo
(el 2026-10-23 estaba roto: 8 de 77, y nadie lo habia visto).

Pasada la guarda, en el orden del preregistro:

    (C) POTENCIA   MDE = 2,8 · sigma / sqrt(n), con n en BARRAS. Si el MDE supera el
                   costo de ida y vuelta del straddle -> "no se pudo medir", se sigue
                   juntando.
    (P) PREMISA    el filo de magnitud del radar tiene que existir en estos nombres.
                   Si falla, el signo no se mira: dice "la idea era mala", no "el dato no
                   alcanza".
    EL SIGNO       pnl por barra, elegidos menos no elegidos de LA MISMA BARRA.

LO QUE ESTE SCRIPT FIJA Y EL PREREGISTRO NO DECIA. Escrito el 2026-10-23, con la guarda
sin pasar y ningun pnl calculado:

  1. UNIVERSO: BTC, ETH, SOL (decidido el 2026-10-23, `PREREGISTRO_SKEW.md`).
  2. VENUE: en cada foto, el de MENOR `spread_iv` entre `okx` y `okx_um`. Es lo que haria
     quien opera: mirar el libro antes de entrar. Se elige por un dato que existe ANTES
     del disparo, nunca por el resultado. No se poolean: cada evento usa un venue.
  3. COSTO: el ancho del libro cruzado entero, `straddle_pct * spread_iv / iv_atm` (para
     una ATM la prima es casi proporcional a la IV), con piso en el 2% de la prima que
     midio la corrida 8. Entrar al ask y salir al bid: ida y vuelta completa.
  4. EL FINAL: S en min(barra + 4h, vencimiento). Si la opcion vence antes de las 4h, lo
     que paga es su liquidacion, no un precio posterior que ya no le toca.
  5. K = el subyacente de la foto (la foto elige la ATM, `prima_radar.foto`). El
     movimiento se mide con Binance spot en los dos extremos, para no mezclar el indice
     del venue con el precio de Binance.
  6. El ELEGIDO es el top-8 re-rankeado por `n_surge` dentro de los 46, exactamente como
     `radar/medir.py`. NO ELEGIDO: los otros de los tres en esa barra, con su foto.

El estimador es CONSERVADOR (§3): liquida a valor intrinseco e ignora el valor temporal
que le queda a la opcion. Un positivo no es artefacto del estimador; un negativo no
prueba que no haya nada.
"""
import argparse
import os
import sys
import time
from concurrent.futures import ThreadPoolExecutor

import numpy as np
import pandas as pd
import requests

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(os.path.dirname(HERE), "radar"))
from medir import UNIVERSO_PRE, K, H, Z, bajar, camino  # noqa: E402

MONEDAS = ("BTC", "ETH", "SOL")
VENUES = ("okx", "okx_um")
DESDE = "2026-10-08"        # la serie limpia de OKX arranca ese dia (PREREGISTRO_SKEW)
VENTANA = pd.Timedelta(minutes=30)
N_MIN = 200                 # §4: disparos apareados, de-solapados por barra
PISO_COSTO = 0.02           # corrida 8: cruzar cuesta 1-2% de la prima
REPS = 5000
SPOT = "https://api.binance.com"


# ───────────────────────────── el apareo (sin precios futuros) ─────────────────────────

def barras(D):
    """De-solapar a H horas, igual que `radar/medir.py`: la primera de cada grupo."""
    ts, quedan, ult = sorted(pd.Series(D["run_at"].unique())), [], None
    for t in ts:
        if ult is None or (t - ult) >= pd.Timedelta(hours=H):
            quedan.append(t)
            ult = t
    return D[D["run_at"].isin(quedan)]


def elegir(D):
    """Top-K re-rankeado por n_surge dentro de los 46, y quedarse con las tres monedas."""
    P = D[D["symbol"].isin(UNIVERSO_PRE)].copy()
    P = P.sort_values(["run_at", "n_surge"], ascending=[True, False], kind="mergesort")
    P["elegido"] = P.groupby("run_at").cumcount() < K
    P["moneda"] = P["symbol"].str.replace("USDT", "", regex=False)
    return P[P["moneda"].isin(MONEDAS)]


def fotos():
    """Una fila por (ts, moneda): el venue de menor spread_iv con precio."""
    F = []
    for m in MONEDAS:
        p = os.path.join(HERE, "prima_radar", f"{m}.csv")
        if os.path.exists(p):
            F.append(pd.read_csv(p))
    F = pd.concat(F, ignore_index=True)
    F = F[F["venue"].isin(VENUES) & F["straddle_pct"].notna() & F["spread_iv"].notna()
          & (F["iv_atm"] > 0)]
    F["ts"] = pd.to_datetime(F["ts"], utc=True)
    F = F.sort_values("spread_iv", kind="mergesort").drop_duplicates(["ts", "moneda"])
    return F.sort_values("ts")


def aparear(E, F):
    """La foto mas cercana a +-30 min de la barra, por moneda. Sin foto, no existe."""
    out = []
    for m, g in E.groupby("moneda"):
        f = F[F["moneda"] == m][["ts", "venue", "dias", "iv_atm", "straddle_pct",
                                 "spread_iv"]]
        if f.empty:
            continue
        j = pd.merge_asof(g.sort_values("run_at"), f.rename(columns={"ts": "ts_foto"}),
                          left_on="run_at", right_on="ts_foto", direction="nearest",
                          tolerance=VENTANA)
        out.append(j[j["ts_foto"].notna()])
    return pd.concat(out, ignore_index=True) if out else E.iloc[0:0]


# ───────────────────────────── despues de la guarda ────────────────────────────────────

def precio(sym, t):
    """Close del minuto que contiene t, Binance spot."""
    ms = int(t.timestamp() * 1000)
    for _ in range(3):
        try:
            r = requests.get(f"{SPOT}/api/v3/klines", timeout=20,
                             params={"symbol": sym, "interval": "1m",
                                     "startTime": ms - 60_000, "limit": 2})
            if r.status_code == 200 and r.json():
                return float(r.json()[-1][4])
        except Exception:
            time.sleep(1)
    return np.nan


def pnl(A):
    """pnl = |S_fin / S_foto - 1| - prima - costo, todo en % del subyacente."""
    A = A.copy()
    vence = A["ts_foto"] + pd.to_timedelta(A["dias"], unit="D")
    A["t_fin"] = np.minimum(A["run_at"] + pd.Timedelta(hours=H), vence)
    A["s0"] = [precio(s, t) for s, t in zip(A["symbol"], A["ts_foto"])]
    A["s1"] = [precio(s, t) for s, t in zip(A["symbol"], A["t_fin"])]
    A["costo"] = A["straddle_pct"] * np.maximum(A["spread_iv"] / A["iv_atm"], PISO_COSTO)
    A["bruto"] = (A["s1"] / A["s0"] - 1).abs() * 100 - A["straddle_pct"]
    A["pnl"] = A["bruto"] - A["costo"]
    return A.dropna(subset=["pnl"])


def por_barra(A, col):
    """Elegidos menos no elegidos DE LA MISMA BARRA. Solo barras con los dos grupos."""
    g = A.groupby(["run_at", "elegido"])[col].mean().unstack()
    if True not in g or False not in g:
        return pd.Series(dtype=float)
    return (g[True] - g[False]).dropna()


def boot_p(v, seed=0):
    rng = np.random.default_rng(seed)
    v = np.asarray(v)
    return float((np.array([rng.choice(v, len(v), True).mean()
                            for _ in range(REPS)]) <= 0).mean())


def main():
    ap = argparse.ArgumentParser(description="Forward test de la prima del radar")
    ap.parse_args()

    D = bajar(DESDE)
    if D.empty:
        sys.exit("sin corridas del radar desde " + DESDE)
    D["run_at"] = pd.to_datetime(D["run_at"], utc=True, format="mixed")
    D = D[D["run_at"] <= pd.Timestamp.now(tz="UTC") - pd.Timedelta(hours=H + 1)]
    B = barras(D)
    E = elegir(B)
    A = aparear(E, fotos())
    ev = A[A["elegido"]]

    dias = max((B["run_at"].max() - B["run_at"].min()).total_seconds() / 86400, 1e-9)
    print("=" * 72)
    print("PRIMA DEL RADAR — el apareo (sin mirar ningun precio posterior)")
    print("=" * 72)
    print(f"barras de-solapadas desde {DESDE}: {B.run_at.nunique()}  ({dias:.1f} dias)")
    print(f"\n{'':6s}{'disparos':>10s}{'con foto':>10s}{'no elegidos c/foto':>20s}")
    for m in MONEDAS:
        print(f"{m:6s}{int((E.elegido & (E.moneda == m)).sum()):>10d}"
              f"{int((ev.moneda == m).sum()):>10d}"
              f"{int(((~A.elegido) & (A.moneda == m)).sum()):>20d}")
    n = len(ev)
    tot = int(E["elegido"].sum())
    print(f"{'TOTAL':6s}{tot:>10d}{n:>10d}{int((~A.elegido).sum()):>20d}")
    print(f"\napareados: {n} de {tot} ({100 * n / max(tot, 1):.0f}%)   "
          f"ritmo {n / dias:.2f}/dia")

    if n < N_MIN:
        print(f"\nTODAVIA NO ALCANZA: {n} de {N_MIN} disparos apareados (§4).")
        if n > 0:
            # el ritmo de los ultimos 7 dias, que ya incluye el apareo arreglado
            corte = B["run_at"].max() - pd.Timedelta(days=7)
            r7 = len(ev[ev.run_at > corte]) / 7
            if r7 > 0:
                f = B["run_at"].max() + pd.Timedelta(days=(N_MIN - n) / r7)
                print(f"al ritmo de la ultima semana ({r7:.2f}/dia) se llega cerca del "
                      f"{f:%Y-%m-%d}.")
        print("No se bajo ningun precio posterior a ninguna barra. Nada que interpretar.")
        return

    # ── pasada la guarda: recien aca se miran precios ─────────────────────────────────
    print("\nbajando precios de Binance...", flush=True)
    A = pnl(A)
    sp = por_barra(A, "pnl")
    costo = float(A.loc[A.elegido, "costo"].median())
    sd = float(sp.std(ddof=1)) if len(sp) > 1 else np.inf
    mde = Z * sd / np.sqrt(max(len(sp), 1))

    print("\n" + "=" * 72)
    print("(C) POTENCIA — antes de mirar el signo")
    print("=" * 72)
    print(f"barras con elegido y no elegido: {len(sp)}   sigma {sd:.3f}   "
          f"MDE +-{mde:.3f}%   costo ida y vuelta (mediana) {costo:.3f}%")
    if mde > costo:
        print("\nNO SE PUDO MEDIR: el MDE supera el costo del straddle. Se sigue juntando.")
        return

    print("\n" + "=" * 72)
    print("(P) PREMISA — el filo de magnitud en estos nombres")
    print("=" * 72)
    with ThreadPoolExecutor(12) as ex:
        A["camino"] = list(ex.map(camino, zip(A.symbol, A.run_at.astype("int64") // 10**6,
                                              A.precio)))
    A["y"] = A["camino"] / A["atr_base"]
    py = por_barra(A.dropna(subset=["y"]), "y")
    mde_y = Z * float(py.std(ddof=1)) / np.sqrt(len(py)) if len(py) > 1 else np.inf
    print(f"spread de magnitud (ATR base) elegido - no elegido: {py.mean():+.3f}   "
          f"MDE +-{mde_y:.3f}")
    if not py.mean() > mde_y:
        print("\nLA PREMISA FALLA: en BTC/ETH/SOL el radar no separa magnitud. El signo NO "
              "se mira.")
        return

    print("\n" + "=" * 72)
    print("EL SIGNO — pnl del straddle, elegido - no elegido de la misma barra")
    print("=" * 72)
    p = boot_p(sp.to_numpy())
    print(f"spread por barra {sp.mean():+.3f}%   MDE +-{mde:.3f}%   p bootstrap {p:.4f}")
    print(f"descriptivo: pnl neto medio de los elegidos "
          f"{A.loc[A.elegido, 'pnl'].mean():+.3f}% (bruto "
          f"{A.loc[A.elegido, 'bruto'].mean():+.3f}%)")
    if sp.mean() > mde and p < 0.05:
        print("\nSUPERA EL MDE. Es la primera vez que la magnitud tiene precio: forward "
              "test con fecha, NUNCA tamaño.")
    else:
        print("\nNO SUPERA EL MDE: elegir con el radar no le gana a la prima que ya "
              "cobraba la opcion.")


if __name__ == "__main__":
    main()
