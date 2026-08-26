"""
METRICAS — posicionamiento de Binance Futures como fuente de features.

POR QUE EXISTE. La idea original era fadear cascadas de liquidaciones, pero Binance
discontinuo `liquidationSnapshot` en data.binance.vision. El dataset `metrics` es un
sustituto mejor: una cascada de liquidaciones ES una caida brusca de open interest
junto con un movimiento de precio. El OI mide el desapalancamiento real; los prints de
liquidacion son solo la parte visible.

Ademas destraba algo que estaba dado por muerto: `project-swing-p1-deriv-gate-result`
cerro el OI porque la API en vivo tiene un muro de 30 dias. **El archivo no tiene ese
muro** — hay datos desde 2020 con granularidad de 5 minutos.

Seis columnas, ninguna de las cuales es precio:
    sum_open_interest                    OI en contratos
    sum_open_interest_value              OI en USD (comparable entre monedas)
    count_toptrader_long_short_ratio     cuentas top: long/short
    sum_toptrader_long_short_ratio       posiciones top: long/short
    count_long_short_ratio               todas las cuentas: long/short
    sum_taker_long_short_vol_ratio       volumen agresor: compra/venta

ALINEACION (esto es donde se cuela el lookahead). En `primer_toque._entradas` la entrada
es el CIERRE de la vela i, o sea el instante t[i]+1h. Entonces las metricas de la propia
vela i —las que caen en [t[i], t[i]+1h)— ya se conocen al entrar y son legitimas. Se
agrupa por hora tomando el ULTIMO valor del bin y se etiqueta con t[i].

SIMBOLOS. El panel del banco es SPOT; los perps a veces llevan prefijo (`1000PEPEUSDT`
para `PEPEUSDT`). Se prueban las variantes.

    py -3.13 metricas.py            # self-test sobre 3 pares
"""
import io
import os
import zipfile
from concurrent.futures import ThreadPoolExecutor

import numpy as np
import pandas as pd
import requests

HERE = os.path.dirname(os.path.abspath(__file__))
CACHE = os.path.join(HERE, ".metrics_cache")
BASE = "https://data.binance.vision/data/futures/um/daily/metrics"
MS_H = 3_600_000

COLS = {
    "sum_open_interest": "oi",
    "sum_open_interest_value": "oi_usd",
    "count_toptrader_long_short_ratio": "tt_cuentas",
    "sum_toptrader_long_short_ratio": "tt_pos",
    "count_long_short_ratio": "ls_cuentas",
    "sum_taker_long_short_vol_ratio": "taker",
}


def perp_de(spot):
    """El perp de un par spot. Binance re-escala los de precio chico."""
    return [spot, f"1000{spot}", f"1000000{spot}"]


def _dia(perp, fecha):
    """Un dia de metricas (288 filas de 5min) o None si no existe."""
    url = f"{BASE}/{perp}/{perp}-metrics-{fecha}.zip"
    try:
        r = requests.get(url, timeout=30)
        if r.status_code != 200:
            return None
        with zipfile.ZipFile(io.BytesIO(r.content)) as z:
            with z.open(z.namelist()[0]) as f:
                return pd.read_csv(f)
    except Exception:
        return None


def _resolver_perp(spot, fecha_muestra):
    """Cual de las variantes existe. Se resuelve con un dia de muestra."""
    for p in perp_de(spot):
        if _dia(p, fecha_muestra) is not None:
            return p
    return None


def _marcar_sin_perp(p):
    """Cachea el hecho de que no hay perp, para no re-preguntar en cada corrida."""
    try:
        pd.DataFrame(columns=["t"]).to_pickle(p)
    except Exception:
        pass


def frame_simbolo(spot, fechas, workers=16):
    """DataFrame horario [t, oi, oi_usd, ...] para un par. Cacheado en disco."""
    os.makedirs(CACHE, exist_ok=True)
    tag = f"{spot}_{fechas[0]}_{fechas[-1]}"
    p = os.path.join(CACHE, f"{tag}.pkl")
    if os.path.exists(p):
        try:
            return pd.read_pickle(p)
        except Exception:
            pass

    perp = _resolver_perp(spot, fechas[len(fechas) // 2])
    if perp is None:
        _marcar_sin_perp(p)
        return None

    with ThreadPoolExecutor(workers) as ex:
        dias = list(ex.map(lambda f: _dia(perp, f), fechas))
    dias = [d for d in dias if d is not None and not d.empty]
    if not dias:
        _marcar_sin_perp(p)
        return None

    d = pd.concat(dias, ignore_index=True)
    d = d.rename(columns=COLS)
    ts = pd.to_datetime(d["create_time"], utc=True, format="mixed").astype("int64") // 10**6
    d["t"] = (ts // MS_H) * MS_H
    # ULTIMO valor de cada hora: es lo que se sabe cuando la vela cierra.
    out = d.groupby("t", as_index=False)[list(COLS.values())].last().sort_values("t")
    out = out.reset_index(drop=True)
    try:
        out.to_pickle(p)
    except Exception:
        pass
    return out


def load_metrics(syms, inicio, fin, workers=16, verbose=True):
    """{spot: DataFrame horario}. Los pares sin perp quedan afuera del dict."""
    fechas = [d.strftime("%Y-%m-%d")
              for d in pd.date_range(inicio, fin, freq="D", inclusive="left")]
    M, sin_perp = {}, []
    for k, s in enumerate(syms, 1):
        f = frame_simbolo(s, fechas, workers=workers)
        if f is None or f.empty:
            sin_perp.append(s)
        else:
            M[s] = f
        if verbose and k % 10 == 0:
            print(f"  metricas {k}/{len(syms)}  ({len(M)} con perp)...", flush=True)
    if verbose:
        print(f"  metricas: {len(M)}/{len(syms)} pares con perp; "
              f"sin perp: {len(sin_perp)}")
    return M


# ------------------------------------------------------------------ features
def _feat(d):
    """Todo mira solo al pasado: la fila i usa hasta i inclusive."""
    out = {}
    oi = d["oi_usd"].to_numpy(float)
    s_oi = pd.Series(oi)

    for k in (1, 4, 24):
        out[f"oi_chg_{k}h"] = (s_oi / s_oi.shift(k) - 1.0).to_numpy()

    # z-score del cambio horario contra su propio fondo: detector de shock de OI.
    ch1 = pd.Series(out["oi_chg_1h"])
    mu, sd = ch1.rolling(168).mean(), ch1.rolling(168).std()
    with np.errstate(invalid="ignore", divide="ignore"):
        out["oi_z"] = ((ch1 - mu) / sd).to_numpy()

    # OI contra su propio nivel de la semana: apalancamiento acumulado.
    out["oi_rel_168"] = (s_oi / s_oi.rolling(168).mean() - 1.0).to_numpy()

    for c in ("tt_cuentas", "tt_pos", "ls_cuentas", "taker"):
        s = pd.Series(d[c].to_numpy(float))
        out[c] = s.to_numpy()
        out[f"{c}_chg24"] = (s - s.shift(24)).to_numpy()
        # percentil propio del par (no comparable entre monedas en crudo)
        out[f"{c}_pct"] = s.rolling(168).rank(pct=True).to_numpy()
    return out


def feat_metricas(M, T, verbose=True):
    """Una fila por entrada de T, alineada por (sym, t). Index = T.index."""
    piezas = []
    for k, (sym, d) in enumerate(M.items(), 1):
        f = pd.DataFrame(_feat(d))
        f.insert(0, "t", d["t"].to_numpy())
        f.insert(0, "sym", sym)
        piezas.append(f)
        if verbose and k % 50 == 0:
            print(f"  feat metricas {k}/{len(M)}...", flush=True)
    FULL = pd.concat(piezas, ignore_index=True)
    F = T[["sym", "t"]].merge(FULL, on=["sym", "t"], how="left")
    F.index = T.index
    return F.drop(columns=["sym", "t"])


if __name__ == "__main__":
    fechas = [d.strftime("%Y-%m-%d")
              for d in pd.date_range("2026-07-01", "2026-07-08", freq="D")]
    for s in ("BTCUSDT", "PEPEUSDT", "SOLUSDT"):
        f = frame_simbolo(s, fechas)
        if f is None:
            print(f"{s:12s} SIN PERP")
        else:
            print(f"{s:12s} {len(f):4d} horas  "
                  f"{pd.to_datetime(f.t.min(), unit='ms')} -> "
                  f"{pd.to_datetime(f.t.max(), unit='ms')}  "
                  f"oi_usd medio {f.oi_usd.mean():,.0f}")
