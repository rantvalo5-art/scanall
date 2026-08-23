"""
FASE 1 — profundidad historica de `tt_pos` SIN alertas (handoff de posicionamiento, §4).

El hallazgo (`posicionamiento.py`, Fase B) sale de 1.028 alertas BEST de 12 semanas:
tt_pos < 1,28 rinde -9,30% a 7d y -9,01pp contra el dardo pareado, cruzando los seis
filtros. La pregunta de esta fase es si eso aguanta 4 anios o era la ventana.

`screener_outcomes` solo tiene 3 meses de alertas reales y el replay del backtest NO las
reproduce (memoria: project-replay-no-reemplaza-vivo), asi que no hay alertas de
2022-2025. La salida es medir el efecto SOBRE EL UNIVERSO, sin alertas: si tt_pos bajo
predice retorno negativo a 7d en una grilla diaria de simbolos x fechas, el mecanismo
existe con independencia del screener y la version sobre alertas es un caso particular.

Sesgo de universo: el panel NO usa el top-volumen de hoy (memoria:
project-swing-backtest-sesgo-universo). Enumera el bucket publico de Binance, que
conserva los perps DELISTEADOS. Los que murieron son justo los que mas importan.

Precio: klines del PROPIO perp (dumps mensuales), no del spot. El spot no existe para
los delisteados y el mecanismo se postula sobre el futuro.

REGLA DE PARADA (preescrita en el handoff, no se afloja despues de ver el numero):
  el margen del quintil bajo de tt_pos contra el dardo pareado tiene que ser negativo
  con IC95 sin cero en AL MENOS 3 DE LOS 4 ANIOS por separado. Si vive en 1 o 2, es
  regimen y se archiva.

  TRADUCCION (fijada ANTES de mirar ningun efecto, 2026-08-23): el handoff decia
  '2022-2025', pero `sum_toptrader_long_short_ratio` viene PRESENTE PERO VACIA casi
  todo 2022 en los dumps (BTCUSDT 87% NaN; idem ETHUSDT). Eso es la fuente, no un
  resultado. Se conserva el 3-de-4 partiendo el tramo limpio (2023-01 en adelante)
  en CUATRO bloques contiguos de igual duracion. Los anios calendario se imprimen
  igual como control de lectura, y el ultimo bloque va marcado porque solapa la
  ventana donde se descubrio el efecto.

Pasos (reanudables, el harness mata a los 10 min):
    py -3.13 panel_tt.py --paso universo     # que perps existen y en que fechas
    py -3.13 panel_tt.py --paso bajar        # metrics diarias + klines (por tandas)
    py -3.13 panel_tt.py --paso panel        # arma la grilla y corre los tests
"""
import argparse
import io
import json
import re
import sys
import time
import zipfile
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import numpy as np
import pandas as pd
import requests

import fase0_plan as F

HERE = Path(__file__).parent
PCACHE = HERE / ".panel_cache"
PCACHE.mkdir(exist_ok=True)
(PCACHE / "met").mkdir(exist_ok=True)
(PCACHE / "kl").mkdir(exist_ok=True)

S3 = "https://s3-ap-northeast-1.amazonaws.com/data.binance.vision"
BASE = "https://data.binance.vision/data/futures/um"
UNIVERSO = PCACHE / "universo.json"

MS_H = 3_600_000
COLS = {
    "sum_open_interest": "oi",
    "sum_open_interest_value": "oi_usd",
    "count_toptrader_long_short_ratio": "tt_cuentas",
    "sum_toptrader_long_short_ratio": "tt_pos",
    "count_long_short_ratio": "ls_cuentas",
    "sum_taker_long_short_vol_ratio": "taker",
}
VARS = list(COLS.values())

_SES = None


def ses():
    """Session con pool grande: son cientos de miles de GET chiquitos."""
    global _SES
    if _SES is None:
        _SES = requests.Session()
        ad = requests.adapters.HTTPAdapter(pool_connections=64, pool_maxsize=64,
                                           max_retries=0)
        _SES.mount("https://", ad)
    return _SES


def s3_listar(prefix, delimiter=""):
    """Todas las keys (o prefijos) bajo `prefix`, paginando."""
    tok, keys, pref = None, [], []
    while True:
        p = {"list-type": "2", "prefix": prefix, "max-keys": "1000"}
        if delimiter:
            p["delimiter"] = delimiter
        if tok:
            p["continuation-token"] = tok
        for intento in range(5):
            try:
                r = ses().get(S3, params=p, timeout=30)
                if r.status_code == 200:
                    break
                time.sleep(1.5 * (intento + 1))
            except requests.exceptions.RequestException:
                time.sleep(1.5 * (intento + 1))
        else:
            return keys, pref
        x = r.text
        keys += re.findall(r"<Key>([^<]+)</Key>", x)
        pref += re.findall(r"<Prefix>([^<]+)</Prefix>", x)
        m = re.search(r"<NextContinuationToken>([^<]+)<", x)
        if not m:
            return keys, pref
        tok = m.group(1)


# ════════════════════════════════════════════════════════════════════════════
# PASO 1 — universo: que perps existieron y en que fechas hay metrics
# ════════════════════════════════════════════════════════════════════════════
def fechas_de(sym):
    keys, _ = s3_listar(f"data/futures/um/daily/metrics/{sym}/")
    out = set()
    for k in keys:
        m = re.search(r"-metrics-(\d{4}-\d{2}-\d{2})\.zip$", k)
        if m:
            out.add(m.group(1))
    return sorted(out)


def paso_universo(workers, desde, hasta):
    _, prefs = s3_listar("data/futures/um/daily/metrics/", delimiter="/")
    syms = sorted({p.rstrip("/").split("/")[-1] for p in prefs})
    syms = [s for s in syms if s.endswith("USDT") and s.isascii()]
    print(f"perps USDT en el bucket (incluye delisteados): {len(syms)}")

    prev = {}
    if UNIVERSO.exists():
        prev = json.loads(UNIVERSO.read_text(encoding="utf-8")).get("simbolos", {})
    falta = [s for s in syms if s not in prev]
    print(f"ya cacheados: {len(prev)}   a listar: {len(falta)}")

    U = dict(prev)
    hecho = 0
    with ThreadPoolExecutor(workers) as ex:
        for s, fs in zip(falta, ex.map(fechas_de, falta)):
            U[s] = fs
            hecho += 1
            if hecho % 50 == 0:
                print(f"    {hecho}/{len(falta)}", flush=True)
                UNIVERSO.write_text(json.dumps({"simbolos": U}), encoding="utf-8")
    UNIVERSO.write_text(json.dumps({"simbolos": U}), encoding="utf-8")

    # resumen: cuantos simbolo-dia reales caen en la ventana pedida
    tot = 0
    por_anio = {}
    vivos = 0
    for s, fs in U.items():
        f = [d for d in fs if desde <= d <= hasta]
        if not f:
            continue
        vivos += 1
        tot += len(f)
        for d in f:
            por_anio[d[:4]] = por_anio.get(d[:4], 0) + 1
    print(f"\nVentana {desde} → {hasta}")
    print(f"  simbolos con metrics: {vivos}")
    print(f"  simbolo-dia totales:  {tot:,}")
    for a in sorted(por_anio):
        print(f"    {a}: {por_anio[a]:>7,} simbolo-dia")
    # perps muertos = sin datos en los ultimos 20 dias de la ventana
    corte = (pd.Timestamp(hasta) - pd.Timedelta(days=20)).strftime("%Y-%m-%d")
    muertos = sum(1 for s, fs in U.items() if fs and max(fs) < corte)
    print(f"  perps DELISTEADOS (sin datos desde {corte}): {muertos}")
    return U


# ════════════════════════════════════════════════════════════════════════════
# PASO 2 — descarga (reanudable por simbolo-anio; el harness mata a los 10 min)
# ════════════════════════════════════════════════════════════════════════════
def _zip_csv(url, header, cols=None):
    """Descarga un zip de data.binance.vision y devuelve su unico CSV, o None."""
    for intento in range(3):
        try:
            r = ses().get(url, timeout=30)
        except requests.exceptions.RequestException:
            time.sleep(1 + intento)
            continue
        if r.status_code == 404:
            return None
        if r.status_code != 200:
            time.sleep(1 + intento)
            continue
        try:
            with zipfile.ZipFile(io.BytesIO(r.content)) as z:
                crudo = z.open(z.namelist()[0]).read()
        except Exception:
            return None
        # Los dumps viejos de klines vienen SIN encabezado: leer con header=0 se
        # comeria la primera vela y dejaria nombres de columna numericos.
        primera = crudo.split(b"\n", 1)[0].split(b",")[0].strip()
        tiene = not primera.replace(b"-", b"").isdigit()
        try:
            d = pd.read_csv(io.BytesIO(crudo), header=0 if tiene else None)
        except Exception:
            return None
        if not tiene and cols:
            d = d.iloc[:, :len(cols)]
            d.columns = cols
        return d
    return None


def _met_dia(sym, dia):
    return _zip_csv(f"{BASE}/daily/metrics/{sym}/{sym}-metrics-{dia}.zip", True)


def bajar_metrics(sym, anio, dias, workers):
    """Frame horario del simbolo-anio. Cacheado; devuelve None si no hay nada."""
    p = PCACHE / "met" / f"{sym}_{anio}.pkl"
    if p.exists():
        return "cache"
    with ThreadPoolExecutor(workers) as ex:
        ds = [d for d in ex.map(lambda d: _met_dia(sym, d), dias)
              if d is not None and not d.empty]
    if not ds:
        pd.DataFrame(columns=["t"]).to_pickle(p)
        return "vacio"
    d = pd.concat(ds, ignore_index=True).rename(columns=COLS)
    falta = [c for c in VARS if c not in d.columns]
    for c in falta:
        d[c] = float("nan")
    ts = pd.to_datetime(d["create_time"], utc=True, format="mixed").astype("int64") // 10**6
    d["t"] = (ts // MS_H) * MS_H
    # El valor de la hora es el ULTIMO dato de esa hora: es lo que se sabe cuando la
    # hora CIERRA, no antes. Quien lo consuma debe tomar la ultima hora COMPLETAMENTE
    # cerrada (searchsorted(t, T - 1h)); si no, hay lookahead de hasta 59 min.
    out = (d.groupby("t", as_index=False)[VARS].last()
           .sort_values("t").reset_index(drop=True))
    for c in VARS:
        out[c] = pd.to_numeric(out[c], errors="coerce").astype("float32")
    out.to_pickle(p)
    return "ok"


KL_COLS = ["open_time", "open", "high", "low", "close", "volume",
           "close_time", "quote_volume", "count", "taker_buy_volume",
           "taker_buy_quote_volume", "ignore"]


FAPI = "https://fapi.binance.com/fapi/v1/klines"


def _kl_rest(sym, desde_ms, hasta_ms):
    """OHLC diario por REST de futuros: 1.500 velas por request en vez de ~70 zips.

    Sirve tambien para los perps DELISTEADOS (verificado contra los dumps: identico
    al centavo en HNTUSDT y SRMUSDT). El limite de peso de fapi es 2.400/min y una
    request de 1.500 velas pesa 10, o sea ~4 req/s: por eso pocos workers.
    """
    filas, cursor = [], desde_ms
    for _ in range(8):
        d = None
        for intento in range(4):
            try:
                r = ses().get(FAPI, params={"symbol": sym, "interval": "1d",
                                            "startTime": cursor, "limit": 1500},
                              timeout=25)
            except requests.exceptions.RequestException:
                time.sleep(1 + intento)
                continue
            if r.status_code == 200:
                d = r.json()
                break
            if r.status_code in (429, 418):
                time.sleep(3 * (intento + 1))
                continue
            return None          # simbolo desconocido para la REST -> al zip
        if not d:
            break
        filas += d
        if len(d) < 1500 or d[-1][0] >= hasta_ms:
            break
        cursor = d[-1][0] + 1
    if not filas:
        return None
    return pd.DataFrame({
        "open_time": [int(x[0]) for x in filas],
        "open": [float(x[1]) for x in filas],
        "high": [float(x[2]) for x in filas],
        "low": [float(x[3]) for x in filas],
        "close": [float(x[4]) for x in filas]})


def _kl_zips(sym, meses, workers):
    """Camino de respaldo: dumps mensuales, y diarios para el mes en curso."""
    KM = f"{BASE}/monthly/klines/{sym}/1d/{sym}-1d-%s.zip"
    KD = f"{BASE}/daily/klines/{sym}/1d/{sym}-1d-%s.zip"
    with ThreadPoolExecutor(workers) as ex:
        ds = list(ex.map(lambda m: _zip_csv(KM % m, True, KL_COLS), meses))
    faltan = [m for m, d in zip(meses, ds) if d is None or d.empty]
    ds = [d for d in ds if d is not None and not d.empty]
    if faltan:
        dias = []
        for m in faltan:
            i0 = pd.Timestamp(m + "-01")
            dias += [d.strftime("%Y-%m-%d")
                     for d in pd.date_range(i0, i0 + pd.offsets.MonthEnd(1), freq="D")]
        with ThreadPoolExecutor(workers) as ex:
            ds += [d for d in ex.map(lambda x: _zip_csv(KD % x, True, KL_COLS), dias)
                   if d is not None and not d.empty]
    if not ds:
        return None
    d = pd.concat(ds, ignore_index=True)
    return d[["open_time", "open", "high", "low", "close"]].copy()


def bajar_klines(sym, meses, workers, desde_ms, hasta_ms):
    """OHLC diario del PROPIO perp — el unico precio que existe para un delisteado."""
    p = PCACHE / "kl" / f"{sym}.pkl"
    if p.exists():
        return "cache"
    d = _kl_rest(sym, desde_ms, hasta_ms)
    via = "rest"
    if d is None or d.empty:
        d = _kl_zips(sym, meses, workers)
        via = "zip"
    if d is None or d.empty:
        pd.DataFrame(columns=["open_time"]).to_pickle(p)
        return "vacio"
    d["open_time"] = pd.to_numeric(d["open_time"], errors="coerce")
    d = d[d["open_time"].notna()].copy()
    if d.empty:
        pd.DataFrame(columns=["open_time"]).to_pickle(p)
        return "vacio"
    d["open_time"] = d["open_time"].astype("int64")
    if d["open_time"].max() > 10**14:        # algun dump viejo viene en microsegundos
        d["open_time"] //= 1000
    for c in ("open", "high", "low", "close"):
        d[c] = pd.to_numeric(d[c], errors="coerce").astype("float64")
    d = (d.dropna().sort_values("open_time")
         .drop_duplicates("open_time").reset_index(drop=True))
    d.to_pickle(p)
    return via


def paso_bajar(args):
    U = json.loads(UNIVERSO.read_text(encoding="utf-8"))["simbolos"]
    syms = sorted(s for s, fs in U.items()
                  if any(args.desde <= d <= args.hasta for d in fs))
    if args.solo:
        syms = [s for s in syms if s in set(args.solo.split(","))]
    print(f"{len(syms)} simbolos · stride {args.stride} · {args.desde} → {args.hasta}")

    # que simbolo-anio faltan (reanudable: se saltean los que ya tienen pickle)
    tareas = []
    for s in syms:
        dias = sorted(d for d in U[s] if args.desde <= d <= args.hasta)
        dias = dias[::args.stride]
        por_anio = {}
        for d in dias:
            por_anio.setdefault(d[:4], []).append(d)
        for a, ds in por_anio.items():
            if not (PCACHE / "met" / f"{s}_{a}.pkl").exists():
                tareas.append((s, a, ds))
    pend = sum(len(t[2]) for t in tareas)
    print(f"metrics: {len(tareas)} simbolo-anio pendientes = {pend:,} archivos "
          f"(~{pend / 48 / 60:.0f} min a 48 req/s)", flush=True)

    t0 = time.time()
    hechos = 0
    for n, (s, a, ds) in enumerate(tareas, 1):
        bajar_metrics(s, a, ds, args.workers)
        hechos += len(ds)
        if n % 25 == 0 or n == len(tareas):
            el = time.time() - t0
            rate = hechos / max(el, 1)
            queda = (pend - hechos) / max(rate, 1)
            print(f"  met {n}/{len(tareas)}  {hechos:,}/{pend:,} archivos  "
                  f"{rate:.0f} req/s  ETA {queda / 60:.0f} min", flush=True)

    # klines del perp: mensuales, mucho mas baratos
    falta_kl = [s for s in syms if not (PCACHE / "kl" / f"{s}.pkl").exists()]
    print(f"\nklines: {len(falta_kl)} simbolos pendientes (REST, ~4 req/s por peso)",
          flush=True)
    fin = (pd.Timestamp(args.hasta) + pd.Timedelta(days=10)).strftime("%Y-%m-%d")
    d_ms = int(pd.Timestamp(args.desde, tz="UTC").value // 10**6)
    h_ms = int(pd.Timestamp(fin, tz="UTC").value // 10**6)
    vias = {}
    t0 = time.time()

    def _una(s):
        fs = [d for d in U[s] if args.desde <= d <= fin]
        if not fs:
            return s, "sin-fechas"
        i0 = pd.Timestamp(min(fs)).replace(day=1)
        meses = [d.strftime("%Y-%m")
                 for d in pd.date_range(i0, pd.Timestamp(fin), freq="MS")]
        try:
            return s, bajar_klines(s, meses, 8, d_ms, h_ms)
        except Exception as e:
            return s, f"ERROR {type(e).__name__}"

    with ThreadPoolExecutor(6) as ex:
        for n, (s, via) in enumerate(ex.map(_una, falta_kl), 1):
            vias[via] = vias.get(via, 0) + 1
            if n % 50 == 0 or n == len(falta_kl):
                print(f"  kl {n}/{len(falta_kl)}  {n / max(time.time() - t0, 1):.1f}/s  "
                      f"{vias}", flush=True)
    print("\nDESCARGA COMPLETA")


# ════════════════════════════════════════════════════════════════════════════
# PASO 3 — panel: grilla simbolo x fecha, forward 7d, dardo pareado
# ════════════════════════════════════════════════════════════════════════════
DAY_MS = 86_400_000
COSTS = 0.003        # round-trip, estandar del repo (dip_previo.COSTS)


def _carga_simbolo(sym):
    """(metrics horarias, klines diarias) del simbolo, o (None, None)."""
    ms = []
    for f in sorted((PCACHE / "met").glob(f"{sym}_*.pkl")):
        try:
            d = pd.read_pickle(f)
        except Exception:
            continue
        if not d.empty:
            ms.append(d)
    pk = PCACHE / "kl" / f"{sym}.pkl"
    if not ms or not pk.exists():
        return None, None
    try:
        k = pd.read_pickle(pk)
    except Exception:
        return None, None
    if k.empty:
        return None, None
    m = pd.concat(ms, ignore_index=True).sort_values("t")
    m = m.drop_duplicates("t").reset_index(drop=True)
    return m, k.sort_values("open_time").reset_index(drop=True)


def _dardos(fechas, rets, k, rng, win_ms, excl_ms):
    """Dardo pareado: MISMO simbolo, fechas al azar de +-win, saltando +-excl.

    Controla la moneda Y el tramo temporal a la vez. Es el estandar del repo,
    el que mato ~450 hipotesis; un promedio sin el no dice nada.
    """
    n = len(fechas)
    out = np.full(n, np.nan)
    lo_w = np.searchsorted(fechas, fechas - win_ms, side="left")
    hi_w = np.searchsorted(fechas, fechas + win_ms, side="right")
    lo_e = np.searchsorted(fechas, fechas - excl_ms, side="left")
    hi_e = np.searchsorted(fechas, fechas + excl_ms, side="right")
    for i in range(n):
        a, b = lo_w[i], lo_e[i]          # tramo izquierdo  [a, b)
        c, d = hi_e[i], hi_w[i]          # tramo derecho    [c, d)
        na, nc = b - a, d - c
        if na + nc < 4:
            continue
        u = rng.integers(0, na + nc, size=k)
        idx = np.where(u < na, a + u, c + (u - na))
        v = rets[idx]
        v = v[~np.isnan(v)]
        if len(v) < 4:
            continue
        out[i] = v.mean()
    return out


def construir_panel(args):
    """Una fila por (simbolo, fecha de grilla). Cacheado: rehacerlo cuesta minutos."""
    p = PCACHE / f"panel_s{args.stride}_h{args.horizonte_d}.pkl"
    if p.exists() and not args.rehacer:
        return pd.read_pickle(p)

    U = json.loads(UNIVERSO.read_text(encoding="utf-8"))["simbolos"]
    syms = sorted(s for s, fs in U.items()
                  if any(args.desde <= d <= args.hasta for d in fs))
    desde_ms = int(pd.Timestamp(args.desde, tz="UTC").value // 10**6)
    hasta_ms = int(pd.Timestamp(args.hasta, tz="UTC").value // 10**6)
    H_MS = args.horizonte_d * DAY_MS
    rng = np.random.default_rng(args.seed)

    filas, sin_datos = [], 0
    for n, sym in enumerate(syms, 1):
        m, k = _carga_simbolo(sym)
        if m is None or len(m) < 48 or len(k) < args.horizonte_d + 2:
            sin_datos += 1
            continue
        mt = m["t"].values.astype("int64")
        kt = k["open_time"].values.astype("int64")
        ko = k["open"].values.astype("float64")

        # Grilla: 00:00 UTC de cada dia que tenga kline de entrada Y de salida.
        # Entrada = APERTURA del dia T, o sea el precio exacto en el instante T.
        gt = kt[(kt >= desde_ms) & (kt <= hasta_ms)]
        if not len(gt):
            continue
        js = np.searchsorted(kt, gt + H_MS)
        ok = js < len(kt)
        ok[ok] &= kt[js[ok]] == (gt[ok] + H_MS)
        gt, js = gt[ok], js[ok]
        if not len(gt):
            continue
        i_ent = np.searchsorted(kt, gt)
        p_ent, p_sal = ko[i_ent], ko[js]
        with np.errstate(divide="ignore", invalid="ignore"):
            r = np.where(p_ent > 0, p_sal / p_ent - 1 - COSTS, np.nan)

        # Observacion: ultima hora COMPLETAMENTE cerrada antes de T. El valor horario
        # es el ULTIMO dato de esa hora, asi que usar la hora que CONTIENE a T seria
        # lookahead de hasta 59 min (trampa 2 del handoff).
        jm = np.searchsorted(mt, gt - MS_H, side="right") - 1
        val = jm >= 0
        jm = np.maximum(jm, 0)
        # y no sirve un dato viejo: tiene que ser la hora inmediatamente anterior
        val &= (gt - mt[jm]) <= args.frescura_h * MS_H
        val &= ~np.isnan(r)
        if val.sum() < 8:
            continue

        d = {"symbol": sym, "t": gt[val], "r": r[val], "px": p_ent[val]}
        for v in VARS:
            d[v] = m[v].values[jm[val]]
        f = pd.DataFrame(d)
        f = f[f.tt_pos.notna() & (f.tt_pos > 0)]
        if len(f) < 8:
            continue
        f = f.sort_values("t").reset_index(drop=True)
        f["dardo"] = _dardos(f["t"].values.astype("int64"),
                             f["r"].values.astype("float64"),
                             args.darts, rng,
                             args.dart_window * DAY_MS, args.dart_excl * DAY_MS)
        filas.append(f)
        if n % 100 == 0:
            print(f"    {n}/{len(syms)}  ({len(filas)} con panel)", flush=True)

    if not filas:
        return pd.DataFrame(columns=["symbol", "t", "r", "px", "dardo",
                                    "fecha", "anio", "week"] + VARS)
    P = pd.concat(filas, ignore_index=True)
    ts = pd.to_datetime(P["t"], unit="ms", utc=True)
    P["fecha"] = ts.dt.strftime("%Y-%m-%d")
    P["anio"] = ts.dt.year
    P["week"] = ts.dt.strftime("%G-W%V")
    P = P[P.dardo.notna()].reset_index(drop=True)
    P.to_pickle(p)
    print(f"  simbolos sin datos: {sin_datos}")
    return P


def _linea(nom, g, minimo=30):
    """Margen contra el dardo + IC por semana + concentracion en los DOS ejes.

    Sobre un panel de cientos de simbolos y ~180 semanas, sacar el top-3 (el estandar
    del repo, calibrado para ~1.000 alertas) no prueba nada: se saca ademas el top 2%
    de simbolos y el top 2% de semanas, que es la version que escala.
    """
    if len(g) < minimo:
        print(f"  {nom:<26} n={len(g):>6}  (pocas)")
        return None
    mg = (g.r - g.dardo).tolist()
    wk, sy = g.week.tolist(), g.symbol.tolist()
    lo, hi = F.boot_ci(mg, wk)
    k_sim = max(3, int(0.02 * g.symbol.nunique()))
    k_sem = max(3, int(0.02 * g.week.nunique()))
    d_sim, _, _ = F.drop_top(mg, wk, sy, k=k_sim)
    d_sem, _, _ = F.drop_top(mg, wk, wk, k=k_sem)
    m_sim = float(np.mean(d_sim)) if len(d_sim) else float("nan")
    m_sem = float(np.mean(d_sem)) if len(d_sem) else float("nan")
    cruza = hi < 0            # la hipotesis es NEGATIVA: el IC no puede tocar el cero
    print(f"  {nom:<26} n={len(g):>6}  ret {g.r.mean()*100:>+7.2f}%  "
          f"dardo {g.dardo.mean()*100:>+7.2f}%  margen {np.mean(mg)*100:>+7.2f}pp  "
          f"IC95 [{lo*100:>+7.2f},{hi*100:>+7.2f}]  "
          f"sin{k_sim}sim {m_sim*100:>+6.2f}  sin{k_sem}sem {m_sem*100:>+6.2f}  "
          f"{'CRUZA' if cruza else '-'}")
    return {"n": int(len(g)), "ret": float(g.r.mean()), "dardo": float(g.dardo.mean()),
            "margen": float(np.mean(mg)), "ic": [float(lo), float(hi)],
            "sin_sim": m_sim, "sin_sem": m_sem, "cruza": bool(cruza)}


def paso_panel(args):
    print("Construyendo panel...", flush=True)
    P = construir_panel(args)
    print(f"\nPANEL CRUDO: {len(P):,} filas · {P.symbol.nunique()} simbolos · "
          f"{P.fecha.min()} -> {P.fecha.max()} · horizonte {args.horizonte_d}d neto")
    print("  por anio: " + "  ".join(
        f"{a}:{v:,}" for a, v in P.anio.value_counts().sort_index().items()))

    # ── El agujero de 2022 ────────────────────────────────────────────────────
    # `sum_toptrader_long_short_ratio` viene PRESENTE PERO VACIA casi todo 2022 en los
    # dumps de Binance (BTCUSDT 87% NaN; verificado mes a mes en BTC y ETH). Los tramos
    # limpios son 2020-09 -> 2021-12 y 2023-01 -> hoy. No es un resultado, es la fuente.
    #
    # Eso rompe el denominador de la regla preescrita ("3 de los 4 anios, 2022-2025").
    # Se traduce ANTES de mirar ningun efecto, y conservando el 3-de-4: el tramo limpio
    # se parte en CUATRO bloques contiguos de igual duracion y se exige el mismo 3 de 4.
    # Los anios calendario van igual como control de lectura.
    L = P[P.fecha >= args.limpio_desde].copy()
    print(f"\nTRAMO LIMPIO (>= {args.limpio_desde}): {len(L):,} filas · "
          f"{L.symbol.nunique()} simbolos · {L.fecha.min()} -> {L.fecha.max()}")
    print(f"  tt_pos: p20={L.tt_pos.quantile(.2):.3f}  mediana={L.tt_pos.median():.3f}  "
          f"p80={L.tt_pos.quantile(.8):.3f}")
    if len(P[P.fecha < "2022-01-01"]):
        print(f"  (hay {len(P[P.fecha < '2022-01-01']):,} filas de 2021 o antes; "
              f"van aparte al final)")

    res = {"n_crudo": int(len(P)), "n_limpio": int(len(L)), "stride": args.stride,
           "horizonte_d": args.horizonte_d, "umbral": args.umbral,
           "limpio_desde": args.limpio_desde,
           "rango": [L.fecha.min(), L.fecha.max()]}

    # ── escalera: el efecto tiene que ser monotono, no un solo grupo raro ──
    print("\n" + "=" * 146)
    print("ESCALERA — quintiles de tt_pos (q1 = el dinero grande mas corto). "
          "Si el efecto es real, baja monotono.")
    print("=" * 146)
    L["q"] = pd.qcut(L.tt_pos, 5, labels=False, duplicates="drop")
    res["escalera"] = [_linea(f"q{int(k)+1}", L[L.q == k])
                       for k in sorted(L.q.dropna().unique())]

    # ── nivel absoluto vs relativo al propio simbolo ──────────────────────────
    # Un umbral absoluto sobre 800 monedas selecciona en parte QUE moneda, no CUANDO.
    # El quintil dentro de la historia de cada simbolo separa las dos cosas.
    print("\n" + "=" * 146)
    print("MISMO CORTE, PERO DENTRO DE CADA SIMBOLO — separa 'que moneda' de 'que momento'")
    print("=" * 146)
    L["qs"] = L.groupby("symbol").tt_pos.transform(
        lambda x: pd.qcut(x, 5, labels=False, duplicates="drop") if x.nunique() > 5
        else np.nan)
    res["escalera_intra"] = [_linea(f"q{int(k)+1} intra-simbolo", L[L.qs == k])
                             for k in sorted(L.qs.dropna().unique())]

    # ── la regla deployable: umbral ABSOLUTO fijado a priori sobre las alertas ──
    print("\n" + "=" * 146)
    print(f"REGLA A PRIORI — tt_pos < {args.umbral} (umbral heredado de las alertas; "
          f"NO se re-ajusta aca)")
    print("=" * 146)
    B = L[L.tt_pos < args.umbral]
    print(f"  cubre {len(B) / max(len(L), 1) * 100:.1f}% del panel limpio")
    res["global"] = _linea("bajo (global)", B)
    res["resto"] = _linea("resto", L[L.tt_pos >= args.umbral])

    # ── LA REGLA DE PARADA ────────────────────────────────────────────────────
    print("\n" + "=" * 146)
    print("REGLA DE PARADA (preescrita, traducida por el agujero de 2022) — margen "
          "NEGATIVO con IC95 sin cero en >= 3 de 4 bloques")
    print("=" * 146)
    t0, t1 = int(L.t.min()), int(L.t.max())
    bordes = [t0 + (t1 - t0) * i // 4 for i in range(5)]
    bordes[-1] = t1 + 1
    bloques, cruzan = {}, []
    for i in range(4):
        g = B[(B.t >= bordes[i]) & (B.t < bordes[i + 1])]
        ini = pd.to_datetime(bordes[i], unit="ms").strftime("%Y-%m-%d")
        fin = pd.to_datetime(bordes[i + 1] - 1, unit="ms").strftime("%Y-%m-%d")
        marca = " *" if fin >= "2026-05-31" else ""          # solapa el descubrimiento
        r = _linea(f"B{i + 1} {ini}→{fin}{marca}", g)
        if r:
            bloques[f"B{i + 1}"] = dict(r, desde=ini, hasta=fin)
            if r["cruza"]:
                cruzan.append(f"B{i + 1}")
    res["bloques"], res["cruzan"] = bloques, cruzan
    print("  * = el bloque solapa la ventana donde se descubrio el efecto "
          "(may-ago 2026): no es independiente")

    print("\n  ── control de lectura: los mismos datos por anio calendario ──")
    anios = {}
    for a in sorted(L.anio.unique()):
        g = B[B.anio == a]
        cob = len(g) / max(len(L[L.anio == a]), 1) * 100
        r = _linea(f"{a} (cubre {cob:>4.1f}%)", g)
        if r:
            anios[int(a)] = r
    res["anios"] = anios

    # ── 2021, si quedo algo: es un regimen distinto y no cuesta nada mirarlo ──
    V = P[P.fecha < "2022-01-01"]
    if len(V) > 200:
        print("\n  ── extra: tramo 2020-09 → 2021-12 (pocos perps, otro regimen) ──")
        _linea("2021 bajo", V[V.tt_pos < args.umbral])
        _linea("2021 resto", V[V.tt_pos >= args.umbral])

    print(f"\n  bloques con margen negativo e IC95 sin cero: {cruzan}  "
          f"({len(cruzan)}/4)")
    veredicto = ("CRUZA LA REGLA DE PARADA" if len(cruzan) >= 3
                 else "NO CRUZA — es regimen, se archiva")
    res["veredicto"] = veredicto
    print(f"\n  >>> {veredicto} <<<")

    Path(args.out).write_text(json.dumps(res, indent=2, ensure_ascii=False),
                              encoding="utf-8")
    print(f"\nGuardado: {args.out}")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--paso", required=True, choices=["universo", "bajar", "panel"])
    ap.add_argument("--desde", default="2022-01-01")
    ap.add_argument("--hasta", default="2026-08-20")
    ap.add_argument("--workers", type=int, default=48)
    ap.add_argument("--stride", type=int, default=3,
                    help="1 = grilla diaria (535k archivos). 3 = cada 3 dias, "
                         "densificable despues sin tirar lo bajado")
    ap.add_argument("--solo", default=None, help="lista de simbolos, para probar")
    ap.add_argument("--horizonte-d", type=int, default=7)
    ap.add_argument("--umbral", type=float, default=1.28,
                    help="p20 de la 1a mitad de las alertas, fijado a priori")
    ap.add_argument("--darts", type=int, default=10)
    ap.add_argument("--dart-window", type=int, default=30)
    ap.add_argument("--dart-excl", type=int, default=7)
    ap.add_argument("--frescura-h", type=int, default=2)
    ap.add_argument("--seed", type=int, default=41)
    ap.add_argument("--limpio-desde", default="2023-01-01",
                    help="las columnas de top traders vienen vacias casi todo 2022 "
                         "en los dumps; el tramo limpio arranca aca")
    ap.add_argument("--rehacer", action="store_true")
    ap.add_argument("--out", default="panel_tt.json")
    args = ap.parse_args()

    if args.paso == "universo":
        paso_universo(args.workers, args.desde, args.hasta)
    elif args.paso == "bajar":
        paso_bajar(args)
    elif args.paso == "panel":
        paso_panel(args)


if __name__ == "__main__":
    sys.stdout.reconfigure(encoding="utf-8")
    main()
