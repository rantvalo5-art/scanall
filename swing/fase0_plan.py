"""
Fase 0 — validar los niveles del plan de trading ANTES de mostrarlos.

No inventa señal ni toca detección: toma las alertas REALES ya emitidas
(archivo_outcomes/screener_outcomes.csv, el feed en vivo — el replay del backtest
genera 1/5 a 1/3 de las alertas y da vuelta la media, así que no sirve acá),
recomputa los niveles de estructura en el instante de cada alerta y mide si el
"objetivo" se toca antes que el "stop".

El número absoluto no dice nada: se compara contra un DARDO PAREADO — la MISMA
moneda entrando a horas al azar de la misma ventana, con LOS MISMOS niveles
relativos (mismo múltiplo de ruptura, mismo múltiplo de objetivo, mismo stop %).
Ese es el estándar del repo; mató ~450 hipótesis.

Corte (preescrito, no se afloja después de ver el número):
  si el margen contra el pareado no tiene el IC95 inferior sobre cero, el nivel
  NO se llama "objetivo": se llama `resistencia cercana` y no lleva R:B.

Concentración en DOS ejes: sacando top-3 símbolos Y sacando top-3 semanas
(el OI shock aguantaba por símbolo y murió en semanas).

Uso:
    py -3.13 fase0_plan.py --bucket BEST
    py -3.13 fase0_plan.py --bucket BEST --darts 20 --out fase0_best.json
"""
import argparse
import json
import random
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import pandas as pd
import requests

HERE = Path(__file__).parent
CSV_DEFAULT = HERE.parent / "archivo_outcomes" / "screener_outcomes.csv"
CACHE = HERE / ".fase0_cache"
CACHE.mkdir(exist_ok=True)

BINANCE = "https://data-api.binance.vision/api/v3/klines"
BINANCE_FALLBACK = "https://api.binance.com/api/v3/klines"

# ── constantes del motor (swing/config.json) — se leen, no se hardcodean ─────
CFG = json.loads((HERE / "config.json").read_text(encoding="utf-8"))
ONE_H_RESIST_LOOKBACK = CFG["hold"]["ONE_H_RESIST_LOOKBACK"]      # 24
MAJOR_STRUCT_LOOKBACK = CFG["hold"]["MAJOR_STRUCT_LOOKBACK"]      # 60
RECENT_LOOKBACK       = CFG["indicators"]["RECENT_LOOKBACK"]      # 15
PREBREAK_NEAR_MAX     = CFG["prebreak"]["PREBREAK_NEAR_MAX"]      # 0.012
STOP_PCT              = CFG["exit_mgmt"]["STOP_PCT"]              # 0.10 (bloqueante resuelto)
WINDOW_DAYS           = CFG["exit_mgmt"]["WINDOW_DAYS"]           # 7

TRIGGER_HOURS = 48        # ventana para que dispare la entrada stop-buy (COILING/PREBREAK)
NO_BREAK = ("COILING", "PREBREAK")   # todavía no rompieron -> entrada al romper
HOUR_MS = 3_600_000
DAY_MS = 86_400_000


# ════════════════════════════════════════════════════════════════════════════
# Descarga de klines (cache propio; el de backtest.py está fragmentado)
# ════════════════════════════════════════════════════════════════════════════
def _get(params, retries=4):
    for base in (BINANCE, BINANCE_FALLBACK):
        for a in range(retries):
            try:
                r = requests.get(base, params=params, timeout=20)
                if r.status_code == 200:
                    return r.json()
                if r.status_code in (429, 418):
                    time.sleep(2 ** a)
                    continue
                break
            except requests.exceptions.RequestException:
                time.sleep(1)
    return None


def fetch_range(symbol, interval, start_ms, end_ms):
    rows, cursor = [], start_ms
    while cursor < end_ms:
        data = _get({"symbol": symbol, "interval": interval,
                     "startTime": cursor, "endTime": end_ms, "limit": 1000})
        if not data:
            break
        rows.extend(data)
        cursor = data[-1][6] + 1
        if len(data) < 1000:
            break
    if not rows:
        return None
    df = pd.DataFrame(rows, columns=[
        "open_time", "open", "high", "low", "close", "volume",
        "close_time", "qv", "trades", "tbb", "tbq", "ign"])
    for c in ("open", "high", "low", "close"):
        df[c] = df[c].astype(float)
    df["open_time"] = df["open_time"].astype("int64")
    return (df[["open_time", "open", "high", "low", "close"]]
            .sort_values("open_time").drop_duplicates("open_time").reset_index(drop=True))


def load_klines(symbol, interval, start_ms, end_ms):
    p = CACHE / f"{symbol}_{interval}_{start_ms}_{end_ms}.pkl"
    if p.exists():
        try:
            return pd.read_pickle(p)
        except Exception:
            pass
    df = fetch_range(symbol, interval, start_ms, end_ms)
    if df is not None and len(df):
        df.to_pickle(p)
    return df


def bulk_load(symbols, interval, start_ms, end_ms, workers=12):
    out, done = {}, 0
    with ThreadPoolExecutor(max_workers=workers) as ex:
        futs = {ex.submit(load_klines, s, interval, start_ms, end_ms): s for s in symbols}
        for f in as_completed(futs):
            s = futs[f]
            try:
                df = f.result()
            except Exception:
                df = None
            if df is not None and len(df) > 60:
                out[s] = df
            done += 1
            if done % 50 == 0:
                print(f"    {interval}: {done}/{len(symbols)}", flush=True)
    return out


# ════════════════════════════════════════════════════════════════════════════
# Niveles: se recomputan igual que el motor (rolling(N).max().shift(2))
# ════════════════════════════════════════════════════════════════════════════
def bar_index(df, ts_ms):
    """Índice de la vela que CONTIENE ts_ms (la que está en formación en ese instante)."""
    i = int(np.searchsorted(df["open_time"].values, ts_ms, side="right")) - 1
    return i if i >= 0 else None


def engine_bar_index(df, ts_ms):
    """Índice de la vela que el motor tomó como actual: la última CERRADA.

    El swing descarta la vela en formación (candle_status == "closed" en las 3.311
    filas del archivo), así que la barra "actual" de analyze_at_time es la anterior
    a la que contiene alerted_at. Verificado: con este offset el recent_max
    recomputado reproduce el ref_price exportado al 100%; sin él, al 64%.
    """
    i = bar_index(df, ts_ms)
    return (i - 1) if (i is not None and i >= 1) else None


def struct_levels(df, i):
    """one_h_resist / major_max / recent_max tal como los calcula analyze_at_time:
    máximo de N barras que TERMINA 2 barras antes de la actual (shift(2))."""
    hi = df["high"].values

    def rmax(n):
        lo = i - (n + 1)
        if lo < 0:
            return None
        v = hi[lo:i - 1]
        return float(v.max()) if len(v) else None

    return rmax(ONE_H_RESIST_LOOKBACK), rmax(MAJOR_STRUCT_LOOKBACK), rmax(RECENT_LOOKBACK)


# ════════════════════════════════════════════════════════════════════════════
# Camino forward: ¿objetivo antes que stop?
# ════════════════════════════════════════════════════════════════════════════
def walk(df1h, t0_ms, price, m_ref, m_target, needs_break, sym, amb_counter):
    """Devuelve dict con el desenlace, o None si no hay datos suficientes.

    - needs_break: la entrada es stop-buy en price*m_ref (COILING/PREBREAK).
      Si no toca ese nivel en TRIGGER_HOURS -> no hay trade ('sin_entrada').
    - si no, la entrada es inmediata al precio de referencia.
    - stop = fill*(1-STOP_PCT); target = price*m_target.
    - ventana total: WINDOW_DAYS desde t0.
    """
    end_ms = t0_ms + WINDOW_DAYS * DAY_MS
    ot = df1h["open_time"].values
    i0 = int(np.searchsorted(ot, t0_ms, side="right"))
    i1 = int(np.searchsorted(ot, end_ms, side="right"))
    if i1 - i0 < 24:
        return None
    hi = df1h["high"].values[i0:i1]
    lo = df1h["low"].values[i0:i1]
    cl = df1h["close"].values[i0:i1]
    tt = ot[i0:i1]

    if needs_break:
        trig = price * m_ref
        trig_lim = t0_ms + TRIGGER_HOURS * HOUR_MS
        k = None
        for j in range(len(hi)):
            if tt[j] > trig_lim:
                break
            if hi[j] >= trig:
                k = j
                break
        if k is None:
            return {"outcome": "sin_entrada", "r": 0.0, "rb": None}
        fill = trig
        start = k
    else:
        fill = price
        start = 0

    target = price * m_target
    stop = fill * (1 - STOP_PCT)
    if target <= fill or stop <= 0:
        return None
    rb = (target - fill) / (fill - stop)

    for j in range(start, len(hi)):
        hit_t = hi[j] >= target
        hit_s = lo[j] <= stop
        if hit_t and hit_s:
            # ambigüedad dentro de la vela -> resolver con 5m
            amb_counter[0] += 1
            first = resolve_5m(sym, int(tt[j]), target, stop)
            if first == "target":
                return {"outcome": "target", "r": rb, "rb": rb}
            if first == "stop":
                return {"outcome": "stop", "r": -1.0, "rb": rb}
            amb_counter[1] += 1
            return {"outcome": "ambiguo", "r": None, "rb": rb}
        if hit_t:
            return {"outcome": "target", "r": rb, "rb": rb}
        if hit_s:
            return {"outcome": "stop", "r": -1.0, "rb": rb}
    final = float(cl[-1])
    return {"outcome": "abierto", "r": (final / fill - 1) / STOP_PCT, "rb": rb}


_5M_CACHE = {}


def resolve_5m(symbol, hour_open_ms, target, stop):
    """Qué se tocó primero DENTRO de la vela 1h ambigua, con velas de 5m."""
    key = (symbol, hour_open_ms)
    if key in _5M_CACHE:
        df = _5M_CACHE[key]
    else:
        df = None
        p = CACHE / f"amb_{symbol}_{hour_open_ms}.pkl"
        if p.exists():
            try:
                df = pd.read_pickle(p)
            except Exception:
                df = None
        if df is None:
            df = fetch_range(symbol, "5m", hour_open_ms, hour_open_ms + HOUR_MS - 1)
            if df is not None:
                df.to_pickle(p)
        _5M_CACHE[key] = df
    if df is None or not len(df):
        return None
    for h, l in zip(df["high"].values, df["low"].values):
        t = h >= target
        s = l <= stop
        if t and s:
            return None          # sigue ambiguo a 5m
        if t:
            return "target"
        if s:
            return "stop"
    return None


# ════════════════════════════════════════════════════════════════════════════
# Bootstrap por SEMANA (la unidad es la semana, no la alerta)
# ════════════════════════════════════════════════════════════════════════════
def boot_ci(values, weeks, n=5000, seed=7):
    """IC95 de la media, remuestreando SEMANAS con reemplazo."""
    if not len(values):
        return (float("nan"), float("nan"))
    rng = random.Random(seed)
    by_week = {}
    for v, w in zip(values, weeks):
        by_week.setdefault(w, []).append(v)
    keys = list(by_week)
    if len(keys) < 2:
        return (float("nan"), float("nan"))
    means = []
    for _ in range(n):
        pool = []
        for _ in range(len(keys)):
            pool.extend(by_week[keys[rng.randrange(len(keys))]])
        if pool:
            means.append(sum(pool) / len(pool))
    means.sort()
    return (means[int(0.025 * len(means))], means[int(0.975 * len(means))])


def summarize(tag, deltas, weeks, unit="pp", scale=100.0):
    if not deltas:
        print(f"  {tag}: sin datos")
        return None
    m = float(np.mean(deltas))
    lo, hi = boot_ci(deltas, weeks)
    cruza = lo > 0
    print(f"  {tag:<26} n={len(deltas):>5}  margen {m*scale:+7.2f}{unit}  "
          f"IC95 [{lo*scale:+7.2f} , {hi*scale:+7.2f}]  {'CRUZA' if cruza else 'no cruza'}")
    return {"n": len(deltas), "mean": m, "ci_lo": lo, "ci_hi": hi, "cruza": bool(cruza)}


def drop_top(deltas, weeks, key_list, k=3):
    """Saca las k claves que más aportan al total y devuelve el resto."""
    agg = {}
    for d, key in zip(deltas, key_list):
        agg[key] = agg.get(key, 0.0) + d
    top = sorted(agg, key=lambda x: -agg[x])[:k]
    keep = [i for i, key in enumerate(key_list) if key not in top]
    return [deltas[i] for i in keep], [weeks[i] for i in keep], top


# ════════════════════════════════════════════════════════════════════════════
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--csv", default=str(CSV_DEFAULT))
    ap.add_argument("--bucket", default="BEST")
    ap.add_argument("--darts", type=int, default=10)
    ap.add_argument("--target", default="both", choices=["res", "major", "both"])
    ap.add_argument("--out", default="fase0_plan.json")
    ap.add_argument("--seed", type=int, default=13)
    ap.add_argument("--dart-window", type=int, default=7,
                    help="días a cada lado de la alerta de donde se sortean los dardos")
    ap.add_argument("--dart-exclude", type=float, default=0.5,
                    help="días alrededor de la alerta que NO se sortean. Con 7 los dardos "
                         "no comparten ninguna vela con la ruta forward de la alerta "
                         "(control de solape).")
    args = ap.parse_args()

    df = pd.read_csv(args.csv)
    df = df[df.bucket == args.bucket].copy()
    df["ts"] = pd.to_datetime(df.alerted_at, utc=True)
    df["ts_ms"] = df.ts.astype("int64") // 10**6
    df = df[df.timeframe.isin(("1h", "4h"))]
    print(f"Alertas {args.bucket}: {len(df)}  símbolos {df.symbol.nunique()}  "
          f"{df.ts.min().date()} → {df.ts.max().date()}")

    # ventana de descarga: 20d antes (estructura 4h × 62 barras) y 9d después (forward)
    start_ms = int(df.ts_ms.min() - 20 * DAY_MS)
    end_ms = int(min(df.ts_ms.max() + 9 * DAY_MS,
                     datetime.now(timezone.utc).timestamp() * 1000))
    syms = sorted(df.symbol.unique())
    print(f"\nDescargando klines de {len(syms)} símbolos "
          f"({datetime.fromtimestamp(start_ms / 1000, timezone.utc).date()} → "
          f"{datetime.fromtimestamp(end_ms / 1000, timezone.utc).date()})")
    k1h = bulk_load(syms, "1h", start_ms, end_ms)
    k4h = bulk_load(syms, "4h", start_ms, end_ms)
    print(f"  1h: {len(k1h)}  4h: {len(k4h)}")

    # alertas sin 7d de forward maduro: fuera
    data_end = max((int(d.open_time.iloc[-1]) for d in k1h.values()), default=end_ms)
    mature = data_end - WINDOW_DAYS * DAY_MS
    n0 = len(df)
    df = df[df.ts_ms <= mature]
    print(f"  maduras (≥{WINDOW_DAYS}d de forward): {len(df)} / {n0}")

    rng = random.Random(args.seed)
    amb = [0, 0]           # [velas 1h ambiguas, irresueltas aun a 5m]
    recs = []
    skipped = {"sin_kline": 0, "sin_estructura": 0, "sin_target": 0,
               "sin_walk": 0, "pocos_dardos": 0}
    ref_check = []
    sin_target_por_senal = {}

    for _, a in df.iterrows():
        sym, tf, sig = a.symbol, a.timeframe, a.signal_type
        kt = (k1h if tf == "1h" else k4h).get(sym)
        kw = k1h.get(sym)
        if kt is None or kw is None:
            skipped["sin_kline"] += 1
            continue
        i = engine_bar_index(kt, int(a.ts_ms))
        if i is None or i < MAJOR_STRUCT_LOOKBACK + 2:
            skipped["sin_estructura"] += 1
            continue
        one_h_resist, major_max, recent_max = struct_levels(kt, i)
        price = float(a.entry_price)
        if price <= 0:
            continue
        # control de indexado: recent_max recomputado vs ref_price exportado
        if recent_max and sig in ("PREBREAK", "COILING", "BREAKOUT") and float(a.ref_price) > 0:
            ref_check.append(abs(recent_max / float(a.ref_price) - 1))

        needs_break = sig in NO_BREAK and float(a.ref_price) > price
        m_ref = float(a.ref_price) / price if needs_break else 1.0

        for name, lvl in (("res", one_h_resist), ("major", major_max)):
            if args.target != "both" and args.target != name:
                continue
            if not lvl or lvl <= price * m_ref:
                # el nivel ya quedó por debajo de la entrada: "sin resistencia a la vista"
                skipped["sin_target"] += 1
                sin_target_por_senal.setdefault((sig, name), [0, 0])[0] += 1
                sin_target_por_senal.setdefault((sig, name), [0, 0])[1] += 1
                continue
            sin_target_por_senal.setdefault((sig, name), [0, 0])[1] += 1
            m_target = lvl / price
            real = walk(kw, int(a.ts_ms), price, m_ref, m_target, needs_break, sym, amb)
            if real is None:
                skipped["sin_walk"] += 1
                continue
            if real["outcome"] == "ambiguo":
                continue
            # ── dardo pareado: misma moneda, hora al azar de ±7d, MISMA geometría
            dh, dr, nd = [], [], 0
            lo_ms = int(a.ts_ms) - args.dart_window * DAY_MS
            hi_ms = int(a.ts_ms) + args.dart_window * DAY_MS
            for _ in range(args.darts * 4):
                if nd >= args.darts:
                    break
                t = rng.randrange(lo_ms, hi_ms)
                if abs(t - int(a.ts_ms)) < args.dart_exclude * DAY_MS:
                    continue
                j = bar_index(kw, t)
                if j is None or j <= 0:
                    continue
                p0 = float(kw["open"].values[j])
                if p0 <= 0:
                    continue
                d = walk(kw, int(kw["open_time"].values[j]), p0, m_ref, m_target,
                         needs_break, sym, amb)
                if d is None or d["outcome"] == "ambiguo":
                    continue
                dh.append(1.0 if d["outcome"] == "target" else 0.0)
                if d["r"] is not None:
                    dr.append(d["r"])
                nd += 1
            if nd < max(3, args.darts // 2):
                skipped["pocos_dardos"] += 1
                continue
            recs.append({
                "symbol": sym, "signal": sig, "tf": tf, "target": name,
                "ts": a.alerted_at, "week": a.ts.strftime("%G-W%V"),
                "rb": real["rb"], "outcome": real["outcome"],
                "hit": 1.0 if real["outcome"] == "target" else 0.0,
                "hit_dart": float(np.mean(dh)),
                "r": real["r"], "r_dart": float(np.mean(dr)) if dr else None,
                "n_darts": nd,
            })

    print(f"\nDescartadas: {skipped}")
    print("\nCobertura de objetivo (alertas con resistencia POR ENCIMA de la entrada):")
    for (sig, name), (sin, tot) in sorted(sin_target_por_senal.items()):
        print(f"    {sig:<10} {name:<6} con objetivo {tot - sin:>4}/{tot:<4} "
              f"({(tot - sin) / tot * 100:5.1f}%)")
    print(f"\nAmbigüedad 1h: {amb[0]} velas -> irresueltas aun a 5m: {amb[1]}")
    if ref_check:
        ok = sum(1 for x in ref_check if x < 1e-6) / len(ref_check)
        print(f"Control de indexado (recent_max recomputado == ref_price exportado): "
              f"{ok * 100:.1f}% exacto sobre {len(ref_check)}")
    if not recs:
        print("Sin registros. Fin.")
        return

    R = pd.DataFrame(recs)
    out = {"n_alertas": int(len(df)), "stop_pct": STOP_PCT, "window_days": WINDOW_DAYS,
           "trigger_hours": TRIGGER_HOURS, "darts": args.darts,
           "dart_window_dias": args.dart_window, "dart_exclude_dias": args.dart_exclude,
           "ambiguedad_1h": amb[0], "ambiguedad_irresuelta": amb[1],
           "cobertura_objetivo": {f"{k[0]}|{k[1]}": {"con_objetivo": v[1] - v[0], "total": v[1]}
                                  for k, v in sin_target_por_senal.items()},
           "control_indexado": (sum(1 for x in ref_check if x < 1e-6) / len(ref_check)
                                if ref_check else None),
           "targets": {}}

    for name, sub in R.groupby("target"):
        print(f"\n{'=' * 78}\nOBJETIVO = {name}   (n={len(sub)})\n{'=' * 78}")
        print(f"  R:B teórico  mediana {sub.rb.median():.2f}  "
              f"p25 {sub.rb.quantile(.25):.2f}  p75 {sub.rb.quantile(.75):.2f}")
        print("  desenlaces:", sub.outcome.value_counts().to_dict())
        print(f"  tocó objetivo primero: alerta {sub.hit.mean() * 100:.1f}%  "
              f"dardo {sub.hit_dart.mean() * 100:.1f}%")

        d_hit = (sub.hit - sub.hit_dart).tolist()
        wk = sub.week.tolist()
        sy = sub.symbol.tolist()
        res = {"n": len(sub),
               "rb_teorico_mediana": float(sub.rb.median()),
               "hit_alerta": float(sub.hit.mean()),
               "hit_dardo": float(sub.hit_dart.mean()),
               "outcomes": {k: int(v) for k, v in sub.outcome.value_counts().items()}}

        print("\n  — margen objetivo-antes-que-stop vs dardo pareado —")
        res["margen"] = summarize("todo", d_hit, wk)
        d1, w1, top_s = drop_top(d_hit, wk, sy)
        res["margen_sin_top3_simbolos"] = summarize("sin top-3 símbolos", d1, w1)
        print(f"      (sacados: {', '.join(map(str, top_s))})")
        d2, w2, top_w = drop_top(d_hit, wk, wk)
        res["margen_sin_top3_semanas"] = summarize("sin top-3 semanas", d2, w2)
        print(f"      (sacadas: {', '.join(map(str, top_w))})")

        # R:B realizado (expectativa en R) — teórico vs lo que pagó
        rr = sub[sub.r.notna() & sub.r_dart.notna()]
        if len(rr):
            d_r = (rr.r - rr.r_dart).tolist()
            print("\n  — expectativa en R (target=+R:B, stop=−1, abierto=retorno/stop) —")
            print(f"      alerta {rr.r.mean():+.3f} R   dardo {rr.r_dart.mean():+.3f} R")
            res["R_alerta"] = float(rr.r.mean())
            res["R_dardo"] = float(rr.r_dart.mean())
            res["margen_R"] = summarize("margen R", d_r, rr.week.tolist(), unit="R", scale=1.0)
            print(f"      R:B teórico {rr.rb.median():.2f}  vs  "
                  f"expectativa realizada {rr.r.mean():+.3f} R")
            res["rb_teorico_vs_realizado"] = {"teorico": float(rr.rb.median()),
                                              "realizado_R": float(rr.r.mean())}
        print("\n  — por señal —")
        res["por_senal"] = {}
        for s, ss in sub.groupby("signal"):
            if len(ss) < 20:
                continue
            m = float((ss.hit - ss.hit_dart).mean())
            print(f"      {s:<10} n={len(ss):>4}  alerta {ss.hit.mean() * 100:5.1f}%  "
                  f"dardo {ss.hit_dart.mean() * 100:5.1f}%  margen {m * 100:+6.2f}pp")
            res["por_senal"][s] = {"n": len(ss), "hit": float(ss.hit.mean()),
                                   "hit_dart": float(ss.hit_dart.mean()), "margen": m}
        out["targets"][name] = res

    Path(args.out).write_text(json.dumps(out, indent=2, ensure_ascii=False), encoding="utf-8")
    rows_csv = Path(args.out).with_suffix("").as_posix() + "_rows.csv"
    R.to_csv(rows_csv, index=False)
    print(f"\nGuardado: {args.out} (+ .csv con las {len(R)} filas)")

    print(f"\n{'=' * 78}\nVEREDICTO (criterio preescrito)\n{'=' * 78}")
    for name, r in out["targets"].items():
        m = r.get("margen") or {}
        s1 = r.get("margen_sin_top3_simbolos") or {}
        s2 = r.get("margen_sin_top3_semanas") or {}
        ok = m.get("cruza") and s1.get("cruza") and s2.get("cruza")
        print(f"  {name}: " + ("OBJETIVO (cruza en los 3 ejes)" if ok else
                               "NO cruza → se llama `resistencia cercana`, sin R:B"))


if __name__ == "__main__":
    main()
