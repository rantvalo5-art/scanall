"""
LA PILETA — ¿qué distingue a las monedas que COILING/PREBREAK elige mal?

`atribucion.py` mostró que las señales tienen enfermedades distintas: COILING
(universo −1,72%) y PREBREAK (−1,49%) no fallan por CUÁNDO avisan sino por QUÉ
monedas enganchan; BREAKOUT/HOLD tienen universo ~0 o positivo. Todo el tuneo
histórico del repo fue de scoring y de timing, así que "qué moneda" nunca se tocó.

Dos partes:

  GATE — ¿el universo negativo de COILING/PREBREAK aguanta? IC95 por bootstrap de
  semanas + concentración en los dos ejes. Si el −1,72% vive en 3 símbolos o 3
  semanas, la premisa de todo esto es falsa y hay que decirlo antes de seguir.

  BÚSQUEDA — 8 atributos de moneda PREREGISTRADOS, todos disponibles en el
  instante de la alerta (nada forward-looking). Por cada uno: quintiles,
  monotonía, y margen del quintil top contra el bottom. Benjamini-Hochberg sobre
  los 8 — con 8 tests sin corregir siempre gana alguno.

El filtro que sobreviva se evalúa como todo acá: contra el DARDO PAREADO (misma
moneda, horas al azar) y sacando top-3 símbolos y top-3 semanas.

    py -3.13 pileta.py --senales COILING PREBREAK
"""
import argparse
import json
import random
from pathlib import Path

import numpy as np
import pandas as pd

import fase0_plan as F
import dip_previo as D

DAY_MS, HOUR_MS = F.DAY_MS, F.HOUR_MS
CACHE = F.CACHE


# ── klines CON volumen (el cache de fase0 sólo guarda OHLC) ─────────────────
def fetch_vol(symbol, interval, start_ms, end_ms):
    p = CACHE / f"v_{symbol}_{interval}_{start_ms}_{end_ms}.pkl"
    if p.exists():
        try:
            return pd.read_pickle(p)
        except Exception:
            pass
    rows, cursor = [], start_ms
    while cursor < end_ms:
        data = F._get({"symbol": symbol, "interval": interval,
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
    for c in ("open", "high", "low", "close", "qv"):
        df[c] = df[c].astype(float)
    df["open_time"] = df["open_time"].astype("int64")
    df = (df[["open_time", "open", "high", "low", "close", "qv"]]
          .sort_values("open_time").drop_duplicates("open_time").reset_index(drop=True))
    df.to_pickle(p)
    return df


def bulk_vol(syms, interval, start_ms, end_ms, workers=12):
    from concurrent.futures import ThreadPoolExecutor, as_completed
    out, done = {}, 0
    with ThreadPoolExecutor(max_workers=workers) as ex:
        futs = {ex.submit(fetch_vol, s, interval, start_ms, end_ms): s for s in syms}
        for f in as_completed(futs):
            s = futs[f]
            try:
                d = f.result()
            except Exception:
                d = None
            if d is not None and len(d) > 60:
                out[s] = d
            done += 1
            if done % 60 == 0:
                print(f"    {done}/{len(syms)}", flush=True)
    return out


# ── atributos de moneda, todos evaluados en la barra de la alerta ───────────
def atributos(d, i, btc, ts_ms):
    """8 atributos PREREGISTRADOS. None si falta historia."""
    if i is None or i < 24 * 30 + 2:
        return None
    hi, lo_, cl, qv = (d["high"].values, d["low"].values,
                       d["close"].values, d["qv"].values)
    price = float(cl[i])
    if price <= 0:
        return None
    v7 = qv[i - 24 * 7:i]
    if not len(v7) or v7.sum() <= 0:
        return None
    # ATR% clásico sobre 14 barras de 1h, en % del precio
    tr = np.maximum(hi[i - 14:i] - lo_[i - 14:i],
                    np.maximum(abs(hi[i - 14:i] - cl[i - 15:i - 1]),
                               abs(lo_[i - 14:i] - cl[i - 15:i - 1])))
    pico90 = float(hi[max(0, i - 24 * 90):i].max())
    r30 = price / float(cl[i - 24 * 30]) - 1
    # beta y correlación contra BTC sobre 30d de retornos horarios
    beta = corr = None
    if btc is not None:
        j = F.bar_index(btc, ts_ms)
        if j is not None and j > 24 * 30 + 2:
            a = np.diff(np.log(cl[i - 24 * 30:i + 1]))
            b = np.diff(np.log(btc["close"].values[j - 24 * 30:j + 1]))
            n = min(len(a), len(b))
            a, b = a[-n:], b[-n:]
            if n > 50 and b.std() > 0 and a.std() > 0:
                beta = float(np.cov(a, b)[0, 1] / b.var())
                corr = float(np.corrcoef(a, b)[0, 1])
    return {
        "liquidez": float(np.log10(max(v7.mean(), 1.0))),      # A1 volumen quote medio 7d
        "volatilidad": float(tr.mean() / price * 100),          # A2 ATR% 1h
        "dd_90": float((pico90 - price) / pico90) if pico90 > 0 else None,  # A3
        "ret_30d": float(r30),                                  # A4
        "edad_barras": float(i),                                # A5 antigüedad de la serie
        "beta_btc": beta,                                       # A6
        "corr_btc": corr,                                       # A7
        "precio": float(np.log10(price)),                       # A8 orden de magnitud
    }


ATRIBUTOS = ["liquidez", "volatilidad", "dd_90", "ret_30d",
             "edad_barras", "beta_btc", "corr_btc", "precio"]


def spearman(x, y):
    x, y = np.asarray(x, float), np.asarray(y, float)
    ok = ~(np.isnan(x) | np.isnan(y))
    if ok.sum() < 30:
        return None
    return float(np.corrcoef(pd.Series(x[ok]).rank(), pd.Series(y[ok]).rank())[0, 1])


def p_boot_corr(x, y, weeks, n=3000, seed=5):
    """p bilateral del Spearman, remuestreando SEMANAS."""
    obs = spearman(x, y)
    if obs is None:
        return 1.0, None
    rng = random.Random(seed)
    by = {}
    for a, b, w in zip(x, y, weeks):
        by.setdefault(w, []).append((a, b))
    keys = list(by)
    if len(keys) < 3:
        return 1.0, obs
    cnt = 0
    for _ in range(n):
        pool = []
        for _ in range(len(keys)):
            pool.extend(by[keys[rng.randrange(len(keys))]])
        s = spearman([p[0] for p in pool], [p[1] for p in pool])
        if s is None:
            continue
        if (s <= 0) if obs > 0 else (s >= 0):
            cnt += 1
    return min(1.0, 2 * cnt / n), obs


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--csv", default=str(F.CSV_DEFAULT))
    ap.add_argument("--bucket", default="BEST")
    ap.add_argument("--senales", nargs="+", default=["COILING", "PREBREAK"])
    ap.add_argument("--horizonte-h", type=int, default=168)
    ap.add_argument("--darts", type=int, default=10)
    ap.add_argument("--out", default="pileta.json")
    ap.add_argument("--seed", type=int, default=31)
    args = ap.parse_args()
    H = args.horizonte_h

    df = pd.read_csv(args.csv)
    df = df[df.bucket == args.bucket].copy()
    df["ts"] = pd.to_datetime(df.alerted_at, utc=True)
    df["ts_ms"] = df.ts.astype("int64") // 10**6
    df["week"] = df.ts.dt.strftime("%G-W%V")

    start = int(df.ts_ms.min() - 100 * DAY_MS)   # 90d de mirada atrás para dd_90
    end = int(df.ts_ms.max() + 9 * DAY_MS)
    syms = sorted(set(df.symbol.unique()) | {"BTCUSDT"})
    print(f"Alertas {args.bucket}: {len(df)} · {len(syms)} símbolos · con volumen")
    K = bulk_vol(syms, "1h", start, end)
    btc = K.get("BTCUSDT")
    de = max(int(d.open_time.iloc[-1]) for d in K.values())
    df = df[df.ts_ms <= de - H * HOUR_MS]
    print(f"  maduras: {len(df)}  ·  símbolos con klines: {len(K)}\n")

    rng = random.Random(args.seed)
    R = []
    for _, a in df.iterrows():
        d = K.get(a.symbol)
        if d is None:
            continue
        i = F.engine_bar_index(d, int(a.ts_ms))
        r = D.fwd(d, i, H)
        if r is None:
            continue
        got, tries = [], 0
        while len(got) < args.darts and tries < args.darts * 8:
            tries += 1
            t = rng.randrange(int(a.ts_ms) - 21 * DAY_MS, int(a.ts_ms) + 21 * DAY_MS)
            if abs(t - int(a.ts_ms)) < 7 * DAY_MS:
                continue
            rr = D.fwd(d, F.bar_index(d, t), H)
            if rr is not None:
                got.append(rr)
        if len(got) < 4:
            continue
        rb = D.fwd(btc, F.bar_index(btc, int(a.ts_ms)), H)
        at = atributos(d, i, btc, int(a.ts_ms))
        if at is None or rb is None:
            continue
        R.append({"symbol": a.symbol, "signal": a.signal_type, "week": a.week,
                  "r": r, "dardo": float(np.mean(got)), "btc": rb,
                  "universo": float(np.mean(got)) - rb, **at})
    T = pd.DataFrame(R)
    print(f"Filas con todo: {len(T)}\n")

    # ── GATE ────────────────────────────────────────────────────────────────
    print("=" * 100)
    print("GATE — ¿el universo negativo de cada señal aguanta bootstrap y concentración?")
    print("=" * 100)
    print(f"  {'señal':<10} {'n':>4} {'universo':>9} {'IC95':>18} "
          f"{'sin top3 sim':>13} {'sin top3 sem':>13}")
    gate = {}
    for s, ss in T.groupby("signal"):
        if len(ss) < 30:
            continue
        v = ss.universo.tolist()
        w = ss.week.tolist()
        lo, hi = F.boot_ci(v, w)
        d2, w2, _ = F.drop_top(v, w, ss.symbol.tolist())
        d3, w3, _ = F.drop_top(v, w, w)
        # para un universo NEGATIVO el "top aportante" que hay que sacar es el más negativo
        d2b, w2b, _ = F.drop_top([-x for x in v], w, ss.symbol.tolist())
        d3b, w3b, _ = F.drop_top([-x for x in v], w, w)
        print(f"  {s:<10} {len(ss):>4} {np.mean(v) * 100:>+8.2f}% "
              f"[{lo * 100:>+6.2f},{hi * 100:>+6.2f}] "
              f"{-np.mean(d2b) * 100:>+12.2f}% {-np.mean(d3b) * 100:>+12.2f}%")
        gate[s] = {"n": len(ss), "universo": float(np.mean(v)), "ci": [lo, hi],
                   "sin_top3_sim": float(-np.mean(d2b)),
                   "sin_top3_sem": float(-np.mean(d3b))}
    print("\n  (las dos últimas columnas sacan los 3 símbolos / 3 semanas MÁS NEGATIVOS:")
    print("   si el universo se va a ~0 al sacarlos, la premisa no aguanta)")

    # ── BÚSQUEDA ────────────────────────────────────────────────────────────
    sel = T[T.signal.isin(args.senales)]
    print(f"\n{'=' * 100}")
    print(f"BÚSQUEDA — {' + '.join(args.senales)}, n={len(sel)}")
    print("=" * 100)
    print(f"  {'atributo':<14} {'rho(atr,ret)':>13} {'p':>7} | "
          f"quintiles del RETORNO de la alerta (q1 → q5)")
    tests = []
    for at in ATRIBUTOS:
        sub = sel[sel[at].notna()]
        if len(sub) < 60:
            print(f"  {at:<14} pocos datos ({len(sub)})")
            continue
        p, rho = p_boot_corr(sub[at].tolist(), sub.r.tolist(), sub.week.tolist())
        try:
            q = pd.qcut(sub[at], 5, labels=False, duplicates="drop")
        except ValueError:
            continue
        medias = [sub.r[q == k].mean() * 100 for k in sorted(set(q.dropna()))]
        print(f"  {at:<14} {rho:>+13.3f} {p:>7.3f} | " +
              " ".join(f"{m:>+7.2f}%" for m in medias))
        tests.append({"atributo": at, "rho": rho, "p": p, "n": len(sub),
                      "quintiles": medias})

    if tests:
        corte = D.bh([t["p"] for t in tests], q=0.10)
        vivos = [t for t in tests if t["p"] <= corte]
        print(f"\n  Benjamini-Hochberg FDR 10% sobre {len(tests)} atributos → "
              f"sobrevive p <= {corte:.4f}  ({len(vivos)} vivos)")
        for t in vivos:
            at = t["atributo"]
            sub = sel[sel[at].notna()].copy()
            sub["q"] = pd.qcut(sub[at], 5, labels=False, duplicates="drop")
            top, bot = sub[sub.q == sub.q.max()], sub[sub.q == sub.q.min()]
            print(f"\n  ── {at} (rho {t['rho']:+.3f}, p {t['p']:.4f}) ──")
            for nom, g in (("q5 (alto)", top), ("q1 (bajo)", bot)):
                lo, hi = F.boot_ci(g.r.tolist(), g.week.tolist())
                d2, w2, _ = F.drop_top(g.r.tolist(), g.week.tolist(), g.symbol.tolist())
                d3, w3, _ = F.drop_top(g.r.tolist(), g.week.tolist(), g.week.tolist())
                marg = (g.r - g.dardo)
                lo2, hi2 = F.boot_ci(marg.tolist(), g.week.tolist())
                print(f"     {nom:<10} n={len(g):>4} ret {g.r.mean() * 100:>+7.2f}% "
                      f"(med {g.r.median() * 100:>+6.2f}%) IC95 [{lo * 100:+6.2f},{hi * 100:+6.2f}]"
                      f"  sin top3 sim {np.mean(d2) * 100:>+6.2f}%  sem {np.mean(d3) * 100:>+6.2f}%")
                print(f"     {'':<10} vs dardo pareado {marg.mean() * 100:>+6.2f}pp "
                      f"IC95 [{lo2 * 100:+6.2f},{hi2 * 100:+6.2f}] "
                      f"{'CRUZA' if lo2 > 0 else 'no cruza'}")

    Path(args.out).write_text(json.dumps(
        {"gate": gate, "senales": args.senales, "n": len(sel), "tests": tests},
        indent=2, ensure_ascii=False), encoding="utf-8")
    print(f"\nGuardado: {args.out}")


if __name__ == "__main__":
    main()
