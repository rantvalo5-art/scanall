"""
¿De DÓNDE sale la pérdida? Descomposición en tres capas.

Todo lo medido hasta ahora contesta "¿el screener tiene ventaja?" (no) pero no
"¿qué me está costando la plata?". Esas son preguntas distintas y la segunda
dice qué se puede arreglar.

    retorno de la alerta  =  BETA  +  UNIVERSO  +  HABILIDAD

  BETA      = lo que hizo BTC en esa misma ventana de 7d.
              No es del screener: es estar en cripto.
  UNIVERSO  = dardo − BTC. Lo que rinde la MONEDA ALERTADA entrando a cualquier
              hora, por encima de BTC. Es la pileta donde el screener pesca:
              qué te cuesta (o te paga) elegir estas monedas y no BTC.
  HABILIDAD = alerta − dardo. Lo único atribuible a "cuándo avisa el bot".

Si HABILIDAD ≈ 0 y UNIVERSO << 0, el problema NO es el screener: es la pileta.
Eso cambia qué se arregla — y no se arregla tocando el scoring.

    py -3.13 atribucion.py --bucket BEST
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


def stats(x):
    x = np.asarray([v for v in x if v is not None])
    if not len(x):
        return None
    return {"n": len(x), "media": float(x.mean()), "mediana": float(np.median(x)),
            "pos": float((x > 0).mean()), "p10": float(np.percentile(x, 10)),
            "p90": float(np.percentile(x, 90))}


def fila(nombre, s, weeks=None, vals=None):
    if not s:
        print(f"  {nombre:<34} sin datos")
        return
    ci = ""
    if weeks is not None and vals is not None:
        lo, hi = F.boot_ci(vals, weeks)
        ci = f"  IC95 [{lo * 100:+6.2f},{hi * 100:+6.2f}]"
    print(f"  {nombre:<34} media {s['media'] * 100:+7.2f}%   "
          f"mediana {s['mediana'] * 100:+7.2f}%   pos {s['pos'] * 100:4.1f}%"
          f"   p10 {s['p10'] * 100:+6.1f}%  p90 {s['p90'] * 100:+7.1f}%{ci}")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--csv", default=str(F.CSV_DEFAULT))
    ap.add_argument("--bucket", default="BEST")
    ap.add_argument("--horizonte-h", type=int, default=168)
    ap.add_argument("--darts", type=int, default=12)
    ap.add_argument("--out", default="atribucion.json")
    ap.add_argument("--seed", type=int, default=23)
    args = ap.parse_args()
    H = args.horizonte_h

    df = pd.read_csv(args.csv)
    if args.bucket != "TODOS":
        df = df[df.bucket == args.bucket]
    df = df.copy()
    df["ts"] = pd.to_datetime(df.alerted_at, utc=True)
    df["ts_ms"] = df.ts.astype("int64") // 10**6
    df["week"] = df.ts.dt.strftime("%G-W%V")

    start = int(df.ts_ms.min() - 45 * DAY_MS)
    end = int(df.ts_ms.max() + 9 * DAY_MS)
    syms = sorted(set(df.symbol.unique()) | {"BTCUSDT", "ETHUSDT"})
    print(f"Alertas {args.bucket}: {len(df)} · {df.symbol.nunique()} símbolos · "
          f"horizonte {H}h · costos {D.COSTS:.2%}")
    K = F.bulk_load(syms, "1h", start, end)
    btc, eth = K.get("BTCUSDT"), K.get("ETHUSDT")
    de = max(int(d.open_time.iloc[-1]) for d in K.values())
    df = df[df.ts_ms <= de - H * HOUR_MS]
    print(f"  maduras: {len(df)}\n")

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
        # dardo: misma moneda, horas al azar de ±21d, sin solapar la ruta de la alerta
        got = []
        tries = 0
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
        rb = D.fwd(btc, F.bar_index(btc, int(a.ts_ms)), H) if btc is not None else None
        re_ = D.fwd(eth, F.bar_index(eth, int(a.ts_ms)), H) if eth is not None else None
        R.append({"symbol": a.symbol, "signal": a.signal_type, "week": a.week,
                  "r": r, "dardo": float(np.mean(got)), "btc": rb, "eth": re_})

    T = pd.DataFrame(R)
    T = T[T.btc.notna()]
    print(f"{'=' * 104}\nDESCOMPOSICIÓN — {len(T)} alertas con las tres capas\n{'=' * 104}")
    fila("ALERTA (lo que pasó)", stats(T.r), T.week.tolist(), T.r.tolist())
    print()
    fila("  BETA  (BTC, misma ventana)", stats(T.btc), T.week.tolist(), T.btc.tolist())
    u = (T.dardo - T.btc)
    fila("  UNIVERSO (dardo − BTC)", stats(u), T.week.tolist(), u.tolist())
    h = (T.r - T.dardo)
    fila("  HABILIDAD (alerta − dardo)", stats(h), T.week.tolist(), h.tolist())
    print()
    fila("  [control] dardo en la moneda", stats(T.dardo), T.week.tolist(), T.dardo.tolist())
    print(f"\n  suma de capas {(T.btc.mean() + u.mean() + h.mean()) * 100:+.2f}%  "
          f"vs alerta {T.r.mean() * 100:+.2f}%  (deben coincidir)")

    print(f"\n{'=' * 104}\n¿CUÁL CAPA MANDA? — cuánto de la pérdida explica cada una\n{'=' * 104}")
    for nom, v in (("BETA", T.btc), ("UNIVERSO", u), ("HABILIDAD", h)):
        lo, hi = F.boot_ci(v.tolist(), T.week.tolist())
        signo = "PAGA" if lo > 0 else ("CUESTA" if hi < 0 else "indistinguible de 0")
        print(f"  {nom:<12} media {v.mean() * 100:+7.2f}%  mediana {np.median(v) * 100:+7.2f}%  "
              f"IC95 [{lo * 100:+6.2f},{hi * 100:+6.2f}]  → {signo}")

    print(f"\n{'=' * 104}\nLA PILETA — mediana de la moneda alertada a cualquier hora\n{'=' * 104}")
    print(f"  Si la mediana del dardo ya es negativa, cualquier estrategia LARGA sobre")
    print(f"  este universo arranca en un pozo, tenga o no ventaja el screener.")
    print(f"    dardo (moneda alertada, hora al azar): mediana "
          f"{np.median(T.dardo) * 100:+.2f}%   positivas {(T.dardo > 0).mean() * 100:.1f}%")
    print(f"    BTC en las mismas ventanas:            mediana "
          f"{np.median(T.btc) * 100:+.2f}%   positivas {(T.btc > 0).mean() * 100:.1f}%")
    if T.eth.notna().any():
        print(f"    ETH en las mismas ventanas:            mediana "
              f"{np.median(T.eth.dropna()) * 100:+.2f}%   positivas "
              f"{(T.eth.dropna() > 0).mean() * 100:.1f}%")

    # concentración de cada capa: ¿alguna vive en pocos símbolos/semanas?
    print(f"\n{'=' * 104}\nCONCENTRACIÓN (top-3 símbolos / top-3 semanas)\n{'=' * 104}")
    for nom, v in (("ALERTA", T.r), ("UNIVERSO", u), ("HABILIDAD", h)):
        d2, w2, ts_ = F.drop_top(v.tolist(), T.week.tolist(), T.symbol.tolist())
        d3, w3, tw = F.drop_top(v.tolist(), T.week.tolist(), T.week.tolist())
        print(f"  {nom:<10} entero {v.mean() * 100:+7.2f}%  "
              f"sin top-3 símbolos {np.mean(d2) * 100:+7.2f}%  "
              f"sin top-3 semanas {np.mean(d3) * 100:+7.2f}%")

    # por señal: ¿alguna capa se comporta distinto?
    print(f"\n{'=' * 104}\nPOR SEÑAL\n{'=' * 104}")
    T2 = T.assign(univ=u, hab=h)
    for s, ss in T2.groupby("signal"):
        if len(ss) < 30:
            continue
        print(f"  {s:<10} n={len(ss):>4}  alerta {ss.r.mean() * 100:+7.2f}% "
              f"(med {ss.r.median() * 100:+6.2f}%)  universo {ss.univ.mean() * 100:+7.2f}%  "
              f"habilidad {ss.hab.mean() * 100:+7.2f}%")

    Path(args.out).write_text(json.dumps({
        "bucket": args.bucket, "horizonte_h": H, "costs": D.COSTS, "n": len(T),
        "alerta": stats(T.r), "btc": stats(T.btc), "universo": stats(u),
        "habilidad": stats(h), "dardo": stats(T.dardo),
    }, indent=2, ensure_ascii=False), encoding="utf-8")
    print(f"\nGuardado: {args.out}")


if __name__ == "__main__":
    main()
