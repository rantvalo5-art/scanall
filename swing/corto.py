"""
¿Se puede cobrar la mediana estable del feed poniéndose CORTO?

`atribucion.py` dejó un número raro por lo estable: la mediana del feed BEST es
−3,8% a 7d y se repite en las dos mitades de la muestra. La media, en cambio, es
~0 y vive entera en un símbolo. O sea: el trade típico pierde, y lo que salva la
media es una cola derecha enorme.

Eso invita al corto. Pero el corto tiene el problema espejo: la cola que salvaba
al largo ahora te mata. Este script mide cuánto de eso aguanta un stop.

Lo importante es el MODELO DE RELLENO del stop, que es donde se esconde el
autoengaño:
  exacto  — rellena en el nivel del stop. Es lo que asume cualquier backtest
            ingenuo, y es falso: la cola de estas monedas es un SALTO.
  cierre  — rellena al cierre de la vela de 1h que cruzó el nivel.
  peor    — rellena en el máximo de esa vela.

NO modela funding ni borrow. En perps el short COBRA funding cuando es positivo
(a favor), pero no todas estas monedas tienen perp y el spot short paga borrow.

    py -3.13 corto.py --bucket BEST
"""
import argparse
import json
from pathlib import Path

import numpy as np
import pandas as pd

import fase0_plan as F
import dip_previo as D

H_DEFAULT = 168
STOPS = (0.10, 0.15, 0.20, 0.30, 0.50)
FILLS = ("exacto", "cierre", "peor")


def corto(d, i, stop, fill, H, costs):
    """Short en el cierre de la barra i, hold H barras. stop = % adverso."""
    hi, cl = d["high"].values, d["close"].values
    if i is None or i + H >= len(cl) or cl[i] <= 0:
        return None
    e = float(cl[i])
    if stop:
        lim = e * (1 + stop)
        seg = hi[i + 1:i + 1 + H]
        idx = np.flatnonzero(seg >= lim)
        if len(idx):
            k = i + 1 + idx[0]
            px = {"exacto": lim,
                  "cierre": max(float(cl[k]), lim),
                  "peor": float(hi[k])}[fill]
            return -(px / e - 1) - costs
    return -(float(cl[i + H]) / e - 1) - costs


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--csv", default=str(F.CSV_DEFAULT))
    ap.add_argument("--bucket", default="BEST")
    ap.add_argument("--horizonte-h", type=int, default=H_DEFAULT)
    ap.add_argument("--out", default="corto.json")
    args = ap.parse_args()
    H, C = args.horizonte_h, D.COSTS

    df = pd.read_csv(args.csv)
    df = df[df.bucket == args.bucket].copy()
    df["ts"] = pd.to_datetime(df.alerted_at, utc=True)
    df["ts_ms"] = df.ts.astype("int64") // 10**6
    df["week"] = df.ts.dt.strftime("%G-W%V")
    K = F.bulk_load(sorted(df.symbol.unique()), "1h",
                    int(df.ts_ms.min() - 30 * D.DAY_MS),
                    int(df.ts_ms.max() + 9 * D.DAY_MS))
    de = max(int(d.open_time.iloc[-1]) for d in K.values())
    df = df[df.ts_ms <= de - H * D.HOUR_MS]

    rows = []
    for _, a in df.iterrows():
        d = K.get(a.symbol)
        if d is None:
            continue
        i = F.engine_bar_index(d, int(a.ts_ms))
        r = {"symbol": a.symbol, "signal": a.signal_type, "week": a.week, "ts": a.ts}
        largo = D.fwd(d, i, H)
        if largo is None:
            continue
        r["largo"] = largo
        r["sin_stop"] = corto(d, i, None, "exacto", H, C)
        ok = r["sin_stop"] is not None
        for s in STOPS:
            for f in FILLS:
                v = corto(d, i, s, f, H, C)
                if v is None:
                    ok = False
                    break
                r[f"{s}_{f}"] = v
            if not ok:
                break
        if ok:
            rows.append(r)
    T = pd.DataFrame(rows)
    mid = T.ts.quantile(0.5)
    print(f"CORTO sobre cada alerta {args.bucket}, {H}h, neto {C:.2%}. n={len(T)}")
    print(f"  referencia LARGO: media {T.largo.mean()*100:+.2f}%  "
          f"mediana {T.largo.median()*100:+.2f}%")
    print(f"  corto SIN stop:   media {T.sin_stop.mean()*100:+.2f}%  "
          f"mediana {T.sin_stop.median()*100:+.2f}%  peor {T.sin_stop.min()*100:+.1f}%\n")

    print(f"{'stop':>6} {'relleno':>9} {'media':>9} {'mediana':>9} {'pos':>6} "
          f"{'IC95 media':>19} {'1a mitad':>10} {'2a mitad':>10} {'peor':>10}")
    out = []
    for s in STOPS:
        for f in FILLS:
            v = T[f"{s}_{f}"]
            lo, hi = F.boot_ci(v.tolist(), T.week.tolist())
            a = T[T.ts <= mid][f"{s}_{f}"]
            b = T[T.ts > mid][f"{s}_{f}"]
            print(f"{s:>6.0%} {f:>9} {v.mean()*100:>+8.2f}% {v.median()*100:>+8.2f}% "
                  f"{(v>0).mean()*100:>5.1f}% [{lo*100:>+6.2f},{hi*100:>+6.2f}] "
                  f"{a.mean()*100:>+9.2f}% {b.mean()*100:>+9.2f}% {v.min()*100:>+9.1f}%")
            out.append({"stop": s, "fill": f, "media": float(v.mean()),
                        "mediana": float(v.median()), "pos": float((v > 0).mean()),
                        "ci": [lo, hi], "mitad1": float(a.mean()),
                        "mitad2": float(b.mean()), "peor": float(v.min())})
        print()

    v = T["0.2_peor"]
    print(f"Con stop +20% y relleno pesimista: {(v < -0.20).mean()*100:.1f}% de los trades")
    print(f"  salen PEOR que −20%.  peor {v.min()*100:.1f}%  "
          f"p1 {np.percentile(v,1)*100:.1f}%  p5 {np.percentile(v,5)*100:.1f}%")
    print("\n  Un stop NO acota un SALTO. Ahí muere la idea: la media entera del")
    print("  corto con stop es el supuesto de relleno exacto.")

    Path(args.out).write_text(json.dumps(
        {"bucket": args.bucket, "horizonte_h": H, "costs": C, "n": len(T),
         "largo_media": float(T.largo.mean()), "largo_mediana": float(T.largo.median()),
         "celdas": out}, indent=2, ensure_ascii=False), encoding="utf-8")
    print(f"\nGuardado: {args.out}")


if __name__ == "__main__":
    main()
