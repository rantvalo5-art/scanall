"""
¿Sirve operar sólo las alertas de monedas que YA subieron y ahora están abajo?

Condición, evaluada en el instante de cada alerta real:
  pico   = máximo de los últimos N días (sin la barra actual)
  run_up = pico / cierre de hace N días − 1      ("previamente subió")
  dd     = (pico − precio) / pico                 ("ahora está abajo")
  pasa si run_up >= A y dd >= B

Mide DOS cosas distintas, que se confunden fácil:

  (1) FILTRAR: ¿las alertas que pasan la condición rinden más que el feed BEST
      entero? Es la pregunta práctica ("¿opero sólo estas?").
  (2) DARDO PAREADO CONDICIONADO: contra la MISMA moneda, a horas al azar que
      TAMBIÉN cumplen la condición. Aísla si la alerta aporta algo *encima* de la
      condición, o si lo que se está midiendo es la condición sola.

Sin (2), un filtro que sube el retorno puede ser sólo "esa moneda en ese estado
sube", que se cobra sin el screener. Ese es el estándar del repo.

Retornos netos de COSTS (0.30% round-trip). Bootstrap por SEMANA. Concentración
en los dos ejes (top-3 símbolos, top-3 semanas). Grilla de 27 celdas con
corrección de Benjamini-Hochberg: 27 tests sin corregir garantizan un ganador.

    py -3.13 dip_previo.py --bucket BEST
"""
import argparse
import json
import random
from pathlib import Path

import numpy as np
import pandas as pd

import fase0_plan as F   # reusa loader de klines, bar_index y bootstrap por semana

COSTS = 0.003            # round-trip, estándar del repo
DAY_MS = F.DAY_MS
HOUR_MS = F.HOUR_MS

# Grilla PREREGISTRADA (se reportan las 27 celdas, no sólo la mejor)
GRID_N = (7, 14, 30)            # días de mirada atrás
GRID_RUNUP = (0.15, 0.30, 0.50)  # cuánto tiene que haber subido
GRID_DD = (0.10, 0.20, 0.30)     # cuánto tiene que haber corregido desde el pico


def condicion(df, i, n_dias, runup_min, dd_min):
    """Evalúa la condición en la barra i de un df de 1h. None si no hay historia."""
    barras = n_dias * 24
    if i is None or i < barras + 1:
        return None
    hi = df["high"].values
    cl = df["close"].values
    ventana = hi[i - barras:i]          # sin la barra actual
    if not len(ventana):
        return None
    pico = float(ventana.max())
    base = float(cl[i - barras])
    price = float(cl[i])
    if pico <= 0 or base <= 0 or price <= 0:
        return None
    run_up = pico / base - 1
    dd = (pico - price) / pico
    return {"pico": pico, "run_up": run_up, "dd": dd, "price": price,
            "pasa": run_up >= runup_min and dd >= dd_min}


def fwd(df, i, horas):
    """Retorno neto de costos desde el cierre de la barra i hasta i+horas."""
    cl = df["close"].values
    j = i + horas
    if i is None or j >= len(cl) or cl[i] <= 0:
        return None
    return float(cl[j]) / float(cl[i]) - 1 - COSTS


def bh(pvals, q=0.10):
    """Benjamini-Hochberg: devuelve el umbral de p que sobrevive con FDR q."""
    ps = sorted(pvals)
    m = len(ps)
    corte = 0.0
    for k, p in enumerate(ps, 1):
        if p <= k / m * q:
            corte = p
    return corte


def p_boot(deltas, weeks, n=4000, seed=11):
    """p bilateral por bootstrap de semanas: fracción de remuestreos que cruzan 0."""
    if len(deltas) < 5:
        return 1.0
    rng = random.Random(seed)
    by = {}
    for d, w in zip(deltas, weeks):
        by.setdefault(w, []).append(d)
    keys = list(by)
    if len(keys) < 3:
        return 1.0
    obs = float(np.mean(deltas))
    neg = 0
    for _ in range(n):
        pool = []
        for _ in range(len(keys)):
            pool.extend(by[keys[rng.randrange(len(keys))]])
        m = sum(pool) / len(pool)
        if (m <= 0) if obs > 0 else (m >= 0):
            neg += 1
    return min(1.0, 2 * neg / n)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--csv", default=str(F.CSV_DEFAULT))
    ap.add_argument("--bucket", default="BEST")
    ap.add_argument("--horizonte-h", type=int, default=168)   # 7d, horizonte del swing
    ap.add_argument("--darts", type=int, default=12)
    ap.add_argument("--out", default="dip_previo.json")
    ap.add_argument("--seed", type=int, default=17)
    args = ap.parse_args()

    df = pd.read_csv(args.csv)
    if args.bucket != "TODOS":
        df = df[df.bucket == args.bucket]
    df = df.copy()
    df["ts"] = pd.to_datetime(df.alerted_at, utc=True)
    df["ts_ms"] = df.ts.astype("int64") // 10**6
    df["week"] = df.ts.dt.strftime("%G-W%V")
    print(f"Alertas {args.bucket}: {len(df)}  símbolos {df.symbol.nunique()}")

    start_ms = int(df.ts_ms.min() - 45 * DAY_MS)   # 30d de mirada atrás + colchón
    end_ms = int(df.ts_ms.max() + 9 * DAY_MS)
    syms = sorted(df.symbol.unique())
    print(f"Klines 1h de {len(syms)} símbolos...")
    K = F.bulk_load(syms, "1h", start_ms, end_ms)
    print(f"  {len(K)} con datos")

    data_end = max(int(d.open_time.iloc[-1]) for d in K.values())
    maduras = data_end - args.horizonte_h * HOUR_MS
    df = df[df.ts_ms <= maduras]
    print(f"  maduras: {len(df)}")

    # ── pase 1: retorno forward y features de la condición, por alerta ────────
    base = []
    for _, a in df.iterrows():
        d = K.get(a.symbol)
        if d is None:
            continue
        i = F.engine_bar_index(d, int(a.ts_ms))
        r = fwd(d, i, args.horizonte_h)
        if r is None:
            continue
        fila = {"symbol": a.symbol, "signal": a.signal_type, "week": a.week,
                "ts_ms": int(a.ts_ms), "i": i, "r": r}
        for n in GRID_N:
            c = condicion(d, i, n, 0, 0)
            fila[f"run_up_{n}"] = c["run_up"] if c else None
            fila[f"dd_{n}"] = c["dd"] if c else None
        base.append(fila)
    B = pd.DataFrame(base)
    print(f"\nAlertas con forward {args.horizonte_h}h: {len(B)}")
    print(f"Retorno medio del feed {args.bucket} (neto {COSTS:.1%}): "
          f"{B.r.mean() * 100:+.2f}%  mediana {B.r.median() * 100:+.2f}%  "
          f"positivas {(B.r > 0).mean() * 100:.1f}%")

    rng = random.Random(args.seed)
    filas = []
    print(f"\n{'=' * 96}")
    print(f"{'N':>3} {'run_up':>7} {'dd':>5} | {'n':>4} {'ret':>8} {'vs feed':>9} "
          f"{'p':>6} | {'dardo':>8} {'margen':>8} {'IC95':>18} {'p':>6}")
    print("=" * 96)

    for n in GRID_N:
        for ru in GRID_RUNUP:
            for dd in GRID_DD:
                sel = B[(B[f"run_up_{n}"] >= ru) & (B[f"dd_{n}"] >= dd)]
                if len(sel) < 25:
                    print(f"{n:>3} {ru:>7.0%} {dd:>5.0%} |  n={len(sel)} — muy pocas")
                    continue

                # (1) ¿mejora sobre el feed entero?
                d_feed = (sel.r - B.r.mean()).tolist()
                p_feed = p_boot(d_feed, sel.week.tolist())

                # (2) dardo pareado CONDICIONADO: misma moneda, horas al azar que
                #     también cumplen la condición
                margen, wks = [], []
                for _, a in sel.iterrows():
                    d = K.get(a.symbol)
                    if d is None:
                        continue
                    lo, hi = a.ts_ms - 21 * DAY_MS, a.ts_ms + 21 * DAY_MS
                    got, tries = [], 0
                    while len(got) < args.darts and tries < args.darts * 25:
                        tries += 1
                        t = rng.randrange(int(lo), int(hi))
                        if abs(t - a.ts_ms) < 2 * DAY_MS:
                            continue
                        j = F.bar_index(d, t)
                        c = condicion(d, j, n, ru, dd)
                        if not c or not c["pasa"]:
                            continue
                        rr = fwd(d, j, args.horizonte_h)
                        if rr is not None:
                            got.append(rr)
                    if len(got) >= 4:
                        margen.append(a.r - float(np.mean(got)))
                        wks.append(a.week)

                if len(margen) < 20:
                    print(f"{n:>3} {ru:>7.0%} {dd:>5.0%} | ret {sel.r.mean()*100:+7.2f}% "
                          f"— sin dardos condicionados suficientes (n={len(margen)})")
                    continue
                lo_ci, hi_ci = F.boot_ci(margen, wks)
                p_dardo = p_boot(margen, wks)
                m = float(np.mean(margen))
                print(f"{n:>3} {ru:>7.0%} {dd:>5.0%} | {len(sel):>4} "
                      f"{sel.r.mean()*100:+7.2f}% {np.mean(d_feed)*100:+8.2f}pp "
                      f"{p_feed:>6.3f} | {(sel.r.mean()-m)*100:+7.2f}% {m*100:+7.2f}pp "
                      f"[{lo_ci*100:+6.2f},{hi_ci*100:+6.2f}] {p_dardo:>6.3f}")
                filas.append({"N": n, "run_up": ru, "dd": dd, "n": len(sel),
                              "ret": float(sel.r.mean()),
                              "vs_feed": float(np.mean(d_feed)), "p_feed": p_feed,
                              "n_dardo": len(margen), "margen": m,
                              "ci_lo": lo_ci, "ci_hi": hi_ci, "p_dardo": p_dardo,
                              "_margen": margen, "_wks": wks,
                              "_syms": sel.symbol.tolist()})

    if not filas:
        print("\nNinguna celda con datos suficientes.")
        return

    print(f"\n{'=' * 96}\nCORRECCIÓN POR MÚLTIPLES COMPARACIONES ({len(filas)} celdas con datos)")
    corte = bh([f["p_dardo"] for f in filas], q=0.10)
    print(f"  Benjamini-Hochberg FDR 10% → sobrevive p <= {corte:.4f}")
    vivos = [f for f in filas if f["p_dardo"] <= corte and f["margen"] > 0]
    print(f"  celdas con margen POSITIVO que sobreviven: {len(vivos)}")

    for f in vivos:
        d2, w2, top_s = F.drop_top(f["_margen"], f["_wks"], f["_syms"])
        d3, w3, top_w = F.drop_top(f["_margen"], f["_wks"], f["_wks"])
        print(f"\n  N={f['N']} run_up>={f['run_up']:.0%} dd>={f['dd']:.0%}  "
              f"margen {f['margen']*100:+.2f}pp")
        F.summarize("sin top-3 símbolos", d2, w2)
        print(f"      (sacados: {', '.join(top_s)})")
        F.summarize("sin top-3 semanas", d3, w3)
        print(f"      (sacadas: {', '.join(top_w)})")

    for f in filas:
        for k in ("_margen", "_wks", "_syms"):
            f.pop(k, None)
    Path(args.out).write_text(json.dumps(
        {"bucket": args.bucket, "horizonte_h": args.horizonte_h, "costs": COSTS,
         "ret_feed": float(B.r.mean()), "celdas": filas,
         "bh_corte": corte, "sobreviven": len(vivos)},
        indent=2, ensure_ascii=False), encoding="utf-8")
    print(f"\nGuardado: {args.out}")

    print(f"\n{'=' * 96}\nVEREDICTO")
    if not vivos:
        print("  NINGUNA celda cruza el dardo pareado condicionado con FDR 10%.")
        print("  O sea: filtrar por 'subió y ahora está abajo' no agrega nada que no")
        print("  se cobre entrando a esa moneda en ese estado a cualquier hora.")
    else:
        print(f"  {len(vivos)} celda(s) sobreviven — revisar concentración arriba antes")
        print("  de creerles.")


if __name__ == "__main__":
    main()
