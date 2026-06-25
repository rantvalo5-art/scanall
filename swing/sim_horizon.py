"""
NUEVA DIRECCIÓN (#3) — horizonte de salida 14d/30d. Nunca se midió más allá de 7d.

Todo el proyecto evaluó outcomes a 7d fijo. Pregunta: ¿hay RUNNERS multi-semana que el corte a
7d invisibiliza? Test directo: correr el exit trailing shipeado (ARM12/TRAIL8/STOP0) con ventana
7 / 14 / 30 días sobre el MISMO subconjunto de alertas con ≥30d de forward (evita sesgo de
madurez). Si extender la ventana sube el NET → dejar correr es un lever. Si lo baja/iguala → el
trailing ya captura y 7d es suficiente.

Además: ¿el efecto se concentra en alto-atr (donde viven los movers)? Reusa load_1h +
exit_with_time. Corre con `py -3.13`. NO toca producción.
"""
import json, sys, statistics as st
import numpy as np
from sim_exit import load_1h
from sim_portfolio import exit_with_time, parse_ms

DAY_MS = 86_400_000
ARM, TRAIL, STOP = 0.12, 0.08, 0.0
WINDOWS = (7, 14, 30)


def main():
    alerts = [a for a in json.load(open("diag_16w.json"))["main"]
              if a.get("bucket") in ("BEST", "STRONG") and a.get("entry_price")]
    by_sym = {}
    for a in alerts:
        by_sym.setdefault(a["symbol"], []).append(a)

    # subconjunto maduro para 30d + realized por ventana
    res = {W: [] for W in WINDOWS}       # realized NET
    res_atr = {W: {"lo": [], "mid": [], "hi": []} for W in WINDOWS}
    raw = {W: [] for W in WINDOWS}       # retorno crudo close a +W (sin exit)
    n_mature = 0
    for k, (sym, lst) in enumerate(sorted(by_sym.items())):
        if k % 40 == 0:
            print(f"  {k}/{len(by_sym)} símbolos...", file=sys.stderr)
        df = load_1h(sym)
        if df is None:
            continue
        omax = df.open_time.values.max()
        cl = df.close.values; ot = df.open_time.values
        for a in lst:
            e = a["entry_price"]; t0 = parse_ms(a["alerted_at"]); atr = a.get("atr_pct_1d")
            if omax < t0 + 30 * DAY_MS:      # exige 30d de forward → subconjunto único
                continue
            n_mature += 1
            band = "lo" if (atr is None or atr < 3) else ("hi" if atr > 12 else "mid")
            for W in WINDOWS:
                r = exit_with_time(e, t0, df, ARM, TRAIL, STOP, W)
                if r is not None:
                    res[W].append(r[0]); res_atr[W][band].append(r[0])
                # crudo: close a +W días
                j = np.searchsorted(ot, t0 + W * DAY_MS, side="right") - 1
                if j >= 0:
                    raw[W].append(cl[j] / e - 1)

    print(f"\nSubconjunto maduro (≥30d forward): {n_mature} alertas\n")

    def line(tag, vals):
        v = np.array(vals)
        return (f"{tag:>10}  n={len(v):>4}  med {np.median(v)*100:>+6.2f}%  "
                f"mean {v.mean()*100:>+6.2f}%  win {np.mean(v>=0)*100:>3.0f}%")

    print("EXIT TRAILING realizado por ventana (mismo subconjunto):")
    for W in WINDOWS:
        print("  " + line(f"{W}d", res[W]))
    print("\nRetorno CRUDO close a +W (sin exit, referencia de runner):")
    for W in WINDOWS:
        print("  " + line(f"{W}d", raw[W]))
    print("\nExit realizado por banda de atr:")
    for band in ("lo", "mid", "hi"):
        print(f"  -- {band} --")
        for W in WINDOWS:
            if res_atr[W][band]:
                print("    " + line(f"{W}d", res_atr[W][band]))

    print("\n  LECTURA: si mean/med SUBE de 7d→14d→30d, hay runners y dejar correr es lever. Si BAJA,"
          "\n  el trailing ya captura y extender sólo agrega giveback/riesgo. Mirar el crudo: si el crudo"
          "\n  sube mucho pero el exit no, el trailing corta runners (perilla de TRAIL, no de ventana).")

    walk_forward_horizon(by_sym)


def walk_forward_horizon(by_sym):
    """WF 7d vs 14d (subset maduro a 14d = más grande, incluye folds bajistas). Por fold:
    ¿el NET a 14d bate al de 7d? Robustez de regímenes del lever 'dejar correr'."""
    from sim_exit import load_1h
    recs = []   # (ts, r7, r14)
    for sym, lst in by_sym.items():
        df = load_1h(sym)
        if df is None:
            continue
        omax = df.open_time.values.max()
        for a in lst:
            e = a["entry_price"]; t0 = parse_ms(a["alerted_at"])
            if omax < t0 + 14 * DAY_MS:
                continue
            r7 = exit_with_time(e, t0, df, ARM, TRAIL, STOP, 7)
            r14 = exit_with_time(e, t0, df, ARM, TRAIL, STOP, 14)
            if r7 and r14:
                recs.append((t0, r7[0], r14[0]))
    recs.sort()
    ts = np.array([r[0] for r in recs]); r7 = np.array([r[1] for r in recs]); r14 = np.array([r[2] for r in recs])
    ta, tb = ts.min(), ts.max(); span = (tb - ta) // 4
    print(f"\n{'='*62}\n  WALK-FORWARD horizonte 7d vs 14d (subset 14d-maduro: {len(recs)}), 4 folds\n{'='*62}")
    print(f"  {'fold':>4} {'n':>5} | {'7d med':>8} {'7d mean':>8} | {'14d med':>8} {'14d mean':>9} | {'Δmed':>6} {'Δmean':>6}")
    w_med = w_mean = 0
    for f in range(4):
        lo = ta + f*span; hi = tb+1 if f == 3 else ta+(f+1)*span
        m = (ts >= lo) & (ts < hi)
        if m.sum() < 20:
            continue
        a7, a14 = r7[m], r14[m]
        dmed = (np.median(a14)-np.median(a7))*100; dmean = (a14.mean()-a7.mean())*100
        w_med += dmed > 0; w_mean += dmean > 0
        print(f"  {f+1:>4} {m.sum():>5} | {np.median(a7)*100:>+7.2f}% {a7.mean()*100:>+7.2f}% | "
              f"{np.median(a14)*100:>+7.2f}% {a14.mean()*100:>+8.2f}% | {dmed:>+5.2f} {dmean:>+5.2f}")
    print(f"\n  14d bate a 7d: mediana {w_med}/4 | media {w_mean}/4")


if __name__ == "__main__":
    main()
