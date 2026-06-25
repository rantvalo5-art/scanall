"""
Prototipo — Gap de RECALL + LATENCIA de detección (Puertas B y A).

Pregunta: "detección tocó techo" solo está probado en precisión/scoring. Faltan dos
dimensiones sin medir:
  B) RECALL: ¿cuántos movers reales del universo NO disparan ninguna señal? Todo el
     análisis previo (meta_label/rank_atr/sim_exit) corre sobre diag_*.json = SOLO
     alertas disparadas; nunca se comparó contra el universo completo.
  A) LATENCIA: de los movers que SÍ agarramos, ¿qué fracción del move ya pasó al entrar,
     por tipo de señal? (confirma/refuta "las tardías entran en el techo").

Insumos: diag_16w.json (alertas) + .backtest_cache/klines_<SYM>_1d_*.pkl (universo ~1360).
Reusa sim_exit.load_tf. NO toca producción ni envía nada. Corre con `py -3.13`.
"""
import json, sys, glob, statistics as st
from datetime import datetime, timezone, timedelta
from pathlib import Path
import numpy as np
import pandas as pd

from sim_exit import load_tf, CACHE_DIR, WINDOW_DAYS

# ---- parámetros ----
THRS = (0.20, 0.30)        # umbrales de "mover" sobre max_high_fwd / ref
COVER_BEFORE = 2           # días antes del inicio del evento donde una alerta cuenta como cobertura
COVER_AFTER = 3            # días después
EVENT_GAP_DAYS = 7         # colapsa mover-days dentro de esta ventana en UN evento
ATR_BANDS = [(0, 3), (3, 6), (6, 9), (9, 12), (12, 15), (15, 999)]
DAY_MS = 86_400_000


def parse_dt(s):
    return datetime.fromisoformat(s.replace("Z", "+00:00"))


def atr_pct(df, i, n=14):
    """ATR(n) Wilder en barra i sobre 1d, en % del close. Devuelve None si no hay historia."""
    if i < n:
        return None
    h = df.high.values; l = df.low.values; c = df.close.values
    trs = []
    for j in range(i - n + 1, i + 1):
        tr = max(h[j] - l[j], abs(h[j] - c[j - 1]), abs(l[j] - c[j - 1]))
        trs.append(tr)
    atr = float(np.mean(trs))
    return atr / c[i] * 100 if c[i] else None


def band_of(atr):
    if atr is None:
        return "n/a"
    for lo, hi in ATR_BANDS:
        if lo <= atr < hi:
            return f"{lo}-{hi if hi < 999 else '+'}"
    return "n/a"


def load_universe(t0_ms, t1_ms):
    """Carga 1d de cada símbolo del cache cuyo rango cubra la ventana del diag."""
    syms = sorted({Path(p).name.split("_1d_")[0].replace("klines_", "")
                   for p in glob.glob(str(CACHE_DIR / "klines_*_1d_*.pkl"))})
    uni = {}
    for k, s in enumerate(syms):
        if k % 300 == 0:
            print(f"  cargando universo... {k}/{len(syms)}", file=sys.stderr)
        df = load_tf(s, "1d")
        if df is None or len(df) < 20:
            continue
        if df.open_time.min() > t0_ms or df.open_time.max() < t0_ms:
            continue
        uni[s] = df.reset_index(drop=True)
    return uni


def mover_events(df, t0_ms, t1_ms, thr):
    """Lista de eventos-mover del símbolo: (idx, start_dt, move, atr). Colapsa runs en uno."""
    ot = df.open_time.values; hi = df.high.values; cl = df.close.values
    events, last_start = [], None
    for i in range(len(df)):
        if ot[i] < t0_ms or ot[i] > t1_ms:
            continue
        ref = cl[i]
        if not ref:
            continue
        end_ms = ot[i] + WINDOW_DAYS * DAY_MS
        mask = (ot > ot[i]) & (ot <= end_ms)
        if mask.sum() < 3:            # forward incompleto -> no madura
            continue
        mx = float(hi[mask].max())
        move = mx / ref - 1
        if move < thr:
            continue
        if last_start is not None and ot[i] - last_start <= EVENT_GAP_DAYS * DAY_MS:
            continue                  # mismo move
        last_start = ot[i]
        net = cl[mask][-1] / ref - 1  # neto al fin de la ventana (¿sostiene o round-trip?)
        events.append((i, datetime.fromtimestamp(ot[i] / 1000, tz=timezone.utc),
                       move, atr_pct(df, i), net))
    return events


def main():
    diag = sys.argv[1] if len(sys.argv) > 1 else "diag_16w.json"
    alerts = json.load(open(diag))["main"]
    t0 = min(parse_dt(a["alerted_at"]) for a in alerts)
    t1 = max(parse_dt(a["alerted_at"]) for a in alerts)
    t0_ms, t1_ms = int(t0.timestamp() * 1000), int(t1.timestamp() * 1000)
    print(f"Diag: {diag} | {len(alerts)} alertas | ventana {t0.date()} -> {t1.date()}")

    # alertas por símbolo: lista de (dt, entry, signal_type)
    by_sym = {}
    for a in alerts:
        by_sym.setdefault(a["symbol"], []).append(
            (parse_dt(a["alerted_at"]), a.get("entry_price"), a.get("signal_type")))
    for s in by_sym:
        by_sym[s].sort()

    print("Cargando universo 1d desde cache...", file=sys.stderr)
    uni = load_universe(t0_ms, t1_ms)
    print(f"Universo: {len(uni)} símbolos | con alerta: {len(by_sym)}")

    for thr in THRS:
        print(f"\n{'='*64}\n  THR = mover si max_high_7d/ref-1 >= {thr*100:.0f}%\n{'='*64}")
        total = covered = 0
        band_tot = {}; band_cov = {}
        missed_never = missed_timing = 0
        missed_held = 0              # perdidos que SOSTIENEN neto >=10% a 7d (alfa real perdida)
        band_missed_held = {}        # banda -> perdidos-que-sostienen
        lat_by_sig = {}              # signal_type -> [fracción ya realizada al entrar]
        missed_rows = []
        for s, df in uni.items():
            evs = mover_events(df, t0_ms, t1_ms, thr)
            alist = by_sym.get(s, [])
            for (i, start, move, atr, net) in evs:
                total += 1
                b = band_of(atr)
                band_tot[b] = band_tot.get(b, 0) + 1
                # cobertura: alguna alerta del símbolo en [start-B, start+A]
                lo = start - timedelta(days=COVER_BEFORE)
                hiw = start + timedelta(days=COVER_AFTER)
                cov = [x for x in alist if lo <= x[0] <= hiw]
                if cov:
                    covered += 1
                    band_cov[b] = band_cov.get(b, 0) + 1
                    # latencia: base = min low en 10 barras previas; peak = max high fwd 7d
                    entry = cov[0][1]; sig = cov[0][2]
                    base = float(df.low.values[max(0, i - 10):i + 1].min())
                    end_ms = df.open_time.values[i] + WINDOW_DAYS * DAY_MS
                    fmask = (df.open_time.values > df.open_time.values[i]) & (df.open_time.values <= end_ms)
                    peak = float(df.high.values[fmask].max())
                    if entry and peak > base:
                        frac = (entry - base) / (peak - base)
                        lat_by_sig.setdefault(sig, []).append(max(0.0, min(1.5, frac)))
                else:
                    if s in by_sym:
                        missed_timing += 1   # el símbolo disparó algo en la ventana, pero no acá
                    else:
                        missed_never += 1    # nunca disparó nada
                    if net >= 0.10:          # sostuvo ganancia neta -> alfa real perdida (no round-trip)
                        missed_held += 1
                        band_missed_held[b] = band_missed_held.get(b, 0) + 1
                    missed_rows.append((move, s, start.date().isoformat(), b, s in by_sym, round(net*100, 1)))

        recall = covered / total if total else 0
        print(f"  Mover-events: {total} | cubiertos: {covered} | "
              f"RECALL = {recall*100:.1f}%  (gap {100-recall*100:.1f}%)")
        print(f"  Perdidos: {total-covered}  "
              f"(timing={missed_timing} sím sí disparó / never={missed_never} sím nunca disparó)")
        print(f"  De los perdidos, SOSTIENEN neto>=10% a 7d (alfa real, no round-trip): "
              f"{missed_held}/{total-covered} = {missed_held/(total-covered)*100:.0f}%")
        print(f"\n  Recall por banda atr_pct_1d (+ perdidos que sostienen):")
        print(f"    {'banda':>8} {'total':>7} {'cubiertos':>10} {'recall':>8} {'perd.sostienen':>15}")
        for lo, hi in ATR_BANDS:
            b = f"{lo}-{hi if hi < 999 else '+'}"
            tt = band_tot.get(b, 0); cc = band_cov.get(b, 0)
            if tt:
                print(f"    {b:>8} {tt:>7} {cc:>10} {cc/tt*100:>7.1f}% {band_missed_held.get(b,0):>15}")

        # Parte A — latencia por signal_type (solo en este thr)
        print(f"\n  Latencia (fracción del move ya realizada al entrar; 0=base, 1=pico):")
        print(f"    {'signal':>9} {'n':>5} {'mediana':>8} {'media':>7}")
        for sig in sorted(lat_by_sig, key=lambda k: -len(lat_by_sig[k])):
            v = lat_by_sig[sig]
            print(f"    {sig:>9} {len(v):>5} {st.median(v)*100:>7.0f}% {st.mean(v)*100:>6.0f}%")

        # top movers perdidos para inspección
        missed_rows.sort(reverse=True)
        out = [{"mfe_pct": round(m*100, 1), "net_7d_pct": net, "symbol": s, "date": d,
                "atr_band": b, "sym_fired_something": ever} for m, s, d, b, ever, net in missed_rows[:50]]
        Path(f"diag_recall_missed_{int(thr*100)}.json").write_text(json.dumps(out, indent=2))
        print(f"  -> top 50 perdidos en diag_recall_missed_{int(thr*100)}.json")
        # lista COMPLETA de timing-missed (símbolo SÍ disparó algo) para el trace de near-miss
        timing = [{"mfe_pct": round(m*100, 1), "net_7d_pct": net, "symbol": s, "date": d,
                   "atr_band": b} for m, s, d, b, ever, net in missed_rows if ever]
        Path(f"diag_recall_timing_{int(thr*100)}.json").write_text(json.dumps(timing, indent=2))
        print(f"  -> {len(timing)} timing-missed en diag_recall_timing_{int(thr*100)}.json")


if __name__ == "__main__":
    main()
