"""
Walk-forward del lever major_struct (variante C = config_ms_age) sobre resultados YA
computados (compare_ms.json). Particiona las 16 semanas en folds y mide por fold:
  - recall baseline vs C sobre los mover-events del universo cuyo inicio cae en el fold
  - Δrecall
  - calidad de las alertas AÑADIDAS por C en ese fold (net med, win>=0)
Pasa si Δrecall es positivo y la calidad aguanta en (casi) todos los folds; falla si algún
fold lo da vuelta (como el regime gate en el fold bajista). py -3.13.
"""
import json, sys, statistics as st
from datetime import timedelta
from diag_recall import load_universe, mover_events, parse_dt, COVER_BEFORE, COVER_AFTER

N_FOLDS = 4
THR = 0.20
CAND = "config_ms_age"


def main():
    comp = json.load(open(sys.argv[1] if len(sys.argv) > 1 else "compare_struct.json"))
    base, cand = comp["old"], comp["new"]
    t0 = min(parse_dt(a["alerted_at"]) for a in base)
    t1 = max(parse_dt(a["alerted_at"]) for a in base)
    t0_ms, t1_ms = int(t0.timestamp() * 1000), int(t1.timestamp() * 1000)
    span = (t1 - t0) / N_FOLDS
    bounds = [t0 + i * span for i in range(N_FOLDS + 1)]

    print("Cargando universo...", file=sys.stderr)
    uni = load_universe(t0_ms, t1_ms)
    print(f"Universo {len(uni)} | THR {THR*100:.0f}% | {N_FOLDS} folds de {span.days}d\n", file=sys.stderr)

    # eventos del universo con su datetime de inicio
    events = []
    for s, df in uni.items():
        for (i, start, move, atr, net) in mover_events(df, t0_ms, t1_ms, THR):
            events.append((s, start))

    base_by = {}; cand_by = {}
    for a in base:  base_by.setdefault(a["symbol"], []).append(parse_dt(a["alerted_at"]))
    for a in cand:  cand_by.setdefault(a["symbol"], []).append(parse_dt(a["alerted_at"]))
    basekeys = {(a["symbol"], a["alerted_at"]) for a in base}
    added = [a for a in cand if (a["symbol"], a["alerted_at"]) not in basekeys]
    net = lambda a: (a["price_7d"]/a["entry_price"]-1) if a.get("entry_price") and a.get("price_7d") else None

    def recall(events_f, by):
        cov = 0
        for (s, start) in events_f:
            lo, hi = start - timedelta(days=COVER_BEFORE), start + timedelta(days=COVER_AFTER)
            if any(lo <= t <= hi for t in by.get(s, [])):
                cov += 1
        return cov, (cov/len(events_f) if events_f else 0)

    print(f"  {'fold':>6} {'evts':>5} {'base':>7} {'C':>7} {'Drecall':>8} | {'+alrt':>6} {'net med':>8} {'win>=0':>7}")
    passes = 0
    for f in range(N_FOLDS):
        lo, hi = bounds[f], bounds[f+1]
        ev_f = [(s, start) for (s, start) in events if lo <= start < hi]
        _, rb = recall(ev_f, base_by)
        _, rc = recall(ev_f, cand_by)
        dr = (rc - rb) * 100
        add_f = [a for a in added if lo <= parse_dt(a["alerted_at"]) < hi]
        nets = [net(a) for a in add_f if net(a) is not None]
        nmed = st.median(nets)*100 if nets else float("nan")
        w0 = sum(x>=0 for x in nets)/len(nets)*100 if nets else float("nan")
        ok = dr > 0
        passes += ok
        print(f"  {f+1:>6} {len(ev_f):>5} {rb*100:>6.1f}% {rc*100:>6.1f}% {dr:>+7.1f}pp | "
              f"{len(add_f):>6} {nmed:>+7.1f}% {w0:>6.0f}%  {'OK' if ok else 'FALLA'}")
    print(f"\n  Folds con Drecall>0: {passes}/{N_FOLDS}")


if __name__ == "__main__":
    main()
