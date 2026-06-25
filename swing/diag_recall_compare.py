"""
Recall real (universo completo, NO Top-30) por variante del --compare de major_struct.
Reusa la lógica de diag_recall pero mide cobertura con las alertas de cada variante
guardadas en compare_ms.json. Responde: ¿cuánto sube el recall al relajar el gate, y a
qué costo de alertas? py -3.13.
"""
import json, sys
from datetime import timedelta
from diag_recall import load_universe, mover_events, parse_dt, COVER_BEFORE, COVER_AFTER, THRS


def main():
    comp = json.load(open("compare_ms.json"))
    # ventana: de las alertas baseline
    base = comp["config"]
    t0 = min(parse_dt(a["alerted_at"]) for a in base)
    t1 = max(parse_dt(a["alerted_at"]) for a in base)
    t0_ms, t1_ms = int(t0.timestamp() * 1000), int(t1.timestamp() * 1000)

    print("Cargando universo 1d...", file=sys.stderr)
    uni = load_universe(t0_ms, t1_ms)
    print(f"Universo: {len(uni)} símbolos\n")

    for thr in THRS:
        # eventos-mover del universo (independientes de la variante) — computar una vez
        events = []  # (sym, start_dt)
        for s, df in uni.items():
            for (i, start, move, atr, net) in mover_events(df, t0_ms, t1_ms, thr):
                events.append((s, start))
        total = len(events)
        print(f"{'='*60}\n  THR {thr*100:.0f}%  | mover-events universo: {total}\n{'='*60}")
        print(f"  {'variante':>16} {'alertas':>8} {'cubiertos':>10} {'recall':>8} {'Δrecall':>8}")
        base_recall = None
        for stem, alerts in comp.items():
            by_sym = {}
            for a in alerts:
                by_sym.setdefault(a["symbol"], []).append(parse_dt(a["alerted_at"]))
            covered = 0
            for (s, start) in events:
                lo = start - timedelta(days=COVER_BEFORE)
                hi = start + timedelta(days=COVER_AFTER)
                if any(lo <= t <= hi for t in by_sym.get(s, [])):
                    covered += 1
            rec = covered / total if total else 0
            if base_recall is None:
                base_recall = rec
            dr = (rec - base_recall) * 100
            print(f"  {stem:>16} {len(alerts):>8} {covered:>10} {rec*100:>7.1f}% "
                  f"{('' if stem=='config' else f'{dr:+.1f}pp'):>8}")
        print()


if __name__ == "__main__":
    main()
