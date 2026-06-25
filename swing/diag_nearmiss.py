"""
Prototipo — ¿POR QUÉ no disparan los 342 timing-missed?

Para cada evento timing-missed (símbolo que SÍ alertó algo en la ventana, pero no en este
move 20%+), reconstruye tf_data desde el trace por-barra (trace16.jsonl) y RE-EJECUTA el
classify() real de producción en cada barra del move [start-1d, start+3d]. Taxonomía:

  SUPPRESSED  -> classify SÍ dispara en alguna barra (con su bucket) pero no hubo alerta:
                 lo mató cooldown/dedup/ranking, o el bucket quedó por debajo del umbral.
                 (fix barato; ver desglose de buckets)
  NEARMISS    -> gates universales OK + trigger primario presente, pero classify None:
                 lo bloqueó un threshold secundario (vol/width/growth). Relajable.
  NO-SETUP    -> gates universales OK pero NUNCA hubo trigger primario (breakout/near_max/
                 hold): el move no se presenta como patrón detectable. Requiere señal nueva.
  BLOCKED:x   -> nunca pasó un gate universal; x = el gate que más bloqueó
                 (resistance / major_struct / ema_1d / atr_low).

NO toca producción. Reusa backtest.classify + config.json. Corre con `py -3.13`.
"""
import json, sys
from datetime import datetime, timezone, timedelta
from collections import Counter
from backtest import classify, Config

PREF = {"h1": "1h", "h4": "4h", "d1": "1d", "w1": "1w"}


def row_to_tfdata(row):
    td = {"1h": {}, "4h": {}, "1d": {}, "1w": {}}
    for k, v in row.items():
        if "_" in k:
            p, rest = k.split("_", 1)
            if p in PREF:
                td[PREF[p]][rest] = v
    return td


def main():
    trace = sys.argv[1] if len(sys.argv) > 1 else "trace16.jsonl"
    timing = sys.argv[2] if len(sys.argv) > 2 else "diag_recall_timing_20.json"
    cfg = Config("config.json")
    atr_min = cfg.g("indicators", "ATR_MIN_PCT")

    bysym = {}
    for line in open(trace, encoding="utf-8"):
        r = json.loads(line)
        bysym.setdefault(r["sym"], []).append(r)
    for s in bysym:
        bysym[s].sort(key=lambda r: r["ts_ms"])

    events = json.load(open(timing))
    cats = Counter()
    fired_buckets = Counter()
    blocked_band = {}            # banda atr -> Counter de categorías (para ver si el gap es atacable)
    for ev in events:
        sym, d, band = ev["symbol"], ev["date"], ev.get("atr_band", "?")
        start = datetime.fromisoformat(d).replace(tzinfo=timezone.utc)
        lo = int((start - timedelta(days=1)).timestamp() * 1000)
        hi = int((start + timedelta(days=3)).timestamp() * 1000)
        rows = [r for r in bysym.get(sym, []) if lo <= r["ts_ms"] <= hi]
        if not rows:
            cats["NO-TRACE"] += 1
            continue

        fired = None
        uni_pass = primary = False
        gate_false = Counter()
        for r in rows:
            td = row_to_tfdata(r)
            try:
                res = classify(sym, td, cfg)
            except Exception:
                res = None
            if res:
                fired = res
            d1, h4, h1 = td["1d"], td["4h"], td["1h"]
            if not d1:
                continue
            nnr = d1.get("not_near_resistance")
            ms = d1.get("major_struct_ok", True)
            ema = d1.get("ema_trend_up")
            atrp = d1.get("atr_pct", 0) or 0
            atrt = d1.get("atr_threshold") or atr_min
            if nnr and ms and ema and atrp >= atrt:
                uni_pass = True
            else:
                if not nnr: gate_false["resistance"] += 1
                if not ms:  gate_false["major_struct"] += 1
                if not ema: gate_false["ema_1d"] += 1
                if atrp < atrt: gate_false["atr_low"] += 1
            if (h4.get("breakout") or h1.get("near_recent_max")
                    or (h4.get("hold_recent_break") and h4.get("hold_kept_zone"))):
                primary = True

        if fired:
            cat = "SUPPRESSED(cooldown/dedup/rank)"
            fired_buckets[fired.get("bucket")] += 1
        elif uni_pass and primary:
            cat = "NEARMISS(secondary thr)"
        elif uni_pass and not primary:
            cat = "NO-SETUP(no primary trigger)"
        else:
            b = gate_false.most_common(1)[0][0] if gate_false else "unknown"
            cat = f"BLOCKED:{b}"
        cats[cat] += 1
        blocked_band.setdefault(band, Counter())[cat] += 1

    tot = sum(cats.values())
    print(f"Timing-missed analizados: {tot}\n")
    print(f"  {'n':>4} {'%':>6}  categoría")
    for k, v in cats.most_common():
        print(f"  {v:>4} {v/tot*100:>5.1f}%  {k}")
    if fired_buckets:
        print(f"\n  Buckets de los SUPPRESSED: {dict(fired_buckets)}")
        print("  (WATCH/sub-umbral = scoring bajo; BEST/STRONG = cooldown/dedup real)")

    print(f"\n  Por banda atr_pct_1d (categoría dominante):")
    for band in sorted(blocked_band):
        c = blocked_band[band]
        top = c.most_common(2)
        print(f"    {band:>6} (n={sum(c.values()):>3}): " + ", ".join(f"{k}={v}" for k, v in top))


if __name__ == "__main__":
    main()
