"""
Prototipo idea #1 — EXIT condicionado por RÉGIMEN de mercado (BTC vs EMA168h).

Tesis: el exit global shipeado (ARM12/TRAIL8/STOP0) deja valor porque su efectividad es
regime-dependiente. El régimen de BTC es la variable de outcome validada
(ver project-swing-meta-labeling-fails-regime-dominates). Probamos: ¿un exit más ajustado en
BTC down-regime + el global en up-regime bate al global-everywhere OUT-OF-SAMPLE?

Cautela: el tuneo del exit POR SEÑAL no sobrevive OOS (memoria). Por eso validamos con split
CRONOLÓGICO train/test: la config de down-regime se elige en TRAIN, se evalúa en TEST vs global.
NO grid-search a ciegas sobre todo el set.

NO toca producción. Corre con `py -3.13`. Diag por argv (default diag_16w.json).
"""
import json, sys, statistics as st
from datetime import datetime, timedelta
import numpy as np

from sim_exit import simulate, load_1h, WINDOW_DAYS, MATURE_BEFORE, BUCKETS
from meta_label import ema_series, val_at, EMA_BARS

GLOBAL = (0.12, 0.08, 0.0)   # ARM, TRAIL, STOP shipeado

# grid chico y principista para el down-regime (más ajustado / armar antes / stop duro)
DOWN_ARMS  = (0.05, 0.06, 0.08, 0.12)
DOWN_TRAILS = (0.04, 0.05, 0.06, 0.08)
DOWN_STOPS = (0.0, 0.05, 0.06, 0.08)


def realized_for(alerts, cache, cfg_by_up):
    """Aplica (arm,trail,stop) según régimen de cada alerta. cfg_by_up = {True: cfg_up, False: cfg_down}.
    Devuelve lista de retornos realizados (frac)."""
    out = []
    for a in alerts:
        cfg = cfg_by_up[a["_btc_up"]]
        r = simulate(a, cache[a["symbol"]], *cfg)
        if r is not None:
            out.append(r[0])
    return out


def stats(arr):
    if not arr:
        return None
    return (st.mean(arr), st.median(arr),
            sum(1 for x in arr if x > 0) / len(arr) * 100,
            st.quantiles(arr, n=10)[0])


def show(tag, arr):
    s = stats(arr)
    if s is None:
        print(f"  {tag:38} (vacío)"); return
    print(f"  {tag:38} n={len(arr):4}  avg {s[0]*100:+.2f}%  med {s[1]*100:+.2f}%  "
          f"win {s[2]:.0f}%  p10 {s[3]*100:+.1f}%")


def main():
    diag = sys.argv[1] if len(sys.argv) > 1 else "diag_16w.json"
    d = json.load(open(diag))["main"]
    alerts = [a for a in d if a.get("bucket") in BUCKETS and a.get("entry_price")
              and datetime.fromisoformat(a["alerted_at"].replace("Z", "+00:00")) <= MATURE_BEFORE]
    alerts.sort(key=lambda a: a["alerted_at"])
    syms = sorted(set(a["symbol"] for a in alerts))
    print(f"Diag: {diag} | cargando klines 1h de {len(syms)} símbolos...")
    cache = {s: load_1h(s) for s in syms}
    alerts = [a for a in alerts if cache.get(a["symbol"]) is not None]

    # régimen BTC al momento de cada alerta (sin leakage: EMA sobre cierres <= t)
    btc = load_1h("BTCUSDT")
    btc_ot, btc_above = ema_series(btc)
    for a in alerts:
        t = int(datetime.fromisoformat(a["alerted_at"].replace("Z", "+00:00")).timestamp() * 1000)
        up = val_at(btc_ot, btc_above, t)
        a["_btc_up"] = bool(up) if up is not None else True   # fail-safe: tratar como up
    n_up = sum(a["_btc_up"] for a in alerts)
    print(f"Alertas simulables: {len(alerts)}  | BTC up: {n_up}  down: {len(alerts)-n_up}\n")

    # ── 1) DIAGNÓSTICO: ¿el exit GLOBAL sangra más en down-regime? ───────────
    print("=" * 60)
    print(" DIAGNÓSTICO — exit GLOBAL (ARM12/TRAIL8/STOP0) por régimen")
    print("=" * 60)
    g = {True: GLOBAL, False: GLOBAL}
    show("global, BTC up", realized_for([a for a in alerts if a["_btc_up"]], cache, g))
    show("global, BTC down", realized_for([a for a in alerts if not a["_btc_up"]], cache, g))

    # ── 2) REGIME-AWARE con split cronológico train/test ─────────────────────
    cut = int(len(alerts) * 0.70)
    train, test = alerts[:cut], alerts[cut:]
    tr_down = [a for a in train if not a["_btc_up"]]
    print(f"\n{'='*60}\n REGIME-AWARE — split cronológico train={len(train)} test={len(test)}\n{'='*60}")
    print(f" (up-regime queda GLOBAL; se tunea SOLO down-regime en train, n_down_train={len(tr_down)})")

    # buscar mejor config down en TRAIN (por avg realizado)
    best = None
    for arm in DOWN_ARMS:
        for trail in DOWN_TRAILS:
            for stop in DOWN_STOPS:
                rs = [simulate(a, cache[a["symbol"]], arm, trail, stop) for a in tr_down]
                rs = [x[0] for x in rs if x is not None]
                if len(rs) < 20:
                    continue
                avg = st.mean(rs)
                if best is None or avg > best[0]:
                    best = (avg, (arm, trail, stop))
    down_cfg = best[1]
    print(f"\n Mejor config down en TRAIN: ARM {down_cfg[0]:.2f} TRAIL {down_cfg[1]:.2f} STOP {down_cfg[2]:.2f} "
          f"(avg train-down {best[0]*100:+.2f}%)")

    # evaluar en TEST: global-everywhere vs regime-aware
    cfg_global = {True: GLOBAL, False: GLOBAL}
    cfg_regime = {True: GLOBAL, False: down_cfg}
    print("\n  TEST (out-of-sample):")
    show("  global-everywhere", realized_for(test, cache, cfg_global))
    show("  regime-aware (down tuneado)", realized_for(test, cache, cfg_regime))
    print("\n  desglose TEST por régimen:")
    show("    test up   (ambos = GLOBAL)", realized_for([a for a in test if a["_btc_up"]], cache, cfg_global))
    show("    test down GLOBAL", realized_for([a for a in test if not a["_btc_up"]], cache, cfg_global))
    show("    test down regime-cfg", realized_for([a for a in test if not a["_btc_up"]], cache, cfg_regime))

    down_stop_experiment(train, test, cache)


def down_stop_experiment(train, test, cache):
    """Refinamiento dirigido: ARM/TRAIL fijos en GLOBAL, barrer SOLO el stop duro en down-regime.
    Ataca la cola izquierda (p10) sin tocar el upside del trailing. ¿El stop en down sube avg vía
    cola, o solo cambia mediana por cola? Tesis: en down los -8% NO recuperan (a dif. del global)."""
    ARM_F, TRAIL_F = 0.12, 0.08
    STOPS = (0.0, 0.05, 0.06, 0.07, 0.08, 0.10, 0.12)
    tr_down = [a for a in train if not a["_btc_up"]]
    te_down = [a for a in test if not a["_btc_up"]]
    print(f"\n{'='*60}\n REFINAMIENTO — stop duro SOLO en down (ARM/TRAIL = GLOBAL)\n{'='*60}")
    print(f" n down: train={len(tr_down)} test={len(te_down)}")

    def run(alerts, stop):
        rs = [simulate(a, cache[a["symbol"]], ARM_F, TRAIL_F, stop) for a in alerts]
        return [x[0] for x in rs if x is not None]

    print(f"\n  TEST down-regime por nivel de stop (ARM12/TRAIL8):")
    print(f"  {'STOP':>5} | {'avg':>7} {'med':>7} {'win':>5} {'p10':>7}  | train-down avg")
    best = None
    for stop in STOPS:
        te = run(te_down, stop); tr = run(tr_down, stop)
        s = stats(te); tr_avg = st.mean(tr) if tr else 0
        tag = "  (GLOBAL)" if stop == 0.0 else ""
        print(f"  {stop:>5.2f} | {s[0]*100:>+6.2f}% {s[1]*100:>+6.2f}% {s[2]:>4.0f}% {s[3]*100:>+6.1f}%  | "
              f"{tr_avg*100:>+6.2f}%{tag}")
        if best is None or tr_avg > best[0]:
            best = (tr_avg, stop)

    # config elegida por avg en train-down, evaluada en TEST completo
    best_stop = best[1]
    print(f"\n  Mejor stop down por avg en TRAIN: {best_stop:.2f}")
    cfg_g = {True: GLOBAL, False: GLOBAL}
    cfg_r = {True: GLOBAL, False: (ARM_F, TRAIL_F, best_stop)}
    print("\n  TEST completo (out-of-sample):")
    show("  global-everywhere", realized_for(test, cache, cfg_g))
    show(f"  down stop={best_stop:.2f}", realized_for(test, cache, cfg_r))


if __name__ == "__main__":
    main()
