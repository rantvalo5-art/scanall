"""
CONSOLIDACIÓN risk-adjusted — apilar los levers DEFENSIVOS robustos en una sola cuenta y medir
Sharpe/maxDD vs el sistema actual. Ninguno sube EV crudo, pero son ortogonales y atacan la cola
→ apilados podrían mejorar el producto risk-adjusted. Walk-forward (la tesis es robustez).

Levers (stage 1, sobre el stream de alertas BEST+STRONG):
  1. sizing atr-soft (mid-band 1.3x, colas 0.7x)        [project-swing-portfolio-sizing-defensive-not-ev]
  2. filtro funding (drop pct-símbolo > 0.80, crowded)  [project-swing-funding-gate-passes-wf]
  3. hold regime-conditional (BTC up→21d, down→7d)      [project-swing-horizon-regime-conditional]

Ablación stacked por fold: BASE → +SIZE → +FUND → +HOLD. Métrica = retorno/Sharpe/maxDD de la
cuenta (cap=10, sin leverage), neto de 15bps. Reusa run_portfolio + exit_with_time + funding/btc.
La manga de REVERSIÓN (alfa descorrelacionada) = stage 2 (requiere re-scan de símbolos).
Corre con `py -3.13`. NO toca producción.
"""
import json, sys
import numpy as np
from sim_exit import load_1h
from sim_portfolio import exit_with_time, parse_ms, run_portfolio
from sim_funding import load_funding, funding_at
from sim_reversion import btc_regime

DAY_MS = 86_400_000
ARM, TRAIL, STOP = 0.12, 0.08, 0.0
CAP, TX = 10, 0.0015
LONG_W, SHORT_W = 21, 7
FUND_PCT_MAX = 0.80


def build_records():
    """Por alerta BEST+STRONG: entry_ms, atr, funding_pct, regime, y exits 7d + regime-hold."""
    alerts = [a for a in json.load(open("diag_16w.json"))["main"]
              if a.get("bucket") in ("BEST", "STRONG") and a.get("entry_price")]
    t0 = min(parse_ms(a["alerted_at"]) for a in alerts)
    t1 = max(parse_ms(a["alerted_at"]) for a in alerts)
    state = btc_regime(t0, t1)
    by_sym = {}
    for a in alerts:
        by_sym.setdefault(a["symbol"], []).append(a)

    recs = []
    n_fund = 0
    for k, (sym, lst) in enumerate(sorted(by_sym.items())):
        if k % 40 == 0:
            print(f"  {k}/{len(by_sym)} símbolos...", file=sys.stderr)
        df = load_1h(sym)
        if df is None:
            continue
        # funding del símbolo: rango = span del diag (para el que se cacheó), no el de las klines
        fdf = load_funding(sym, set(), t0, t1)
        for a in lst:
            e = a["entry_price"]; ts = parse_ms(a["alerted_at"]); atr = a.get("atr_pct_1d")
            reg = state(ts)
            # funding pct (sin leakage); None → no filtrar (fail-safe)
            fpct = None
            if fdf is not None:
                _, fpct = funding_at(fdf, ts)
                if fpct is not None:
                    n_fund += 1
            r7 = exit_with_time(e, ts, df, ARM, TRAIL, STOP, 7)
            wh = LONG_W if reg == "up" else SHORT_W
            rh = exit_with_time(e, ts, df, ARM, TRAIL, STOP, wh)
            if r7 is None or rh is None:
                continue
            recs.append({"symbol": sym, "entry_ms": ts, "atr": atr, "regime": reg,
                         "fpct": fpct,
                         "realized7": r7[0], "exit7": r7[1],
                         "realizedH": rh[0], "exitH": rh[1]})
    recs.sort(key=lambda r: r["entry_ms"])
    print(f"\nRecords: {len(recs)} | con funding: {n_fund} ({n_fund/len(recs)*100:.0f}%)\n")
    return recs


def rows_for(recs, use_size, use_fund, use_hold):
    """Arma la lista de filas para run_portfolio según los levers activos."""
    out = []
    for r in recs:
        if use_fund and r["fpct"] is not None and r["fpct"] > FUND_PCT_MAX:
            continue
        if use_hold:
            real, ex = r["realizedH"], r["exitH"]
        else:
            real, ex = r["realized7"], r["exit7"]
        out.append({"entry_ms": r["entry_ms"], "exit_ms": ex, "realized": real,
                    "atr": r["atr"], "bucket": None, "signal": None})
    return out


def run_cfg(recs, use_size, use_fund, use_hold):
    rows = rows_for(recs, use_size, use_fund, use_hold)
    return run_portfolio(rows, cap_slots=CAP,
                         sizing="atr_soft" if use_size else "equal", tx_cost=TX)


def main():
    recs = build_records()
    t0 = recs[0]["entry_ms"]; t1 = recs[-1]["entry_ms"]; span = (t1 - t0) // 4

    configs = [
        ("BASE (equal,7d,sin-fund)", False, False, False),
        ("+SIZE (atr-soft)",          True,  False, False),
        ("+FUND (drop crowded)",      True,  True,  False),
        ("+HOLD (regime 21/7d)",      True,  True,  True),
    ]

    print(f"{'='*92}\n  ABLACIÓN STACKED — cuenta cap={CAP}, neto {TX*100:.2f}%, walk-forward 4 folds\n{'='*92}")
    # full-sample primero
    print("\n  FULL SAMPLE:")
    print(f"  {'config':<28} {'ret':>8} {'sharpe':>7} {'maxDD':>7} {'trades':>7}")
    for name, s, f, h in configs:
        m = run_cfg(recs, s, f, h)
        print(f"  {name:<28} {m['total_ret']*100:>+7.1f}% {m['sharpe']:>7.2f} {m['maxdd']*100:>6.1f}% {m['trades']:>7}")

    print("\n  POR FOLD (Sharpe | maxDD):")
    hdr = "  " + f"{'fold':>4} " + " ".join(f"{n.split()[0]:>10}" for n, _, _, _ in configs)
    for f in range(4):
        lo = t0 + f*span; hi = t1+1 if f == 3 else t0+(f+1)*span
        sub = [r for r in recs if lo <= r["entry_ms"] < hi]
        if len(sub) < 20:
            continue
        if f == 0:
            print(hdr)
        shp = []; dds = []
        for name, s, fl, h in configs:
            m = run_cfg(sub, s, fl, h)
            shp.append(m["sharpe"]); dds.append(m["maxdd"]*100)
        print("  " + f"{f+1:>4} " + " ".join(f"{x:>10.2f}" for x in shp) + "   sh")
        print("  " + f"{'':>4} " + " ".join(f"{x:>9.1f}%" for x in dds) + "  DD")

    print("\n  LECTURA: ¿cada lever sube Sharpe / baja maxDD sobre el anterior, por fold? "
          "\n  Si el stack final bate a BASE en Sharpe robustamente → producto risk-adjusted mejor.")


if __name__ == "__main__":
    main()
