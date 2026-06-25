"""
Validación LIVE a 7d — sin tocar la DB ni el tracker compartido.

El tracker live (update_outcomes.py) solo llena checkpoints hasta 24h, y el payoff del swing
madura a 7d → la precisión real no se ve en la tabla. Pero cada alerta trae symbol+alerted_at+
entry_price, así que el outcome 7d se computa OFFLINE desde klines (igual que el backtest),
sin ALTER TABLE ni tocar el tracker del daytrader.

Toma las alertas live (sw_out.json, exportado de screener_outcomes) maduras a 7d, baja klines 1h
cubriendo alerted+7d, y calcula: retorno crudo 7d, MFE, y el NET del exit trailing shipeado
(ARM12/TRAIL8). Compara contra la expectativa del backtest. Excluye RIDING (daytrader) y filtra
timeframes del swing. Corre con `py -3.13`.
"""
import json, sys, statistics as st
from datetime import datetime, timezone, timedelta
from collections import defaultdict
from sim_exit import load_tf
from sim_portfolio import exit_with_time

ARM, TRAIL, STOP, WD = 0.12, 0.08, 0.0, 7
DAY_MS = 86_400_000
SWING_TFS = {"1h", "4h", "1d", "1w"}


def main():
    rows = json.load(open("sw_out.json"))
    now = datetime.now(timezone.utc)
    # swing, no RIDING (daytrader), maduras a 7d, con entry
    cand = []
    for r in rows:
        if r["signal_type"] == "RIDING" or r["timeframe"] not in SWING_TFS:
            continue
        if not r.get("entry_price"):
            continue
        t = datetime.fromisoformat(r["alerted_at"])
        if now - t < timedelta(days=WD):      # aún no madura a 7d
            continue
        cand.append(r)
    print(f"Alertas swing maduras a 7d (sin RIDING): {len(cand)}")
    print(f"  rango: {min(r['alerted_at'] for r in cand)[:10]} -> {max(r['alerted_at'] for r in cand)[:10]}\n")

    res = []  # (bucket, signal, raw7, mfe, net)
    miss = 0
    by_sym = defaultdict(list)
    for r in cand:
        by_sym[r["symbol"]].append(r)
    for k, (sym, lst) in enumerate(sorted(by_sym.items())):
        if k % 30 == 0:
            print(f"  {k}/{len(by_sym)} símbolos...", file=sys.stderr)
        # rango de klines: desde la 1ª alerta hasta la última +7d
        t0 = min(int(datetime.fromisoformat(r["alerted_at"]).timestamp()*1000) for r in lst)
        t1 = max(int(datetime.fromisoformat(r["alerted_at"]).timestamp()*1000) for r in lst) + (WD+1)*DAY_MS
        df = load_tf(sym, "1h", span=(t0, t1))
        if df is None:
            miss += len(lst); continue
        ot = df.open_time.values
        for r in lst:
            e = r["entry_price"]; ts = int(datetime.fromisoformat(r["alerted_at"]).timestamp()*1000)
            ex = exit_with_time(e, ts, df, ARM, TRAIL, STOP, WD)
            if ex is None:
                miss += 1; continue
            # crudo 7d + MFE
            end = ts + WD*DAY_MS
            fmask = (ot > ts) & (ot <= end)
            if fmask.sum() < 3:
                miss += 1; continue
            raw7 = float(df.close.values[fmask][-1])/e - 1
            mfe = float(df.high.values[fmask].max())/e - 1
            res.append((r["bucket"], r["signal_type"], raw7, mfe, ex[0]))
    print(f"\nComputadas: {len(res)} | sin klines: {miss}\n")

    def line(tag, rows):
        if not rows: print(f"  {tag:16} n=0"); return
        raw=[x[2] for x in rows]; mfe=[x[3] for x in rows]; net=[x[4] for x in rows]
        print(f"  {tag:16} n={len(rows):3} | crudo7d med {st.median(raw)*100:+5.2f}% | "
              f"MFE med {st.median(mfe)*100:+5.2f}% | NET(exit) med {st.median(net)*100:+5.2f}% "
              f"mean {st.mean(net)*100:+5.2f}% win {sum(x>=0 for x in net)/len(net)*100:3.0f}%")

    print("PRECISIÓN LIVE a 7d (NET = exit trailing shipeado):")
    for b in ("BEST","STRONG","WATCH"):
        line(b, [x for x in res if x[0]==b])
    print()
    bs=[x for x in res if x[0] in ("BEST","STRONG")]
    for s in ("HOLD","BREAKOUT","PREBREAK","COILING"):
        line(s, [x for x in bs if x[1]==s])
    print()
    line("ALL BEST+STRONG", bs)
    print(f"\n  REFERENCIA backtest (diag_16w, exit shipeado): BEST+STRONG NET mean ~+1.37% / "
          f"med ~+0.71% / win ~53%.\n  Si lo live está en ese orden → detección+exit replican fuera de muestra.")


if __name__ == "__main__":
    main()
