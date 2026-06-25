"""
NUEVA DIRECCIÓN (alfa descorrelacionada) — señal de REVERSIÓN / mean-reversion.

El sistema es 100% long-continuation (breakouts). Una manga de reversión = entrar long tras
sobre-extensión bajista o barrida de liquidez (sweep del low previo + reclaim), esperando rebote.
Sería la ÚNICA fuente de alfa que NO re-explica la misma señal → potencialmente ADITIVA, no
defensiva. Clave: tiene que rendir TAMBIÉN en down-regime (si sólo es "comprar el dip en bull",
es regime-timing y falla igual que todo lo demás).

FASE 1 (factibilidad in-sample): eventos universo-wide, NET con el exit trailing shipeado
(ARM12/TRAIL8/STOP0), vs baseline incondicional, SEPARADO por régimen BTC. Si no aparece edge
—o sólo en up-regime— STOP. Si aparece robusto al régimen → walk-forward.

Eventos (sobre 1h, por símbolo):
  A) STRETCH: close < EMA50 − K·ATR14 (sobre-extendido abajo) y reclaim (close>close[-1]).
  B) SWEEP: low < min(low de 48 barras previas) pero close reclama por encima (liquidity grab).

Reusa load_tf + exit_with_time. Corre con `py -3.13`. NO toca producción.
"""
import sys, glob, json, statistics as st
from pathlib import Path
import numpy as np
import pandas as pd

from sim_exit import load_tf, CACHE_DIR
from sim_portfolio import exit_with_time

HOUR_MS = 3_600_000
DAY_MS = 86_400_000
ARM, TRAIL, STOP, WD = 0.12, 0.08, 0.0, 7
K_STRETCH = 2.5         # nº de ATR por debajo de la EMA para "sobre-extendido"
SWEEP_LOOKBACK = 48     # barras para el min previo
EVENT_GAP = 12          # colapsar eventos del mismo símbolo dentro de N barras
BASE_PER_SYM = 25       # barras random por símbolo para baseline


def parse_window():
    a = json.load(open("diag_16w.json"))["main"]
    from datetime import datetime
    t0 = min(datetime.fromisoformat(x["alerted_at"].replace("Z", "+00:00")) for x in a)
    t1 = max(datetime.fromisoformat(x["alerted_at"].replace("Z", "+00:00")) for x in a)
    return int(t0.timestamp()*1000), int(t1.timestamp()*1000)


def btc_regime(t0_ms, t1_ms):
    """Estado BTC por barra 1h: close > EMA168 (7d) → up. Devuelve función ts_ms → 'up'/'down'."""
    df = load_tf("BTCUSDT", "1h")
    df = df[(df.open_time >= t0_ms - 200*HOUR_MS) & (df.open_time <= t1_ms)].reset_index(drop=True)
    ema = df.close.ewm(span=168, adjust=False).mean().values
    up = df.close.values > ema
    ot = df.open_time.values
    def state(ts):
        k = np.searchsorted(ot, ts, side="right") - 1
        if k < 0:
            return "up"
        return "up" if up[k] else "down"
    return state


def scan_events(df, t0_ms, t1_ms):
    """Devuelve listas de (i, ts_ms, kind) para STRETCH y SWEEP en la ventana."""
    c = df.close.values; h = df.high.values; lo = df.low.values; ot = df.open_time.values
    n = len(df)
    ema = df.close.ewm(span=50, adjust=False).mean().values
    tr = np.maximum.reduce([h[1:]-lo[1:], np.abs(h[1:]-c[:-1]), np.abs(lo[1:]-c[:-1])])
    atr = pd.Series(np.concatenate([[np.nan], tr])).rolling(14).mean().values
    ev = []
    last = -10**9
    for i in range(SWEEP_LOOKBACK, n):
        if ot[i] < t0_ms or ot[i] > t1_ms:
            continue
        if i - last < EVENT_GAP:
            continue
        if not (np.isfinite(ema[i]) and np.isfinite(atr[i]) and atr[i] > 0):
            continue
        kind = None
        # STRETCH: sobre-extendido abajo + reclaim
        if c[i] < ema[i] - K_STRETCH * atr[i] and c[i] > c[i-1]:
            kind = "STRETCH"
        else:
            prev_min = lo[i-SWEEP_LOOKBACK:i].min()
            if lo[i] < prev_min and c[i] > prev_min:
                kind = "SWEEP"
        if kind:
            ev.append((i, int(ot[i]), kind))
            last = i
    return ev


def main():
    t0_ms, t1_ms = parse_window()
    state = btc_regime(t0_ms, t1_ms)
    syms = sorted({Path(p).name.split("_1h_")[0].replace("klines_", "")
                   for p in glob.glob(str(CACHE_DIR / "klines_*_1h_*.pkl"))})
    print(f"Universo candidato: {len(syms)} símbolos | ventana {t0_ms}→{t1_ms}")

    rng = np.random.default_rng(0)
    rows = []          # (kind, ts, realized, regime)
    base = []          # (realized, regime) baseline incondicional
    nsym = 0
    for k, s in enumerate(syms):
        if k % 300 == 0:
            print(f"  escaneando... {k}/{len(syms)}", file=sys.stderr)
        df = load_tf(s, "1h")
        if df is None or len(df) < SWEEP_LOOKBACK + 50:
            continue
        df = df.sort_values("open_time").reset_index(drop=True)
        if df.open_time.min() > t0_ms or df.open_time.max() < t1_ms:
            continue
        nsym += 1
        ev = scan_events(df, t0_ms, t1_ms)
        for (i, ts, kind) in ev:
            e = df.close.values[i]
            r = exit_with_time(e, ts, df, ARM, TRAIL, STOP, WD)
            if r is None:
                continue
            rows.append((kind, ts, r[0], state(ts)))
        # baseline: barras random del símbolo dentro de la ventana
        valid = df.index[(df.open_time >= t0_ms) & (df.open_time <= t1_ms - WD*DAY_MS)].values
        if len(valid):
            pick = rng.choice(valid, min(BASE_PER_SYM, len(valid)), replace=False)
            for i in pick:
                e = df.close.values[i]; ts = int(df.open_time.values[i])
                r = exit_with_time(e, ts, df, ARM, TRAIL, STOP, WD)
                if r is not None:
                    base.append((r[0], state(ts), ts))

    print(f"Símbolos escaneados: {nsym} | eventos: {len(rows)} | baseline: {len(base)}\n")

    def stats(vals):
        if not vals:
            return "n=0"
        v = np.array(vals)
        return f"n={len(v):>5} med {np.median(v)*100:>+6.2f}% mean {v.mean()*100:>+6.2f}% win {np.mean(v>=0)*100:>3.0f}%"

    base_all = [r for r, _, _ in base]
    base_up = [r for r, g, _ in base if g == "up"]
    base_dn = [r for r, g, _ in base if g == "down"]
    print("BASELINE incondicional (exit trailing sobre barra random):")
    print(f"  all   {stats(base_all)}")
    print(f"  up    {stats(base_up)}")
    print(f"  down  {stats(base_dn)}\n")

    for kind in ("STRETCH", "SWEEP", "ALL"):
        sel = [(r, g) for (kd, ts, r, g) in rows if kind == "ALL" or kd == kind]
        allv = [r for r, _ in sel]
        upv = [r for r, g in sel if g == "up"]
        dnv = [r for r, g in sel if g == "down"]
        print(f"=== REVERSIÓN {kind} ===")
        print(f"  all   {stats(allv)}   (baseline mean {np.mean(base_all)*100:+.2f}%)")
        print(f"  up    {stats(upv)}   (Δ vs base_up   {(np.mean(upv)-np.mean(base_up))*100:+.2f}pp)"
              if upv else "  up    n=0")
        print(f"  down  {stats(dnv)}   (Δ vs base_down {(np.mean(dnv)-np.mean(base_dn))*100:+.2f}pp)"
              if dnv else "  down  n=0")
        print()

    Path("sim_reversion_events.json").write_text(json.dumps(
        [{"kind": k, "ts": ts, "realized": r, "regime": g} for (k, ts, r, g) in rows]))

    # ── WALK-FORWARD: el edge en DOWN-regime es toda la tesis (alfa descorrelacionada) ──
    print(f"\n{'='*68}\n  WALK-FORWARD — reversión vs baseline POR RÉGIMEN, 4 folds temporales\n{'='*68}")
    tcost = 0.0015  # 15bps round-trip, neto
    ev_ts = np.array([ts for (_, ts, _, _) in rows])
    ev_r = np.array([r for (_, _, r, _) in rows]) - tcost
    ev_g = np.array([g for (_, _, _, g) in rows])
    bs_ts = np.array([ts for (_, _, ts) in base])
    bs_r = np.array([r for (r, _, _) in base]) - tcost
    bs_g = np.array([g for (_, g, _) in base])
    span = (t1_ms - t0_ms) // 4
    print(f"  (neto de {tcost*100:.2f}% round-trip)")
    print(f"  {'fold':>4} | {'DOWN base':>10} {'DOWN rev':>10} {'Δdown':>7} | {'UP base':>9} {'UP rev':>9} {'Δup':>7}")
    win_dn = win_up = 0
    for f in range(4):
        lo = t0_ms + f*span; hi = t1_ms+1 if f == 3 else t0_ms+(f+1)*span
        em = (ev_ts >= lo) & (ev_ts < hi); bm = (bs_ts >= lo) & (bs_ts < hi)
        def rmean(arr, ts_m, gm, reg):
            sel = ts_m & (gm == reg)
            return arr[sel].mean()*100 if sel.sum() else float("nan"), int(sel.sum())
        bd, nbd = rmean(bs_r, bm, bs_g, "down"); rd, nrd = rmean(ev_r, em, ev_g, "down")
        bu, nbu = rmean(bs_r, bm, bs_g, "up");   ru, nru = rmean(ev_r, em, ev_g, "up")
        ddn = rd - bd; dup = ru - bu
        win_dn += ddn > 0; win_up += dup > 0
        print(f"  {f+1:>4} | {bd:>+9.2f}% {rd:>+9.2f}% {ddn:>+6.2f} | {bu:>+8.2f}% {ru:>+8.2f}% {dup:>+6.2f}")
    print(f"\n  Reversión bate baseline: DOWN {win_dn}/4 | UP {win_up}/4")
    print("  TESIS: el lever es aditivo si DOWN gana en (casi) todos los folds Y el rev-down absoluto ≥ 0.")


if __name__ == "__main__":
    main()
