"""
El unico hilo vivo de la Fase 1: NO cobrar funding en todo el universo (eso ya dio
negativo), sino entrar solo cuando el funding esta muy alto.

Lo decisivo no es el retorno del trade: es el retorno de LA CARTERA. Si solo el 0,36%
de las oportunidades califica, el capital pasa la mayor parte del año ocioso — y el
capital ocioso ya rinde el piso de stablecoins sin riesgo. La pregunta correcta es si
la cartera COMPLETA le gana al piso, no si el trade aislado le gana.

Simula: capital 1.0 en S slots. Cada entrada que califica toma un slot libre y lo
ocupa H dias. Slot ocioso = rinde el piso. Aplica la regla de parada completa:
mediana por simbolo, mediana por semana, y sobrevivir sin el top-3.
"""
import numpy as np
import pandas as pd

from fetch_funding import load_config, build_universe, to_ms
from phase1 import (build_panel, funding_between, entry_grid,
                    round_trip_cost_pct, capital_factor, MS_DAY)

cfg = load_config()
data, _ = build_universe(cfg)
panel = build_panel(data, cfg)
cost = round_trip_cost_pct(cfg)
capf = capital_factor(cfg)
floor = cfg["floor"]["STABLECOIN_APY_PCT"]
look = cfg["persistence"]["LOOKBACK_DAYS"]
start_ms, end_ms = to_ms(cfg["window"]["start_utc"]), to_ms(cfg["window"]["end_utc"])
SPAN_D = (end_ms - start_ms) / MS_DAY


def candidates(thr, hold):
    """Entradas que califican: trailing `look`d anualizado > thr."""
    rows = []
    for sym, (t_arr, csum) in panel.items():
        t0 = entry_grid(t_arr, start_ms, end_ms, hold)
        t0 = t0[t0 >= int(t_arr[0]) + look * MS_DAY]
        if t0.size == 0:
            continue
        trail = funding_between(t_arr, csum, t0 - look * MS_DAY, t0) * 100.0
        trail_apy = (trail / capf) * (365.0 / look)
        m = trail_apy > thr
        if not m.any():
            continue
        t0 = t0[m]
        gross = funding_between(t_arr, csum, t0, t0 + hold * MS_DAY) * 100.0
        rows.append(pd.DataFrame({
            "symbol": sym, "entry_ms": t0,
            "gross_pct": gross,
            "net_on_capital": (gross - cost) / capf,     # % del capital del slot
        }))
    if not rows:
        return pd.DataFrame()
    df = pd.concat(rows, ignore_index=True).sort_values("entry_ms").reset_index(drop=True)
    df["week"] = pd.to_datetime(df["entry_ms"], unit="ms", utc=True).dt.strftime("%G-W%V")
    return df


def portfolio(df, hold, slots, drop=()):
    """Retorno anual de la cartera. Slots ocupados ganan el trade; ociosos, el piso."""
    if drop:
        df = df[~df["symbol"].isin(drop)]
    if df.empty:
        return floor, 0, 0.0
    free_at = np.zeros(slots, dtype=np.int64)      # ms en que cada slot se libera
    taken_pnl, used_slot_days = 0.0, 0.0
    n = 0
    for _, r in df.iterrows():
        t = r["entry_ms"]
        i = int(np.argmin(free_at))
        if free_at[i] > t:                          # ningun slot libre -> se pierde
            continue
        free_at[i] = t + hold * MS_DAY
        taken_pnl += r["net_on_capital"] / slots    # cada slot es 1/slots del capital
        used_slot_days += hold / slots
        n += 1
    idle_frac = max(0.0, 1.0 - used_slot_days / SPAN_D)
    ann = taken_pnl * (365.0 / SPAN_D) + floor * idle_frac
    return ann, n, (1 - idle_frac) * 100


print("=" * 78)
print("ESTRATEGIA SELECTIVA — solo entra cuando el funding esta alto")
print("=" * 78)
print(f"Piso a superar: {floor:.1f}% anual | round-trip {cost:.2f}% | capital {capf:.2f}x notional")

for thr in (20, 30, 50):
    for hold in (7, 14):
        df = candidates(thr, hold)
        if df.empty:
            continue
        print("\n" + "-" * 78)
        print(f"umbral trailing {look}d > {thr}% APY  |  tenencia {hold}d  "
              f"|  {len(df)} entradas, {df['symbol'].nunique()} simbolos")
        print("-" * 78)

        net = df["net_on_capital"] * (365.0 / hold)     # APY del trade aislado
        by_sym = df.groupby("symbol").apply(
            lambda g: (g["net_on_capital"] * (365.0 / hold)).median(), include_groups=False)
        by_week = df.groupby("week").apply(
            lambda g: (g["net_on_capital"] * (365.0 / hold)).median(), include_groups=False)
        print(f"  TRADE aislado    : mediana {net.median():+7.2f}% APY   "
              f"media {net.mean():+7.2f}%   positivos {100*(net>0).mean():.0f}%")
        print(f"  mediana POR SIMBOLO {by_sym.median():+7.2f}%  "
              f"(simbolos con mediana > piso: {100*(by_sym>floor).mean():.0f}%)")
        print(f"  mediana POR SEMANA  {by_week.median():+7.2f}%  "
              f"(semanas con mediana > piso: {100*(by_week>floor).mean():.0f}%, "
              f"{len(by_week)} semanas activas de 52)")

        top3 = list(df.groupby("symbol")["net_on_capital"].sum()
                    .sort_values(ascending=False).head(3).index)
        net_d3 = df[~df["symbol"].isin(top3)]
        if not net_d3.empty:
            nd = net_d3["net_on_capital"] * (365.0 / hold)
            print(f"  SIN top-3 ({', '.join(top3)}): mediana {nd.median():+7.2f}% APY")

        print(f"  CARTERA (capital ocioso al piso):")
        for slots in (3, 5, 10):
            ann, n, util = portfolio(df, hold, slots)
            annd, nd_, utild = portfolio(df, hold, slots, drop=top3)
            print(f"    {slots:2d} slots: {ann:+6.2f}% anual  (n={n:3d} trades, "
                  f"capital usado {util:4.1f}%)   |  sin top-3: {annd:+6.2f}% (n={nd_})")
