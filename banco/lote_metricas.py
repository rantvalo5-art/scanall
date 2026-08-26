"""
LOTE METRICAS — la familia "posicionamiento de futuros", que el repo nunca probo.

Fuente NUEVA (no son cruces de las features de precio de siempre): el dataset `metrics`
de Binance Futures. Ver `metricas.py` para el por que y la alineacion.

Horizonte elegido a proposito: target/stop 8%, **7 dias**, entradas cada 4h. Eso es
"1h a 1 semana", que es la ventana que se quiere operar.

LAS DOS DIRECCIONES. Media docena de estas hipotesis son CONTRARIAS (si los longs estan
amontonados, la apuesta es SHORT). El repo ya casi cierra 4.2 por medir solo el lado
largo. Con barreras simetricas alcanza con dar vuelta el signo de `res`, asi que se
corren los dos lotes.

    py -3.13 lote_metricas.py --pares 40        # piloto
    py -3.13 lote_metricas.py --pares 200       # corrida completa
"""
import argparse

import pandas as pd

from klines import load_panel
from lote import features, lote
from metricas import feat_metricas, load_metrics
from primer_toque import tabla

INICIO, FIN = "2025-08-01", "2026-08-01"


def hipotesis(F, G):
    """F = features de precio (lote.py), G = features de metricas (metricas.py)."""
    H = {}

    # --- A. cascada / desapalancamiento (la idea original de liquidaciones) -----
    H["oi shock -2z"]        = G.oi_z < -2
    H["oi shock -3z"]        = G.oi_z < -3
    H["oi -10% 24h"]         = G.oi_chg_24h < -0.10
    H["oi -20% 24h"]         = G.oi_chg_24h < -0.20
    H["oi -5% 4h"]           = G.oi_chg_4h < -0.05
    H["oi -3% 1h"]           = G.oi_chg_1h < -0.03
    H["cascada larga"]       = (G.oi_chg_24h < -0.10) & (F.roc_24 < -0.10)
    H["cascada extrema"]     = (G.oi_chg_24h < -0.15) & (F.roc_24 < -0.15)
    H["squeeze corto"]       = (G.oi_chg_24h < -0.10) & (F.roc_24 > 0.10)

    # --- B. apalancamiento acumulado -------------------------------------------
    H["oi 20% s/media 168"]  = G.oi_rel_168 > 0.20
    H["oi 40% s/media 168"]  = G.oi_rel_168 > 0.40
    H["oi deprimido"]        = G.oi_rel_168 < -0.20
    H["oi +20% 24h"]         = G.oi_chg_24h > 0.20
    H["euforia oi+precio"]   = (G.oi_chg_24h > 0.20) & (F.roc_24 > 0.10)

    # --- C. posicionamiento de cuentas -----------------------------------------
    H["retail poco largo"]   = G.ls_cuentas_pct < 0.10
    H["retail muy largo"]    = G.ls_cuentas_pct > 0.90
    H["top poco largo"]      = G.tt_pos_pct < 0.10
    H["top muy largo"]       = G.tt_pos_pct > 0.90
    H["top cuentas largas"]  = G.tt_cuentas_pct > 0.90
    H["top largo/retail no"] = (G.tt_pos_pct > 0.80) & (G.ls_cuentas_pct < 0.20)
    H["retail largo/top no"] = (G.tt_pos_pct < 0.20) & (G.ls_cuentas_pct > 0.80)
    H["retail sale longs"]   = G.ls_cuentas_chg24 < -0.3
    H["retail entra longs"]  = G.ls_cuentas_chg24 > 0.3

    # --- D. flujo agresor -------------------------------------------------------
    H["taker compra extrema"] = G.taker_pct > 0.90
    H["taker venta extrema"]  = G.taker_pct < 0.10
    H["taker < 0.7"]          = G.taker < 0.7
    H["taker > 1.3"]          = G.taker > 1.3
    H["capitulacion"]         = (G.taker_pct < 0.10) & (F.roc_24 < -0.10)
    H["climax comprador"]     = (G.taker_pct > 0.90) & (F.roc_24 > 0.10)

    # --- E. cruce con lo unico que replico (banda / drawdown) -------------------
    H["oi shock lejos maximo"] = (G.oi_z < -2) & (F.dd_168 < -0.20)

    return {k: v.fillna(False) for k, v in H.items()}


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--pares", type=int, default=40)
    ap.add_argument("--inicio", default=INICIO)
    ap.add_argument("--fin", default=FIN)
    ap.add_argument("--target", type=float, default=8)
    ap.add_argument("--stop", type=float, default=8)
    ap.add_argument("--dias", type=int, default=7)
    ap.add_argument("--paso-h", type=int, default=4)
    a = ap.parse_args()

    print(f"panel {a.inicio} -> {a.fin}, {a.pares} pares")
    panel = load_panel(a.inicio, a.fin, n=a.pares, pin=f"metricas{a.pares}")
    print(f"\nprimer toque: target {a.target}% / stop {a.stop}% / "
          f"{a.dias}d / entradas cada {a.paso_h}h")
    T = tabla(panel, target=a.target, stop=a.stop,
              horizonte_d=a.dias, paso_h=a.paso_h)
    F = features(panel, T)

    print(f"\nbajando metricas de futuros...")
    M = load_metrics(list(panel.keys()), a.inicio, a.fin, workers=24)
    G = feat_metricas(M, T)
    print(f"  cobertura: {100*G.oi_z.notna().mean():.1f}% de las entradas tienen OI")

    H = hipotesis(F, G)

    print("\n" + "#" * 100)
    print("# LARGO — comprar en la senal")
    print("#" * 100)
    lote(T, H)

    Tc = T.copy()
    Tc["res"] = -Tc["res"]
    Tc.attrs.update(T.attrs)
    print("\n" + "#" * 100)
    print("# CORTO — vender en la senal (mismo evento, signo dado vuelta)")
    print("#" * 100)
    lote(Tc, H)
