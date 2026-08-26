"""
El mismo lote, partido por REGIMEN de BTC (alcista / bajista).

La pregunta del usuario: "las estrategias se usan en mercados alcistas, cambia algo?"

La trampa que esto mide: en un bull, comprar AL AZAR ya gana. Asi que lo que importa
no es el win rate de la idea sino si le gana a la linea base DE ESE MISMO REGIMEN.
Por eso se imprime siempre la linea base de cada mitad.

OJO — esto es un corte POST-HOC sobre hipotesis que ya se midieron enteras. Es
exploratorio por construccion: partir 60 celdas en dos da 120, y algo se ve bien de
casualidad. No alcanza para declarar nada; alcanza para contestar la pregunta.

Regimen = close de BTC vs su EMA de 168h (1 semana), igual que sim_reversion.
"""
import numpy as np
import pandas as pd

from klines import load_panel
from lote import features, lote
from lote_metricas import hipotesis
from metricas import feat_metricas, load_metrics
from primer_toque import tabla

INICIO, FIN, PARES = "2021-08-01", "2026-08-01", 40

panel = load_panel(INICIO, FIN, n=PARES, pin=f"metricas{PARES}")
T = tabla(panel, target=8, stop=8, horizonte_d=7, paso_h=4)
F = features(panel, T)
M = load_metrics(list(panel.keys()), INICIO, FIN, verbose=False)
G = feat_metricas(M, T, verbose=False)
H = hipotesis(F, G)

# --- regimen de BTC ---------------------------------------------------------
btc = panel["BTCUSDT"][["t", "c"]].copy()
btc["ema"] = btc["c"].ewm(span=168, adjust=False).mean()
btc["alcista"] = btc["c"] > btc["ema"]
reg = T[["t"]].merge(btc[["t", "alcista"]], on="t", how="left")["alcista"]
reg.index = T.index
reg = reg.fillna(False)

print(f"\nentradas: {len(T):,}  |  alcista {100*reg.mean():.1f}%  "
      f"bajista {100*(~reg).mean():.1f}%")

for nombre, mask in (("ALCISTA", reg), ("BAJISTA", ~reg)):
    Ts = T[mask].copy()
    Ts.attrs.update(T.attrs)
    Hs = {k: v[mask] for k, v in H.items()}
    for lado, signo in (("LARGO", 1), ("CORTO", -1)):
        Tx = Ts.copy()
        Tx["res"] = signo * Tx["res"]
        Tx.attrs.update(T.attrs)
        print("\n" + "#" * 100)
        print(f"# BTC {nombre}  —  {lado}")
        print("#" * 100)
        lote(Tx, Hs)
