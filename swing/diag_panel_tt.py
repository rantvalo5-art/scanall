"""
Por que el panel de `panel_tt.py` no reproduce el -9,30% de las alertas.

La Fase 1 no cruzo la regla de parada, pero un veredicto sin descomposicion no
sirve para decidir que sigue. Esto contesta cuatro preguntas concretas:

  Q1  en la VENTANA donde se descubrio el efecto (31-may -> 22-ago 2026), ¿el
      panel lo ve? Si lo ve, el hallazgo era del regimen y no de las alertas.
  Q2  restringido a los 285 simbolos que el swing alerto en BEST, ¿revive?
      Separa "es la poblacion de monedas" de "es el estado de senal".
  Q3  el signo del efecto bloque por bloque. Es la evidencia de regimen.
  Q4  el escalon crudo, ¿es tt_pos o es que moneda es? El dardo pareado lo dice.

    py -3.13 diag_panel_tt.py
"""
import argparse

import numpy as np
import pandas as pd

import fase0_plan as F
import panel_tt as PT

CSV = F.HERE.parent / "archivo_outcomes" / "screener_outcomes.csv"


def m(g, nom, minimo=30):
    if len(g) < minimo:
        print(f"  {nom:<40} n={len(g):>6}  (pocas)")
        return
    d = (g.r - g.dardo).tolist()
    lo, hi = F.boot_ci(d, g.week.tolist())
    print(f"  {nom:<40} n={len(g):>6}  ret {g.r.mean()*100:>+7.2f}%  "
          f"margen {np.mean(d)*100:>+6.2f}pp  IC95 [{lo*100:>+6.2f},{hi*100:>+6.2f}]")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--panel", default=str(PT.PCACHE / "panel_s3_h7.pkl"))
    ap.add_argument("--csv", default=str(CSV))
    ap.add_argument("--umbral", type=float, default=1.28)
    ap.add_argument("--limpio-desde", default="2023-01-01")
    ap.add_argument("--ventana-desde", default="2026-05-31")
    args = ap.parse_args()
    U = args.umbral

    P = pd.read_pickle(args.panel)
    L = P[P.fecha >= args.limpio_desde].copy()
    print(f"panel limpio {len(L):,} filas · {L.symbol.nunique()} simbolos · "
          f"{L.fecha.min()} -> {L.fecha.max()}")
    print(f"  ret medio {L.r.mean()*100:+.2f}%  mediana {L.r.median()*100:+.2f}%  "
          f"dardo {L.dardo.mean()*100:+.2f}%")

    print(f"\nQ1 — la ventana del descubrimiento ({args.ventana_desde} -> fin), "
          f"vista por el panel")
    V = L[L.fecha >= args.ventana_desde]
    print(f"  {len(V):,} filas · {V.symbol.nunique()} simbolos")
    m(V[V.tt_pos < U], f"tt_pos < {U}")
    m(V[V.tt_pos >= U], "resto")

    print("\nQ2 — solo los simbolos que el swing alerto en BEST")
    A = pd.read_csv(args.csv)
    A = A[A.bucket == "BEST"].copy()
    syms = set(A.symbol.unique())
    S = L[L.symbol.isin(syms)]
    print(f"  {len(syms)} simbolos alertados · {len(syms & set(L.symbol))} en el panel "
          f"· {len(S):,} filas")
    m(S[S.tt_pos < U], f"tt_pos < {U}  (2023 -> hoy)")
    m(S[S.tt_pos >= U], "resto          (2023 -> hoy)")
    SV = S[S.fecha >= args.ventana_desde]
    m(SV[SV.tt_pos < U], f"tt_pos < {U}  (ventana del hallazgo)")
    m(SV[SV.tt_pos >= U], "resto          (ventana del hallazgo)")

    # Los dias-simbolo que ademas tuvieron alerta BEST real. Son pocos porque la
    # grilla es cada 3 dias: sirve de sonda, no de prueba.
    A["fecha"] = pd.to_datetime(A.alerted_at, utc=True).dt.strftime("%Y-%m-%d")
    par = set(zip(A.symbol, A.fecha))
    E = L[[(s, f) in par for s, f in zip(L.symbol, L.fecha)]]
    print(f"\n  sonda: filas del panel que caen en un dia-simbolo CON alerta BEST "
          f"({len(E):,})")
    m(E[E.tt_pos < U], f"tt_pos < {U}  (dias con alerta)")
    m(E[E.tt_pos >= U], "resto          (dias con alerta)")

    print("\nQ3 — signo del efecto bloque por bloque: margen(q1) - margen(q5)")
    L["q"] = pd.qcut(L.tt_pos, 5, labels=False, duplicates="drop")
    t0, t1 = int(L.t.min()), int(L.t.max())
    b = [t0 + (t1 - t0) * i // 4 for i in range(5)]
    b[-1] = t1 + 1
    for i in range(4):
        g = L[(L.t >= b[i]) & (L.t < b[i + 1])]
        q1 = (g[g.q == 0].r - g[g.q == 0].dardo).mean()
        q5 = (g[g.q == 4].r - g[g.q == 4].dardo).mean()
        ini = pd.to_datetime(b[i], unit="ms").strftime("%Y-%m")
        fin = pd.to_datetime(b[i + 1] - 1, unit="ms").strftime("%Y-%m")
        print(f"  B{i+1} {ini}->{fin}   q1 {q1*100:>+6.2f}pp   q5 {q5*100:>+6.2f}pp   "
              f"q1-q5 {(q1-q5)*100:>+6.2f}pp")

    print("\nQ4 — el escalon crudo, ¿es tt_pos o es que moneda es?")
    print("  (el dardo pareado ya controla la moneda: si el dardo baja igual, es la moneda)")
    for k in sorted(L.q.dropna().unique()):
        g = L[L.q == k]
        print(f"  q{int(k)+1}  ret {g.r.mean()*100:>+6.2f}%  dardo {g.dardo.mean()*100:>+6.2f}%"
              f"  px mediano ${g.px.median():>9.4f}  simbolos {g.symbol.nunique():>4}")


if __name__ == "__main__":
    import sys
    sys.stdout.reconfigure(encoding="utf-8")
    main()
