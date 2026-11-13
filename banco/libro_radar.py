"""
LIBRO + RADAR — `PREREGISTRO_LIBRO_RADAR.md` y su ENMIENDA del 2026-11-13, en un comando.

    py -u libro_radar.py              # la principal: cinco pesos, eleccion y potencia
    py -u libro_radar.py --reserva    # lo mismo, y la reserva SOLO si el script la autoriza

La pregunta: si el radar elige su top-8 con `n_surge` Y la profundidad del libro, ¿los 8
elegidos se mueven mas que los 8 que elige hoy con `n_surge` solo, EN LA MISMA BARRA?

    BASE      n_surge, tal cual `radar.py`
    COMBO(w)  (1 - w) · pct(n_surge) + w · pct(prof_1_rel), percentil DENTRO de la barra
              w ∈ {0,20 · 0,35 · 0,50 · 0,65 · 0,80}, fijado en la enmienda

    Δ = media semanal de [ magnitud(top-8 COMBO) − magnitud(top-8 BASE) ], misma barra

Pareado por barra: el universo, el regimen y la hora se cancelan, y una barra donde los dos
top-8 son identicos aporta 0. No hay modelo lineal en ningun lado (era la duda de §6.3).

LO QUE ESTE SCRIPT NO DECIDE A MANO. La tabla de los cinco pesos sale en el orden de la
grilla, no ordenada por Δ. El peso lo elige `elegir()` con la regla de la enmienda. La
reserva la autoriza `potencia()`, y sin `--reserva` no se baja aunque este autorizada: asi
la principal y la autorizacion se leen ANTES de que exista el dato de la reserva en disco.

Lo que la enmienda dejo fijado y aca se implementa tal cual:
  - Holm sobre los 5 en el bootstrap (el menor p contra 0,01, el segundo contra 0,0125...).
  - Elegido = mayor Δ entre los que pasan TODAS las compuertas de §3.
  - Potencia de la reserva contra la MEDIANA de Δ de los que pasan (la mitad, si pasa uno
    solo), no contra el elegido, que viene inflado por ser el maximo.
  - La reserva prueba UN brazo: el elegido. Δ_R > 0 y bootstrap p < 0,05. Nada mas.
"""
import argparse
import sys

import numpy as np
import pandas as pd

import ranking as R
from klines import load_panel
from libro_hist import FIN_PRINCIPAL, INICIO, feat_libro, load_libro

# ------------------------------------------------------------------ fijado en el preregistro
PESOS = [0.20, 0.35, 0.50, 0.65, 0.80]   # enmienda §1
K, PASO, H = 8, 4, 4                     # el tablero de PREREGISTRO_LIBRO
Y = "y_magnitud"
ALFA = 0.05
Z = 2.8                                  # MDE = 2,8 · sd / √n, el del resto del repo
SEM_OK = 0.60                            # §3.5
ANIOS = [2023, 2024, 2025, 2026]         # §3.7
ATR_RANGO = (0.95, 1.05)                 # §3.8
SEM_RESERVA = 13                         # 2026-08-01 -> 2026-10-31
FIN_RESERVA = "2026-11-01"               # exclusivo


# ------------------------------------------------------------------ datos
def armar(inicio, fin, reserva):
    """El tablero de PREREGISTRO_LIBRO, con `prof_1_rel` pegado por (sym, t)."""
    panel = load_panel(inicio, fin, n=46, pin="deriv46", full=True)
    if not panel:
        sys.exit("FATAL: no se pudo cargar el panel")
    TB = R.tablero(panel, paso=PASO, horizonte=H)
    L = load_libro(list(panel), inicio, fin, reserva=reserva)
    FL = feat_libro(L, TB[["sym", "t"]])
    TB = pd.concat([TB, FL[["prof_1_rel"]]], axis=1)
    print(f"prof_1_rel presente en {TB['prof_1_rel'].notna().mean():.1%} de las filas")
    return TB


# ------------------------------------------------------------------ los dos rankings
def _pct(TB, col):
    return TB.groupby("t")[col].rank(pct=True)


def combo(TB, w):
    """§2: si el par no tiene libro en esa barra, COMBO usa el percentil de n_surge solo,
    asi BASE y COMBO eligen sobre el mismo universo (COMBO es NaN solo donde n_surge lo es)."""
    pn = _pct(TB, "n_surge")
    pl = _pct(TB, "prof_1_rel")
    return ((1 - w) * pn + w * pl).where(pl.notna(), pn)


def _top(TB, score):
    """Los k elegidos por barra, con el mismo filtro de barras de `ranking._spread_semanal`."""
    D = TB[["t", "sym", "dt", "semana", "atr_24", Y, f"{Y}_crudo"]].copy()
    D["score"] = score.to_numpy()
    D = D[D["score"].notna() & D[Y].notna() & D["atr_24"].gt(0)]
    D = D[D.groupby("t")["score"].transform("count") >= max(R.MIN_SYMS, 2 * K)]
    D = D.sort_values(["t", "score"], ascending=[True, False], kind="mergesort")
    return D[D.groupby("t").cumcount() < K]


def _semanal(d, semana):
    return d.groupby(semana.reindex(d.index)).mean()


def comparar(TB, w):
    """Δ por barra entre COMBO(w) y BASE, y todo lo que las compuertas necesitan."""
    sb, sc = _top(TB, TB["n_surge"]), _top(TB, combo(TB, w))
    mb = sb.groupby("t")[[Y, f"{Y}_crudo"]].mean()
    mc = sc.groupby("t")[[Y, f"{Y}_crudo"]].mean()
    idx = mb.index.intersection(mc.index)
    d = mc.loc[idx, Y] - mb.loc[idx, Y]
    dc = mc.loc[idx, f"{Y}_crudo"] - mb.loc[idx, f"{Y}_crudo"]
    semana = sb.groupby("t")["semana"].first()
    anio = sb.groupby("t")["dt"].first().dt.year

    # aporte de cada par a Δ: lo que suma cuando COMBO lo elige menos cuando BASE lo elige
    ap = (sc.groupby("sym")[Y].sum().sub(sb.groupby("sym")[Y].sum(), fill_value=0)) / K
    comun = sb[["t", "sym"]].merge(sc[["t", "sym"]], on=["t", "sym"])
    return dict(
        d=d, sem=_semanal(d, semana), sem_crudo=_semanal(dc, semana),
        anio=anio.reindex(idx), semana=semana,
        aporte=ap.sort_values(ascending=False),
        solape=len(comun) / (K * len(idx)) if len(idx) else np.nan,
        atr_ratio=float((sc.groupby("t")["atr_24"].median()
                         / sb.groupby("t")["atr_24"].median()).reindex(idx).mean()),
    )


# ------------------------------------------------------------------ compuertas
def holm(p, alfa=ALFA):
    """Holm step-down (enmienda §2). Devuelve (umbral de cada p, pasa o no).
    El menor p contra alfa/m, el siguiente contra alfa/(m-1)...; en el primero que falla
    se corta y ninguno de los que siguen pasa, aunque su p este bajo su propio umbral."""
    umbral = pd.Series(np.nan, index=p.index)
    ok = pd.Series(False, index=p.index)
    for i, j in enumerate(p.sort_values(kind="mergesort").index):
        umbral[j] = alfa / (len(p) - i)
        if p[j] < umbral[j]:
            ok[j] = True
        else:
            break
    return umbral, ok


def principal(TB):
    filas = []
    for w in PESOS:
        c = comparar(TB, w)
        sem = c["sem"]
        f = dict(w=w, semanas=len(sem), solape=c["solape"], delta=float(sem.mean()),
                 sd=float(sem.std(ddof=1)), sem_ok=float((sem > 0).mean()),
                 crudo=float(c["sem_crudo"].mean()), atr_ratio=c["atr_ratio"])
        f["mde"] = Z * f["sd"] / np.sqrt(len(sem))
        f["p"] = R._p_bloques(sem) if f["delta"] > 0 else 1.0

        fuera = set(c["aporte"].head(R.TOP_N).index)
        f["sin_top3"] = float(comparar(TB[~TB["sym"].isin(fuera)], w)["sem"].mean())

        for a in ANIOS:
            m = c["anio"] == a
            f[f"y{a}"] = (float(_semanal(c["d"][m], c["semana"]).mean())
                          if m.any() else np.nan)
        filas.append(f)

    T = pd.DataFrame(filas)
    T["holm_umbral"], T["holm_ok"] = holm(T["p"])

    def veredicto(r):
        # en el orden de §3; la primera que falla es el veredicto
        if not r.delta > r.mde:
            return f"Δ <= MDE ({r.delta:+.4f} vs ±{r.mde:.4f})"
        if not r.holm_ok:
            return f"no pasa Holm (p {r.p:.4f})"
        if not r.sem_ok >= SEM_OK:
            return f"semanas > 0 {r.sem_ok:.0%} < {SEM_OK:.0%}"
        if not r.sin_top3 > 0:
            return "muere sin los 3 pares que mas aportan"
        malos = [str(a) for a in ANIOS if not r[f"y{a}"] > 0]
        if malos:
            return "cambia de signo en " + ", ".join(malos)
        if not r.crudo > 0:
            return "la vista cruda no da > 0"
        if not ATR_RANGO[0] <= r.atr_ratio <= ATR_RANGO[1]:
            return f"artefacto de escala (atr_ratio {r.atr_ratio:.2f})"
        return "PASA"

    T["veredicto"] = T.apply(veredicto, axis=1)
    return T


def elegir(T):
    """Enmienda §3: el mayor Δ entre los que pasan TODAS. None si ninguno."""
    ok = T[T["veredicto"] == "PASA"]
    return None if ok.empty else ok.loc[ok["delta"].idxmax()]


def potencia(T, elegido):
    """Enmienda §4: MDE_R con la sd del elegido, contra la MEDIANA de Δ de los que pasan."""
    ok = T[T["veredicto"] == "PASA"]
    ref = float(ok["delta"].median()) if len(ok) > 1 else float(elegido["delta"]) / 2
    mde_r = Z * float(elegido["sd"]) / np.sqrt(SEM_RESERVA)
    return mde_r, ref, mde_r < ref


# ------------------------------------------------------------------ salida
def mostrar(T):
    print("\n" + "=" * 100)
    print(f"PRINCIPAL — {INICIO} -> {FIN_PRINCIPAL} | COMBO(w) contra BASE (n_surge) | "
          f"top-{K}, {H}h | en el orden de la grilla")
    print("=" * 100)
    print(f"{'w':>5}{'sem':>5}{'solape':>8}{'Δ':>9}{'MDE':>8}{'p':>8}{'holm<':>8}"
          f"{'sem>0':>7}{'sin3':>9}" + "".join(f"{a:>8}" for a in ANIOS)
          + f"{'crudo':>9}{'atrR':>6}  veredicto")
    for _, r in T.iterrows():
        print(f"{r.w:5.2f}{r.semanas:5d}{r.solape:8.1%}{r.delta:+9.4f}{r.mde:8.4f}"
              f"{r.p:8.4f}{r.holm_umbral:8.4f}{r.sem_ok:7.0%}{r.sin_top3:+9.4f}"
              + "".join(f"{r[f'y{a}']:+8.4f}" for a in ANIOS)
              + f"{r.crudo:+9.5f}{r.atr_ratio:6.2f}  {r.veredicto}")


def main():
    ap = argparse.ArgumentParser(description="PREREGISTRO_LIBRO_RADAR")
    ap.add_argument("--reserva", action="store_true",
                    help="bajar y correr la reserva, SOLO si la principal la autoriza")
    a = ap.parse_args()

    TB = armar(INICIO, FIN_PRINCIPAL, reserva=False)

    # descriptivo, antes de cualquier Δ (§3.1 y §5.4)
    pn, pl = _pct(TB, "n_surge"), _pct(TB, "prof_1_rel")
    rho = pd.DataFrame({"t": TB["t"], "n": pn, "l": pl}).dropna() \
        .groupby("t").apply(lambda g: g["n"].corr(g["l"])).mean()
    print(f"\ncorrelacion de rangos n_surge ~ prof_1_rel dentro de la barra (media): "
          f"{rho:+.3f}")
    base = R.evaluar(TB, TB["n_surge"], "n_surge", objetivo="magnitud", k=K)
    print(f"BASE contra el universo: {base['spread']:+.4f} ATR | {base['semanas']} semanas "
          f"| {base['sem_ok']:.0%} arriba  (contexto, no compuerta)")

    T = principal(TB)
    mostrar(T)

    elegido = elegir(T)
    if elegido is None:
        print("\nNINGUN PESO PASA LA PRINCIPAL -> §4.1: el libro no mejora la eleccion del "
              "radar. El radar no cambia y la familia queda cerrada en esta forma.")
        return
    mde_r, ref, autoriza = potencia(T, elegido)
    print(f"\nELEGIDO: w = {elegido.w:.2f}  (Δ {elegido.delta:+.4f}, "
          f"{int((T.veredicto == 'PASA').sum())} de {len(T)} pesos pasan)")
    print(f"POTENCIA DE LA RESERVA: MDE_R ±{mde_r:.4f} contra {ref:+.4f} "
          f"({'mediana de los que pasan' if (T.veredicto == 'PASA').sum() > 1 else 'mitad del unico'})")
    if not autoriza:
        print("-> RESERVA NO AUTORIZADA (§4.2): pasa en muestra, la reserva no alcanza. "
              "No se baja; queda intacta.")
        return
    print("-> RESERVA AUTORIZADA para el brazo elegido.")
    if not a.reserva:
        print("   Correr de nuevo con --reserva para bajarla y medirla.")
        return

    # ── la reserva: un unico brazo ─────────────────────────────────────────────────────
    print("\nbajando la reserva...", flush=True)
    TR = armar(INICIO, FIN_RESERVA, reserva=True)
    corte = pd.Timestamp(FIN_PRINCIPAL, tz="UTC").value // 10**6
    TR = TR[TR["t"] >= corte]
    c = comparar(TR, float(elegido.w))
    sem = c["sem"]
    d_r = float(sem.mean())
    p_r = R._p_bloques(sem) if d_r > 0 else 1.0
    print("\n" + "=" * 100)
    print(f"RESERVA — {FIN_PRINCIPAL} -> {FIN_RESERVA} | w = {elegido.w:.2f} | "
          f"{len(sem)} semanas | solape {c['solape']:.1%}")
    print("=" * 100)
    print(f"Δ_R {d_r:+.4f} | p {p_r:.4f} | semanas > 0 {(sem > 0).mean():.0%} "
          f"(informativo) | crudo {float(c['sem_crudo'].mean()):+.5f}")
    if d_r > 0 and p_r < ALFA:
        print("-> PASA P Y R (§4.4): COMBO es candidato a reemplazar n_surge en el radar. "
              "No antes de la compuerta de PREREGISTRO_SKEW, y con §6 resuelto.")
    else:
        print("-> FALLA R (§4.3): era la muestra. Se cierra como §4.1.")


if __name__ == "__main__":
    main()
