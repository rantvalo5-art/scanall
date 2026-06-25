"""
P-C — Lead-lag sectorial (#3 del backlog). Prototipo OFFLINE, no toca producción.

Hipótesis tradeable: cuando un LÍDER rompe (pop 1h-3h fuerte), los LAGGARDS
correlacionados que TODAVÍA NO se movieron rinden por encima de su baseline en las
próximas horas → entrada TEMPRANA en el laggard (antes de su propia ruptura). Es el
único lever candidato a escapar el muro "shape-not-expectation" porque captura
símbolos DISTINTOS más temprano.

OJO al meta-patrón: si el movimiento del peer es CONTEMPORÁNEO (ya se movió con el
líder) NO hay edge; sólo cuenta el lag genuino (peer plano en t, sube en t→t+H).

FASE 1 (este pass) = factibilidad IN-SAMPLE sobre toda la ventana:
  - construir matriz de retornos horarios alineados del universo
  - corr contemporánea → clusters (top-k peers por símbolo)
  - eventos de ruptura del líder → forward return de peers no-movidos vs baseline
  Si NO aparece edge ni in-sample → STOP (sin gastar en walk-forward).
  Si aparece → FASE 2 = walk-forward (clusters en TRAIN, edge en TEST).

Insumos: diag_16w.json (ventana) + .backtest_cache/klines_<SYM>_1h_*.pkl.
Reusa sim_exit.load_tf. Corre con `py -3.13`.
"""
import json, sys, glob, statistics as st
from pathlib import Path
import numpy as np
import pandas as pd

from sim_exit import load_tf, CACHE_DIR

HOUR_MS = 3_600_000

# ---- parámetros de fase 1 ----
COVERAGE_MIN = 0.90      # símbolo entra si tiene >=90% de barras del grid horario
RET_CLIP = 0.50          # clip de retornos horarios (anti-junk)
TOPK_PEERS = 8           # peers por líder (los más correlacionados)
CORR_MIN = 0.45          # corr contemporánea mínima para ser peer
POP_WIN_H = 3            # ventana del "pop" del líder (retorno acumulado 3h)
POP_PCTL = 99.0          # percentil global de pops para marcar evento de líder
POP_ABS_MIN = 0.06       # ... y además el pop debe superar +6% absoluto
PEER_QUIET_MAX = 0.02    # el peer "no se movió aún" si su retorno 3h hasta t <= +2%
FWD_HS = (3, 6, 12, 24)  # horizontes forward a medir en el peer


def parse_dt(s):
    from datetime import datetime
    return datetime.fromisoformat(s.replace("Z", "+00:00"))


def build_return_matrix(t0_ms, t1_ms):
    """Matriz de retornos horarios alineados (grid común) del universo que cubre la ventana.
    Retorna (grid_ms[T], syms[N], R[T,N] con NaN donde falta, C close[T,N])."""
    grid = np.arange(t0_ms, t1_ms + HOUR_MS, HOUR_MS, dtype=np.int64)
    T = len(grid)
    pos = {int(t): k for k, t in enumerate(grid)}
    files = sorted({Path(p).name.split("_1h_")[0].replace("klines_", "")
                    for p in glob.glob(str(CACHE_DIR / "klines_*_1h_*.pkl"))})
    syms, cols = [], []
    for k, s in enumerate(files):
        if k % 300 == 0:
            print(f"  cargando 1h... {k}/{len(files)}", file=sys.stderr)
        df = load_tf(s, "1h")
        if df is None or len(df) < 100:
            continue
        ot = df.open_time.values.astype(np.int64)
        cl = df.close.values.astype(float)
        col = np.full(T, np.nan)
        m = (ot >= t0_ms) & (ot <= t1_ms)
        if m.sum() < COVERAGE_MIN * T:
            continue
        for t, c in zip(ot[m], cl[m]):
            j = pos.get(int(t))
            if j is not None:
                col[j] = c
        if np.isfinite(col).sum() < COVERAGE_MIN * T:
            continue
        syms.append(s); cols.append(col)
    C = np.array(cols).T                       # [T, N]
    R = np.full_like(C, np.nan)
    R[1:] = C[1:] / C[:-1] - 1.0
    R = np.clip(R, -RET_CLIP, RET_CLIP)
    return grid, syms, R, C


def corr_matrix(R):
    """Corr contemporánea pareada (Pearson) sobre retornos, ignorando NaN por columna via
    z-score con media/std nan-robusta y producto sobre solapamiento completo (filas sin NaN)."""
    good = np.isfinite(R).all(axis=1)          # filas sin ningún NaN → universo balanceado
    X = R[good]
    X = (X - X.mean(0)) / (X.std(0) + 1e-12)
    Cc = (X.T @ X) / X.shape[0]
    return Cc, good.sum()


def roll_cumret(C_col, win):
    """Retorno acumulado 'win' barras hasta t (close[t]/close[t-win]-1) por columna 1D."""
    out = np.full_like(C_col, np.nan)
    out[win:] = C_col[win:] / C_col[:-win] - 1.0
    return out


def residual_returns(R):
    """Retornos residuales tras quitar el factor de mercado (media transversal) con beta
    por símbolo: r_res[:,j] = r[:,j] - beta_j * m. Aísla el move IDIOSINCRÁTICO del líder
    (rompe mientras el mercado está plano) → único escenario con lead-lag genuino posible."""
    m = np.nanmean(R, axis=1)                  # factor de mercado [T]
    Rres = np.full_like(R, np.nan)
    good_t = np.isfinite(m)
    for j in range(R.shape[1]):
        col = R[:, j]
        ok = good_t & np.isfinite(col)
        mm, cc = m[ok], col[ok]
        beta = np.dot(mm - mm.mean(), cc - cc.mean()) / (np.dot(mm - mm.mean(), mm - mm.mean()) + 1e-12)
        Rres[ok, j] = col[ok] - beta * m[ok]
    return Rres


def roll_sum(M, win):
    """Suma móvil 'win' barras hasta t (aprox retorno acumulado para series de retornos)."""
    out = np.full_like(M, np.nan)
    cs = np.cumsum(np.nan_to_num(M, nan=0.0), axis=0)
    out[win:] = cs[win:] - cs[:-win]
    out[:win] = np.nan
    # invalidar donde había NaN dentro de la ventana
    nanmask = np.isnan(M)
    csn = np.cumsum(nanmask.astype(int), axis=0)
    bad = np.full_like(M, False, dtype=bool)
    bad[win:] = (csn[win:] - csn[:-win]) > 0
    out[bad] = np.nan
    return out


def fwd_sum(M, win):
    """Suma de retornos t→t+win (forward), por columna."""
    T = M.shape[0]
    out = np.full_like(M, np.nan)
    cs = np.cumsum(np.nan_to_num(M, nan=0.0), axis=0)
    out[:T-win] = cs[win:] - cs[:T-win]
    nanmask = np.isnan(M)
    csn = np.cumsum(nanmask.astype(int), axis=0)
    bad = np.full_like(M, False, dtype=bool)
    bad[:T-win] = (csn[win:] - csn[:T-win]) > 0
    out[bad] = np.nan
    return out


def event_study(label, popmat, fwd_by_H, peers, pop_thr, N, T):
    """Estudio de evento: líder rompe (popmat[t,i]>=pop_thr), peers quietos (popmat[t,p]<=quiet)
    → forward del peer vs baseline incondicional (peer quieto sin pop de líder)."""
    cond = {H: [] for H in FWD_HS}
    contemp = {H: [] for H in FWD_HS}
    base = {H: [] for H in FWD_HS}
    n_events = quiet_hits = moved_hits = 0
    for i in peers:
        ev_t = np.where(popmat[:, i] >= pop_thr)[0]
        ev_keep, last = [], -10_000
        for t in ev_t:
            if t - last >= POP_WIN_H:
                ev_keep.append(t); last = t
        for t in ev_keep:
            if t + max(FWD_HS) >= T:
                continue
            n_events += 1
            for p in peers[i]:
                pq = popmat[t, p]
                if not np.isfinite(pq):
                    continue
                quiet = pq <= PEER_QUIET_MAX
                for H in FWD_HS:
                    fv = fwd_by_H[H][t, p]
                    if np.isfinite(fv):
                        (cond[H] if quiet else contemp[H]).append(fv)
                quiet_hits += quiet; moved_hits += (not quiet)
    rng = np.random.default_rng(0)
    all_peers = sorted({p for v in peers.values() for p in v})
    for p in all_peers:
        idx = np.where(popmat[:, p] <= PEER_QUIET_MAX)[0]
        idx = idx[idx + max(FWD_HS) < T]
        if len(idx) > 2000:
            idx = rng.choice(idx, 2000, replace=False)
        for H in FWD_HS:
            base[H].extend(v for v in fwd_by_H[H][idx, p] if np.isfinite(v))

    qf = quiet_hits / (quiet_hits + moved_hits + 1e-9)
    print(f"\n=== {label} ===")
    print(f"Eventos de ruptura de líder: {n_events} | fracción peer quieta = {qf*100:.0f}%")
    print(f"  {'H(h)':>5} | {'COND quiet':>22} | {'BASELINE quiet':>22} | {'EDGE':>8} | {'contemp':>8}")
    print(f"  {'':>5} | {'n':>6} {'med%':>7} {'mean%':>7} | {'n':>6} {'med%':>7} {'mean%':>7} | {'Δmean':>8} | {'mean%':>8}")
    rows = {}
    for H in FWD_HS:
        c, b, cm = cond[H], base[H], contemp[H]
        cmean = np.mean(c)*100 if c else float("nan")
        bmean = np.mean(b)*100 if b else float("nan")
        cmmean = np.mean(cm)*100 if cm else float("nan")
        print(f"  {H:>5} | {len(c):>6} {st.median(c)*100 if c else float('nan'):>6.2f} {cmean:>7.2f} | "
              f"{len(b):>6} {st.median(b)*100 if b else float('nan'):>6.2f} {bmean:>7.2f} | "
              f"{cmean-bmean:>+7.2f} | {cmmean:>7.2f}")
        rows[H] = {"cond_n": len(c), "cond_mean": cmean, "base_mean": bmean,
                   "edge_mean_pp": cmean-bmean, "contemp_mean": cmmean}
    return {"label": label, "n_events": n_events, "quiet_frac": qf, "by_H": rows}


def main():
    diag = sys.argv[1] if len(sys.argv) > 1 else "diag_16w.json"
    alerts = json.load(open(diag))["main"]
    t0 = min(parse_dt(a["alerted_at"]) for a in alerts)
    t1 = max(parse_dt(a["alerted_at"]) for a in alerts)
    t0_ms, t1_ms = int(t0.timestamp() * 1000), int(t1.timestamp() * 1000)
    print(f"Ventana {t0.date()} -> {t1.date()} | grid horario ~{(t1_ms-t0_ms)//HOUR_MS} barras")

    grid, syms, R, C = build_return_matrix(t0_ms, t1_ms)
    N, T = len(syms), len(grid)
    print(f"Universo 1h con cobertura>= {COVERAGE_MIN*100:.0f}%: {N} símbolos | {T} barras\n")
    if N < 20:
        print("Universo insuficiente; abortar.")
        return

    Cc, nrows = corr_matrix(R)
    print(f"Corr contemporánea sobre {nrows} barras balanceadas. "
          f"corr media off-diag = {(Cc.sum()-N)/(N*N-N):.3f}\n")

    # pops 3h por símbolo (líder) + flag quiet del peer
    pop = np.vstack([roll_cumret(C[:, j], POP_WIN_H) for j in range(N)]).T   # [T,N]
    finite_pops = pop[np.isfinite(pop)]
    pop_thr = max(np.percentile(finite_pops, POP_PCTL), POP_ABS_MIN)
    print(f"Umbral de pop líder (3h): max(p{POP_PCTL:.0f}={np.percentile(finite_pops,POP_PCTL)*100:.1f}%, "
          f"{POP_ABS_MIN*100:.0f}%) = {pop_thr*100:.1f}%")

    # peers por símbolo
    peers = {}
    for i in range(N):
        order = np.argsort(-Cc[:, i])
        ps = [j for j in order if j != i and Cc[j, i] >= CORR_MIN][:TOPK_PEERS]
        if ps:
            peers[i] = ps
    print(f"Símbolos con >=1 peer (corr>={CORR_MIN}): {len(peers)} | "
          f"peers medios = {np.mean([len(v) for v in peers.values()]):.1f}\n")

    # forward returns RAW por horizonte
    fwd = {}
    for H in FWD_HS:
        f = np.full((T, N), np.nan)
        f[:T-H] = C[H:] / C[:T-H] - 1.0
        fwd[H] = f

    # ── (A) RAW: pop = retorno 3h crudo (confundido por factor de mercado) ──
    res_raw = event_study("RAW (pop crudo — confundido por factor de mercado)",
                          pop, fwd, peers, pop_thr, N, T)

    # ── (B) IDIOSINCRÁTICO: quitar factor de mercado (beta·m) → líder rompe SOLO ──
    Rres = residual_returns(R)
    pop_idio = roll_sum(Rres, POP_WIN_H)            # pop residual 3h
    fwd_idio = {H: fwd_sum(Rres, H) for H in FWD_HS}
    fin = pop_idio[np.isfinite(pop_idio)]
    pop_thr_idio = max(np.percentile(fin, POP_PCTL), POP_ABS_MIN)
    print(f"\nUmbral pop IDIO (3h residual): max(p{POP_PCTL:.0f}={np.percentile(fin,POP_PCTL)*100:.1f}%, "
          f"{POP_ABS_MIN*100:.0f}%) = {pop_thr_idio*100:.1f}%")
    res_idio = event_study("IDIOSINCRÁTICO (líder rompe vs mercado; forward residual)",
                           pop_idio, fwd_idio, peers, pop_thr_idio, N, T)

    Path("sim_leadlag_phase1.json").write_text(json.dumps({
        "n_syms": N, "corr_mean_offdiag": float((Cc.sum()-N)/(N*N-N)),
        "raw": res_raw, "idio": res_idio}, indent=2))
    print("\n  -> sim_leadlag_phase1.json")
    print("\n  VEREDICTO: el lever pasa SOLO si IDIO tiene EDGE (Δmean) claramente >0 en varios H "
          "\n  con fracción quieta no trivial. Si IDIO también es ~0/negativo → STOP (sin walk-forward).")


if __name__ == "__main__":
    main()
