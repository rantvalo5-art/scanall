"""
diag_score_gap.py — Diagnóstico de scoring gap: ¿por qué los top movers no caen en BEST?

Lee swing/compare_X1b_12w.json["old"] (1716 filas, sin backtest).
Produce:
  A1 · Score-gap de top movers: score, signal_type, bucket, distancia al umbral
  A2 · Barrido de thresholds: tabla de tradeoff para ~12 configs simuladas
  A3 · Separabilidad: ¿hay features que separen movers de no-movers en el cohorte STRONG?
  A4 · Estabilidad OOS: repite A2/A3 en mitad-1 vs mitad-2 temporal
"""

import json
from collections import defaultdict

# ──────────────────────────────────────────────────────────────────────────────
# Utilidades básicas
# ──────────────────────────────────────────────────────────────────────────────

INPUT_FILE = "swing/compare_DIAG_REDUMP.json"
OOS_SPLIT  = "2026-05-01T00:00:00+00:00"  # midpoint ~8 semanas (2026-04-03 → 2026-05-29)


def pct_move(high, base):
    if not high or not base:
        return None
    return (high - base) / base * 100


def final_bucket_sim(score, signal_type, best_per, strong_per, best_flat, strong_flat):
    """Réplica modo absolute de final_bucket (swing/backtest.py:1003)."""
    best_thr   = best_per.get(signal_type, best_flat)
    strong_thr = strong_per.get(signal_type, strong_flat)
    if score >= best_thr:   return "BEST"
    if score >= strong_thr: return "STRONG"
    return "WATCH"


def compute_stats(rows, best_per, strong_per, best_flat, strong_flat):
    """
    Dado un conjunto de filas y una config de thresholds, devuelve:
      top30_best, pct_20_in_best, best_count, best_win7d
    La métrica top30 usa DEDUPE POR SÍMBOLO (alerta más temprana por símbolo).
    """
    # Calcular move 7d y rebucketear
    for r in rows:
        r["_move7"] = pct_move(r.get("max_high_7d"), r.get("entry_price"))
        r["_bucket"] = final_bucket_sim(r["score"], r["signal_type"],
                                        best_per, strong_per, best_flat, strong_flat)

    # Dedupe por símbolo: quedarse con la alerta más temprana por símbolo
    seen = {}
    for r in sorted(rows, key=lambda x: x["alerted_at"]):
        if r["symbol"] not in seen:
            seen[r["symbol"]] = r

    deduped = list(seen.values())
    deduped_with_move = [(r["_move7"], r) for r in deduped if r["_move7"] is not None]
    deduped_with_move.sort(key=lambda x: x[0], reverse=True)

    # Top-30 movers (deduped por símbolo)
    top30 = [r for _, r in deduped_with_move[:30]]
    top30_best = sum(1 for r in top30 if r["_bucket"] == "BEST")

    # % movers ≥20% que caen en BEST (sobre todas las filas, no solo deduped)
    big_movers = [r for r in rows if (r["_move7"] or 0) >= 20]
    pct_20_best = (sum(1 for r in big_movers if r["_bucket"] == "BEST") /
                   len(big_movers) * 100) if big_movers else 0.0

    # BEST count (todas las filas)
    best_rows = [r for r in rows if r["_bucket"] == "BEST"]
    best_count = len(best_rows)

    # BEST win7d% (filas con move disponible)
    best_moves = [r["_move7"] for r in best_rows if r["_move7"] is not None]
    best_win7 = (sum(1 for m in best_moves if m >= 7) /
                 len(best_moves) * 100) if best_moves else 0.0

    return top30_best, pct_20_best, best_count, best_win7


# ──────────────────────────────────────────────────────────────────────────────
# Configuraciones a barrer (A2)
# ──────────────────────────────────────────────────────────────────────────────

BASELINE_BEST = 12
BASELINE_STRONG = 10

THRESHOLD_SWEEP = [
    # (label, best_per, strong_per, best_flat, strong_flat)
    ("BASELINE (flat 12)",  {},                                      {}, 12, 10),
    ("flat-11",             {},                                      {}, 11, 10),
    ("flat-10",             {},                                      {}, 10,  8),

    # Handoff variante A
    ("A: flat-11",          {},                                      {}, 11, 10),

    # Handoff variante B (per-signal)
    ("B: BO11/PRE10/H11/R9",
        {"BREAKOUT": 11, "PREBREAK": 10, "HOLD": 11, "RIDING": 9},  {}, 12, 10),

    # Handoff variante C (solo BREAKOUT)
    ("C: BREAKOUT=11",      {"BREAKOUT": 11},                        {}, 12, 10),

    # Combinaciones adicionales
    ("BO11+HOLD11",         {"BREAKOUT": 11, "HOLD": 11},            {}, 12, 10),
    ("BO11+R11",            {"BREAKOUT": 11, "RIDING": 11},          {}, 12, 10),
    ("BO10",                {"BREAKOUT": 10},                        {}, 12, 10),
    ("HOLD11",              {"HOLD": 11},                            {}, 12, 10),
    ("BO11+HOLD11+R11",     {"BREAKOUT": 11, "HOLD": 11, "RIDING": 11}, {}, 12, 10),
    ("all-per-signal-11",   {"BREAKOUT": 11, "PREBREAK": 11, "HOLD": 11, "RIDING": 11}, {}, 12, 10),
]


# ──────────────────────────────────────────────────────────────────────────────
# A1 — Score-gap: estado actual de los top movers
# ──────────────────────────────────────────────────────────────────────────────

def print_a1(rows):
    print()
    print("═" * 78)
    print(" A1 · SCORE-GAP: estado actual de los TOP movers (deduped por símbolo)")
    print("═" * 78)

    # Rebucketear con baseline
    for r in rows:
        r["_move7"] = pct_move(r.get("max_high_7d"), r.get("entry_price"))
        r["_bucket"] = final_bucket_sim(r["score"], r["signal_type"],
                                        {}, {}, BASELINE_BEST, BASELINE_STRONG)

    # Dedupe por símbolo (alerta más temprana)
    seen = {}
    for r in sorted(rows, key=lambda x: x["alerted_at"]):
        if r["symbol"] not in seen:
            seen[r["symbol"]] = r

    deduped_with_move = [(r["_move7"], r) for r in seen.values() if r["_move7"] is not None]
    deduped_with_move.sort(key=lambda x: x[0], reverse=True)
    top30 = [(m, r) for m, r in deduped_with_move[:30]]

    print(f"\n{'#':>3}  {'Symbol':<14}  {'Type':<10}  {'Score':>5}  {'Bucket':<6}  "
          f"{'move7d%':>8}  {'gap':>5}")
    print("─" * 70)
    for i, (move, r) in enumerate(top30, 1):
        best_thr = BASELINE_BEST
        gap = best_thr - r["score"]  # >0 => le falta gap puntos para llegar a BEST
        flag = ""
        if r["_bucket"] != "BEST":
            flag = " ← MISS"
        print(f"{i:>3}  {r['symbol']:<14}  {r['signal_type']:<10}  {r['score']:>5}  "
              f"{r['_bucket']:<6}  {move:>+8.2f}%  {gap:>+5}{flag}")

    # Resumen
    best_n   = sum(1 for _, r in top30 if r["_bucket"] == "BEST")
    strong_n = sum(1 for _, r in top30 if r["_bucket"] == "STRONG")
    watch_n  = sum(1 for _, r in top30 if r["_bucket"] == "WATCH")
    print(f"\n  Resumen top30 (deduped): BEST={best_n}/30  STRONG={strong_n}/30  WATCH={watch_n}/30")

    # ¿A cuántos les falta 1 punto? ¿2 puntos?
    miss_by = defaultdict(int)
    for _, r in top30:
        if r["_bucket"] != "BEST":
            gap = BASELINE_BEST - r["score"]
            miss_by[gap] += 1
    print("  Distancia al umbral BEST (no-BEST movers):")
    for g in sorted(miss_by):
        print(f"    gap={g}: {miss_by[g]} movers")

    # % movers ≥20%
    big_movers = [r for r in rows if (r["_move7"] or 0) >= 20]
    big_best   = sum(1 for r in big_movers if r["_bucket"] == "BEST")
    print(f"\n  Movers ≥20%: {len(big_movers)} total,  {big_best} en BEST "
          f"({big_best/len(big_movers)*100:.0f}%)" if big_movers else
          "\n  Movers ≥20%: ninguno")

    # Distribución de scores en el top30 por señal
    print("\n  Distribución score × señal (top30 movers):")
    sd = defaultdict(lambda: defaultdict(int))
    for _, r in top30:
        sd[r["signal_type"]][r["score"]] += 1
    for st in sorted(sd):
        print(f"    {st:<10}  {dict(sorted(sd[st].items()))}")


# ──────────────────────────────────────────────────────────────────────────────
# A2 — Barrido de thresholds (simulado)
# ──────────────────────────────────────────────────────────────────────────────

def print_a2(rows, title="A2 · BARRIDO DE THRESHOLDS (simulado, todas las filas)"):
    print()
    print("═" * 90)
    print(f" {title}")
    print("═" * 90)
    print(f"  {'Config':<30}  {'top30_best':>10}  {'%≥20%_best':>11}  "
          f"{'BEST_n':>7}  {'BEST_win7d%':>11}  {'BEST_delta':>10}")
    print("─" * 90)

    # Baseline para calcular delta
    base_best_n = sum(1 for r in rows if r.get("_bucket") == "BEST")
    # Asegurar bucket calculado para baseline
    for r in rows:
        r["_move7"] = pct_move(r.get("max_high_7d"), r.get("entry_price"))
        r["_bucket"] = final_bucket_sim(r["score"], r["signal_type"],
                                        {}, {}, BASELINE_BEST, BASELINE_STRONG)
    base_best_n = sum(1 for r in rows if r["_bucket"] == "BEST")

    for label, best_per, strong_per, best_flat, strong_flat in THRESHOLD_SWEEP:
        t30, pct20, bn, bw = compute_stats(rows, best_per, strong_per,
                                           best_flat, strong_flat)
        delta = bn - base_best_n
        marker = ""
        if t30 >= 11 and bw >= 60 and delta <= base_best_n * 0.5:
            marker = " ✓"
        print(f"  {label:<30}  {t30:>10}  {pct20:>10.1f}%  "
              f"{bn:>7}  {bw:>10.1f}%  {delta:>+10}{marker}")


# ──────────────────────────────────────────────────────────────────────────────
# A3 — Separabilidad: features en STRONG (scores 10-11)
# ──────────────────────────────────────────────────────────────────────────────

def print_a3(rows, title="A3 · SEPARABILIDAD DE FEATURES EN COHORTE STRONG (scores 10-11)"):
    print()
    print("═" * 90)
    print(f" {title}")
    print("═" * 90)

    FEATURES = ["cvd_ratio", "obv_slope", "recent_long_ok", "htf_1d_up",
                "htf_1w_up",
                # features extendidas (solo disponibles en JSONs post-redump)
                "vol_ratio", "bb_width", "vol_growth",
                "dist_to_res", "breakout_distance", "bars_since_break", "riding_gain"]

    # Re-asegurar _move7
    for r in rows:
        if "_move7" not in r:
            r["_move7"] = pct_move(r.get("max_high_7d"), r.get("entry_price"))

    signal_types = sorted(set(r["signal_type"] for r in rows))

    for st in signal_types:
        strong_rows = [r for r in rows
                       if r["signal_type"] == st
                       and r.get("bucket") == "STRONG"  # bucket original del archivo
                       and r["score"] in (10, 11)
                       and r["_move7"] is not None]
        if len(strong_rows) < 8:
            print(f"\n  {st}: cohorte STRONG muy pequeña (n={len(strong_rows)}), skip")
            continue

        moves = sorted([r["_move7"] for r in strong_rows])
        q25 = moves[len(moves) // 4]
        q75 = moves[3 * len(moves) // 4]
        top_q = [r for r in strong_rows if r["_move7"] >= q75]
        bot_q = [r for r in strong_rows if r["_move7"] <= q25]

        print(f"\n  ── {st}  (n={len(strong_rows)}, "
              f"move7d median={moves[len(moves)//2]:+.2f}%  "
              f"Q25={q25:+.2f}%  Q75={q75:+.2f}%)")
        print(f"  {'Feature':<20}  {'top-Q avg':>10}  {'bot-Q avg':>10}  {'Δ':>8}  {'note'}")
        print("  " + "─" * 65)

        for feat in FEATURES:
            def get_val(r, f):
                v = r.get(f)
                if isinstance(v, bool):
                    return float(v)
                if v is None:
                    return None
                try:
                    return float(v)
                except Exception:
                    return None

            tv = [get_val(r, feat) for r in top_q if get_val(r, feat) is not None]
            bv = [get_val(r, feat) for r in bot_q if get_val(r, feat) is not None]
            if not tv or not bv:
                print(f"  {feat:<20}  {'n/a':>10}  {'n/a':>10}  {'':>8}")
                continue
            ta, ba = sum(tv) / len(tv), sum(bv) / len(bv)
            delta = ta - ba
            # Separabilidad fuerte si |delta| > 0.1 para ratios, > 0.15 para booleanos
            strong_sep = abs(delta) >= 0.1
            note = "← SEPARABLE" if strong_sep else ""
            print(f"  {feat:<20}  {ta:>+10.3f}  {ba:>+10.3f}  {delta:>+8.3f}  {note}")


# ──────────────────────────────────────────────────────────────────────────────
# A4 — Split OOS temporal
# ──────────────────────────────────────────────────────────────────────────────

def print_a4(rows):
    print()
    print("═" * 90)
    print(f" A4 · ESTABILIDAD OOS TEMPORAL  (split en {OOS_SPLIT})")
    print("═" * 90)

    h1 = [r for r in rows if r["alerted_at"] <  OOS_SPLIT]
    h2 = [r for r in rows if r["alerted_at"] >= OOS_SPLIT]
    print(f"  Mitad-1 (antes split): n={len(h1)}  |  Mitad-2 (después split): n={len(h2)}")

    for split_rows, split_label in [(h1, "mitad-1"), (h2, "mitad-2")]:
        print(f"\n  ── Sweep {split_label} ──")
        print(f"  {'Config':<30}  {'top30_best':>10}  {'%≥20%_best':>11}  "
              f"{'BEST_n':>7}  {'BEST_win7d%':>11}")
        print("  " + "─" * 70)
        for label, best_per, strong_per, best_flat, strong_flat in THRESHOLD_SWEEP:
            t30, pct20, bn, bw = compute_stats(
                [dict(r) for r in split_rows],  # copia para no mutar
                best_per, strong_per, best_flat, strong_flat)
            marker = " ✓" if t30 >= 8 and bw >= 58 else ""
            print(f"  {label:<30}  {t30:>10}  {pct20:>10.1f}%  {bn:>7}  {bw:>10.1f}%{marker}")

    # Separabilidad en cada mitad
    for split_rows, split_label in [(h1, "mitad-1"), (h2, "mitad-2")]:
        print_a3([dict(r) for r in split_rows],
                 title=f"A3 · SEPARABILIDAD — {split_label}")


# ──────────────────────────────────────────────────────────────────────────────
# A5 — Simulación de bonus en el cohorte WATCH
# ──────────────────────────────────────────────────────────────────────────────

# Grilla de candidatos: (label, signal_type, feature, threshold, +N)
# vol_ratio/vol_growth/bb_width/dist_to_res ya son INPUTS del score actual
# (backtest.py:1136-1139).  Un bonus aquí = reponderar algo ya priceado.
BONUS_GRID = [
    # Los más plausibles para PREBREAK (multiplicativo vol·growth·bb)
    ("PRE vol_growth>3.5 +2",     "PREBREAK", "vol_growth", 3.5, 2),
    ("PRE vol_growth>3.5 +3",     "PREBREAK", "vol_growth", 3.5, 3),
    ("PRE vol_growth>5.0 +3",     "PREBREAK", "vol_growth", 5.0, 3),
    ("PRE vol_ratio>4.0  +2",     "PREBREAK", "vol_ratio",  4.0, 2),
    ("PRE vol_ratio>4.0  +3",     "PREBREAK", "vol_ratio",  4.0, 3),
    ("PRE vol_ratio>5.0  +3",     "PREBREAK", "vol_ratio",  5.0, 3),
    # BREAKOUT
    ("BO  vol_ratio>4.5  +2",     "BREAKOUT", "vol_ratio",  4.5, 2),
    ("BO  vol_ratio>4.5  +3",     "BREAKOUT", "vol_ratio",  4.5, 3),
    # Controles: features ya priceadas vía bb_factor/gate
    ("PRE bb_width>0.025 +2 [c]", "PREBREAK", "bb_width",   0.025, 2),
    ("BO  bb_width>0.08  +2 [c]", "BREAKOUT", "bb_width",   0.080, 2),
]


def apply_bonus(rows, signal_type, feature, threshold, plus):
    """Devuelve lista de copias (dict) con score ajustado donde aplica el bonus."""
    result = []
    for r in rows:
        nr = dict(r)
        if nr.get("signal_type") == signal_type:
            v = nr.get(feature)
            if v is not None:
                try:
                    if float(v) > threshold:
                        nr["score"] = nr["score"] + plus
                except (TypeError, ValueError):
                    pass
        result.append(nr)
    return result


def promoted_split(rows_base, rows_bonus, move_thr=20.0):
    """
    Cuenta las filas que cruzan no-BEST → BEST gracias al bonus.
    Requiere que _bucket esté calculado en ambas listas.
    Devuelve (movers_promovidos, non_movers_promovidos).
    """
    movers_in = 0
    non_in = 0
    for rb, r2 in zip(rows_base, rows_bonus):
        if rb.get("_bucket") != "BEST" and r2.get("_bucket") == "BEST":
            if (rb.get("_move7") or 0) >= move_thr:
                movers_in += 1
            else:
                non_in += 1
    return movers_in, non_in


def _a5_run(rows_base, rows_source, sig, feat, thr, plus, base_n, base_t30):
    """Calcula stats de un candidato bonus sobre un conjunto de filas."""
    b = apply_bonus(rows_source, sig, feat, thr, plus)
    t30, _, bn, bw = compute_stats(b, {}, {}, BASELINE_BEST, BASELINE_STRONG)
    pm, pn = promoted_split(rows_base, b)
    return t30, bn - base_n, bw, pm, pn


def print_a5(rows):
    print()
    print("═" * 105)
    print(" A5 · SIMULACIÓN DE BONUS EN COHORTE WATCH  (score' = score + N·[feature > thr])")
    print("    NOTA: vol_ratio / vol_growth / bb_width ya son inputs del score PREBREAK/BO")
    print("    (backtest.py:1136-1139).  Bonus aquí = reponderar lo ya priceado.")
    print("═" * 105)
    print(f"  Criterio ✓: top30_best sube  Y  ΔBEST≤+50%  Y  pmov>pnon  Y  se sostiene h1 y h2")
    print()

    # Asegurar baseline _bucket y _move7 en rows (puede que ya estén de A4 — forzar para certeza)
    for r in rows:
        r["_move7"]  = pct_move(r.get("max_high_7d"), r.get("entry_price"))
        r["_bucket"] = final_bucket_sim(r["score"], r["signal_type"],
                                        {}, {}, BASELINE_BEST, BASELINE_STRONG)

    base_best_n       = sum(1 for r in rows if r["_bucket"] == "BEST")
    base_t30, _, _, _ = compute_stats([dict(r) for r in rows], {}, {}, BASELINE_BEST, BASELINE_STRONG)

    h1 = [r for r in rows if r["alerted_at"] <  OOS_SPLIT]
    h2 = [r for r in rows if r["alerted_at"] >= OOS_SPLIT]
    base_h1_n        = sum(1 for r in h1 if r["_bucket"] == "BEST")
    base_h2_n        = sum(1 for r in h2 if r["_bucket"] == "BEST")
    base_h1_t30, _, _, _ = compute_stats([dict(r) for r in h1], {}, {}, BASELINE_BEST, BASELINE_STRONG)
    base_h2_t30, _, _, _ = compute_stats([dict(r) for r in h2], {}, {}, BASELINE_BEST, BASELINE_STRONG)

    # Cabecera
    print(f"  {'Candidato':<30}  "
          f"{'FULL: t30 ΔB  w7d  pmov/pnon':>30}  "
          f"{'H1: t30 ΔB pmov/pnon':>22}  "
          f"{'H2: t30 ΔB pmov/pnon':>22}  ok")
    print("  " + "─" * 112)

    for label, sig, feat, thr, plus in BONUS_GRID:
        # Full
        t30,  db,  bw,  pm,  pn  = _a5_run(rows, [dict(r) for r in rows],
                                             sig, feat, thr, plus, base_best_n, base_t30)
        # H1
        t30_h1, db_h1, _, pm_h1, pn_h1 = _a5_run(h1, [dict(r) for r in h1],
                                                   sig, feat, thr, plus, base_h1_n, base_h1_t30)
        # H2
        t30_h2, db_h2, _, pm_h2, pn_h2 = _a5_run(h2, [dict(r) for r in h2],
                                                   sig, feat, thr, plus, base_h2_n, base_h2_t30)

        # Criterio de victoria
        ok = (t30 > base_t30
              and db <= base_best_n * 0.5
              and pm > pn
              and t30_h1 >= base_h1_t30 and t30_h2 >= base_h2_t30
              and (pm_h1 + pm_h2) > (pn_h1 + pn_h2))
        marker = " ✓" if ok else "  "

        full_s = f"{t30:2d}  {db:+4d}  {bw:4.0f}%  {pm}mov/{pn}non"
        h1_s   = f"{t30_h1:2d}  {db_h1:+4d}  {pm_h1}m/{pn_h1}n"
        h2_s   = f"{t30_h2:2d}  {db_h2:+4d}  {pm_h2}m/{pn_h2}n"

        print(f"  {label:<30}  {full_s:>30}  {h1_s:>22}  {h2_s:>22}{marker}")

    print(f"\n  Baseline:  full t30={base_t30}  BEST_n={base_best_n}"
          f"  |  h1 t30={base_h1_t30} BEST_n={base_h1_n}"
          f"  |  h2 t30={base_h2_t30} BEST_n={base_h2_n}")


# ──────────────────────────────────────────────────────────────────────────────
# Main
# ──────────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    with open(INPUT_FILE, encoding="utf-8") as f:
        data = json.load(f)

    # Soporta tanto lista directa como dict {"old": [...]}
    if isinstance(data, list):
        rows = data
    elif isinstance(data, dict) and "old" in data:
        rows = data["old"]
    else:
        rows = list(data.values())[0]
    print(f"Input: {INPUT_FILE}  |  filas: {len(rows)}")

    print_a1(rows)
    print_a2(rows)
    print_a3(rows)
    print_a4(rows)
    print_a5(rows)

    print()
    print("═" * 105)
    print(" DONE — no se modificó ningún archivo")
    print("═" * 105)
