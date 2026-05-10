"""
sweep.py — Sweep sistemático de parámetros para backtest.py.

Uso:
    python sweep.py sweep_example.json
    python sweep.py sweep_example.json --top 15 --no-cleanup

Lee un sweep.json con la grilla de parámetros, genera configs temporales,
llama a backtest.py --variants por cada ventana temporal, parsea los resultados,
calcula una métrica objetivo única y emite un ranking con robustez multi-ventana.
"""

import argparse
import copy
import json
import os
import shutil
import subprocess
import sys
from itertools import product
from pathlib import Path
from statistics import mean, stdev


# ──────────────────────────────────────────────────────────────────────────────
# Métricas
# ──────────────────────────────────────────────────────────────────────────────

def pct_move(new, old):
    if new is None or old is None or old == 0:
        return None
    return (new - old) / old * 100


def compute_stats(alerts, period_days=7):
    """Replica la lógica de stats() en compare_runs() de backtest.py."""
    if not alerts:
        return None
    moves = [pct_move(a.get("max_high_24h"), a.get("entry_price")) for a in alerts]
    moves = [m for m in moves if m is not None]
    drops = [pct_move(a.get("min_low_24h"), a.get("entry_price")) for a in alerts]
    drops = [d for d in drops if d is not None]

    best   = [a for a in alerts if a.get("bucket") == "BEST"]
    strong = [a for a in alerts if a.get("bucket") == "STRONG"]

    best_moves = [pct_move(a.get("max_high_24h"), a.get("entry_price")) for a in best]
    best_moves = [m for m in best_moves if m is not None]
    best_drops = [pct_move(a.get("min_low_24h"), a.get("entry_price")) for a in best]
    best_drops = [d for d in best_drops if d is not None]

    # catch rate — top 30 movers en BEST+STRONG
    ranked = sorted(alerts,
                    key=lambda a: pct_move(a.get("max_high_24h"), a.get("entry_price")) or -999,
                    reverse=True)
    top30 = ranked[:30]
    top30_best_strong = sum(1 for a in top30 if a.get("bucket") in ("BEST", "STRONG"))

    best_win_10 = (sum(1 for m in best_moves if m >= 10) * 100 / len(best_moves)
                   if best_moves else 0)
    best_avg_max = mean(best_moves) if best_moves else 0
    best_avg_dd  = mean(best_drops) if best_drops else 0
    best_rr      = abs(best_avg_max / best_avg_dd) if best_avg_dd != 0 else 0
    best_per_day = len(best) / period_days if period_days > 0 else 0

    return {
        "n": len(alerts),
        "best_n": len(best),
        "strong_n": len(strong),
        "best_per_day": best_per_day,
        "top30_in_best_strong": top30_best_strong,
        "best_win_10pct": best_win_10,
        "best_max": best_avg_max,
        "best_rr": best_rr,
    }


def score_stats(stats, weights):
    """Calcula el score objetivo [0, 1] para un conjunto de stats."""
    w = weights
    catch    = stats["top30_in_best_strong"] / 30
    quality  = stats["best_win_10pct"] / 100
    volume   = min(1.0, stats["best_per_day"] / 15)
    rr       = min(1.0, stats["best_rr"] / 4)
    overflow = max(0.0, stats["best_per_day"] - 25) / 25

    return (w.get("catch",   0.40) * catch
          + w.get("quality", 0.30) * quality
          + w.get("volume",  0.20) * volume
          + w.get("rr",      0.10) * rr
          - w.get("volume_overflow_penalty", 0.20) * overflow)


# ──────────────────────────────────────────────────────────────────────────────
# Generación de configs
# ──────────────────────────────────────────────────────────────────────────────

def set_nested(d, dotted_key, value):
    """Asigna d["section"]["KEY"] = value usando "section.KEY"."""
    parts = dotted_key.split(".", 1)
    if len(parts) == 1:
        d[parts[0]] = value
    else:
        section, key = parts
        d.setdefault(section, {})[key] = value


def generate_configs(base_cfg, params, out_dir):
    """
    Genera todos los configs en out_dir/ a partir de la grilla de params.
    Devuelve lista de (combo_dict, config_path).
    """
    keys   = list(params.keys())
    values = list(params.values())
    combos = []
    for vals in product(*values):
        combo = dict(zip(keys, vals))
        cfg = copy.deepcopy(base_cfg)
        for k, v in combo.items():
            set_nested(cfg, k, v)
        combos.append((combo, cfg))

    paths = []
    for i, (combo, cfg) in enumerate(combos):
        path = out_dir / f"cfg_{i:04d}.json"
        path.write_text(json.dumps(cfg, indent=2), encoding="utf-8")
        paths.append((combo, path))
    return paths


# ──────────────────────────────────────────────────────────────────────────────
# Llamadas a backtest.py
# ──────────────────────────────────────────────────────────────────────────────

def run_backtest_variants(base_path, variant_paths, weeks, max_pairs,
                          end_date, out_json, scan_interval=15):
    """Invoca backtest.py --variants ... --end-date ... --out ... como subproceso."""
    cmd = [
        sys.executable, "backtest.py",
        "--weeks", str(weeks),
        "--max-pairs", str(max_pairs),
        "--scan-interval-min", str(scan_interval),
        "--end-date", end_date,
        "--out", str(out_json),
        "--variants", str(base_path),
        *[str(p) for p in variant_paths],
    ]
    print(f"  Corriendo backtest --end-date {end_date} con {len(variant_paths)} variantes...")
    result = subprocess.run(cmd, capture_output=False, text=True)
    return result.returncode == 0


# ──────────────────────────────────────────────────────────────────────────────
# Main
# ──────────────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Sweep sistemático de parámetros para backtest.py")
    parser.add_argument("sweep_json", help="Archivo de configuración del sweep (ej: sweep_example.json)")
    parser.add_argument("--top", type=int, default=10, help="Cuántos top configs mostrar (default 10)")
    parser.add_argument("--no-cleanup", action="store_true",
                        help="No borrar configs temporales en .sweep_results/ al terminar")
    args = parser.parse_args()

    sweep_cfg = json.loads(Path(args.sweep_json).read_text(encoding="utf-8"))

    base_path   = Path(sweep_cfg["base"])
    weeks       = sweep_cfg.get("weeks", 1)
    max_pairs   = sweep_cfg.get("max_pairs", 200)
    windows     = sweep_cfg["windows"]
    params      = sweep_cfg["params"]
    weights     = sweep_cfg.get("scoring_weights", {})
    out_dir     = Path(sweep_cfg.get("out_dir", ".sweep_results"))
    scan_interval = sweep_cfg.get("scan_interval_min", 15)

    base_cfg = json.loads(base_path.read_text(encoding="utf-8"))

    # Advertencia si la grilla es grande
    n_combos = 1
    for vals in params.values():
        n_combos *= len(vals)
    n_runs = n_combos * len(windows)
    if n_combos > 50:
        print(f"AVISO: {n_combos} combos × {len(windows)} ventanas = {n_runs} corridas.")
        print("Esto puede tomar mucho tiempo. Continuando en 5s (Ctrl-C para cancelar)...")
        import time; time.sleep(5)

    out_dir.mkdir(exist_ok=True)

    print(f"\n=== SWEEP: {n_combos} combos × {len(windows)} ventanas ===")
    print(f"Params: {list(params.keys())}")
    print(f"Output: {out_dir}/\n")

    # Generar configs
    combo_paths = generate_configs(base_cfg, params, out_dir)
    variant_paths = [p for _, p in combo_paths]
    print(f"Generados {len(combo_paths)} configs en {out_dir}/\n")

    # Correr sweep por ventana
    period_days = weeks * 7
    # scores_by_combo[i] = lista de scores por ventana
    scores_by_combo  = [[] for _ in range(len(combo_paths))]
    stats_by_combo   = [[] for _ in range(len(combo_paths))]
    base_key = base_path.stem

    for window in windows:
        out_json = out_dir / f"run_{window}.json"
        ok = run_backtest_variants(base_path, variant_paths, weeks, max_pairs,
                                   window, out_json, scan_interval)
        if not ok or not out_json.exists():
            print(f"  ERROR en ventana {window}, saltando.")
            continue

        run_data = json.loads(out_json.read_text(encoding="utf-8"))

        for i, (combo, path) in enumerate(combo_paths):
            key = path.stem  # "cfg_0000", etc.
            alerts = run_data.get(key)
            if alerts is None:
                continue
            s = compute_stats(alerts, period_days)
            if s is None:
                continue
            sc = score_stats(s, weights)
            scores_by_combo[i].append(sc)
            stats_by_combo[i].append(s)

    # Calcular score final por combo
    results = []
    for i, (combo, path) in enumerate(combo_paths):
        sc_list = scores_by_combo[i]
        st_list = stats_by_combo[i]
        if not sc_list:
            continue
        avg_score = mean(sc_list)
        std_score = stdev(sc_list) if len(sc_list) > 1 else 0.0
        # Promediar stats entre ventanas
        avg_stats = {k: mean(s[k] for s in st_list) for k in st_list[0]}
        results.append({
            "rank": 0,
            "score": avg_score,
            "std": std_score,
            "stats": avg_stats,
            "combo": combo,
            "path": str(path),
            "n_windows": len(sc_list),
        })

    results.sort(key=lambda r: r["score"], reverse=True)
    for i, r in enumerate(results):
        r["rank"] = i + 1

    # Emitir ranking
    top_n = results[:args.top]
    param_keys = list(params.keys())

    lines = []
    lines.append(f"\n=== TOP {len(top_n)} CONFIGS (de {len(results)} evaluados, {len(windows)} ventanas) ===\n")

    # Cabecera tabla
    header_params = "  ".join(f"{k.split('.')[-1]}" for k in param_keys)
    lines.append(f"| rank | score | std  | catch | win10% | best/d | rr   | {header_params} |")
    sep_params = "  ".join("-" * max(len(k.split(".")[-1]), 5) for k in param_keys)
    lines.append(f"|------|-------|------|-------|--------|--------|------|{sep_params}-|")

    for r in top_n:
        s  = r["stats"]
        c  = r["combo"]
        catch_str   = f"{s['top30_in_best_strong']:.0f}/30"
        win10_str   = f"{s['best_win_10pct']:.0f}%"
        bestd_str   = f"{s['best_per_day']:.1f}"
        rr_str      = f"{s['best_rr']:.2f}"
        params_str  = "  ".join(str(c.get(k, "?")) for k in param_keys)
        lines.append(f"| {r['rank']:4d} | {r['score']:.3f} | {r['std']:.3f} | {catch_str:5s} | {win10_str:6s} | {bestd_str:6s} | {rr_str:4s} | {params_str} |")

    lines.append("")
    lines.append("Top configs (paths para inspección o promoción):")
    for r in top_n[:5]:
        lines.append(f"  #{r['rank']:2d} score={r['score']:.3f}  {r['path']}")

    output = "\n".join(lines)
    print(output)

    ranking_path = out_dir / "ranking.md"
    ranking_path.write_text(output, encoding="utf-8")
    print(f"\nRanking guardado en: {ranking_path}")

    # Cleanup opcional
    if not args.no_cleanup:
        for _, path in combo_paths:
            try:
                path.unlink()
            except OSError:
                pass
        print(f"Configs temporales eliminados (--no-cleanup para conservarlos).")

    print("\nDONE")


if __name__ == "__main__":
    main()
