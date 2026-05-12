"""
sweep.py — Sweep sistemático de parámetros para backtest.py.

Uso:
    python sweep.py sweep_example.json
    python sweep.py sweep_example.json --top 15 --no-cleanup

Lee un sweep.json con la grilla de parámetros, genera configs temporales,
llama a backtest.py --variants por cada ventana temporal (en paralelo),
parsea los resultados, calcula una métrica objetivo única y emite un ranking
con robustez multi-ventana (columna std).

Clave de eficiencia: --variants descarga klines UNA vez y corre N configs encima.
Las ventanas se corren en paralelo (max_parallel_windows en sweep.json).
"""

import argparse
import concurrent.futures
import copy
import json
import os
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

    best  = [a for a in alerts if a.get("bucket") == "BEST"]
    strong = [a for a in alerts if a.get("bucket") == "STRONG"]

    best_moves = [pct_move(a.get("max_high_24h"), a.get("entry_price")) for a in best]
    best_moves = [m for m in best_moves if m is not None]
    best_drops = [pct_move(a.get("min_low_24h"), a.get("entry_price")) for a in best]
    best_drops = [d for d in best_drops if d is not None]

    ranked = sorted(alerts,
                    key=lambda a: pct_move(a.get("max_high_24h"), a.get("entry_price")) or -999,
                    reverse=True)
    top30_best_strong = sum(1 for a in ranked[:30] if a.get("bucket") in ("BEST", "STRONG"))

    best_win_10  = (sum(1 for m in best_moves if m >= 10) * 100 / len(best_moves)
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
    # catch: denominador = min(30, total alertas) — evita penalizar configs estrictos
    # que generan pocos alerts pero muy precisos
    n_denom  = max(1, min(30, stats["n"]))
    catch    = stats["top30_in_best_strong"] / n_denom
    # quality/rr: escala suave por tamaño de muestra — 1 BEST lucky no puede dar quality=1.0
    sample   = min(1.0, stats["best_n"] / 10)
    quality  = (stats["best_win_10pct"] / 100) * sample
    volume   = min(1.0, stats["best_per_day"] / 15)
    rr       = min(1.0, stats["best_rr"] / 4) * min(1.0, stats["best_n"] / 5)
    overflow_thresh = max(1, w.get("volume_overflow_threshold", 25))
    overflow = max(0.0, stats["best_per_day"] - overflow_thresh) / overflow_thresh

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
    section, _, key = dotted_key.partition(".")
    if key:
        d.setdefault(section, {})[key] = value
    else:
        d[section] = value


def _is_valid_combo(cfg):
    """Descarta combos donde STRONG_MIN_SCORE >= BEST_MIN_SCORE (STRONG bucket vacío)."""
    scoring = cfg.get("scoring", {})
    strong = scoring.get("STRONG_MIN_SCORE")
    best   = scoring.get("BEST_MIN_SCORE")
    if strong is not None and best is not None and strong >= best:
        return False
    return True


def generate_configs(base_cfg, params, out_dir, shard_i=None, shard_n=1):
    """Genera configs en out_dir/. Con shard_i/shard_n (0-indexed) solo escribe el shard indicado.
    La numeración cfg_NNNN es global para que múltiples shards no colisionen.
    Devuelve lista de (combo_dict, path) para el shard (o todos si shard_i is None)."""
    keys    = list(params.keys())
    values  = list(params.values())
    paths   = []
    skipped = 0
    i = 0  # índice global de combos válidos
    for vals in product(*values):
        combo = dict(zip(keys, vals))
        cfg   = copy.deepcopy(base_cfg)
        for k, v in combo.items():
            set_nested(cfg, k, v)
        if not _is_valid_combo(cfg):
            skipped += 1
            continue
        if shard_i is None or i % shard_n == shard_i:
            path = out_dir / f"cfg_{i:04d}.json"
            path.write_text(json.dumps(cfg, indent=2), encoding="utf-8")
            paths.append((combo, path))
        i += 1
    if skipped:
        print(f"  [sweep] {skipped} combos inválidos descartados (STRONG_MIN >= BEST_MIN).")
    return paths


# ──────────────────────────────────────────────────────────────────────────────
# Llamada a backtest.py (una por ventana, en hilo separado)
# ──────────────────────────────────────────────────────────────────────────────

def run_window(base_path, variant_paths, weeks, max_pairs, scan_interval,
               end_date, out_json, log_path, variant_workers=1, results_dir=None):
    """
    Corre backtest.py --variants para una ventana. Output va a log_path.
    Si variant_workers > 1, divide las variantes en sub-grupos y corre en paralelo.
    Devuelve (end_date, ok, out_json).
    """
    env = {**os.environ, "PYTHONUTF8": "1", "PYTHONUNBUFFERED": "1"}

    def _make_cmd(vp_list, sub_out):
        cmd = [
            sys.executable, "-u", "backtest.py",
            "--weeks", str(weeks),
            "--max-pairs", str(max_pairs),
            "--scan-interval-min", str(scan_interval),
            "--end-date", end_date,
            "--out", str(sub_out),
            "--variants", str(base_path),
            *[str(p) for p in vp_list],
        ]
        if results_dir:
            cmd += ["--results-dir", str(results_dir)]
        return cmd

    if variant_workers <= 1 or len(variant_paths) <= 1:
        print(f"  [{end_date}] Iniciando... (log: {log_path.name})")
        with open(log_path, "w", encoding="utf-8") as log_f:
            result = subprocess.run(_make_cmd(variant_paths, out_json),
                                    stdout=log_f, stderr=log_f, text=True, env=env)
        ok = result.returncode == 0
        status = "OK" if ok else f"ERROR (ver {log_path.name})"
        print(f"  [{end_date}] {status}")
        return end_date, ok, out_json

    # Dividir variantes en chunks y correr en paralelo
    chunk_size = max(1, (len(variant_paths) + variant_workers - 1) // variant_workers)
    chunks = [variant_paths[i:i + chunk_size] for i in range(0, len(variant_paths), chunk_size)]
    sub_outs = []
    print(f"  [{end_date}] Iniciando {len(chunks)} workers ({len(variant_paths)} variantes)...")

    def _run_chunk(chunk, sub_out, sub_log):
        with open(sub_log, "w", encoding="utf-8") as lf:
            r = subprocess.run(_make_cmd(chunk, sub_out),
                               stdout=lf, stderr=lf, text=True, env=env)
        return r.returncode == 0

    with concurrent.futures.ThreadPoolExecutor(max_workers=len(chunks)) as pool:
        futs = []
        for k, chunk in enumerate(chunks):
            sub_out = out_json.parent / f"{out_json.stem}__w{k}.json"
            sub_log = log_path.parent / f"{log_path.stem}__w{k}.log"
            sub_outs.append(sub_out)
            futs.append(pool.submit(_run_chunk, chunk, sub_out, sub_log))
        statuses = [f.result() for f in futs]

    ok = all(statuses)
    merged = {}
    for sub_out in sub_outs:
        if sub_out.exists():
            try:
                merged.update(json.loads(sub_out.read_text(encoding="utf-8")))
            except Exception as e:
                print(f"  AVISO: no se pudo leer {sub_out.name}: {e}")

    if merged:
        out_json.write_text(json.dumps(merged, indent=2), encoding="utf-8")
    else:
        ok = False

    status = "OK" if ok else f"ERROR parcial (ver logs {log_path.stem}__w*.log)"
    print(f"  [{end_date}] {status}")
    return end_date, ok, out_json


# ──────────────────────────────────────────────────────────────────────────────
# Main
# ──────────────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Sweep sistemático de parámetros para backtest.py")
    parser.add_argument("sweep_json", help="Archivo sweep (ej: sweep_example.json)")
    parser.add_argument("--top", type=int, default=10, help="Cuántos top configs mostrar (default 10)")
    parser.add_argument("--no-cleanup", action="store_true",
                        help="Conservar configs temporales en out_dir/")
    parser.add_argument("--shard", default=None, metavar="i/N",
                        help="Procesa solo el shard i de N (1-indexed, ej: --shard 1/4). "
                             "Múltiples shards pueden correr en paralelo apuntando al mismo out_dir.")
    parser.add_argument("--variant-workers", type=int, default=1, metavar="N",
                        help="Sub-procesos paralelos por ventana (default 1). "
                             "Divide las variantes en N grupos; cada grupo corre backtest.py aparte.")
    parser.add_argument("--max-parallel-windows", type=int, default=None,
                        help="Override de max_parallel_windows del JSON.")
    args = parser.parse_args()

    sweep_cfg     = json.loads(Path(args.sweep_json).read_text(encoding="utf-8"))
    base_path     = Path(sweep_cfg["base"])
    weeks         = sweep_cfg.get("weeks", 1)
    max_pairs     = sweep_cfg.get("max_pairs", 200)
    windows       = sweep_cfg["windows"]
    params        = sweep_cfg["params"]
    weights       = sweep_cfg.get("scoring_weights", {})
    out_dir       = Path(sweep_cfg.get("out_dir", ".sweep_results"))
    scan_interval = sweep_cfg.get("scan_interval_min", 15)
    max_parallel  = sweep_cfg.get("max_parallel_windows", len(windows))
    if args.max_parallel_windows is not None:
        max_parallel = args.max_parallel_windows

    shard_i = None
    shard_n = 1
    if args.shard:
        try:
            si, sn = args.shard.split("/")
            shard_i = int(si) - 1  # convertir a 0-indexed
            shard_n = int(sn)
            if not (0 <= shard_i < shard_n):
                raise ValueError
        except (ValueError, AttributeError):
            print(f"ERROR: --shard debe ser i/N con 1<=i<=N (ej: 1/4). Got: {args.shard!r}")
            sys.exit(1)

    variant_workers = args.variant_workers

    base_cfg = json.loads(base_path.read_text(encoding="utf-8"))

    n_combos = 1
    for vals in params.values():
        n_combos *= len(vals)

    if n_combos > 50 and shard_i is None:
        print(f"AVISO: {n_combos} combos × {len(windows)} ventanas = {n_combos * len(windows)} corridas.")
        print("Continuando en 5s (Ctrl-C para cancelar)...")
        import time; time.sleep(5)

    out_dir.mkdir(exist_ok=True)
    results_dir = out_dir / "results"
    results_dir.mkdir(exist_ok=True)

    shard_label = f" [shard {shard_i+1}/{shard_n}]" if shard_i is not None else ""
    print(f"\n=== SWEEP{shard_label}: {n_combos} combos × {len(windows)} ventanas "
          f"(paralelo: {max_parallel}, workers/ventana: {variant_workers}) ===")
    print(f"Params: {list(params.keys())}")
    print(f"Output: {out_dir}/\n")

    combo_paths   = generate_configs(base_cfg, params, out_dir, shard_i=shard_i, shard_n=shard_n)
    variant_paths = [p for _, p in combo_paths]
    n_shard = len(combo_paths)
    if shard_i is not None:
        print(f"  Shard {shard_i+1}/{shard_n}: {n_shard} configs de ~{n_combos} totales.")
    else:
        print(f"Generados {n_shard} configs en {out_dir}/")
    print(f"Lanzando {len(windows)} ventanas en paralelo (max {max_parallel})...\n")

    period_days = weeks * 7

    shard_suffix = f"__shard{shard_i+1}of{shard_n}" if shard_i is not None else ""

    # Correr ventanas en paralelo
    window_results = {}  # end_date -> out_json path
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_parallel) as ex:
        futures = {}
        for window in windows:
            out_json = out_dir / f"run_{window}{shard_suffix}.json"
            log_path = out_dir / f"window_{window}{shard_suffix}.log"
            f = ex.submit(run_window, base_path, variant_paths, weeks, max_pairs,
                          scan_interval, window, out_json, log_path,
                          variant_workers, str(results_dir))
            futures[f] = window
        for f in concurrent.futures.as_completed(futures):
            end_date, ok, out_json = f.result()
            if ok and out_json.exists():
                window_results[end_date] = out_json
            else:
                print(f"  AVISO: ventana {end_date} falló, excluida del ranking.")

    if not window_results:
        print("ERROR: ninguna ventana completó exitosamente.")
        sys.exit(1)

    print(f"\nVentanas completadas: {len(window_results)}/{len(windows)}")
    print("Calculando scores...\n")

    # Acumular stats por combo
    scores_by_combo = [[] for _ in range(n_combos)]
    stats_by_combo  = [[] for _ in range(n_combos)]

    for out_json in window_results.values():
        run_data = json.loads(out_json.read_text(encoding="utf-8"))
        for i, (_, path) in enumerate(combo_paths):
            alerts = run_data.get(path.stem)
            if not alerts:
                continue
            s = compute_stats(alerts, period_days)
            if s is None:
                continue
            scores_by_combo[i].append(score_stats(s, weights))
            stats_by_combo[i].append(s)

    # Calcular score final y armar resultados
    results = []
    for i, (combo, path) in enumerate(combo_paths):
        sc_list = scores_by_combo[i]
        st_list = stats_by_combo[i]
        if not sc_list:
            continue
        avg_stats = {k: mean(s[k] for s in st_list) for k in st_list[0]}
        results.append({
            "rank":      0,
            "score":     mean(sc_list),
            "std":       stdev(sc_list) if len(sc_list) > 1 else 0.0,
            "stats":     avg_stats,
            "combo":     combo,
            "path":      str(path),
            "n_windows": len(sc_list),
        })

    results.sort(key=lambda r: r["score"], reverse=True)
    for i, r in enumerate(results):
        r["rank"] = i + 1

    # Emitir tabla
    top_n      = results[:args.top]
    param_keys = list(params.keys())
    short_keys = [k.split(".")[-1] for k in param_keys]

    lines = [f"\n=== TOP {len(top_n)} de {len(results)} configs ({len(window_results)} ventanas) ===\n"]
    header = "| rank | score | std  | catch | win10% | best/d | rr   | " + "  ".join(short_keys) + " |"
    sep    = "|------|-------|------|-------|--------|--------|------|-" + "--".join("-"*len(k) for k in short_keys) + "-|"
    lines += [header, sep]

    for r in top_n:
        s = r["stats"]
        c = r["combo"]
        row = (f"| {r['rank']:4d} | {r['score']:.3f} | {r['std']:.3f} |"
               f" {s['top30_in_best_strong']:.0f}/30 |"
               f" {s['best_win_10pct']:.0f}%    |"
               f" {s['best_per_day']:.1f}    |"
               f" {s['best_rr']:.2f} |"
               f" {'  '.join(str(c.get(k,'?')) for k in param_keys)} |")
        lines.append(row)

    lines += ["", "Top-5 configs (paths para inspección o promoción):"]
    for r in top_n[:5]:
        lines.append(f"  #{r['rank']:2d} score={r['score']:.3f}  {r['path']}")

    output = "\n".join(lines)
    print(output)

    ranking_path = out_dir / "ranking.md"
    ranking_path.write_text(output, encoding="utf-8")
    print(f"\nRanking guardado en: {ranking_path}")
    print(f"Logs por ventana: {out_dir}/window_<fecha>.log")

    if not args.no_cleanup:
        for _, path in combo_paths:
            try:
                path.unlink()
            except OSError:
                pass
        print("Configs temporales eliminados (--no-cleanup para conservarlos).")

    print("\nDONE")


if __name__ == "__main__":
    main()
