"""
optuna_tune.py — Búsqueda bayesiana alrededor del mejor config del grid sweep.

Uso:
  python optuna_tune.py --best-config .sweep_results3/cfg_0042.json \
                        --windows 2026-05-09,2026-04-25,2026-04-11,2025-06-15,2024-08-05,2024-03-10 \
                        --n-trials 80 --max-pairs 200 --out-dir .optuna_results
"""

import argparse
import json
import os
import sys
from pathlib import Path
from datetime import datetime

import optuna
from optuna.samplers import TPESampler

# Funciones importadas de tu sweep.py (ajustá las rutas si es necesario)
from sweep import run_window, compute_stats, score_stats, generate_configs


def objective(trial, base_cfg, windows, weeks, max_pairs, scan_interval, weights, results_dir):
    """Objetivo Optuna: devuelve el score compuesto promediado sobre todas las ventanas."""
    cfg = json.loads(json.dumps(base_cfg))  # copia profunda

    # Definir rangos alrededor del mejor config (parseá desde el base_cfg)
    # Aquí asumimos que el JSON del mejor config tiene exactamente los valores base.
    # Rangos: ±30% del valor base, con límites de sentido común.
    base_obv = cfg["indicators"]["OBV_RISING_MIN"]
    cfg["indicators"]["OBV_RISING_MIN"] = trial.suggest_float(
        "OBV_RISING_MIN", base_obv * 0.7, base_obv * 1.8, step=0.005
    )

    base_cvd = cfg["indicators"]["CVD_BULLISH_MIN"]
    cfg["indicators"]["CVD_BULLISH_MIN"] = trial.suggest_float(
        "CVD_BULLISH_MIN", base_cvd * 0.7, base_cvd * 1.8, step=0.005
    )

    base_atr = cfg["indicators"]["ATR_MIN_PCT"]
    cfg["indicators"]["ATR_MIN_PCT"] = trial.suggest_float(
        "ATR_MIN_PCT", max(0.5, base_atr - 0.5), base_atr + 0.8, step=0.1
    )

    base_early = cfg["scoring_breakout"]["EARLY_ENTRY_BONUS"]
    cfg["scoring_breakout"]["EARLY_ENTRY_BONUS"] = trial.suggest_int(
        "EARLY_ENTRY_BONUS", int(base_early * 0.5), int(base_early * 1.8)
    )

    base_late_pen = cfg["scoring_breakout"]["LATE_ENTRY_PENALTY"]
    cfg["scoring_breakout"]["LATE_ENTRY_PENALTY"] = trial.suggest_int(
        "LATE_ENTRY_PENALTY", int(base_late_pen * 1.8), int(base_late_pen * 0.5)
    )

    base_struct = cfg["scoring_hold"]["STRUCT_PENALTY"]
    cfg["scoring_hold"]["STRUCT_PENALTY"] = trial.suggest_int(
        "STRUCT_PENALTY", max(0, base_struct - 2), base_struct + 1
    )

    # BEST y STRONG los movemos discretamente alrededor del base
    base_best = cfg["scoring"]["BEST_MIN_SCORE"]
    cfg["scoring"]["BEST_MIN_SCORE"] = trial.suggest_int(
        "BEST_MIN_SCORE", base_best - 1, base_best + 1
    )
    base_strong = cfg["scoring"]["STRONG_MIN_SCORE"]
    cfg["scoring"]["STRONG_MIN_SCORE"] = trial.suggest_int(
        "STRONG_MIN_SCORE", base_strong - 1, base_strong + 1
    )

    # Guardamos el config en tmp y usamos run_window con variante única
    tmp_cfg_path = Path(results_dir) / f"optuna_{trial.number}.json"
    tmp_cfg_path.parent.mkdir(parents=True, exist_ok=True)
    tmp_cfg_path.write_text(json.dumps(cfg, indent=2), encoding="utf-8")

    # Para cada ventana corremos backtest.py y calculamos stats
    scores = []
    for window in windows:
        out_json = Path(results_dir) / f"out_{trial.number}_{window}.json"
        log_path = Path(results_dir) / f"log_{trial.number}_{window}.log"
        # Variant único es este config temporal
        variant_paths = [tmp_cfg_path]
        base_path = tmp_cfg_path  # En --variants el primero se considera base, pero
                                  # como solo hay uno no importa.
        # Llamar a la función run_window pero con base_path apuntando al config base real.
        # Mejor usar el mismo config como base y variante.
        from sweep import run_window  # ya importado

        _, ok, out_json = run_window(
            base_path=tmp_cfg_path,   # ambos iguales, ignora la comparación
            variant_paths=variant_paths,
            weeks=weeks,
            max_pairs=max_pairs,
            scan_interval=scan_interval,
            end_date=window,
            out_json=out_json,
            log_path=log_path,
            variant_workers=1,
            results_dir=str(Path(results_dir) / "results")
        )
        if not ok or not out_json.exists():
            # Si falla, asignamos score 0 para que Optuna evite esta región
            scores.append(0.0)
            continue

        # Extraer alertas del único config
        data = json.loads(out_json.read_text(encoding="utf-8"))
        alerts = list(data.values())[0] if data else []
        stats = compute_stats(alerts, weeks * 7)
        if stats is None:
            scores.append(0.0)
        else:
            scores.append(score_stats(stats, weights))

    mean_score = sum(scores) / len(scores) if scores else 0.0
    return mean_score


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--best-config", required=True, help="Path al mejor config del grid")
    parser.add_argument("--windows", required=True, help="Ventanas separadas por coma")
    parser.add_argument("--n-trials", type=int, default=80)
    parser.add_argument("--max-pairs", type=int, default=200)
    parser.add_argument("--out-dir", default=".optuna_results")
    parser.add_argument("--scan-interval-min", type=int, default=15)
    parser.add_argument("--weeks", type=int, default=4)
    args = parser.parse_args()

    windows = [w.strip() for w in args.windows.split(",")]
    base_cfg = json.loads(Path(args.best_config).read_text(encoding="utf-8"))
    weights = {
        "catch": 0.25,
        "quality": 0.45,
        "volume": 0.15,
        "rr": 0.15,
        "volume_overflow_penalty": 0.20,
        "volume_overflow_threshold": 12,
        "inversion_penalty": 0.50
    }

    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    study = optuna.create_study(
        direction="maximize",
        sampler=TPESampler(seed=42),
    )

    def obj(trial):
        return objective(trial, base_cfg, windows, args.weeks,
                         args.max_pairs, args.scan_interval_min, weights, out_dir)

    study.optimize(obj,  n_trials=20, n_jobs=4, show_progress_bar=True)

    print(f"\nMejor trial (#{study.best_trial.number})")
    print(f"Score: {study.best_trial.value:.4f}")
    print("Parámetros:")
    for k, v in study.best_trial.params.items():
        print(f"  {k}: {v}")

    # Guardar el mejor config
    best_cfg = json.loads(json.dumps(base_cfg))
    best_cfg["indicators"]["OBV_RISING_MIN"] = study.best_trial.params["OBV_RISING_MIN"]
    best_cfg["indicators"]["CVD_BULLISH_MIN"] = study.best_trial.params["CVD_BULLISH_MIN"]
    best_cfg["indicators"]["ATR_MIN_PCT"] = study.best_trial.params["ATR_MIN_PCT"]
    best_cfg["scoring_breakout"]["EARLY_ENTRY_BONUS"] = study.best_trial.params["EARLY_ENTRY_BONUS"]
    best_cfg["scoring_breakout"]["LATE_ENTRY_PENALTY"] = study.best_trial.params["LATE_ENTRY_PENALTY"]
    best_cfg["scoring_hold"]["STRUCT_PENALTY"] = study.best_trial.params["STRUCT_PENALTY"]
    best_cfg["scoring"]["BEST_MIN_SCORE"] = study.best_trial.params["BEST_MIN_SCORE"]
    best_cfg["scoring"]["STRONG_MIN_SCORE"] = study.best_trial.params["STRONG_MIN_SCORE"]

    best_path = out_dir / "best_config.json"
    best_path.write_text(json.dumps(best_cfg, indent=2), encoding="utf-8")
    print(f"\nMejor config guardado en: {best_path}")


if __name__ == "__main__":
    main()