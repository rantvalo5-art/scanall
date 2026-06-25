# CLAUDE.md — SWING (fork)

⚠️ **Esta carpeta `swing/` es el SWINGER, un fork propio.** El day trader vive en la
**raíz** del repo (`../screener.py`, `../backtest.py`, `../config.json`). Son DOS
proyectos en el mismo monorepo. **No mezclarlos.** Cuando trabajes acá, tocás solo
`swing/`; el day trader se toca solo en la raíz.

## Identidad (vs el day trader de la raíz)
- **Timeframes altos:** 1h/4h/1d (+1w opcional). Scan cada **60 min**.
- **Señales:** PREBREAK / BREAKOUT / HOLD / **COILING** (RIDING y FADING off). Hold ~7d.
- **SIN derivados** (no funding/OI; no hay sección `derivatives` en `swing/config.json`).
- El day trader, en cambio: 5m/15m/1h, RIDING/EXPLOSION, scan 15min, CON derivados.
- **Cómo distinguir de un vistazo:** si ves COILING y NO RIDING/EXPLOSION, es swing.

## Arquitectura
- `swing/screener.py` — scan en vivo (I/O: Binance, Telegram, Supabase, anti-spam).
  **NO duplica** detección/scoring: importa `analyze_at_time()` + `classify()` de
  `swing/backtest.py`. Así screener y backtest no se desincronizan (mismo código).
- `swing/backtest.py` — motor compartido por ambos. Correr desde `swing/`:
  `py -3.13 backtest.py ... --cache-dir .backtest_cache`. **NUNCA `../backtest.py`.**
- `swing/exit_tracker.py` — gestiona salidas a lo largo de ~7d (tabla `swing_exit_alerts`).
- `sim_*.py` / `diag_*.py` — batería de experimentos (correr con `py -3.13`).

## Supabase (POST-separación 2026-06-25)
El day trader se separó a tablas propias. **El reparto ahora es:**
- **Swing escribe/lee:** `screener_outcomes`, `screener_history`, `swing_exit_alerts`.
- **Day trader (raíz):** `daytrader_outcomes`, `daytrader_history` (NO tocar desde swing).
- **Compartida a propósito:** `screener_pairs_snapshot` (universo de Binance, referencia
  agnóstica al bot; conserva histórico para los backtests de ambos).
- ⚠️ El docstring de `swing/screener.py` (~línea 15) todavía dice "mismas tablas que el
  daytrader" — **quedó desactualizado** tras la separación. No confiar en esa línea.

## Workflows
- `.github/workflows/swing.yml` — scan manual (sin cron; `workflow_dispatch` / `run-swing`).
  El paso "Fill outcomes" corre **`swing/update_outcomes.py`** (filler PROPIO del swing →
  `screener_outcomes`, retención de `swing/config.json`). NO el del root (ese apunta a
  `daytrader_outcomes`). Es un fork de `../update_outcomes.py`: si arreglás un bug del filler,
  aplicalo a los dos.
- `.github/workflows/exit_tracker.yml` — cron **horario**; lee `screener_outcomes`.

## Reglas
- Editar **solo** `swing/` acá. Branches del swing: `swing/*`. (El day trader usa `day/*`.)
- Comentarios/código en español; mantener el estilo.
- El contexto acumulado del swinger vive en la memoria (`project-swing-*`). El day trader
  usa el prefijo `daytrader-*`. No cruzar aprendizajes de un proyecto al otro.
