# CLAUDE.md

Crypto screener for Binance USDT spot pairs. Runs on GitHub Actions cron, alerts to Telegram, persists outcomes to Supabase. Code/comments mostly in Spanish.

## Architecture
- `screener.py` (~1.5k LoC) — main scan. Loads `config.json`, fetches klines (5m/15m/1h), `analyze()` per symbol/TF → `classify_symbol()` picks one signal → `dedupe_and_rank()` → Telegram + Supabase. Anti-spam via cooldowns + history table.
- `backtest.py` (~1.3k LoC) — replays Binance history through the same `classify()` logic. `Config` class with `g()` falls back to v3 hardcoded defaults if a key is missing. Supports `--compare a.json b.json`, `--ablation`, `--variants`, `--weeks`, `--max-pairs`, `--scan-interval-min` (default 15).
- `update_outcomes.py` — separate job: fills 15m/1h/4h/24h price + max/min checkpoints for alerts in Supabase. Auto-purges rows older than `outcomes.RETENTION_DAYS`.
- `dump_outcomes.py` + `outcomes.html` / `outcomes_dump.json` — outcome viewer.
- `index.html` — main display.
- Workflows: `.github/workflows/screener.yml` (cron `*/5`), `outcomes.yml` (cron `*/15`).

## Signal types (one per symbol per run)
- **PREBREAK**: 5m, near recent max, volume rising, BB compressed.
- **BREAKOUT**: 15m breaks recent high with volume + BB expansion + body strength.
- **RIDING**: prior breakout still extending and holding zone (repeats).
- **FADING**: post-breakout giveback (off by default — `active_signals.FADING=false`).
- **HOLD**: 15m broke and is sustaining the zone above resistance.

## Scoring philosophy
- Every magic number lives in `config.json` under `scoring`, `scoring_prebreak`, `scoring_breakout`, `scoring_riding`, `scoring_hold`, `scoring_fading`. Edits should target config, not Python.
- Per-signal score = base + tiered bonuses (OBV/CVD/momentum/distance) − penalties (climax, late entry, structural resistance, repeat).
- BREAKOUT and RIDING optionally read OI delta + funding rate from Binance Futures when `derivatives.ENABLED=true` (off by default). Live fetch in `screener.py` (`fetch_derivatives`), historical bulk-fetch in `backtest.py` (`download_all_derivatives`).
- Capped by `scoring.SCORE_CAP` (15). Buckets: `BEST_MIN_SCORE` (default 13) > `STRONG_MIN_SCORE` (11). `IMMEDIATE_MIN_SCORE` triggers same-run Telegram (limited by `anti_spam.MAX_IMMEDIATE_PER_RUN`).
- `FORMING_CANDLE_PENALTY` discounts in-progress candles.

## Important configs
- `config.json` — production baseline. Sections: `general`, `history`, `anti_spam`, `cooldowns_minutes`, `active_signals`, `indicators`, signal params, `scoring*`, `chart`, `outcomes`.
- `config_F/G/H/J/K/M.json` — A/B variants for backtesting; do not deploy directly.
- Cooldowns (minutes): PREBREAK 30, BREAKOUT 20, RIDING 15, FADING 120, HOLD 45.

## Backtest goals
- Tune scoring/threshold without code changes. Compare a candidate vs `config.json` baseline:
  ```
  python backtest.py --weeks 1 --max-pairs 200 --compare config.json config_X.json --out compare_X.json
  ```
- Watch CATCH RATE (recall on real movers) and bucket QUALITY (precision in BEST/STRONG).
- `run_FG.bat`, `run_compares.bat` — batch runners (Windows). Variants run in separate Python invocations to dodge a pandas issue under Python 3.14.

## Key workflows
- **Local scan/test**: needs env `TELEGRAM_TOKEN`, `TELEGRAM_CHAT_ID`, `SUPABASE_KEY`. Supabase URL hardcoded at `screener.py:66`.
- **Add/tweak a signal**: edit `analyze()` (~line 516) for detection, `classify_symbol()` (~line 769) for selection, `scoring_<name>` config for weights. Mirror any non-config logic into `backtest.py`'s `classify()` (~line 465) — they must stay in sync.
- **New scoring knob**: add to `config.json` + read via `_cfg_score()` in `screener.py` and `cfg.g()` in `backtest.py` (with hardcoded default for backwards compat).
- **Tracking outcomes**: `outcomes.ENABLED=true` writes alert rows to Supabase `screener_outcomes`; tracker fills price snapshots later.

## Stack
Python 3.11, `pandas==2.2.3`, `ta`, `requests`, `mplfinance` (optional, for chart PNGs in Telegram).

## Conventions
- No new files unless asked. Prefer editing.
- Comments in Spanish — match style if adding any.
- Don't touch the hardcoded Supabase URL or Telegram secrets.
- `screener.py` and `backtest.py` share scoring semantics — changes to one usually need changes in the other.
