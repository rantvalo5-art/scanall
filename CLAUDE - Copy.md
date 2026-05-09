# CLAUDE.md

Crypto screener for Binance USDT pairs. GitHub Actions cron → Telegram alerts → Supabase. Spanish comments.

## Files
- `screener.py` — scan: fetch klines (5m/15m/1h) → `analyze()` → `classify_symbol()` → dedupe → alert.
- `backtest.py` — replays history through the same `classify()`. CLI: `--weeks`, `--max-pairs`, `--compare a.json b.json`, `--ablation`.
- `update_outcomes.py` — fills 15m/1h/4h/24h price checkpoints in Supabase.
- `config.json` — baseline. `config_{F,G,H,J,K,M}.json` — A/B variants (backtest only).
- Workflows: `screener.yml` (`*/5`), `outcomes.yml` (`*/15`).

## Signals (one per symbol/run)
PREBREAK · BREAKOUT · RIDING · FADING (off) · HOLD. Cooldowns in `config.cooldowns_minutes`.

## Scoring
All weights in `config.json` under `scoring*` sections. Capped at `SCORE_CAP=15`. Buckets: BEST ≥ `BEST_MIN_SCORE`, STRONG ≥ `STRONG_MIN_SCORE`. Edit config, not Python.

## Rules
- `screener.py` and `backtest.py` share scoring — keep `classify()` logic in sync.
- New scoring knob: add to `config.json` + read via `_cfg_score()` (screener) and `cfg.g()` (backtest), each with hardcoded default for backwards compat.
- Env: `TELEGRAM_TOKEN`, `TELEGRAM_CHAT_ID`, `SUPABASE_KEY`. Supabase URL hardcoded.
- Stack: Python 3.11, `pandas==2.2.3`, `ta`, `requests`, `mplfinance` (optional).
