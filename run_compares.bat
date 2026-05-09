@echo off
echo ============================================
echo  BACKTEST H
echo ============================================
call python backtest.py --weeks 1 --max-pairs 200 --compare config.json config_H.json --out compare_H.json

echo ============================================
echo  BACKTEST J
echo ============================================
call python backtest.py --weeks 1 --max-pairs 200 --compare config.json config_J.json --out compare_J.json

echo ============================================
echo  BACKTEST M
echo ============================================
call python backtest.py --weeks 1 --max-pairs 200 --compare config.json config_M.json --out compare_M.json

echo ============================================
echo  BACKTEST K
echo ============================================
call python backtest.py --weeks 1 --max-pairs 200 --compare config.json config_K.json --out compare_K.json

echo ============================================
echo  TODOS LOS BACKTESTS COMPLETADOS
echo ============================================
pause