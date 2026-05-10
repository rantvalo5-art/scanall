@echo off
echo ============================================
echo  BACKTEST JM (hibrido J+M)
echo ============================================
call python backtest.py --weeks 1 --max-pairs 200 --compare config.json config_JM.json --out compare_JM.json

echo ============================================
echo  COMPLETADO
echo ============================================
pause
