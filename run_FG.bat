@echo off
REM Corre BASE vs F y BASE vs G en dos llamadas separadas.
REM Llamadas separadas evitan el error de pandas en Python 3.14
REM (que pasa cuando --variants procesa muchas configs de una).
REM
REM Uso:
REM   set SUPABASE_KEY=tu_key_aqui   (opcional)
REM   run_FG.bat

if not defined WEEKS set WEEKS=1
if not defined MAX_PAIRS set MAX_PAIRS=200

echo ====================================================================
echo  BACKTEST: BASE vs F  (threshold + bases moderadas)
echo ====================================================================
echo  Cambios en F:
echo    scoring_hold.BASE_SCORE:    5 -^> 6  (techo HOLD: 12 -^> 13)
echo    scoring_riding.BASE_SCORE:  4 -^> 5  (techo RIDING: 13 -^> 14)
echo    BEST_MIN_SCORE:             14 -^> 13
echo    STRONG_MIN_SCORE:           13 -^> 11
echo ====================================================================
echo.

python backtest.py ^
    --weeks %WEEKS% ^
    --max-pairs %MAX_PAIRS% ^
    --compare config.json config_F.json

echo.
echo ====================================================================
echo  BACKTEST: BASE vs G  (threshold + bonuses por momentum extremo)
echo ====================================================================
echo  Cambios en G:
echo    BEST_MIN_SCORE:             14 -^> 13
echo    STRONG_MIN_SCORE:           13 -^> 11
echo    scoring_hold:               +2 si OBV^>0.2 + CVD bullish + dist^>5%%
echo    scoring_riding:             +2 si gain ^>= 8%%
echo ====================================================================
echo.

python backtest.py ^
    --weeks %WEEKS% ^
    --max-pairs %MAX_PAIRS% ^
    --compare config.json config_G.json

echo.
echo ====================================================================
echo  Las 2 comparativas estan completas arriba.
echo  Mira las metricas CATCH RATE y CALIDAD del bucket BEST.
echo ====================================================================
pause
