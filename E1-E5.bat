@echo off
REM Bucle para E1 a E5 con logs y rename
for %%E in (E1 E2 E3 E4 E5) do (
  echo === Ejecutando %%E === %date% %time%
  python -X utf8 -u backtest.py --weeks 2 --max-pairs 300 --compare config.json config_%%E.json --out compare_%%E.json 2>&1 | powershell -NoProfile -Command "$input | Tee-Object -FilePath log_%%E.txt"
  python -X utf8 -u audit_misses.py --config config_%%E.json catch-rate --days 14 --max-pairs 400 2>&1 | powershell -NoProfile -Command "$input | Tee-Object -FilePath log_%%E.txt -Append"
  for /f "delims=" %%F in ('dir /b /o-d .audit_results\catch_rate_*.json ^| findstr /v "_E[1-5]"') do (
    if not exist ".audit_results\catch_rate_%%E.json" ren ".audit_results\%%F" "catch_rate_%%E.json"
  )
)
echo === Terminado ===
pause
