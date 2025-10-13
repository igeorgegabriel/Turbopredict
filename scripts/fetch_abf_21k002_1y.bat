@echo off
setlocal ENABLEEXTENSIONS

REM Ensure CWD is the repo root (parent of this script directory)
set "_SCRIPT_DIR=%~dp0"
REM %~dp0 includes trailing backslash; go up one level to repo root
pushd "%_SCRIPT_DIR%.." >nul 2>&1

REM Fetch 1 year of 6-minute data for ABF 21-K002 via Excel PI DataLink
REM This wrapper avoids smart-dash/quote issues in CMD.

set "XLSX=excel\ABFSB\ABF LIMIT REVIEW (CURRENT).xlsx"
set "TAGS=config\tags_abf_21k002.txt"
set "OUT=data\processed\21-K002_1y_0p1h.parquet"
set "PLANT=ABF"
set "UNIT=21-K002"

echo Running batch-unit fetch...
echo   XLSX : %XLSX%
echo   TAGS : %TAGS%
echo   OUT  : %OUT%

REM Use workbook default PI server by passing an empty server (Excel will resolve)
python -m pi_monitor.cli batch-unit --xlsx "%XLSX%" --tags "%TAGS%" --out "%OUT%" --plant %PLANT% --unit %UNIT% --server "" --start="-1y" --end="*" --step="-0.1h" --visible

echo.
echo Exit code: %ERRORLEVEL%
echo.
dir "data\processed\21-K002_1y_0p1h*.parquet" 2>nul
echo.
pause

popd >nul 2>&1
