@echo off
title TURBOPREDICT X PROTEAN - Continuous Monitoring Loop

REM Set UTF-8 encoding for optimal display
chcp 65001 >nul 2>&1

cd /d "%~dp0"

echo.
echo ========================================================================
echo   TURBOPREDICT X PROTEAN - CONTINUOUS MONITORING LOOP
echo   Auto-refresh every 1 hour with PI DataLink integration
echo ========================================================================
echo.
echo Starting continuous monitoring mode...
echo.
echo Using Parquet-only mode (DuckDB disabled for now)
set "DISABLE_DUCKDB=1"
echo To stop monitoring: Press Ctrl+C
echo To change interval: python turbopredict.py --loop [hours]
echo.

python turbopredict.py --loop 1

if errorlevel 1 (
    echo.
    echo ========================================================================
    echo   Monitoring loop stopped or encountered an error
    echo.
    echo   To restart: Double-click turbopredict_loop.bat
    echo   To run different intervals:
    echo   - Every 30 minutes: python turbopredict.py --loop 0.5
    echo   - Every 2 hours: python turbopredict.py --loop 2
    echo ========================================================================
    pause
)

echo.
echo ========================================================================
echo   TURBOPREDICT X PROTEAN monitoring loop terminated successfully
echo ========================================================================
pause
