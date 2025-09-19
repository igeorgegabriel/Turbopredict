@echo off
echo ================================================================
echo  TURBOPREDICT X PROTEAN - HOURLY REFRESH SERVICE
echo  Maintains 1-hour data freshness standard
echo ================================================================
echo.

cd /d "%~dp0"

echo Starting Hourly Refresh Service...
echo Press Ctrl+C to stop
echo.
echo Using Parquet-only mode (DuckDB disabled for now)
set "DISABLE_DUCKDB=1"

python scripts\hourly_refresh.py

echo.
echo Service stopped.
pause
