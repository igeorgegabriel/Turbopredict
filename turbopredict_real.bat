@echo off
title TURBOPREDICT X PROTEAN - Real Data Cyberpunk Interface

REM Set UTF-8 encoding for better character support
chcp 65001 >nul 2>&1

cd /d "%~dp0"

echo.
echo ====================================================
echo   TURBOPREDICT X PROTEAN - REAL DATA EDITION
echo   Cyberpunk Neural Interface with Parquet Data
echo ====================================================
echo.
echo Connecting to Parquet Matrix...
echo Data Directory: %~dp0data\processed
echo.
echo Using Parquet-only mode (DuckDB disabled for now)
set "DISABLE_DUCKDB=1"

python real_cyberpunk_cli.py

if errorlevel 1 (
    echo.
    echo ====================================================
    echo   ERROR: Neural interface failed to initialize
    echo   Please check:
    echo   - Python installation
    echo   - Required packages: pip install -r requirements.txt
    echo   - Data directory access
    echo ====================================================
    pause
) else (
    echo.
    echo ====================================================
    echo   Real Data Parquet matrix link terminated
    echo   Thank you for using TURBOPREDICT X PROTEAN
    echo ====================================================
)

pause
