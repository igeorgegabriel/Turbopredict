@echo off
title TURBOPREDICT X PROTEAN - Unified Neural Matrix

REM Set UTF-8 encoding for optimal display
chcp 65001 >nul 2>&1

cd /d "%~dp0"

echo.
echo ========================================================================
echo   TURBOPREDICT X PROTEAN - UNIFIED NEURAL MATRIX
echo   Real Data Integration + Cyberpunk Interface + Auto-Scan System
echo ========================================================================
echo.
echo Initializing quantum neural processors...
echo Using Parquet-only mode (DuckDB disabled for now)
set "DISABLE_DUCKDB=1"
echo Data Directory: %~dp0data\processed
echo System: Unified entry point for all functionality
echo.

python turbopredict.py

if errorlevel 1 (
    echo.
    echo ========================================================================
    echo   ERROR: Neural matrix initialization failed
    echo   
    echo   Troubleshooting:
    echo   1. Ensure Python 3.10+ is installed
    echo   2. Install dependencies: pip install -r requirements.txt  
    echo   3. Check data directory permissions
    echo   4. Verify Parquet files are accessible
    echo ========================================================================
    pause
) else (
    echo.
    echo ========================================================================
    echo   TURBOPREDICT X PROTEAN neural matrix terminated successfully
    echo   All quantum processors offline
    echo   Thank you for using the unified system
    echo ========================================================================
)

pause
