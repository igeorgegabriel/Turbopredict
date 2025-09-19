@echo off
title TURBOPREDICT X PROTEAN - Cyberpunk Neural Interface

echo ========================================
echo  TURBOPREDICT X PROTEAN
echo  Cyberpunk Neural Interface Loading...
echo ========================================
echo.

cd /d "%~dp0"
python launch_cyberpunk.py

if errorlevel 1 (
    echo.
    echo ========================================
    echo  ERROR: Failed to launch neural interface
    echo  Please check Python installation
    echo ========================================
    pause
)

echo.
echo ========================================
echo  Neural link terminated
echo  Thank you for using TURBOPREDICT X PROTEAN
echo ========================================
pause