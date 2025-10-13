@echo off
REM TURBOPREDICT - Install Windows Scheduled Task
REM This script creates a background service that runs continuously
REM even when the laptop is locked or sleeping
REM NO ADMIN RIGHTS REQUIRED - runs as current user

echo ========================================
echo TURBOPREDICT Background Service Setup
echo ========================================
echo.
echo Note: This runs as your user account (no admin needed)
echo The service works when locked but stops on logout/reboot
echo.

echo Creating background service task...
echo.

REM Create the scheduled task using schtasks command
REM Runs at logon and continues forever
REM No /ru or /rl flags = runs as current user, no admin needed
schtasks /create ^
    /tn "TurboPredictHourlyAnalysis" ^
    /tr "powershell.exe -ExecutionPolicy Bypass -File \"C:\Users\george.gabrielujai\Documents\CodeX\scripts\scheduled_analysis.ps1\"" ^
    /sc onlogon ^
    /f

if %errorLevel% EQU 0 (
    echo.
    echo ========================================
    echo SUCCESS: Background service created!
    echo ========================================
    echo.
    echo Task Name: TurboPredictHourlyAnalysis
    echo Mode: Continuous service (runs forever)
    echo Runs as: %USERNAME% (current user - no admin needed)
    echo Starts: At logon automatically
    echo Works when locked: YES
    echo.
    echo The service will start automatically and run hourly cycles.
    echo.
    echo To start immediately, run:
    echo   schtasks /run /tn "TurboPredictHourlyAnalysis"
    echo.
    echo To view logs:
    echo   type logs\hourly_refresh.log
    echo   type logs\scheduled_service.log
    echo.
    echo To stop the service:
    echo   Use Option [3] in turbopredict.py
    echo   OR: schtasks /end /tn "TurboPredictHourlyAnalysis"
    echo.
) else (
    echo.
    echo ERROR: Failed to create scheduled task
    echo Error code: %errorLevel%
    echo.
)

pause
