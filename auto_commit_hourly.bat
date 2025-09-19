@echo off
REM TURBOPREDICT X PROTEAN - Hourly Auto-Commit Launcher
REM This batch file runs the auto-commit script every hour

echo ========================================
echo TURBOPREDICT X PROTEAN Auto-Commit
echo Time: %date% %time%
echo ========================================

REM Change to project directory
cd /d "%~dp0"

REM Run the auto-commit Python script
python scripts\auto_commit.py

REM Log the result
if %ERRORLEVEL% EQU 0 (
    echo [%date% %time%] Auto-commit completed successfully >> auto_commit_history.log
) else (
    echo [%date% %time%] Auto-commit failed with error code %ERRORLEVEL% >> auto_commit_history.log
)

echo.
echo Auto-commit process completed.
echo Check auto_commit.log for detailed logs.
echo.

REM Keep window open for 5 seconds to see the result
timeout /t 5 /nobreak > nul