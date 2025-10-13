@echo off
REM TURBOPREDICT - NO ADMIN Install
REM Uses Windows Startup folder instead of Task Scheduler

echo ========================================
echo TURBOPREDICT Service Install (NO ADMIN)
echo ========================================
echo.

REM Get startup folder path
set STARTUP=%APPDATA%\Microsoft\Windows\Start Menu\Programs\Startup

echo Installing to Windows Startup folder...
echo Location: %STARTUP%
echo.

REM Create shortcut to VBS launcher
powershell "$ws = New-Object -ComObject WScript.Shell; $s = $ws.CreateShortcut('%STARTUP%\TurboPredict Service.lnk'); $s.TargetPath = '%CD%\start_service.vbs'; $s.WorkingDirectory = '%CD%'; $s.Description = 'TurboPredict Background Service'; $s.Save()"

if %errorLevel% EQU 0 (
    echo.
    echo ========================================
    echo SUCCESS: Service installed!
    echo ========================================
    echo.
    echo Shortcut created in Startup folder
    echo Service will auto-start at next logon
    echo.
    echo To start now without rebooting:
    echo   Double-click: start_service.vbs
    echo   OR use Option [3] in turbopredict.py
    echo.
    echo To uninstall:
    echo   Delete shortcut from: %STARTUP%
    echo.
) else (
    echo.
    echo ERROR: Failed to create startup shortcut
    echo Error code: %errorLevel%
    echo.
)

pause
