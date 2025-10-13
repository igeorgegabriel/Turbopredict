@echo off
REM Set permanent PI fetch timeout to 60 seconds
echo Setting permanent PI_FETCH_TIMEOUT to 60 seconds...
setx PI_FETCH_TIMEOUT 60

REM Also set for current session
set PI_FETCH_TIMEOUT=60

echo.
echo [OK] PI_FETCH_TIMEOUT permanently set to 60 seconds
echo [OK] Current session also updated
echo.
echo Restart your command prompt or IDE for the permanent setting to take effect.
pause
