@echo off
REM Enable PI Web API for TURBOPREDICT
REM This script sets the PI_WEBAPI_URL environment variable

echo ========================================
echo TURBOPREDICT - Enable PI Web API
echo ========================================
echo.

REM Set for current session
set PI_WEBAPI_URL=http://PTSG-1MMPDPdb01/piwebapi

REM Set permanently for user
setx PI_WEBAPI_URL "http://PTSG-1MMPDPdb01/piwebapi"

echo.
echo ========================================
echo SUCCESS: PI Web API Enabled!
echo ========================================
echo.
echo PI_WEBAPI_URL = %PI_WEBAPI_URL%
echo.
echo Option [1] will now use:
echo   PRIMARY: PI Web API (fast, parallel)
echo   FALLBACK: Excel PI DataLink (if Web API fails)
echo.
echo Ready to test!
echo Run: python turbopredict.py
echo Select option [1]
echo.

pause
