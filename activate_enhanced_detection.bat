@echo off
echo === ACTIVATING ENHANCED ANOMALY DETECTION ===
echo Setting environment variables for full detection capability...

set ENABLE_AE_LIVE=1
set TF_ENABLE_ONEDNN_OPTS=0
set FORCE_ANALYSIS=1

echo.
echo Environment variables set:
echo   ENABLE_AE_LIVE=1         (AutoEncoder + Live inference)
echo   TF_ENABLE_ONEDNN_OPTS=0  (Reduce TensorFlow warnings)
echo   FORCE_ANALYSIS=1         (Bypass unit offline detection)
echo.
echo Enhanced detection now includes:
echo   - 2.5-sigma detection
echo   - AutoEncoder verification (26K+ timestamps)
echo   - MTD (Mahalanobis-Taguchi Distance)
echo   - Isolation Forest
echo.
echo Run: python turbopredict.py
echo Then select option [2] for full enhanced analysis
echo.
pause