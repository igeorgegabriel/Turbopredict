@echo off
echo ================================================================
echo AUTOMATED PI DATA FETCH FOR C-02001
echo ================================================================
echo This will automatically:
echo 1. Create Excel file with PISampDat formulas for all 80 tags
echo 2. Refresh PI DataLink to fetch 1.5 years of data
echo 3. Generate final parquet file with complete dataset
echo.
echo Expected output: 15-20MB parquet file with all 80 PI tags
echo.
echo Starting automation...
echo ================================================================

cd /d "C:\Users\george.gabrielujai\Documents\CodeX"

python complete_automated_pi_fetch.py

echo.
echo ================================================================
echo AUTOMATION COMPLETED
echo ================================================================
echo Check the output above for results.
echo Your parquet file should be in: data\processed\
echo.
pause