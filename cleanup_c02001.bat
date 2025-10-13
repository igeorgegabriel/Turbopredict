@echo off
echo ========================================
echo C-02001 PARQUET CLEANUP UTILITY
echo ========================================
echo.
echo This will move old/stale C-02001 parquet files to backup.
echo.
python scripts\cleanup_redundant_parquet.py
echo.
echo ========================================
pause
