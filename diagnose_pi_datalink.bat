@echo off
echo Diagnosing PI DataLink add-in issue...
echo.

set DEBUG_XL_ADDINS=1
set PI_DATALINK_WARMUP=10

echo Running ONE tag test with debug enabled...
echo This will show all Excel add-ins to diagnose the issue
echo.

python -c "import os; os.environ['DEBUG_XL_ADDINS']='1'; os.environ['PI_DATALINK_WARMUP']='10'; from pi_monitor.batch import build_unit_from_tags; from pathlib import Path; build_unit_from_tags(Path('excel/PCFS_Automation.xlsx'), ['PCFS.K-12-01.12PI-007.PV'], Path('tmp/test_diagnose.parquet'), plant='PCFS', unit='K-12-01-TEST', server='\\\\PTSG-1MMPDPdb01', start='-1h', end='*', step='-0.1h', visible=True, settle_seconds=10)"

echo.
echo Check output above for add-in list
pause
