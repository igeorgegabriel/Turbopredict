#!/usr/bin/env python3
"""
Fixed version to check PCMSB data freshness without encoding issues.
"""

import pandas as pd
import pyarrow.parquet as pq
from pathlib import Path
from datetime import datetime
import os

def check_pcmsb_freshness():
    """Check freshness of all PCMSB unit data files."""
    
    pcmsb_units = ['C-02001', 'C-104', 'C-13001', 'C-1301', 'C-1302', 'C-201', 'C-202', 'XT-07002']
    
    print('PCMSB DATA FRESHNESS INVESTIGATION')
    print('=' * 70)
    
    for unit in pcmsb_units:
        file_path = Path(f'data/processed/{unit}_1y_0p1h.parquet')
        
        if file_path.exists():
            # Get file modification time
            mtime = datetime.fromtimestamp(file_path.stat().st_mtime)
            file_age_hours = (datetime.now() - mtime).total_seconds() / 3600
            
            # Check data timestamps in the file
            try:
                table = pq.read_table(file_path)
                df = table.to_pandas()
                
                if not df.empty:
                    latest_time = df['time'].max()
                    data_age_hours = (datetime.now() - latest_time).total_seconds() / 3600
                    
                    print(f'{unit}:')
                    print(f'  File modified: {mtime.strftime("%Y-%m-%d %H:%M")} ({file_age_hours:.1f} hours ago)')
                    print(f'  Latest data:   {latest_time.strftime("%Y-%m-%d %H:%M")} ({data_age_hours:.1f} hours old)')
                    print(f'  Data points:   {len(df):,} rows')
                    print(f'  Unique tags:   {df["tag"].nunique()} tags')
                    
                    # Check if data is stale
                    if data_age_hours > 24:
                        print(f'  WARNING: STALE DATA - Over 24 hours old!')
                    elif data_age_hours > 6:
                        print(f'  WARNING: OLD DATA - Over 6 hours old!')
                    else:
                        print(f'  OK: FRESH DATA - Less than 6 hours old')
                        
                else:
                    print(f'{unit}: EMPTY DATA FILE')
                    
            except Exception as e:
                print(f'{unit}: ERROR reading file - {e}')
                
        else:
            print(f'{unit}: DATA FILE NOT FOUND')
            
        print('-' * 50)

def check_pcmsb_build_scripts():
    """Check if PCMSB build scripts exist and are configured."""
    
    print('\nPCMSB BUILD SCRIPT STATUS')
    print('=' * 70)
    
    pcmsb_scripts = {
        'C-02001': 'scripts/build_pcmsb_c02001.py',
        'C-104': 'scripts/build_pcmsb_c104.py', 
        'C-13001': 'scripts/build_pcmsb_c13001.py',
        'C-1301': 'scripts/build_pcmsb_c1301.py',
        'C-1302': 'scripts/build_pcmsb_c1302.py',
        'C-201': 'scripts/build_pcmsb_c201.py',
        'C-202': 'scripts/build_pcmsb_c202.py',
        'XT-07002': 'scripts/build_pcmsb_xt07002.py'
    }
    
    for unit, script_path in pcmsb_scripts.items():
        script_file = Path(script_path)
        
        if script_file.exists():
            print(f'{unit}: OK - Build script exists - {script_path}')
            
            # Check if script is configured for the right Excel file
            try:
                with open(script_file, 'r', encoding='utf-8') as f:
                    content = f.read()
                    if 'PCMSB_Automation_Master.xlsx' in content:
                        print(f'       OK - Correctly configured for PCMSB Excel')
                    else:
                        print(f'       WARNING - May not be configured for PCMSB Excel')
            except Exception as e:
                print(f'       ERROR - Error reading script: {e}')
                
        else:
            print(f'{unit}: ERROR - Build script missing - {script_path}')

def check_pcmsb_excel_files():
    """Check PCMSB Excel file configuration."""
    
    print('\nPCMSB EXCEL FILE STATUS')
    print('=' * 70)
    
    excel_path = Path('excel/PCMSB/PCMSB_Automation_Master.xlsx')
    
    if excel_path.exists():
        print(f'OK - PCMSB Excel file exists: {excel_path}')
        
        # Check Excel file structure
        try:
            import openpyxl
            wb = openpyxl.load_workbook(excel_path)
            sheets = wb.sheetnames
            wb.close()
            
            print(f'   Worksheets: {sheets}')
            
            if 'DL_WORK' in sheets:
                print(f'   OK - DL_WORK sheet present')
            else:
                print(f'   WARNING - DL_WORK sheet missing')
                
        except Exception as e:
            print(f'   ERROR - Error reading Excel file: {e}')
            
    else:
        print(f'ERROR - PCMSB Excel file missing: {excel_path}')
        
        # Check if alternative Excel files exist
        alt_files = list(Path('excel/PCMSB').glob('*.xlsx'))
        if alt_files:
            print(f'   Found alternative Excel files:')
            for f in alt_files:
                print(f'   - {f.name}')

def main():
    """Main investigation function."""
    
    print('INVESTIGATING PCMSB DATA FETCHING ISSUES')
    print('=' * 70)
    
    # 1. Check data freshness
    check_pcmsb_freshness()
    
    # 2. Check build scripts
    check_pcmsb_build_scripts()
    
    # 3. Check Excel files
    check_pcmsb_excel_files()
    
    print('\nRECOMMENDED ACTIONS:')
    print('1. Run individual PCMSB build scripts to refresh stale data')
    print('2. Check PI server connectivity for PCMSB units')
    print('3. Verify Excel file configurations')
    print('4. Run: python scripts/refresh_stale.py (for automated refresh)')

if __name__ == "__main__":
    main()