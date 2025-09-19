#!/usr/bin/env python3
"""
Debug script to understand data freshness calculation
"""
import pandas as pd
from pathlib import Path
from datetime import datetime
import os

def debug_freshness():
    """Debug the data freshness calculation"""
    
    print("DEBUGGING DATA FRESHNESS CALCULATION")
    print("=" * 50)
    
    data_dir = Path("data/processed")
    units = ['K-12-01', 'K-16-01', 'K-19-01', 'K-31-01']
    
    for unit in units:
        print(f"\n--- Unit: {unit} ---")
        
        # Find all files for this unit
        unit_files = []
        for pattern in [f"*{unit}*.parquet", f"**/*{unit}*.parquet"]:
            unit_files.extend(list(data_dir.glob(pattern)))
        
        if not unit_files:
            print(f"  No files found for {unit}")
            continue
            
        # Show all files with timestamps
        print(f"  Files found ({len(unit_files)}):")
        for f in unit_files:
            stat = f.stat()
            mod_time = datetime.fromtimestamp(stat.st_mtime)
            size_mb = stat.st_size / (1024 * 1024)
            print(f"    {f.name}: {mod_time} ({size_mb:.1f} MB)")
        
        # Apply file selection logic
        updated_files = [f for f in unit_files if 'updated' in f.name]
        refreshed_files = [f for f in unit_files if 'refreshed' in f.name and 'updated' not in f.name]
        dedup_files = [f for f in unit_files if 'dedup' in f.name and 'refreshed' not in f.name and 'updated' not in f.name]
        regular_files = [f for f in unit_files if 'dedup' not in f.name and 'refreshed' not in f.name and 'updated' not in f.name]
        
        target_file = None
        if updated_files:
            target_file = max(updated_files, key=lambda x: x.stat().st_mtime)
            selection_reason = "newest updated file"
        elif refreshed_files:
            target_file = max(refreshed_files, key=lambda x: x.stat().st_mtime)
            selection_reason = "newest refreshed file"
        else:
            all_candidates = regular_files + dedup_files
            if all_candidates:
                target_file = max(all_candidates, key=lambda x: x.stat().st_mtime)
                selection_reason = "newest among regular/dedup files"
        
        if target_file:
            file_mod_time = datetime.fromtimestamp(target_file.stat().st_mtime)
            print(f"  Selected file: {target_file.name} ({selection_reason})")
            print(f"  File modified: {file_mod_time}")
            
            # Load and check data timestamps
            try:
                df = pd.read_parquet(target_file)
                
                # Check for time columns
                time_cols = ['time', 'timestamp', 'Time', 'Timestamp']
                time_col = None
                for col in time_cols:
                    if col in df.columns:
                        time_col = col
                        break
                
                if time_col:
                    df[time_col] = pd.to_datetime(df[time_col])
                    if time_col != 'time':
                        df = df.rename(columns={time_col: 'time'})
                    
                    latest_data_time = df['time'].max()
                    earliest_data_time = df['time'].min()
                    
                    # Calculate age like the system does
                    now = datetime.now()
                    if hasattr(latest_data_time, 'tz_localize'):
                        latest_data_time = latest_data_time.tz_localize(None) if latest_data_time.tz is None else latest_data_time.tz_convert(None).tz_localize(None)
                    
                    age_delta = now - latest_data_time
                    data_age_hours = age_delta.total_seconds() / 3600
                    is_stale = data_age_hours > 1.0
                    
                    print(f"  Data records: {len(df):,}")
                    print(f"  Data time range: {earliest_data_time} to {latest_data_time}")
                    print(f"  Latest data age: {data_age_hours:.2f} hours")
                    print(f"  Is stale (>1hr): {is_stale}")
                    
                    # File age vs data age
                    file_age_delta = now - file_mod_time
                    file_age_hours = file_age_delta.total_seconds() / 3600
                    print(f"  File age: {file_age_hours:.2f} hours")
                    print(f"  Gap (file vs data): {abs(file_age_hours - data_age_hours):.2f} hours")
                    
                else:
                    print(f"  No time column found in {df.columns}")
                    
            except Exception as e:
                print(f"  Error reading file: {e}")
        else:
            print(f"  No target file selected!")

if __name__ == "__main__":
    debug_freshness()