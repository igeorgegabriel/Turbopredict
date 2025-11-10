#!/usr/bin/env python3
"""
Check Data Ranges - Compare master vs refreshed data
"""

import sys
from pathlib import Path
import pandas as pd

# Add current directory to path
sys.path.insert(0, str(Path(__file__).parent))

def check_data_ranges():
    """Check what data ranges we have in different files"""
    
    print("CHECKING DATA RANGES")
    print("=" * 40)
    
    # Files to check
    files_to_check = [
        "data/processed/K-12-01_1y_0p1h.dedup.parquet",  # Master historical data
        "data/processed/K-12-01_refreshed_20250912_113353.parquet"  # Fresh data
    ]
    
    for file_path in files_to_check:
        path = Path(file_path)
        if not path.exists():
            print(f"File not found: {file_path}")
            continue
            
        print(f"\nFile: {path.name}")
        print("-" * 30)
        
        try:
            df = pd.read_parquet(path)
            print(f"   Records: {len(df):,}")
            print(f"   Size: {path.stat().st_size / (1024*1024):.1f} MB")
            
            if 'time' in df.columns:
                df['time'] = pd.to_datetime(df['time'])
                print(f"   Time range: {df['time'].min()} to {df['time'].max()}")
                
                # Check data age
                from datetime import datetime
                latest = df['time'].max()
                if pd.notna(latest):
                    age_hours = (datetime.now() - latest).total_seconds() / 3600
                    print(f"   Data age: {age_hours:.1f} hours")
            else:
                print("   No time column found")
                
        except Exception as e:
            print(f"   Error reading file: {e}")

if __name__ == "__main__":
    check_data_ranges()