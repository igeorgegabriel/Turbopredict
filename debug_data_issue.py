#!/usr/bin/env python3
"""
Debug Data Processing Issue
Find where the dict vs DataFrame issue is occurring
"""

import sys
from pathlib import Path

# Add current directory to path
sys.path.insert(0, str(Path(__file__).parent))

def debug_data_processing():
    """Debug the data processing issue step by step"""
    
    print("DEBUGGING DATA PROCESSING ISSUE")
    print("=" * 50)
    
    try:
        from pi_monitor.parquet_database import ParquetDatabase
        from pi_monitor.ingest import load_latest_frame
        
        # Initialize database
        db = ParquetDatabase()
        print("+ ParquetDatabase initialized")
        
        # Test unit
        unit = "K-12-01"
        xlsx_path = Path("excel/PCFS_Automation.xlsx")
        
        print(f"\nTesting unit: {unit}")
        print(f"Excel file: {xlsx_path}")
        
        # Test 1: Check get_unit_data
        print("\n1. Testing get_unit_data...")
        df = db.get_unit_data(unit)
        print(f"   Type returned: {type(df)}")
        print(f"   Is DataFrame: {type(df).__name__ == 'DataFrame'}")
        if hasattr(df, 'empty'):
            print(f"   Is empty: {df.empty}")
            print(f"   Shape: {df.shape}")
        else:
            print(f"   Content: {df}")
        
        # Test 2: Check get_data_freshness_info
        print("\n2. Testing get_data_freshness_info...")
        info = db.get_data_freshness_info(unit)
        print(f"   Type returned: {type(info)}")
        print(f"   Is dict: {type(info).__name__ == 'dict'}")
        if isinstance(info, dict):
            print(f"   Keys: {list(info.keys())}")
        
        # Test 3: Check load_latest_frame
        if xlsx_path.exists():
            print("\n3. Testing load_latest_frame...")
            try:
                fresh_df = load_latest_frame(xlsx_path, unit=unit)
                print(f"   Type returned: {type(fresh_df)}")
                print(f"   Is DataFrame: {type(fresh_df).__name__ == 'DataFrame'}")
                if hasattr(fresh_df, 'empty'):
                    print(f"   Is empty: {fresh_df.empty}")
                    print(f"   Shape: {fresh_df.shape}")
                    if not fresh_df.empty:
                        print(f"   Columns: {list(fresh_df.columns)}")
                else:
                    print(f"   Content: {fresh_df}")
            except Exception as e:
                print(f"   ERROR in load_latest_frame: {e}")
        else:
            print(f"\n3. Excel file not found: {xlsx_path}")
        
        print("\nDEBUG COMPLETED")
        return True
        
    except Exception as e:
        print(f"DEBUG FAILED: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    debug_data_processing()