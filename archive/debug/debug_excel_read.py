#!/usr/bin/env python3
"""
Debug Excel Reading Issue
"""

import sys
from pathlib import Path
import pandas as pd

# Add current directory to path
sys.path.insert(0, str(Path(__file__).parent))

def debug_excel_read():
    """Debug Excel reading step by step"""
    
    print("DEBUGGING EXCEL READ ISSUE")
    print("=" * 40)
    
    xlsx_path = Path("excel/PCFS_Automation.xlsx")
    
    if not xlsx_path.exists():
        print(f"Excel file not found: {xlsx_path}")
        return False
    
    try:
        print(f"Reading Excel file: {xlsx_path}")
        
        # Step 1: Read with no header to find header row
        print("\n1. Reading Excel with header=None...")
        raw = pd.read_excel(xlsx_path, header=None, engine="openpyxl")
        print(f"   Type: {type(raw)}")
        print(f"   Shape: {raw.shape}")
        print(f"   First few values: {raw.iloc[0].tolist()[:5]}")
        
        # Step 2: Find header row
        print("\n2. Finding header row...")
        for i in range(min(10, len(raw))):
            vals = [str(v).strip().upper() if isinstance(v, str) else v for v in raw.iloc[i].tolist()]
            print(f"   Row {i}: {vals[:5]}...")
            if "TIME" in vals:
                print(f"   Found header at row {i}")
                hdr_idx = i
                break
        else:
            print("   No TIME header found")
            return False
        
        # Step 3: Read with proper header
        print(f"\n3. Reading with header={hdr_idx}...")
        df = pd.read_excel(xlsx_path, header=hdr_idx, engine="openpyxl")
        print(f"   Type: {type(df)}")
        print(f"   Shape: {df.shape}")
        print(f"   Columns: {list(df.columns)[:5]}")
        
        return True
        
    except Exception as e:
        print(f"ERROR: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    debug_excel_read()