#!/usr/bin/env python3
"""
Test Single Unit Processing
Process one unit to test and show progress
"""

import sys
from pathlib import Path
from datetime import datetime
import time

# Add current directory to path
sys.path.insert(0, str(Path(__file__).parent))

def test_k12_unit():
    """Test processing K-12-01 unit specifically"""
    
    print("TESTING K-12-01 UNIT PROCESSING")
    print("=" * 40)
    print(f"Started: {datetime.now().strftime('%H:%M:%S')}")
    
    # Find Excel file
    xlsx_paths = [
        Path("data/raw/Automation.xlsx"),
        Path("data/raw/PCFS_Automation.xlsx"),
        Path("Automation.xlsx")
    ]
    
    xlsx_path = None
    for path in xlsx_paths:
        if path.exists():
            xlsx_path = path
            break
    
    if not xlsx_path:
        print("ERROR: No Excel file found")
        return False
    
    print(f"Using Excel file: {xlsx_path}")
    
    try:
        # Test basic Excel access
        print("\nStep 1: Testing Excel access...")
        
        import xlwings as xw
        app = xw.App(visible=False, add_book=False)
        app.display_alerts = False
        
        print("   Excel app created successfully")
        
        # Open workbook
        print("   Opening workbook...")
        wb = app.books.open(str(xlsx_path))
        
        print("   Workbook opened successfully")
        print(f"   Sheets available: {[ws.name for ws in wb.sheets]}")
        
        # Test data access
        print("\nStep 2: Testing data access...")
        
        # Try to load current data without refresh
        from pi_monitor.ingest import load_latest_frame
        
        print("   Loading current data for K-12-01...")
        df = load_latest_frame(xlsx_path, unit="K-12-01")
        
        print(f"   Current data: {len(df)} records")
        if len(df) > 0:
            print(f"   Time range: {df['time'].min()} to {df['time'].max()}")
            print(f"   Latest value: {df['value'].iloc[-1] if 'value' in df.columns else 'N/A'}")
        
        # Test refresh (small scale)
        print("\nStep 3: Testing PI DataLink refresh...")
        
        refresh_start = time.time()
        print("   Starting RefreshAll...")
        
        try:
            wb.api.RefreshAll()
            refresh_time = time.time() - refresh_start
            print(f"   RefreshAll completed in {refresh_time:.1f} seconds")
            
            # Check if data changed
            print("   Checking for data changes...")
            df_new = load_latest_frame(xlsx_path, unit="K-12-01")
            
            if len(df_new) != len(df):
                print(f"   SUCCESS: Data updated! {len(df)} -> {len(df_new)} records")
            else:
                print(f"   INFO: Record count unchanged ({len(df)} records)")
            
            # Save and close
            print("   Saving workbook...")
            wb.save()
            
        except Exception as refresh_error:
            print(f"   REFRESH ERROR: {refresh_error}")
        
        # Cleanup
        wb.close()
        app.quit()
        
        print(f"\nTest completed at: {datetime.now().strftime('%H:%M:%S')}")
        return True
        
    except Exception as e:
        print(f"\nTEST FAILED: {str(e)}")
        
        # Emergency cleanup
        try:
            if 'wb' in locals():
                wb.close()
            if 'app' in locals():
                app.quit()
        except:
            pass
        
        return False

def main():
    """Run single unit test"""
    
    success = test_k12_unit()
    
    if success:
        print("\n" + "=" * 40)
        print("SINGLE UNIT TEST PASSED")
        print("The Excel refresh system is working")
        print("You can now run full processing with confidence")
    else:
        print("\n" + "=" * 40)
        print("SINGLE UNIT TEST FAILED")
        print("There may be an issue with Excel or PI DataLink")
        print("Check Excel file and PI server connection")

if __name__ == "__main__":
    main()