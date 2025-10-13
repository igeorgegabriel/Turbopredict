#!/usr/bin/env python3
"""
Test PCMSB single tag fetch to identify timeout issue
"""

import sys
import os
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent))

def test_single_pcmsb_tag():
    """Test fetching a single PCMSB tag to diagnose timeout"""

    print("TESTING SINGLE PCMSB TAG FETCH")
    print("=" * 35)

    # Test with one tag from C-02001
    test_tag = "PCM.C-02001.020FI0101.PV"
    excel_path = Path("excel/PCMSB/PCMSB_Automation.xlsx")

    print(f"Testing tag: {test_tag}")
    print(f"Excel file: {excel_path}")

    try:
        import xlwings as xw
        from datetime import datetime
        import time

        print(f"\nOpening Excel...")

        # Use visible mode to see what happens
        app = xw.App(visible=False, add_book=False)

        try:
            wb = app.books.open(str(excel_path))
            print(f"Excel opened successfully")

            # Use DL_WORK sheet (our fix)
            sheet_name = "DL_WORK"

            try:
                sht = wb.sheets[sheet_name]
                print(f"Using sheet: {sheet_name}")

                # Clear the sheet
                sht.clear()
                print(f"Sheet cleared")

                # Test PI formula
                server = "\\\\PTSG-1MMPDPdb01"
                start = "-1h"  # Just 1 hour for testing
                end = "*"
                step = "-6m"  # 6 minute intervals

                tag_escaped = test_tag.replace('"', '""')
                formula = f'=PISampDat("{tag_escaped}","{start}","{end}","{step}",1,"{server}")'

                print(f"Testing formula: {formula}")
                print(f"Parameters:")
                print(f"   Tag: {test_tag}")
                print(f"   Server: {server}")
                print(f"   Time range: {start} to {end}")
                print(f"   Step: {step}")

                # Write formula to A2
                start_time = time.time()
                sht.range("A2").formula = formula

                print(f"Formula written, waiting for calculation...")

                # Force calculation
                wb.app.api.CalculateFullRebuild()

                # Wait a bit
                time.sleep(5)

                # Check result
                result = sht.range("A2").value
                elapsed = time.time() - start_time

                print(f"Result after {elapsed:.1f}s: {result}")

                # Check if we got data in surrounding cells
                for row in range(2, 12):  # Check 10 rows
                    cell_value = sht.range(f"A{row}").value
                    if cell_value is not None:
                        print(f"   A{row}: {cell_value}")

                print(f"\nTest completed in {elapsed:.1f} seconds")

            except Exception as e:
                print(f"Error with sheet operations: {e}")

            wb.close()

        except Exception as e:
            print(f"Error with Excel operations: {e}")

        app.quit()

    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()

    print(f"\nDIAGNOSIS:")
    print(f"If this test shows timeouts or errors, it indicates:")
    print(f"1. PI server connectivity issues")
    print(f"2. Authentication problems")
    print(f"3. Tag name or server path incorrect")
    print(f"4. Network issues")

if __name__ == "__main__":
    test_single_pcmsb_tag()