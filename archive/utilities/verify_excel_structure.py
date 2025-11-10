#!/usr/bin/env python3
"""
Verify that the Excel file now has all 80 PI tag columns properly set up.
"""

import xlwings as xw
from pathlib import Path

def verify_excel_structure(excel_path):
    """Verify the Excel file structure after modification."""

    print(f"Verifying Excel file: {excel_path}")

    app = xw.App(visible=False)
    try:
        wb = app.books.open(excel_path)
        ws = wb.sheets['Sheet1']

        # Check headers
        print("Checking headers...")
        used_range = ws.used_range
        if used_range:
            first_row = ws.range('1:1').value
            non_empty_headers = [h for h in first_row if h is not None]

            print(f"Total columns with headers: {len(non_empty_headers)}")
            print(f"Expected: 81 (Timestamp + 80 PI tags)")

            # Show first few and last few headers
            print(f"\nFirst 5 headers: {non_empty_headers[:5]}")
            print(f"Last 5 headers: {non_empty_headers[-5:]}")

            # Count C-02001 tags
            c02001_tags = [h for h in non_empty_headers if 'PCM.C-02001' in str(h)]
            print(f"\nC-02001 tags found: {len(c02001_tags)}")

            # Check for specific tags
            sample_tags = [
                'PCM.C-02001.020FI0101.PV',
                'PCM.C-02001.020TI6701.PV',
                'PCM.C-02001.020ZI6102C.PV'
            ]

            print(f"\nChecking for sample tags:")
            for tag in sample_tags:
                if tag in non_empty_headers:
                    col_index = non_empty_headers.index(tag) + 1
                    print(f"  ✓ {tag} found in column {col_index}")
                else:
                    print(f"  ✗ {tag} NOT found")

            return len(c02001_tags) == 80

        return False

    except Exception as e:
        print(f"Error verifying Excel structure: {e}")
        return False

    finally:
        if 'wb' in locals():
            wb.close()
        app.quit()

def main():
    excel_file = r"C:\Users\george.gabrielujai\Documents\CodeX\excel\PCMSB\PCMSB_Automation.xlsx"

    if not Path(excel_file).exists():
        print(f"Excel file not found: {excel_file}")
        return

    print("=" * 60)
    print("VERIFYING EXCEL FILE STRUCTURE")
    print("=" * 60)

    success = verify_excel_structure(excel_file)

    if success:
        print("\n✓ Excel file verification PASSED!")
        print("All 80 C-02001 PI tags are properly configured as column headers.")
    else:
        print("\n✗ Excel file verification FAILED!")

if __name__ == "__main__":
    main()