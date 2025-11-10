#!/usr/bin/env python3
"""
Examine the DL_WORK sheet to understand the PI DataLink setup.
"""

import xlwings as xw
from pathlib import Path

def examine_dl_work_sheet(excel_path):
    """Examine the DL_WORK sheet structure."""

    print(f"Examining DL_WORK sheet in: {excel_path}")

    app = xw.App(visible=True)
    try:
        wb = app.books.open(excel_path)

        # Focus on DL_WORK sheet
        if 'DL_WORK' in [sheet.name for sheet in wb.sheets]:
            ws = wb.sheets['DL_WORK']
            print(f"\n=== ANALYZING DL_WORK SHEET ===")

            used_range = ws.used_range
            if used_range:
                print(f"Used range: {used_range.address}")

                # Get all data from the sheet
                print(f"\n=== ALL DATA IN DL_WORK ===")
                try:
                    all_data = used_range.value
                    if all_data:
                        print(f"Shape: {len(all_data)} rows x {len(all_data[0]) if all_data[0] else 0} columns")

                        # Show first 10 rows
                        for i, row in enumerate(all_data[:10]):
                            print(f"Row {i+1}: {row}")

                        if len(all_data) > 10:
                            print(f"... and {len(all_data) - 10} more rows")

                except Exception as e:
                    print(f"Error reading all data: {e}")

                # Look for PI tag patterns
                print(f"\n=== SEARCHING FOR PI TAGS ===")
                try:
                    all_data = used_range.value
                    pi_tags_found = []

                    for row_idx, row in enumerate(all_data):
                        if row:
                            for col_idx, cell in enumerate(row):
                                if cell and isinstance(cell, str) and 'PCM.' in cell:
                                    pi_tags_found.append((row_idx + 1, col_idx + 1, cell))

                    if pi_tags_found:
                        print(f"Found {len(pi_tags_found)} PI tags:")
                        for row, col, tag in pi_tags_found[:20]:  # Show first 20
                            print(f"  Row {row}, Col {col}: {tag}")
                        if len(pi_tags_found) > 20:
                            print(f"  ... and {len(pi_tags_found) - 20} more")
                    else:
                        print("No PI tags found in DL_WORK sheet")

                except Exception as e:
                    print(f"Error searching for PI tags: {e}")

        else:
            print("DL_WORK sheet not found")

        return True

    except Exception as e:
        print(f"Error examining DL_WORK sheet: {e}")
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
    print("EXAMINING DL_WORK SHEET")
    print("=" * 60)

    examine_dl_work_sheet(excel_file)

if __name__ == "__main__":
    main()