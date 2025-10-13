#!/usr/bin/env python3
"""
Debug the Excel data extraction to understand why only 1 column was extracted instead of 80 tags.
"""

import xlwings as xw
from pathlib import Path
import pandas as pd

def debug_excel_structure(excel_path):
    """Debug the Excel file structure to understand the data layout."""

    print(f"Debugging Excel file: {excel_path}")

    app = xw.App(visible=True)  # Make visible for debugging
    try:
        wb = app.books.open(excel_path)

        print(f"\n=== SHEET NAMES ===")
        sheet_names = [sheet.name for sheet in wb.sheets]
        for i, name in enumerate(sheet_names):
            print(f"{i+1}. {name}")

        # Examine the first sheet (what the parquet builder used)
        ws = wb.sheets[0]
        print(f"\n=== ANALYZING SHEET: {ws.name} ===")

        # Get used range
        used_range = ws.used_range
        if used_range:
            print(f"Used range: {used_range.address}")

            # Get the first few rows to understand structure
            print(f"\n=== FIRST 5 ROWS ===")
            try:
                first_rows = ws.range('A1:Z5').value  # Get first 5 rows, 26 columns
                if first_rows:
                    for i, row in enumerate(first_rows):
                        print(f"Row {i+1}: {row}")
            except:
                print("Could not read first rows")

            # Check column headers specifically
            print(f"\n=== COLUMN HEADERS (Row 1) ===")
            try:
                headers = ws.range('1:1').value  # Get entire first row
                if headers:
                    non_empty_headers = [h for h in headers if h is not None]
                    print(f"Found {len(non_empty_headers)} non-empty headers:")
                    for i, h in enumerate(non_empty_headers[:20]):  # Show first 20
                        print(f"  Col {i+1}: {h}")
                    if len(non_empty_headers) > 20:
                        print(f"  ... and {len(non_empty_headers) - 20} more")
            except Exception as e:
                print(f"Error reading headers: {e}")

        # Check other sheets for C-02001 data
        print(f"\n=== CHECKING FOR C-02001 SHEETS ===")
        for sheet in wb.sheets:
            if 'C-02001' in sheet.name or 'c02001' in sheet.name.lower():
                print(f"Found C-02001 related sheet: {sheet.name}")
                try:
                    used_range = sheet.used_range
                    if used_range:
                        print(f"  Used range: {used_range.address}")
                        # Check if it has our tags
                        first_row = sheet.range('1:1').value
                        if first_row:
                            tag_count = sum(1 for cell in first_row if cell and 'PCM.C-02001' in str(cell))
                            print(f"  Found {tag_count} C-02001 tags in headers")
                except Exception as e:
                    print(f"  Error examining sheet: {e}")

        print(f"\n=== WORKBOOK INFO ===")
        print(f"Total sheets: {len(wb.sheets)}")

        # Look for PI DataLink or other data sources
        print(f"\n=== CHECKING FOR DATA SOURCES ===")
        for sheet in wb.sheets:
            try:
                # Look for cells containing PI-related text
                if hasattr(sheet.api, 'Cells'):
                    found_pi = False
                    # Check a reasonable range for PI references
                    for row in range(1, 50):
                        for col in range(1, 50):
                            try:
                                cell_value = sheet.cells(row, col).value
                                if cell_value and isinstance(cell_value, str):
                                    if 'PCM.C-02001' in cell_value or 'PI' in cell_value.upper():
                                        if not found_pi:
                                            print(f"  Sheet '{sheet.name}' contains PI references:")
                                            found_pi = True
                                        print(f"    {chr(64+col)}{row}: {cell_value[:50]}...")
                                        if found_pi and row > 10:  # Limit output
                                            break
                            except:
                                continue
                        if found_pi and row > 10:
                            break
            except Exception as e:
                continue

        return True

    except Exception as e:
        print(f"Error debugging Excel file: {e}")
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
    print("DEBUGGING EXCEL DATA EXTRACTION")
    print("=" * 60)

    debug_excel_structure(excel_file)

if __name__ == "__main__":
    main()