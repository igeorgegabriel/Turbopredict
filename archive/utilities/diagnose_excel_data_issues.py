#!/usr/bin/env python3
"""
Diagnose why the Excel file is not fetching complete data.
Check various potential issues with PI DataLink setup.
"""

import xlwings as xw
from pathlib import Path
import pandas as pd
from datetime import datetime

def diagnose_excel_data_issues(excel_path):
    """Comprehensive diagnosis of Excel data fetching issues."""

    print(f"Diagnosing data issues in: {excel_path}")

    app = xw.App(visible=False)
    try:
        wb = app.books.open(excel_path)

        print(f"\n=== EXCEL FILE ANALYSIS ===")
        print(f"File: {Path(excel_path).name}")
        print(f"Sheets: {[sheet.name for sheet in wb.sheets]}")

        # Check each sheet for issues
        for sheet in wb.sheets:
            print(f"\n--- SHEET: {sheet.name} ---")

            try:
                used_range = sheet.used_range
                if not used_range:
                    print("No data found in sheet")
                    continue

                print(f"Used range: {used_range.address}")
                print(f"Last cell: Row {used_range.last_cell.row}, Col {used_range.last_cell.column}")

                # Check for formulas
                formula_count = 0
                error_count = 0
                pi_formula_count = 0

                # Sample first 50 rows and columns for analysis
                max_check_rows = min(50, used_range.last_cell.row)
                max_check_cols = min(50, used_range.last_cell.column)

                print(f"Checking {max_check_rows} rows x {max_check_cols} columns for issues...")

                for row in range(1, max_check_rows + 1):
                    for col in range(1, max_check_cols + 1):
                        try:
                            cell = sheet.cells(row, col)

                            # Check for formulas
                            if hasattr(cell, 'Formula') and cell.Formula:
                                formula = str(cell.Formula)
                                formula_count += 1

                                if 'PI' in formula.upper():
                                    pi_formula_count += 1

                                # Check for errors in formula results
                                if hasattr(cell, 'Value'):
                                    value = cell.Value
                                    if isinstance(value, str) and ('#' in value or 'Error' in value or 'N/A' in value):
                                        error_count += 1
                                        if error_count <= 5:  # Show first 5 errors
                                            print(f"    ERROR at {chr(64+col)}{row}: {formula} → {value}")

                        except Exception as e:
                            continue

                print(f"  Total formulas: {formula_count}")
                print(f"  PI formulas: {pi_formula_count}")
                print(f"  Formula errors: {error_count}")

                # Check data population
                if used_range.last_cell.row > 1:
                    # Sample some data to check population
                    sample_range = f"A1:{chr(64 + min(10, used_range.last_cell.column))}{min(10, used_range.last_cell.row)}"
                    try:
                        sample_data = sheet.range(sample_range).value
                        if sample_data:
                            print(f"  Sample data preview:")
                            for i, row_data in enumerate(sample_data[:3]):
                                print(f"    Row {i+1}: {row_data}")
                    except Exception as e:
                        print(f"  Could not read sample data: {e}")

            except Exception as e:
                print(f"Error analyzing sheet {sheet.name}: {e}")

        # Check for PI DataLink specific issues
        print(f"\n=== PI DATALINK DIAGNOSTICS ===")

        # Look for PI-related add-ins
        try:
            addins = wb.Application.AddIns
            pi_addins = []
            for i in range(1, addins.Count + 1):
                try:
                    addin = addins.Item(i)
                    if 'PI' in addin.Name.upper() or 'DATALINK' in addin.Name.upper():
                        pi_addins.append(f"{addin.Name} (Installed: {addin.Installed})")
                except:
                    continue

            if pi_addins:
                print("PI DataLink Add-ins found:")
                for addin in pi_addins:
                    print(f"  {addin}")
            else:
                print("⚠ WARNING: No PI DataLink add-ins detected")

        except Exception as e:
            print(f"Could not check add-ins: {e}")

        # Check for external data connections
        try:
            connections = wb.Connections
            print(f"External connections: {connections.Count}")
            for i in range(1, connections.Count + 1):
                try:
                    conn = connections.Item(i)
                    print(f"  Connection {i}: {conn.Name}")
                except:
                    continue
        except Exception as e:
            print(f"Could not check connections: {e}")

        return True

    except Exception as e:
        print(f"Error diagnosing Excel file: {e}")
        import traceback
        traceback.print_exc()
        return False

    finally:
        if 'wb' in locals():
            wb.close()
        app.quit()

def check_original_working_file():
    """Check the original working Excel file to understand the working pattern."""

    original_file = r"C:\Users\george.gabrielujai\Documents\CodeX\excel\PCMSB\PCMSB_Automation.xlsx"

    if not Path(original_file).exists():
        print(f"Original file not found: {original_file}")
        return

    print(f"\n" + "="*60)
    print("ANALYZING ORIGINAL WORKING FILE FOR COMPARISON")
    print("="*60)

    app = xw.App(visible=False)
    try:
        wb = app.books.open(original_file)

        for sheet in wb.sheets:
            print(f"\n--- ORIGINAL SHEET: {sheet.name} ---")

            used_range = sheet.used_range
            if used_range:
                print(f"Used range: {used_range.address}")

                # Check how data is actually being fetched in the working file
                # Look for the pattern that successfully gets data

                # Check first few rows for the working pattern
                for row in range(1, min(10, used_range.last_cell.row + 1)):
                    for col in range(1, min(5, used_range.last_cell.column + 1)):
                        try:
                            cell = sheet.cells(row, col)
                            value = cell.Value
                            formula = getattr(cell, 'Formula', None)

                            if formula:
                                print(f"  {chr(64+col)}{row}: Formula='{formula}' Value='{value}'")
                            elif value is not None:
                                print(f"  {chr(64+col)}{row}: Value='{value}'")

                        except Exception as e:
                            continue

    except Exception as e:
        print(f"Error analyzing original file: {e}")

    finally:
        if 'wb' in locals():
            wb.close()
        app.quit()

def test_simple_pi_connection():
    """Test a simple PI connection to verify basic connectivity."""

    print(f"\n" + "="*60)
    print("TESTING SIMPLE PI CONNECTION")
    print("="*60)

    # Create a simple test file
    test_file = r"C:\Users\george.gabrielujai\Documents\CodeX\excel\PCMSB\PI_Connection_Test.xlsx"

    app = xw.App(visible=True)
    try:
        # Create new workbook
        wb = app.books.add()

        ws = wb.sheets[0]
        ws.name = "PI_Test"

        # Add a simple test
        ws.range('A1').value = 'PI Connection Test'
        ws.range('A2').value = 'Tag:'
        ws.range('B2').value = 'PCM.C-02001.020FI0101.PV'  # First tag from list
        ws.range('A3').value = 'Current Value:'

        # Try different PI formula syntaxes
        formulas_to_test = [
            '=PIValue(B2,NOW())',
            '=PI(B2)',
            '=PIArchive(B2,NOW())',
            '=PIINTERP(B2,NOW())',
            '=PICurrentValue(B2)',
        ]

        print("Testing PI formula patterns:")
        for i, formula in enumerate(formulas_to_test):
            row = 4 + i
            ws.range(f'A{row}').value = f'Test {i+1}:'
            ws.range(f'B{row}').value = formula
            print(f"  Row {row}: {formula}")

        # Save test file
        wb.save_as(test_file)
        print(f"\nTest file saved: {test_file}")
        print("Please check this file manually to see which formulas work")

        return True

    except Exception as e:
        print(f"Error creating test file: {e}")
        return False

    finally:
        print("Test file created - leaving Excel open for manual testing")

def main():
    """Main entry point."""

    excel_file = r"C:\Users\george.gabrielujai\Documents\CodeX\excel\PCMSB\PCMSB_C02001_Full_Structure.xlsx"

    print("=" * 70)
    print("DIAGNOSING WHY EXCEL IS NOT FETCHING COMPLETE DATA")
    print("=" * 70)

    # Check if the file exists
    if not Path(excel_file).exists():
        print(f"File not found: {excel_file}")
        return

    # Step 1: Diagnose the current file
    print("STEP 1: Analyzing current Excel file...")
    diagnose_excel_data_issues(excel_file)

    # Step 2: Compare with original working file
    print("\nSTEP 2: Comparing with original working file...")
    check_original_working_file()

    # Step 3: Test simple PI connection
    print("\nSTEP 3: Testing simple PI connection...")
    test_simple_pi_connection()

    print(f"\n" + "="*70)
    print("DIAGNOSIS COMPLETE")
    print("="*70)

    print("\nCommon issues and solutions:")
    print("1. PI DataLink not installed → Install PI DataLink add-in")
    print("2. PI Server not configured → Configure PI Server connection")
    print("3. Wrong formula syntax → Use working formula from original file")
    print("4. Network/permission issues → Check PI Server access")
    print("5. Excel version compatibility → Try Excel 32-bit vs 64-bit")
    print("6. Time range too large → Start with smaller date range")

if __name__ == "__main__":
    main()