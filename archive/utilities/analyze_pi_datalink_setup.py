#!/usr/bin/env python3
"""
Analyze the existing PI DataLink setup in the working Excel files to understand
the correct formula syntax and configuration.
"""

import xlwings as xw
from pathlib import Path
import re

def analyze_pi_datalink_formulas(excel_path):
    """Analyze existing PI DataLink formulas in the Excel file."""

    print(f"Analyzing PI DataLink setup in: {excel_path}")

    app = xw.App(visible=False)
    try:
        wb = app.books.open(excel_path)

        pi_formulas = []
        pi_connections = []

        for sheet in wb.sheets:
            print(f"\n=== ANALYZING SHEET: {sheet.name} ===")

            try:
                used_range = sheet.used_range
                if not used_range:
                    print("No used range found")
                    continue

                # Search for PI formulas in a reasonable range
                max_rows = min(100, used_range.last_cell.row)
                max_cols = min(50, used_range.last_cell.column)

                print(f"Scanning {max_rows} rows x {max_cols} columns for PI formulas...")

                for row in range(1, max_rows + 1):
                    for col in range(1, max_cols + 1):
                        try:
                            cell = sheet.cells(row, col)

                            # Check formula
                            if hasattr(cell, 'Formula') and cell.Formula:
                                formula = str(cell.Formula)
                                if any(pi_term in formula.upper() for pi_term in ['PI', 'DATALINK', 'ARCVAL', 'PIVALUE']):
                                    pi_formulas.append({
                                        'sheet': sheet.name,
                                        'cell': f'{chr(64 + col)}{row}',
                                        'formula': formula
                                    })
                                    print(f"  Found PI formula at {chr(64 + col)}{row}: {formula[:100]}...")

                            # Check value for PI tag patterns
                            if hasattr(cell, 'Value') and cell.Value:
                                value = str(cell.Value)
                                if 'PCM.' in value and ('PI' in value or 'FI' in value or 'TI' in value):
                                    pi_connections.append({
                                        'sheet': sheet.name,
                                        'cell': f'{chr(64 + col)}{row}',
                                        'tag': value
                                    })

                        except Exception as e:
                            continue

            except Exception as e:
                print(f"Error scanning sheet {sheet.name}: {e}")

        # Analyze PI DataLink connections
        print(f"\n=== PI DATALINK ANALYSIS ===")
        print(f"Found {len(pi_formulas)} PI formulas")
        print(f"Found {len(pi_connections)} PI tag references")

        if pi_formulas:
            print(f"\n=== SAMPLE PI FORMULAS ===")
            for i, formula_info in enumerate(pi_formulas[:5]):
                print(f"{i+1}. {formula_info['sheet']}.{formula_info['cell']}: {formula_info['formula']}")

        if pi_connections:
            print(f"\n=== SAMPLE PI TAGS ===")
            for i, tag_info in enumerate(pi_connections[:10]):
                print(f"{i+1}. {tag_info['sheet']}.{tag_info['cell']}: {tag_info['tag']}")

        # Look for PI DataLink configuration
        print(f"\n=== LOOKING FOR PI CONFIGURATION ===")
        config_patterns = ['server', 'start', 'end', 'interval', 'time']

        for sheet in wb.sheets:
            for row in range(1, 20):
                for col in range(1, 10):
                    try:
                        cell_value = sheet.cells(row, col).value
                        if cell_value and isinstance(cell_value, str):
                            cell_lower = cell_value.lower()
                            if any(pattern in cell_lower for pattern in config_patterns):
                                adjacent_value = sheet.cells(row, col + 1).value
                                print(f"  {sheet.name}.{chr(64 + col)}{row}: {cell_value} = {adjacent_value}")
                    except:
                        continue

        return pi_formulas, pi_connections

    except Exception as e:
        print(f"Error analyzing PI DataLink: {e}")
        return [], []

    finally:
        if 'wb' in locals():
            wb.close()
        app.quit()

def main():
    """Main entry point."""
    excel_files = [
        r"C:\Users\george.gabrielujai\Documents\CodeX\excel\PCMSB\PCMSB_Automation.xlsx",
        r"C:\Users\george.gabrielujai\Documents\CodeX\excel\PCMSB\PCMSB_C02001_Full_Structure.xlsx"
    ]

    print("=" * 70)
    print("ANALYZING PI DATALINK SETUP IN EXISTING FILES")
    print("=" * 70)

    all_formulas = []
    all_connections = []

    for excel_file in excel_files:
        if Path(excel_file).exists():
            print(f"\n{'='*50}")
            print(f"ANALYZING: {Path(excel_file).name}")
            print(f"{'='*50}")

            formulas, connections = analyze_pi_datalink_formulas(excel_file)
            all_formulas.extend(formulas)
            all_connections.extend(connections)
        else:
            print(f"File not found: {excel_file}")

    print(f"\n" + "=" * 70)
    print(f"SUMMARY")
    print("=" * 70)
    print(f"Total PI formulas found: {len(all_formulas)}")
    print(f"Total PI connections found: {len(all_connections)}")

    if all_formulas:
        print(f"\nMost common formula patterns:")
        formula_patterns = {}
        for formula_info in all_formulas:
            # Extract formula pattern (before first quote or parenthesis)
            pattern = re.split(r'[("\'=]', formula_info['formula'])[0]
            formula_patterns[pattern] = formula_patterns.get(pattern, 0) + 1

        for pattern, count in sorted(formula_patterns.items(), key=lambda x: x[1], reverse=True):
            print(f"  {pattern}: {count} occurrences")

if __name__ == "__main__":
    main()