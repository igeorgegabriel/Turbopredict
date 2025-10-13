#!/usr/bin/env python3
"""Check if Excel file already has PI DataLink data from previous fetch."""
from __future__ import annotations

from pathlib import Path
import sys

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))


def check_excel_data() -> int:
    """Open Excel and check if sheets have existing PI data."""
    try:
        import xlwings as xw
    except ImportError:
        print("[X] xlwings not installed")
        return 1

    xlsx_path = PROJECT_ROOT / "excel" / "PCFS" / "PCFS_Automation.xlsx"

    print("="*80)
    print("CHECK EXCEL EXISTING PI DATA")
    print("="*80)
    print(f"\nOpening: {xlsx_path.name}")

    app = xw.App(visible=True, add_book=False)
    try:
        app.display_alerts = False
        wb = app.books.open(str(xlsx_path))

        print(f"\nSheets in workbook:")
        for i, sheet in enumerate(wb.sheets, 1):
            print(f"  {i}. {sheet.name}")

        # Check if DL_WORK has any data
        try:
            sht = wb.sheets["DL_WORK"]
            print(f"\nChecking DL_WORK sheet...")

            # Try to read A1 (should have formula or value)
            a1_value = sht.range("A1").value
            a1_formula = sht.range("A1").formula

            print(f"  A1 value: {a1_value}")
            print(f"  A1 formula: {a1_formula}")

            # Try to expand from A1
            try:
                data = sht.range("A1").expand().value
                if data:
                    if isinstance(data, list):
                        rows = len(data) if isinstance(data[0], list) else 1
                        print(f"  Data rows: {rows:,}")
                    else:
                        print(f"  Data: {data}")
                else:
                    print(f"  No data found")
            except Exception as e:
                print(f"  Could not expand: {e}")

        except Exception as e:
            print(f"  DL_WORK sheet error: {e}")

        # Check PI DataLink add-in status
        print(f"\nChecking COM Add-ins:")
        try:
            for c in app.api.COMAddIns:
                try:
                    desc = str(getattr(c, 'Description', ''))
                    prog = str(getattr(c, 'ProgId', ''))
                    connected = getattr(c, 'Connect', False)
                    name = desc or prog
                    if 'pi' in name.lower() or 'datalink' in name.lower():
                        status = "CONNECTED" if connected else "DISCONNECTED"
                        print(f"  {name}: {status}")
                except Exception:
                    pass
        except Exception as e:
            print(f"  Could not check add-ins: {e}")

        print(f"\nLeaving Excel open for manual inspection...")
        print(f"Close Excel manually when done.")
        print(f"\n[i] Check if you can manually refresh a PI formula in Excel")
        print(f"    Try typing in any cell: =PICompDat(\"PCFS.K-31-01.31KI-302.PV\",\"*\",\"\\\\PTSG-1MMPDPdb01\")")

        return 0

    except Exception as e:
        print(f"\n[X] ERROR: {e}")
        import traceback
        traceback.print_exc()
        try:
            app.quit()
        except Exception:
            pass
        return 1


if __name__ == "__main__":
    raise SystemExit(check_excel_data())
