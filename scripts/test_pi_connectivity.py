#!/usr/bin/env python3
"""Test if PI DataLink can connect to PI server at all - fetch single tag, single point."""
from __future__ import annotations

from pathlib import Path
import sys

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))


def test_pi_connectivity() -> int:
    """Try to fetch just 1 current value from 1 tag to test connectivity."""
    try:
        import xlwings as xw
    except ImportError:
        print("[X] xlwings not installed")
        return 1

    xlsx_path = PROJECT_ROOT / "excel" / "PCFS" / "PCFS_Automation.xlsx"

    print("="*80)
    print("PI SERVER CONNECTIVITY TEST")
    print("="*80)
    print("\nThis test will:")
    print("1. Open Excel with PI DataLink")
    print("2. Try to fetch ONE current value from ONE tag")
    print("3. Determine if PI server is reachable")

    tag = "PCFS.K-31-01.31KI-302.PV"
    server = r"\\PTSG-1MMPDPdb01"

    print(f"\nTest configuration:")
    print(f"  Tag: {tag}")
    print(f"  Server: {server}")
    print(f"  Formula: =PICompDat(\"{tag}\",\"*\",\"{server}\")")
    print(f"\n  PICompDat fetches just the current/latest value (faster than PISampDat)")

    app = xw.App(visible=True, add_book=False)
    try:
        app.display_alerts = False

        # Ensure PI DataLink is connected
        try:
            for c in app.api.COMAddIns:
                try:
                    desc = str(getattr(c, 'Description', ''))
                    prog = str(getattr(c, 'ProgId', ''))
                    name = (desc or prog).lower()
                    if ('pi' in name and 'datalink' in name) or ('pitime' in name):
                        print(f"\n  Found PI add-in: {desc or prog}")
                        c.Connect = True
                except Exception:
                    pass
        except Exception:
            pass

        wb = app.books.open(str(xlsx_path))

        # Clear and use DL_WORK sheet
        try:
            sht = wb.sheets["DL_WORK"]
            sht.clear()
        except Exception:
            sht = wb.sheets.add("DL_WORK", after=wb.sheets[-1])

        # Write simple current value formula
        formula = f'=PICompDat("{tag}","*","{server}")'
        print(f"\nWriting formula to A1...")
        sht.range("A1").formula = formula

        # Wait for calculation
        import time
        print(f"Waiting for PI DataLink response (max 30s)...")

        for i in range(30):
            try:
                app.api.CalculateFull()
                time.sleep(1)

                value = sht.range("A1").value
                if value and not isinstance(value, str):
                    print(f"\n[OK] SUCCESS!")
                    print(f"  PI Server is REACHABLE")
                    print(f"  Current value: {value}")
                    wb.close()
                    app.quit()
                    return 0

                if isinstance(value, str) and "#" in value:
                    print(f"\n[X] Excel error: {value}")
                    break

            except Exception as e:
                pass

        value = sht.range("A1").value
        print(f"\n[X] TIMEOUT or NO DATA")
        print(f"  Final cell value: {value}")
        print(f"  PI Server might be UNREACHABLE or tag doesn't exist")

        wb.close()
        app.quit()
        return 1

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
    raise SystemExit(test_pi_connectivity())
