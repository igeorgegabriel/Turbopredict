#!/usr/bin/env python3
"""
Diagnose PI DataLink runtime in the COM‑launched Excel instance.

What it does
- Opens your plant workbook (PCFS by default)
- Lists COM/Excel add‑ins and their Connected/Installed state
- Evaluates PI formulas directly (PISnapshot and PISampDat) and prints results

Usage
  python scripts/diagnose_datalink_runtime.py [excel_path]

If no excel_path is supplied it uses excel/PCFS/PCFS_Automation.xlsx.
"""
from __future__ import annotations

import sys
from pathlib import Path
import time

try:
    import xlwings as xw
except Exception as e:
    print(f"[ERR] xlwings is required: {e}")
    sys.exit(2)


def _wait_calc(app, timeout=30.0):
    t0 = time.time()
    done = 0
    while time.time() - t0 < timeout:
        try:
            try:
                app.api.CalculateUntilAsyncQueriesDone()
            except Exception:
                pass
            state = int(getattr(app.api, 'CalculationState', 0))
            if state == 0:
                done += 1
                if done >= 3:
                    return True
            else:
                done = 0
        except Exception:
            pass
        time.sleep(0.5)
    return False


def main() -> int:
    excel_path = Path(sys.argv[1]) if len(sys.argv) > 1 else Path('excel/PCFS/PCFS_Automation.xlsx')
    if not excel_path.exists():
        print(f"[ERR] Workbook not found: {excel_path}")
        return 2

    app = xw.App(visible=True, add_book=False)
    try:
        # Lower security and reduce UI prompts
        try:
            app.api.AutomationSecurity = 1  # msoAutomationSecurityLow
        except Exception:
            pass
        app.display_alerts = False
        app.screen_updating = False

        # List add‑ins info
        print("\n[INFO] COM Add-ins:")
        try:
            for c in app.api.COMAddIns:
                try:
                    print("  -", getattr(c, 'ProgId', ''), "| Connected=", getattr(c, 'Connect', None))
                except Exception:
                    pass
        except Exception as e:
            print("  (failed to enumerate COM Add-ins)", e)

        print("\n[INFO] Excel Add-ins:")
        try:
            for a in app.api.AddIns:
                try:
                    print("  -", getattr(a, 'Name', ''), "| Installed=", getattr(a, 'Installed', None))
                except Exception:
                    pass
        except Exception as e:
            print("  (failed to enumerate Add-ins)", e)

        # Try to ensure PI DataLink COM add-in connected
        try:
            for c in app.api.COMAddIns:
                try:
                    desc = str(getattr(c, 'Description', ''))
                    prog = str(getattr(c, 'ProgId', ''))
                    name = (desc or prog).lower()
                    if ('pi' in name and 'datalink' in name) or ('pitime' in name):
                        c.Connect = True
                except Exception:
                    pass
        except Exception:
            pass

        wb = app.books.open(str(excel_path), update_links=False, read_only=False, ignore_read_only_recommended=True)
        try:
            # Use or create a DIAG sheet
            try:
                sht = wb.sheets['DIAG']
            except Exception:
                sht = wb.sheets.add('DIAG', after=wb.sheets[-1])
            sht.clear()
            sht.range('A1').value = 'Formula'
            sht.range('B1').value = 'Result'

            tag = 'PCFS.K-12-01.12SI-401B.PV'
            server = 'PTSG-1MMPDPdb01'

            # PISnapshot with explicit server
            f1 = f'=PISnapshot("{tag}","","{server}")'
            sht.range('A2').value = f1
            _wait_calc(app, 30)
            v1 = sht.range('A2').value
            print("\n[Test] PISnapshot explicit server =>", v1)

            # PISnapshot with default server
            f2 = f'=PISnapshot("{tag}","","")'
            sht.range('A3').value = f2
            _wait_calc(app, 30)
            v2 = sht.range('A3').value
            print("[Test] PISnapshot default server  =>", v2)

            # Short PISampDat spill test (-5m)
            f3 = f'=PISampDat("{tag}","-5m","*","-1m",1,"{server}")'
            sht.range('A5').value = f3
            _wait_calc(app, 45)
            try:
                vals = sht.range('A5').expand().value
            except Exception:
                vals = None
            if vals:
                print("[Test] PISampDat spill rows:", len(vals) if isinstance(vals, list) else 1)
            else:
                print("[Test] PISampDat returned no spill values")

        finally:
            # Do not save workbook changes
            try:
                wb.close()
            except Exception:
                pass
    finally:
        app.display_alerts = True
        app.screen_updating = True
        app.quit()
    return 0


if __name__ == '__main__':
    raise SystemExit(main())

