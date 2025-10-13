#!/usr/bin/env python3
"""
Quick PI DataLink diagnostic via xlwings.

What it does:
- Opens Excel (visible) and lists COM Add-ins.
- Writes a PISampDat() formula for a sample tag and waits up to 60s.
- Prints how many rows were returned.

Usage:
  python scripts/diagnose_pidl.py "PCM.C-02001.020FI1102.PV" "-1y" "*" "-0.1h" "\\\\PTSG-1MMPDPdb01"
"""

from __future__ import annotations

import sys
import time

try:
    import xlwings as xw
except Exception as e:  # pragma: no cover
    print(f"xlwings not available: {e}")
    raise SystemExit(1)


def main() -> int:
    tag = sys.argv[1] if len(sys.argv) > 1 else "PCM.C-02001.020FI1102.PV"
    start = sys.argv[2] if len(sys.argv) > 2 else "-1y"
    end = sys.argv[3] if len(sys.argv) > 3 else "*"
    step = sys.argv[4] if len(sys.argv) > 4 else "-0.1h"
    server = sys.argv[5] if len(sys.argv) > 5 else "\\\\PTSG-1MMPDPdb01"

    app = xw.App(visible=True, add_book=True)
    try:
        app.display_alerts = False
        app.screen_updating = False

        # List COM Add-ins to confirm PI DataLink is present and connected
        try:
            print("COM Add-ins:")
            for c in app.api.COMAddIns:
                try:
                    desc = str(getattr(c, 'Description', ''))
                    prog = str(getattr(c, 'ProgId', ''))
                    conn = bool(getattr(c, 'Connect', False))
                    print(f"  - {desc or prog} | ProgId={prog} | Connected={conn}")
                except Exception:
                    pass
        except Exception as e:
            print(f"Unable to enumerate COM Add-ins: {e}")

        sht = app.books.active.sheets[0]
        sht.name = "DL_TEST"
        sht.clear()
        tag_escaped = tag.replace('"', '""')
        formula = f'=PISampDat("{tag_escaped}","{start}","{end}","{step}",1,"{server}")'
        sht.range("A2").formula = formula

        # Wait up to 60 seconds for PI DataLink calculation
        t0 = time.monotonic()
        rows = 0
        while (time.monotonic() - t0) < 60:
            try:
                app.api.CalculateUntilAsyncQueriesDone()
            except Exception:
                pass
            try:
                vals = sht.range("A2").expand().value
            except Exception:
                vals = None
            if vals:
                try:
                    if isinstance(vals, tuple):
                        vals = list(vals)
                    if isinstance(vals, list) and vals and not isinstance(vals[0], (list, tuple)):
                        vals = [list(vals)]
                    rows = len(vals)
                except Exception:
                    rows = 0
            if rows > 2:
                break
            time.sleep(0.5)

        print(f"Rows detected: {rows}")
        if rows == 0:
            print("No data detected from PI DataLink in automated Excel instance.")
            return 2
        return 0
    finally:
        try:
            app.quit()
        except Exception:
            pass


if __name__ == "__main__":
    raise SystemExit(main())

