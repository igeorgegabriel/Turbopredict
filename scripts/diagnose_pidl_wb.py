#!/usr/bin/env python3
from __future__ import annotations

import sys
import time
from pathlib import Path

try:
    import xlwings as xw
except Exception as e:
    print(f"xlwings not available: {e}")
    raise SystemExit(1)


def main() -> int:
    book = Path(sys.argv[1]) if len(sys.argv) > 1 else Path('excel/PCMSB/PCMSB_Automation.xlsx')
    sheet = sys.argv[2] if len(sys.argv) > 2 else 'DL_WORK'
    tag = sys.argv[3] if len(sys.argv) > 3 else 'PCM.C-02001.020FI1102.PV'
    start = sys.argv[4] if len(sys.argv) > 4 else '-1y'
    end = sys.argv[5] if len(sys.argv) > 5 else '*'
    step = sys.argv[6] if len(sys.argv) > 6 else '-0.1h'
    server = sys.argv[7] if len(sys.argv) > 7 else '\\PTSG-1MMPDPdb01'

    app = xw.App(visible=True, add_book=False)
    try:
        app.display_alerts = False
        app.screen_updating = False
        wb = app.books.open(str(book))
        try:
            sht = wb.sheets[sheet]
        except Exception:
            sht = wb.sheets.add(sheet)
        sht.clear()
        formula = f'=PISampDat("{tag}","{start}","{end}","{step}",1,"{server}")'
        sht.range('A2').formula = formula
        t0 = time.monotonic()
        rows = 0
        while (time.monotonic() - t0) < 90:
            try:
                app.api.CalculateUntilAsyncQueriesDone()
            except Exception:
                pass
            try:
                vals = sht.range('A2').expand().value
            except Exception:
                vals = None
            if vals:
                if isinstance(vals, tuple):
                    vals = list(vals)
                if isinstance(vals, list) and vals and not isinstance(vals[0], (list, tuple)):
                    vals = [list(vals)]
                rows = len(vals)
                if rows > 2:
                    break
            time.sleep(0.5)
        print(f"rows: {rows}")
        return 0 if rows > 0 else 2
    finally:
        try:
            wb.close()
        except Exception:
            pass
        app.quit()


if __name__ == '__main__':
    raise SystemExit(main())

