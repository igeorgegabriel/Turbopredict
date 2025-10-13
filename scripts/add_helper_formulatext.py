#!/usr/bin/env python3
from __future__ import annotations

from pathlib import Path
import argparse
import xlwings as xw


def add_helper(xlsx: Path, source_col: str = 'E', helper_col: str = 'N') -> None:
    app = None
    wb = None
    try:
        app = xw.App(visible=True, add_book=False)
        app.display_alerts = False
        wb = app.books.open(str(xlsx))
        for sht in wb.sheets:
            used = sht.used_range
            nrows = used.last_cell.row if used else 0
            if nrows < 2:
                continue
            # Write header
            sht.range(f"{helper_col}1").value = f"FORMULATEXT({source_col})"
            # Fill helper with FORMULATEXT(source_col)
            sht.range(f"{helper_col}2").formula = f"=FORMULATEXT({source_col}2)"
            sht.range(f"{helper_col}2:{helper_col}{nrows}").api.FillDown()
        wb.save()
    finally:
        try:
            if wb is not None:
                wb.close()
        except Exception:
            pass
        try:
            if app is not None:
                app.quit()
        except Exception:
            pass


def main() -> int:
    ap = argparse.ArgumentParser(description='Add helper column with FORMULATEXT on a source column')
    ap.add_argument('--xlsx', required=True, type=Path)
    ap.add_argument('--source-col', default='E')
    ap.add_argument('--helper-col', default='N')
    args = ap.parse_args()

    add_helper(args.xlsx, source_col=args.source_col, helper_col=args.helper_col)
    print(f'Helper column {args.helper_col} added with FORMULATEXT({args.source_col}). Saved workbook.')
    return 0

if __name__ == '__main__':
    raise SystemExit(main())
