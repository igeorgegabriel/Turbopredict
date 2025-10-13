#!/usr/bin/env python3
from __future__ import annotations

from pathlib import Path
import argparse
import re

# Use xlwings to read formulas reliably
import xlwings as xw

PI_FUNCS = ("PICurrVal", "PISampDat", "PICompDat", "PISummary", "PIData")
PAT = re.compile(r"^\s*=?\s*(?:" + "|".join(PI_FUNCS) + r")\s*\(\s*\"([^\"]+)\"", re.IGNORECASE)


def extract_af_paths_with_excel(xlsx: Path, contains: str | None = None, max_rows: int | None = None) -> list[str]:
    app = None
    wb = None
    out: list[str] = []
    seen: set[str] = set()
    try:
        app = xw.App(visible=False, add_book=False)
        app.display_alerts = False
        app.screen_updating = False
        wb = app.books.open(str(xlsx))
        for sht in wb.sheets:
            # Determine used range rows
            used = sht.used_range
            nrows = used.last_cell.row if used else 0
            if max_rows:
                nrows = min(nrows, max_rows)
            if nrows < 2:
                continue
            # Column E = 5
            rng = sht.range((2,5),(nrows,5))
            formulas = rng.formula
            if not isinstance(formulas, list):
                formulas = [formulas]
            # Flatten nested lists that xlwings returns
            flat = []
            for row in formulas:
                if isinstance(row, list):
                    flat.extend(row)
                else:
                    flat.append(row)
            for f in flat:
                if not isinstance(f, str):
                    continue
                m = PAT.match(f.strip())
                if not m:
                    continue
                path = m.group(1).strip()
                if contains and contains not in path:
                    continue
                if path not in seen:
                    seen.add(path)
                    out.append(path)
        return out
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
    ap = argparse.ArgumentParser(description='Extract AF attribute paths from Column E formulas via Excel COM')
    ap.add_argument('--xlsx', required=True, type=Path)
    ap.add_argument('--out', required=True, type=Path)
    ap.add_argument('--contains', default=None)
    ap.add_argument('--max-rows', type=int, default=None)
    args = ap.parse_args()

    tags = extract_af_paths_with_excel(args.xlsx, contains=args.contains, max_rows=args.max_rows)
    args.out.parent.mkdir(parents=True, exist_ok=True)
    args.out.write_text('\n'.join(tags) + ('\n' if tags else ''), encoding='utf-8')
    print(f'Extracted {len(tags)} AF path(s) -> {args.out}')
    if tags:
        print('Sample:', tags[:5])
    return 0

if __name__ == '__main__':
    raise SystemExit(main())
