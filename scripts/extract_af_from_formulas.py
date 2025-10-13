#!/usr/bin/env python3
from __future__ import annotations

from pathlib import Path
import argparse
import re
import openpyxl

def col_to_index(col: str) -> int:
    col = col.strip().upper()
    n = 0
    for ch in col:
        if not ('A' <= ch <= 'Z'):
            raise ValueError(f'Invalid column ref: {col}')
        n = n * 26 + (ord(ch) - ord('A') + 1)
    return n - 1

PI_FUNCS = ("PICurrVal","PISampDat","PICompDat","PISummary","PIData")
PAT = re.compile(r'^\s*(?:' + '|'.join(PI_FUNCS) + r')\s*\(\s*\"([^\"]+)\"', re.IGNORECASE)

def extract(xlsx: Path, col: str, contains: str|None=None, sheet: str|None=None) -> list[str]:
    wb = openpyxl.load_workbook(xlsx, data_only=False, read_only=True)
    try:
        ws = wb[sheet] if sheet else wb.active
        idx = col_to_index(col)
        out: list[str] = []
        seen: set[str] = set()
        for row in ws.iter_rows(min_row=2):
            if idx >= len(row):
                continue
            cell = row[idx]
            v = cell.value
            if not isinstance(v,str):
                continue
            m = PAT.match(v.strip())
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
            wb.close()
        except Exception:
            pass


def main()->int:
    ap = argparse.ArgumentParser(description='Extract AF attribute paths from formula cells in a specific column')
    ap.add_argument('--xlsx', required=True, type=Path)
    ap.add_argument('--col', required=True, type=str)
    ap.add_argument('--out', required=True, type=Path)
    ap.add_argument('--contains', default=None)
    ap.add_argument('--sheet', default=None)
    args = ap.parse_args()

    tags = extract(args.xlsx, args.col, contains=args.contains, sheet=args.sheet)
    args.out.parent.mkdir(parents=True, exist_ok=True)
    args.out.write_text('\n'.join(tags) + ('\n' if tags else ''), encoding='utf-8')
    print(f'Extracted {len(tags)} AF path(s) -> {args.out}')
    if tags:
        print('Sample:', tags[:5])
    return 0

if __name__ == '__main__':
    raise SystemExit(main())
