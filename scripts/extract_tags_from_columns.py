#!/usr/bin/env python3
"""
Extract PI tags from specific Excel columns.

Defaults match ABF LIMIT REVIEW (CURRENT).xlsx:
 - Column D: TAG (PI tag id like PI-21023.PV or S2AH-21002A.PV)
 - Column C: OPERATING PARAMETER (name/description)
 - Column B: PITAG (alternate/special naming)
 - Column A: family/criticality (ignored by default)

Usage:
  python scripts/extract_tags_from_columns.py \
      --xlsx "excel/ABFSB/ABF LIMIT REVIEW (CURRENT).xlsx" \
      --tag-col D --out config/tags_abf_21k002.txt

Options:
  --name-col, --special-col, --family-col are optional and only used for
  diagnostics; the output file is 1 tag per line for use by batch-unit.
"""

from __future__ import annotations

from pathlib import Path
import argparse
import openpyxl


def col_to_index(col: str) -> int:
    col = col.strip().upper()
    if not col:
        return -1
    n = 0
    for ch in col:
        if not ('A' <= ch <= 'Z'):
            raise ValueError(f"Invalid column ref: {col}")
        n = n * 26 + (ord(ch) - ord('A') + 1)
    return n - 1  # zero-based


def extract(
    xlsx: Path,
    tag_col: str,
    sheet: str | None = None,
    contains: str | None = None,
    filter_col: str | None = None,
) -> list[str]:
    wb = openpyxl.load_workbook(xlsx, data_only=True, read_only=True)
    ws = wb[sheet] if sheet else wb.active
    idx = col_to_index(tag_col)
    fidx = col_to_index(filter_col) if filter_col else -1
    tags: list[str] = []
    seen: set[str] = set()
    for row in ws.iter_rows(min_row=2, values_only=True):  # skip header
        if idx >= len(row):
            continue
        # Apply optional row filter on a separate column
        if fidx >= 0:
            fv = row[fidx] if fidx < len(row) else None
            if contains and isinstance(fv, str) and contains not in fv:
                # If filter column provided, apply contains on that column
                continue

        v = row[idx]
        if not isinstance(v, str):
            continue
        s = v.strip()
        if not s:
            continue
        # If no separate filter column is provided, allow contains on the tag cell itself
        if contains and fidx < 0 and contains not in s:
            continue
        # AF attribute path? keep full string (contains '|' and leading backslashes)
        if '|' in s or s.startswith('\\'):
            token = s
        else:
            # PI tag text: capture the first whitespace-separated token
            token = s.split()[0]
        if token not in seen:
            seen.add(token)
            tags.append(token)
    wb.close()
    return tags


def main() -> int:
    ap = argparse.ArgumentParser(description="Extract simple PI tags from a specific Excel column")
    ap.add_argument("--xlsx", type=Path, required=True)
    ap.add_argument("--out", type=Path, required=True)
    ap.add_argument("--tag-col", type=str, default="D")
    ap.add_argument("--sheet", type=str, default=None)
    ap.add_argument("--contains", type=str, default=None, help="Substring to match (on --filter-col if provided, else on tag cell)")
    ap.add_argument("--filter-col", type=str, default=None, help="Optional column to apply --contains filter (e.g., A)")
    args = ap.parse_args()

    tags = extract(args.xlsx, args.tag_col, sheet=args.sheet, contains=args.contains, filter_col=args.filter_col)
    args.out.parent.mkdir(parents=True, exist_ok=True)
    args.out.write_text("\n".join(tags) + ("\n" if tags else ""), encoding="utf-8")
    print(f"Extracted {len(tags)} tags -> {args.out}")
    if tags:
        print("Sample:", tags[:10])
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
