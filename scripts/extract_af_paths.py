#!/usr/bin/env python3
"""
Extract AF attribute paths used in PI DataLink formulas from an Excel workbook
and write them to a text file (one per line).

Supports PI DataLink functions commonly used with AF paths:
 - PICurrVal("\\AFHOST\\DB\\Elem\\Attr|SubAttr", ...)
 - PICompDat("...", ...)
 - PISampDat("...", ...)

Usage (examples):
  python scripts/extract_af_paths.py \
      --xlsx "excel/ABFSB/ABF LIMIT REVIEW (CURRENT).xlsx" \
      --filter "21-K002" \
      --out config/tags_abf_21k002.txt
"""

from __future__ import annotations

from pathlib import Path
import argparse
import re

try:
    import openpyxl  # type: ignore
except Exception as e:  # pragma: no cover
    raise SystemExit(f"openpyxl is required: {e}")


PI_FUNCS = ("PICurrVal", "PICompDat", "PISampDat", "PIData")


def extract_af_paths(xlsx: Path, unit_filter: str | None = None) -> list[str]:
    wb = openpyxl.load_workbook(xlsx, data_only=False, read_only=True)
    found: set[str] = set()

    # Regex: first quoted string inside the PI function call
    # e.g., PICurrVal("\\HOST\\DB\\Elem\\Attr|Path", ...)
    pat = re.compile(r"^\s*(?:" + "|".join(PI_FUNCS) + r")\s*\(\s*\"([^\"]+)\"", re.IGNORECASE)

    try:
        for ws in wb.worksheets:
            for row in ws.iter_rows():
                for cell in row:
                    v = cell.value
                    if not isinstance(v, str):
                        continue
                    s = v.strip()
                    # Fast prefilter to reduce regex checks
                    if not any(s.upper().startswith(fn.upper()) for fn in PI_FUNCS):
                        continue
                    m = pat.match(s)
                    if not m:
                        continue
                    path = m.group(1).strip()
                    # Keep only AF attribute-like paths that contain a '|'
                    if '|' not in path:
                        continue
                    if unit_filter and unit_filter not in path:
                        continue
                    found.add(path)
    finally:
        try:
            wb.close()
        except Exception:
            pass

    # Stable order
    return sorted(found)


def main() -> int:
    ap = argparse.ArgumentParser(description="Extract AF attribute paths from PI DataLink formulas")
    ap.add_argument("--xlsx", required=True, type=Path, help="Path to Excel workbook")
    ap.add_argument("--filter", default=None, help="Substring filter (e.g., unit id like 21-K002)")
    ap.add_argument("--out", required=True, type=Path, help="Output tag file path")
    args = ap.parse_args()

    paths = extract_af_paths(args.xlsx, args.filter)
    args.out.parent.mkdir(parents=True, exist_ok=True)
    with open(args.out, "w", encoding="utf-8") as f:
        for p in paths:
            f.write(p + "\n")

    print(f"Extracted {len(paths)} AF path(s) to {args.out}")
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())

