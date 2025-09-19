"""
Create/refresh a sheet with PI DataLink PISampDat formulas for a list of tags.

Usage examples:
  python scripts/seed_datalink_formulas.py --xlsx excel/PCFS_Automation_1.xlsx \
      --sheet DL_K12_01 --tags config/tags_k12_01.txt \
      --server "\\PTSG-1MMPDPdb01" --start "-1y" --end "*" --step "-0.1h"

Notes:
  - Requires Excel + PI DataLink add-in installed locally.
  - The formula used is: =PISampDat(<tag>, <start>, <end>, <step>, 1, <server>)
    The '1' argument requests timestamps in the result (DataLink setting).
"""

from __future__ import annotations

from pathlib import Path
import argparse
import xlwings as xw


def read_tags(path: Path) -> list[str]:
    tags: list[str] = []
    for line in Path(path).read_text(encoding="utf-8").splitlines():
        s = line.strip()
        if not s or s.startswith("#"):
            continue
        tags.append(s)
    return tags


def _col_letter(n: int) -> str:
    s = ""
    while n > 0:
        n, r = divmod(n - 1, 26)
        s = chr(65 + r) + s
    return s


def seed_sheet(xlsx: Path, sheet_name: str, tags_file: Path, server: str, start: str, end: str, step: str) -> None:
    tags = read_tags(tags_file)
    if not tags:
        raise SystemExit(f"No tags found in {tags_file}")

    app = xw.App(visible=False, add_book=False)
    try:
        app.display_alerts = False
        app.screen_updating = False
        wb = app.books.open(str(xlsx))

        # Create or clear sheet
        try:
            sht = wb.sheets[sheet_name]
            sht.clear()
        except Exception:
            sht = wb.sheets.add(sheet_name, after=wb.sheets[-1])

        # Headers and params
        sht.range("A1").value = [
            ["TAG", "START", "END", "STEP", "SERVER", "RESULT (Time & Value spill)"],
        ]
        sht.range("B2").value = start
        sht.range("C2").value = end
        sht.range("D2").value = step
        sht.range("E2").value = server

        # Tag list (row 2, horizontally), so each formula can spill downward without overlap
        sht.range("A2").value = tags

        # Write PISampDat formulas per tag (one per column). Results spill down 2 columns wide (time,value)
        col = 6  # column F
        for idx in range(len(tags)):
            # Header for this tag above the result pair
            sht.range((1, col)).value = f"Result for tag {idx+1}"
            # Place formula per tag; spill 2 columns (time,value) downward
            addr = f"${_col_letter(1 + idx)}$2"
            formula = f"=PISampDat({addr},$B$2,$C$2,$D$2,1,$E$2)"
            sht.range((2, col)).formula = formula
            col += 2  # leave two columns for time+value spill

        # Calculate and save
        app.api.CalculateFull()
        wb.save()
        wb.close()
    finally:
        app.quit()


def main() -> None:
    ap = argparse.ArgumentParser(description="Seed PI DataLink formulas for tag list")
    ap.add_argument("--xlsx", type=Path, required=True)
    ap.add_argument("--sheet", type=str, default="DL_K12_01")
    ap.add_argument("--tags", type=Path, required=True)
    ap.add_argument("--server", type=str, default="\\\\PTSG-1MMPDPdb01")
    ap.add_argument("--start", type=str, default="-1y")
    ap.add_argument("--end", type=str, default="*")
    ap.add_argument("--step", type=str, default="-0.1h")
    args = ap.parse_args()

    seed_sheet(args.xlsx, args.sheet, args.tags, args.server, args.start, args.end, args.step)


if __name__ == "__main__":
    main()
