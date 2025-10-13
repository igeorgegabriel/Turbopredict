#!/usr/bin/env python3
"""
Probe PI DataLink tag naming variants for a sample tag on a given server.

Usage:
  python scripts/test_tag_variants.py \
      --xlsx excel/ABFSB/ABFSB_Automation_Master.xlsx \
      --tag ABF.07-MT001.FI-07054.PV \
      --server "\\PTSG-1MMPDPdb01"

The script will:
- Open a working copy of the workbook (no changes to the original)
- Try several tag-name variants (unit code patterns, optional PRISM prefix,
  suffix underscore vs dot) using PI DataLink PISampDat
- Report which variant(s) spill data and how many rows
"""

from __future__ import annotations

from pathlib import Path
import argparse
import shutil
import time
from typing import List, Tuple

try:
    import xlwings as xw
except Exception as e:  # pragma: no cover
    raise SystemExit(f"xlwings not available: {e}")


def _build_variants(sample_tag: str) -> List[str]:
    """Return a list of plausible tag variants for ABF namespace.

    Heuristics applied on the middle "unit" segment and PRISM prefix.
    """
    s = sample_tag.strip()
    variants: List[str] = []

    # Base
    variants.append(s)

    # Split into parts: PREFIX.UNIT.REST (best-effort)
    parts = s.split('.')
    if len(parts) < 3:
        return list(dict.fromkeys(variants))

    prefix, unit, rest = parts[0], parts[1], '.'.join(parts[2:])

    # Common unit representations for ABF 07-MT01-K001
    unit_forms = [unit]
    if unit.upper() == '07-MT001':
        unit_forms += ['07-MT01-K001', '07-MT01K001']
    elif unit.upper() == '07-MT01-K001':
        unit_forms += ['07-MT001', '07-MT01K001']

    # Allow PRISM prefix
    prefixes = [prefix]
    if prefix.upper() == 'ABF':
        prefixes = ['ABF', 'PRISM.ABF']

    # Suffix separator variations (e.g., FI-07054.PV vs FI-07054_PV)
    rest_forms = [rest]
    if rest.endswith('.PV'):
        rest_forms.append(rest.replace('.PV', '_PV'))
    if rest.endswith('_PV'):
        rest_forms.append(rest.replace('_PV', '.PV'))

    # Compose variants
    for px in prefixes:
        for uf in unit_forms:
            for rf in rest_forms:
                variants.append(f"{px}.{uf}.{rf}")

    # Deduplicate and keep order
    seen = set()
    out: List[str] = []
    for v in variants:
        if v not in seen:
            seen.add(v)
            out.append(v)
    return out


def _try_formula(sht, tag: str, start: str, end: str, step: str, server: str) -> Tuple[int, str]:
    """Write a PISampDat formula directly and return (rows, message)."""
    # Direct formula string with properly escaped backslashes for server
    # Excel expects two backslashes; Python string must therefore contain two backslashes
    server_lit = server
    if server_lit.startswith('\\'):
        # ok
        pass
    else:
        server_lit = '\\' + server_lit

    fml = (
        f"=PISampDat(\"{tag}\",\"{start}\",\"{end}\",\"{step}\",1,\"{server_lit}\")"
    )

    # Reset a small sandbox area to avoid residue from prior tests
    sht.clear()
    # Put formula at A2
    sht.range('A1').value = 'TEST'
    sht.range('A2').formula = fml

    # Calculate
    try:
        sht.book.api.RefreshAll()
    except Exception:
        pass
    try:
        sht.api.Calculate()
    except Exception:
        pass
    time.sleep(2.0)

    # Check spill
    try:
        data = sht.range('A2').expand().value
    except Exception:
        data = None

    msg = str(sht.range('A2').value)
    # Normalize to list-of-lists
    lol = []
    if isinstance(data, list):
        if data and not isinstance(data[0], (list, tuple)):
            lol = [data]
        else:
            lol = data  # type: ignore

    # Count only rows that look like time/value pairs and are not error strings
    valid_rows = 0
    for row in lol:
        if not row:
            continue
        v0 = row[0]
        v1 = row[1] if len(row) > 1 else None
        if isinstance(v0, str) and v0.strip().lower() in {"tag not found", "na", "#na"}:
            continue
        # Excel date serials come as float; timestamps can come back as strings; accept both if second value numeric
        if (isinstance(v0, (float, int)) or isinstance(v0, str)) and isinstance(v1, (float, int)):
            valid_rows += 1

    # If the top-left cell shows Tag not found, force rows to 0
    if isinstance(msg, str) and msg.strip().lower() == 'tag not found':
        valid_rows = 0
    return valid_rows, msg


def test_variants(xlsx: Path, sample_tag: str, server: str) -> List[Tuple[str, int, str]]:
    wc = xlsx.with_name(xlsx.stem + '_variant_probe.xlsx')
    try:
        shutil.copy2(str(xlsx), str(wc))
    except Exception:
        wc = xlsx

    results: List[Tuple[str, int, str]] = []
    app = xw.App(visible=False, add_book=False)
    try:
        app.display_alerts = False
        app.screen_updating = False
        app.api.Application.DisplayAlerts = False
        app.api.Application.EnableEvents = False
        wb = app.books.open(str(wc))
        try:
            sht = wb.sheets['DL_VARIANTS']
            sht.clear()
        except Exception:
            sht = wb.sheets.add('DL_VARIANTS')

        variants = _build_variants(sample_tag)
        start, end, step = '-1d', '*', '-0.1h'

        row = 1
        sht.range(row, 1).value = ['TAG', 'ROWS', 'MESSAGE']
        row += 1

        for tag in variants:
            # Clear previous formula area to avoid huge expand reads on next
            sht.range('A2').clear()
            r, msg = _try_formula(sht, tag, start, end, step, server)
            results.append((tag, r, msg))
            sht.range(row, 1).value = [tag, r, msg]
            row += 1

        try:
            wb.save()
        except Exception:
            pass
        wb.close()
    finally:
        app.quit()
        if wc != xlsx:
            try:
                wc.unlink()
            except Exception:
                pass
    return results


def main() -> None:
    ap = argparse.ArgumentParser(description='Probe PI tag naming variants for a sample tag')
    ap.add_argument('--xlsx', type=Path, required=True, help='Workbook to open (working copy is used)')
    ap.add_argument('--tag', type=str, required=True, help='Sample tag, e.g. ABF.07-MT001.FI-07054.PV')
    ap.add_argument('--server', type=str, default='\\PTSG-1MMPDPdb01', help='PI server (two leading backslashes)')
    args = ap.parse_args()

    results = test_variants(args.xlsx, args.tag, args.server)

    print('VARIANT TEST RESULTS')
    print('=' * 80)
    hits = [(t, r, m) for t, r, m in results if r > 0]
    for t, r, m in results:
        status = 'OK' if r > 0 else 'NO DATA'
        print(f"{status:8} rows={r:<6} tag={t} msg={m}")
    print('-' * 80)
    if hits:
        print(f"Working variants: {len(hits)}")
        for t, r, _ in hits[:10]:
            print(f"  {t}  (rows={r})")
    else:
        print('No variants returned data. Confirm tag names on the target server via Tag Search.')


if __name__ == '__main__':
    main()
