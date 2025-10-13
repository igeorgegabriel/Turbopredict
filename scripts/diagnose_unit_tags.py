#!/usr/bin/env python3
"""Diagnose PI DataLink access for any unit/tag list.

Usage examples:
  python scripts/diagnose_unit_tags.py --unit K-12-01 --plant PCFS \
      --tags-file config/tags_k12_01.txt --excel excel/PCFS/PCFS_Automation.xlsx

  python scripts/diagnose_unit_tags.py --unit K-16-01 --plant PCFS \
      --tags-file config/tags_k16_01.txt --excel excel/PCFS/PCFS_Automation.xlsx
"""
from __future__ import annotations

from pathlib import Path
import argparse
import sys
import time
import os

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))

from pi_monitor.batch import build_unit_from_tags


def read_tags(path: Path) -> list[str]:
    tags: list[str] = []
    for line in Path(path).read_text(encoding="utf-8").splitlines():
        s = line.strip()
        if not s or s.startswith("#"):
            continue
        tags.append(s)
    return tags


def test_individual_tags(tags: list[str], xlsx: Path, plant: str, unit: str, server: str,
                         limit: int | None = 10) -> dict:
    print(f"\n{'='*80}")
    print(f"DIAGNOSTIC: Testing {len(tags) if not limit else min(len(tags), limit)} tags individually for {unit}")
    print(f"{'='*80}\n")

    results = {
        'success': [],
        'timeout': [],
        'no_data': [],
        'error': []
    }

    # Use robust defaults while diagnosing
    os.environ.setdefault('PI_FETCH_TIMEOUT', '180')
    os.environ.setdefault('PI_FETCH_LINGER', '30')
    os.environ.setdefault('EXCEL_CALC_MODE', 'full')

    test_set = tags if limit is None else tags[:limit]
    for i, tag in enumerate(test_set, 1):
        print(f"\n[{i}/{len(test_set)}] Testing tag: {tag}")
        print("-" * 80)

        try:
            temp_out = PROJECT_ROOT / "tmp" / f"diag_{unit}_{tag.replace('.', '_')}.parquet"
            temp_out.parent.mkdir(parents=True, exist_ok=True)

            t0 = time.time()

            build_unit_from_tags(
                xlsx=xlsx,
                tags=[tag],
                out_parquet=temp_out,
                plant=plant,
                unit=unit,
                server=server,
                start="-2h",
                end="*",
                step="-0.1h",
                work_sheet="DL_WORK",
                settle_seconds=2.0,
                visible=True,
            )

            elapsed = time.time() - t0

            if temp_out.exists() and temp_out.stat().st_size > 0:
                results['success'].append((tag, elapsed))
                print(f"[OK] SUCCESS: {tag} ({elapsed:.1f}s)")
            else:
                results['no_data'].append(tag)
                print(f"[X] NO DATA: {tag} ({elapsed:.1f}s)")

            if temp_out.exists():
                temp_out.unlink(missing_ok=True)

        except TimeoutError:
            results['timeout'].append(tag)
            print(f"[X] TIMEOUT: {tag}")
        except Exception as e:
            results['error'].append((tag, str(e)))
            print(f"[X] ERROR: {tag} - {e}")

    print(f"\n{'='*80}")
    print("DIAGNOSTIC SUMMARY")
    print(f"{'='*80}\n")
    total = len(test_set)
    print(f"[OK] Success: {len(results['success'])}/{total} tags")
    if results['success']:
        for tag, elapsed in results['success']:
            print(f"  - {tag} ({elapsed:.1f}s)")
    if results['timeout']:
        print(f"\n[X] Timeout: {len(results['timeout'])} tags")
        for tag in results['timeout']:
            print(f"  - {tag}")
    if results['no_data']:
        print(f"\n[X] No Data: {len(results['no_data'])} tags")
        for tag in results['no_data']:
            print(f"  - {tag}")
    if results['error']:
        print(f"\n[X] Errors: {len(results['error'])} tags")
        for tag, err in results['error']:
            print(f"  - {tag}: {err}")

    return results


def main() -> int:
    ap = argparse.ArgumentParser(description="Diagnose PI DataLink access for a unit/tag list")
    ap.add_argument('--unit', required=True, help='Unit name, e.g., K-12-01')
    ap.add_argument('--plant', required=True, help='Plant name, e.g., PCFS')
    ap.add_argument('--tags-file', required=True, type=str, help='Path to tags file')
    ap.add_argument('--excel', required=True, type=str, help='Path to Excel workbook to use')
    ap.add_argument('--server', default=r"\\PTSG-1MMPDPdb01", help='PI Server path or alias')
    ap.add_argument('--limit', type=int, default=10, help='Number of tags to test (default 10)')
    args = ap.parse_args()

    tags = read_tags(Path(args.tags_file))
    if not tags:
        print(f"No tags found in {args.tags_file}")
        return 2

    results = test_individual_tags(tags, Path(args.excel), args.plant, args.unit, args.server, limit=args.limit)
    # Return non-zero if nothing succeeded to simplify CI/observability
    return 0 if results['success'] else 1


if __name__ == '__main__':
    raise SystemExit(main())

