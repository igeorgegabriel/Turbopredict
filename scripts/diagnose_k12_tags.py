#!/usr/bin/env python3
"""Diagnostic script to test individual K-12-01 PI tags for connectivity issues."""
from __future__ import annotations

from pathlib import Path
import sys
import time

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


def test_individual_tags(tags: list[str], xlsx: Path, plant: str, unit: str, server: str):
    """Test each tag individually to identify which ones are failing."""
    print(f"\n{'='*80}")
    print(f"DIAGNOSTIC: Testing {len(tags)} tags individually for {unit}")
    print(f"{'='*80}\n")

    results = {
        'success': [],
        'timeout': [],
        'no_data': [],
        'error': []
    }

    for i, tag in enumerate(tags, 1):
        print(f"\n[{i}/{len(tags)}] Testing tag: {tag}")
        print("-" * 80)

        try:
            temp_out = PROJECT_ROOT / "tmp" / f"test_{tag.replace('.', '_')}.parquet"
            temp_out.parent.mkdir(parents=True, exist_ok=True)

            start_time = time.time()

            # Test with single tag
            build_unit_from_tags(
                xlsx,
                [tag],
                temp_out,
                plant=plant,
                unit=unit,
                server=server,
                start="-1d",  # Only fetch 1 day for testing
                end="*",
                step="-1h",   # 1 hour intervals
                work_sheet="DL_WORK",
                settle_seconds=1.5,
                visible=False,  # Run in background
            )

            elapsed = time.time() - start_time

            # Check if data was retrieved
            if temp_out.exists() and temp_out.stat().st_size > 0:
                results['success'].append((tag, elapsed))
                print(f"[OK] SUCCESS: {tag} ({elapsed:.1f}s)")
            else:
                results['no_data'].append(tag)
                print(f"[X] NO DATA: {tag} ({elapsed:.1f}s)")

            # Cleanup
            if temp_out.exists():
                temp_out.unlink()

        except TimeoutError:
            results['timeout'].append(tag)
            print(f"[X] TIMEOUT: {tag}")
        except Exception as e:
            results['error'].append((tag, str(e)))
            print(f"[X] ERROR: {tag} - {e}")

    # Print summary
    print(f"\n{'='*80}")
    print("DIAGNOSTIC SUMMARY")
    print(f"{'='*80}\n")

    print(f"[OK] Success: {len(results['success'])}/{len(tags)} tags")
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
    xlsx = PROJECT_ROOT / "excel" / "PCFS" / "PCFS_Automation.xlsx"
    tags_file = PROJECT_ROOT / "config" / "tags_k12_01.txt"

    plant = "PCFS"
    unit = "K-12-01"
    server = r"\\PTSG-1MMPDPdb01"

    tags = read_tags(tags_file)
    if not tags:
        raise SystemExit(f"No tags found in {tags_file}")

    # Test only the first 10 tags to start
    test_tags = tags[:10]
    print(f"Testing first {len(test_tags)} tags from {len(tags)} total")

    results = test_individual_tags(test_tags, xlsx, plant, unit, server)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
