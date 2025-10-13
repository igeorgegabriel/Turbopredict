#!/usr/bin/env python3
"""
Test XT-07002 tags selectively - identify working vs problematic tags
"""

import os
import sys
from pathlib import Path
import time

# Set timeout
os.environ['PI_FETCH_TIMEOUT'] = '60'

PROJECT_ROOT = Path(__file__).parent
sys.path.insert(0, str(PROJECT_ROOT))

from pi_monitor.batch import _fetch_single
import xlwings as xw

def test_xt07002_tags():
    """Test each XT-07002 tag individually to identify working ones"""

    print("=== XT-07002 Selective Tag Testing ===")

    # Read all tags
    tags_file = PROJECT_ROOT / "config" / "tags_pcmsb_xt07002.txt"
    all_tags = []
    for line in tags_file.read_text().splitlines():
        line = line.strip()
        if line and not line.startswith('#'):
            all_tags.append(line)

    xlsx = PROJECT_ROOT / "excel" / "PCMSB" / "PCMSB_Automation.xlsx"

    working_tags = []
    timeout_tags = []
    error_tags = []

    print(f"Testing {len(all_tags)} tags individually...")

    # Known working tag from your screenshot
    known_working = 'PCM.XT-07002.070GZI8402.PV'

    with xw.App(visible=False) as app:
        wb = app.books.open(xlsx)

        # Test known working tag first
        print(f"\n1. Testing known working tag: {known_working}")
        try:
            df = _fetch_single(
                wb, 'DL_WORK', known_working, r"\\PTSG-1MMPDPdb01",
                '-2h', '*', '-0.1h', settle_seconds=2.0
            )
            if len(df) > 0:
                print(f"   SUCCESS: {len(df)} rows")
                working_tags.append(known_working)
            else:
                print(f"   Empty data")
                timeout_tags.append(known_working)
        except Exception as e:
            print(f"   ERROR: {e}")
            error_tags.append(known_working)

        # Test other tags (limit to first 10 for speed)
        test_tags = [tag for tag in all_tags[:10] if tag != known_working]

        for i, tag in enumerate(test_tags, 2):
            print(f"\n{i}. Testing: {tag}")
            try:
                start_time = time.time()
                df = _fetch_single(
                    wb, 'DL_WORK', tag, r"\\PTSG-1MMPDPdb01",
                    '-2h', '*', '-0.1h', settle_seconds=2.0
                )
                elapsed = time.time() - start_time

                if len(df) > 0:
                    print(f"   SUCCESS: {len(df)} rows in {elapsed:.1f}s")
                    working_tags.append(tag)
                else:
                    print(f"   Empty data in {elapsed:.1f}s")
                    timeout_tags.append(tag)

            except Exception as e:
                print(f"   ERROR: {e}")
                error_tags.append(tag)

        wb.close()

    # Results summary
    print(f"\n=== Results Summary ===")
    print(f"Working tags: {len(working_tags)}")
    print(f"Timeout/empty tags: {len(timeout_tags)}")
    print(f"Error tags: {len(error_tags)}")

    if working_tags:
        print(f"\nWorking tags:")
        for tag in working_tags:
            print(f"  - {tag}")

    if timeout_tags:
        print(f"\nTimeout/empty tags:")
        for tag in timeout_tags:
            print(f"  - {tag}")

    if error_tags:
        print(f"\nError tags:")
        for tag in error_tags:
            print(f"  - {tag}")

    # Create filtered tag file
    if working_tags:
        filtered_file = PROJECT_ROOT / "config" / "tags_pcmsb_xt07002_working.txt"
        with open(filtered_file, 'w') as f:
            f.write("# Working XT-07002 tags (auto-generated)\n")
            for tag in working_tags:
                f.write(f"{tag}\n")
        print(f"\nCreated filtered tag file: {filtered_file}")

        # Try building with working tags only
        print(f"\nAttempting build with {len(working_tags)} working tags...")
        return build_with_working_tags(working_tags)

    return False

def build_with_working_tags(working_tags):
    """Build parquet with only the working tags"""

    from pi_monitor.batch import build_unit_from_tags

    xlsx = PROJECT_ROOT / "excel" / "PCMSB" / "PCMSB_Automation.xlsx"
    out_parquet = PROJECT_ROOT / "data" / "processed" / "XT-07002_working_1y_0p1h.parquet"

    try:
        result = build_unit_from_tags(
            xlsx,
            working_tags,
            out_parquet,
            plant="PCMSB",
            unit="XT-07002",
            server=r"\\PTSG-1MMPDPdb01",
            start="-1y",
            end="*",
            step="-0.1h",
            work_sheet="DL_WORK",
            settle_seconds=3.0,
            visible=False,
            use_working_copy=True
        )

        print(f"SUCCESS: Built {result}")

        # Deduplicate
        try:
            from pi_monitor.clean import dedup_parquet
            dedup_file = dedup_parquet(result)
            print(f"Deduplicated: {dedup_file}")
        except Exception as e:
            print(f"Dedup warning: {e}")

        return True

    except Exception as e:
        print(f"Build failed: {e}")
        return False

if __name__ == "__main__":
    success = test_xt07002_tags()
    print(f"\nFinal result: {'SUCCESS' if success else 'FAILED'}")