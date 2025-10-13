#!/usr/bin/env python3
"""Batch processing version for K-12-01 to handle slow PI server response."""
from __future__ import annotations

from pathlib import Path
import sys
import time

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))

from pi_monitor.batch import build_unit_from_tags
from pi_monitor.clean import dedup_parquet


def read_tags(path: Path) -> list[str]:
    tags: list[str] = []
    for line in Path(path).read_text(encoding="utf-8").splitlines():
        s = line.strip()
        if not s or s.startswith("#"):
            continue
        tags.append(s)
    return tags


def batch_process_tags(
    tags: list[str],
    xlsx: Path,
    plant: str,
    unit: str,
    server: str,
    start: str,
    end: str,
    step: str,
    batch_size: int = 10
) -> list[Path]:
    """Process tags in smaller batches to avoid timeout issues."""
    import pandas as pd
    import pyarrow.parquet as pq

    temp_dir = PROJECT_ROOT / "tmp" / "batches"
    temp_dir.mkdir(parents=True, exist_ok=True)

    batch_files = []
    total_batches = (len(tags) + batch_size - 1) // batch_size

    print(f"\n{'='*80}")
    print(f"BATCH PROCESSING: {len(tags)} tags in {total_batches} batches of ~{batch_size} tags")
    print(f"{'='*80}\n")

    for i in range(0, len(tags), batch_size):
        batch_num = (i // batch_size) + 1
        batch_tags = tags[i:i + batch_size]

        print(f"\n[Batch {batch_num}/{total_batches}] Processing {len(batch_tags)} tags...")
        print(f"Tags: {', '.join(batch_tags[:3])}{'...' if len(batch_tags) > 3 else ''}")
        print("-" * 80)

        batch_file = temp_dir / f"batch_{batch_num:03d}.parquet"

        try:
            start_time = time.time()

            build_unit_from_tags(
                xlsx,
                batch_tags,
                batch_file,
                plant=plant,
                unit=unit,
                server=server,
                start=start,
                end=end,
                step=step,
                work_sheet="DL_WORK",
                settle_seconds=1.5,
                visible=False,  # Run in background for speed
            )

            elapsed = time.time() - start_time

            if batch_file.exists() and batch_file.stat().st_size > 0:
                batch_files.append(batch_file)
                print(f"[OK] Batch {batch_num} completed in {elapsed:.1f}s")
            else:
                print(f"[X] Batch {batch_num} failed: No data written")

        except Exception as e:
            print(f"[X] Batch {batch_num} failed: {e}")
            continue

    return batch_files


def merge_batch_files(batch_files: list[Path], output: Path) -> Path:
    """Merge all batch Parquet files into a single master file."""
    import pandas as pd
    import pyarrow.parquet as pq
    import pyarrow as pa

    print(f"\n{'='*80}")
    print(f"MERGING: {len(batch_files)} batch files into master Parquet")
    print(f"{'='*80}\n")

    dfs = []
    for batch_file in batch_files:
        try:
            df = pd.read_parquet(batch_file)
            dfs.append(df)
            print(f"[OK] Loaded {batch_file.name}: {len(df):,} records")
        except Exception as e:
            print(f"[X] Failed to load {batch_file.name}: {e}")

    if not dfs:
        raise SystemExit("No batch files to merge!")

    # Combine all dataframes
    master_df = pd.concat(dfs, ignore_index=True)

    # Sort by time and tag
    if 'time' in master_df.columns:
        master_df = master_df.sort_values(['time', 'tag'] if 'tag' in master_df.columns else ['time'])

    print(f"\n[OK] Total records: {len(master_df):,}")

    # Write master file
    output.parent.mkdir(parents=True, exist_ok=True)
    master_df.to_parquet(output, index=False, compression='snappy')

    print(f"[OK] Master file written: {output}")
    print(f"  Size: {output.stat().st_size / (1024*1024):.2f} MB")

    # Cleanup batch files
    for batch_file in batch_files:
        try:
            batch_file.unlink()
        except Exception:
            pass

    return output


def main() -> int:
    xlsx = PROJECT_ROOT / "excel" / "PCFS" / "PCFS_Automation.xlsx"
    tags_file = PROJECT_ROOT / "config" / "tags_k12_01.txt"
    out_parquet = PROJECT_ROOT / "data" / "processed" / "K-12-01_1y_0p1h.parquet"

    plant = "PCFS"
    unit = "K-12-01"
    server = r"\\PTSG-1MMPDPdb01"
    start = "-1y"
    end = "*"
    step = "-0.1h"
    batch_size = 10  # Process 10 tags at a time

    tags = read_tags(tags_file)
    if not tags:
        raise SystemExit(f"No tags found in {tags_file}")

    print(f"Building Parquet for {plant} {unit} with {len(tags)} tags...")
    print(f"Using batch size: {batch_size} tags per batch")

    # Process in batches
    batch_files = batch_process_tags(
        tags, xlsx, plant, unit, server, start, end, step, batch_size
    )

    if not batch_files:
        raise SystemExit("No batches completed successfully!")

    # Merge all batches
    merge_batch_files(batch_files, out_parquet)

    # Deduplicate
    print(f"\n{'='*80}")
    print("DEDUPLICATION")
    print(f"{'='*80}\n")
    dedup = dedup_parquet(out_parquet)
    print(f"[OK] Master (dedup) ready: {dedup}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
