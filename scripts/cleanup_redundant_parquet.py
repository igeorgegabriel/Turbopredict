#!/usr/bin/env python3
"""
Safe cleanup script for redundant/stale parquet files.

This script identifies and removes old parquet files that cause stale data issues,
while keeping active files and creating backups.
"""

from pathlib import Path
import shutil
from datetime import datetime
import os


def cleanup_redundant_c02001_files(dry_run=True):
    """
    Safely clean up redundant C-02001 parquet files.

    Strategy:
    1. Identify active files (recently modified, being refreshed)
    2. Identify stale files (old lookback periods no longer in use)
    3. Create backup directory
    4. Move (not delete) old files to backup
    5. Verify system still works

    Args:
        dry_run: If True, only show what would be done without actually doing it
    """
    processed_dir = Path("data/processed")
    backup_dir = Path("data/backup_parquet") / datetime.now().strftime("%Y%m%d_%H%M%S")

    # Find all C-02001 files
    c02001_files = list(processed_dir.glob("C-02001*.parquet"))

    if not c02001_files:
        print("No C-02001 parquet files found.")
        return

    print("=" * 70)
    print("C-02001 PARQUET FILE CLEANUP")
    print("=" * 70)
    print(f"Mode: {'DRY RUN (no changes)' if dry_run else 'LIVE (will move files)'}")
    print(f"Found {len(c02001_files)} C-02001 parquet files\n")

    # Categorize files
    active_files = []
    stale_files = []
    temp_files = []

    for f in c02001_files:
        mtime = datetime.fromtimestamp(f.stat().st_mtime)
        size_mb = f.stat().st_size / (1024 * 1024)
        age_hours = (datetime.now() - mtime).total_seconds() / 3600

        # Categorization logic
        if 'dedup_refreshed' in f.name or 'temp' in f.name.lower():
            category = 'TEMP'
            temp_files.append(f)
        elif '1p5y' in f.name and age_hours > 24:  # Old 1.5-year lookback, not modified in 24h
            category = 'STALE'
            stale_files.append(f)
        elif '1y' in f.name and age_hours < 24:  # Current 1-year lookback, recently modified
            category = 'ACTIVE'
            active_files.append(f)
        elif age_hours > 48:  # Any file not modified in 2 days is suspicious
            category = 'STALE'
            stale_files.append(f)
        else:
            category = 'ACTIVE'
            active_files.append(f)

        print(f"[{category:6}] {f.name}")
        print(f"         Size: {size_mb:6.1f}MB  Modified: {mtime.strftime('%Y-%m-%d %H:%M')}  Age: {age_hours:.1f}h")

    print("\n" + "=" * 70)
    print(f"Summary:")
    print(f"  ACTIVE files (keep): {len(active_files)}")
    print(f"  STALE files (remove): {len(stale_files)}")
    print(f"  TEMP files (remove): {len(temp_files)}")

    files_to_remove = stale_files + temp_files

    if not files_to_remove:
        print("\nNo files need cleanup. All files are active.")
        return

    print("\n" + "=" * 70)
    if dry_run:
        print("DRY RUN - Would move these files to backup:")
        for f in files_to_remove:
            print(f"  {f.name} -> {backup_dir.name}/{f.name}")
        print("\nTo actually perform cleanup, run with dry_run=False")
    else:
        # Create backup directory
        backup_dir.mkdir(parents=True, exist_ok=True)
        print(f"Created backup directory: {backup_dir}")

        # Move files to backup
        moved_count = 0
        for f in files_to_remove:
            try:
                backup_path = backup_dir / f.name
                shutil.move(str(f), str(backup_path))
                print(f"✓ Moved: {f.name} → backup/")
                moved_count += 1
            except Exception as e:
                print(f"✗ Failed to move {f.name}: {e}")

        print(f"\n{'=' * 70}")
        print(f"Cleanup complete: {moved_count}/{len(files_to_remove)} files moved to backup")
        print(f"Backup location: {backup_dir}")
        print(f"\nActive files remaining:")
        for f in active_files:
            if f.exists():  # Double-check it still exists
                print(f"  ✓ {f.name}")

        print("\n" + "=" * 70)
        print("IMPORTANT: Test the system now!")
        print("1. Run: python turbopredict.py (Option 1 - Scan)")
        print("2. Verify C-02001 shows as FRESH")
        print("3. If issues occur, restore from backup:")
        print(f"   Move files from {backup_dir} back to {processed_dir}")
        print("=" * 70)


if __name__ == "__main__":
    print("Starting C-02001 cleanup script...\n")

    # First, run in dry-run mode to preview
    cleanup_redundant_c02001_files(dry_run=True)

    # Prompt user to confirm
    print("\n" + "="*70)
    try:
        response = input("\nProceed with cleanup? (yes/no): ").strip().lower()
        if response == 'yes':
            print("\nProceeding with cleanup...")
            cleanup_redundant_c02001_files(dry_run=False)
        else:
            print("Cleanup cancelled.")
    except (EOFError, KeyboardInterrupt):
        print("\nCleanup cancelled.")
