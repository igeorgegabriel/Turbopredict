#!/usr/bin/env python3
"""
Cleanup utility for TURBOPREDICT X PROTEAN dummy and backup files
Prevents disk space exhaustion from Excel automation temporary files
"""

import os
import sys
from pathlib import Path
import time
from datetime import datetime, timedelta
import logging

# Add the parent directory to sys.path so we can import pi_monitor
sys.path.insert(0, str(Path(__file__).parent.parent))

from pi_monitor.excel_file_manager import ExcelFileManager

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def cleanup_all_excel_directories(max_age_hours: int = 24, dry_run: bool = False) -> dict:
    """Clean up dummy and backup files across all Excel directories.

    Args:
        max_age_hours: Maximum age in hours for files to keep
        dry_run: If True, only report what would be cleaned without actually deleting

    Returns:
        Dictionary with cleanup statistics
    """
    # Excel directories to clean
    excel_directories = [
        Path("excel/PCFS"),
        Path("excel/PCMSB"),
        Path("excel/ABFSB"),
        Path("excel/MLNG"),
        Path("excel/PFLNG1"),
        Path("excel/PFLNG2"),
        Path("excel"),  # Root excel directory
    ]

    stats = {
        'directories_processed': 0,
        'total_files_found': 0,
        'files_cleaned': 0,
        'space_reclaimed_mb': 0,
        'errors': []
    }

    cutoff_time = time.time() - (max_age_hours * 3600)
    cutoff_datetime = datetime.fromtimestamp(cutoff_time)

    print(f"{'DRY RUN - ' if dry_run else ''}Cleaning files older than {cutoff_datetime}")
    print("=" * 60)

    for excel_dir in excel_directories:
        if not excel_dir.exists():
            continue

        stats['directories_processed'] += 1
        print(f"\nProcessing directory: {excel_dir}")

        # Patterns for temporary files to clean
        cleanup_patterns = [
            "*_dummy_*.xlsx",
            "*_backup_*.xlsx",
            "*_temp_*.xlsx",
            "*_working_*.xlsx"
        ]

        dir_cleaned = 0
        dir_space = 0

        for pattern in cleanup_patterns:
            temp_files = list(excel_dir.glob(pattern))

            for temp_file in temp_files:
                stats['total_files_found'] += 1

                try:
                    file_mtime = temp_file.stat().st_mtime
                    file_size = temp_file.stat().st_size

                    if file_mtime < cutoff_time:
                        file_age = datetime.fromtimestamp(file_mtime)
                        size_mb = file_size / (1024 * 1024)

                        print(f"  {'[DRY RUN] ' if dry_run else ''}Cleaning: {temp_file.name} "
                              f"({size_mb:.1f}MB, created {file_age})")

                        if not dry_run:
                            temp_file.unlink()

                        stats['files_cleaned'] += 1
                        stats['space_reclaimed_mb'] += size_mb
                        dir_cleaned += 1
                        dir_space += size_mb

                except Exception as e:
                    error_msg = f"Failed to clean {temp_file}: {e}"
                    print(f"  ERROR: {error_msg}")
                    stats['errors'].append(error_msg)

        if dir_cleaned > 0:
            print(f"  Directory total: {dir_cleaned} files, {dir_space:.1f}MB reclaimed")
        else:
            print("  No old files to clean in this directory")

    return stats


def check_disk_usage_alert(threshold_gb: float = 5.0) -> bool:
    """Check if available disk space is below threshold and issue alert.

    Args:
        threshold_gb: Alert threshold in gigabytes

    Returns:
        True if disk space is low
    """
    try:
        import shutil
        total, used, free = shutil.disk_usage(".")

        free_gb = free / (1024**3)
        total_gb = total / (1024**3)
        used_pct = (used / total) * 100

        print(f"Disk usage: {used_pct:.1f}% used, {free_gb:.1f}GB free of {total_gb:.1f}GB total")

        if free_gb < threshold_gb:
            print(f"⚠️  LOW DISK SPACE ALERT: Only {free_gb:.1f}GB free (threshold: {threshold_gb}GB)")
            return True

        return False

    except Exception as e:
        print(f"Could not check disk usage: {e}")
        return False


def main():
    """Main cleanup function with command line options."""
    import argparse

    parser = argparse.ArgumentParser(
        description="Clean up TURBOPREDICT X PROTEAN temporary Excel files"
    )
    parser.add_argument(
        "--max-age",
        type=int,
        default=24,
        help="Maximum age in hours for files to keep (default: 24)"
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would be cleaned without actually deleting files"
    )
    parser.add_argument(
        "--aggressive",
        action="store_true",
        help="Use aggressive cleanup (12 hour threshold instead of 24)"
    )
    parser.add_argument(
        "--disk-check",
        action="store_true",
        help="Check disk usage and show alerts"
    )

    args = parser.parse_args()

    # Adjust max age for aggressive cleanup
    max_age = 12 if args.aggressive else args.max_age

    print("TURBOPREDICT X PROTEAN - Dummy File Cleanup Utility")
    print("=" * 60)

    # Check disk usage if requested
    if args.disk_check:
        low_space = check_disk_usage_alert()
        if low_space and not args.aggressive:
            print("Enabling aggressive cleanup due to low disk space...")
            max_age = 12
        print()

    # Perform cleanup
    stats = cleanup_all_excel_directories(max_age_hours=max_age, dry_run=args.dry_run)

    # Print summary
    print("\n" + "=" * 60)
    print("CLEANUP SUMMARY:")
    print(f"Directories processed: {stats['directories_processed']}")
    print(f"Total temp files found: {stats['total_files_found']}")
    print(f"Files {'would be ' if args.dry_run else ''}cleaned: {stats['files_cleaned']}")
    print(f"Space {'would be ' if args.dry_run else ''}reclaimed: {stats['space_reclaimed_mb']:.1f}MB "
          f"({stats['space_reclaimed_mb']/1024:.2f}GB)")

    if stats['errors']:
        print(f"\nErrors encountered: {len(stats['errors'])}")
        for error in stats['errors']:
            print(f"  - {error}")

    if args.dry_run and stats['files_cleaned'] > 0:
        print(f"\nTo actually perform cleanup, run without --dry-run flag")
        print(f"Command: python scripts/cleanup_dummy_files.py --max-age {max_age}")

    return 0 if not stats['errors'] else 1


if __name__ == "__main__":
    sys.exit(main())