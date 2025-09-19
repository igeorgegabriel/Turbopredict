#!/usr/bin/env python3
from __future__ import annotations

import sys
from pathlib import Path

project_root = Path(__file__).resolve().parents[1]
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

from pi_monitor.parquet_database import ParquetDatabase


def main():
    db = ParquetDatabase()
    status = db.get_database_status()
    print('Parquet Database Status')
    print('=' * 60)
    print('Processed dir:', status.get('processed_directory'))
    print('Total files  :', status.get('total_files'))
    print('Total size GB:', status.get('total_size_gb'))
    print('\nUnits:')
    print(f"{'Unit':<10} {'Files':>5} {'Size(MB)':>10} {'Records':>12} {'Age(hrs)':>10} {'Tags':>6}")
    for u in status.get('units', []):
        age = u.get('data_age_hours')
        tags = u.get('unique_tags')
        print(f"{u['unit']:<10} {u['files']:>5} {u['size_mb']:>10.1f} {u['records']:>12} {age if age is not None else 'n/a':>10} {tags:>6}")
    return 0


if __name__ == '__main__':
    raise SystemExit(main())

