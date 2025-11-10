#!/usr/bin/env python3
"""
Investigate why PCMSB units fail to fetch new data
"""

import sys
import os
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent))

import pandas as pd
import numpy as np
from datetime import datetime, timedelta

from pi_monitor.parquet_database import ParquetDatabase

def investigate_pcmsb_failure():
    """Deep investigation of PCMSB data fetching issues"""

    print("INVESTIGATING PCMSB DATA FETCHING FAILURE")
    print("=" * 60)
    print(f"Investigation Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print()

    # Phase 1: Check PCMSB units current status
    print("PHASE 1: PCMSB UNITS CURRENT STATUS")
    print("-" * 40)

    db = ParquetDatabase()
    all_units = db.get_all_units()

    # Filter PCMSB units (C-units)
    pcmsb_units = [unit for unit in all_units if unit.startswith('C-')]
    print(f"PCMSB units found: {len(pcmsb_units)}")
    print(f"Units: {', '.join(pcmsb_units)}")
    print()

    pcmsb_status = {}
    for unit in pcmsb_units:
        try:
            freshness_info = db.get_data_freshness_info(unit)
            latest_time = freshness_info.get('latest_timestamp')
            data_age_hours = freshness_info.get('data_age_hours', 0)
            total_records = freshness_info.get('total_records', 0)

            pcmsb_status[unit] = {
                'latest_timestamp': latest_time,
                'age_hours': data_age_hours,
                'total_records': total_records,
                'is_stale': data_age_hours > 2.0
            }

            status = "STALE" if data_age_hours > 2.0 else "FRESH"
            latest_str = latest_time.strftime('%Y-%m-%d %H:%M:%S') if latest_time else "None"

            print(f"{unit:<12} | {status:<6} | {data_age_hours:<8.1f}h | {latest_str} | {total_records:,} records")

        except Exception as e:
            print(f"{unit:<12} | ERROR  | Failed to get info: {e}")
            pcmsb_status[unit] = {'error': str(e)}

    # Summary
    stale_units = [unit for unit, info in pcmsb_status.items() if info.get('is_stale', False)]
    fresh_units = [unit for unit, info in pcmsb_status.items() if not info.get('is_stale', True)]

    print(f"\\nSUMMARY:")
    print(f"Fresh PCMSB units: {len(fresh_units)} - {fresh_units}")
    print(f"Stale PCMSB units: {len(stale_units)} - {stale_units}")

    # Phase 2: Check PCMSB Excel file status
    print(f"\\n\\nPHASE 2: PCMSB EXCEL FILE STATUS")
    print("-" * 40)

    excel_dir = Path("excel")
    pcmsb_excel = excel_dir / "PCMSB_Automation.xlsx"

    if pcmsb_excel.exists():
        stat = pcmsb_excel.stat()
        excel_modified = datetime.fromtimestamp(stat.st_mtime)
        excel_age = (datetime.now() - excel_modified).total_seconds() / 3600

        print(f"PCMSB_Automation.xlsx:")
        print(f"  File exists: YES")
        print(f"  Last modified: {excel_modified.strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"  Age: {excel_age:.1f} hours")
        print(f"  Size: {stat.st_size / (1024*1024):.1f} MB")

        # Compare Excel age vs data age
        if stale_units:
            avg_data_age = sum(pcmsb_status[unit]['age_hours'] for unit in stale_units) / len(stale_units)
            print(f"\\n  Excel file age: {excel_age:.1f}h")
            print(f"  Avg PCMSB data age: {avg_data_age:.1f}h")

            if excel_age < avg_data_age:
                print("  ⚠️  Excel file newer than data - refresh may have failed to update parquet")
            elif excel_age > 8:
                print("  ❌ Excel file is stale - needs refresh")
            else:
                print("  ✅ Excel file age looks reasonable")

    else:
        print("PCMSB_Automation.xlsx: FILE MISSING!")
        return

    # Phase 3: Check recent PCMSB refresh activity
    print(f"\\n\\nPHASE 3: PCMSB REFRESH ACTIVITY ANALYSIS")
    print("-" * 40)

    # Check for recent PCMSB backup/dummy files
    now = datetime.now()
    recent_threshold = now - timedelta(hours=8)

    pcmsb_backups = list(excel_dir.glob("PCMSB*backup*.xlsx"))
    pcmsb_dummies = list(excel_dir.glob("PCMSB*dummy*.xlsx"))

    recent_pcmsb_activity = []
    for backup in pcmsb_backups:
        backup_time = datetime.fromtimestamp(backup.stat().st_mtime)
        if backup_time > recent_threshold:
            recent_pcmsb_activity.append(('backup', backup.name, backup_time))

    for dummy in pcmsb_dummies:
        dummy_time = datetime.fromtimestamp(dummy.stat().st_mtime)
        if dummy_time > recent_threshold:
            recent_pcmsb_activity.append(('dummy', dummy.name, dummy_time))

    print(f"Recent PCMSB refresh activity (last 8h): {len(recent_pcmsb_activity)}")
    for activity_type, name, time in sorted(recent_pcmsb_activity, key=lambda x: x[2], reverse=True):
        print(f"  {activity_type}: {name} at {time.strftime('%Y-%m-%d %H:%M:%S')}")

    if not recent_pcmsb_activity:
        print("❌ NO RECENT PCMSB REFRESH ACTIVITY DETECTED!")
        print("   This suggests PCMSB Excel refresh is not executing")

    # Phase 4: Check PCMSB parquet files
    print(f"\\n\\nPHASE 4: PCMSB PARQUET FILE ANALYSIS")
    print("-" * 40)

    data_dir = Path("data/processed")
    pcmsb_parquet_files = []

    for unit in pcmsb_units:
        unit_files = list(data_dir.glob(f"*{unit}*.parquet"))
        for file_path in unit_files:
            try:
                stat = file_path.stat()
                file_modified = datetime.fromtimestamp(stat.st_mtime)
                file_age = (datetime.now() - file_modified).total_seconds() / 3600

                pcmsb_parquet_files.append({
                    'unit': unit,
                    'file': file_path.name,
                    'modified': file_modified,
                    'age_hours': file_age,
                    'size_mb': stat.st_size / (1024*1024)
                })
            except Exception as e:
                print(f"Error analyzing {file_path}: {e}")

    # Sort by age
    pcmsb_parquet_files.sort(key=lambda x: x['age_hours'])

    print("PCMSB Parquet files (sorted by age):")
    print(f"{'Unit':<12} {'File':<35} {'Modified':<20} {'Age(h)':<8} {'Size(MB)':<10}")
    print("-" * 95)

    for file_info in pcmsb_parquet_files[:10]:  # Show top 10
        unit = file_info['unit']
        filename = file_info['file'][:34] if len(file_info['file']) <= 34 else file_info['file'][:31] + "..."
        modified = file_info['modified'].strftime('%Y-%m-%d %H:%M')
        age = file_info['age_hours']
        size = file_info['size_mb']

        print(f"{unit:<12} {filename:<35} {modified:<20} {age:<8.1f} {size:<10.1f}")

    # Phase 5: Root cause determination
    print(f"\\n\\nPHASE 5: PCMSB ROOT CAUSE ANALYSIS")
    print("-" * 40)

    issues = []

    if len(stale_units) > 0:
        issues.append(f"CONFIRMED: {len(stale_units)} PCMSB units have stale data")

    if not recent_pcmsb_activity:
        issues.append("CRITICAL: No PCMSB Excel refresh activity in last 8h")

    if pcmsb_parquet_files:
        oldest_file = max(pcmsb_parquet_files, key=lambda x: x['age_hours'])
        if oldest_file['age_hours'] > 6:
            issues.append(f"CRITICAL: PCMSB parquet files are {oldest_file['age_hours']:.1f}h old")

    print("PCMSB ROOT CAUSES:")
    for i, issue in enumerate(issues, 1):
        print(f"{i}. {issue}")

    if not issues:
        print("No obvious PCMSB issues found")

    print(f"\\nRECOMMENDED ACTIONS:")
    print("1. Check if PCMSB_Automation.xlsx has valid PI DataLink connections")
    print("2. Verify PCMSB units are correctly mapped to PCMSB_Automation.xlsx")
    print("3. Test manual PCMSB Excel refresh")
    print("4. Check if option [1] is actually refreshing PCMSB Excel file")

if __name__ == "__main__":
    investigate_pcmsb_failure()