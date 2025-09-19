#!/usr/bin/env python3
"""
Test the ParquetAutoScanner to see what units it finds
"""

import sys
from pathlib import Path

# Add current directory to path
sys.path.insert(0, str(Path(__file__).parent))

from pi_monitor.parquet_auto_scan import ParquetAutoScanner
from pi_monitor.parquet_database import ParquetDatabase

def test_parquet_scanner():
    print("TURBOPREDICT X PROTEAN - Parquet Scanner Test")
    print("=" * 60)
    
    # Initialize the scanner
    scanner = ParquetAutoScanner()
    
    # Test ParquetDatabase directly
    print("\n1. TESTING PARQUETDATABASE DIRECTLY:")
    print("-" * 40)
    
    db = ParquetDatabase()
    
    # Get available files
    files = db.get_available_parquet_files()
    print(f"Found {len(files)} Parquet files:")
    for file_info in files:
        print(f"  {file_info['filename']}: {file_info['unit']} ({file_info['size_mb']} MB)")
    
    # Get all units
    units = db.get_all_units()
    print(f"\nUnits found: {units}")
    
    # Get database status
    status = db.get_database_status()
    print(f"\nDatabase Status:")
    print(f"  Directory: {status['processed_directory']}")
    print(f"  Total Files: {status['total_files']}")
    print(f"  Storage Size: {status['total_size_gb']:.2f} GB")
    print(f"  DuckDB Available: {status['duckdb_available']}")
    print(f"  Active Units: {len(status['units'])}")
    
    for unit_info in status['units']:
        print(f"    {unit_info['unit']}: {unit_info['files']} files, {unit_info['size_mb']:.1f} MB, {unit_info['records']:,} records")
    
    # Test ParquetAutoScanner
    print("\n2. TESTING PARQUETAUTOSCANNER:")
    print("-" * 40)
    
    scan_results = scanner.scan_all_units()
    
    print(f"Scan Results Summary:")
    print(f"  Total Units: {scan_results['summary']['total_units']}")
    print(f"  Fresh Units: {scan_results['summary']['fresh_units']}")
    print(f"  Stale Units: {scan_results['summary']['stale_units']}")
    print(f"  Empty Units: {scan_results['summary']['empty_units']}")
    
    print("\nUnit Details:")
    for unit_result in scan_results['units_scanned']:
        status_str = "FRESH" if not unit_result['is_stale'] else "STALE"
        age_str = f"{unit_result['data_age_hours']:.1f}" if unit_result['data_age_hours'] else "N/A"
        print(f"  {unit_result['unit']:<12} {unit_result['total_records']:>8,} records, {age_str:>6} hrs, {status_str}")

if __name__ == "__main__":
    test_parquet_scanner()