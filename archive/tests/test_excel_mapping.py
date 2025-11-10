#!/usr/bin/env python3
"""
Test the intelligent Excel file mapping for units
"""

from pi_monitor.parquet_auto_scan import ParquetAutoScanner
from pathlib import Path

def test_excel_mapping():
    """Test unit-to-Excel file mapping"""
    scanner = ParquetAutoScanner()

    # Test units from different plants
    test_units = [
        "K-12-01",      # PCFS
        "K-16-01",      # PCFS
        "K-19-01",      # PCFS
        "K-31-01",      # PCFS
        "07-MT01-K001", # ABF
        "PCMSB-U01",    # PCMSB
        "ABFSB-U01",    # ABFSB
        "UNKNOWN-U01"   # Unknown
    ]

    print("INTELLIGENT EXCEL FILE MAPPING TEST")
    print("=" * 50)

    for unit in test_units:
        try:
            excel_file = scanner._get_excel_file_for_unit(unit)
            print(f"{unit:<15} -> {excel_file.name}")
        except Exception as e:
            print(f"{unit:<15} -> ERROR: {e}")

    print("\n✓ Test completed - K-units should map to PCFS_Automation_2.xlsx")

if __name__ == "__main__":
    test_excel_mapping()