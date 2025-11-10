#!/usr/bin/env python3
"""
Test Excel mapping for C-units specifically
"""

from pi_monitor.parquet_auto_scan import ParquetAutoScanner

def test_c_unit_mapping():
    """Test C-unit mapping specifically"""
    scanner = ParquetAutoScanner()

    # Test the actual C-units showing as stale
    c_units = [
        "C-104",
        "C-13001",
        "C-1301",
        "C-1302",
        "C-201",
        "C-202"
    ]

    print("C-UNIT EXCEL FILE MAPPING TEST")
    print("=" * 40)

    for unit in c_units:
        try:
            excel_file = scanner._get_excel_file_for_unit(unit)
            print(f"{unit:<10} -> {excel_file.name}")
        except Exception as e:
            print(f"{unit:<10} -> ERROR: {e}")

if __name__ == "__main__":
    test_c_unit_mapping()