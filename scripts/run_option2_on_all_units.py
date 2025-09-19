#!/usr/bin/env python3
from __future__ import annotations

import sys
from pathlib import Path

# Ensure project root on path when invoked directly
project_root = Path(__file__).resolve().parents[1]
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

from pi_monitor.parquet_auto_scan import ParquetAutoScanner


def main():
    scanner = ParquetAutoScanner()
    units = scanner.db.get_all_units()
    if not units:
        print("No units found.")
        return 1
    print("Option [2] analysis summary:")
    print("Unit, AnomalyRate, Total, Method, Status")
    for u in units:
        res = scanner.analyze_unit_data(u, run_anomaly_detection=True)
        an = res.get('anomalies', {}) if isinstance(res, dict) else {}
        rate = an.get('anomaly_rate')
        tot = an.get('total_anomalies')
        method = an.get('method')
        status = an.get('unit_status', res.get('status')) if isinstance(res, dict) else None
        print(f"{u}, {rate}, {tot}, {method}, {status}")
    return 0


if __name__ == '__main__':
    raise SystemExit(main())

