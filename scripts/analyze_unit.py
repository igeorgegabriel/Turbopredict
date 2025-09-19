#!/usr/bin/env python3
from __future__ import annotations

import argparse
from pathlib import Path
import sys

# Ensure project root on path
project_root = Path(__file__).resolve().parents[1]
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

from pi_monitor.parquet_auto_scan import ParquetAutoScanner


def main():
    ap = argparse.ArgumentParser(description='Analyze a unit using Option [2] logic')
    ap.add_argument('--unit', required=True)
    ap.add_argument('--top', type=int, default=10, help='Show top N tags by anomalies')
    args = ap.parse_args()

    scanner = ParquetAutoScanner()
    res = scanner.analyze_unit_data(args.unit, run_anomaly_detection=True)

    print("UNIT ANALYSIS SUMMARY")
    print("=" * 60)
    print(f"Unit: {args.unit}")
    print(f"Records: {res.get('records')}")
    dr = res.get('date_range', {})
    print(f"Date range: {dr.get('start')} -> {dr.get('end')}")

    an = res.get('anomalies', {})
    print(f"Method: {an.get('method')}")
    print(f"Total anomalies: {an.get('total_anomalies', 0)}")
    print(f"Anomaly rate: {an.get('anomaly_rate', 0.0)}")

    by_tag = an.get('by_tag', {}) or {}
    if by_tag:
        print("\nTop tags:")
        items = sorted(by_tag.items(), key=lambda kv: kv[1].get('count', 0), reverse=True)[:args.top]
        for tag, info in items:
            print(f" - {tag}: {info.get('count', 0)} anomalies (rate={info.get('rate', 0.0):.4f})")
    else:
        print("\nNo anomalous tags reported.")
    return 0


if __name__ == '__main__':
    raise SystemExit(main())

