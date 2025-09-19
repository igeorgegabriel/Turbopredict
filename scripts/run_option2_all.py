#!/usr/bin/env python3
from __future__ import annotations

import os
import sys
from pathlib import Path
import json

project_root = Path(__file__).resolve().parents[1]
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

from pi_monitor.parquet_auto_scan import ParquetAutoScanner
from turbopredict import TurbopredictSystem


def run_option2_and_plot() -> None:
    # Force analysis even if smart detector says offline
    os.environ['FORCE_ANALYSIS'] = '1'
    system = TurbopredictSystem()
    system.run_unit_analysis()


def collect_summaries() -> list[dict]:
    scanner = ParquetAutoScanner()
    units = scanner.db.get_all_units()
    out = []
    for u in units:
        res = scanner.analyze_unit_data(u, run_anomaly_detection=True)
        an = res.get('anomalies', {}) if isinstance(res, dict) else {}
        by_tag = an.get('by_tag', {}) or {}
        out.append({
            'unit': u,
            'records': int(res.get('records', 0)),
            'method': an.get('method'),
            'total_anomalies': int(an.get('total_anomalies', 0) or 0),
            'anomaly_rate': float(an.get('anomaly_rate', 0.0) or 0.0),
            'suspected_tags': int(len(by_tag)),
        })
    return out


def main():
    run_option2_and_plot()
    summaries = collect_summaries()
    print(json.dumps({'summaries': summaries}, indent=2))
    return 0


if __name__ == '__main__':
    raise SystemExit(main())

