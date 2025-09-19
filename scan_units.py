from pi_monitor.parquet_auto_scan import ParquetAutoScanner
from pi_monitor.config import Config
scanner = ParquetAutoScanner(Config())
scanner.db = scanner.db.__class__()
scan = scanner.scan_all_units(max_age_hours=1.0)
print(scan['summary'])
for unit in scan['units_scanned']:
    print(unit['unit'], unit['data_age_hours'], unit['is_stale'])
