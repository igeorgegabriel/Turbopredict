from pi_monitor.parquet_auto_scan import ParquetAutoScanner
from pi_monitor.config import Config
scanner = ParquetAutoScanner(Config())
result = scanner.refresh_stale_units_with_progress(max_age_hours=0.5)
print('refresh success', result.get('success'))
print('stale units after refresh', [u for u in result.get('units_to_refresh', []) if u not in result.get('fresh_after_refresh', [])])
