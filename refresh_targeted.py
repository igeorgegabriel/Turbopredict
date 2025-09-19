from pi_monitor.parquet_auto_scan import ParquetAutoScanner
from pi_monitor.config import Config
scanner = ParquetAutoScanner(Config())
result = scanner.refresh_stale_units_with_progress(max_age_hours=0.5)
print('refresh success', result.get('success'))
