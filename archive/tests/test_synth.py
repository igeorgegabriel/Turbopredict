from pi_monitor.parquet_auto_scan import ParquetAutoScanner
from pi_monitor.config import Config
scanner = ParquetAutoScanner(Config())
fallback = scanner._synthesize_refresh_from_existing('07-MT01-K001')
print('rows', len(fallback))
print(fallback.head())
