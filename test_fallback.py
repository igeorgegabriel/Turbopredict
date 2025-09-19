from pi_monitor.parquet_auto_scan import ParquetAutoScanner
from pi_monitor.config import Config
scanner = ParquetAutoScanner(Config())
excel_path = scanner._get_excel_file_for_unit('07-MT01-K001', None)
df = scanner._load_unit_from_tags('07-MT01-K001', excel_path, lookback='-1d')
print('rows', len(df))
print(df.head())
