from pi_monitor.parquet_database import ParquetDatabase
from datetime import datetime

db = ParquetDatabase()
info = db.get_data_freshness_info('C-02001')
print(info)
