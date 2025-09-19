from pi_monitor.parquet_database import ParquetDatabase

db = ParquetDatabase()
info = db.get_data_freshness_info('07-MT01-K001')
print('latest', info['latest_timestamp'])
print('age_hours', info['data_age_hours'])
