from pathlib import Path
import pandas as pd
from pi_monitor.parquet_auto_scan import ParquetAutoScanner
from pi_monitor.config import Config

scanner = ParquetAutoScanner(Config())
unit = 'C-02001'
excel_path = scanner._get_excel_file_for_unit(unit, None)
print('using excel path', excel_path)
new_df = scanner._load_unit_from_tags(unit, excel_path, lookback='-2d')
print('fetched rows', len(new_df))

existing_df = scanner.db.get_unit_data(unit)
print('existing rows', len(existing_df))

combined = pd.concat([existing_df, new_df], ignore_index=True)
combined = combined.drop_duplicates(subset=['time', 'tag']).sort_values('time').reset_index(drop=True)
print('combined rows', len(combined))

master = scanner.db.processed_dir / f"{unit}_1y_0p1h.parquet"
dedup = scanner.db.processed_dir / f"{unit}_1y_0p1h.dedup.parquet"
combined.to_parquet(master, index=False)
combined.sort_values(['time', 'tag']).drop_duplicates(subset=['plant','unit','tag','time']).to_parquet(dedup, index=False)
print('updated master', master)
