from pi_monitor.parquet_auto_scan import ParquetAutoScanner
from pi_monitor.config import Config
import pandas as pd

scanner = ParquetAutoScanner(Config())
units = scanner.db.get_all_units()
print('units', units)
for unit in units:
    print(f'--- Refreshing {unit} via fallback ---')
    excel_path = scanner._get_excel_file_for_unit(unit, None)
    try:
        new_df = scanner._load_unit_from_tags(unit, excel_path, lookback='-0.5d')
    except Exception as exc:
        print(f'   Fallback failed for {unit}: {exc}')
        continue
    existing_df = scanner.db.get_unit_data(unit)
    combined = pd.concat([existing_df, new_df], ignore_index=True)
    combined = combined.drop_duplicates(subset=['time','tag']).sort_values('time').reset_index(drop=True)
    master = scanner.db.processed_dir / f"{unit}_1y_0p1h.parquet"
    dedup = scanner.db.processed_dir / f"{unit}_1y_0p1h.dedup.parquet"
    combined.to_parquet(master, index=False)
    combined.sort_values(['time','tag']).drop_duplicates(subset=['plant','unit','tag','time']).to_parquet(dedup, index=False)
    print(f'   Updated {unit}: {len(combined)} rows, master -> {master.name}')
