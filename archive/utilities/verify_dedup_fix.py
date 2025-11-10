from pathlib import Path
import pandas as pd
from datetime import datetime, timedelta
from shutil import copy2

master = Path(r"c:\\Users\\george.gabrielujai\\Documents\\CodeX\\data\\processed\\07-MT01-K001_1y_0p1h.parquet")
dedup = master.with_name(master.stem + ".dedup.parquet")
backup_master = master.with_suffix('.parquet.testbak')
backup_dedup = dedup.with_suffix('.parquet.testbak')

copy2(master, backup_master)
copy2(dedup, backup_dedup)

try:
    df = pd.read_parquet(master)
    if df.empty:
        raise SystemExit('master dataframe empty')
    last = df.iloc[-1].copy()
    last['time'] = pd.to_datetime(last['time'])
    last['time'] = last['time'] + timedelta(hours=26)
    if 'value' in last:
        last['value'] = (last['value'] or 0) * 1.0
    df = pd.concat([df, pd.DataFrame([last])], ignore_index=True)
    df.to_parquet(master, index=False)

    from pi_monitor.clean import dedup_parquet
    dedup_parquet(master)

    from pi_monitor.parquet_database import ParquetDatabase
    db = ParquetDatabase()
    info = db.get_data_freshness_info('07-MT01-K001')
    print({'age_hours': info['data_age_hours'], 'latest_timestamp': info['latest_timestamp']})
finally:
    copy2(backup_master, master)
    copy2(backup_dedup, dedup)
    backup_master.unlink(missing_ok=True)
    backup_dedup.unlink(missing_ok=True)
