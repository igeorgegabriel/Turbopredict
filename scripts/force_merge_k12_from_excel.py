#!/usr/bin/env python3
from __future__ import annotations

from pathlib import Path
import pandas as pd
from datetime import datetime
import sys

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))

from pi_monitor.excel_refresh import refresh_excel_safe
from pi_monitor.ingest import load_latest_frame, write_parquet
from pi_monitor.clean import dedup_parquet


def main() -> int:
    plant = 'PCFS'
    unit = 'K-12-01'
    xlsx = PROJECT_ROOT / 'excel' / 'PCFS' / 'PCFS_Automation.xlsx'
    master = PROJECT_ROOT / 'data' / 'processed' / f'{unit}_1y_0p1h.parquet'

    print(f'Forcing merge from Excel for {plant} {unit}')
    print(f'Excel:  {xlsx}')
    print(f'Master: {master}')

    if not xlsx.exists():
        print(f'ERROR: Excel not found: {xlsx}')
        return 1

    # Refresh Excel to ensure cached values saved
    print('Refreshing Excel (working-copy strategy)...')
    refresh_excel_safe(xlsx, settle_seconds=5, use_working_copy=True, auto_cleanup=True)

    # Load latest frame from DL_WORK
    print('Loading DL_WORK from Excel...')
    df_excel = load_latest_frame(xlsx, unit=unit, plant=plant, sheet_name='DL_WORK')
    if df_excel is None or df_excel.empty:
        print('ERROR: Loaded no data from Excel DL_WORK')
        return 2

    df_excel['time'] = pd.to_datetime(df_excel['time'], errors='coerce')
    df_excel = df_excel.dropna(subset=['time']).sort_values('time').reset_index(drop=True)

    before_latest = None
    if master.exists():
        print('Reading existing master...')
        df_master = pd.read_parquet(master)
        if 'time' in df_master.columns:
            df_master['time'] = pd.to_datetime(df_master['time'], errors='coerce')
            before_latest = df_master['time'].max()
    else:
        df_master = pd.DataFrame(columns=['time', 'value', 'plant', 'unit', 'tag'])

    # Combine and dedup using appropriate keys
    print('Combining Excel data with master...')
    df_combined = pd.concat([df_master, df_excel], ignore_index=True)
    if 'tag' in df_combined.columns and 'tag' in df_master.columns and 'tag' in df_excel.columns:
        keys = ['time', 'tag']
    else:
        keys = ['time']
    df_combined = (
        df_combined
        .dropna(subset=['time'])
        .drop_duplicates(subset=keys, keep='last')
        .sort_values('time')
        .reset_index(drop=True)
    )

    out = write_parquet(df_combined, master)
    print(f'Updated master written: {out}')
    dedup = dedup_parquet(out)
    print(f'Dedup written: {dedup}')

    df_after = pd.read_parquet(master)
    df_after['time'] = pd.to_datetime(df_after['time'], errors='coerce')
    after_latest = df_after['time'].max()
    print(f'Latest before: {before_latest}')
    print(f'Latest after:  {after_latest}')
    age_h = None
    if after_latest is not None:
        age_h = (datetime.now() - after_latest).total_seconds() / 3600.0
        print(f'Data age now: {age_h:.1f} hours')

    print('Done.')
    return 0


if __name__ == '__main__':
    raise SystemExit(main())

