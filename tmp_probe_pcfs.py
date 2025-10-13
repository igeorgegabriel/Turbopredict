from pathlib import Path
from pi_monitor.ingest import load_latest_frame
p=Path('excel/PCFS/PCFS_Automation.xlsx')
for sheet in ['DL_WORK','DL_K1201','DL_K12_01','DL_K_12_01','Sheet1']:
  try:
    df=load_latest_frame(p, sheet_name=sheet)
    print(sheet, len(df), df['time'].max())
  except Exception as e:
    print(sheet, 'ERR', e)
