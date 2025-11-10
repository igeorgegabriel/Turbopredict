from pathlib import Path
from pi_monitor.ingest import load_latest_frame
p = Path('excel/PCMSB/PCMSB_Automation.xlsx')
df = load_latest_frame(p, sheet_name='DL_WORK')
print('rows', len(df))
print('latest', df['time'].max())
