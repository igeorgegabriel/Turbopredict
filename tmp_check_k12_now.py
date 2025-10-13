from pathlib import Path
import pyarrow.parquet as pq
import pandas as pd
from datetime import datetime
p=Path('data/processed/K-12-01_1y_0p1h.parquet')
if p.exists():
  t=pq.read_table(p).to_pandas()
  t['time']=pd.to_datetime(t['time'])
  print('file mtime', datetime.fromtimestamp(p.stat().st_mtime))
  print('rows', len(t), 'latest', t['time'].max())
  print('has tag', 'tag' in t.columns, 'unique_tags', (t['tag'].nunique() if 'tag' in t.columns else 'NA'))
else:
  print('missing file')
