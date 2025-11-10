from pathlib import Path
import pyarrow.parquet as pq
from datetime import datetime
p=Path('data/processed/K-12-01_1y_0p1h.dedup.parquet')
if p.exists():
  t=pq.read_table(p).to_pandas()
  print('dedup mtime', datetime.fromtimestamp(p.stat().st_mtime))
  print('rows', len(t), 'latest', t['time'].max())
else:
  print('missing file')
