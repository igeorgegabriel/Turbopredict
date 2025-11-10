import pyarrow.parquet as pq
import pandas as pd
from pathlib import Path
for u in ['K-16-01','K-19-01','K-31-01']:
 p=Path(f'data/processed/{u}_1y_0p1h.parquet')
 t=pq.read_table(p).to_pandas()
 print(u, 'rows', len(t), 'latest', pd.to_datetime(t['time']).max(), 'has_tag', 'tag' in t.columns, 'unique_tags', len(set(t['tag'])) if 'tag' in t.columns else 'NA')
