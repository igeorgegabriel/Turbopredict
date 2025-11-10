import pyarrow.parquet as pq
import pandas as pd
from pathlib import Path
p=Path('data/processed/K-12-01_1y_0p1h.parquet')
if not p.exists():
  print('missing')
else:
  df=pq.read_table(p).to_pandas()
  df['time']=pd.to_datetime(df['time'])
  print(df.tail(5)[['time','tag','value']])
