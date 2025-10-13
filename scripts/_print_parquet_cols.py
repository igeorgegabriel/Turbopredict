import pandas as pd
from pathlib import Path

units = ['C-104','C-13001','C-1301','C-1302','C-201','C-202','C-02001','XT-07002']
for name in units:
    p1 = Path('data/processed')/f'{name}_1y_0p1h.dedup.parquet'
    p2 = Path('data/processed')/f'{name}_1y_0p1h.parquet'
    p = p1 if p1.exists() else p2
    if not p.exists():
        print(name, 'missing')
        continue
    try:
        df = pd.read_parquet(p)
        cols = list(df.columns)
        print(name, 'rows', len(df), 'cols', cols[:8])
        if 'time' in df.columns:
            print(' latest', df['time'].max())
    except Exception as e:
        print(name, 'error', e)

