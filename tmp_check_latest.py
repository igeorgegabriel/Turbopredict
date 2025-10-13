import duckdb, pandas as pd
from datetime import datetime
p='data/processed/C-02001_1y_0p1h.parquet'
try:
    con=duckdb.connect()
    latest=con.execute(f"SELECT max(time) FROM read_parquet('{p}')").fetchone()[0]
    print('duckdb latest:', latest)
except Exception as e:
    print('duckdb error:', e)
try:
    df=pd.read_parquet(p, columns=['time'])
    t=pd.to_datetime(df['time']).max()
    print('pandas latest:', t)
except Exception as e:
    print('pandas error:', e)
