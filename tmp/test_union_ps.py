import sys
from pathlib import Path
sys.path.insert(0, str(Path('.').resolve()))

from pi_monitor.ingest import write_parquet
import duckdb, os, pandas as pd
import pyarrow.parquet as pq

p=Path('data/processed/K-31-01_1y_0p1h.parquet')
now=pd.Timestamp.now()
df=pd.DataFrame({'plant':['PCFS'],'unit':['K-31-01'],'tag':['TEST_TAG3'],'time':[now],'value':[8.88]})
tmp=p.with_name(p.stem+'_refreshed_test3.parquet')
write_parquet(df,tmp)
out_tmp=p.with_name(p.stem+'.updated.parquet')
con=duckdb.connect()
sql=f"COPY (SELECT * FROM read_parquet('{p.as_posix()}') UNION ALL SELECT * FROM read_parquet('{tmp.as_posix()}')) TO '{out_tmp.as_posix()}' (FORMAT PARQUET, COMPRESSION 'ZSTD');"
con.execute(sql)
con.close()
os.replace(out_tmp,p)
os.remove(tmp)
pf=pq.ParquetFile(p)
print('rows',pf.metadata.num_rows)
