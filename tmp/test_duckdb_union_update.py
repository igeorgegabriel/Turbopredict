from pathlib import Path
import os
import pandas as pd
import duckdb
import pyarrow.parquet as pq

from pi_monitor.ingest import write_parquet

p = Path('data/processed/K-31-01_1y_0p1h.parquet')

df = pd.DataFrame({
    'plant': ['PCFS'],
    'unit': ['K-31-01'],
    'tag': ['TEST_TAG2'],
    'time': [pd.Timestamp.now()],
    'value': [7.77],
})

tmp = p.with_name(p.stem + '_refreshed_test.parquet')
out_tmp = p.with_name(p.stem + '.updated.parquet')

# Write the fresh chunk
write_parquet(df, tmp)

con = duckdb.connect()
sql = f"""
COPY (
  SELECT * FROM read_parquet('{p.as_posix()}')
  UNION ALL
  SELECT * FROM read_parquet('{tmp.as_posix()}')
) TO '{out_tmp.as_posix()}' (FORMAT PARQUET, COMPRESSION 'ZSTD');
"""
con.execute(sql)
con.close()

os.replace(out_tmp, p)
os.remove(tmp)

pf = pq.ParquetFile(p)
print('rows', pf.metadata.num_rows)
