import duckdb
p='data/processed/C-02001_1y_0p1h.parquet'
con=duckdb.connect()
print(con.execute(f"SELECT COUNT(*), COUNT(DISTINCT unit), MIN(time), MAX(time) FROM read_parquet('{p}')").fetchall())
print(con.execute(f"SELECT unit, COUNT(*) c, MAX(time) FROM read_parquet('{p}') GROUP BY unit ORDER BY c DESC LIMIT 5").fetchall())
