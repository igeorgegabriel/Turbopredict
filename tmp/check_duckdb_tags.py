import duckdb

conn = duckdb.connect(database=':memory:')
q = (
    "SELECT COUNT(*) AS total, MIN(time) AS earliest, MAX(time) AS latest, "
    "       COUNT(DISTINCT tag) AS uniq "
    "FROM read_parquet('data/processed/*dedup.parquet') WHERE unit = ?"
)
for unit in [
    'K-31-01', 'K-16-01', 'K-19-01', 'C-02001', 'XT-07002'
]:
    print(unit, conn.execute(q, [unit]).fetchone())
