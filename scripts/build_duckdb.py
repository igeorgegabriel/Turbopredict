from pathlib import Path
import argparse
import duckdb


def main():
    ap = argparse.ArgumentParser(description="Create a DuckDB database with a view over the Parquet dataset")
    ap.add_argument("--dataset", type=Path, default=Path("data/processed/dataset"))
    ap.add_argument("--db", type=Path, default=Path("data/processed/pi.duckdb"))
    ap.add_argument("--materialize", action="store_true", help="Materialize a table instead of a view")
    args = ap.parse_args()

    args.db.parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(str(args.db))

    pattern = str(args.dataset / "**/*.parquet")
    if args.materialize:
        con.execute("DROP TABLE IF EXISTS pi")
        con.execute("CREATE TABLE pi AS SELECT * FROM read_parquet(?);", [pattern])
    else:
        con.execute("CREATE OR REPLACE VIEW pi AS SELECT * FROM read_parquet(?);", [pattern])

    # Lightweight stats to speed up queries
    con.execute("ANALYZE")
    con.close()
    print(f"DuckDB ready: {args.db}")


if __name__ == "__main__":
    main()

