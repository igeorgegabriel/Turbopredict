#!/usr/bin/env python3
from __future__ import annotations

import argparse
from pathlib import Path
import duckdb


def main() -> int:
    ap = argparse.ArgumentParser(
        description="Create/refresh DuckDB (pi.duckdb) over processed Parquet masters"
    )
    ap.add_argument("--processed", type=Path, default=Path("data/processed"), help="Processed directory")
    ap.add_argument("--db", type=Path, default=Path("data/processed/pi.duckdb"), help="DuckDB file path")
    ap.add_argument("--materialize", action="store_true", help="Materialize a table instead of a view")
    ap.add_argument("--include-non-dedup", action="store_true", help="Also include non-dedup files")
    args = ap.parse_args()

    args.db.parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(str(args.db))

    # Build glob patterns
    pats = [str(args.processed / "*dedup.parquet")]
    if args.include_non_dedup:
        pats.append(str(args.processed / "*.parquet"))

    # Build a union-all read over all patterns
    union_sql = " UNION ALL ".join(["SELECT * FROM read_parquet(?)" for _ in pats])

    if args.materialize:
        con.execute("DROP TABLE IF EXISTS pi")
        con.execute(f"CREATE TABLE pi AS {union_sql}", pats)
    else:
        con.execute(f"CREATE OR REPLACE VIEW pi AS {union_sql}", pats)

    con.execute("ANALYZE")
    con.close()
    print(f"DuckDB ready: {args.db}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

