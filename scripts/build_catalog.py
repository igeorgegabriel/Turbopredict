from pathlib import Path
import argparse
import duckdb


def main():
    ap = argparse.ArgumentParser(description="Build catalog summaries from partitioned dataset")
    ap.add_argument("--dataset", type=Path, default=Path("data/processed/dataset"))
    ap.add_argument("--out-dir", type=Path, default=Path("data/processed"))
    args = ap.parse_args()

    args.out_dir.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect()
    pattern = str(args.dataset / "**/*.parquet")

    con.execute(
        """
        CREATE OR REPLACE VIEW v AS SELECT * FROM read_parquet(?);
        """,
        [pattern],
    )

    tags = con.execute(
        """
        SELECT plant, unit, tag,
               MIN(time) AS min_time,
               MAX(time) AS max_time,
               COUNT(*)  AS rows
        FROM v
        GROUP BY 1,2,3
        ORDER BY 1,2,3
        """
    ).fetch_df()
    tags_path = args.out_dir / "catalog_tags.parquet"
    tags.to_parquet(tags_path, index=False)

    units = con.execute(
        """
        SELECT plant, unit,
               COUNT(DISTINCT tag) AS tag_count,
               MIN(time) AS min_time,
               MAX(time) AS max_time,
               COUNT(*)  AS rows
        FROM v
        GROUP BY 1,2
        ORDER BY 1,2
        """
    ).fetch_df()
    units_path = args.out_dir / "catalog_units.parquet"
    units.to_parquet(units_path, index=False)

    con.close()
    print(f"Wrote: {tags_path}")
    print(f"Wrote: {units_path}")


if __name__ == "__main__":
    main()

