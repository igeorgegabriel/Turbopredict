#!/usr/bin/env python3
from __future__ import annotations

from pathlib import Path
from datetime import datetime
import sys

# Ensure project root on sys.path so 'pi_monitor' can be imported reliably
PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

try:
    from pi_monitor.parquet_database import ParquetDatabase
except Exception as e:
    print(f"Failed to import ParquetDatabase: {e}")
    sys.exit(1)


def main() -> int:
    db = ParquetDatabase()
    units = db.get_all_units()
    now = datetime.now()

    processed = PROJECT_ROOT / "data" / "processed"

    rows: list[dict] = []
    for u in units:
        # Prefer dedup; fall back to raw 1y_0p1h file
        p1 = processed / f"{u}_1y_0p1h.dedup.parquet"
        p2 = processed / f"{u}_1y_0p1h.parquet"
        p = p1 if p1.exists() else p2 if p2.exists() else None

        recs = 0
        latest = None
        tags = 0
        if p and p.exists():
            try:
                import pandas as pd
                # Read only needed columns to be fast
                df = pd.read_parquet(p, columns=["time", "tag"]) if p.suffix == ".parquet" else pd.read_parquet(p)
                recs = len(df)
                if "time" in df.columns and recs:
                    latest = pd.to_datetime(df["time"], errors="coerce").max()
                if "tag" in df.columns and recs:
                    try:
                        tags = int(df["tag"].nunique())
                    except Exception:
                        tags = 0
            except Exception:
                pass

        age_h = None
        if latest is not None:
            try:
                age_h = (now - latest).total_seconds() / 3600.0
            except Exception:
                age_h = None

        rows.append(
            {
                "unit": u,
                "records": recs,
                "latest": latest,
                "age_h": age_h,
                "tags": tags,
                "stale": (age_h is not None and age_h > float((__import__('os').getenv('MAX_AGE_HOURS') or 1.0))),
            }
        )

    rows_sorted = sorted(rows, key=lambda r: (r["age_h"] is None, r["age_h"]), reverse=True)

    # Write report file
    out_dir = Path("reports")
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / f"db_freshness_{now.strftime('%Y%m%d_%H%M%S')}.txt"

    def _fmt(r: dict) -> str:
        latest = r["latest"].strftime("%Y-%m-%d %H:%M") if r.get("latest") else "-"
        age = f"{r['age_h']:.1f}" if r.get("age_h") is not None else "-"
        status = "STALE" if r.get("stale") else "FRESH"
        return f"{r['unit']:<10} | age_h: {age:>6} | latest: {latest:<16} | rows: {r['records']:<9} | tags: {r['tags']:<4} | {status}"

    header = "DB Freshness Report\n" + ("=" * 72) + f"\nGenerated: {now}\n\n"
    content = header + "\n".join(_fmt(r) for r in rows_sorted) + "\n"
    out_path.write_text(content, encoding="utf-8")

    # Also print to stdout
    print(content)
    print(f"Saved report: {out_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
