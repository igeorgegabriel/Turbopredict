#!/usr/bin/env python3
from __future__ import annotations

import os
import sys
import time
from datetime import datetime
from pathlib import Path

# Ensure imports work from project root
PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from pi_monitor.parquet_auto_scan import ParquetAutoScanner
from pi_monitor.parquet_database import ParquetDatabase


def _set_default_env() -> None:
    os.environ.setdefault("PI_FETCH_TIMEOUT", "60")      # seconds per tag
    os.environ.setdefault("PI_FETCH_LINGER", "20")       # extra settle on empty
    os.environ.setdefault("EXCEL_CALC_MODE", "full")     # robust calc mode
    os.environ.setdefault("NO_VISIBLE_FALLBACK", "0")    # allow visible retry
    os.environ.setdefault("DEDUP_MODE", "end")           # dedup once at end
    os.environ.setdefault("MAX_AGE_HOURS", "1.0")        # target freshness


def _stamp() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def main() -> int:
    _set_default_env()

    max_age_hours = float(os.getenv("MAX_AGE_HOURS", "1.0"))
    max_passes = int(os.getenv("REFRESH_PASSES", "4"))
    pause_between_passes = int(os.getenv("REFRESH_PAUSE_SECS", "60"))

    print("================ OVERNIGHT REFRESH ================")
    print(f"Start: {_stamp()}")
    print(f"Target freshness: {max_age_hours}h, passes: {max_passes}")
    print("Env: PI_FETCH_TIMEOUT=", os.getenv("PI_FETCH_TIMEOUT"),
          " PI_FETCH_LINGER=", os.getenv("PI_FETCH_LINGER"),
          " EXCEL_CALC_MODE=", os.getenv("EXCEL_CALC_MODE"),
          " DEDUP_MODE=", os.getenv("DEDUP_MODE"))

    scanner = ParquetAutoScanner()
    db = scanner.db

    for attempt in range(1, max_passes + 1):
        print("\n====================================================")
        print(f"Pass {attempt}/{max_passes} at {_stamp()}")

        try:
            results = scanner.refresh_stale_units_with_progress(max_age_hours=max_age_hours)
        except Exception as e:
            print(f"refresh_stale_units_with_progress failed: {e}")
            results = {"success": False, "error": str(e), "units_processed": []}

        # Quick status after the pass
        db = ParquetDatabase(db.data_dir)  # reload view of files
        status = db.get_database_status()
        units_info = status.get("units", [])
        still_stale = [u for u in units_info if u.get("is_stale")]
        fresh = [u for u in units_info if not u.get("is_stale")]

        print("\nPass summary:")
        print(f"  Units processed this pass: {len(results.get('units_processed', []))}")
        print(f"  Fresh units now: {len(fresh)}")
        print(f"  Stale units remaining: {len(still_stale)}")

        if not still_stale:
            print("\nAll units are fresh. Exiting.")
            break

        if attempt < max_passes:
            print(f"\nSleeping {pause_between_passes}s before next pass...")
            time.sleep(pause_between_passes)

    print("\n================= REFRESH COMPLETE =================")
    print(f"End: {_stamp()}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

