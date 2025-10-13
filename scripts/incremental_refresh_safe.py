#!/usr/bin/env python3
"""Safe incremental refresh - skips problematic units, only refreshes working ones."""
from __future__ import annotations

from pathlib import Path
import sys
from datetime import datetime, timedelta
import time
from collections import defaultdict

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))

from pi_monitor.batch import build_unit_from_tags
from pi_monitor.excel_refresh import refresh_excel_with_pi_coordination
from pi_monitor.ingest import load_latest_frame
try:
    from pi_monitor.webapi import fetch_tags_via_webapi, PIWebAPIClient
except Exception:
    fetch_tags_via_webapi = None  # type: ignore
    PIWebAPIClient = None  # type: ignore


def read_tags(path: Path) -> list[str]:
    tags: list[str] = []
    for line in Path(path).read_text(encoding="utf-8").splitlines():
        s = line.strip()
        if not s or s.startswith("#"):
            continue
        tags.append(s)
    return tags


def get_latest_timestamp(parquet_file: Path) -> datetime | None:
    """Get the latest timestamp from existing Parquet file."""
    if not parquet_file.exists():
        return None

    import pandas as pd
    import warnings as _w
    try:
        with _w.catch_warnings():
            _w.simplefilter("ignore")
            df = pd.read_parquet(parquet_file)

        # Check if index is DatetimeIndex (time is the index)
        if isinstance(df.index, pd.DatetimeIndex) and len(df) > 0:
            return df.index.max().to_pydatetime()
        # Fallback: check if 'time' or 'timestamp' is in columns
        elif 'time' in df.columns and len(df) > 0:
            with _w.catch_warnings():
                _w.simplefilter("ignore")
                return pd.to_datetime(df['time'].max()).to_pydatetime()
        elif 'timestamp' in df.columns and len(df) > 0:
            with _w.catch_warnings():
                _w.simplefilter("ignore")
                return pd.to_datetime(df['timestamp'].max()).to_pydatetime()
    except Exception:
        pass
    return None


def incremental_refresh_unit(
    unit: str,
    tags: list[str],
    xlsx: Path,
    plant: str,
    server: str,
) -> bool:
    """Fetch data from last timestamp until now to prevent gaps."""
    import pandas as pd
    import os

    # Set reasonable timeouts for incremental refresh (fail fast approach)
    os.environ['PI_FETCH_TIMEOUT'] = '60'   # 60s per tag (balance between speed and reliability)
    os.environ['PI_FETCH_LINGER'] = '20'    # 20s linger to catch slow async queries
    # Force full calculation mode for reliability with PI DataLink
    os.environ.setdefault('EXCEL_CALC_MODE', 'full')
    # Enable early error detection to skip bad tags quickly
    os.environ.setdefault('PI_EARLY_ERROR_DETECT', '1')
    # PI DataLink initialization warmup (critical - gives add-in time to connect after Excel opens)
    os.environ.setdefault('PI_DATALINK_WARMUP', '5')  # 5s warmup after opening Excel

    parquet_file = PROJECT_ROOT / "data" / "processed" / f"{unit}_1y_0p1h.dedup.parquet"
    temp_file = PROJECT_ROOT / "tmp" / f"{unit}_incremental.parquet"
    temp_file.parent.mkdir(parents=True, exist_ok=True)

    print(f"\n{'='*80}")
    print(f"INCREMENTAL REFRESH: {unit}")
    print(f"{'='*80}\n")

    # Get latest timestamp from existing data
    latest_time = get_latest_timestamp(parquet_file)

    if not latest_time:
        print(f"[X] No existing data found - skipping (use full build first)")
        return False

    age = datetime.now() - latest_time.replace(tzinfo=None)
    print(f"Existing data:")
    print(f"  Latest timestamp: {latest_time}")
    print(f"  Data age: {age}")

    if age < timedelta(hours=1):
        print(f"\n[OK] Data is fresh (< 1 hour old) - no refresh needed")
        return True

    # PI DataLink requires RELATIVE time format (e.g., "-6h"), NOT absolute timestamps!
    # Calculate gap and add small buffer to ensure overlap (dedup will handle it)
    hours_gap = age.total_seconds() / 3600
    hours_to_fetch = int(hours_gap * 1.1) + 1  # 10% buffer + 1 hour

    # Use PI DataLink relative time format
    start_time = f"-{hours_to_fetch}h"

    print(f"\nFilling data gap:")
    print(f"  Gap detected: {hours_gap:.1f} hours")
    print(f"  Fetching: {hours_to_fetch} hours (with 10% buffer)")
    print(f"  PI format: {start_time} to *")
    print(f"  Tags: {len(tags)}")

    try:
        # 1) Primary path: PI Web API (if configured)
        webapi_url = os.getenv('PI_WEBAPI_URL', '').strip()
        used_webapi = False
        if webapi_url and fetch_tags_via_webapi is not None:
            try:
                # Preflight Web API to avoid long per-tag retries when unreachable
                try:
                    if 'PIWebAPIClient' in globals() and PIWebAPIClient is not None:
                        client = PIWebAPIClient(base_url=webapi_url, auth_mode='windows', timeout=15.0, verify_ssl=False)
                        ok, info = client.health_check()
                        if not ok:
                            print(f"[warn] Web API preflight failed: {info}. Falling back to Excel.")
                            raise RuntimeError(f"webapi_unreachable: {info}")
                except Exception:
                    # Surface to outer try so Excel fallback is used
                    raise
                print(f"[info] Trying PI Web API at {webapi_url} for {unit}â€¦")
                # Web API expects relative start as "*-Nh"
                df_api = fetch_tags_via_webapi(
                    tags=tags,
                    server=server,
                    start=start_time,
                    end='*',
                    step='-0.1h',
                )
                if df_api is not None and not df_api.empty:
                    df_api['plant'] = plant
                    df_api['unit'] = unit
                    # Standardize columns
                    df_api = df_api[['time', 'value', 'plant', 'unit', 'tag']]
                    df_api.to_parquet(temp_file, index=False, engine='pyarrow')
                    print(f"[OK] PI Web API fetched {len(df_api):,} records for {unit}")
                    used_webapi = True
                else:
                    print("[warn] PI Web API returned no data; will try Excel fallback")
            except Exception as api_err:
                print(f"[warn] PI Web API fetch failed: {api_err}")

        # 2) Excel PI DataLink fallback (two modes):
        #    a) Refresh-only using a known-good workbook (preferred for PCFS if provided)
        #    b) Direct formula injection per tag
        if not used_webapi:
            import os as _os_fb
            use_refresh_only = str(_os_fb.getenv('EXCEL_REFRESH_ONLY', '')).strip().lower() in {"1","true","yes","on"}
            # Allow plant-specific workbook override, e.g., PCFS_EXCEL_PATH pointing to the manual working copy
            plant_excel_override = _os_fb.getenv(f"{plant.upper()}_EXCEL_PATH", "").strip()
            xlsx_effective = Path(plant_excel_override) if plant_excel_override else xlsx
            if plant_excel_override:
                print(f"[info] Using plant-specific workbook override for {plant}: {xlsx_effective}")

            if use_refresh_only or plant_excel_override:
                try:
                    print("[i] Excel fallback: RefreshAll + DL_WORK parse (no injection)â€¦")
                    refresh_excel_with_pi_coordination(xlsx_effective, settle_seconds=4, use_working_copy=True)
                    df_fallback = load_latest_frame(xlsx_effective, sheet_name='DL_WORK', unit=unit, plant=plant)
                    if df_fallback is None or df_fallback.empty:
                        print("[X] DL_WORK parse returned no data after RefreshAll")
                        return False
                    if latest_time is not None:
                        df_fallback = df_fallback[df_fallback['time'] > latest_time]
                    if df_fallback.empty:
                        print("[i] Refresh-only produced no rows newer than existing data")
                        return True
                    df_fallback.to_parquet(temp_file, index=False, compression='snappy')
                    print(f"[OK] Excel refresh-only produced {len(df_fallback):,} rows")
                except Exception as fb_err:
                    print(f"[X] Excel refresh-only fallback failed: {fb_err}")
                    return False
            else:
                # Direct injection path
                build_unit_from_tags(
                    xlsx_effective,
                    tags,
                    temp_file,
                    plant=plant,
                    unit=unit,
                    server=server,
                    start=start_time,  # Relative time format that PI DataLink understands!
                    end="*",
                    step="-0.1h",
                    work_sheet="DL_WORK",
                    settle_seconds=5.0,  # Increased from 3.0 to give PI DataLink more time to load
                    visible=True,
                )

                if not temp_file.exists() or temp_file.stat().st_size == 0:
                    print(f"[X] No new data fetched via perâ€‘tag formula injection")
                    return False

        # Load new data
        df_new = pd.read_parquet(temp_file)
        print(f"\n[OK] Fetched {len(df_new):,} new records")

        # Merge with existing data
        print(f"Merging with existing data...")
        df_old = pd.read_parquet(parquet_file)
        print(f"  Existing records: {len(df_old):,}")

        # Combine
        df_combined = pd.concat([df_old, df_new], ignore_index=True)

        # Deduplicate
        if 'time' in df_combined.columns and 'tag' in df_combined.columns:
            print(f"  Deduplicating...")
            before = len(df_combined)
            df_combined = df_combined.drop_duplicates(subset=['time', 'tag'], keep='last')
            after = len(df_combined)
            print(f"  Removed {before - after:,} duplicates")

        # Sort
        df_combined = df_combined.sort_values('time')

        print(f"  Final records: {len(df_combined):,}")

        # Save
        print(f"Saving to {parquet_file.name}...")
        df_combined.to_parquet(parquet_file, index=False, compression='snappy')

        # Cleanup
        if temp_file.exists():
            temp_file.unlink()

        print(f"\n[OK] Incremental refresh completed!")
        return True

    except Exception as e:
        print(f"[X] Error: {e}")
        return False


def main() -> int:
    """Refresh ALL units - PCFS, ABF, and PCMSB."""

    # Allow overriding PI server via env
    import os
    DEFAULT_SERVER = os.getenv('PI_SERVER_NAME', r"\\PTSG-1MMPDPdb01")

    # Enable PI Web API as primary fetch if not configured yet, using polite defaults
    # Derive host from DEFAULT_SERVER (strip any leading backslashes)
    try:
        _server_host = DEFAULT_SERVER.lstrip('\\') if isinstance(DEFAULT_SERVER, str) else 'PTSG-1MMPDPdb01'
        if not os.getenv('PI_WEBAPI_URL'):
            os.environ['PI_WEBAPI_URL'] = f"https://{_server_host}/piwebapi"
        os.environ.setdefault('PI_WEBAPI_MAX_WORKERS', '4')
        os.environ.setdefault('PI_WEBAPI_QPS', '3')
        os.environ.setdefault('PI_WEBAPI_RETRIES', '2')
        os.environ.setdefault('PI_WEBAPI_VERIFY_SSL', 'true')
        print(f"[info] PI Web API primary enabled: {os.getenv('PI_WEBAPI_URL')} (workers={os.getenv('PI_WEBAPI_MAX_WORKERS')}, qps={os.getenv('PI_WEBAPI_QPS')})")
    except Exception:
        pass

    # Optional skip list driven by env (default: do not skip PCFS units)
    # Set SKIP_UNITS env to a semicolon-separated list like "PCFS:K-12-01;PCFS:K-16-01"
    SKIP_UNITS: dict[tuple[str, str], str] = {}
    raw_skip = os.getenv('SKIP_UNITS', '').strip()
    if raw_skip:
        for item in [s.strip() for s in raw_skip.split(';') if s.strip()]:
            if ':' in item:
                plant, unit = item.split(':', 1)
                SKIP_UNITS[(plant.strip(), unit.strip())] = 'Skipped by SKIP_UNITS env'
        if SKIP_UNITS:
            print(f"\n[info] Skip list loaded from SKIP_UNITS env: {', '.join([f'{p}:{u}' for (p,u) in SKIP_UNITS.keys()])}")

    # All units from all plants (plant, unit, tags_file, excel_file, server)
    all_units = [
        # PCFS units
        ("PCFS", "K-12-01", "tags_k12_01.txt", "PCFS/PCFS_Automation.xlsx", DEFAULT_SERVER),
        ("PCFS", "K-16-01", "tags_k16_01.txt", "PCFS/PCFS_Automation.xlsx", DEFAULT_SERVER),
        ("PCFS", "K-19-01", "tags_k19_01.txt", "PCFS/PCFS_Automation.xlsx", DEFAULT_SERVER),
        ("PCFS", "K-31-01", "tags_k31_01.txt", "PCFS/PCFS_Automation.xlsx", DEFAULT_SERVER),
        # ABF units
        ("ABF", "07-MT01-K001", "tags_abf_07mt01_k001.txt", "ABFSB/ABFSB_Automation_Master.xlsx", DEFAULT_SERVER),
        ("ABF", "21-K002", "tags_abf_21k002.txt", "ABFSB/ABF LIMIT REVIEW (CURRENT).xlsx", DEFAULT_SERVER),
        # PCMSB units
        ("PCMSB", "C-02001", "tags_pcmsb_c02001.txt", "PCMSB/PCMSB_Automation.xlsx", DEFAULT_SERVER),
        ("PCMSB", "C-104", "tags_pcmsb_c104.txt", "PCMSB/PCMSB_Automation.xlsx", DEFAULT_SERVER),
        ("PCMSB", "C-13001", "tags_pcmsb_c13001.txt", "PCMSB/PCMSB_Automation.xlsx", DEFAULT_SERVER),
        ("PCMSB", "C-1301", "tags_pcmsb_c1301.txt", "PCMSB/PCMSB_Automation.xlsx", DEFAULT_SERVER),
        ("PCMSB", "C-1302", "tags_pcmsb_c1302.txt", "PCMSB/PCMSB_Automation.xlsx", DEFAULT_SERVER),
        ("PCMSB", "C-201", "tags_pcmsb_c201.txt", "PCMSB/PCMSB_Automation.xlsx", DEFAULT_SERVER),
        ("PCMSB", "C-202", "tags_pcmsb_c202.txt", "PCMSB/PCMSB_Automation.xlsx", DEFAULT_SERVER),
        ("PCMSB", "XT-07002", "tags_pcmsb_xt07002.txt", "PCMSB/PCMSB_Automation.xlsx", DEFAULT_SERVER),
    ]

    print("="*80)
    print("INCREMENTAL REFRESH - ALL PLANTS")
    print("="*80)
    print("\nRefreshing all units with exact timestamp matching:")
    print("  PCFS:  K-12-01, K-16-01, K-19-01, K-31-01")
    print("  ABF:   07-MT01-K001, 21-K002")
    print("  PCMSB: C-02001, C-104, C-13001, C-1301, C-1302, C-201, C-202, XT-07002\n")

    def _format_duration(seconds: float) -> str:
        secs = int(round(seconds))
        mins, secs = divmod(secs, 60)
        hours, mins = divmod(mins, 60)
        if hours:
            return f"{hours}h {mins:02d}m {secs:02d}s"
        if mins:
            return f"{mins}m {secs:02d}s"
        return f"{secs}s"

    results: dict[str, bool | None] = {}
    durations: dict[str, float] = {}
    plant_totals: defaultdict[str, float] = defaultdict(float)

    for plant, unit, tags_file, excel_file, server in all_units:
        # Respect skip list for safe mode
        if (plant, unit) in SKIP_UNITS:
            reason = SKIP_UNITS[(plant, unit)]
            print(f"\n[-] Skipping {unit} ({plant}) â€” {reason}")
            results[unit] = None  # mark as skipped
            durations[unit] = 0.0
            continue

        tags_path = PROJECT_ROOT / "config" / tags_file
        xlsx_path = PROJECT_ROOT / "excel" / excel_file

        if not tags_path.exists():
            print(f"\n[X] Tags file not found: {tags_path}")
            results[unit] = False
            durations[unit] = 0.0
            continue

        if not xlsx_path.exists():
            print(f"\n[X] Excel file not found: {xlsx_path}")
            results[unit] = False
            durations[unit] = 0.0
            continue

        tags = read_tags(tags_path)
        if not tags:
            print(f"\n[X] No tags found in {tags_file}")
            results[unit] = False
            durations[unit] = 0.0
            continue

        # Prefer UNIT-specific override, then plant-specific, else default
        unit_key = unit.upper().replace('-', '_').replace('/', '_')
        unit_env = f"{plant.upper()}_{unit_key}_PI_SERVER"
        plant_env = f"{plant.upper()}_PI_SERVER"
        unit_override = os.getenv(unit_env)
        plant_override = os.getenv(plant_env)
        chosen_server = unit_override or plant_override or server
        if unit_override:
            print(f"\n[info] Using unit override {unit_env}: {chosen_server}")
        elif plant_override:
            print(f"\n[info] Using plant override {plant_env}: {chosen_server}")
        else:
            print(f"\n[info] Using default server for {plant}: {chosen_server}")

        start_time = time.perf_counter()
        result = incremental_refresh_unit(
            unit, tags, xlsx_path, plant, chosen_server
        )
        elapsed = time.perf_counter() - start_time
        results[unit] = result
        durations[unit] = elapsed
        plant_totals[plant] += elapsed

    # Summary
    print(f"\n{'='*80}")
    print("INCREMENTAL REFRESH SUMMARY")
    print(f"{'='*80}\n")

    print("Unit Results:")
    for plant, unit, *_ in all_units:
        success = results.get(unit)
        elapsed = durations.get(unit, 0.0)
        status = "[-] SKIPPED" if success is None else ("[OK] SUCCESS" if success else "[X] FAILED")
        print(f"  {status:<12} {plant}/{unit:<12} ({_format_duration(elapsed)})")

    if plant_totals:
        print("\nPlant Totals:")
        for plant, total in plant_totals.items():
            print(f"  {plant:<6}: {_format_duration(total)}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())


