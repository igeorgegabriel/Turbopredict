"""Simple incremental refresh: Just refresh existing Excel and read new data"""
from pathlib import Path
import sys
import pandas as pd
from datetime import datetime, timedelta
import xlwings as xw
import time
import subprocess

PROJECT_ROOT = Path(__file__).parent.parent

def get_latest_timestamp(parquet_path: Path) -> datetime | None:
    """Get the latest timestamp from existing parquet file."""
    if not parquet_path.exists():
        return None
    try:
        df = pd.read_parquet(parquet_path)
        if isinstance(df.index, pd.DatetimeIndex) and len(df) > 0:
            return df.index.max().to_pydatetime()
        if 'time' in df.columns:
            return pd.to_datetime(df['time']).max().to_pydatetime()
    except Exception:
        pass
    return None

def check_tag_freshness(
    parquet_path: Path,
    max_age_hours: float = 1.0,
    allowed_tags: set[str] | None = None,
) -> tuple[bool, int, int]:
    """Check if at least 50% of tags have fresh data (< max_age_hours old).

    Returns:
        (is_fresh, fresh_tag_count, total_tag_count)
        is_fresh = True if >= 50% of tags have data within max_age_hours
    """
    if not parquet_path.exists():
        return (False, 0, 0)

    try:
        df = pd.read_parquet(parquet_path)

        # Ensure time column is datetime
        if 'time' not in df.columns:
            if isinstance(df.index, pd.DatetimeIndex):
                df = df.reset_index()
                if 'index' in df.columns:
                    df = df.rename(columns={'index': 'time'})
            else:
                return (False, 0, 0)

        df['time'] = pd.to_datetime(df['time'], errors='coerce')

        # Must have tag column
        if 'tag' not in df.columns:
            # Wide format - approximate per-tag freshness using column names
            tag_cols = [col for col in df.columns if col != 'time']
            if allowed_tags is not None:
                tag_cols = [col for col in tag_cols if _slug(str(col)) in allowed_tags]
                total_tags = len(allowed_tags)
            else:
                total_tags = len(tag_cols)

            if not tag_cols or total_tags == 0:
                return (False, 0, total_tags)

            cutoff_time = datetime.now() - timedelta(hours=max_age_hours)
            df['time'] = pd.to_datetime(df['time'], errors='coerce')
            fresh_count = 0

            for col in tag_cols:
                col_slug = _slug(str(col))
                if allowed_tags is not None and col_slug not in allowed_tags:
                    continue
                recent_values = df.loc[df['time'] > cutoff_time, col]
                if recent_values.notna().any():
                    fresh_count += 1

            if allowed_tags is not None:
                fresh_count = min(fresh_count, total_tags)

            is_fresh = (fresh_count / total_tags) >= 0.5 if total_tags > 0 else False
            return (is_fresh, fresh_count, total_tags)

        # Long format - check each tag's latest timestamp
        cutoff_time = datetime.now() - timedelta(hours=max_age_hours)

        # Filter out None/null tags (legacy data issue)
        df_with_tags = df[df['tag'].notna()]
        if allowed_tags is not None:
            df_with_tags = df_with_tags[df_with_tags['tag'].isin(allowed_tags)]
            if len(df_with_tags) == 0:
                total_tags = len(allowed_tags)
                return (False, 0, total_tags)

        if len(df_with_tags) == 0:
            # No valid tags - fall back to overall timestamp check
            # This handles legacy data where tag column is all None
            # (Silently fall back - this is expected for legacy single-tag data)
            latest = df['time'].max()
            age = datetime.now() - latest.to_pydatetime()
            is_fresh = age < timedelta(hours=max_age_hours)
            return (is_fresh, 1 if is_fresh else 0, 1)

        # Get latest timestamp per tag
        tag_latest = df_with_tags.groupby('tag')['time'].max()

        # Count how many tags are fresh
        if allowed_tags is not None:
            tag_latest_dict = tag_latest.to_dict()
            total_tags = len(allowed_tags)
            fresh_tags = sum(
                1
                for tag in allowed_tags
                if tag in tag_latest_dict and tag_latest_dict[tag] > cutoff_time
            )
        else:
            fresh_tags = (tag_latest > cutoff_time).sum()
            total_tags = len(tag_latest)

        # Require at least 50% of tags to be fresh
        freshness_threshold = 0.5
        is_fresh = (fresh_tags / total_tags) >= freshness_threshold if total_tags > 0 else False

        return (is_fresh, fresh_tags, total_tags)

    except Exception as e:
        print(f"[!] Error checking tag freshness: {e}")
        import traceback
        traceback.print_exc()
        return (False, 0, 0)

def count_tags_in_parquet(
    parquet_path: Path,
    unit: str = None,
    plant: str = None,
    allowed_tags: set[str] | None = None,
) -> tuple[int, int]:
    """Count total and active tags in parquet file or partitioned dataset.

    Returns:
        (total_tags, active_tags) where active = tags with data in last 90 days
    """
    # First try to read from partitioned dataset structure
    if unit and plant:
        dataset_path = PROJECT_ROOT / "data" / "processed" / "dataset" / f"plant={plant}" / f"unit={unit}"
        if dataset_path.exists():
            try:
                import os
                # Count tag directories
                tags = []
                for d in os.listdir(dataset_path):
                    if not d.startswith('tag='):
                        continue
                    tag_slug = d.split('tag=')[-1]
                    if allowed_tags is not None and tag_slug not in allowed_tags:
                        continue
                    tags.append(tag_slug)
                total_tags = len(tags)

                # Only return if we found tags - otherwise fall through to flat file
                if total_tags > 0:
                    # For now, assume all tags are active (full dataset scan would be slow)
                    # TODO: Implement actual activity check by reading recent data from each tag
                    active_tags = total_tags  # Conservative estimate
                    return (total_tags, active_tags)
            except Exception:
                pass  # Fall through to flat file check

    # Fallback to flat parquet file
    if not parquet_path.exists():
        return (0, 0)

    try:
        df = pd.read_parquet(parquet_path)

        # Check if data is in long format (with 'tag' column) or wide format (tags as columns)
        if 'tag' in df.columns:
            # LONG FORMAT: tags are stored as values in the 'tag' column
            # Get unique tags (filter out None values)
            df['tag'] = df['tag'].astype(str)
            df_with_tags = df[df['tag'].notna()]
            if allowed_tags is not None:
                df_with_tags = df_with_tags[df_with_tags['tag'].isin(allowed_tags)]

            unique_tags = df_with_tags['tag'].dropna().unique()
            total_tags = len(allowed_tags) if allowed_tags is not None else len(unique_tags)

            # Count active tags (tags with data in last 90 days)
            cutoff = datetime.now() - timedelta(days=90)

            if 'time' in df.columns:
                df['time'] = pd.to_datetime(df['time'], errors='coerce')
                recent_df = df[df['time'] > cutoff]
            elif isinstance(df.index, pd.DatetimeIndex):
                recent_df = df[df.index > cutoff]
            else:
                recent_df = df_with_tags

            # Count unique tags with non-null values in recent data
            if allowed_tags is not None:
                recent_tags = set(recent_df['tag'].dropna().unique())
                active_tags = sum(1 for tag in allowed_tags if tag in recent_tags)
            else:
                active_tags = recent_df['tag'].dropna().nunique()

            return (total_tags, active_tags)
        else:
            # WIDE FORMAT: each tag is a separate column
            # Get tag columns (exclude 'time' column)
            tag_cols = [col for col in df.columns if col != 'time']
            if allowed_tags is not None:
                tag_cols = [col for col in tag_cols if _slug(str(col)) in allowed_tags]
                total_tags = len(allowed_tags)
            else:
                total_tags = len(tag_cols)

            # Count active tags (non-null in last 90 days)
            cutoff = datetime.now() - timedelta(days=90)

            if isinstance(df.index, pd.DatetimeIndex):
                recent_df = df[df.index > cutoff]
            elif 'time' in df.columns:
                df['time'] = pd.to_datetime(df['time'])
                recent_df = df[df['time'] > cutoff]
            else:
                recent_df = df

            # Count columns with any non-null values
            if allowed_tags is not None:
                active_tags = sum(1 for col in tag_cols if recent_df[col].notna().any())
                active_tags = min(active_tags, len(allowed_tags))
            else:
                active_tags = (recent_df[tag_cols].notna().any()).sum()

            return (total_tags, active_tags)
    except Exception as e:
        import traceback
        print(f"ERROR in count_tags_in_parquet: {e}")
        traceback.print_exc()
        return (0, 0)

def get_unit_tags(unit: str, plant: str) -> list[str]:
    """Get ALL PI tags for a unit from config files.

    Returns:
        List of all tags configured for this unit
    """
    # Map of unit -> tag config file
    TAG_FILE_MAP = {
        # PCFS units (4)
        "K-12-01": "config/tags_k12_01.txt",
        "K-16-01": "config/tags_k16_01.txt",
        "K-19-01": "config/tags_k19_01.txt",
        "K-31-01": "config/tags_k31_01.txt",

        # ABF units (1)
        "07-MT01-K001": "config/tags_abf_07mt01_k001.txt",

        # PCMSB units (8)
        "C-02001": "config/tags_pcmsb_c02001.txt",
        "C-104": "config/tags_pcmsb_c104.txt",
        "C-13001": "config/tags_pcmsb_c13001.txt",
        "C-1301": "config/tags_pcmsb_c1301.txt",
        "C-1302": "config/tags_pcmsb_c1302.txt",
        "C-201": "config/tags_pcmsb_c201.txt",
        "C-202": "config/tags_pcmsb_c202.txt",
        "XT-07002": "config/tags_pcmsb_xt07002.txt",
    }

    tag_file = TAG_FILE_MAP.get(unit)
    if not tag_file:
        return []

    tag_path = PROJECT_ROOT / tag_file
    if not tag_path.exists():
        return []

    tags = []
    with open(tag_path, 'r') as f:
        for line in f:
            tag = line.strip()
            if tag and not tag.startswith('#'):
                tags.append(tag)

    return tags

def _slug(tag: str) -> str:
    """Convert tag name to slug for storage."""
    return tag.replace('.', '_').replace('-', '_').lower()

def simple_refresh_unit(unit: str, plant: str) -> dict:
    """Refresh Excel file and append new data to parquet FOR ALL TAGS.

    Returns:
        dict with keys: success (bool), total_tags (int), active_tags (int), rows_added (int)
    """

    # Get ALL tags for this unit
    unit_tags = get_unit_tags(unit, plant)
    if not unit_tags:
        print(f"[X] No tags configured for {unit}")
        print(f"    Please add tag configuration file")
        return {"success": False, "total_tags": 0, "active_tags": 0, "rows_added": 0}

    configured_tag_slugs = {_slug(tag) for tag in unit_tags}
    total_config_tags = len(unit_tags)

    print(f"[i] Found {total_config_tags} configured tags for {unit}")

    # Try multiple Excel file naming patterns
    excel_candidates = [
        PROJECT_ROOT / "excel" / plant / f"{plant}_Automation.xlsx",
        PROJECT_ROOT / "excel" / plant / f"{plant}_Automation_Master.xlsx",
        PROJECT_ROOT / "excel" / plant / "ABF_Automation.xlsx",  # ABF fallback
    ]

    excel_path = None
    for candidate in excel_candidates:
        if candidate.exists():
            excel_path = candidate
            break

    if not excel_path:
        print(f"[X] Excel file not found for {plant}")
        print(f"    Tried: {[str(c) for c in excel_candidates]}")
        return {
            "success": False,
            "total_tags": total_config_tags,
            "active_tags": 0,
            "rows_added": 0,
        }

    parquet_file = PROJECT_ROOT / "data" / "processed" / f"{unit}_1y_0p1h.dedup.parquet"

    if not parquet_file.exists():
        print(f"[X] Parquet file not found: {parquet_file}")
        print(f"    Run full build first to create database")
        return {
            "success": False,
            "total_tags": total_config_tags,
            "active_tags": 0,
            "rows_added": 0,
        }

    # Count tags in parquet (check partitioned dataset first, then flat file)
    dataset_total_tags, dataset_active_tags = count_tags_in_parquet(
        parquet_file,
        unit=unit,
        plant=plant,
        allowed_tags=configured_tag_slugs,
    )

    # Get latest timestamp (for display purposes)
    latest_time = get_latest_timestamp(parquet_file)
    if not latest_time:
        print(f"[X] Cannot read latest timestamp from {parquet_file}")
        return {
            "success": False,
            "total_tags": total_config_tags,
            "active_tags": dataset_active_tags,
            "rows_added": 0,
        }

    age = datetime.now() - latest_time.replace(tzinfo=None)

    # NEW: Check per-tag freshness (require at least 50% of tags to be fresh)
    is_fresh, fresh_tag_count, total_tag_count = check_tag_freshness(
        parquet_file,
        max_age_hours=1.0,
        allowed_tags=configured_tag_slugs,
    )

    print(f"\n{'='*80}")
    print(f"SIMPLE INCREMENTAL REFRESH: {unit}")
    print(f"{'='*80}")
    print(f"Excel file: {excel_path.name}")
    print(f"Latest data: {latest_time}")
    print(f"Overall age: {age}")
    print(f"Configured tags: {total_config_tags}")
    print(f"Dataset tags (last 90d): {dataset_active_tags}/{dataset_total_tags} active")
    print(f"Tag freshness (configured): {fresh_tag_count}/{total_tag_count} tags fresh (< 1h old)")
    print(f"Freshness threshold: >= 50% tags must be fresh")

    if is_fresh:
        freshness_pct = int(fresh_tag_count * 100 // total_tag_count) if total_tag_count > 0 else 0
        print(f"[OK] Data is fresh ({fresh_tag_count}/{total_tag_count} tags = {freshness_pct}% fresh) - no refresh needed")
        print(f"     Configured tags fresh: {fresh_tag_count}/{total_config_tags}")
        return {
            "success": True,
            "total_tags": total_config_tags,
            "active_tags": fresh_tag_count,
            "rows_added": 0,
        }
    else:
        print(f"[!] Data is STALE ({fresh_tag_count}/{total_tag_count} tags = {fresh_tag_count*100//total_tag_count if total_tag_count > 0 else 0}% fresh) - refresh needed!")

    print(f"\nOpening Excel and refreshing PI DataLink for {len(unit_tags)} tags...")

    # NEW: Get per-tag latest timestamps for accurate filtering
    print(f"Loading per-tag timestamps from parquet...")
    tag_latest_times = {}
    oldest_tag_time = latest_time  # Fallback for tags not in dict
    try:
        df_existing = pd.read_parquet(parquet_file)
        if 'tag' in df_existing.columns and 'time' in df_existing.columns:
            df_with_tags = df_existing[df_existing['tag'].notna()]
            if len(df_with_tags) > 0:
                tag_latest = df_with_tags.groupby('tag')['time'].max()
                tag_latest_times = tag_latest.to_dict()
                print(f"  Loaded timestamps for {len(tag_latest_times)} tags")

                # Get the OLDEST tag timestamp as fallback (to ensure we fetch ALL new data)
                oldest_tag_time = tag_latest.min()
                print(f"  Oldest tag timestamp: {oldest_tag_time}")
                print(f"  Newest tag timestamp: {tag_latest.max()}")
            else:
                print(f"  No valid tags found - will use overall timestamp")
    except Exception as e:
        print(f"  [!] Could not load per-tag timestamps: {e}")
        print(f"  Will use overall latest timestamp for all tags")

    # Calculate time range based on REAL data gaps (not just overall latest)
    # Use oldest_tag_time to ensure we fill ALL gaps
    if len(tag_latest_times) > 0:
        # Use the oldest tag timestamp to calculate the real gap
        real_gap = datetime.now() - oldest_tag_time.replace(tzinfo=None)
        hours_gap = real_gap.total_seconds() / 3600
        print(f"  Real data gap: {hours_gap:.1f} hours ({hours_gap/24:.1f} days)")
    else:
        # Fallback to overall age
        hours_gap = age.total_seconds() / 3600

    # Fetch with 10% buffer, but cap at reasonable limits
    hours_to_fetch = int(hours_gap * 1.1) + 1

    # Cap at 30 days (720 hours) for safety
    max_hours = 720
    if hours_to_fetch > max_hours:
        print(f"  [!] Gap is {hours_to_fetch:.0f}h ({hours_to_fetch/24:.1f} days) - capping at {max_hours/24:.0f} days")
        hours_to_fetch = max_hours

    start_time = f"-{hours_to_fetch}h"
    server = "\\\\PTSG-1MMPDPdb01"

    print(f"  Fetching last {hours_to_fetch} hours ({hours_to_fetch/24:.1f} days) of data")

    # Close any existing Excel instances with this file
    print(f"Checking for existing Excel instances...")
    closed_count = 0

    # First try graceful close via xlwings
    try:
        for app_inst in xw.apps:
            for book in list(app_inst.books):  # Use list() to avoid modification during iteration
                try:
                    if book.fullname == str(excel_path.absolute()):
                        print(f"  Closing existing instance of {excel_path.name}...")
                        book.close()
                        closed_count += 1
                except:
                    pass
            # Try to quit the app if it has no books
            try:
                if len(app_inst.books) == 0:
                    app_inst.quit()
            except:
                pass
    except Exception as e:
        print(f"  [!] Error during graceful close: {e}")

    # If we closed instances, wait for them to fully exit
    if closed_count > 0:
        print(f"  Closed {closed_count} existing instance(s), waiting 3s...")
        time.sleep(3)

    # Verify no Excel processes with our file are stuck
    # Check if there are any EXCEL.EXE processes and kill zombie ones if needed
    try:
        result = subprocess.run(['tasklist', '/FI', 'IMAGENAME eq EXCEL.EXE'],
                              capture_output=True, text=True, timeout=5)
        if 'EXCEL.EXE' in result.stdout:
            excel_count = result.stdout.count('EXCEL.EXE')
            if excel_count > 2:  # More than 2 is suspicious (could be stuck)
                print(f"  [!] Warning: {excel_count} Excel processes detected")
                print(f"  If refresh fails, you may need to manually close Excel")
    except:
        pass  # Tasklist not critical

    # Open Excel with fresh instance (with retry and cleanup)
    print(f"Starting fresh Excel instance...")
    app = None
    max_retries = 3
    for attempt in range(max_retries):
        try:
            app = xw.App(visible=False, add_book=False)
            app.display_alerts = False
            app.screen_updating = False  # Disable screen updates for performance
            print(f"  Excel instance created successfully")
            break
        except Exception as e:
            print(f"  [Attempt {attempt+1}/{max_retries}] Excel creation failed: {e}")
            if attempt < max_retries - 1:
                print(f"  Cleaning up stuck Excel processes and retrying...")
                try:
                    subprocess.run(['taskkill', '/F', '/IM', 'EXCEL.EXE'],
                                 capture_output=True, timeout=10)
                    time.sleep(3)
                except:
                    pass
            else:
                print(f"  ERROR: Failed to create Excel instance after {max_retries} attempts")
                raise

    all_new_data = []  # Collect data from all tags
    total_rows_added = 0
    tags_with_new_data = 0
    tags_with_any_rows = 0

    try:
        # Open with update_links=False to avoid prompts
        wb = app.books.open(str(excel_path.absolute()), update_links=False, read_only=False)

        # Wait for PI DataLink to initialize
        print(f"Waiting 5s for PI DataLink...")
        time.sleep(5)

        # Access sheet with retry logic (RPC errors are common)
        max_retries = 3
        sht = None
        for retry in range(max_retries):
            try:
                sht = wb.sheets["DL_WORK"]
                break
            except Exception as e:
                if "RPC server" in str(e) and retry < max_retries - 1:
                    print(f"  [!] RPC error accessing sheet, retrying in 3s... (attempt {retry+1}/{max_retries})")
                    time.sleep(3)
                else:
                    raise

        if sht is None:
            raise RuntimeError("Could not access DL_WORK sheet after retries")

        # BATCH PROCESSING: Fetch 10 tags at a time for performance
        # Balanced between speed (10x faster) and stability
        BATCH_SIZE = 10
        num_batches = (len(unit_tags) + BATCH_SIZE - 1) // BATCH_SIZE

        print(f"\n[BATCH MODE] Processing {len(unit_tags)} tags in batches of {BATCH_SIZE}")
        print(f"[BATCH MODE] Total batches: {num_batches}")
        print(f"[BATCH MODE] Performance: ~10x faster than single-tag fetching")

        for batch_num in range(num_batches):
            # Get tags for this batch
            start_idx = batch_num * BATCH_SIZE
            end_idx = min(start_idx + BATCH_SIZE, len(unit_tags))
            batch_tags = unit_tags[start_idx:end_idx]

            print(f"\n{'='*80}")
            print(f"BATCH [{batch_num+1}/{num_batches}] - Tags {start_idx+1} to {end_idx} of {len(unit_tags)}")
            print(f"{'='*80}")
            for i, tag in enumerate(batch_tags, 1):
                print(f"  [{i}] {tag}")

            # Clear worksheet (all columns we'll use)
            try:
                # Clear up to 20 columns (10 tags × 2 columns each: A-T)
                sht.range("A2:T100000").clear_contents()
            except:
                pass
            time.sleep(1)

            # Estimate rows: 6-minute step -> 10 rows/hour approx (60/6)
            est_rows = int(max(1, min(87600, (hours_to_fetch * 10) + 20)))
            end_row = 1 + 1 + est_rows  # header row at 1, data starts at A2

            # Set formulas for all tags in this batch
            for i, tag in enumerate(batch_tags):
                col_offset = i * 2  # Each tag uses 2 columns (time + value)
                col_time = chr(65 + col_offset)  # A, C, E
                col_value = chr(65 + col_offset + 1)  # B, D, F

                formula_time = f'=PISampDat("{tag}","{start_time}","*","-0.1h",1,"{server}")'
                formula_value = f'=PISampDat("{tag}","{start_time}","*","-0.1h",0,"{server}")'

                # Use array formulas
                try:
                    sht.range(f"{col_time}2:{col_time}{end_row}").formula_array = formula_time
                    sht.range(f"{col_value}2:{col_value}{end_row}").formula_array = formula_value
                except Exception:
                    try:
                        # Fallback to formula2 (dynamic spill)
                        sht.range(f"{col_time}2").formula2 = formula_time
                        sht.range(f"{col_value}2").formula2 = formula_value
                    except Exception:
                        # Last resort: single-cell formulas
                        sht.range(f"{col_time}2").formula = formula_time
                        sht.range(f"{col_value}2").formula = formula_value

            print(f"[BATCH] Formulas written for {len(batch_tags)} tags, saving and refreshing...")

            # Save and refresh ONCE for all tags in batch
            wb.save()
            time.sleep(1)
            wb.api.RefreshAll()

            # Wait for refresh to complete
            print(f"[BATCH] Waiting for PI DataLink refresh...")
            time.sleep(5)
            app.api.CalculateUntilAsyncQueriesDone()
            time.sleep(2)

            # Process each tag in the batch
            for i, tag in enumerate(batch_tags):
                tag_idx = start_idx + i + 1
                col_offset = i * 2
                col_time = chr(65 + col_offset)  # A, C, E
                col_value = chr(65 + col_offset + 1)  # B, D, F

                print(f"\n[{tag_idx}/{len(unit_tags)}] Processing: {tag}")

                # Read data for this tag
                data = sht.range(f"{col_time}2:{col_value}87602").value

                # Convert to rows
                rows = []
                for row in data:
                    if row[0] is not None and row[1] is not None:
                        if not isinstance(row[0], str) and not isinstance(row[1], str):
                            rows.append((row[0], row[1]))

                if not rows:
                    print(f"  [!] No data for tag: {tag}")
                    continue

                tags_with_any_rows += 1

                # Create dataframe
                df = pd.DataFrame(rows, columns=['time', 'value'])

                # Convert timestamps
                if df['time'].dtype == 'float64':
                    df['time'] = pd.to_datetime(df['time'], unit='D', origin='1899-12-30', errors='coerce')
                else:
                    df['time'] = pd.to_datetime(df['time'], errors='coerce')

                df = df.dropna(subset=['time'])

                if df.empty:
                    print(f"  [!] Empty after time parsing")
                    continue

                # Filter to only new data using THIS TAG's latest timestamp
                tag_slug = _slug(tag)
                tag_latest = tag_latest_times.get(tag_slug, oldest_tag_time)

                df_new = df[df['time'] > tag_latest]

                if df_new.empty:
                    print(f"  [i] No new data (all <= {tag_latest})")
                    continue

                tags_with_new_data += 1

                # Add metadata (avoid SettingWithCopyWarning)
                df_new = df_new.copy()
                df_new.loc[:, 'plant'] = plant
                df_new.loc[:, 'unit'] = unit
                df_new.loc[:, 'tag'] = _slug(tag)

                print(f"  [OK] {len(df_new)} new rows ({df_new['time'].min()} to {df_new['time'].max()})")

                all_new_data.append(df_new)
                total_rows_added += len(df_new)

        # Close Excel
        try:
            wb.close()
        except:
            pass
        try:
            app.quit()
        except:
            pass
        time.sleep(2)

        # Process all collected data
        if not all_new_data:
            print(f"\n[i] No new data from any tag")
            print(f"     Tags returning data: {tags_with_any_rows}/{total_config_tags}")
            return {
                "success": True,
                "total_tags": total_config_tags,
                "active_tags": tags_with_new_data,
                "rows_added": 0,
            }

        print(f"\n[OK] Collected {total_rows_added} new rows from {len(all_new_data)} tags")

        # Combine all new data
        df_all_new = pd.concat(all_new_data, ignore_index=True)

        # Reorder columns to match existing format
        df_all_new = df_all_new[['time', 'value', 'plant', 'unit', 'tag']]

        # Load existing data and merge
        print(f"Merging with existing parquet...")
        df_existing = pd.read_parquet(parquet_file)

        # Ensure existing data is in correct format
        if isinstance(df_existing.index, pd.DatetimeIndex):
            df_existing = df_existing.reset_index()
            if 'index' in df_existing.columns:
                df_existing = df_existing.rename(columns={'index': 'time'})

        # Append and deduplicate
        df_combined = pd.concat([df_existing, df_all_new], ignore_index=True)
        df_combined = df_combined.drop_duplicates(subset=['time', 'tag'], keep='last')
        df_combined = df_combined.sort_values(['tag', 'time']).reset_index(drop=True)

        # Save
        df_combined.to_parquet(parquet_file, index=False, compression='snappy')
        print(f"[OK] Updated {parquet_file.name}")
        print(f"     Total rows: {len(df_combined):,}")
        print(f"     New rows added: {total_rows_added:,}")
        print(f"     Tags with new data this run: {tags_with_new_data}/{total_config_tags}")
        print(f"     Tags returning any data: {tags_with_any_rows}/{total_config_tags}")

        return {
            "success": True,
            "total_tags": total_config_tags,
            "active_tags": tags_with_new_data,
            "rows_added": total_rows_added,
        }

    except Exception as e:
        print(f"[X] Error: {e}")
        import traceback
        traceback.print_exc()
        # Force close Excel
        try:
            for app_inst in xw.apps:
                for book in list(app_inst.books):
                    try:
                        book.close()
                    except:
                        pass
                try:
                    app_inst.quit()
                except:
                    pass
        except:
            pass
        time.sleep(2)
        return {
            "success": False,
            "total_tags": total_config_tags,
            "active_tags": tags_with_new_data,
            "rows_added": 0,
        }

if __name__ == "__main__":
    # All plants and units
    UNITS = {
        "PCFS": ["K-12-01", "K-16-01", "K-19-01", "K-31-01"],
        "ABFSB": ["07-MT01-K001"],
        "PCMSB": ["C-02001", "C-104", "C-13001", "C-1301", "C-1302", "C-201", "C-202", "XT-07002"],
    }

    print("=" * 80)
    print("SIMPLE INCREMENTAL REFRESH - ALL PLANTS")
    print("=" * 80)
    print(f"PCFS: {len(UNITS['PCFS'])} units")
    print(f"ABF:  {len(UNITS['ABFSB'])} units")
    print(f"PCMSB: {len(UNITS['PCMSB'])} units")
    print("=" * 80)

    results = {}
    for plant, units in UNITS.items():
        for unit in units:
            print()  # Blank line between units
            success = simple_refresh_unit(unit, plant)
            results[f"{plant}/{unit}"] = success

    print("\n" + "=" * 80)
    print("SUMMARY")
    print("=" * 80)

    # Group by plant
    for plant in ["PCFS", "ABFSB", "PCMSB"]:
        print(f"\n{plant}:")
        for key, success in results.items():
            if key.startswith(plant + "/"):
                unit = key.split("/")[1]
                status = "✓ OK" if success else "✗ FAILED"
                print(f"  {status}  {unit}")

    total = len(results)
    success_count = sum(1 for s in results.values() if s)
    print(f"\nTotal: {success_count}/{total} units refreshed successfully")

    sys.exit(0 if all(results.values()) else 1)
