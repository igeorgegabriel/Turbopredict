"""Simple plant-level refresh: Refresh Excel once, extract all units"""
from pathlib import Path
import sys
import pandas as pd
from datetime import datetime, timedelta
import xlwings as xw
import time

PROJECT_ROOT = Path(__file__).parent.parent

def refresh_plant_excel(plant: str, excel_path: Path) -> pd.DataFrame | None:
    """Refresh Excel and read all data from Sheet1."""

    print(f"\n{'='*80}")
    print(f"REFRESHING {plant} EXCEL FILE")
    print(f"{'='*80}")
    print(f"Excel: {excel_path.name}")

    app = xw.App(visible=False, add_book=False)
    app.display_alerts = False

    try:
        wb = app.books.open(str(excel_path.absolute()))

        # Wait for PI DataLink to initialize
        print(f"Waiting 5s for PI DataLink...")
        time.sleep(5)

        # Trigger RefreshAll
        print(f"Refreshing all data (this may take a few minutes)...")
        wb.api.RefreshAll()

        # Wait for refresh to complete
        print(f"Waiting for refresh to complete...")
        time.sleep(30)
        app.api.CalculateUntilAsyncQueriesDone()
        time.sleep(10)

        # Read Sheet1 which contains all the data
        print(f"Reading data from Sheet1...")
        sht = wb.sheets["Sheet1"]

        # Get used range to determine size
        used_range = sht.used_range
        print(f"  Used range: {used_range.address}")

        # Read all data
        data = used_range.value

        wb.close()
        app.quit()

        # Convert to DataFrame
        # First row is headers
        if not data or len(data) < 2:
            print("[X] No data in Sheet1")
            return None

        headers = data[0]
        rows = data[1:]

        df = pd.DataFrame(rows, columns=headers)
        print(f"[OK] Read {len(df):,} rows with {len(df.columns)} columns")
        print(f"  Columns: {list(df.columns)[:10]}...")  # Show first 10 columns

        return df

    except Exception as e:
        print(f"[X] Error: {e}")
        import traceback
        traceback.print_exc()
        try:
            wb.close()
            app.quit()
        except:
            pass
        return None

def extract_unit_data(df: pd.DataFrame, unit: str, plant: str) -> pd.DataFrame | None:
    """Extract data for specific unit from combined dataframe."""

    # Find column that matches unit pattern
    # Look for columns like "PCFS K-12-01 ST_PERFORMANCE"
    unit_columns = [col for col in df.columns if isinstance(col, str) and unit in col]

    if not unit_columns:
        print(f"  [X] No columns found for unit {unit}")
        return None

    # Use first matching column
    value_col = unit_columns[0]
    time_col = None

    # Find TIME column (usually column B, next to the first None/empty column A)
    for col in df.columns:
        if col and isinstance(col, str) and 'TIME' in col.upper():
            time_col = col
            break

    if not time_col:
        print(f"  [X] No TIME column found")
        return None

    # Extract time and value columns
    unit_df = df[[time_col, value_col]].copy()
    unit_df.columns = ['time', 'value']

    # Remove null rows
    unit_df = unit_df.dropna()

    # Convert time to datetime
    unit_df['time'] = pd.to_datetime(unit_df['time'])

    print(f"  [OK] Extracted {len(unit_df):,} rows for {unit}")
    return unit_df

def update_unit_parquet(unit: str, plant: str, new_data: pd.DataFrame) -> bool:
    """Update unit parquet file with new data."""

    parquet_file = PROJECT_ROOT / "data" / "processed" / f"{unit}_1y_0p1h.dedup.parquet"

    if not parquet_file.exists():
        print(f"  [X] Parquet file not found: {parquet_file.name}")
        return False

    # Get latest timestamp from existing data
    try:
        df_existing = pd.read_parquet(parquet_file)

        # Handle indexed data
        if isinstance(df_existing.index, pd.DatetimeIndex):
            df_existing = df_existing.reset_index()
            if 'index' in df_existing.columns:
                df_existing = df_existing.rename(columns={'index': 'time'})

        latest_time = pd.to_datetime(df_existing['time']).max()

        # Filter to only new rows
        new_rows = new_data[new_data['time'] > latest_time]

        if new_rows.empty:
            print(f"  [i] No new data (latest: {latest_time})")
            return True

        print(f"  [+] {len(new_rows)} new rows (after {latest_time})")

        # Merge and save
        df_combined = pd.concat([df_existing, new_rows], ignore_index=True)
        df_combined = df_combined.drop_duplicates(subset=['time'], keep='last')
        df_combined = df_combined.sort_values('time').reset_index(drop=True)

        df_combined.to_parquet(parquet_file, index=False, compression='snappy')
        print(f"  [OK] Updated {parquet_file.name} ({len(df_combined):,} total rows)")

        return True

    except Exception as e:
        print(f"  [X] Error updating parquet: {e}")
        return False

if __name__ == "__main__":
    # Configuration
    PLANTS = {
        "PCFS": {
            "excel": PROJECT_ROOT / "excel" / "PCFS" / "PCFS_Automation.xlsx",
            "units": ["K-12-01", "K-16-01", "K-19-01", "K-31-01"]
        },
    }

    print("=" * 80)
    print("SIMPLE PLANT-LEVEL REFRESH")
    print("=" * 80)

    results = {}

    for plant, config in PLANTS.items():
        excel_path = config["excel"]
        units = config["units"]

        if not excel_path.exists():
            print(f"[X] Excel file not found: {excel_path}")
            continue

        # Refresh Excel once for entire plant
        df_all = refresh_plant_excel(plant, excel_path)

        if df_all is None:
            print(f"[X] Failed to refresh {plant}")
            for unit in units:
                results[f"{plant}/{unit}"] = False
            continue

        # Extract and update each unit
        print(f"\nExtracting unit data...")
        for unit in units:
            print(f"\n{unit}:")
            unit_data = extract_unit_data(df_all, unit, plant)

            if unit_data is not None and not unit_data.empty:
                success = update_unit_parquet(unit, plant, unit_data)
                results[f"{plant}/{unit}"] = success
            else:
                results[f"{plant}/{unit}"] = False

    # Summary
    print("\n" + "=" * 80)
    print("SUMMARY")
    print("=" * 80)

    for key, success in results.items():
        status = "✓ OK" if success else "✗ FAILED"
        print(f"{status}  {key}")

    total = len(results)
    success_count = sum(1 for s in results.values() if s)
    print(f"\nTotal: {success_count}/{total} units updated successfully")

    sys.exit(0 if all(results.values()) else 1)
