# FETCH TROUBLESHOOTING GUIDE

## Overview

This document explains the **incremental refresh system** (Option [1] in TurboPredict CLI) and how to troubleshoot common fetch issues across all plants (PCFS, ABFSB, PCMSB).

---

## System Architecture

### How Incremental Refresh Works

1. **Smart Staleness Detection**: Only refreshes units with data > 1 hour old
2. **Excel PI DataLink Integration**: Updates formulas and refreshes data via COM automation
3. **Array Formula Management**: Handles PISampDat array formulas spanning thousands of rows
4. **Parquet Merge**: Appends new data to existing `.dedup.parquet` files
5. **Status Reporting**: Color-coded tables showing fresh vs stale units

### Key Files

- **turbopredict.py** - Main CLI, option [1] calls smart refresh
- **scripts/smart_incremental_refresh.py** - Orchestrates refresh for all 13 units
- **scripts/simple_incremental_refresh.py** - Core refresh logic per unit
- **excel/PCFS/PCFS_Automation.xlsx** - PCFS Excel with PI DataLink
- **excel/ABFSB/ABFSB_Automation_Master.xlsx** - ABF Excel with PI DataLink
- **excel/PCMSB.xlsx** - PCMSB Excel with PI DataLink
- **data/processed/*.dedup.parquet** - Master parquet databases

---

## Common Issues and Fixes

### Issue 1: ABF Returns 1970 Epoch Dates (CRITICAL BUG - FIXED)

**Symptom:**
```
Fetched data range: 1970-01-01 00:00:00.000045935 to 1970-01-01 00:00:00.000045940
Total rows fetched: 1121
After filtering: 0 rows (was 1121)
[i] No new data after refresh
```

**Root Cause:**

ABF Excel returns **Excel serial numbers** (floats like `45935.949`) instead of datetime objects. The original code used:

```python
df['time'] = pd.to_datetime(df['time'], errors='coerce')
```

Without specifying `unit` and `origin`, pandas treats floats as **seconds since Unix epoch (1970-01-01)**, resulting in invalid dates like `1970-01-01 00:00:00.000045935`.

**Why ABF Behaves Differently:**

- **PCFS/PCMSB**: xlwings automatically converts Excel dates to Python datetime objects
- **ABF**: xlwings returns raw float values (Excel serial dates) due to Excel workbook settings or regional formatting

**The Fix:**

Detect data type and convert appropriately:

```python
# Handle both datetime objects and Excel serial numbers
if df['time'].dtype == 'float64':
    # Excel serial dates - convert with proper origin
    df['time'] = pd.to_datetime(df['time'], unit='D', origin='1899-12-30', errors='coerce')
else:
    # Already datetime or string - use standard conversion
    df['time'] = pd.to_datetime(df['time'], errors='coerce')
```

**Excel Date System:**
- Excel epoch: **1899-12-30** (day 0)
- Excel serial `45935.949` = 45,935 days + 0.949 fractional day since 1899-12-30
- Converts to: **2025-10-05 22:46:40**

**Location:** `scripts/simple_incremental_refresh.py:219-224`

**Result:**
- ✅ ABF now successfully fetches and converts dates
- ✅ All 13 units working at 100% success rate

---

### Issue 2: "You Can't Change Part of an Array" Error

**Symptom:**
```
Error: You can't change part of an array
```

**Root Cause:**

PISampDat creates **array formulas** spanning many rows (e.g., A2:B87602). You cannot modify just A2:B2 when it's part of a larger array.

**The Fix:**

Clear the **entire used range** before updating formulas:

```python
# CRITICAL: Clear array formula first
used_range = sht.used_range
used_range.clear_contents()
time.sleep(2)  # Give Excel time to clear

# Then set new array formulas
sht.range(f"A2:A{estimated_rows + 1}").api.FormulaArray = formula_time
sht.range(f"B2:B{estimated_rows + 1}").api.FormulaArray = formula_value
```

**Location:** `scripts/simple_incremental_refresh.py:141-178`

---

### Issue 3: Only 1 Row Fetched Instead of Full Array

**Symptom:**
```
Total rows fetched: 1
Expected: 200+ rows
```

**Root Cause:**

Using `.formula` property only updates a single cell, not an array formula.

**The Fix:**

Use `.api.FormulaArray` over the entire range:

```python
# WRONG - only updates single cell
sht.range("A2").formula = formula_time

# CORRECT - creates array formula over range
estimated_rows = min(hours_to_fetch * 10 + 10, 87600)
sht.range(f"A2:A{estimated_rows + 1}").api.FormulaArray = formula_time
sht.range(f"B2:B{estimated_rows + 1}").api.FormulaArray = formula_value
```

**Array Size Calculation:**
- PI interval: `-0.1h` (6-minute intervals)
- Rows per hour: 10
- For 112 hours: ~1,120 rows

---

### Issue 4: Column B Returns Timestamps Instead of Values

**Symptom:**

Both columns A and B show Excel serial numbers (timestamps).

**Root Cause:**

Both columns used `mode=1` (timestamps parameter in PISampDat).

**The Fix:**

Split formulas - column A uses `mode=1`, column B uses `mode=0`:

```python
# Column A: timestamps (mode=1)
formula_time = f'=PISampDat("{unit_tag}","{start_time}","*","-0.1h",1,"{server}")'

# Column B: values (mode=0)
formula_value = f'=PISampDat("{unit_tag}","{start_time}","*","-0.1h",0,"{server}")'
```

**PISampDat Mode Parameter:**
- `mode=1` → Returns timestamps
- `mode=0` → Returns tag values

---

### Issue 5: Memory Allocation Error During Merge

**Symptom:**
```
Unable to allocate 117. MiB for an array with shape (1, 15368395)
```

**Root Cause:**

Existing parquet has `DatetimeIndex`, trying to concatenate with non-indexed data.

**The Fix:**

Reset index before merging:

```python
if isinstance(df_existing.index, pd.DatetimeIndex):
    df_existing = df_existing.reset_index()
    if 'index' in df_existing.columns:
        df_existing = df_existing.rename(columns={'index': 'time'})
```

**Location:** `scripts/simple_incremental_refresh.py:240-250`

---

## Best Practices for Option [1] Incremental Refresh

### 1. Always Run Full Build First

Before using incremental refresh, ensure base parquet files exist:

```bash
# For each unit, run its build script first
python scripts/build_pcfs_k1201.py
python scripts/build_abf_07mt01_k001.py
python scripts/build_pcmsb_c02001.py
# ... etc
```

This creates the `.dedup.parquet` files that incremental refresh appends to.

### 2. Understand the Unit Tag Map

Each unit needs a representative tag configured in `get_unit_tag()`:

```python
UNIT_TAG_MAP = {
    # PCFS units
    "K-12-01": "PCFS.K-12-01.12PI-007.PV",
    "K-16-01": "PCFS.K-16-01.16SI-501B.PV",
    "K-19-01": "PCFS.K-19-01.19SI-601B.PV",
    "K-31-01": "PCFS.K-31-01.31KI-302.PV",

    # ABF units
    "07-MT01-K001": "ABF.07-MT001.FI-07001.PV",

    # PCMSB units
    "C-02001": "PCM.C-02001.020FI0101.PV",
    "C-104": "PCM.C-104.XIAHH-1452B.PV",
    # ... etc
}
```

**If adding new units:**
1. Find the first tag from `config/tags_*.txt`
2. Add mapping to `get_unit_tag()`
3. Ensure Excel file exists with PI DataLink configured

### 3. Excel File Naming Convention

The system searches for Excel files in this order:

```python
excel_candidates = [
    PROJECT_ROOT / "excel" / plant / f"{plant}_Automation.xlsx",
    PROJECT_ROOT / "excel" / plant / f"{plant}_Automation_Master.xlsx",
    PROJECT_ROOT / "excel" / plant / "ABF_Automation.xlsx",  # ABF fallback
]
```

**Expected paths:**
- PCFS: `excel/PCFS/PCFS_Automation.xlsx`
- ABFSB: `excel/ABFSB/ABFSB_Automation_Master.xlsx`
- PCMSB: `excel/PCMSB.xlsx` (root level, not in subdirectory)

### 4. PI Server Configuration

Different plants may use different PI servers:

```python
server_map = {
    "PCFS": r"\\PTSG-1MMPDPdb01",
    "ABFSB": r"\\PTSG-1MMPDPdb01",  # Same as PCFS
    "PCMSB": r"\\PTSG-1MMPDPdb01",  # Same server for all
}
```

**Note:** ABF unit 21-K002 may need a different server - check with process engineers.

### 5. Fetch Window Sizing

The system calculates how much data to fetch based on staleness:

```python
hours_to_fetch = max(int(age_hours) + 12, 24)  # Add 12h buffer, min 24h
```

**Why the buffer?**
- Handles clock drift between systems
- Ensures overlap to prevent data gaps
- PI DataLink may have slight timing variations

### 6. Data Validation

After each fetch, the system validates:

```python
# Filter to only new data
df_before = len(df)
df = df[df['time'] > latest_time]
df_after = len(df)

if df_after == 0:
    print(f"  [i] No new data after filtering")
else:
    print(f"  [OK] Found {df_after} new rows")
    print(f"       Time range: {df['time'].min()} to {df['time'].max()}")
```

**Red flags:**
- `df_after == 0` when data is > 1 hour stale → fetch failed
- Large difference between `df_before` and `df_after` → clock drift issue
- Timestamps in the future → system time misconfiguration

### 7. Scheduled Task Integration

For automatic hourly refresh:

```bash
# Option 1: Use TurboPredict CLI (RECOMMENDED)
python turbopredict.py
# Select option [3] SCHEDULED TASK MANAGER
# Install task to run hourly

# Option 2: Manual setup
setup_scheduled_task.bat  # Run as Administrator
```

**Task runs even when:**
- Laptop is locked
- User is logged off
- Screen is off/sleeping

**Task logs location:** `logs/hourly_refresh_*.log`

### 8. Monitoring Fresh vs Stale Units

The smart refresh shows color-coded status:

```
+------------------------------------------------------------------------------+
|                     FRESH UNITS (13) - Data < 1 hour old                     |
+--------------+------------------+---------------+----------------------------+
|    STATUS    |      PLANT       |      UNIT     |          DATA AGE          |
+--------------+------------------+---------------+----------------------------+
| OK FRESH     | PCFS             | K-12-01       | 0.3h                       |
| OK FRESH     | ABFSB            | 07-MT01-K001  | 0.0h                       |
```

**Status colors:**
- 🟢 Green (< 1h): Fresh, no action needed
- 🟡 Yellow (1-24h): Stale, will refresh
- 🔴 Red (> 24h): Very stale, may need manual intervention

---

## Debugging Checklist

If a unit fails to refresh:

### Step 1: Check Excel File Exists
```bash
ls excel/PCFS/PCFS_Automation.xlsx
ls excel/ABFSB/ABFSB_Automation_Master.xlsx
ls excel/PCMSB.xlsx
```

### Step 2: Check Parquet File Exists
```bash
ls data/processed/K-12-01_1y_0p1h.dedup.parquet
ls data/processed/07-MT01-K001_1y_0p1h.dedup.parquet
```

### Step 3: Verify PI DataLink is Installed

Open Excel manually and check if PISampDat function exists:
```excel
=PISampDat("PCFS.K-12-01.12PI-007.PV","-1h","*","-0.1h",1,"\\PTSG-1MMPDPdb01")
```

Should return timestamps, not `#NAME?` error.

### Step 4: Check PI Server Connectivity

```bash
ping PTSG-1MMPDPdb01
```

If unreachable, VPN may be disconnected or server is down.

### Step 5: Inspect Excel Data Manually

```python
python -c "
import xlwings as xw
app = xw.App(visible=False)
try:
    wb = app.books.open(r'excel\PCFS\PCFS_Automation.xlsx')
    sht = wb.sheets['DL_WORK']
    print('A2 value:', sht.range('A2').value)
    print('A2 type:', type(sht.range('A2').value).__name__)
    print('A2 formula:', sht.range('A2').formula)
    wb.close()
finally:
    app.quit()
"
```

Expected output:
- **PCFS/PCMSB**: `datetime.datetime` object
- **ABF**: `float` (Excel serial number)

### Step 6: Test Single Unit Refresh

```bash
# Edit simple_incremental_refresh.py to comment out all but one unit
python scripts/simple_incremental_refresh.py 2>&1 | tee debug.log
```

Review `debug.log` for detailed error messages.

### Step 7: Check for Lock Files

```bash
ls excel/PCFS/~$*.xlsx
ls excel/ABFSB/~$*.xlsx
```

If Excel lock files exist, close all Excel instances:

```bash
taskkill /F /IM EXCEL.EXE
```

---

## Performance Optimization

### Current Performance (all units fresh):

```
Total runtime: ~3-5 seconds (status check only, no fetch)
Per-unit refresh: ~15-30 seconds (when stale)
Full 13-unit refresh: ~5-8 minutes (when all stale)
```

### Optimization Tips:

1. **Reduce settle time** if formulas refresh quickly:
   ```python
   time.sleep(5)  # Reduce from 10s if your Excel is fast
   ```

2. **Parallel refresh** (future enhancement):
   - Open multiple Excel instances
   - Refresh multiple units simultaneously
   - Risk: Excel COM threading issues

3. **DuckDB caching** for faster parquet reads:
   ```python
   import duckdb
   conn = duckdb.connect()
   result = conn.execute("SELECT MAX(time) FROM 'file.parquet'").fetchone()
   ```

4. **Incremental backups** to prevent data loss:
   ```python
   # Already implemented in merge logic
   shutil.copy2(parquet_file, parquet_file.with_suffix('.parquet.backup'))
   ```

---

## Success Indicators

✅ **All 13 units fresh** - System working perfectly
✅ **Color-coded status table with tag metrics** - Easy visual monitoring
✅ **Tag success rates** - Shows active/total tags (e.g., 90/121 = 74% success)
✅ **No epoch dates (1970)** - ABF date conversion working
✅ **Array formulas** - Full data range fetched
✅ **Parquet merge** - No memory errors
✅ **Scheduled task running** - Automatic hourly updates

### Interpreting the Enhanced Status Table

The status table now shows comprehensive tag health metrics:

```
+--------------------------------------------------------------------------------------------------+
|                               FRESH UNITS (13) - Data < 1 hour old                               |
+--------------+------------------+---------------+----------------------------+--------------------+
|    STATUS    |      PLANT       |      UNIT     |          DATA AGE          | TAGS (Active/Total)|
+--------------+------------------+---------------+----------------------------+--------------------+
| OK FRESH     | PCFS             | K-12-01       | 0.4h                       | 90/121             |
| OK FRESH     | PCMSB            | C-02001       | 0.3h                       | 245/280            |
+--------------+------------------+---------------+----------------------------+--------------------+
```

**TAGS Column Format:** `Active/Total`
- **Active**: Tags with non-null data in last 90 days
- **Total**: All tag columns in the parquet file

**Color Coding:**
- 🟢 **Green**: ≥90% tags active (excellent health) - e.g., `280/280` or `110/121`
- 🟡 **Yellow**: 70-89% tags active (good, some offline sensors) - e.g., `90/121`
- 🔴 **Red**: <70% tags active (poor, many failed sensors) - e.g., `50/200`

**Example Interpretations:**
- `90/121` (74%) → 🟡 Yellow → Some sensors may be offline or disconnected
- `280/280` (100%) → 🟢 Green → Perfect, all sensors reporting
- `50/200` (25%) → 🔴 Red → Major data quality issue, investigate tag connectivity

**When Stale Units Are Refreshed:**

After refresh completes, you'll see a summary table showing how many rows were added:

```
+--------------------------------------------------------------------------------------------------+
|                                      REFRESH RESULTS                                             |
+--------------+------------------------------------------+--------------------+---------------------+
|    STATUS    |                   UNIT                   | TAGS (Active/Total)|     ROWS ADDED      |
+--------------+------------------------------------------+--------------------+---------------------+
|    [OK]      | PCFS/K-12-01                             | 90/121             | 271                 |
|    [OK]      | PCFS/K-16-01                             | 121/121            | 239                 |
+--------------+------------------------------------------+--------------------+---------------------+
```

This enhanced visibility helps identify:
- **Data quality issues** at a glance (tag success rate)
- **Fetch effectiveness** (rows added per refresh)
- **Sensor health** (active vs total tags)

---

## Contact and Support

**If errors persist:**

1. Check this troubleshooting guide first
2. Review recent git commits for breaking changes
3. Inspect Excel files manually to verify PI DataLink connectivity
4. Check Windows Event Viewer for scheduled task errors
5. Review `logs/hourly_refresh_*.log` for detailed error traces

**Key maintainers:**
- Excel PI DataLink setup: Process engineering team
- Python refresh logic: `scripts/simple_incremental_refresh.py`
- Scheduled task: `scripts/hourly_refresh.py` + Windows Task Scheduler

---

---

## Issue 6: Tag Count Showing 0/0 for PCMSB and ABF Units (CRITICAL BUG - FIXED)

**Symptom:**
```
+------------------------------------------------------------------------------+
|                     STALE UNITS (13) – Data > 1 hour old                     |
+--------------+------------------+---------------+----------------------------+--------------------+
|    STATUS    |      PLANT       |      UNIT     |          DATA AGE          | TAGS (Active/Total)|
+--------------+------------------+---------------+----------------------------+--------------------+
| !! STALE     | PCFS             | K-12-01       | 8.1h                       | 56/56              |
| !! STALE     | ABFSB            | 07-MT01-K001  | 7.8h                       | 4/4                |
| !! STALE     | PCMSB            | C-02001       | 8.0h                       | 0/0                |
| !! STALE     | PCMSB            | XT-07002      | 7.9h                       | 0/0                |
+--------------+------------------+---------------+----------------------------+--------------------+
```

PCMS and ABF units showed **0/0 tags** even though they had valid data (e.g., C-02001 actually has 80 tags, XT-07002 has 116 tags).

**Root Cause:**

The `count_tags_in_parquet()` function in `scripts/simple_incremental_refresh.py` had **two critical bugs**:

### Bug 1: Wrong Data Format Assumption

The function expected **wide format** parquet files where each PI tag is a separate column:
```
| time                | tag1 | tag2 | tag3 | ... |
|---------------------|------|------|------|-----|
| 2025-10-10 08:00:00 | 12.5 | 34.2 | 56.7 | ... |
```

But the actual data is in **long format** with tags as row values:
```
| time                | plant | unit    | tag                      | value |
|---------------------|-------|---------|--------------------------|-------|
| 2025-10-10 08:00:00 | PCMSB | C-02001 | PCM.C-02001.020FI0101.PV | 12.5  |
| 2025-10-10 08:00:00 | PCMSB | C-02001 | PCM.C-02001.020FI1102.PV | 34.2  |
```

The original code tried to count tag columns:
```python
# WRONG - expects tags as columns
tag_cols = [col for col in df.columns if col != 'time']
total_tags = len(tag_cols)  # Returns 4 (plant, unit, tag, value) instead of 80
```

### Bug 2: Early Return from Partitioned Dataset Check

When checking for partitioned datasets, if a dataset directory existed but had no `tag=` subdirectories, the function returned `(0, 0)` immediately instead of falling through to check the flat parquet file:

```python
# Check partitioned dataset
if dataset_path.exists():
    tags = [d for d in os.listdir(dataset_path) if d.startswith('tag=')]
    total_tags = len(tags)
    return (total_tags, total_tags)  # BUG: Returns (0, 0) even if flat file exists!
```

**Why This Affected PCMSB But Not PCFS:**

- **PCFS units**: Have proper partitioned structure with `tag=PCFS_K-12-01_*` subdirectories
  ```
  data/processed/dataset/plant=PCFS/unit=K-12-01/
  ├── tag=PCFS_K-12-01_12FHC-004_MV/
  ├── tag=PCFS_K-12-01_12FI-004A_PV/
  └── ... (56 tag directories)
  ```

- **PCMSB C-02001**: Has dataset directory but with single `data.parquet` file instead of tag subdirectories
  ```
  data/processed/dataset/plant=PCMSB/unit=C-02001/
  └── data.parquet
  ```

**The Fix:**

Modified `count_tags_in_parquet()` to:

1. **Detect data format** and handle both wide and long formats:
```python
# Check if data is in long format (with 'tag' column) or wide format (tags as columns)
if 'tag' in df.columns:
    # LONG FORMAT: tags are stored as values in the 'tag' column
    unique_tags = df['tag'].dropna().unique()
    total_tags = len(unique_tags)

    # Count active tags (tags with data in last 90 days)
    cutoff = datetime.now() - timedelta(days=90)
    df['time'] = pd.to_datetime(df['time'], errors='coerce')
    recent_df = df[df['time'] > cutoff]
    active_tags = recent_df['tag'].dropna().nunique()

    return (total_tags, active_tags)
else:
    # WIDE FORMAT: each tag is a separate column (original logic)
    tag_cols = [col for col in df.columns if col != 'time']
    total_tags = len(tag_cols)
    # ... count active tags ...
```

2. **Only return from partitioned dataset check if tags found**:
```python
# Only return if we found tags - otherwise fall through to flat file
if total_tags > 0:
    active_tags = total_tags  # Conservative estimate
    return (total_tags, active_tags)
# Falls through to check flat parquet file if no tags found
```

**Location:** `scripts/simple_incremental_refresh.py:25-100`

**Verification Results:**

After the fix, all units now show correct tag counts:

```
PCFS:
  K-12-01              [OK]         56/56 tags
  K-16-01              [OK]         124/124 tags
  K-19-01              [OK]         149/149 tags
  K-31-01              [OK]         156/156 tags

ABFSB:
  07-MT01-K001         [OK]         125/125 tags

PCMSB:
  C-02001              [OK]         80/80 tags   ✅ FIXED (was 0/0)
  C-104                [OK]         60/60 tags   ✅ FIXED
  C-13001              [OK]         81/81 tags   ✅ FIXED
  C-1301               [PARTIAL]    59/60 tags
  C-1302               [OK]         82/82 tags   ✅ FIXED
  C-201                [OK]         77/77 tags   ✅ FIXED
  C-202                [OK]         54/54 tags   ✅ FIXED
  XT-07002             [OK]         116/116 tags ✅ FIXED (was 0/0)
```

**Impact:**

- ✅ **Tag counts now accurate** across all 13 units (PCFS, ABF, PCMSB)
- ✅ **Stale units table displays correctly** with proper Active/Total tag metrics
- ✅ **CLI option [1] works as expected** - detects and refreshes stale units with correct tag visibility
- ✅ **Both data formats supported** - handles wide format (PCFS) and long format (PCMSB/ABF) parquet files
- ✅ **Partitioned dataset fallback** - properly falls through to flat files when partitioned structure is incomplete

---

## Version History

**2025-10-10 (v3)**: Fixed tag counting for long-format parquet files (PCMSB/ABF now show correct counts)
**2025-10-10 (v2)**: Added tag success metrics to status tables (Active/Total display)
**2025-10-10 (v1)**: Fixed ABF epoch date issue (Excel serial date conversion)
**2025-10-09**: Implemented array formula management for PISampDat
**2025-10-08**: Added PCMSB support (8 units)
**2025-10-07**: Initial incremental refresh system

---

**End of FETCH_TROUBLESHOOTING.md**
