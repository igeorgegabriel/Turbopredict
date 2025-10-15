# PI DATA FETCHING - Complete Guide
**Last Updated: October 2025**

Comprehensive guide for TurboPredict's PI data fetching system. This document covers setup, operation, troubleshooting, and performance optimization for Option [1] incremental refresh across all plants (PCFS, ABFSB, PCMSB).

---

## Table of Contents

1. [Quick Start](#quick-start)
2. [System Architecture](#system-architecture)
3. [Configuration & Environment](#configuration--environment)
4. [Batch Processing Performance](#batch-processing-performance)
5. [Common Issues & Fixes](#common-issues--fixes)
6. [Best Practices](#best-practices)
7. [Performance Optimization](#performance-optimization)
8. [Troubleshooting Checklist](#troubleshooting-checklist)
9. [Version History](#version-history)

---

## Quick Start

### Running Option [1] - Smart Incremental Refresh

```bash
# Method 1: TurboPredict CLI (recommended)
python turbopredict.py
# Select option [1] SMART INCREMENTAL REFRESH

# Method 2: Direct script execution
python scripts/smart_incremental_refresh.py
```

### Expected Behavior

- **Fresh units** (data < 1 hour old): Skipped, no refresh needed
- **Stale units** (data > 1 hour old): Automatically refreshed via Excel/PI DataLink
- **All 13 units**: Completes in ~25-30 minutes with 10-tag batch processing

### Known Good Environment Settings

```cmd
set PI_WEBAPI_URL=
set EXCEL_VISIBLE=1
set PI_FETCH_TIMEOUT=45
set PI_FETCH_LINGER=10
set PCFS_PI_SERVER=\\PTSG-1MMPDPdb01
set ABF_21_K002_PI_SERVER=\\VSARMNGPIMDB01
set ABF_07_MT01_K001_PI_SERVER=\\PTSG-1MMPDPdb01
set PCMSB_PI_SERVER=\\PTSG-1MMPDPdb01
```

---

## System Architecture

### How Incremental Refresh Works

```
┌─────────────────────────────────────────────────────────────────────┐
│ 1. STALENESS CHECK                                                  │
│    Check latest timestamp in each unit's .dedup.parquet file        │
│    Classify units: FRESH (<1h old) vs STALE (>1h old)              │
└─────────────────────────────────────────────────────────────────────┘
                                ↓
┌─────────────────────────────────────────────────────────────────────┐
│ 2. BATCH PROCESSING (10 tags per batch)                            │
│    For each stale unit:                                             │
│    - Open Excel with PI DataLink                                    │
│    - Write PISampDat formulas for 10 tags (columns A-T)            │
│    - Refresh all formulas with single RefreshAll() call            │
│    - Read and process data for each tag                             │
│    - Repeat for next batch until all tags processed                 │
└─────────────────────────────────────────────────────────────────────┘
                                ↓
┌─────────────────────────────────────────────────────────────────────┐
│ 3. DATA MERGE & VALIDATION                                          │
│    - Filter to only new data (time > latest_timestamp)             │
│    - Append to existing .dedup.parquet file                         │
│    - Deduplicate based on (time, tag)                               │
│    - Report rows added and tag success rate                         │
└─────────────────────────────────────────────────────────────────────┘
```

### Key Files

| File | Purpose |
|------|---------|
| `turbopredict.py` | Main CLI interface (Options 1-3) |
| `scripts/smart_incremental_refresh.py` | Orchestrates refresh for all 13 units |
| `scripts/simple_incremental_refresh.py` | Core refresh logic with batch processing |
| `excel/PCFS/PCFS_Automation.xlsx` | PCFS Excel with PI DataLink formulas |
| `excel/ABFSB/ABFSB_Automation_Master.xlsx` | ABF Excel with PI DataLink formulas |
| `excel/PCMSB.xlsx` | PCMSB Excel with PI DataLink formulas |
| `data/processed/*.dedup.parquet` | Master parquet databases (13 units) |
| `config/tags_*.txt` | Tag configuration files for each unit |

---

## Configuration & Environment

### PI Server Configuration

Different plants use different PI servers:

```python
# Per-unit/per-plant PI server overrides
PCFS_PI_SERVER=\\PTSG-1MMPDPdb01
ABF_21_K002_PI_SERVER=\\VSARMNGPIMDB01
ABF_07_MT01_K001_PI_SERVER=\\PTSG-1MMPDPdb01
PCMSB_PI_SERVER=\\PTSG-1MMPDPdb01
```

Priority: `PLANT_UNIT_PI_SERVER` > `PLANT_PI_SERVER` > default

### Excel File Naming Convention

The system searches for Excel files in this order:

```python
excel_candidates = [
    "excel/{plant}/{plant}_Automation.xlsx",
    "excel/{plant}/{plant}_Automation_Master.xlsx",
    "excel/{plant}/ABF_Automation.xlsx",  # ABF fallback
]
```

**Expected paths:**
- PCFS: `excel/PCFS/PCFS_Automation.xlsx`
- ABFSB: `excel/ABFSB/ABFSB_Automation_Master.xlsx`
- PCMSB: `excel/PCMSB.xlsx` (root level)

### Web API Configuration

**IMPORTANT**: Web API is disabled for Option [1] to ensure Excel-only fetching:

```cmd
set PI_WEBAPI_URL=
```

This prevents the system from attempting slow Web API calls that timeout and fallback to Excel anyway.

---

## Batch Processing Performance

### Overview

As of October 2025, the system uses **10-tag batch processing** for optimal performance and stability.

### What is Batch Processing?

Instead of fetching 1 tag at a time:
```
Tag 1 → Write formula → Save → Refresh → Wait → Read → Process (8s)
Tag 2 → Write formula → Save → Refresh → Wait → Read → Process (8s)
...
125 tags × 8s = ~17 minutes
```

Batch processing fetches **10 tags simultaneously**:
```
Batch 1 (Tags 1-10):
  → Write formulas A/B, C/D, E/F, G/H, I/J, K/L, M/N, O/P, Q/R, S/T
  → Save ONCE → Refresh ONCE → Wait ONCE (8s)
  → Read and process all 10 tags
125 tags ÷ 10 = 13 batches × 8s = ~2 minutes
```

### Performance Comparison

| Method | K-16-01 (124 tags) | All 13 Units | Speedup |
|--------|-------------------|--------------|---------|
| 1 tag/batch (old) | ~17 minutes | 3-4 hours | 1x |
| 3 tags/batch | ~6 minutes | ~1 hour | 3x |
| **10 tags/batch (current)** | **~2 minutes** | **~25-30 minutes** | **~10x** |
| 20 tags/batch | ~1 minute | ~15 minutes | ~20x |

### Batch Size Configuration

**Location**: `scripts/simple_incremental_refresh.py` line 547

```python
# BATCH PROCESSING: Fetch 10 tags at a time for performance
# Balanced between speed (10x faster) and stability
BATCH_SIZE = 10
```

### Recommended Batch Sizes by Scenario

| Batch Size | Use Case | Risk | Notes |
|-----------|----------|------|-------|
| **10 (default)** | **Production 24/7 operation** | **LOW** ✅ | **Recommended - proven stable** |
| 15 | Need faster refresh cycles | MEDIUM | Test with monitoring first |
| 20 | Maximum speed for urgent needs | MEDIUM-HIGH | Only if 10 is insufficient |
| 30+ | Not recommended | HIGH ❌ | Risk of Excel/PI server issues |

### Technical Constraints

| Constraint | Limit | 10-Tag Impact | 20-Tag Impact |
|-----------|-------|---------------|---------------|
| Excel columns | 16,384 (A-XFD) | 20 cols (A-T) ✅ | 40 cols (A-AN) ✅ |
| Excel memory | Millions of cells | 1.75M cells ✅ | 3.5M cells ⚠️ |
| PI server load | ~50 concurrent | 10 queries ✅ | 20 queries ⚠️ |
| Formula refresh | PI timeout limit | ~45s ✅ | ~45s ⚠️ |

### How to Adjust Batch Size

**To increase batch size** (e.g., from 10 to 15):

1. Edit `scripts/simple_incremental_refresh.py`:
   ```python
   BATCH_SIZE = 15  # Changed from 10
   ```

2. Update worksheet clear range:
   ```python
   # Clear up to 30 columns (15 tags × 2 columns each: A-AD)
   sht.range("A2:AD100000").clear_contents()
   ```

3. Test with **single unit first**:
   ```bash
   # Monitor for errors, timeouts, memory issues
   python scripts/smart_incremental_refresh.py
   ```

4. If successful for 1 week, keep the new batch size
5. If issues occur, rollback to `BATCH_SIZE = 10`

**To decrease batch size** (e.g., if experiencing issues):

```python
BATCH_SIZE = 5  # More conservative
# Clear range: A2:J100000 (5 tags × 2 columns)
```

### When NOT to Increase Batch Size

- ❌ During peak PI server usage hours (daytime operations)
- ❌ When network is unstable or VPN connection is poor
- ❌ After Windows/Excel updates (wait for stability)
- ❌ For 24/7 unattended operation (stability > speed)

### Column Mapping Reference

| Batch Size | Columns Used | Range | Excel Columns |
|-----------|--------------|-------|---------------|
| 1 tag | 2 (time + value) | A-B | A:B |
| 3 tags | 6 | A-F | A:F |
| 5 tags | 10 | A-J | A:J |
| **10 tags** | **20** | **A-T** | **A:T** |
| 15 tags | 30 | A-AD | A:AD |
| 20 tags | 40 | A-AN | A:AN |

### Performance Monitoring

After changing batch size, monitor for:

1. **Excel memory usage** (Task Manager → EXCEL.EXE)
2. **Timeout errors** (should complete within 45s per batch)
3. **Data quality** (rows added should match historical averages)
4. **Tag success rate** (Active/Total should remain high)

### Validation Results (10-Tag Batching)

**Date**: October 15, 2025
**Batch Size**: 10 tags
**Status**: ✅ Proven stable in production

| Unit | Tags | Batches | Time/Unit | Rows Added | Status |
|------|------|---------|-----------|------------|--------|
| K-12-01 | 56 | 6 | ~2 min | 6,387 | ✅ |
| K-16-01 | 124 | 13 | ~2.5 min | 612,641 | ✅ |
| K-19-01 | 149 | 15 | ~3 min | 754,929 | ✅ |
| K-31-01 | 156 | 16 | ~3 min | 792,227 | ✅ |
| 07-MT01-K001 | 125 | 13 | ~2.5 min | 889,915 | ✅ |
| C-02001 | 80 | 8 | ~2 min | 272,057 | ✅ |
| C-104 | 60 | 6 | ~1.5 min | 394,505 | ✅ |
| C-13001 | 81 | 9 | ~2 min | 499,196 | ✅ |
| C-1301 | 59 | 6 | ~1.5 min | 410,457 | ✅ |
| C-1302 | 82 | 9 | ~2 min | 505,337 | ✅ |
| C-201 | 77 | 8 | ~2 min | 532,871 | ✅ |
| C-202 | 54 | 6 | ~1.5 min | 328,404 | ✅ |
| XT-07002 | 116 | 12 | ~2.5 min | 713,726 | ✅ |

**Total**: 13 units refreshed successfully in ~25-30 minutes

---

## Common Issues & Fixes

### Issue 1: ABF Returns 1970 Epoch Dates (FIXED)

**Symptom:**
```
Fetched data: 1970-01-01 00:00:00.000045935 to 1970-01-01 00:00:00.000045940
After filtering: 0 rows
```

**Root Cause**: ABF Excel returns float serial numbers instead of datetime objects.

**The Fix** (already implemented):
```python
# Handle both datetime objects and Excel serial numbers
if df['time'].dtype == 'float64':
    df['time'] = pd.to_datetime(df['time'], unit='D', origin='1899-12-30', errors='coerce')
else:
    df['time'] = pd.to_datetime(df['time'], errors='coerce')
```

**Location**: `scripts/simple_incremental_refresh.py:641-645`

---

### Issue 2: "You Can't Change Part of an Array" Error

**Symptom**: Error when trying to modify Excel cells with existing array formulas.

**The Fix**: Clear entire range before writing new formulas:
```python
# Clear worksheet (all columns we'll use)
sht.range("A2:T100000").clear_contents()  # For 10-tag batching
time.sleep(1)
```

**Location**: `scripts/simple_incremental_refresh.py:567-570`

---

### Issue 3: Only 1 Row Fetched Instead of Full Array

**Symptom**: Only getting 1 row when expecting 200+ rows.

**The Fix**: Use array formulas over the entire range:
```python
# Estimate rows: 6-minute interval -> 10 rows/hour
est_rows = int(max(1, min(87600, (hours_to_fetch * 10) + 20)))
end_row = 1 + 1 + est_rows

# Apply array formula to entire range
sht.range(f"A2:A{end_row}").formula_array = formula_time
sht.range(f"B2:B{end_row}").formula_array = formula_value
```

**Location**: `scripts/simple_incremental_refresh.py:572-597`

---

### Issue 4: Tag Count Showing 0/0 (FIXED)

**Symptom**: PCMSB units showing 0/0 tags instead of actual counts (e.g., 80/80).

**Root Cause**: Function expected wide-format parquet (tags as columns) but data is in long-format (tags as row values).

**The Fix** (already implemented):
```python
if 'tag' in df.columns:
    # LONG FORMAT: tags stored as values in 'tag' column
    unique_tags = df['tag'].dropna().unique()
    total_tags = len(unique_tags)
    active_tags = recent_df['tag'].dropna().nunique()
else:
    # WIDE FORMAT: tags as separate columns
    tag_cols = [col for col in df.columns if col != 'time']
    total_tags = len(tag_cols)
```

**Location**: `scripts/simple_incremental_refresh.py:175-237`

---

### Issue 5: Memory Allocation Error During Merge

**Symptom**: `Unable to allocate 117. MiB for array with shape (1, 15368395)`

**The Fix**: Reset DatetimeIndex before merging:
```python
if isinstance(df_existing.index, pd.DatetimeIndex):
    df_existing = df_existing.reset_index()
    if 'index' in df_existing.columns:
        df_existing = df_existing.rename(columns={'index': 'time'})
```

**Location**: `scripts/simple_incremental_refresh.py:679-682`

---

### Issue 6: Excel Stays Open Between Batches

**Symptom**: Multiple EXCEL.EXE processes accumulating in Task Manager.

**The Fix**: Force kill Excel between Option [3] cycles:
```python
# In turbopredict.py Option [3]
subprocess.run(['taskkill', '/F', '/IM', 'EXCEL.EXE'],
               capture_output=True, timeout=10)
```

**Location**: `turbopredict.py:780-783`

---

## Best Practices

### 1. Always Run Full Build First

Before using incremental refresh, ensure base parquet files exist:

```bash
# For each unit, run its build script first (one-time setup)
python scripts/build_pcfs_k1201.py
python scripts/build_abf_07mt01_k001.py
python scripts/build_pcmsb_c02001.py
```

This creates the `.dedup.parquet` files that incremental refresh appends to.

### 2. Clear PI_WEBAPI_URL for Excel-Only Fetching

```bash
set PI_WEBAPI_URL=
```

This prevents wasted time attempting unreachable Web API calls.

### 3. Kill Zombie Excel Processes Before Running

```bash
taskkill /F /IM EXCEL.EXE
```

Prevents file lock conflicts and COM automation issues.

### 4. Monitor Fresh vs Stale Status

The smart refresh shows color-coded tables:

```
+--------------+------------------+---------------+----------------------------+--------------------+
|    STATUS    |      PLANT       |      UNIT     |          DATA AGE          | TAGS (Active/Total)|
+--------------+------------------+---------------+----------------------------+--------------------+
| OK FRESH     | PCFS             | K-12-01       | 0.3h                       | 56/56              |
| !! STALE     | PCMSB            | C-02001       | 2.5h                       | 80/80              |
+--------------+------------------+---------------+----------------------------+--------------------+
```

**Status colors:**
- 🟢 Green (< 1h): Fresh, no action needed
- 🟡 Yellow (1-24h): Stale, will refresh
- 🔴 Red (> 24h): Very stale, may need investigation

**Tag health colors:**
- 🟢 Green (≥90% active): Excellent - e.g., 56/56, 110/121
- 🟡 Yellow (70-89% active): Good - e.g., 90/121
- 🔴 Red (<70% active): Poor - investigate sensor connectivity

### 5. Use Scheduled Task for 24/7 Operation

**RECOMMENDED**: Use TurboPredict Option [3] Scheduled Task Manager

```bash
python turbopredict.py
# Select option [3] SCHEDULED TASK MANAGER
# Install task to run hourly

# Task runs even when:
# - Laptop is locked
# - User is logged off
# - Screen is sleeping
```

**Task logs**: Check `logs/hourly_refresh_*.log` for errors

### 6. Fetch Window Sizing

The system calculates fetch window based on data staleness:

```python
hours_to_fetch = int(hours_gap * 1.1) + 1  # 10% buffer
max_hours = 720  # Cap at 30 days for safety
```

**Why the buffer?**
- Handles clock drift between systems
- Ensures overlap to prevent gaps
- PI DataLink timing variations

### 7. Data Validation After Each Fetch

The system validates:
```python
df_new = df[df['time'] > latest_time]

if df_new.empty:
    print(f"  [i] No new data (all <= {latest_time})")
else:
    print(f"  [OK] {len(df_new)} new rows ({df_new['time'].min()} to {df_new['time'].max()})")
```

**Red flags:**
- `df_new.empty` when data is > 1h stale → fetch failed
- Timestamps in the future → system clock misconfigured
- Very large row counts (millions) → date conversion error

---

## Performance Optimization

### Current Performance Metrics

**With 10-tag batching** (proven stable):
- Single unit (56 tags): ~2 minutes
- Single unit (124 tags): ~2.5 minutes
- All 13 units (fresh): ~5 seconds (status check only)
- All 13 units (stale): ~25-30 minutes

**Comparison to original (1-tag batching)**:
- Single unit: ~17 minutes → **2 minutes** (8.5x faster)
- All 13 units: ~3-4 hours → **~30 minutes** (8x faster)

### Optimization Tips

#### 1. Adjust Batch Size (Advanced)

If you need even faster refresh and have stable PI server:

```python
# scripts/simple_incremental_refresh.py:547
BATCH_SIZE = 15  # Increase from 10 (test first!)
```

**Testing protocol:**
1. Test with 1 unit manually
2. Monitor for errors/timeouts
3. If stable for 1 week, keep it
4. If issues, rollback to 10

#### 2. Reduce Wait Times (If Excel is Fast)

```python
# scripts/simple_incremental_refresh.py:608-610
time.sleep(5)  # Reduce from 5s if your Excel refreshes quickly
app.api.CalculateUntilAsyncQueriesDone()
time.sleep(2)  # Reduce from 2s if stable
```

**Warning**: Too aggressive reduction may cause incomplete data fetches.

#### 3. Use DuckDB for Faster Parquet Reads

```python
import duckdb
conn = duckdb.connect()
result = conn.execute("SELECT MAX(time) FROM 'file.parquet'").fetchone()
```

Faster than pandas for large parquet files (>1GB).

#### 4. Parallel Unit Processing (Future Enhancement)

Currently all units process sequentially. Future optimization:
- Open multiple Excel instances
- Process 2-3 units in parallel
- Risk: Excel COM threading issues

---

## Troubleshooting Checklist

### If a unit fails to refresh:

#### Step 1: Check Excel File Exists
```bash
ls excel/PCFS/PCFS_Automation.xlsx
ls excel/ABFSB/ABFSB_Automation_Master.xlsx
ls excel/PCMSB.xlsx
```

#### Step 2: Check Parquet File Exists
```bash
ls data/processed/K-12-01_1y_0p1h.dedup.parquet
ls data/processed/C-02001_1y_0p1h.dedup.parquet
```

#### Step 3: Verify PI DataLink Installed

Open Excel manually and test:
```excel
=PISampDat("PCFS.K-12-01.12PI-007.PV","-1h","*","-0.1h",1,"\\PTSG-1MMPDPdb01")
```

Should return timestamps, not `#NAME?` error.

#### Step 4: Check PI Server Connectivity
```bash
ping PTSG-1MMPDPdb01
```

If unreachable, VPN may be disconnected or server is down.

#### Step 5: Test Single Unit Manually
```bash
python scripts/smart_incremental_refresh.py 2>&1 | tee debug.log
```

Review `debug.log` for detailed errors.

#### Step 6: Check for Excel Lock Files
```bash
ls excel/PCFS/~$*.xlsx
ls excel/ABFSB/~$*.xlsx
```

If lock files exist:
```bash
taskkill /F /IM EXCEL.EXE
```

#### Step 7: Verify Tag Configuration

Each unit needs tags configured in `config/tags_*.txt`:
```bash
cat config/tags_k12_01.txt
cat config/tags_pcmsb_c02001.txt
```

Should contain PI tag names (one per line).

---

## Lessons Learned / SOP

1. **Run one-tag Excel check** with explicit `--server` before blaming PI or Excel
2. **Clear PI_WEBAPI_URL** if Web API endpoint isn't responsive
3. **Keep Excel visible** (`EXCEL_VISIBLE=1`) and kill zombie EXCEL.EXE before Option [1]
4. **Use array formulas** for PISampDat (not single-cell formulas)
5. **Handle both datetime and float** timestamp formats (ABF vs PCFS difference)
6. **Start with BATCH_SIZE=10** for production (proven stable)
7. **Only increase batch size** after weeks of stable operation
8. **Monitor tag success rates** - drop below 70% indicates sensor issues

---

## Success Indicators

✅ **All 13 units fresh** - System working perfectly
✅ **Color-coded status tables** - Easy visual monitoring
✅ **Tag success rates** - Shows active/total (e.g., 90/121 = 74%)
✅ **No epoch dates (1970)** - ABF date conversion working
✅ **Array formulas** - Full data range fetched
✅ **Parquet merge** - No memory errors
✅ **Batch processing** - 10x faster than original
✅ **Scheduled task running** - Automatic hourly updates

---

## Version History

**2025-10-15 (v5)**: Implemented 10-tag batch processing (10x performance improvement)
**2025-10-15 (v4)**: Added comprehensive batch size configuration guide
**2025-10-10 (v3)**: Fixed tag counting for long-format parquet files (PCMSB/ABF)
**2025-10-10 (v2)**: Added tag success metrics to status tables
**2025-10-10 (v1)**: Fixed ABF epoch date issue (Excel serial date conversion)
**2025-10-09**: Implemented array formula management for PISampDat
**2025-10-08**: Added PCMSB support (8 units)
**2025-10-07**: Initial incremental refresh system

---

## Contact and Support

**If errors persist:**

1. Check this guide first (especially Common Issues section)
2. Review recent git commits for breaking changes
3. Inspect Excel files manually to verify PI DataLink connectivity
4. Check Windows Event Viewer for scheduled task errors
5. Review `logs/hourly_refresh_*.log` for detailed traces

**Key files to check:**
- `scripts/simple_incremental_refresh.py` - Core refresh logic
- `scripts/smart_incremental_refresh.py` - Unit orchestration
- `turbopredict.py` - CLI interface

---

**End of PI_DATA_FETCHING_GUIDE.md**
