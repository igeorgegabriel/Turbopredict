# All Plants Incremental Refresh - Complete Coverage

## Summary

Updated incremental refresh to include **ALL plants and units**:
- **PCFS**: 4 units
- **ABF**: 1 unit
- **PCMSB**: 8 units
- **Total**: 13 units across 3 plants

## Complete Unit List

### PCFS (4 units)
- K-12-01
- K-16-01
- K-19-01
- K-31-01

### ABF (1 unit)
- 07-MT01-K001

### PCMSB (8 units)
- C-02001
- C-104
- C-13001
- C-1301
- C-1302
- C-201
- C-202
- XT-07002

## Configuration

All units use the same PI server:
```
Server: \\PTSG-1MMPDPdb01
Timeout: 60 seconds
```

Excel files:
- **PCFS**: `excel/PCFS/PCFS_Automation.xlsx`
- **ABF**: `excel/ABFSB/ABFSB_Automation_Master.xlsx`
- **PCMSB**: `excel/PCMSB/PCMSB_Automation.xlsx`

## How It Works

For each unit:
1. Read last timestamp from existing Parquet file
2. Calculate exact data gap (last timestamp → now)
3. Fetch ONLY the missing data (no overlap, no waste)
4. Merge with existing data
5. Deduplicate by (time, tag)
6. Save updated Parquet file

**Example:**
```
Unit: K-12-01
Last data: 2025-10-03 10:34:57
Current time: 2025-10-03 16:15:23
Gap: 5 hours 40 minutes

Action: Fetch from 10:34:57 to 16:15:23 (exact gap)
Result: No data gaps, continuous timeline
```

## Menu Changes

**Option 1** now refreshes ALL plants:
```
1. INCREMENTAL REFRESH  - All plants (PCFS/ABF/PCMSB)
```

Output shows:
```
INCREMENTAL REFRESH - ALL PLANTS

Refreshing all units with exact timestamp matching:
  PCFS:  K-12-01, K-16-01, K-19-01, K-31-01
  ABF:   07-MT01-K001
  PCMSB: C-02001, C-104, C-13001, C-1301, C-1302, C-201, C-202, XT-07002
```

## Benefits

✅ **Complete coverage** - All plants included
✅ **No gaps** - Exact timestamp matching
✅ **Efficient** - Only fetches missing data
✅ **Scalable** - Easy to add new units
✅ **Reliable** - Same proven logic for all plants

## Usage

**From CLI:**
```bash
python turbopredict.py
# Select option 1
```

**Direct:**
```bash
python scripts/incremental_refresh_safe.py
```

## Expected Behavior

The script will:
1. Process units sequentially (PCFS → ABF → PCMSB)
2. Show progress for each unit
3. Skip units with fresh data (< 1 hour old)
4. Display summary at the end

**Success criteria:**
- All units refreshed or skipped (if fresh)
- No data gaps in timeline
- Parquet files updated with latest data

## Troubleshooting

**If a unit fails:**
- Check if tags file exists in `config/`
- Check if Excel file exists in `excel/`
- Verify PI server connectivity
- Check timeout setting (should be 60s)

**If timeout occurs:**
- Normal for first few tags (PI DataLink initialization)
- Should complete within 60 seconds
- If all tags timeout, check PI server access

## Integration

This refresh logic is used by:
- **Option 1** - Manual incremental refresh
- **Option 3** - Scheduled hourly refresh
- All automated data update workflows

Now you have complete coverage across all plants!
