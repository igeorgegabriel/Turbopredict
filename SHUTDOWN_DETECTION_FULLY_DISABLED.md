# Shutdown Detection - COMPLETELY DISABLED

## Problem Identified

Gray "Unit Stopped/Shutdown" squares were polluting anomaly diagnostic plots, marking normal operating periods as "shutdown" incorrectly.

**Example from C-201:**
- Sensor reading consistently at ~44,000 (normal operation)
- System incorrectly flagged many points as "shutdown" (gray squares)
- Chart became cluttered and confusing

## Root Cause

Shutdown detection logic was attempting to identify when units were stopped by:
1. Finding speed-related tags (SI, SIA, SPEED, RPM)
2. Checking if values were near zero
3. Marking those periods as "shutdown"

**Why it failed:**
- False positives: Normal operating variations misidentified as shutdowns
- Tag misidentification: Non-speed tags incorrectly treated as speed sensors
- Threshold issues: Operating speeds sometimes below arbitrary thresholds

## Complete Fix Applied

### 1. Detection Logic ([hybrid_anomaly_detection.py:154-156](pi_monitor/hybrid_anomaly_detection.py#L154-L156))
```python
# SHUTDOWN DETECTION DISABLED - was causing false positives
# Use all data for cleaner, more accurate detection
grouped_running = grouped.copy()  # No filtering
```

**Before:** Filtered data to "running only" periods
**After:** Uses ALL data without shutdown filtering

### 2. Plot Data Loading ([anomaly_triggered_plots.py:314-324](pi_monitor/anomaly_triggered_plots.py#L314-L324))
```python
# SHUTDOWN DETECTION DISABLED - Was causing false positives and chart pollution
# running_times = self._derive_running_timestamps(unit_window)

# Mark all data as "running" - no shutdown filtering
tag_data['running'] = True
```

**Before:** Called `_derive_running_timestamps()` to detect stopped periods
**After:** Marks ALL data as running, no detection attempted

### 3. Plot Overlay ([anomaly_triggered_plots.py:541](pi_monitor/anomaly_triggered_plots.py#L541))
```python
# SHUTDOWN OVERLAY DISABLED - was causing false positives and messy charts
if False:  # Disabled shutdown detection overlay
    # ... gray square plotting code (never executes)
```

**Before:** Plotted gray squares for "stopped" periods
**After:** Code path disabled, no gray markers drawn

### 4. MTD Chart Overlay ([anomaly_triggered_plots.py:625](pi_monitor/anomaly_triggered_plots.py#L625))
```python
# SHUTDOWN DETECTION DISABLED for cleaner charts
if False:  # Disabled running status filtering
    # ... stopped period plotting (never executes)
```

**Before:** Plotted gray lines for stopped periods in MTD chart
**After:** Code path disabled, clean continuous plot

## Result - Clean Charts

**Before (with shutdown detection):**
```
Chart: [Normal data points] + [Gray shutdown squares everywhere] + [Red anomalies]
Legend: Value (Normal) | Unit Stopped/Shutdown | Anomaly (Running)
Status: Cluttered, confusing, false shutdown markers
```

**After (shutdown disabled):**
```
Chart: [Normal data points] + [Red anomalies]
Legend: Value (Normal) | Anomaly | Forward Filled
Status: Clean, clear, accurate
```

## Benefits

✅ **Cleaner Charts** - No false shutdown markers cluttering the view
✅ **More Accurate** - All operating data included in analysis
✅ **Better Detection** - No valid data filtered out incorrectly
✅ **Simpler Logic** - Less code paths, fewer edge cases
✅ **Consistent** - Same behavior across all units and plants

## Affected Files

1. **[pi_monitor/hybrid_anomaly_detection.py](pi_monitor/hybrid_anomaly_detection.py#L154-156)**
   - Removed shutdown filtering from 2.5σ candidate detection

2. **[pi_monitor/anomaly_triggered_plots.py](pi_monitor/anomaly_triggered_plots.py)**
   - Line 314-324: Disabled `_derive_running_timestamps()` call
   - Line 541: Disabled gray shutdown square overlay
   - Line 625: Disabled MTD chart shutdown filtering

## Test Verification

To verify shutdown detection is fully disabled:

1. Run anomaly detection:
   ```bash
   python turbopredict.py
   # Select Option [2]
   ```

2. Check generated plots - should see:
   - ✅ No gray "Unit Stopped/Shutdown" squares
   - ✅ No gray lines in MTD charts
   - ✅ Clean, continuous data visualization
   - ✅ Only red markers for actual anomalies

3. Check logs - should NOT see:
   - ❌ "Filtered out X shutdown/standby points"
   - ❌ "No running state detected"
   - ❌ "Unit is offline" messages

## Legacy Code

The `_derive_running_timestamps()` function (line 886-915) still exists but is never called:
- Kept for backward compatibility
- Can be removed in future cleanup
- Currently harmless (dead code)

## Configuration

**No configuration needed** - Shutdown detection is hard-disabled in code.

Cannot be re-enabled without code modifications.

---

**Status**: ✅ FULLY DISABLED
**Date**: October 12, 2025
**Scope**: All units (ABF, PCFS, PCMSB)
**Impact**: Charts are now clean and accurate
