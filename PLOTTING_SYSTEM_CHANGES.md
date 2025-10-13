# Plotting System Changes - TURBOPREDICT X PROTEAN

## Summary
Extended analysis plotting system has been **DISABLED**. Only anomaly-triggered plotting remains active.

## What Changed

### ❌ DISABLED: Extended Analysis Plotting
**Location**: `turbopredict.py` lines 1180-1183, 1231-1234, 1870-1874

**What it did:**
- Generated `*_SMART_ANALYSIS.png` files for ALL tags
- Created directory structure: `reports/YYYY-MM-DD_to_YYYY-MM-DD_Analysis/HH-MM-SS_PlottingTime/`
- Plotted historical 90-day trends regardless of anomaly status
- Created plots even for tags with no recent issues

**Why disabled:**
- Redundant with new anomaly-triggered system
- Generated unnecessary plots for normal-operating tags
- Used confusing date-range directory naming
- Not focused on actionable, recent anomalies

### ✅ ACTIVE: Anomaly-Triggered Plotting Only
**Location**: `pi_monitor/anomaly_triggered_plots.py`

**What it does:**
- Generates `ANOMALY_*.png` files ONLY for verified anomalies
- Creates directory structure: `reports/DD-MM-YYYY/HHAM/`
- Plots ONLY when anomalies meet ALL criteria:
  - ✅ Detected in last **24 hours** (NEW/FRESH)
  - ✅ Priority = **CRITICAL** or **HIGH**
  - ✅ Verified by MTD and/or Isolation Forest
  - ✅ High confidence from primary detectors (2.5σ or Autoencoder)

**Benefits:**
- Focused on actionable, recent issues only
- Reduces noise from historical anomalies
- Clear, simple directory naming
- Automatic PDF compilation of all anomaly plots

## Directory Structure Comparison

### Old System (DISABLED)
```
reports/
└── 2025-07-14_to_2025-10-12_Analysis/
    └── 07-26-27_PlottingTime/
        ├── 07-MT01-K001/
        │   ├── tag1_SMART_ANALYSIS.png
        │   ├── tag2_SMART_ANALYSIS.png  ← Generated for ALL tags
        │   └── ... (dozens of plots)
        └── K-31-01/
            └── ... (more plots for all tags)
```

### New System (ACTIVE)
```
reports/
└── 12-10-2025/
    └── 7AM/
        ├── 07-MT01-K001/
        │   └── ANOMALY_*.png  ← Only recent (<24h) verified anomalies
        ├── K-31-01/
        │   └── ANOMALY_*.png
        ├── ANOMALY_SESSION_SUMMARY.txt
        └── ANOMALY_REPORT_YYYYMMDD_HHMMSS.pdf  ← All plots in one PDF
```

## Files Modified

1. **turbopredict.py**
   - Line 1180-1183: Commented out `_generate_enhanced_plots()` call
   - Line 1231-1234: Commented out `_generate_enhanced_plots()` call (fallback)
   - Line 1870-1874: Commented out `_generate_enhanced_option2_plots()` call

2. **pi_monitor/anomaly_triggered_plots.py**
   - Enhanced with 24-hour recency filter
   - Added automatic PDF generation
   - Updated summary reports with recency information

## Migration Notes

**No migration needed** - Both systems used different naming:
- Old: `*_SMART_ANALYSIS.png`
- New: `ANOMALY_*.png`

Existing old plots will remain in `reports/YYYY-MM-DD_to_YYYY-MM-DD_Analysis/` directories but won't be generated going forward.

## Testing

Run the test suite to verify:
```bash
python test_recency_filter.py
```

Expected: All 5 tests pass, confirming 24-hour filter works correctly.

## What to Expect Going Forward

When you run `python turbopredict.py` → Option [2]:

1. **Analysis runs** on all units (ABF, PCFS, PCMSB)
2. **Anomaly detection** identifies issues using hybrid pipeline (2.5σ + AE → MTD + IF)
3. **Plots generated** ONLY IF:
   - Anomaly detected in **last 24 hours**
   - Priority is **CRITICAL** or **HIGH**
4. **PDF compiled** automatically with all anomaly plots
5. **No extended analysis plots** generated for normal-operating tags

Result: **Fewer, more actionable plots** focused on fresh issues requiring attention.

---

**Date**: October 12, 2025
**System Version**: TURBOPREDICT X PROTEAN (Post-24h-Filter)
