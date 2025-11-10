# Archive Directory

This directory contains **171 deprecated, unused, or experimental Python files** that have been moved out of the main codebase to improve organization and maintainability.

## Archive Organization

### 📁 `tmp/` (12 files)
**Temporary diagnostic scripts** used for one-off debugging and data verification.

These files were used during development for quick checks and probes:
- `tmp_check_*.py` - Database and data verification scripts
- `tmp_probe*.py` - PI server connection probes
- `tmp_duckdb_check.py` - DuckDB database diagnostics
- `tmp_fetch_k12_probe.py` - Tag fetch testing

**Status**: Safe to delete after verifying no unique functionality is needed.

---

### 🐛 `debug/` (10 files)
**Debugging scripts** used to diagnose specific issues during development.

Examples:
- `debug_abf_fetch.py` - ABF plant data fetch debugging
- `debug_excel_extraction.py` - Excel reading issues
- `debug_freshness.py` - Data freshness detection
- `debug_primary_detection.py` - Anomaly detection tuning
- `debug_verification_layer.py` - Multi-layer detection verification

**Status**: Historical debugging tools. Can be deleted unless debugging similar issues.

---

### 🎮 `demo/` (5 files)
**Demonstration and example scripts** showing specific features.

- `demo_cyberpunk.py` - Cyberpunk CLI interface demo
- `demo_c02001_extended_analysis.py` - Extended unit analysis example
- `demo_extended_staleness_analysis.py` - Staleness detection demo
- `demo_simple.py` - Simple usage example
- `demo_unified_plant_patch.py` - Plant unification demo

**Status**: Keep for reference or documentation purposes. Not part of production system.

---

### 🧪 `tests/` (74 files)
**Test scripts** used during feature development and integration testing.

These should have been in a dedicated `tests/` directory from the start. Categories include:

**Unit-specific tests:**
- `test_21k002_*.py` (6 files) - ABF 21-K002 unit testing
- `test_pcmsb_*.py` (10 files) - PCMSB plant testing
- `test_xt07002_*.py` (3 files) - XT-07002 unit testing

**Feature tests:**
- `test_excel_*.py` (5 files) - Excel automation testing
- `test_pi_*.py` (8 files) - PI server integration testing
- `test_anomaly_*.py` - Anomaly detection testing
- `test_formula_*.py` - PI DataLink formula testing
- `test_plot_*.py` - Plotting functionality testing

**Integration tests:**
- `test_all_plants_*.py` - Multi-plant testing
- `test_integration.py` - Full system integration
- `test_final_system.py` - End-to-end testing

**Status**: These were one-off integration tests. Modern testing should use pytest in a proper `tests/` directory structure.

---

### 🔧 `utilities/` (56 files)
**One-off utility and diagnostic scripts** used for maintenance, fixes, and investigations.

**Check utilities (23 files):**
- `check_*.py` - Various data quality, freshness, and structure checks
- Used for diagnosing data issues and verifying system state

**Fix utilities (16 files):**
- `fix_*.py` - Scripts to repair broken indexes, data formats, Excel issues
- Historical fixes for issues that have been resolved

**Verification utilities (7 files):**
- `verify_*.py` - Scripts to verify fixes and data integrity

**Diagnostic utilities (5 files):**
- `diagnose_*.py` - Deep diagnostic scripts for complex issues
- `investigate_*.py` - Investigation scripts for data anomalies

**Analysis utilities (5 files):**
- `analyze_*.py` - One-off analysis scripts

**Status**: Historical maintenance scripts. Most issues have been fixed in the main codebase. Keep for reference when similar issues occur.

---

### 📦 `old_versions/` (7 files)
**Deprecated versions** of scripts that have been superseded by better implementations.

**Old PI fetch automation:**
- `automated_pi_data_fetch.py` (v1)
- `automated_pi_fetch_v2.py` (v2)
- `complete_automated_pi_fetch.py`
- `final_automated_pi_solution.py`
- `fully_automated_pi_fetch.py`
- `robust_automated_pi_fetch.py`

**Replaced by:** `scripts/smart_incremental_refresh.py` and `scripts/simple_incremental_refresh.py`

**Backup file:**
- `ingest.py.bak` - Old backup of pi_monitor/ingest.py

**Status**: Safe to delete. Functionality has been superseded.

---

### 📊 `plotting_iterations/` (7 files)
**Old plotting implementations** showing the evolution of the plotting system.

- `plot_anomalies.py` - Original plotting script
- `simple_anomaly_plots.py` - Simplified version
- `controlled_anomaly_plots.py` - Added controls
- `enhanced_plot_anomalies.py` - Enhanced features
- `enhanced_plot_conditional.py` - Conditional plotting
- `final_anomaly_plots.py` - "Final" version (superseded)
- `create_unit_plots.py` - Unit-specific plotting

**Replaced by:** `pi_monitor/anomaly_triggered_plots.py`

**Status**: Historical iterations. Can be deleted.

---

### 🏗️ `build_iterations/` (0 files)
**Reserved for duplicate build script versions.**

When cleaning up scripts/, duplicate versions like:
- `build_abf_21k002.py` vs `build_abf_21k002_fixed.py`
- `build_pcmsb_c02001.py` vs `build_pcmsb_c02001_improved.py`

Should be moved here, keeping only the latest version.

---

## Summary Statistics

| Category | Files | Percentage |
|----------|-------|------------|
| Tests | 74 | 43.3% |
| Utilities | 56 | 32.7% |
| Tmp/Debug | 22 | 12.9% |
| Old Versions | 7 | 4.1% |
| Plotting | 7 | 4.1% |
| Demo | 5 | 2.9% |
| **TOTAL** | **171** | **100%** |

## Recommendations

### Safe to Delete (146 files - 85%)
- ✅ All `tmp/` files (12)
- ✅ All `debug/` files (10)
- ✅ All `tests/` files (74) - unless moving to proper pytest structure
- ✅ All `utilities/` files (56) - issues are fixed
- ✅ All `old_versions/` files (7)
- ✅ All `plotting_iterations/` files (7)

### Keep for Reference (5 files - 3%)
- 📚 `demo/` files - useful for documentation and examples

### Further Cleanup Needed
Review `scripts/` directory for:
- Duplicate build script versions
- Unused Excel setup scripts
- Manual operation scripts that may be obsolete

---

## What You Should Use Instead

| Old Pattern | New Solution |
|-------------|-------------|
| `automated_pi_*.py` | `scripts/smart_incremental_refresh.py` |
| `*_anomaly_plots.py` | `pi_monitor/anomaly_triggered_plots.py` |
| `tmp_check_*.py` | Use `turbopredict.py` → Database Overview |
| `debug_*.py` | Use `turbopredict.py` → System Diagnostics |
| `test_*.py` | Create proper pytest tests in `tests/` |
| `check_freshness.py` | Use `scripts/freshness_monitor.py` |
| `fix_*.py` utilities | Issues resolved in main codebase |

---

**Archive Created:** 2025-11-10
**Files Archived:** 171
**Space Saved:** ~45% reduction in root directory clutter

For questions about archived files, check git history or contact the development team.
