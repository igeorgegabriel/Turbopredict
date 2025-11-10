# 📂 Codebase Organization Guide

This document provides a comprehensive guide to the Turbopredict codebase organization, including which files to use, which to avoid, and how to navigate the repository effectively.

---

## 📊 Repository Statistics

**Total Python Files**: ~377 files (before cleanup)
- **Active Files**: ~206 files (55%)
- **Archived Files**: 171 files (45%)

**After Reorganization**:
- Root directory: ~50% cleaner
- Clear separation between active and deprecated code
- Easier to navigate and maintain

---

## 🗂️ Directory Structure

```
Turbopredict/
│
├── 📁 pi_monitor/                      # Core Python package (38 modules)
│   ├── __init__.py
│   ├── config.py                       # ✅ Configuration management
│   ├── database.py                     # ✅ SQLite operations
│   ├── parquet_database.py             # ✅ Parquet data management
│   ├── parquet_auto_scan.py            # ✅ Auto-scanning system
│   ├── cyberpunk_cli.py                # ✅ Cyberpunk UI interface
│   ├── cli.py                          # ✅ Original CLI
│   ├── excel_refresh.py                # ✅ Excel/PI automation
│   ├── excel_file_manager.py           # ✅ Safe Excel handling
│   ├── speed_aware_anomaly.py          # ✅ Speed-compensated detection
│   ├── tuned_anomaly_detection.py      # ✅ Advanced anomaly tuning
│   ├── hybrid_anomaly_detection.py     # ✅ Hybrid detection
│   ├── smart_anomaly_detection.py      # ✅ Intelligent detection
│   ├── anomaly_triggered_plots.py      # ✅ Plotting system
│   ├── email_sender.py                 # ✅ Email notifications
│   ├── ingest.py                       # ✅ Data ingestion
│   ├── batch.py                        # ✅ Batch processing
│   ├── auto_scan.py                    # Legacy auto-scan
│   ├── anomaly.py                      # Base anomaly detection
│   ├── plotting.py                     # Legacy plotting
│   ├── pipeline.py                     # Pipeline orchestration
│   ├── dataset.py                      # Dataset handling
│   ├── breakout.py                     # Breakout detection
│   ├── clean.py                        # Data cleaning
│   ├── emailer.py                      # Legacy emailer
│   ├── webapi.py                       # PI Web API integration
│   ├── instant_cache.py                # DuckDB caching
│   ├── memory_optimizer.py             # Memory optimization
│   ├── progress_tracker.py             # Progress tracking
│   └── ... (additional support modules)
│
├── 📁 scripts/                         # Active utility scripts (~100 files)
│   ├── smart_incremental_refresh.py    # ✅ Main refresh orchestrator
│   ├── simple_incremental_refresh.py   # ✅ Core refresh logic
│   ├── hourly_refresh.py               # ✅ Scheduled background refresh
│   ├── freshness_monitor.py            # ✅ Data freshness monitoring
│   ├── auto_commit.py                  # ✅ Automated git commits
│   ├── anomaly_incident_reporter.py    # Detailed incident reports
│   ├── build_pcfs_*.py                 # PCFS unit builders
│   ├── build_pcmsb_*.py                # PCMSB unit builders
│   ├── build_abf_*.py                  # ABF unit builders
│   └── ... (additional build and utility scripts)
│
├── 📁 config/                          # Configuration files
│   ├── .env.example                    # ✅ Environment template
│   ├── tags_K-12-01.txt                # Tag list for K-12-01
│   ├── tags_K-16-01.txt                # Tag list for K-16-01
│   ├── tags_*.txt                      # Tag lists for all units
│   ├── speed_K-12-01.json              # Speed config for K-12-01
│   ├── speed_*.json                    # Speed configs per unit
│   └── units/                          # Per-unit configuration
│       ├── K-12-01/
│       ├── K-16-01/
│       └── ...
│
├── 📁 data/                            # Data storage (gitignored)
│   ├── raw/                            # Excel files with PI DataLink
│   │   ├── Automation.xlsx
│   │   └── Unit-specific Excel files
│   ├── processed/                      # Parquet databases (~1.9GB)
│   │   ├── K-12-01_1y_0p1h.dedup.parquet
│   │   ├── K-16-01_1y_0p1h.dedup.parquet
│   │   ├── ... (all 13 units)
│   │   ├── pi.duckdb
│   │   └── timeseries.parquet
│   └── units/                          # Per-unit isolated data
│       ├── K-12-01/
│       ├── K-16-01/
│       └── ...
│
├── 📁 reports/                         # Generated plots and reports
│   ├── {unit}_anomalies.png
│   ├── {unit}_anomalies.csv
│   └── ...
│
├── 📁 containers/                      # Docker containerization
│   ├── unit-base/                      # Base container for units
│   │   ├── Dockerfile
│   │   ├── api_server.py
│   │   └── requirements.txt
│   ├── orchestrator/                   # Central orchestrator
│   │   ├── Dockerfile
│   │   ├── orchestrator.py
│   │   └── dashboard.html
│   └── monitoring/                     # Prometheus/Grafana
│       ├── prometheus.yml
│       └── grafana-dashboard.json
│
├── 📁 archive/                         # ⚠️ DEPRECATED FILES (171 files)
│   ├── README.md                       # Documentation of archived files
│   ├── tmp/                            # Temporary scripts (12)
│   ├── debug/                          # Debug scripts (10)
│   ├── demo/                           # Demo scripts (5)
│   ├── tests/                          # Old test files (74)
│   ├── utilities/                      # One-off utilities (56)
│   ├── old_versions/                   # Superseded code (7)
│   ├── plotting_iterations/            # Plotting evolution (7)
│   └── build_iterations/               # Old build scripts (0)
│
├── 📁 docs/                            # Documentation (if exists)
│   └── ...
│
├── 🚀 turbopredict.py                  # ✅ MAIN ENTRY POINT
├── 📋 turbopredict.bat                 # ✅ Windows launcher
├── 🐳 docker-compose.yml               # ✅ Container orchestration
├── 📄 requirements.txt                 # ✅ Python dependencies
├── 📖 README.md                        # ✅ Main documentation
├── 🏗️ ARCHITECTURE.md                  # ✅ System architecture
├── 📂 CODEBASE_ORGANIZATION.md         # ✅ This file
├── .env                                # Local environment (gitignored)
├── .gitignore                          # Git ignore rules
└── ... (additional config files)
```

---

## ✅ Active Files - USE THESE

### 🚀 Main Entry Points

| File | Purpose | Usage |
|------|---------|-------|
| `turbopredict.py` | Main interactive CLI | `python turbopredict.py` |
| `turbopredict.bat` | Windows launcher | Double-click |
| `pi_monitor/cli.py` | Original CLI | `python -m pi_monitor.cli` |

### 🔧 Core Modules (`pi_monitor/`)

**Essential Modules:**
- ✅ `config.py` - Configuration and environment management
- ✅ `parquet_database.py` - Parquet data operations (PRIMARY)
- ✅ `database.py` - SQLite operations (LEGACY)
- ✅ `cyberpunk_cli.py` - Beautiful terminal interface

**Data Acquisition:**
- ✅ `excel_refresh.py` - Excel/PI DataLink automation
- ✅ `excel_file_manager.py` - Safe Excel file handling
- ✅ `webapi.py` - PI Web API integration (experimental)
- ✅ `ingest.py` - Data ingestion pipeline
- ✅ `batch.py` - Batch processing logic

**Anomaly Detection:**
- ✅ `speed_aware_anomaly.py` - Speed-compensated detection (PRIMARY)
- ✅ `tuned_anomaly_detection.py` - Advanced tuning
- ✅ `hybrid_anomaly_detection.py` - Hybrid statistical+ML
- ✅ `smart_anomaly_detection.py` - Intelligent detection
- ✅ `anomaly.py` - Base anomaly detection

**Visualization & Reporting:**
- ✅ `anomaly_triggered_plots.py` - Main plotting system
- ✅ `email_sender.py` - Email notifications (Office365)
- ✅ `plotting.py` - Legacy plotting utilities

**Auto-Scanning:**
- ✅ `parquet_auto_scan.py` - Parquet-based auto-scan (PRIMARY)
- ✅ `auto_scan.py` - SQLite-based auto-scan (LEGACY)

**Performance & Optimization:**
- ✅ `instant_cache.py` - DuckDB caching layer
- ✅ `memory_optimizer.py` - Memory optimization
- ✅ `progress_tracker.py` - Progress tracking

### 🛠️ Active Scripts (`scripts/`)

**Primary Automation:**
| Script | Purpose | When to Use |
|--------|---------|-------------|
| `smart_incremental_refresh.py` | Main refresh orchestrator | Called by turbopredict.py |
| `simple_incremental_refresh.py` | Core refresh logic | Used by smart refresh |
| `hourly_refresh.py` | Scheduled background refresh | Windows Task Scheduler |
| `freshness_monitor.py` | Monitor data freshness | Status checks |
| `auto_commit.py` | Automated git commits | Optional automation |

**Unit Builders:**
- `build_pcfs_*.py` - PCFS unit data builders
- `build_pcmsb_*.py` - PCMSB unit data builders
- `build_abf_*.py` - ABF unit data builders

**Use these when:**
- Initial data build for a new unit
- Rebuilding data from scratch
- Migrating to new format

### 🐳 Container Files

| File | Purpose |
|------|---------|
| `docker-compose.yml` | Multi-container orchestration |
| `containers/unit-base/Dockerfile` | Unit container image |
| `containers/unit-base/api_server.py` | Unit API server |
| `containers/orchestrator/orchestrator.py` | Central orchestrator |

---

## ⚠️ Deprecated Files - AVOID THESE

All files in the `archive/` directory are deprecated. See [`archive/README.md`](archive/README.md) for details.

### 🚫 DO NOT USE:

**Root Directory (moved to archive/):**
- ❌ `tmp_*.py` - Temporary diagnostic scripts
- ❌ `debug_*.py` - Debug scripts
- ❌ `demo_*.py` - Demo scripts
- ❌ `test_*.py` - Old test files
- ❌ `check_*.py` - One-off check scripts
- ❌ `fix_*.py` - One-off fix scripts
- ❌ `verify_*.py` - One-off verification scripts
- ❌ `diagnose_*.py` - Diagnostic scripts
- ❌ `automated_pi_*.py` - Old PI fetch versions
- ❌ `plot_anomalies.py` - Old plotting scripts

**Why These Were Archived:**
1. **Temporary nature** - Created for one-time debugging
2. **Superseded** - Better implementations exist
3. **No longer needed** - Issues have been fixed
4. **Cluttered codebase** - Made navigation difficult

---

## 🎯 File Usage Guidelines

### When to Create New Files

**✅ CREATE new files when:**
- Adding a completely new feature
- Creating a new unit builder
- Adding a new container service
- Writing proper tests (in `tests/` directory)

**❌ DON'T create new files for:**
- One-off debugging (use existing debug tools)
- Temporary experiments (use Jupyter notebooks)
- Quick fixes (edit existing files)
- Testing ideas (use `demo/` or proper `tests/`)

### File Naming Conventions

**Modules** (`pi_monitor/`):
```
{feature}_{type}.py
Examples:
- parquet_database.py (feature: parquet, type: database)
- speed_aware_anomaly.py (feature: speed_aware, type: anomaly)
- excel_file_manager.py (feature: excel_file, type: manager)
```

**Scripts** (`scripts/`):
```
{action}_{target}.py
Examples:
- build_pcfs_k1201.py (action: build, target: pcfs_k1201)
- smart_incremental_refresh.py (action: smart_incremental, target: refresh)
```

**Avoid**:
- `tmp_*.py` (use proper naming)
- `test_*.py` in root (use `tests/` directory)
- `old_*.py` or `*_old.py` (use git history)
- `*_backup.py` or `*.bak` (use git)

---

## 📖 Finding the Right File

### Common Tasks & Files

| Task | File to Use |
|------|-------------|
| Run the system | `turbopredict.py` |
| Refresh stale data | `scripts/smart_incremental_refresh.py` |
| Build new unit data | `scripts/build_{plant}_{unit}.py` |
| Detect anomalies | `pi_monitor/speed_aware_anomaly.py` |
| Create plots | `pi_monitor/anomaly_triggered_plots.py` |
| Send emails | `pi_monitor/email_sender.py` |
| Access parquet data | `pi_monitor/parquet_database.py` |
| Configure settings | `pi_monitor/config.py` + `.env` |
| Setup scheduled task | `turbopredict.py` → Option 3 |
| Check system health | `turbopredict.py` → Option 8 |

### Searching the Codebase

**By Functionality:**
```bash
# Find all anomaly detection code
grep -r "def.*anomaly" pi_monitor/ scripts/

# Find Excel automation
grep -r "xlwings\|Excel" pi_monitor/

# Find PI server code
grep -r "PI.*server\|PIComp" pi_monitor/ scripts/
```

**By Module:**
```bash
# List all imports of a module
grep -r "from pi_monitor.parquet_database import" .

# Find config usage
grep -r "config\." pi_monitor/ scripts/
```

---

## 🔄 Migration Guide

### Replacing Deprecated Code

| Old Code (Deprecated) | New Code (Active) |
|----------------------|-------------------|
| `automated_pi_data_fetch.py` | `scripts/smart_incremental_refresh.py` |
| `plot_anomalies.py` | `pi_monitor/anomaly_triggered_plots.py` |
| `tmp_check_db.py` | `turbopredict.py` → Database Overview |
| `debug_freshness.py` | `turbopredict.py` → System Diagnostics |
| `check_freshness.py` | `scripts/freshness_monitor.py` |

### Updating Imports

**Old:**
```python
# DON'T USE
from automated_pi_data_fetch import fetch_all
```

**New:**
```python
# USE THIS
from scripts.smart_incremental_refresh import run_smart_refresh
```

---

## 📚 Documentation Files

| File | Purpose | Audience |
|------|---------|----------|
| `README.md` | User guide, quick start | End users, operators |
| `ARCHITECTURE.md` | System design, technical details | Developers, architects |
| `CODEBASE_ORGANIZATION.md` | File organization guide | Developers, maintainers |
| `archive/README.md` | Deprecated files documentation | Maintainers |
| `PI_DATA_FETCHING_GUIDE.md` | Data fetching strategies | Operators, developers |

---

## 🧪 Testing Organization

### Current State
- ❌ 74 test files were in root directory (now archived)
- ❌ No proper test framework (pytest)
- ❌ Tests were one-off integration scripts

### Recommended Structure

```
tests/
├── unit/
│   ├── test_config.py
│   ├── test_parquet_database.py
│   ├── test_anomaly_detection.py
│   └── ...
├── integration/
│   ├── test_refresh_pipeline.py
│   ├── test_excel_automation.py
│   └── ...
├── fixtures/
│   ├── sample_data.parquet
│   └── test_config.json
└── conftest.py
```

### Creating Proper Tests

**Don't:**
```python
# DON'T create test_*.py in root
# test_something.py
if __name__ == "__main__":
    # Quick test code
    print("Testing...")
```

**Do:**
```python
# CREATE tests/unit/test_something.py
import pytest

def test_something():
    result = function_under_test()
    assert result == expected
```

---

## 🚀 Best Practices

### 1. Before Creating a New File

**Ask yourself:**
1. Does this functionality belong in an existing module?
2. Is this a one-time script or reusable code?
3. Will this be needed long-term or just for debugging?
4. Is there already a deprecated version in `archive/`?

### 2. Editing Existing Files

**Prefer editing over creating:**
- ✅ Add function to existing module
- ✅ Extend existing class
- ✅ Update configuration
- ❌ Create new file with similar functionality

### 3. Deprecating Files

**If you need to deprecate a file:**
1. Move to `archive/` under appropriate subdirectory
2. Update `archive/README.md`
3. Add note in commit message
4. Update documentation

### 4. Code Organization

**Module responsibilities:**
- Each module should have a **single, clear purpose**
- Related functionality should be **grouped together**
- Avoid **duplicate code** across modules
- Use **clear, descriptive names**

---

## 📊 Metrics & Statistics

### Code Organization Improvement

**Before Cleanup:**
- 377 Python files
- 224 files in root directory (cluttered)
- Difficult to find active code
- Unclear which files to use

**After Cleanup:**
- 206 active files (55%)
- 171 files archived (45%)
- ~50% cleaner root directory
- Clear organization and documentation

### Module Distribution

| Directory | Files | Purpose |
|-----------|-------|---------|
| `pi_monitor/` | 38 | Core functionality |
| `scripts/` | ~100 | Utility scripts |
| `archive/` | 171 | Deprecated code |
| `config/` | ~50 | Configuration files |
| `containers/` | ~15 | Docker deployment |
| Root | ~20 | Main entry points |

---

## 🔍 Quick Reference

### File Extensions

- `.py` - Python source code
- `.txt` - Tag lists, plain text config
- `.json` - JSON configuration (speed configs)
- `.parquet` - Parquet data files
- `.xlsx` - Excel data sources
- `.bat` - Windows batch scripts
- `.sh` - Shell scripts
- `.md` - Markdown documentation

### Important Directories

| Directory | Gitignored? | Purpose |
|-----------|-------------|---------|
| `data/` | ✅ Yes | Large data files |
| `reports/` | ✅ Yes | Generated outputs |
| `pi_monitor/` | ❌ No | Core code |
| `scripts/` | ❌ No | Utility scripts |
| `archive/` | ❌ No | Deprecated code |
| `config/` | ⚠️ Partial | Config (`.env` ignored) |

---

## 🎓 Learning the Codebase

### For New Developers

**Start Here:**
1. Read `README.md` - Understand what the system does
2. Read `ARCHITECTURE.md` - Understand how it works
3. Read this file - Understand where everything is
4. Run `turbopredict.py` - See it in action
5. Explore `pi_monitor/` modules - Core functionality

**Then:**
1. Study `scripts/smart_incremental_refresh.py` - Main automation
2. Study `pi_monitor/parquet_database.py` - Data access
3. Study `pi_monitor/speed_aware_anomaly.py` - Anomaly detection
4. Review `archive/README.md` - Learn what NOT to do

### For Maintainers

**Regular Tasks:**
1. Monitor `archive/` size - Consider deleting old files
2. Review new scripts - Should they be in root or `scripts/`?
3. Check for duplicate code - Consolidate when possible
4. Update documentation - Keep guides current
5. Run `git log --oneline` - Track changes

---

## 📞 Getting Help

**Questions about:**
- **Which file to use?** → Check this guide
- **How system works?** → Read `ARCHITECTURE.md`
- **User operations?** → Read `README.md`
- **Archived files?** → Read `archive/README.md`
- **Data fetching?** → Read `PI_DATA_FETCHING_GUIDE.md`

---

**Organization Guide Version**: 1.0
**Last Updated**: 2025-11-10
**Codebase**: Turbopredict X Protean
**Files Documented**: 377 (206 active, 171 archived)
