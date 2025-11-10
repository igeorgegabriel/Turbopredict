# 🚀 TURBOPREDICT X PROTEAN - Unified Neural Matrix

## Overview

**TURBOPREDICT X PROTEAN** is a unified, intelligent industrial PI data monitoring and anomaly detection system that combines:
- 🎯 **Real Parquet data integration** with multi-plant monitoring (13 industrial units across 3 plants)
- 🎨 **Beautiful cyberpunk-themed interface** with ASCII art and colors
- 🧠 **Intelligent auto-scan system** that only fetches when data is stale
- ⚡ **High-performance analysis** of industrial time series data
- 🔍 **Multi-layered anomaly detection** using statistical and ML algorithms
- 🔄 **Unified entry point** for all functionality
- 🐳 **Containerized deployment** for scalable 24/7 monitoring

## 🏭 Monitored Industrial Units

### **13 Units Across 3 Plants:**

**PCFS Plant (4 units):**
- K-12-01, K-16-01, K-19-01, K-31-01

**PCMSB Plant (8 units):**
- C-02001, C-104, C-13001, C-1301, C-1302, C-201, C-202, XT-07002

**ABFSB Plant (2 units):**
- 21-K002, 07-MT01-K001

**Total Monitoring:** ~800K+ records, 1.9GB+ historical data, 56-156 tags per unit

## ✨ Key Features

### 🎯 **Intelligent Data Management**
- **Auto-detects** all 13 industrial units across 3 plants
- **Smart caching** - only fetches from PI when local data is stale (>1 hour)
- **Batch processing** - fetches 10 tags simultaneously for 10x performance
- **Real-time analysis** of 1.9GB+ of historical data
- **DuckDB integration** for lightning-fast queries
- **Auto-deduplication** - removes duplicate records automatically

### 🔍 **Advanced Anomaly Detection**
- **2.5-sigma threshold detection** - Primary statistical detection layer
- **MTD verification** - Modified Thompson Tau for validation
- **Isolation Forest** - ML-based anomaly detection
- **Speed-aware detection** - Compensates for equipment speed changes
- **State-aware analysis** - Differentiates running vs. shutdown states
- **Hybrid approach** - Combines statistical and ML methods

### 🎨 **Beautiful Interface**
- **Cyberpunk ASCII art** with full "TURBOPREDICT" banner
- **Rich colors and animations** with progress bars
- **Fallback support** for terminals without color
- **Interactive menus** with intuitive navigation

### 🔧 **Unified System**
- **Single entry point** (`turbopredict.py`) for all functionality
- **Graceful degradation** - works even if some modules are missing
- **Original CLI integration** - access legacy features
- **System diagnostics** - health monitoring
- **Docker containers** - 13 unit containers + orchestrator + monitoring

## 🚀 Quick Start

### **Method 1: Double-Click (Easiest!)**
```
📁 Navigate to: C:\Users\george.gabrielujai\Documents\CodeX\
🖱️ Double-click: turbopredict.bat
```

### **Method 2: Command Line**
```bash
cd C:\Users\george.gabrielujai\Documents\CodeX
python turbopredict.py
```

### **Method 3: Python Module**
```bash
cd C:\Users\george.gabrielujai\Documents\CodeX
python -m pi_monitor.cli
```

## 🎮 System Menu

```
+================================================================+
|         TURBOPREDICT X PROTEAN NEURAL COMMAND MATRIX          |
+================================================================+
| 1. SMART INCREMENTAL REFRESH - Auto-refresh only stale units |
| 2. UNIT DEEP ANALYSIS        - Detailed unit analysis        |
| 3. SCHEDULED TASK MANAGER    - Setup 24/7 background refresh |
| 4. DATABASE OVERVIEW         - Complete database statistics  |
| 5. DATA QUALITY AUDIT        - Quality analysis reports      |
| 6. UNIT EXPLORER             - Browse all 13 units           |
| 7. ORIGINAL CLI              - Legacy command interface      |
| 8. SYSTEM DIAGNOSTICS        - Health checks and monitoring  |
| 0. NEURAL DISCONNECT         - Exit system                   |
+================================================================+
```

### 🎯 Main Features Explained:

**1. Smart Incremental Refresh:**
- Automatically detects which units have stale data (>1 hour old)
- Only refreshes units that need it (saves time!)
- Batch processing for maximum efficiency
- Full progress tracking with color-coded status

**2. Scheduled Task Manager:**
- Setup automated hourly refresh (runs 24/7, even when locked)
- Windows Task Scheduler integration
- Unattended operation for continuous monitoring
- Email notifications on completion

## 📊 Data Integration

The system automatically detects and works with data from all 13 industrial units:

```
📁 data/
├── 📁 raw/                              # Excel files with PI DataLink
│   ├── Automation.xlsx                 # Main data source
│   └── Unit-specific Excel files...
├── 📁 processed/                        # Parquet databases
│   ├── 🗃️ K-12-01_1y_0p1h.dedup.parquet
│   ├── 🗃️ K-16-01_1y_0p1h.dedup.parquet
│   ├── 🗃️ K-19-01_1y_0p1h.dedup.parquet
│   ├── 🗃️ K-31-01_1y_0p1h.dedup.parquet
│   ├── 🗃️ C-02001_1y_0p1h.dedup.parquet
│   ├── 🗃️ [8 more PCMSB units...]
│   ├── 🗃️ [2 ABFSB units...]
│   ├── 🚀 pi.duckdb                    # 1.0 GB DuckDB database
│   └── 📊 timeseries.parquet           # Legacy unified format
└── 📁 units/                            # Per-unit isolated data
    ├── K-12-01/, K-16-01/, K-19-01/, K-31-01/
    ├── C-02001/, C-104/, C-13001/, ...
    └── 21-K002/, 07-MT01-K001/
```

**Total: 1.9+ GB of real industrial data across 13 units**

## 🎯 Core Functionality

### **1. Real Data Scanner**
- Scans all your K-units (K-12-01, K-16-01, K-19-01, K-31-01)
- Shows data freshness, record counts, and status
- Beautiful table output with color coding

### **2. Unit Deep Analysis**
- Select any unit for detailed analysis
- Value statistics, anomaly detection
- Tag-level breakdowns with time series info
- Quality metrics and health indicators

### **3. Database Overview**
- Complete Parquet database statistics
- File sizes, record counts, storage usage
- DuckDB integration status
- System performance metrics

### **4. Auto-Scan System**
- Intelligent freshness detection
- Only fetches from PI when data is stale (>1 hour old)
- Batch processing for efficiency
- Email notifications and reports

## 🛠️ Technical Architecture

### **Core Modules**
- `turbopredict.py` - Unified entry point and main interface
- `parquet_database.py` - Real Parquet file integration
- `parquet_auto_scan.py` - Intelligent scanning system
- `cyberpunk_cli.py` - Beautiful interface components

### **Legacy Integration**
- `cli.py` - Original command-line interface
- `auto_scan.py` - SQLite-based auto-scanning
- `database.py` - SQLite database management
- All original PI monitoring functionality

### **Data Flow**
```
Real Parquet Files → ParquetDatabase → AutoScanner → Analysis → Beautiful UI
                                   ↓
                              DuckDB (Optional) → Fast Queries → Results
```

## 🎨 Visual Experience

### **Startup Banner**
```
+========================================================================+
|  TURBOPREDICT X PROTEAN - UNIFIED NEURAL INTERFACE                    |
|                                                                        |
|  TTTTT U   U RRRR  BBBB   OOO  PPPP  RRRR  EEEEE DDDD  III  CCCC TTTTT|
|    T   U   U R   R B   B O   O P   P R   R E     D   D  I  C   C  T   |
|    T   U   U RRRR  BBBB  O   O PPPP  RRRR  EEEE  D   D  I  C      T   |
|    T   U   U R  R  B   B O   O P     R  R  E     D   D  I  C   C  T   |
|    T    UUU  R   R BBBB   OOO  P     R   R EEEEE DDDD  III  CCCC  T   |
|                                                                        |
|               >>> UNIFIED QUANTUM NEURAL MATRIX <<<                   |
|          >>> REAL DATA + INTELLIGENT AUTO-SCAN SYSTEM <<<             |
+========================================================================+
```

### **System Status**
- 🟢 **ONLINE - REAL DATA CONNECTED** (when Parquet files detected)
- 🟡 **LIMITED MODE ACTIVE** (when some features unavailable)
- 🔴 **OFFLINE - NO DATA CONNECTION** (when data inaccessible)

## 📋 Requirements

### **Python Environment**
- Python 3.10+ (recommended 3.11+)
- Windows 10/11 (optimized for Windows Terminal)

### **Required Packages**
```bash
pip install -r requirements.txt
```

**Core Dependencies:**
- `pandas>=2.1` - Data manipulation
- `pyarrow>=15.0` - Parquet file support
- `rich>=14.0` - Beautiful terminal interface
- `colorama>=0.4` - Color fallback support
- `duckdb>=1.0` - Fast analytical queries (optional)

### **Optional Dependencies**
- `xlwings>=0.30` - Excel/PI DataLink integration
- `matplotlib>=3.8` - Plotting capabilities
- `scikit-learn>=1.4` - Advanced anomaly detection

## 🔧 Configuration

### **Environment Variables**
```bash
# Optional configuration via .env file
XLSX_PATH=data/raw/Automation.xlsx
PARQUET_PATH=data/processed/timeseries.parquet
PLANT=PCFS
UNIT=K-31-01
MAX_AGE_HOURS=1.0
```

### **Data Directory Structure**
```
📁 Turbopredict/
├── 📁 data/
│   ├── 📁 raw/              # Excel files with PI DataLink
│   ├── 📁 processed/        # Parquet databases (1.9GB+)
│   └── 📁 units/            # Per-unit isolated data
├── 📁 reports/              # Generated plots and reports
├── 📁 pi_monitor/           # Core Python modules (38 files)
├── 📁 scripts/              # Utility scripts (100+ scripts)
├── 📁 config/               # Configuration files and tags
├── 📁 containers/           # Docker containerization
├── 📁 archive/              # Archived/deprecated files (171 files)
│   ├── tmp/, debug/, demo/  # Development artifacts
│   ├── tests/               # Old test scripts
│   └── utilities/           # One-off utility scripts
├── 🚀 turbopredict.py       # Main entry point
└── 📋 turbopredict.bat      # Easy launcher
```

## 📁 Archive & Code Organization

The repository has been reorganized for better maintainability. **171 deprecated files** have been moved to the `archive/` directory:

- **12 tmp files** - Temporary diagnostic scripts
- **10 debug files** - Development debugging tools
- **5 demo files** - Example and demonstration scripts
- **74 test files** - Old integration tests (should use pytest)
- **56 utility scripts** - One-off maintenance tools (check_*, fix_*, verify_*)
- **7 old versions** - Superseded implementations
- **7 plotting iterations** - Evolution of plotting system

See [`archive/README.md`](archive/README.md) for detailed information about archived files.

**Active codebase is now ~50% cleaner and easier to navigate!**

## 🚀 Advanced Usage

### **Command Line Options**
```bash
# Direct Python execution
python turbopredict.py

# Original CLI system
python -m pi_monitor.cli --help

# Auto-scan specific unit
python -m pi_monitor.cli auto-scan --plant PCFS --unit K-31-01

# Database status check
python -m pi_monitor.cli db-status
```

### **API Usage**
```python
from pi_monitor.parquet_database import ParquetDatabase
from pi_monitor.parquet_auto_scan import ParquetAutoScanner

# Initialize systems
db = ParquetDatabase()
scanner = ParquetAutoScanner()

# Get database status
status = db.get_database_status()
print(f"Total size: {status['total_size_gb']:.1f} GB")

# Scan all units
results = scanner.scan_all_units()
print(f"Fresh units: {results['summary']['fresh_units']}")

# Analyze specific unit
analysis = scanner.analyze_unit_data("K-31-01")
print(f"Records: {analysis['records']:,}")
```

## 🔍 Troubleshooting

### **Common Issues**

#### **"Data systems offline"**
- Ensure you're in the correct directory: `C:\Users\george.gabrielujai\Documents\CodeX`
- Check that `data/processed/` contains Parquet files
- Verify Python can read the files: `ls -la data/processed/`

#### **"Import errors"**
- Install dependencies: `pip install -r requirements.txt`
- Check Python version: `python --version` (need 3.10+)
- Ensure you're in the CodeX directory

#### **"Colors not showing"**
- Install rich: `pip install rich colorama`
- Use Windows Terminal for best results
- System falls back to text mode automatically

#### **"Unicode errors"**
- The system is designed to handle Windows encoding issues
- Falls back gracefully to ASCII-only mode
- All functionality works without Unicode support

### **Performance Optimization**
- DuckDB provides 10x faster queries on large datasets
- Parquet files are optimized for analytical workloads
- System uses lazy loading - only reads data when needed
- Batch processing minimizes memory usage

## 🎯 What Makes This Special

### **🧠 Intelligence**
- **Adaptive** - only fetches when data is actually stale
- **Efficient** - works with your existing 1.9GB database
- **Smart** - detects data quality and freshness automatically

### **🎨 Beauty**
- **Gorgeous** cyberpunk interface with ASCII art
- **Professional** tables and progress indicators  
- **Accessible** - works even on basic terminals

### **🔧 Practical**
- **Real data** - works with your actual PI monitoring system
- **Backwards compatible** - doesn't break existing workflows
- **Extensible** - easy to add new features and modules

## 📈 Performance

**Your System Statistics:**
- **4 active units** (K-12, K-16, K-19, K-31)
- **1.9+ GB total data** across all Parquet files
- **~800k+ total records** of historical PI data
- **Sub-second response** times for most operations
- **DuckDB acceleration** available for complex queries

## 🎉 Success!

You now have a **unified, beautiful, intelligent** PI data monitoring system that:

✅ **Works with your real data** (1.9GB Parquet files)  
✅ **Looks amazing** (cyberpunk ASCII art interface)  
✅ **Performs intelligently** (only fetches when needed)  
✅ **Integrates everything** (single entry point)  
✅ **Scales beautifully** (handles large datasets efficiently)

**Just double-click `turbopredict.bat` and enjoy your quantum neural matrix!** 🚀✨

---

**TURBOPREDICT X PROTEAN v1.0.0** - Where industrial data meets cyberpunk aesthetics! 🎭⚡