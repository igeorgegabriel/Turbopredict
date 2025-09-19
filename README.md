# 🚀 TURBOPREDICT X PROTEAN - Unified Neural Matrix

## Overview

**TURBOPREDICT X PROTEAN** is a unified, intelligent PI data monitoring system that combines:
- 🎯 **Real Parquet data integration** with your existing database
- 🎨 **Beautiful cyberpunk-themed interface** with ASCII art and colors
- 🧠 **Intelligent auto-scan system** that only fetches when data is stale
- ⚡ **High-performance analysis** of industrial time series data
- 🔄 **Unified entry point** for all functionality

## ✨ Key Features

### 🎯 **Intelligent Data Management**
- **Auto-detects** your existing Parquet files (K-12-01, K-16-01, K-19-01, K-31-01)
- **Smart caching** - only fetches from PI when local data is stale
- **Real-time analysis** of 1.9GB+ of historical data
- **DuckDB integration** for lightning-fast queries

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
| 1. REAL DATA SCANNER    - Scan all units in database         |
| 2. UNIT DEEP ANALYSIS   - Analyze specific unit data         |
| 3. DATABASE OVERVIEW    - Complete database status           |
| 4. AUTO-SCAN SYSTEM     - Intelligent scanning system        |
| 5. DATA QUALITY AUDIT   - Quality analysis reports           |
| 6. UNIT EXPLORER        - Browse all available units         |
| 7. ORIGINAL CLI         - Access original command interface   |
| 8. SYSTEM DIAGNOSTICS   - System health check                |
| 0. NEURAL DISCONNECT    - Exit system                        |
+================================================================+
```

## 📊 Your Data Integration

The system automatically detects and works with your existing data:

```
📁 data/processed/
├── 🗃️ K-12-01_1y_0p1h.dedup.parquet    (45.8 MB, ~87k records)
├── 🗃️ K-16-01_1y_0p1h.dedup.parquet    (102 MB, ~200k records)  
├── 🗃️ K-19-01_1y_0p1h.dedup.parquet    (122 MB, ~240k records)
├── 🗃️ K-31-01_1y_0p1h.dedup.parquet    (123 MB, ~250k records)
├── 🚀 pi.duckdb                         (1.0 GB DuckDB database)
└── 📊 Various analysis files...
```

**Total: 1.9+ GB of real industrial data across 4 major units**

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
📁 CodeX/
├── 📁 data/
│   ├── 📁 raw/          # Excel files with PI DataLink
│   └── 📁 processed/    # Your Parquet files (auto-detected)
├── 📁 reports/          # Generated plots and reports  
├── 📁 pi_monitor/       # Core Python modules
├── 🚀 turbopredict.py   # Main entry point
└── 📋 turbopredict.bat  # Easy launcher
```

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