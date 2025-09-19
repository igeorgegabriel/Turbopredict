# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

**TURBOPREDICT X PROTEAN** is a unified PI (Process Information) data monitoring system that processes industrial time series data. It combines real Parquet data integration, cyberpunk-themed interface, intelligent auto-scanning, and high-performance analysis capabilities.

## Commands

### Main Application
```bash
# Primary entry point - unified interface with all functionality
python turbopredict.py

# Double-click launcher (Windows)
turbopredict.bat

# Original CLI system
python -m pi_monitor.cli

# Module-based execution
python -m pi_monitor.cli --help
python -m pi_monitor.cli auto-scan --plant PCFS --unit K-31-01
python -m pi_monitor.cli db-status
```

### Data Freshness Management
```bash
# Hourly refresh service (maintains 1-hour freshness standard)
python scripts/hourly_refresh.py          # Run as continuous service
python scripts/hourly_refresh.py --once   # Run single refresh cycle
start_hourly_refresh.bat                  # Windows service launcher

# Anomaly incident reporting (WHO-WHAT-WHEN-WHERE details)
python scripts/anomaly_incident_reporter.py --unit K-31-01 --hours 24  # Detailed incident report
python scripts/anomaly_validator.py --unit K-31-01 --top-tags 10       # Tag validation analysis

# Manual refresh commands
python -m pi_monitor.cli auto-scan --refresh  # Force refresh stale units
```

### Development Utilities
```bash
# Install dependencies
pip install -r requirements.txt

# Build utilities (in scripts/)
python scripts/build_catalog.py
python scripts/build_dataset.py
python scripts/build_duckdb.py
python scripts/validate_excel.py

# Test Excel refresh functionality
python test_excel_refresh.py
```

## Architecture

### Core System Components
- **turbopredict.py** - Unified entry point and main interface system
- **pi_monitor/** - Core Python package containing all functionality
- **data/** - Contains both raw (Excel/PI DataLink) and processed (Parquet) data

### Key Modules
- **parquet_database.py** - Real Parquet file integration and database management
- **parquet_auto_scan.py** - Intelligent scanning system that only fetches when data is stale
- **cyberpunk_cli.py** - Beautiful cyberpunk-themed interface components with ASCII art
- **cli.py** - Original command-line interface with full CLI functionality
- **auto_scan.py** - SQLite-based auto-scanning for legacy support
- **database.py** - SQLite database management for legacy functionality
- **config.py** - Configuration management with environment variable support
- **excel_refresh.py** - Excel automation with PI DataLink refresh capabilities
- **excel_file_manager.py** - Smart Excel file handling to avoid save prompts during automation

### Data Flow Architecture
```
Excel/PI DataLink → Raw Data → Parquet Files → ParquetDatabase → AutoScanner → Analysis → Cyberpunk UI
                                          ↓
                                     DuckDB (optional) → Fast Queries → Results
```

### Data Structure
The system works with existing Parquet files containing PI data:
- K-12-01, K-16-01, K-19-01, K-31-01 units
- ~1.9GB total data across 4 major industrial units
- DuckDB integration for high-performance analytical queries
- Smart caching - only fetches from PI when local data is stale (>1 hour old)

## Configuration

### Environment Variables
```bash
XLSX_PATH=data/raw/Automation.xlsx
PARQUET_PATH=data/processed/timeseries.parquet
PLANT=PCFS
UNIT=K-31-01
MAX_AGE_HOURS=1.0
```

### Directory Structure
```
CodeX/
├── data/
│   ├── raw/          # Excel files with PI DataLink
│   └── processed/    # Parquet files (auto-detected)
├── reports/          # Generated plots and reports
├── pi_monitor/       # Core Python modules
├── scripts/          # Build and utility scripts
├── turbopredict.py   # Main entry point
└── turbopredict.bat  # Windows launcher
```

## Dependencies

Core dependencies managed in requirements.txt:
- pandas>=2.1 - Data manipulation
- pyarrow>=15.0 - Parquet file support
- xlwings>=0.30 - Excel/PI DataLink integration
- matplotlib>=3.8 - Plotting capabilities
- scikit-learn>=1.4 - Advanced anomaly detection
- duckdb>=1.0 - Fast analytical queries
- polars>=1.6 - High-performance data processing

## Key Features

### Interface System
- **Rich/Colorama Support** - Beautiful terminal interface with fallback to basic text
- **Windows Unicode Handling** - Designed to handle Windows encoding issues gracefully
- **Cyberpunk Aesthetics** - Full ASCII art banners and themed interface elements

### Data Management
- **Auto-detection** of existing Parquet files in data/processed/
- **Smart caching** - intelligent freshness detection
- **Real-time analysis** of large industrial datasets
- **Batch processing** for efficiency with memory optimization

### Analysis Capabilities
- Unit-specific deep analysis with value statistics and anomaly detection
- Database overview with complete Parquet statistics
- Quality metrics and health indicators
- Tag-level breakdowns with time series information

## Excel Automation

### Save Prompt Issue Resolution
The system implements intelligent Excel file management to solve the common automation issue where Excel prompts for saves:

#### Dummy File Strategy
- **excel_file_manager.py** creates dummy files by renaming the original Excel file
- Excel can then save updated PI DataLink data to the original filename without prompting
- Automatic cleanup and restoration of file structure after refresh

#### Usage
```python
# Safe Excel refresh with automatic save handling
from pi_monitor.excel_refresh import refresh_excel_safe

# Use dummy file strategy (default)
refresh_excel_safe(excel_path)

# Use working copy strategy (alternative)
refresh_excel_safe(excel_path, use_working_copy=True)
```

#### Fallback Strategies
- **Dummy File Strategy**: Renames original → Excel saves to original name → No save prompt
- **Working Copy Strategy**: Creates copy → Refreshes copy → Copies back to original
- **Automatic Fallback**: If one strategy fails, automatically tries the other
- do not over engineer task given.