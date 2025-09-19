# TURBOPREDICT X PROTEAN - Auto-Scan Feature

## Overview

TURBOPREDICT X PROTEAN enhances the original PI Monitor with intelligent local database caching and auto-scanning capabilities. This eliminates unnecessary PI DataLink fetches by maintaining a local SQLite database and only fetching new data when the local cache is stale.

## Key Features

### 🚀 Auto-Scan Command
- **Smart Caching**: Automatically checks local database freshness before fetching from PI
- **Batch Processing**: Efficiently processes multiple tags in batches
- **Anomaly Detection**: Runs anomaly detection on fetched data
- **Email Notifications**: Sends comprehensive reports with attachments
- **Plot Generation**: Creates time series plots for each tag

### 🗄️ Database Status
- **Comprehensive Statistics**: Shows database size, date ranges, and activity
- **Tag-specific Details**: View freshness info for individual tags
- **Cleanup Utilities**: Remove old data to manage storage

## Usage Examples

### 1. Basic Auto-Scan
```bash
# Scan tags from a text file
python -m pi_monitor.cli auto-scan \
  --xlsx "data/raw/Automation.xlsx" \
  --tags "sample_tags.txt" \
  --plant "PCFS" \
  --unit "K-31-01"
```

### 2. Auto-Scan with Custom Settings
```bash
# Scan with 2-hour staleness threshold and custom batch size
python -m pi_monitor.cli auto-scan \
  --xlsx "data/raw/Automation.xlsx" \
  --tags "sample_tags.txt" \
  --plant "PCFS" \
  --unit "K-31-01" \
  --max-age-hours 2.0 \
  --batch-size 5 \
  --start "-48h" \
  --step "-10min"
```

### 3. Read Tags from Excel Sheet
```bash
# Read tags directly from Excel sheet
python -m pi_monitor.cli auto-scan \
  --xlsx "data/raw/Automation.xlsx" \
  --tags-sheet "DL_K31_TAGS" \
  --plant "PCFS" \
  --unit "K-31-01"
```

### 4. Force Refresh All Data
```bash
# Force refresh even if local data is fresh
python -m pi_monitor.cli auto-scan \
  --xlsx "data/raw/Automation.xlsx" \
  --tags "sample_tags.txt" \
  --plant "PCFS" \
  --unit "K-31-01" \
  --force-refresh
```

### 5. Skip Notifications and Plots
```bash
# Run scan without email notifications or plots
python -m pi_monitor.cli auto-scan \
  --xlsx "data/raw/Automation.xlsx" \
  --tags "sample_tags.txt" \
  --plant "PCFS" \
  --unit "K-31-01" \
  --no-email \
  --no-plots
```

### 6. Check Database Status
```bash
# View overall database status
python -m pi_monitor.cli db-status

# View specific tag details
python -m pi_monitor.cli db-status \
  --tag "PCFS.K3101.ST_PERFORMANCE" \
  --plant "PCFS" \
  --unit "K-31-01"

# Cleanup old data (keep only 30 days)
python -m pi_monitor.cli db-status --cleanup-days 30
```

## Command Line Options

### Auto-Scan Options
- `--xlsx`: Path to Excel workbook with PI DataLink
- `--tags`: Text file with one PI tag per line
- `--tags-sheet`: Read tags from Excel sheet (row 2+)
- `--plant`: Plant identifier (required)
- `--unit`: Unit identifier (required)
- `--server`: PI server path (default: \\\\PTSG-1MMPDPdb01)
- `--max-age-hours`: Maximum data age before fetching from PI (default: 1.0)
- `--start`: Start time for PI fetch (default: -24h)
- `--end`: End time for PI fetch (default: *)
- `--step`: Time step for PI fetch (default: -6min)
- `--batch-size`: Tags per batch for PI fetch (default: 10)
- `--force-refresh`: Force refresh even if data is fresh
- `--no-anomaly`: Skip anomaly detection
- `--no-plots`: Skip plot generation
- `--no-email`: Skip email notifications
- `--db-path`: Custom local database path
- `--cleanup-days`: Days of data to keep in local DB (default: 90)

### DB-Status Options
- `--db-path`: Custom local database path
- `--cleanup-days`: Clean up old data (days to keep)
- `--tag`: Show details for specific tag
- `--plant`: Filter by plant
- `--unit`: Filter by unit

## Sample Tags File Format

Create a text file with one PI tag per line:

```
# Sample PI tags for TURBOPREDICT X PROTEAN testing
# Plant: PCFS, Unit: K-31-01
PCFS.K3101.ST_PERFORMANCE
PCFS.K3101.TEMPERATURE
PCFS.K3101.PRESSURE
PCFS.K3101.FLOW_RATE
PCFS.K3101.VIBRATION

# Plant: PCFS, Unit: K-31-02
PCFS.K3102.ST_PERFORMANCE
PCFS.K3102.TEMPERATURE
PCFS.K3102.PRESSURE
```

## Benefits

### ⚡ Performance
- **Reduced PI Server Load**: Only fetch when data is stale
- **Faster Response**: Use local cache for recent data
- **Batch Optimization**: Process multiple tags efficiently

### 🔍 Intelligence
- **Freshness Detection**: Automatically determine when to fetch
- **Comprehensive Reporting**: Detailed scan results and statistics
- **Flexible Configuration**: Customize staleness thresholds and batch sizes

### 📊 Monitoring
- **Database Statistics**: Track storage and activity
- **Tag-level Insights**: View freshness for individual tags
- **Automated Cleanup**: Manage storage with configurable retention

## Output

The auto-scan command provides comprehensive output:

```
[TURBOPREDICT X PROTEAN] Auto-Scan starting for 8 tags
Plant: PCFS, Unit: K-31-01
Max age: 1.0 hours, Force refresh: False

Auto-scan complete!
Results Summary:
   * Total tags: 8
   * Fetched from PI: 3 tags
   * Used local cache: 5 tags
   * Failed: 0 tags
   * Success rate: 100.0%
   * Total alerts: 2
   * Analyzed successfully: 8 tags

Detailed results saved: reports/auto_scan_results_PCFS_K-31-01.json
```

## Database Schema

The local SQLite database contains:
- **pi_data**: Time series data (timestamp, value, plant, unit, tag)
- **update_metadata**: Tracking information (last_update, last_pi_fetch, record_count)

## Integration with Existing Workflow

TURBOPREDICT X PROTEAN seamlessly integrates with your existing PI monitoring pipeline:

1. **Replace** batch data collection with intelligent auto-scan
2. **Maintain** all existing analysis and alerting capabilities  
3. **Add** local caching for improved performance
4. **Enhance** with comprehensive reporting and statistics

The system is backwards compatible with existing configurations and can be adopted incrementally.