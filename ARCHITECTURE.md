# 🏗️ Turbopredict System Architecture

## System Overview

**Turbopredict X Protean** is a multi-layered industrial monitoring system designed for 24/7 operation across 13 industrial units in 3 plants. The architecture emphasizes:

- **Reliability**: Automatic fallbacks, error handling, and graceful degradation
- **Performance**: Batch processing, lazy loading, and DuckDB acceleration
- **Scalability**: Containerized deployment with per-unit isolation
- **Intelligence**: Auto-detection of stale data, smart refresh scheduling
- **Maintainability**: Modular design with clear separation of concerns

---

## 📐 High-Level Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                     USER INTERFACES                             │
├─────────────────────────────────────────────────────────────────┤
│  turbopredict.py  │  Cyberpunk CLI  │  Original CLI  │  Docker  │
└──────────┬──────────────────┬─────────────────┬────────────────┘
           │                  │                 │
           ▼                  ▼                 ▼
┌─────────────────────────────────────────────────────────────────┐
│                     CORE SERVICES                               │
├─────────────────────────────────────────────────────────────────┤
│  Smart Refresh │ Auto Scanner │ Anomaly Detection │ Emailer    │
└──────────┬──────────────────┬─────────────────┬────────────────┘
           │                  │                 │
           ▼                  ▼                 ▼
┌─────────────────────────────────────────────────────────────────┐
│                     DATA LAYER                                  │
├─────────────────────────────────────────────────────────────────┤
│  Parquet DB  │  DuckDB  │  SQLite  │  Excel Manager  │  Cache  │
└──────────┬──────────────────┬─────────────────┬────────────────┘
           │                  │                 │
           ▼                  ▼                 ▼
┌─────────────────────────────────────────────────────────────────┐
│                     DATA SOURCES                                │
├─────────────────────────────────────────────────────────────────┤
│  PI DataLink (Excel)  │  PI Web API  │  Local Parquet Files    │
└─────────────────────────────────────────────────────────────────┘
```

---

## 🏢 Multi-Plant Architecture

### Plant Topology

```
Turbopredict System
├── PCFS Plant (\\PTSG-1MMPDPdb01)
│   ├── K-12-01 (121 tags, 45.8 MB)
│   ├── K-16-01 (121 tags, 102 MB)
│   ├── K-19-01 (121 tags, 122 MB)
│   └── K-31-01 (121 tags, 123 MB)
│
├── PCMSB Plant (\\PTSG-1MMPDPdb01)
│   ├── C-02001 (56 tags)
│   ├── C-104 (86 tags)
│   ├── C-13001 (86 tags)
│   ├── C-1301 (156 tags)
│   ├── C-1302 (156 tags)
│   ├── C-201 (68 tags)
│   ├── C-202 (68 tags)
│   └── XT-07002 (83 tags)
│
└── ABFSB Plant (\\VSARMNGPIMDB01)
    ├── 21-K002 (138 tags)
    └── 07-MT01-K001 (tags TBD)
```

**Total**: 13 units, ~1,400+ tags, 1.9GB+ data

---

## 🧩 Component Architecture

### 1. User Interface Layer

#### A. `turbopredict.py` - Unified Entry Point
**Purpose**: Main interactive CLI for all system functionality
**Key Features**:
- Menu-driven interface with 8 main options
- Smart incremental refresh orchestration
- Unit analysis and exploration
- Scheduled task management
- System diagnostics

**Dependencies**:
- `pi_monitor.cyberpunk_cli` - UI components
- `pi_monitor.parquet_database` - Data access
- `scripts.smart_incremental_refresh` - Refresh logic

#### B. Cyberpunk CLI (`pi_monitor/cyberpunk_cli.py`)
**Purpose**: Beautiful terminal UI with ASCII art and colors
**Features**:
- Rich library integration for tables, progress bars, panels
- Colorama fallback for basic terminals
- Graceful degradation to text-only mode
- ASCII art banners and status displays

#### C. Original CLI (`pi_monitor/cli.py`)
**Purpose**: Legacy command-line interface with argparse
**Commands**:
```bash
refresh          # Refresh Excel workbook
ingest           # Ingest Excel → Parquet
scan             # Scan anomalies
run              # Full pipeline + email
batch-unit       # Build unit Parquet from tags
plot             # Plot time series
dedup            # Deduplicate Parquet
webapi-check     # Check PI Web API connectivity
```

---

### 2. Core Services Layer

#### A. Smart Incremental Refresh System

**Primary Script**: `scripts/smart_incremental_refresh.py`
**Supporting**: `scripts/simple_incremental_refresh.py`

**Architecture**:
```
┌─────────────────────────────────────────────┐
│  Smart Incremental Refresh                  │
│                                              │
│  1. Freshness Detection                     │
│     ├─ Check all 13 units                   │
│     ├─ Compare last_update vs current time  │
│     └─ Classify: Fresh/Stale/Very Stale     │
│                                              │
│  2. Batch Planning                          │
│     ├─ Group stale units                    │
│     ├─ Calculate optimal batch size (10)    │
│     └─ Estimate total time                  │
│                                              │
│  3. Sequential Processing                   │
│     ├─ Process one unit at a time           │
│     ├─ Batch fetch 10 tags simultaneously   │
│     ├─ Progress tracking per unit           │
│     └─ Error handling and retry logic       │
│                                              │
│  4. Verification & Deduplication            │
│     ├─ Verify data freshness                │
│     ├─ Auto-deduplicate records             │
│     └─ Update metadata                      │
│                                              │
│  5. Reporting                               │
│     ├─ Summary statistics                   │
│     ├─ Email notifications (optional)       │
│     └─ Status dashboard                     │
└─────────────────────────────────────────────┘
```

**Performance Characteristics**:
- All units fresh: ~5 seconds (status check only)
- Single unit refresh: 2-3 minutes (56-156 tags)
- All 13 units stale: 25-30 minutes (with 10-tag batching)
- **10x faster** with batch processing vs. sequential tag fetch

#### B. Anomaly Detection System

**Multi-Layered Approach**:

```
┌────────────────────────────────────────────────────────────┐
│                  Anomaly Detection Pipeline                 │
├────────────────────────────────────────────────────────────┤
│                                                             │
│  Input: Time series data (time, value, tag)                │
│     │                                                       │
│     ▼                                                       │
│  ┌──────────────────────────────────────┐                  │
│  │ Layer 1: State Detection             │                  │
│  │ - Running vs. Shutdown               │                  │
│  │ - Speed compensation (if available)  │                  │
│  └──────────┬───────────────────────────┘                  │
│             │                                               │
│             ▼                                               │
│  ┌──────────────────────────────────────┐                  │
│  │ Layer 2: Primary Detection (2.5σ)    │                  │
│  │ - Statistical threshold detection    │                  │
│  │ - Rolling mean & std calculation     │                  │
│  │ - Speed-aware thresholds             │                  │
│  └──────────┬───────────────────────────┘                  │
│             │                                               │
│             ▼                                               │
│  ┌──────────────────────────────────────┐                  │
│  │ Layer 3: MTD Verification            │                  │
│  │ - Modified Thompson Tau              │                  │
│  │ - Statistical validation             │                  │
│  └──────────┬───────────────────────────┘                  │
│             │                                               │
│             ▼                                               │
│  ┌──────────────────────────────────────┐                  │
│  │ Layer 4: Isolation Forest (ML)       │                  │
│  │ - Machine learning anomaly score     │                  │
│  │ - Contamination factor tuning        │                  │
│  └──────────┬───────────────────────────┘                  │
│             │                                               │
│             ▼                                               │
│  ┌──────────────────────────────────────┐                  │
│  │ Layer 5: Hybrid Decision             │                  │
│  │ - Combine statistical + ML           │                  │
│  │ - Confidence scoring                 │                  │
│  │ - Final anomaly classification       │                  │
│  └──────────┬───────────────────────────┘                  │
│             │                                               │
│             ▼                                               │
│  Output: Anomaly flags + confidence scores                 │
│                                                             │
└────────────────────────────────────────────────────────────┘
```

**Key Modules**:
- `pi_monitor/speed_aware_anomaly.py` - Speed-compensated detection
- `pi_monitor/tuned_anomaly_detection.py` - Advanced tuning
- `pi_monitor/hybrid_anomaly_detection.py` - Hybrid approach
- `pi_monitor/smart_anomaly_detection.py` - Intelligent detection
- `pi_monitor/anomaly.py` - Base anomaly detection

**Configuration**: Speed compensation configs in `config/speed_*.json`

#### C. Automated Scheduling System

**Script**: `scripts/hourly_refresh.py`
**Deployment**: Windows Task Scheduler

**Features**:
- Runs every hour, 24/7
- Works even when user is locked out
- Email notifications on completion
- Unattended operation

**Setup**:
```bash
# Via main interface
python turbopredict.py
→ Select Option 3: SCHEDULED TASK MANAGER

# Manual setup (requires Administrator)
setup_scheduled_task.bat
```

---

### 3. Data Layer

#### A. Parquet Database (`pi_monitor/parquet_database.py`)

**Purpose**: High-performance analytical database for time series
**Format**: Apache Parquet with PyArrow

**Schema**:
```python
# Long format (preferred)
{
    'time': datetime64[ns],
    'value': float64,
    'plant': str,
    'unit': str,
    'tag': str
}

# Wide format (legacy)
{
    'time': datetime64[ns],
    'tag1_value': float64,
    'tag2_value': float64,
    ...
}
```

**File Naming Convention**:
```
{UNIT}_1y_0p1h.dedup.parquet
├─ UNIT: K-12-01, C-02001, etc.
├─ 1y: 1 year of data
├─ 0p1h: 0.1 hour (6 minute) sampling
└─ dedup: Deduplicated
```

**Operations**:
- `load_parquet()` - Load unit data
- `get_unit_stats()` - Statistical summary
- `get_database_status()` - Overall status
- `scan_all_units()` - Multi-unit scan

#### B. DuckDB Integration (`pi_monitor/instant_cache.py`)

**Purpose**: 10x faster analytical queries on large datasets
**Location**: `data/processed/pi.duckdb`

**Features**:
- Read-only mode (prevents Windows file locking)
- In-memory fallback when file unavailable
- SQL-based query interface
- Automatic schema detection

**Use Cases**:
- Large-scale aggregations
- Complex multi-table joins
- Time-based windowing queries
- Statistical computations

#### C. SQLite Database (`pi_monitor/database.py`)

**Purpose**: Metadata tracking and legacy support
**Schema**:

```sql
-- Main data table
CREATE TABLE pi_data (
    id INTEGER PRIMARY KEY,
    timestamp DATETIME NOT NULL,
    value REAL,
    plant TEXT,
    unit TEXT,
    tag TEXT NOT NULL,
    created_at DATETIME,
    UNIQUE(timestamp, tag, plant, unit)
);

-- Metadata tracking
CREATE TABLE update_metadata (
    id INTEGER PRIMARY KEY,
    tag TEXT NOT NULL,
    plant TEXT,
    unit TEXT,
    last_update DATETIME NOT NULL,
    last_pi_fetch DATETIME,
    record_count INTEGER,
    updated_at DATETIME,
    UNIQUE(tag, plant, unit)
);
```

**Indexes**:
- `idx_timestamp` on `pi_data(timestamp)`
- `idx_tag` on `pi_data(tag)`
- `idx_plant_unit` on `pi_data(plant, unit)`

#### D. Excel File Manager (`pi_monitor/excel_file_manager.py`)

**Purpose**: Safe Excel file handling with PI DataLink
**Features**:
- Automatic file locking detection
- Retry logic with exponential backoff
- Process cleanup on errors
- Read-only and edit modes

**Safety Mechanisms**:
```python
# File locking check
def is_file_locked(filepath):
    try:
        with open(filepath, 'a'):
            return False
    except IOError:
        return True

# Safe Excel operations
with ExcelFileManager(filepath, mode='edit') as manager:
    workbook = manager.workbook
    # ... operations ...
    # Automatic cleanup on exit
```

---

### 4. Data Acquisition Layer

#### A. PI DataLink (Excel-based)

**Primary Method**: `pi_monitor/excel_refresh.py`
**Technology**: xlwings + Excel COM automation

**Architecture**:
```
┌────────────────────────────────────────────┐
│  Excel Automation Pipeline                 │
│                                             │
│  1. Excel Instance Management              │
│     ├─ Launch Excel (visible/invisible)    │
│     ├─ Open workbook with PI formulas      │
│     └─ Manage Excel process lifecycle      │
│                                             │
│  2. PI DataLink Refresh                    │
│     ├─ Batch: Refresh 10 tags at once      │
│     ├─ Formula: =PICompDat(tag,...)        │
│     ├─ Timeout: 45 seconds per batch       │
│     └─ Linger: 10 seconds post-refresh     │
│                                             │
│  3. Data Extraction                        │
│     ├─ Read values from cells              │
│     ├─ Parse timestamps & values           │
│     ├─ Handle errors & #N/A                │
│     └─ Validate data quality               │
│                                             │
│  4. Cleanup                                │
│     ├─ Close workbook (no save)            │
│     ├─ Quit Excel gracefully               │
│     └─ Kill process if hung                │
└────────────────────────────────────────────┘
```

**Performance Tuning** (config/.env):
```bash
EXCEL_VISIBLE=1              # 1=visible, 0=hidden
PI_FETCH_TIMEOUT=45          # Seconds per batch
PI_FETCH_LINGER=10           # Seconds after refresh
BATCH_SIZE=10                # Tags per batch (10x speedup!)
```

#### B. PI Web API (Future/Fallback)

**Module**: `pi_monitor/webapi.py`
**Status**: Experimental, not primary method

**API Endpoints**:
```python
# Check connection
GET {PI_SERVER}/piwebapi/system/status

# Fetch data point
GET {PI_SERVER}/piwebapi/points/{tag}/interpolated
    ?startTime={start}&endTime={end}&interval={interval}
```

**Benefits**:
- No Excel dependency
- Faster for large queries
- Better automation support

**Challenges**:
- Authentication complexity
- Server configuration required
- Different data format

---

## 🐳 Containerized Architecture

### Docker Deployment

```
┌─────────────────────────────────────────────────────────────┐
│                    Docker Network (bridge)                   │
│                     172.20.0.0/16                            │
├─────────────────────────────────────────────────────────────┤
│                                                              │
│  ┌──────────────────┐  ┌──────────────────┐                 │
│  │   Orchestrator   │  │   Prometheus     │                 │
│  │   Port: 8080     │  │   Port: 9090     │                 │
│  └─────────┬────────┘  └─────────┬────────┘                 │
│            │                      │                          │
│            │  ┌───────────────────┴────────────┐             │
│            │  │                                 │             │
│  ┌─────────▼──▼───┐  ┌──────────────┐  ┌──────▼──────┐     │
│  │    Grafana      │  │ Unit: K-12-01│  │ Unit: K-16-01│    │
│  │   Port: 3000    │  │ Port: 8081   │  │ Port: 8082   │    │
│  └─────────────────┘  └──────────────┘  └──────────────┘    │
│                                                              │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐      │
│  │ Unit: K-19-01│  │ Unit: K-31-01│  │ Unit: C-02001│       │
│  │ Port: 8083   │  │ Port: 8084   │  │ Port: 8085   │       │
│  └──────────────┘  └──────────────┘  └──────────────┘      │
│                                                              │
│  ... (8 more PCMSB units + 2 ABFSB units)                   │
│                                                              │
└─────────────────────────────────────────────────────────────┘
```

### Container Specifications

**Unit Container** (`containers/unit-base/`):
```dockerfile
FROM python:3.11-slim

# Install dependencies
RUN pip install pandas pyarrow duckdb fastapi uvicorn prometheus-client

# Copy unit-specific code
COPY pi_monitor/ /app/pi_monitor/
COPY config/tags_{unit}.txt /app/config/
COPY data/units/{unit}/ /app/data/

# Expose API and metrics
EXPOSE 8080 9090

# Health check
HEALTHCHECK --interval=30s CMD curl -f http://localhost:8080/health || exit 1

CMD ["uvicorn", "api_server:app", "--host", "0.0.0.0", "--port", "8080"]
```

**Per-Unit API** (`containers/unit-base/api_server.py`):
```python
from fastapi import FastAPI
from prometheus_client import Counter, Gauge, generate_latest

app = FastAPI()

# Metrics
data_freshness = Gauge('data_freshness_seconds', 'Data freshness in seconds')
anomaly_count = Counter('anomaly_count', 'Number of anomalies detected')

@app.get("/health")
async def health():
    return {"status": "healthy", "unit": UNIT}

@app.get("/status")
async def status():
    # Return unit status and data freshness
    pass

@app.post("/refresh")
async def refresh():
    # Trigger manual refresh
    pass

@app.get("/analyze")
async def analyze():
    # Run analysis functions
    pass

@app.get("/metrics")
async def metrics():
    # Prometheus metrics
    return generate_latest()
```

**Orchestrator** (`containers/orchestrator/`):
- Monitors all 13 unit containers
- Dashboard for system-wide status
- Coordinates refresh schedules
- Aggregates metrics

---

## 🔄 Data Flow Diagrams

### 1. Standard Refresh Flow

```
User Request (turbopredict.py)
    │
    ▼
Smart Incremental Refresh
    │
    ├─► Freshness Check (all 13 units)
    │   ├─ Load Parquet metadata
    │   ├─ Compare timestamps
    │   └─ Classify units
    │
    ├─► Batch Planning
    │   ├─ Filter stale units
    │   └─ Estimate time
    │
    ▼
For each stale unit:
    │
    ├─► Simple Incremental Refresh
    │   │
    │   ├─► Load tag list (config/tags_{unit}.txt)
    │   │
    │   ├─► Excel Refresh (batch mode)
    │   │   ├─ Open Excel workbook
    │   │   ├─ For each batch of 10 tags:
    │   │   │   ├─ Insert PI formulas
    │   │   │   ├─ Wait for PI refresh (45s timeout)
    │   │   │   ├─ Extract data
    │   │   │   └─ Progress update
    │   │   └─ Close Excel
    │   │
    │   ├─► Parquet Ingestion
    │   │   ├─ Load existing Parquet
    │   │   ├─ Append new data
    │   │   ├─ Deduplicate
    │   │   └─ Save Parquet
    │   │
    │   └─► Verification
    │       ├─ Check freshness
    │       └─ Update metadata
    │
    ▼
Summary Report
    ├─ Fresh count
    ├─ Refreshed count
    ├─ Failed count
    └─ Total time
```

### 2. Anomaly Detection Flow

```
Unit Data (Parquet)
    │
    ▼
Load Time Series
    │
    ├─► Filter by date range
    ├─► Resample to 6-min intervals
    └─► Handle missing values
    │
    ▼
Speed-Aware Preprocessing
    │
    ├─► Load speed sensor data (if available)
    ├─► Classify states (running/shutdown)
    └─► Calculate speed-adjusted thresholds
    │
    ▼
Primary Detection (2.5-sigma)
    │
    ├─► Rolling mean (window=50)
    ├─► Rolling std (window=50)
    ├─► Threshold = mean ± 2.5 * std
    └─► Flag outliers
    │
    ▼
MTD Verification
    │
    ├─► Calculate modified Z-scores
    ├─► Apply Thompson Tau test
    └─► Filter false positives
    │
    ▼
Isolation Forest (ML)
    │
    ├─► Train on recent data (1000 points)
    ├─► Contamination factor = 0.01
    ├─► Anomaly score per point
    └─► ML-based classification
    │
    ▼
Hybrid Decision
    │
    ├─► Combine statistical + ML results
    ├─► Calculate confidence scores
    ├─► Final anomaly flags
    └─► Generate incident reports
    │
    ▼
Output
    ├─► Anomaly plots (reports/{unit}_anomalies.png)
    ├─► CSV export (reports/{unit}_anomalies.csv)
    ├─► Email alerts (if configured)
    └─► Dashboard updates
```

---

## 🔧 Configuration Management

### Configuration Sources

1. **Environment Variables** (`.env`)
2. **Config Files** (`config/`)
3. **Unit-specific Configs** (`config/units/`)
4. **Speed Configs** (`config/speed_*.json`)

### Configuration Hierarchy

```
pi_monitor/config.py
    │
    ├─► Load .env file (if exists)
    │   └─ Override with environment variables
    │
    ├─► Load unit configs
    │   ├─ config/tags_{unit}.txt
    │   ├─ config/units/{unit}/settings.json
    │   └─ config/speed_{unit}.json
    │
    └─► Apply defaults
        └─ Fallback values for all settings
```

### Key Configuration Options

**Data Paths**:
```bash
XLSX_PATH=data/raw/Automation.xlsx
PARQUET_PATH=data/processed/timeseries.parquet
UNIT_DATA_DIR=data/units/
```

**PI Server Connections**:
```bash
PCFS_PI_SERVER=\\PTSG-1MMPDPdb01
PCMSB_PI_SERVER=\\PTSG-1MMPDPdb01
ABF_21_K002_PI_SERVER=\\VSARMNGPIMDB01
```

**Performance**:
```bash
BATCH_SIZE=10
MAX_AGE_HOURS=1.0
PI_FETCH_TIMEOUT=45
PI_FETCH_LINGER=10
EXCEL_VISIBLE=1
```

**Email (Office365 SMTP)**:
```bash
SMTP_HOST=smtp.office365.com
SMTP_PORT=587
SMTP_USERNAME=user@example.com
SMTP_PASSWORD=***
EMAIL_SENDER=noreply@example.com
EMAIL_RECIPIENTS=recipient1@example.com,recipient2@example.com
```

---

## 📊 Performance Optimization

### 1. Batch Processing (10x Speedup)

**Before** (Sequential):
```python
for tag in tags:  # 121 tags
    refresh_single_tag(tag)  # ~15 seconds each
    # Total: 121 * 15s = 30 minutes
```

**After** (Batch):
```python
for batch in chunks(tags, 10):  # 13 batches of 10
    refresh_batch(batch)  # ~45 seconds per batch
    # Total: 13 * 45s = 10 minutes (3x faster)
```

### 2. Lazy Loading

```python
# Only load data when needed
class ParquetDatabase:
    def __init__(self):
        self._cache = {}  # Empty cache

    def load_parquet(self, unit):
        if unit not in self._cache:
            # Load on first access
            self._cache[unit] = pd.read_parquet(f"{unit}.parquet")
        return self._cache[unit]
```

### 3. DuckDB Acceleration

```python
# Pandas query (slow on large datasets)
df[df['timestamp'] > '2024-01-01'].groupby('tag').mean()
# Time: ~10 seconds on 1.9GB

# DuckDB query (fast)
duckdb.query("""
    SELECT tag, AVG(value)
    FROM parquet_scan('*.parquet')
    WHERE timestamp > '2024-01-01'
    GROUP BY tag
""")
# Time: ~1 second (10x faster)
```

### 4. Memory Optimization

**Module**: `pi_monitor/memory_optimizer.py`

**Strategies**:
- Downcast numeric dtypes (`float64` → `float32`)
- Categorical encoding for text columns
- Chunk-based processing for large files
- Garbage collection after operations

---

## 🔒 Error Handling & Resilience

### 1. Graceful Degradation

```python
try:
    from pi_monitor.parquet_database import ParquetDatabase
    db = ParquetDatabase()
except ImportError:
    # Fallback to SQLite
    from pi_monitor.database import Database
    db = Database()
```

### 2. Retry Logic

```python
@retry(max_attempts=3, backoff=2.0)
def fetch_pi_data(tag):
    try:
        return pi_datalink.fetch(tag)
    except TimeoutError:
        logger.warning(f"Timeout fetching {tag}, retrying...")
        raise
```

### 3. Process Cleanup

```python
try:
    excel = xlwings.App(visible=True)
    # ... operations ...
finally:
    try:
        excel.quit()
    except:
        pass
    # Kill any hung Excel processes
    os.system("taskkill /F /IM EXCEL.EXE /T 2>nul")
```

### 4. Data Validation

```python
def validate_parquet_data(df):
    # Check for required columns
    assert all(col in df.columns for col in ['time', 'value', 'tag'])

    # Check for null values
    assert df['time'].notna().all()

    # Check for duplicates
    duplicates = df.duplicated(subset=['time', 'tag'])
    if duplicates.any():
        logger.warning(f"Found {duplicates.sum()} duplicates")
        df = df.drop_duplicates(subset=['time', 'tag'])

    return df
```

---

## 📈 Scalability Considerations

### Current Scale
- **13 units** across 3 plants
- **~1,400 tags** total
- **1.9GB** historical data
- **~800K records**

### Horizontal Scaling (Containerized)
- Each unit runs in isolated container
- Independent refresh schedules
- Parallel processing across units
- Orchestrator coordinates activities

### Vertical Scaling (Single Machine)
- DuckDB for analytical queries
- Batch processing for data fetch
- Lazy loading and caching
- Memory-mapped Parquet files

### Future Improvements
1. **Distributed Processing**: Apache Spark for massive datasets
2. **Time-Series Database**: InfluxDB or TimescaleDB
3. **Message Queue**: RabbitMQ for async job processing
4. **Caching Layer**: Redis for hot data
5. **API Gateway**: NGINX for load balancing

---

## 🎯 Design Patterns Used

### 1. Factory Pattern
```python
def create_database(backend='parquet'):
    if backend == 'parquet':
        return ParquetDatabase()
    elif backend == 'sqlite':
        return SQLiteDatabase()
    elif backend == 'duckdb':
        return DuckDBDatabase()
```

### 2. Strategy Pattern
```python
class AnomalyDetector:
    def __init__(self, strategy='hybrid'):
        if strategy == 'statistical':
            self.detector = SigmaDetector()
        elif strategy == 'ml':
            self.detector = IsolationForestDetector()
        elif strategy == 'hybrid':
            self.detector = HybridDetector()
```

### 3. Singleton Pattern
```python
class Config:
    _instance = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance.load_config()
        return cls._instance
```

### 4. Observer Pattern
```python
# Progress tracking
class ProgressTracker:
    def __init__(self):
        self.observers = []

    def attach(self, observer):
        self.observers.append(observer)

    def notify(self, event):
        for observer in self.observers:
            observer.update(event)
```

---

## 🔐 Security Considerations

### 1. Credentials Management
- **Never commit** `.env` files
- Use `.env.example` for templates
- Store passwords in environment variables
- Use Office365 OAuth when possible

### 2. File Access
- Read-only mode for production data
- Separate staging and production environments
- Backup before destructive operations

### 3. Network Security
- Use HTTPS for PI Web API
- Firewall rules for PI servers
- VPN for remote access
- Container network isolation

### 4. Data Privacy
- Anonymize data for testing
- Encrypt sensitive data at rest
- Audit logs for data access
- Role-based access control (future)

---

## 📚 Further Reading

- [README.md](README.md) - User guide and quick start
- [CODEBASE_ORGANIZATION.md](CODEBASE_ORGANIZATION.md) - File organization guide
- [archive/README.md](archive/README.md) - Deprecated files documentation
- [PI_DATA_FETCHING_GUIDE.md](PI_DATA_FETCHING_GUIDE.md) - Data fetching strategies

---

**Architecture Version**: 1.0
**Last Updated**: 2025-11-10
**System**: Turbopredict X Protean
