"""
Parquet-based database manager for TURBOPREDICT X PROTEAN
Works with existing Parquet files in data directory
"""

from __future__ import annotations

import pandas as pd
import duckdb
from pathlib import Path
from typing import Dict, List, Any, Optional, Union
from datetime import datetime, timedelta
import logging
import glob
import os

from .polars_optimizer import PolarsOptimizer

logger = logging.getLogger(__name__)


class ParquetDatabase:
    """Parquet-based database manager for PI data"""
    
    def __init__(self, data_dir: Path | str = None):
        """Initialize Parquet database manager.
        
        Args:
            data_dir: Path to data directory containing Parquet files
        """
        if data_dir is None:
            # Default to data directory relative to current location
            current_dir = Path(__file__).parent.parent
            data_dir = current_dir / "data"
        
        self.data_dir = Path(data_dir)
        self.processed_dir = self.data_dir / "processed"
        self.raw_dir = self.data_dir / "raw"
        
        # Initialize DuckDB for fast queries if available
        self.duckdb_path = self.processed_dir / "pi.duckdb"
        self.conn = None
        self._init_duckdb()
        
        # Initialize Polars optimizer for high-performance operations
        self.polars_opt = PolarsOptimizer()
        logger.info(f"Polars optimization: {'enabled' if self.polars_opt.available else 'disabled'}")
        
    def _init_duckdb(self):
        """Initialize DuckDB connection.

        Prefer read-only open on existing DB to avoid Windows locking.
        If not available, use an in-memory connection which still
        provides fast SQL over Parquet via read_parquet().
        """
        try:
            # Honor opt-out: set environment variable DISABLE_DUCKDB=1 to force Parquet-only path
            import os as _os
            if _os.getenv("DISABLE_DUCKDB", "0") == "1":
                logger.info("DISABLE_DUCKDB=1 set: using Parquet-only path (DuckDB disabled)")
                self.conn = None
                return
            if self.duckdb_path.exists():
                try:
                    self.conn = duckdb.connect(database=str(self.duckdb_path), read_only=True)
                    self.conn.execute("SELECT 1").fetchone()
                    logger.info(f"DuckDB ready (read-only): {self.duckdb_path}")
                    return
                except Exception as e:
                    logger.warning(f"DuckDB file open failed ({e}); using in-memory fallback")
            # In-memory fallback
            self.conn = duckdb.connect(database=":memory:")
            self.conn.execute("SELECT 1").fetchone()
            logger.info("DuckDB in-memory connection ready")
        except Exception as e:
            logger.warning(f"DuckDB unavailable: {e}")
            self.conn = None

    def invalidate_cache(self):
        """Force invalidation of DuckDB file cache by recreating connection.

        Call this after Parquet files have been updated to ensure fresh reads.
        """
        if self.conn is not None:
            try:
                self.conn.close()
            except Exception:
                pass
            self.conn = None
            self._init_duckdb()
            logger.info("DuckDB cache invalidated - connection recreated")

    def _parquet_glob(self, dedup_preferred: bool = True) -> str:
        """Return glob pattern for Parquet reads.

        Historically we preferred reading only ``*dedup.parquet`` files for
        stability, but during refresh runs a unit's master file may be updated
        before the corresponding ``*.dedup.parquet`` is regenerated (or the
        dedup step may be deferred). In those cases, restricting to
        ``*dedup.parquet`` makes freshness checks look stale even though the
        master file already contains newer rows.

        To support accurate freshness checks, callers that care about "latest"
        timestamps should request the broader pattern (``dedup_preferred=False``)
        so both master and dedup files are visible to DuckDB. The
        ``WHERE unit = ?`` filter in queries ensures we only scan the relevant
        unit, and ``MAX(time)`` remains correct even if both files are present.
        """
        return str(self.processed_dir / ("*dedup.parquet" if dedup_preferred else "*.parquet"))
    
    def get_available_parquet_files(self) -> List[Dict[str, Any]]:
        """Get list of available Parquet files with metadata"""
        files = []
        
        # Find only root-level Parquet files, not partitioned dataset files
        parquet_patterns = [
            "*.parquet"  # Only root level, not recursive
        ]
        
        for pattern in parquet_patterns:
            for file_path in self.processed_dir.glob(pattern):
                try:
                    # Get file stats
                    stat = file_path.stat()
                    size_mb = stat.st_size / (1024 * 1024)
                    modified = datetime.fromtimestamp(stat.st_mtime)
                    
                    # Try to get basic info from file
                    try:
                        df_info = pd.read_parquet(file_path, engine="pyarrow").head(1)
                        columns = list(df_info.columns)
                        sample_data = df_info.to_dict('records')[0] if len(df_info) > 0 else {}
                    except Exception:
                        columns = []
                        sample_data = {}
                    
                    # Parse filename for unit info (only accept realistic unit patterns)
                    filename = file_path.name
                    candidate = filename.split('_')[0] if '_' in filename else filename.replace('.parquet', '')
                    unit_match = self._normalize_unit_from_token(candidate)
                    
                    files.append({
                        'file_path': str(file_path),
                        'filename': filename,
                        'unit': unit_match,
                        'size_mb': round(size_mb, 2),
                        'modified': modified,
                        'columns': columns,
                        'sample_data': sample_data,
                        'is_dedup': 'dedup' in filename.lower()
                    })
                    
                except Exception as e:
                    logger.warning(f"Could not read file {file_path}: {e}")
        
        # Sort by modification time (newest first)
        files.sort(key=lambda x: x['modified'], reverse=True)
        return files

    def _normalize_unit_from_token(self, token: str) -> Optional[str]:
        """Return a normalized unit id from a filename token if it looks like a real unit.

        This guards against tag-prefixed files (e.g., 'FI-07001_...') being
        mistaken for units. Only known unit patterns are accepted.
        """
        import re
        t = token.strip()
        # Canonical replacements
        if t in ("07-MT01/K001", "07-MT001/K001", "07-MT01_K001"):
            return "07-MT01-K001"
        # Known unit patterns
        patterns = [
            r"^K-\d{2}-\d{2}$",
            r"^C-\d{2,5}(?:-\d{2})?$",
            r"^XT-\d{5}$",
            r"^\d{2}-MT0?1[-_]?K\d{3}$",
            r"^\d{2}-K\d{3}$",
        ]
        for p in patterns:
            if re.match(p, t, flags=re.IGNORECASE):
                # Normalize ABF separators to hyphens
                if re.match(r"^\d{2}-MT0?1[-_]?K\d{3}$", t, flags=re.IGNORECASE):
                    t = t.replace("_", "-")
                return t
        return None

    def archive_non_unit_parquet(self) -> list[Path]:
        """Move any Parquet files that are not prefixed by a recognized unit id to archive/.

        This helps avoid accidental unit rows such as files starting with tag ids
        (e.g., 'FI-07001_...parquet'). Returns the list of archived paths.
        """
        archived: list[Path] = []
        archive_dir = self.processed_dir / "archive"
        archive_dir.mkdir(parents=True, exist_ok=True)

        for file_path in self.processed_dir.glob("*.parquet"):
            try:
                name = file_path.name
                token = name.split("_")[0] if "_" in name else name.replace(".parquet", "")
                unit = self._normalize_unit_from_token(token)
                if unit is None:
                    target = archive_dir / name
                    # If target exists, append a counter suffix
                    if target.exists():
                        stem = target.stem
                        suffix = target.suffix
                        i = 1
                        while True:
                            cand = archive_dir / f"{stem}.{i}{suffix}"
                            if not cand.exists():
                                target = cand
                                break
                            i += 1
                    file_path.replace(target)
                    archived.append(target)
            except Exception:
                # Best-effort; ignore files we cannot move
                continue
        return archived

    def _discover_config_units(self) -> List[str]:
        """Discover units defined by tag files in the config directory.

        This supplements file-based discovery so that freshly-configured
        units (without Parquet yet) still appear in scans and can be
        auto-refreshed by higher-level tools.
        """
        units: set[str] = set()
        try:
            config_dir = Path(__file__).parent.parent / "config"
            if not config_dir.exists():
                return []

            for tags_file in config_dir.glob("tags_*.txt"):
                name = tags_file.stem.lower()
                # Common patterns
                if "k12_01" in name:
                    units.add("K-12-01")
                if "k16_01" in name:
                    units.add("K-16-01")
                if "k19_01" in name:
                    units.add("K-19-01")
                if "k31_01" in name:
                    units.add("K-31-01")
                if "c02001" in name or "c-02001" in name:
                    units.add("C-02001")
                if "c104" in name or "c-104" in name:
                    units.add("C-104")
                if "c13001" in name or "c-13001" in name:
                    units.add("C-13001")
                if "c1301" in name or "c-1301" in name:
                    units.add("C-1301")
                if "c1302" in name or "c-1302" in name:
                    units.add("C-1302")
                if "c201" in name or "c-201" in name:
                    units.add("C-201")
                if "c202" in name or "c-202" in name:
                    units.add("C-202")
                if ("abf" in name) and ("07" in name) and ("mt01" in name) and ("k001" in name):
                    units.add("07-MT01/K001")
                if ("abf" in name) and ("07" in name) and ("mt001" in name) and ("k001" in name):
                    units.add("07-MT001/K001")
                if "xt07002" in name or "xt-07002" in name or "xt_07002" in name:
                    units.add("XT-07002")

                # Try to infer other units from first non-comment tag line
                try:
                    first_line = None
                    for raw in tags_file.read_text(encoding="utf-8").splitlines():
                        s = raw.strip()
                        if s and not s.startswith('#'):
                            first_line = s
                            break
                    if first_line:
                        import re
                        m = re.search(r"\b([A-Z]{1,4}-\d{2,5}(?:-\d{2})?)\b", first_line)
                        if m:
                            units.add(m.group(1))
                except Exception:
                    pass
        except Exception:
            return list(units)
        return sorted(list(units))
    
    def get_unit_data(self, unit: str, start_time: datetime = None, end_time: datetime = None) -> pd.DataFrame:
        """Get data for a specific unit.
        
        Args:
            unit: Unit identifier (e.g., 'K-31-01')
            start_time: Optional start time filter
            end_time: Optional end time filter
            
        Returns:
            DataFrame with unit data
        """
        # Fast path: DuckDB over Parquet
        if self.conn is not None:
            try:
                q = (
                    "SELECT CAST(time AS TIMESTAMP) AS time, value, plant, unit, tag "
                    f"FROM read_parquet('{self._parquet_glob(True)}') WHERE unit = ?"
                )
                params = [unit]
                if start_time is not None:
                    q += " AND time >= ?"
                    params.append(start_time)
                if end_time is not None:
                    q += " AND time <= ?"
                    params.append(end_time)
                q += " ORDER BY time"
                df = self.conn.execute(q, params).fetchdf()
                logger.info(f"Loaded {len(df)} records for unit {unit} via DuckDB")
                return df
            except Exception as e:
                logger.warning(f"DuckDB read failed for {unit}: {e}; falling back to pandas")

        import re as _re
        # Find Parquet files for this unit (try sanitized variants for filename safety)
        unit_files = []
        variants = [
            unit,
            unit.replace('/', '-'),
            unit.replace('/', '_'),
            unit.replace('-', '_'),
            _re.sub(r"[^A-Za-z0-9._-]", "_", unit),
        ]
        # Include known alias filenames for ABF unit
        if unit == '07-MT01-K001':
            variants.extend(['FI-07001'])
        seen = set()
        patterns = []
        for v in variants:
            if v in seen:
                continue
            seen.add(v)
            patterns.extend([f"*{v}*.parquet", f"**/*{v}*.parquet"])
        for pattern in patterns:
            unit_files.extend(list(self.processed_dir.glob(pattern)))
        
        if not unit_files:
            logger.warning(f"No Parquet files found for unit {unit}")
            return pd.DataFrame()
        
        # Prioritize files by completeness and freshness: updated > refreshed > dedup > regular
        # Exclude fallback and temporary files that may be corrupted
        valid_files = [f for f in unit_files if not any(exclude in f.name.lower() for exclude in ['fallback', 'temp', 'tmp'])]

        updated_files = [f for f in valid_files if 'updated' in f.name]
        # Prefer stable masters first: dedup > regular; treat 'refreshed' as lowest priority
        dedup_files = [f for f in valid_files if 'dedup' in f.name and 'refreshed' not in f.name and 'updated' not in f.name]
        regular_files = [f for f in valid_files if 'dedup' not in f.name and 'refreshed' not in f.name and 'updated' not in f.name]
        refreshed_files = [f for f in valid_files if 'refreshed' in f.name and 'updated' not in f.name]

        # If a span preference is set (e.g., '1p5y'), prefer matching files
        span_pref = os.getenv('PREFERRED_SPAN', '1p5y').strip().lower()
        def _prefer_span(files: list[Path]) -> list[Path]:
            if not span_pref:
                return files
            try:
                return sorted(
                    files,
                    key=lambda p: (span_pref in p.name.lower(), p.stat().st_mtime),
                    reverse=True,
                )
            except Exception:
                return files
        dedup_files = _prefer_span(dedup_files)
        regular_files = _prefer_span(regular_files)
        updated_files = _prefer_span(updated_files)

        target_file = None
        if updated_files:
            # Use newest updated file (highest priority - historical + fresh data combined)
            target_file = updated_files[0]
        elif dedup_files:
            # Prefer dedup files (cleaned, reliable data)
            target_file = dedup_files[0]
        elif regular_files:
            # Use regular files as next option
            target_file = regular_files[0]
        elif refreshed_files:
            # Only use refreshed files if nothing else exists (may lack 'tag' column)
            target_file = refreshed_files[0]
        
        if not target_file:
            return pd.DataFrame()
        
        logger.info(f"Loading data from: {target_file}")
        
        try:
            # Load the Parquet file
            df = pd.read_parquet(target_file)
            
            # Ensure time column exists and is datetime
            time_cols = ['time', 'timestamp', 'Time', 'Timestamp']
            time_col = None
            for col in time_cols:
                if col in df.columns:
                    time_col = col
                    break
            
            if time_col:
                df[time_col] = pd.to_datetime(df[time_col])
                if time_col != 'time':
                    df = df.rename(columns={time_col: 'time'})
                
                # Apply time filters
                if start_time:
                    df = df[df['time'] >= start_time]
                if end_time:
                    df = df[df['time'] <= end_time]
                
                df = df.sort_values('time')
            
            logger.info(f"Loaded {len(df)} records for unit {unit}")
            return df
            
        except Exception as e:
            logger.error(f"Error loading data from {target_file}: {e}")
            return pd.DataFrame()
    
    def get_latest_timestamp(self, unit: str, tag: str = None) -> Optional[datetime]:
        """Get latest timestamp for a unit/tag combination.

        Args:
            unit: Unit identifier
            tag: Optional tag filter

        Returns:
            Latest timestamp or None
        """
        df = self.get_unit_data(unit)
        if df.empty:
            return None

        if tag and 'tag' in df.columns:
            df = df[df['tag'] == tag]

        if 'time' in df.columns and not df.empty:
            return df['time'].max()

        return None

    def check_tag_freshness(self, unit: str, max_age_hours: float = 1.0) -> tuple[bool, int, int]:
        """Check if at least 50% of tags have fresh data (< max_age_hours old).

        This prevents false positives where only 1-2 tags are fresh but others are stale.

        Args:
            unit: Unit identifier
            max_age_hours: Maximum age in hours before considering stale

        Returns:
            (is_fresh, fresh_tag_count, total_tag_count)
            is_fresh = True if >= 50% of tags have data within max_age_hours
        """
        df = self.get_unit_data(unit)
        if df.empty:
            return (False, 0, 0)

        # Ensure time column exists
        if 'time' not in df.columns:
            return (False, 0, 0)

        # Must have tag column for per-tag validation
        if 'tag' not in df.columns:
            # Wide format or single tag - fall back to overall timestamp check
            latest = df['time'].max()
            age = datetime.now() - latest
            is_fresh = age < timedelta(hours=max_age_hours)
            return (is_fresh, 1 if is_fresh else 0, 1)

        # Filter out None/null tags
        df_with_tags = df[df['tag'].notna()]

        if len(df_with_tags) == 0:
            # No valid tags - fall back to overall timestamp check
            latest = df['time'].max()
            age = datetime.now() - latest
            is_fresh = age < timedelta(hours=max_age_hours)
            return (is_fresh, 1 if is_fresh else 0, 1)

        # Get latest timestamp per tag
        cutoff_time = datetime.now() - timedelta(hours=max_age_hours)
        tag_latest = df_with_tags.groupby('tag')['time'].max()

        # Count how many tags are fresh
        fresh_tags = (tag_latest > cutoff_time).sum()
        total_tags = len(tag_latest)

        # Require at least 50% of tags to be fresh
        freshness_threshold = 0.5
        is_fresh = (fresh_tags / total_tags) >= freshness_threshold if total_tags > 0 else False

        return (is_fresh, int(fresh_tags), int(total_tags))

    def get_tag_latest_timestamps(self, unit: str) -> Dict[str, datetime]:
        """Get latest timestamp for EACH tag in a unit.

        Args:
            unit: Unit identifier

        Returns:
            Dictionary mapping tag name to latest timestamp
        """
        df = self.get_unit_data(unit)
        if df.empty or 'tag' not in df.columns or 'time' not in df.columns:
            return {}

        # Filter out None/null tags
        df_with_tags = df[df['tag'].notna()]
        if len(df_with_tags) == 0:
            return {}

        # Get latest timestamp per tag
        tag_latest = df_with_tags.groupby('tag')['time'].max()
        return tag_latest.to_dict()
    
    def get_data_freshness_info(self, unit: str, tag: str = None) -> Dict[str, Any]:
        """Get data freshness information for a unit/tag.
        
        Args:
            unit: Unit identifier
            tag: Optional tag filter
            
        Returns:
            Dictionary with freshness information
        """
        # Fast path: try to compute aggregates directly in DuckDB to avoid
        # materialising tens of millions of rows into memory (can OOM on ABF).
        if self.conn is not None:
            try:
                # Use a broad parquet glob so we consider the newest file for a
                # unit even when *.dedup.parquet has not been regenerated yet
                # (e.g., when dedup is deferred or failed on a large file).
                # Avoid double-counting when both master and *.dedup.parquet exist
                # by counting DISTINCT (time, tag) pairs for the unit.
                #
                # IMPORTANT: When multiple files exist for same unit (e.g., different lookback periods),
                # we want the MOST RECENT data (MAX timestamp), which should come from the actively
                # refreshed file. The WHERE unit = ? filter ensures we only scan the target unit.
                # If DuckDB returns stale data, it means an old file with same unit exists.
                # Solution: Read from specific file pattern or sort by file mtime.

                # Get all parquet files and find the most recently modified one for this unit
                import glob as _glob
                from pathlib import Path as _Path
                import os as _os_path
                from datetime import datetime as _datetime

                parquet_pattern = str(self.processed_dir / "*.parquet")
                all_files = _glob.glob(parquet_pattern)

                # Find files matching this unit (by filename convention: {unit}_*)
                # Match patterns: C-02001_*, \\C-02001_*, /C-02001_*
                unit_files = []
                for f in all_files:
                    filename = _os_path.basename(f)
                    # Check if filename starts with unit prefix
                    if filename.startswith(f"{unit}_"):
                        unit_files.append(f)

                # DEBUG: Print file selection for problematic units
                if unit == 'C-02001':
                    print(f"\n[DEBUG] C-02001 file search:")
                    print(f"  Pattern: {parquet_pattern}")
                    print(f"  Total files found: {len(all_files)}")
                    print(f"  Unit-specific files found: {len(unit_files)}")
                    if unit_files:
                        for f in unit_files[:5]:  # Show first 5
                            mtime = _datetime.fromtimestamp(_os_path.getmtime(f))
                            print(f"    - {_os_path.basename(f)}")
                            print(f"      mtime: {mtime.strftime('%Y-%m-%d %H:%M:%S')}")

                if not unit_files:
                    # Fallback: use broad glob
                    logger.debug(f"No specific files found for {unit}, using broad glob")
                    q = (
                        f"WITH src AS (SELECT time, tag FROM read_parquet('{self._parquet_glob(False)}') WHERE unit = ?) "
                        "SELECT (SELECT COUNT(*) FROM (SELECT DISTINCT time, tag FROM src) t) AS total, "
                        "       MIN(time) AS earliest, MAX(time) AS latest, "
                        "       COUNT(DISTINCT tag) AS uniq "
                        "FROM src"
                    )
                    total, earliest, latest, uniq = self.conn.execute(q, [unit]).fetchone()
                else:
                    # Read only from the most recently modified file for this unit
                    # Prefer .dedup.parquet over master .parquet
                    dedup_files = [f for f in unit_files if f.endswith('.dedup.parquet')]
                    if dedup_files:
                        unit_files_with_mtime = [(f, _os_path.getmtime(f)) for f in dedup_files]
                    else:
                        unit_files_with_mtime = [(f, _os_path.getmtime(f)) for f in unit_files]

                    latest_file = max(unit_files_with_mtime, key=lambda x: x[1])[0]

                    # DEBUG: Show selected file for C-02001
                    if unit == 'C-02001':
                        selected_mtime = _datetime.fromtimestamp(_os_path.getmtime(latest_file))
                        print(f"  SELECTED FILE: {_os_path.basename(latest_file)}")
                        print(f"    mtime: {selected_mtime.strftime('%Y-%m-%d %H:%M:%S')}")
                        print(f"    Reading data...")

                    q = (
                        f"WITH src AS (SELECT time, tag FROM read_parquet('{latest_file}') WHERE unit = ?) "
                        "SELECT (SELECT COUNT(*) FROM (SELECT DISTINCT time, tag FROM src) t) AS total, "
                        "       MIN(time) AS earliest, MAX(time) AS latest, "
                        "       COUNT(DISTINCT tag) AS uniq "
                        "FROM src"
                    )
                    total, earliest, latest, uniq = self.conn.execute(q, [unit]).fetchone()

                    # DEBUG: Show query result for C-02001
                    if unit == 'C-02001' and latest:
                        import pandas as _pd
                        latest_dt = _pd.to_datetime(latest)
                        age_hours = (_datetime.now() - latest_dt).total_seconds() / 3600
                        print(f"  QUERY RESULT:")
                        print(f"    Latest timestamp: {latest_dt}")
                        print(f"    Age: {age_hours:.1f}h")
                        print(f"    Records: {total:,}\n")
                # Prepare base structure
                info = {
                    'unit': unit,
                    'tag': tag,
                    'total_records': int(total or 0),
                    'latest_timestamp': latest,
                    'earliest_timestamp': earliest,
                    'data_age_hours': None,
                    'is_stale': True,
                    'unique_tags': [],
                    'date_range_days': None,
                }
                # Attach unique tag count as list only if needed elsewhere
                try:
                    info['unique_tags'] = [None] * int(uniq or 0)
                except Exception:
                    info['unique_tags'] = []
                # Age maths
                if latest is not None:
                    try:
                        import pandas as _pd
                        latest_dt = _pd.to_datetime(latest)
                        now_utc = _pd.Timestamp.now(tz='UTC')
                        if getattr(latest_dt, 'tz', None) is None:
                            # treat as local -> convert to UTC for age math
                            latest_dt = latest_dt.tz_localize(_pd.Timestamp.now().tz).tz_convert('UTC')
                        age_h = (now_utc - latest_dt.tz_convert('UTC')).total_seconds() / 3600
                        info['data_age_hours'] = age_h
                    except Exception:
                        pass
                # Date range days
                if info.get('earliest_timestamp') and info.get('latest_timestamp'):
                    try:
                        import pandas as _pd
                        e = _pd.to_datetime(info['earliest_timestamp'])
                        l = _pd.to_datetime(info['latest_timestamp'])
                        info['date_range_days'] = (l - e).total_seconds() / (24 * 3600)
                    except Exception:
                        pass
                # NEW: Use per-tag freshness validation instead of just overall latest timestamp
                try:
                    max_age_env = float(os.getenv('MAX_AGE_HOURS', '1.0'))
                except Exception:
                    max_age_env = 1.0

                # Check if at least 50% of tags are fresh (prevents false positives)
                try:
                    is_fresh, fresh_count, total_count = self.check_tag_freshness(unit, max_age_hours=max_age_env)
                    info['is_stale'] = not is_fresh  # Stale if NOT fresh
                    info['fresh_tag_count'] = fresh_count
                    info['total_tag_count'] = total_count
                except Exception as e:
                    # Fallback to old behavior if per-tag check fails
                    logger.debug(f"Per-tag freshness check failed for {unit}, using overall timestamp: {e}")
                    info['is_stale'] = (info.get('data_age_hours') is not None) and (info['data_age_hours'] > max_age_env)
                    info['fresh_tag_count'] = None
                    info['total_tag_count'] = None
                # If user requested a specific tag, fall back to full load to compute tag-specific metrics only
                if tag is None:
                    return info
            except Exception:
                # Fall back to the generic path below
                pass

        # Generic path: materialise unit rows (slower and memory heavy, keep for compatibility)
        df = self.get_unit_data(unit)

        info = {
            'unit': unit,
            'tag': tag,
            'total_records': len(df),
            'latest_timestamp': None,
            'earliest_timestamp': None,
            'data_age_hours': None,
            'is_stale': True,
            'unique_tags': [],
            'date_range_days': None
        }
        
        if df.empty:
            return info
        
        # Filter by tag if specified
        if tag and 'tag' in df.columns:
            tag_df = df[df['tag'] == tag]
            if not tag_df.empty:
                df = tag_df
                info['records_for_tag'] = len(df)
        
        # Get unique tags
        if 'tag' in df.columns:
            info['unique_tags'] = df['tag'].unique().tolist()
        
        # Get time information
        if 'time' in df.columns and not df.empty:
            # Normalize timestamps: treat naive times as LOCAL, then convert to UTC for age math
            try:
                times = pd.to_datetime(df['time'], errors='coerce')
                local_tz = datetime.now().astimezone().tzinfo
                if getattr(times.dt, 'tz', None) is None:
                    times = times.dt.tz_localize(local_tz)
                else:
                    # ensure times are timezone-aware
                    times = times.dt.tz_convert(local_tz)
                # Convert to UTC for consistent comparison
                times_utc = times.dt.tz_convert('UTC')
                latest_utc = times_utc.max()
                earliest_utc = times_utc.min()
                # Store naive versions for display
                info['latest_timestamp'] = latest_utc.tz_convert(local_tz).tz_localize(None)
                info['earliest_timestamp'] = earliest_utc.tz_convert(local_tz).tz_localize(None)
            except Exception:
                # Fallback to naive
                info['latest_timestamp'] = pd.to_datetime(df['time'].max(), errors='coerce')
                info['earliest_timestamp'] = pd.to_datetime(df['time'].min(), errors='coerce')
                latest_utc = None

            # Calculate data age using UTC
            try:
                now_utc = pd.Timestamp.now(tz='UTC')
                if latest_utc is None:
                    latest_utc = pd.to_datetime(info['latest_timestamp']).tz_localize(datetime.now().astimezone().tzinfo).tz_convert('UTC')
                age_delta = now_utc - latest_utc
                info['data_age_hours'] = age_delta.total_seconds() / 3600
            except Exception:
                now = datetime.now()
                latest = info['latest_timestamp']
                info['data_age_hours'] = ((now - latest).total_seconds() / 3600) if latest is not None else None

            # Determine stale using configured max age (env), default 1.0h
            try:
                max_age_env = float(os.getenv('MAX_AGE_HOURS', '1.0'))
            except Exception:
                max_age_env = 1.0
            info['is_stale'] = (info['data_age_hours'] is not None) and (info['data_age_hours'] > max_age_env)
            
            # Calculate date range
            if info['earliest_timestamp'] and info['latest_timestamp']:
                range_delta = info['latest_timestamp'] - info['earliest_timestamp']
                info['date_range_days'] = range_delta.total_seconds() / (24 * 3600)
        
        return info

    def get_all_units(self) -> List[str]:
        """Get list of all units with data or configuration.

        Combines units discovered from existing Parquet files with units
        declared via config tag files so new units (e.g., C-02001) show up
        in scans even before the first fetch.
        """
        units = set()
        # Prefer DuckDB distinct units from Parquet contents
        if self.conn is not None:
            try:
                q = f"SELECT DISTINCT unit FROM read_parquet('{self._parquet_glob(True)}') WHERE unit IS NOT NULL"
                for (u,) in self.conn.execute(q).fetchall():
                    units.add(str(u))
            except Exception:
                pass
        # Units from file names as backup
        for file_info in self.get_available_parquet_files():
            unit_name = file_info.get('unit')
            if unit_name and unit_name.lower() not in ('pcfs', 'pcmsb', 'abf'):
                units.add(unit_name)
        # Units from config
        for u in self._discover_config_units():
            units.add(u)
        # Normalize legacy aliases (e.g., ABF unit name)
        alias_map = {
            '07-MT01/K001': '07-MT01-K001',
            '07-MT001/K001': '07-MT01-K001',
            'FI-07001': '07-MT01-K001',
        }
        normalized = set(alias_map.get(u, u) for u in units)
        return sorted(list(normalized))
    
    def should_fetch_from_pi(self, unit: str, tag: str = None, max_age_hours: float = 1.0) -> bool:
        """Determine if data should be fetched from PI based on freshness.
        
        Args:
            unit: Unit identifier
            tag: Optional tag filter
            max_age_hours: Maximum age in hours before considering stale
            
        Returns:
            True if should fetch from PI
        """
        info = self.get_data_freshness_info(unit, tag)
        
        # No data exists
        if info['total_records'] == 0:
            logger.info(f"No data for {unit}/{tag}, should fetch from PI")
            return True
        
        # Data is stale
        if info['data_age_hours'] and info['data_age_hours'] > max_age_hours:
            logger.info(f"Data for {unit}/{tag} is {info['data_age_hours']:.1f} hours old, should fetch from PI")
            return True
        
        logger.info(f"Data for {unit}/{tag} is fresh ({info['data_age_hours']:.1f} hours old), no need to fetch")
        return False
    
    
    
    def get_database_status(self) -> Dict[str, Any]:
        """Get comprehensive database status."""
        files = self.get_available_parquet_files()
        
        total_size_mb = sum(f['size_mb'] for f in files)
        total_files = len(files)
        
        # Get units and their info
        units_info = []
        for unit in self.get_all_units():
            unit_files = [f for f in files if unit in f['filename']]
            unit_size = sum(f['size_mb'] for f in unit_files)
            
            # Get freshness info
            freshness = self.get_data_freshness_info(unit)
            
            units_info.append({
                'unit': unit,
                'files': len(unit_files),
                'size_mb': unit_size,
                'records': freshness['total_records'],
                'latest_data': freshness['latest_timestamp'],
                'data_age_hours': freshness['data_age_hours'],
                'unique_tags': len(freshness['unique_tags']),
                'is_stale': freshness['is_stale']
            })
        
        # Sort by latest data
        units_info.sort(key=lambda x: x['latest_data'] or datetime.min, reverse=True)
        
        return {
            'data_directory': str(self.data_dir),
            'processed_directory': str(self.processed_dir),
            'total_files': total_files,
            'total_size_mb': round(total_size_mb, 2),
            'total_size_gb': round(total_size_mb / 1024, 2),
            'units': units_info,
            'duckdb_available': self.conn is not None,
            'duckdb_path': str(self.duckdb_path) if self.duckdb_path.exists() else None,
            'status_timestamp': datetime.now().isoformat()
        }
    
    def query_with_duckdb(self, query: str) -> pd.DataFrame:
        """Execute SQL query using DuckDB if available.
        
        Args:
            query: SQL query string
            
        Returns:
            Query results as DataFrame
        """
        if not self.conn:
            raise RuntimeError("DuckDB connection not available")
        
        try:
            result = self.conn.execute(query).fetchdf()
            return result
        except Exception as e:
            logger.error(f"DuckDB query failed: {e}")
            raise
    
    def get_tag_summary(self, unit: str) -> pd.DataFrame:
        """Get summary statistics for all tags in a unit.
        
        Args:
            unit: Unit identifier
            
        Returns:
            DataFrame with tag statistics
        """
        df = self.get_unit_data(unit)
        
        if df.empty or 'tag' not in df.columns:
            return pd.DataFrame()
        
        # Calculate statistics per tag
        summary = df.groupby('tag').agg({
            'value': ['count', 'mean', 'std', 'min', 'max'],
            'time': ['min', 'max']
        }).round(3)
        
        # Flatten column names
        summary.columns = ['_'.join(col).strip() for col in summary.columns]
        summary = summary.reset_index()
        
        # Calculate data age for each tag
        # Ensure datetime arithmetic is safe (handle tz-aware vs naive)
        now_local = datetime.now().astimezone()
        try:
            # Normalize to local naive timestamps for subtraction
            tmax = pd.to_datetime(summary['time_max'], errors='coerce')
            if getattr(getattr(tmax, 'dt', None), 'tz', None) is not None:
                tmax = tmax.dt.tz_convert(now_local.tzinfo).dt.tz_localize(None)
            summary['hours_since_last'] = tmax.apply(
                lambda x: (now_local.replace(tzinfo=None) - x).total_seconds() / 3600 if pd.notna(x) else None
            )
        except Exception:
            # Fallback without tz handling
            now = datetime.now()
            summary['hours_since_last'] = summary['time_max'].apply(
                lambda x: (now - x).total_seconds() / 3600 if pd.notna(x) else None
            )
        
        return summary
    
    def cleanup_old_files(self, days_to_keep: int = 30) -> int:
        """Remove old backup files (not the main data files).
        
        Args:
            days_to_keep: Number of days to keep files
            
        Returns:
            Number of files removed
        """
        cutoff_date = datetime.now() - timedelta(days=days_to_keep)
        removed_count = 0
        
        # Only remove backup or temporary files, not main data files
        backup_patterns = ['*.bak', '*.tmp', '*.backup', '*_old.parquet']
        
        for pattern in backup_patterns:
            for file_path in self.processed_dir.glob(pattern):
                try:
                    modified = datetime.fromtimestamp(file_path.stat().st_mtime)
                    if modified < cutoff_date:
                        file_path.unlink()
                        removed_count += 1
                        logger.info(f"Removed old file: {file_path}")
                except Exception as e:
                    logger.warning(f"Could not remove {file_path}: {e}")
        
        return removed_count
    
    def __del__(self):
        """Close DuckDB connection on cleanup"""
        if self.conn:
            try:
                self.conn.close()
            except Exception:
                pass
