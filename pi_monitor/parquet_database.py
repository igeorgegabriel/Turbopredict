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

    def _parquet_glob(self, dedup_preferred: bool = True) -> str:
        """Return glob pattern for Parquet reads."""
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
        
        # Prioritize files by completeness and freshness: updated > refreshed > newest_of(regular, dedup)
        updated_files = [f for f in unit_files if 'updated' in f.name]
        refreshed_files = [f for f in unit_files if 'refreshed' in f.name and 'updated' not in f.name]
        dedup_files = [f for f in unit_files if 'dedup' in f.name and 'refreshed' not in f.name and 'updated' not in f.name]
        regular_files = [f for f in unit_files if 'dedup' not in f.name and 'refreshed' not in f.name and 'updated' not in f.name]
        
        target_file = None
        if updated_files:
            # Use newest updated file (highest priority - historical + fresh data combined)
            target_file = max(updated_files, key=lambda x: x.stat().st_mtime)
        elif refreshed_files:
            # Use newest refreshed file (fresh PI data only)
            target_file = max(refreshed_files, key=lambda x: x.stat().st_mtime)
        else:
            # Choose the newest between regular and dedup files (prefer fresh data)
            all_candidates = regular_files + dedup_files
            if all_candidates:
                target_file = max(all_candidates, key=lambda x: x.stat().st_mtime)
        
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
    
    def get_data_freshness_info(self, unit: str, tag: str = None) -> Dict[str, Any]:
        """Get data freshness information for a unit/tag.
        
        Args:
            unit: Unit identifier
            tag: Optional tag filter
            
        Returns:
            Dictionary with freshness information
        """
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
