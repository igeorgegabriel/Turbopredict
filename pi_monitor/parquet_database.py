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
            # Default to DISABLED to use reliable dedup parquet files instead
            import os as _os
            if _os.getenv("DISABLE_DUCKDB", "1") == "1":
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

    def _is_temp_file(self, filename: str) -> bool:
        """Check if a filename is a temporary/backup file that should be excluded.

        Args:
            filename: The filename to check

        Returns:
            True if this is a temp/backup file
        """
        filename_lower = filename.lower()
        temp_patterns = [
            '_incremental_temp',
            '_retry_',
            '_refreshed_',
            '_backup',
            '.tmp',
            '.temp',
            '_old',
        ]
        return any(pattern in filename_lower for pattern in temp_patterns)

    def _is_temp_unit(self, unit_name: str) -> bool:
        """Check if a unit name looks like it came from a temp/backup file.

        Args:
            unit_name: The unit name to check

        Returns:
            True if this unit name contains temp/backup patterns
        """
        if not unit_name:
            return True
        unit_lower = unit_name.lower()
        temp_patterns = [
            'backup',
            'temp',
            'retry',
            'refreshed',
            'incremental',
            '_old',
            '.tmp',
        ]
        return any(pattern in unit_lower for pattern in temp_patterns)

    def _get_stable_parquet_files(self, unit: str = None, dedup_preferred: bool = True) -> List[str]:
        """Get list of stable (non-temp) parquet file paths for querying.

        This method filters out temp/backup/retry files and handles the case where
        multiple files exist for the same unit by selecting the most appropriate one.

        Args:
            unit: Optional unit filter - if provided, only return files for this unit
            dedup_preferred: If True, prefer .dedup.parquet files over master files

        Returns:
            List of absolute file paths to query
        """
        import glob as _glob
        from pathlib import Path as _Path
        import os as _os

        # Get all parquet files
        all_files = _glob.glob(str(self.processed_dir / "*.parquet"))

        # Filter out temp files
        stable_files = [f for f in all_files if not self._is_temp_file(_os.path.basename(f))]

        # If unit specified, filter to that unit and select best file
        if unit:
            unit_files = []
            for f in stable_files:
                filename = _os.path.basename(f)
                if filename.startswith(f"{unit}_"):
                    unit_files.append(f)

            if not unit_files:
                return []

            # Prefer dedup files over master files
            dedup_files = [f for f in unit_files if f.endswith('.dedup.parquet')]
            if dedup_preferred and dedup_files:
                # Among dedup files, select the most recently modified
                unit_files_with_mtime = [(f, _os.path.getmtime(f)) for f in dedup_files]
            else:
                # Select from all stable files for this unit
                unit_files_with_mtime = [(f, _os.path.getmtime(f)) for f in unit_files]

            # Return only the most recent file for this unit to avoid duplicates
            if unit_files_with_mtime:
                latest_file = max(unit_files_with_mtime, key=lambda x: x[1])[0]
                return [latest_file]
            return []

        return stable_files

    def _parquet_glob(self, dedup_preferred: bool = True, exclude_temp: bool = True) -> str:
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

        NOTE: This method is deprecated in favor of _get_stable_parquet_files()
        which properly handles temp file exclusion and duplicate file selection.

        Args:
            dedup_preferred: If True, only read *dedup.parquet files
            exclude_temp: If True, exclude temp/backup/retry files (default: True)
        """
        if dedup_preferred:
            return str(self.processed_dir / "*dedup.parquet")
        else:
            # Return pattern that excludes temp files by default
            if exclude_temp:
                # Return a list pattern that we'll need to handle specially
                return str(self.processed_dir / "*.parquet")
            else:
                return str(self.processed_dir / "*.parquet")
    
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
                # New ABF equipment mapping by file token
                if "21k002" in name or "21-k002" in name or "21_k002" in name:
                    units.add("21-K002")

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
                            token = m.group(1).upper()
                            # Whitelist only real unit prefixes; avoid interpreting
                            # instrument tag names (e.g., SI-21001, TI-*, PI-*) as units.
                            prefix = token.split('-')[0]
                            if prefix in {"K", "C", "XT"}:
                                units.add(token)
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
        # Fast path: DuckDB over Parquet - use stable file selection to avoid duplicates
        if self.conn is not None:
            try:
                # Get the stable file(s) for this unit (excludes temp files, handles duplicates)
                stable_files = self._get_stable_parquet_files(unit=unit, dedup_preferred=True)

                if not stable_files:
                    logger.warning(f"No stable parquet files found for unit {unit}")
                    return pd.DataFrame()

                # Build file list for DuckDB query
                if len(stable_files) == 1:
                    file_pattern = f"'{stable_files[0]}'"
                else:
                    file_list = ", ".join(f"'{f}'" for f in stable_files)
                    file_pattern = f"[{file_list}]"

                q = (
                    "SELECT CAST(time AS TIMESTAMP) AS time, value, plant, unit, tag "
                    f"FROM read_parquet({file_pattern}) WHERE unit = ?"
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
                logger.info(f"Loaded {len(df)} records for unit {unit} via DuckDB (from {len(stable_files)} file(s))")
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
        # Heuristic: when caller requests a long lookback window (e.g., 90 days
        # for plotting), prefer long-span master files over small "updated"
        # slices that may only contain a few recent days.
        long_lookback = False
        try:
            if start_time is not None:
                long_lookback = (datetime.now() - start_time) > timedelta(days=30)
        except Exception:
            long_lookback = False

        if long_lookback and dedup_files:
            # Prefer dedup which typically spans months/years
            target_file = dedup_files[0]
        elif long_lookback and regular_files:
            target_file = regular_files[0]
        elif updated_files:
            # Use newest updated file (usually recent slice)
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

    def get_unit_tag_data(
        self,
        unit: str,
        tag: str,
        start_time: datetime | None = None,
        end_time: datetime | None = None,
    ) -> pd.DataFrame:
        """Return rows for a specific unit+tag within an optional time window.

        This method avoids loading entire year-long Parquet files into memory.
        It prefers DuckDB for predicate pushdown; when DuckDB is disabled or
        unavailable, it falls back to a pyarrow.dataset scanner with filters,
        and finally to a streaming row-group reader as a last resort.
        """
        # Fast path via DuckDB (handles predicate pushdown efficiently)
        if self.conn is not None:
            try:
                # Use stable file selection to avoid temp files and duplicates
                stable_files = self._get_stable_parquet_files(unit=unit, dedup_preferred=True)

                if not stable_files:
                    return pd.DataFrame()

                # Build file pattern for DuckDB
                if len(stable_files) == 1:
                    file_pattern = f"'{stable_files[0]}'"
                else:
                    file_list = ", ".join(f"'{f}'" for f in stable_files)
                    file_pattern = f"[{file_list}]"

                q = (
                    "SELECT CAST(time AS TIMESTAMP) AS time, value, plant, unit, tag "
                    f"FROM read_parquet({file_pattern}) "
                    "WHERE unit = ? AND tag = ?"
                )
                params: list[object] = [unit, tag]
                if start_time is not None:
                    q += " AND time >= ?"
                    params.append(start_time)
                if end_time is not None:
                    q += " AND time <= ?"
                    params.append(end_time)
                q += " ORDER BY time"
                df = self.conn.execute(q, params).fetchdf()
                logger.info(
                    f"Loaded {len(df)} records for {unit}/{tag} via DuckDB (from {len(stable_files)} file(s))"
                )
                return df
            except Exception as e:
                logger.warning(
                    f"DuckDB read failed for {unit}/{tag}: {e}; falling back to Arrow"
                )

        # Fallback: pyarrow.dataset with filter pushdown over top-level *.parquet
        try:
            import pyarrow.dataset as ds
            import pyarrow as pa  # noqa: F401 (ensures pyarrow present)

            files = list(self.processed_dir.glob("*.parquet"))
            if not files:
                return pd.DataFrame()

            dataset = ds.dataset([str(p) for p in files], format="parquet")
            schema_names = set(dataset.schema.names)
            # Require fields to exist; inconsistent legacy files (e.g. refreshed slices)
            # may miss 'tag' and/or 'unit'. In that case, skip to streaming fallback.
            if not {"unit", "tag", "time"}.issubset(schema_names):
                raise RuntimeError("dataset missing required fields for filter pushdown")

            filt = (ds.field("unit") == unit) & (ds.field("tag") == tag)
            if start_time is not None:
                filt = filt & (ds.field("time") >= start_time)
            if end_time is not None:
                filt = filt & (ds.field("time") <= end_time)

            cols = [c for c in ["time", "value", "unit", "tag"] if c in schema_names]
            table = dataset.to_table(filter=filt, columns=cols)
            df = table.to_pandas()
            if "time" in df.columns:
                df["time"] = pd.to_datetime(df["time"], errors="coerce")
                df = df.dropna(subset=["time"]).sort_values("time")
            logger.info(
                f"Loaded {len(df)} records for {unit}/{tag} via pyarrow.dataset"
            )
            return df
        except Exception as e:
            logger.warning(
                f"pyarrow.dataset read failed for {unit}/{tag}: {e}; falling back to streaming"
            )

        # Last-resort fallback: stream row groups to keep memory bounded
        try:
            import pyarrow.parquet as pq
            import pyarrow as pa

            # Prefer reading only files that likely contain this unit
            unit_patterns = [
                f"*{unit}*.parquet",
                f"*{unit.replace('/', '-')}*.parquet",
                f"*{unit.replace('/', '_')}*.parquet",
                f"*{unit.replace('-', '_')}*.parquet",
            ]
            candidate_files: list[Path] = []
            for pat in unit_patterns:
                candidate_files.extend(list(self.processed_dir.glob(pat)))
            # If nothing matched, fall back to dedup/*.parquet
            if not candidate_files:
                candidate_files = list(self.processed_dir.glob("*dedup.parquet")) or list(
                    self.processed_dir.glob("*.parquet")
                )
            if not candidate_files:
                return pd.DataFrame()

            batches: list[pd.DataFrame] = []
            for path in candidate_files:
                try:
                    pf = pq.ParquetFile(str(path))
                except Exception:
                    continue
                # Only columns we need
                wanted_cols = [
                    c for c in ["time", "value", "plant", "unit", "tag"]
                    if c in pf.schema.names
                ]
                for rb in pf.iter_batches(columns=wanted_cols):
                    tbl = pa.Table.from_batches([rb])
                    df_part = tbl.to_pandas()
                    # Apply filters in pandas; batches keep memory bounded
                    try:
                        if "unit" in df_part.columns:
                            df_part = df_part[df_part["unit"] == unit]
                        if "tag" in df_part.columns:
                            df_part = df_part[df_part["tag"] == tag]
                        if start_time is not None and "time" in df_part.columns:
                            df_part = df_part[pd.to_datetime(df_part["time"]) >= start_time]
                        if end_time is not None and "time" in df_part.columns:
                            df_part = df_part[pd.to_datetime(df_part["time"]) <= end_time]
                        if not df_part.empty:
                            batches.append(df_part)
                    except Exception:
                        # Skip malformed batch but continue streaming others
                        pass
            if not batches:
                return pd.DataFrame()
            out = pd.concat(batches, ignore_index=True)
            if "time" in out.columns:
                out["time"] = pd.to_datetime(out["time"], errors="coerce")
                out = out.dropna(subset=["time"]).sort_values("time")
            logger.info(
                f"Loaded {len(out)} records for {unit}/{tag} via streaming fallback"
            )
            return out
        except Exception as e:
            logger.error(f"Failed to stream Parquet for {unit}/{tag}: {e}")
            return pd.DataFrame()

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

                # Use the new stable file selection method to avoid temp files and duplicates
                stable_files = self._get_stable_parquet_files(unit=unit, dedup_preferred=True)

                if not stable_files:
                    # No stable files found for this unit
                    logger.debug(f"No stable files found for {unit}")
                    info = {
                        'unit': unit,
                        'tag': tag,
                        'total_records': 0,
                        'latest_timestamp': None,
                        'earliest_timestamp': None,
                        'data_age_hours': None,
                        'is_stale': True,
                        'unique_tags': [],
                        'date_range_days': None,
                    }
                    return info

                # Use the selected stable file for querying
                latest_file = stable_files[0]  # _get_stable_parquet_files returns only one file per unit

                q = (
                    f"WITH src AS (SELECT time, tag FROM read_parquet('{latest_file}') WHERE unit = ?) "
                    "SELECT (SELECT COUNT(*) FROM (SELECT DISTINCT time, tag FROM src) t) AS total, "
                    "       MIN(time) AS earliest, MAX(time) AS latest, "
                    "       COUNT(DISTINCT tag) AS uniq "
                    "FROM src"
                )
                total, earliest, latest, uniq = self.conn.execute(q, [unit]).fetchone()
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

        This method now checks both:
        - Top-level parquet files (data/processed/*.parquet)
        - Partitioned dataset directory (data/processed/dataset/plant=*/unit=*/...)
        """
        units = set()

        # Check partitioned dataset directory if it exists
        dataset_dir = self.processed_dir / "dataset"
        if dataset_dir.exists():
            try:
                # Scan for unit directories in partitioned structure
                for plant_dir in dataset_dir.glob("plant=*"):
                    for unit_dir in plant_dir.glob("unit=*"):
                        unit_name = unit_dir.name.replace("unit=", "")
                        # Filter out backup/temp unit names and plant-level names
                        if unit_name and unit_name.lower() not in ('pcfs', 'pcmsb', 'abf') and not self._is_temp_unit(unit_name):
                            units.add(unit_name)
            except Exception as e:
                logger.warning(f"Failed to scan partitioned dataset: {e}")

        # Get units from stable top-level parquet files (excludes temp files)
        if self.conn is not None:
            try:
                # Get stable files only
                stable_files = self._get_stable_parquet_files(unit=None, dedup_preferred=False)
                if stable_files:
                    # Build file list for DuckDB
                    file_list = ", ".join(f"'{f}'" for f in stable_files)
                    q = f"SELECT DISTINCT unit FROM read_parquet([{file_list}]) WHERE unit IS NOT NULL"
                    for (u,) in self.conn.execute(q).fetchall():
                        unit_str = str(u)
                        # Filter out unit names that look like temp/backup artifacts
                        if not self._is_temp_unit(unit_str):
                            units.add(unit_str)
            except Exception as e:
                logger.warning(f"Failed to get units from DuckDB: {e}")

        # Units from file names as backup (exclude temp files and temp unit names)
        for file_info in self.get_available_parquet_files():
            filename = file_info.get('filename', '')
            if not self._is_temp_file(filename):
                unit_name = file_info.get('unit')
                # Also check if the unit name itself looks like a temp/backup artifact
                if unit_name and unit_name.lower() not in ('pcfs', 'pcmsb', 'abf') and not self._is_temp_unit(unit_name):
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
