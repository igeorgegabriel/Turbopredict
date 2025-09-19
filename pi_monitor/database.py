"""Database utilities for TURBOPREDICT X PROTEAN local database operations."""

from __future__ import annotations

import sqlite3
from pathlib import Path
from typing import Optional, Any
import pandas as pd
from datetime import datetime, timedelta
import logging

logger = logging.getLogger(__name__)


class LocalDatabase:
    """Local SQLite database handler for PI data caching and timestamp tracking."""
    
    def __init__(self, db_path: Path | str = "data/processed/local_db.sqlite"):
        """Initialize local database connection.
        
        Args:
            db_path: Path to SQLite database file
        """
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._init_tables()
    
    def _init_tables(self) -> None:
        """Initialize required database tables."""
        with sqlite3.connect(self.db_path) as conn:
            # Main data table
            conn.execute("""
                CREATE TABLE IF NOT EXISTS pi_data (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    timestamp DATETIME NOT NULL,
                    value REAL,
                    plant TEXT,
                    unit TEXT,
                    tag TEXT NOT NULL,
                    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(timestamp, tag, plant, unit)
                )
            """)
            
            # Metadata table for tracking last updates
            conn.execute("""
                CREATE TABLE IF NOT EXISTS update_metadata (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    tag TEXT NOT NULL,
                    plant TEXT,
                    unit TEXT,
                    last_update DATETIME NOT NULL,
                    last_pi_fetch DATETIME,
                    record_count INTEGER DEFAULT 0,
                    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(tag, plant, unit)
                )
            """)
            
            # Create indices for performance
            conn.execute("CREATE INDEX IF NOT EXISTS idx_pi_data_timestamp ON pi_data(timestamp)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_pi_data_tag ON pi_data(tag)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_metadata_tag ON update_metadata(tag)")
            
            conn.commit()
    
    def get_latest_timestamp(self, tag: str, plant: str | None = None, unit: str | None = None) -> Optional[datetime]:
        """Get the latest timestamp for a specific tag from local database.
        
        Args:
            tag: PI tag name
            plant: Plant identifier (optional)
            unit: Unit identifier (optional)
            
        Returns:
            Latest timestamp or None if no data exists
        """
        with sqlite3.connect(self.db_path) as conn:
            query = "SELECT MAX(timestamp) FROM pi_data WHERE tag = ?"
            params = [tag]
            
            if plant is not None:
                query += " AND plant = ?"
                params.append(plant)
                
            if unit is not None:
                query += " AND unit = ?"
                params.append(unit)
            
            result = conn.execute(query, params).fetchone()
            if result[0]:
                return datetime.fromisoformat(result[0])
            return None
    
    def get_data_freshness_info(self, tag: str, plant: str | None = None, unit: str | None = None) -> dict[str, Any]:
        """Get comprehensive freshness information for a tag.
        
        Args:
            tag: PI tag name  
            plant: Plant identifier (optional)
            unit: Unit identifier (optional)
            
        Returns:
            Dictionary with freshness information
        """
        with sqlite3.connect(self.db_path) as conn:
            # Get latest data timestamp
            query = """
                SELECT MAX(timestamp) as latest_timestamp, COUNT(*) as record_count
                FROM pi_data 
                WHERE tag = ?
            """
            params = [tag]
            
            if plant is not None:
                query += " AND plant = ?"
                params.append(plant)
                
            if unit is not None:
                query += " AND unit = ?"
                params.append(unit)
            
            data_result = conn.execute(query, params).fetchone()
            
            # Get metadata info
            meta_query = """
                SELECT last_update, last_pi_fetch, record_count, updated_at
                FROM update_metadata 
                WHERE tag = ?
            """
            meta_params = [tag]
            
            if plant is not None:
                meta_query += " AND plant = ?"
                meta_params.append(plant)
                
            if unit is not None:
                meta_query += " AND unit = ?"
                meta_params.append(unit)
            
            meta_result = conn.execute(meta_query, meta_params).fetchone()
            
            info = {
                'tag': tag,
                'plant': plant,
                'unit': unit,
                'latest_data_timestamp': datetime.fromisoformat(data_result[0]) if data_result[0] else None,
                'local_record_count': data_result[1] if data_result else 0,
                'last_update': datetime.fromisoformat(meta_result[0]) if meta_result and meta_result[0] else None,
                'last_pi_fetch': datetime.fromisoformat(meta_result[1]) if meta_result and meta_result[1] else None,
                'metadata_record_count': meta_result[2] if meta_result else 0,
                'metadata_updated_at': datetime.fromisoformat(meta_result[3]) if meta_result and meta_result[3] else None
            }
            
            # Calculate staleness
            now = datetime.now()
            if info['latest_data_timestamp']:
                info['data_age_hours'] = (now - info['latest_data_timestamp']).total_seconds() / 3600
                info['is_stale'] = info['data_age_hours'] > 1.0  # Consider stale if > 1 hour old
            else:
                info['data_age_hours'] = None
                info['is_stale'] = True
                
            return info
    
    def should_fetch_from_pi(self, tag: str, plant: str | None = None, unit: str | None = None, 
                           max_age_hours: float = 1.0) -> bool:
        """Determine if data should be fetched from PI based on local database freshness.
        
        Args:
            tag: PI tag name
            plant: Plant identifier (optional)
            unit: Unit identifier (optional)
            max_age_hours: Maximum age in hours before considering data stale
            
        Returns:
            True if data should be fetched from PI
        """
        info = self.get_data_freshness_info(tag, plant, unit)
        
        # No local data exists
        if info['latest_data_timestamp'] is None:
            logger.info(f"No local data for {tag}, fetching from PI")
            return True
        
        # Data is stale
        if info['data_age_hours'] and info['data_age_hours'] > max_age_hours:
            logger.info(f"Data for {tag} is {info['data_age_hours']:.1f} hours old, fetching from PI")
            return True
            
        logger.info(f"Data for {tag} is fresh ({info['data_age_hours']:.1f} hours old), skipping PI fetch")
        return False
    
    def store_dataframe(self, df: pd.DataFrame, tag: str | None = None, plant: str | None = None, 
                       unit: str | None = None) -> int:
        """Store DataFrame data into local database.
        
        Args:
            df: DataFrame with columns ['time', 'value', 'plant', 'unit', 'tag']
            tag: Override tag value (optional)
            plant: Override plant value (optional)
            unit: Override unit value (optional)
            
        Returns:
            Number of rows inserted
        """
        if df.empty:
            return 0
            
        # Prepare dataframe for insertion
        df = df.copy()
        
        # Override identifiers if provided
        if tag is not None:
            df['tag'] = tag
        if plant is not None:
            df['plant'] = plant
        if unit is not None:
            df['unit'] = unit
        
        # Ensure required columns exist
        required_cols = ['time', 'value', 'tag']
        if not all(col in df.columns for col in required_cols):
            raise ValueError(f"DataFrame must contain columns: {required_cols}")
        
        # Rename time column to timestamp for database
        df = df.rename(columns={'time': 'timestamp'})
        
        # Insert data
        with sqlite3.connect(self.db_path) as conn:
            inserted = 0
            for _, row in df.iterrows():
                try:
                    conn.execute("""
                        INSERT OR IGNORE INTO pi_data (timestamp, value, plant, unit, tag)
                        VALUES (?, ?, ?, ?, ?)
                    """, (
                        row['timestamp'].isoformat() if pd.notna(row['timestamp']) else None,
                        row['value'] if pd.notna(row['value']) else None,
                        row.get('plant'),
                        row.get('unit'),
                        row['tag']
                    ))
                    inserted += conn.total_changes - inserted
                except Exception as e:
                    logger.warning(f"Failed to insert row for {row.get('tag', 'unknown')}: {e}")
            
            # Update metadata
            self._update_metadata(conn, df)
            conn.commit()
            
        logger.info(f"Stored {inserted} new records for tag(s)")
        return inserted
    
    def _update_metadata(self, conn: sqlite3.Connection, df: pd.DataFrame) -> None:
        """Update metadata table with latest information."""
        # Group by tag/plant/unit combinations
        groups = df.groupby(['tag', df.get('plant', None), df.get('unit', None)], dropna=False)
        
        for (tag, plant, unit), group in groups:
            latest_timestamp = group['timestamp'].max()
            record_count = len(group)
            
            conn.execute("""
                INSERT OR REPLACE INTO update_metadata 
                (tag, plant, unit, last_update, last_pi_fetch, record_count, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
            """, (
                tag,
                plant if pd.notna(plant) else None,
                unit if pd.notna(unit) else None,
                latest_timestamp.isoformat(),
                datetime.now().isoformat(),
                record_count
            ))
    
    def get_local_data(self, tag: str, plant: str | None = None, unit: str | None = None,
                      start_time: datetime | None = None, end_time: datetime | None = None) -> pd.DataFrame:
        """Retrieve data from local database.
        
        Args:
            tag: PI tag name
            plant: Plant identifier (optional)
            unit: Unit identifier (optional)
            start_time: Start timestamp filter (optional)
            end_time: End timestamp filter (optional)
            
        Returns:
            DataFrame with local data
        """
        with sqlite3.connect(self.db_path) as conn:
            query = "SELECT timestamp, value, plant, unit, tag FROM pi_data WHERE tag = ?"
            params = [tag]
            
            if plant is not None:
                query += " AND plant = ?"
                params.append(plant)
                
            if unit is not None:
                query += " AND unit = ?"
                params.append(unit)
                
            if start_time is not None:
                query += " AND timestamp >= ?"
                params.append(start_time.isoformat())
                
            if end_time is not None:
                query += " AND timestamp <= ?"
                params.append(end_time.isoformat())
            
            query += " ORDER BY timestamp"
            
            df = pd.read_sql_query(query, conn, parse_dates=['timestamp'])
            df = df.rename(columns={'timestamp': 'time'})
            
            return df
    
    def cleanup_old_data(self, days_to_keep: int = 90) -> int:
        """Remove old data from local database.
        
        Args:
            days_to_keep: Number of days of data to retain
            
        Returns:
            Number of records deleted
        """
        cutoff_date = datetime.now() - timedelta(days=days_to_keep)
        
        with sqlite3.connect(self.db_path) as conn:
            result = conn.execute(
                "DELETE FROM pi_data WHERE timestamp < ?",
                (cutoff_date.isoformat(),)
            )
            deleted = result.rowcount
            conn.commit()
            
        logger.info(f"Cleaned up {deleted} old records older than {days_to_keep} days")
        return deleted