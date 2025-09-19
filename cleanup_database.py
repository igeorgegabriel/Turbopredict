#!/usr/bin/env python3
"""
Database Cleanup Utility for TURBOPREDICT X PROTEAN
Cleans up orphaned entries and optimizes database structure
"""

import sqlite3
import logging
from pathlib import Path
import pandas as pd
from datetime import datetime
import sys

# Add current directory to path
sys.path.insert(0, str(Path(__file__).parent))

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def analyze_sqlite_database(db_path: Path):
    """Analyze the SQLite database to understand its structure"""
    logger.info(f"Analyzing SQLite database: {db_path}")
    
    if not db_path.exists():
        logger.warning(f"Database not found: {db_path}")
        return
    
    conn = sqlite3.connect(str(db_path))
    cursor = conn.cursor()
    
    try:
        # Get all tables
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
        tables = cursor.fetchall()
        logger.info(f"Found {len(tables)} tables: {[t[0] for t in tables]}")
        
        for table_name, in tables:
            # Get table info
            cursor.execute(f"PRAGMA table_info({table_name});")
            columns = cursor.fetchall()
            logger.info(f"\nTable: {table_name}")
            logger.info(f"Columns: {[col[1] for col in columns]}")
            
            # Get row count
            cursor.execute(f"SELECT COUNT(*) FROM {table_name};")
            count = cursor.fetchone()[0]
            logger.info(f"Row count: {count:,}")
            
            # Show sample data
            cursor.execute(f"SELECT * FROM {table_name} LIMIT 5;")
            sample = cursor.fetchall()
            if sample:
                logger.info("Sample data:")
                for row in sample[:3]:  # Show first 3 rows
                    logger.info(f"  {row}")
                    
            # Check for unit/tag patterns
            try:
                cursor.execute(f"SELECT DISTINCT unit FROM {table_name} LIMIT 10;")
                units = cursor.fetchall()
                if units:
                    logger.info(f"Sample units: {[u[0] for u in units]}")
            except:
                pass
                
    except Exception as e:
        logger.error(f"Error analyzing database: {e}")
    finally:
        conn.close()


def clean_sqlite_database(db_path: Path, keep_units: list = None):
    """Clean up the SQLite database, keeping only specified units"""
    if keep_units is None:
        keep_units = ['K-12-01', 'K-16-01', 'K-19-01', 'K-31-01']
    
    logger.info(f"Cleaning SQLite database: {db_path}")
    logger.info(f"Keeping units: {keep_units}")
    
    if not db_path.exists():
        logger.warning(f"Database not found: {db_path}")
        return
    
    # Backup first
    backup_path = db_path.with_suffix('.backup.sqlite')
    import shutil
    shutil.copy2(str(db_path), str(backup_path))
    logger.info(f"Backup created: {backup_path}")
    
    conn = sqlite3.connect(str(db_path))
    cursor = conn.cursor()
    
    try:
        # Get tables
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
        tables = [t[0] for t in cursor.fetchall()]
        
        for table_name in tables:
            # Check if table has unit column
            cursor.execute(f"PRAGMA table_info({table_name});")
            columns = [col[1] for col in cursor.fetchall()]
            
            if 'unit' in columns:
                # Count before cleanup
                cursor.execute(f"SELECT COUNT(*) FROM {table_name};")
                before_count = cursor.fetchone()[0]
                
                # Keep only specified units
                placeholders = ','.join(['?' for _ in keep_units])
                cursor.execute(f"DELETE FROM {table_name} WHERE unit NOT IN ({placeholders});", keep_units)
                
                # Count after cleanup
                cursor.execute(f"SELECT COUNT(*) FROM {table_name};")
                after_count = cursor.fetchone()[0]
                
                deleted = before_count - after_count
                logger.info(f"Table {table_name}: Deleted {deleted:,} rows, kept {after_count:,} rows")
            
            # Also clean up any entries with hash-like names
            if 'unit' in columns:
                cursor.execute(f"DELETE FROM {table_name} WHERE length(unit) > 20 AND unit LIKE '%-%';")
                hash_deleted = cursor.rowcount
                if hash_deleted > 0:
                    logger.info(f"Table {table_name}: Deleted {hash_deleted:,} hash-like entries")
        
        # Vacuum database to reclaim space
        cursor.execute("VACUUM;")
        conn.commit()
        logger.info("Database vacuumed and optimized")
        
    except Exception as e:
        logger.error(f"Error cleaning database: {e}")
        conn.rollback()
    finally:
        conn.close()


def analyze_parquet_files():
    """Analyze actual Parquet files to understand what should be in database"""
    logger.info("Analyzing Parquet files...")
    
    data_dir = Path("data/processed")
    parquet_files = list(data_dir.glob("*.parquet"))
    
    expected_units = set()
    
    for file_path in parquet_files:
        if 'K-' in file_path.name:  # Unit files
            try:
                # Extract unit from filename
                unit = file_path.name.split('_')[0]
                expected_units.add(unit)
                
                # Quick file stats
                size_mb = file_path.stat().st_size / (1024 * 1024)
                logger.info(f"  {file_path.name}: {size_mb:.1f} MB, unit: {unit}")
                
            except Exception as e:
                logger.warning(f"Could not analyze {file_path.name}: {e}")
    
    logger.info(f"Expected units from Parquet files: {sorted(expected_units)}")
    return sorted(expected_units)


def main():
    """Main cleanup process"""
    logger.info("TURBOPREDICT X PROTEAN - Database Cleanup Utility")
    logger.info("=" * 60)
    
    # Analyze Parquet files first
    expected_units = analyze_parquet_files()
    
    # Analyze SQLite database
    sqlite_path = Path("data/processed/turbopredict_local.sqlite")
    logger.info("\n" + "=" * 60)
    analyze_sqlite_database(sqlite_path)
    
    # Clean up SQLite database
    logger.info("\n" + "=" * 60)
    clean_sqlite_database(sqlite_path, expected_units)
    
    # Re-analyze after cleanup
    logger.info("\n" + "=" * 60)
    logger.info("POST-CLEANUP ANALYSIS:")
    analyze_sqlite_database(sqlite_path)
    
    logger.info("\n" + "=" * 60)
    logger.info("✅ Database cleanup completed!")
    logger.info("You can now run the scanner and should see only your 4 expected units.")


if __name__ == "__main__":
    main()