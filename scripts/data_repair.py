#!/usr/bin/env python3
"""
Independent Data Validation and Repair Script for TurboPredict

This script safely validates and repairs parquet data without breaking the main system.
It handles NoneType values and missing data that cause anomaly detection failures.

Usage:
    python scripts/data_repair.py --scan-all         # Scan all parquet files
    python scripts/data_repair.py --unit K-12-01     # Scan specific unit
    python scripts/data_repair.py --repair --unit K-12-01  # Repair specific unit
    python scripts/data_repair.py --validate         # Validate repairs
"""

import argparse
import pandas as pd
import numpy as np
from pathlib import Path
import sys
import os
from typing import List, Dict, Optional, Tuple
import logging
from datetime import datetime
import glob

# Add parent directory to path for imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from pi_monitor.config import Config

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('data_repair.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


class DataRepairEngine:
    """Independent data repair engine that fixes NoneType and missing value issues"""

    def __init__(self, backup_enabled: bool = True):
        self.config = Config()
        self.backup_enabled = backup_enabled
        self.processed_dir = Path("data/processed")
        self.backup_dir = Path("data/backup")

        if backup_enabled:
            self.backup_dir.mkdir(exist_ok=True)

    def find_parquet_files(self, unit_filter: Optional[str] = None) -> List[Path]:
        """Find all parquet files, optionally filtered by unit"""
        patterns = []

        if unit_filter:
            # When unit is specified, search for that specific unit
            patterns = [
                f"data/processed/dataset/plant=PCFS/unit={unit_filter}/**/*.parquet",
                "data/processed/archive/*.parquet"  # Always include archive
            ]
        else:
            # When scanning all, include all patterns
            patterns = [
                "data/processed/dataset/**/*.parquet",
                "data/processed/archive/*.parquet"
            ]

        files = []
        for pattern in patterns:
            found = glob.glob(pattern, recursive=True)
            files.extend(found)
            if len(found) > 0:
                logger.info(f"Pattern '{pattern}' found {len(found)} files")

        parquet_files = [Path(f) for f in files]

        if unit_filter:
            # Filter by unit name in the path
            parquet_files = [f for f in parquet_files if unit_filter in str(f)]

        logger.info(f"Selected {len(parquet_files)} parquet files for processing")
        return parquet_files

    def validate_dataframe(self, df: pd.DataFrame, file_path: Path) -> Dict:
        """Validate dataframe for common issues that cause NoneType errors"""
        issues = {
            'file': str(file_path),
            'shape': df.shape,
            'none_values': {},
            'null_counts': {},
            'dtype_issues': {},
            'critical_issues': []
        }

        # Check for None values (different from NaN)
        for col in df.columns:
            none_count = (df[col] == None).sum() if hasattr(df[col], '__eq__') else 0
            if none_count > 0:
                issues['none_values'][col] = none_count
                issues['critical_issues'].append(f"Column '{col}' has {none_count} None values")

        # Check for null/NaN counts
        for col in df.columns:
            null_count = df[col].isnull().sum()
            if null_count > 0:
                issues['null_counts'][col] = null_count

        # Check for object dtypes that should be numeric (potential string/None mix)
        for col in df.columns:
            if df[col].dtype == 'object' and col not in ['time', 'timestamp']:
                # Try to convert to numeric
                try:
                    pd.to_numeric(df[col], errors='raise')
                except (ValueError, TypeError):
                    issues['dtype_issues'][col] = str(df[col].dtype)
                    issues['critical_issues'].append(f"Column '{col}' has mixed types (object dtype)")

        # Check for empty dataframes
        if df.empty:
            issues['critical_issues'].append("DataFrame is empty")

        return issues

    def repair_dataframe(self, df: pd.DataFrame, issues: Dict) -> pd.DataFrame:
        """Repair dataframe issues that cause NoneType errors"""
        df_repaired = df.copy()
        repairs_made = []

        # Fix None values
        for col, count in issues['none_values'].items():
            if count > 0:
                if df_repaired[col].dtype in ['float64', 'int64']:
                    # Replace None with NaN for numeric columns
                    df_repaired[col] = df_repaired[col].where(df_repaired[col] != None, np.nan)
                    repairs_made.append(f"Replaced {count} None values with NaN in '{col}'")
                else:
                    # Replace None with empty string for text columns
                    df_repaired[col] = df_repaired[col].where(df_repaired[col] != None, "")
                    repairs_made.append(f"Replaced {count} None values with empty string in '{col}'")

        # Fix object dtype columns that should be numeric
        for col, dtype in issues['dtype_issues'].items():
            try:
                # Attempt to convert to numeric, coercing errors to NaN
                df_repaired[col] = pd.to_numeric(df_repaired[col], errors='coerce')
                repairs_made.append(f"Converted '{col}' from object to numeric")
            except Exception as e:
                logger.warning(f"Could not repair dtype for column '{col}': {e}")

        # Fill remaining NaN values with appropriate defaults
        for col in df_repaired.columns:
            if df_repaired[col].isnull().any():
                if df_repaired[col].dtype in ['float64', 'int64']:
                    # Use forward fill then backward fill for time series data
                    df_repaired[col] = df_repaired[col].fillna(method='ffill').fillna(method='bfill')
                    # If still NaN, use 0
                    df_repaired[col] = df_repaired[col].fillna(0)
                elif df_repaired[col].dtype == 'bool':
                    df_repaired[col] = df_repaired[col].fillna(False)
                else:
                    df_repaired[col] = df_repaired[col].fillna("")

        if repairs_made:
            logger.info(f"Repairs made: {'; '.join(repairs_made)}")

        return df_repaired

    def backup_file(self, file_path: Path) -> Path:
        """Create backup of original file"""
        if not self.backup_enabled:
            return None

        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        backup_path = self.backup_dir / f"{file_path.stem}_{timestamp}.parquet"

        try:
            import shutil
            shutil.copy2(file_path, backup_path)
            logger.info(f"Backup created: {backup_path}")
            return backup_path
        except Exception as e:
            logger.error(f"Failed to create backup: {e}")
            return None

    def scan_file(self, file_path: Path) -> Dict:
        """Scan a single parquet file for issues"""
        try:
            df = pd.read_parquet(file_path)
            issues = self.validate_dataframe(df, file_path)
            return issues
        except Exception as e:
            logger.error(f"Failed to scan {file_path}: {e}")
            return {
                'file': str(file_path),
                'error': str(e),
                'critical_issues': [f"Failed to read file: {e}"]
            }

    def repair_file(self, file_path: Path, dry_run: bool = False) -> bool:
        """Repair a single parquet file"""
        try:
            # Read and validate
            df = pd.read_parquet(file_path)
            issues = self.validate_dataframe(df, file_path)

            if not issues['critical_issues']:
                logger.info(f"No repairs needed for {file_path}")
                return True

            logger.info(f"Repairing {file_path}: {len(issues['critical_issues'])} issues found")

            if dry_run:
                logger.info(f"DRY RUN: Would repair {file_path}")
                return True

            # Create backup
            backup_path = self.backup_file(file_path)

            # Repair dataframe
            df_repaired = self.repair_dataframe(df, issues)

            # Validate repair
            new_issues = self.validate_dataframe(df_repaired, file_path)
            if new_issues['critical_issues']:
                logger.warning(f"Repair incomplete for {file_path}: {new_issues['critical_issues']}")

            # Save repaired file
            df_repaired.to_parquet(file_path, index=False)
            logger.info(f"Successfully repaired {file_path}")

            return True

        except Exception as e:
            logger.error(f"Failed to repair {file_path}: {e}")
            return False

    def scan_all(self, unit_filter: Optional[str] = None) -> Dict:
        """Scan all parquet files for issues"""
        files = self.find_parquet_files(unit_filter)
        results = {
            'total_files': len(files),
            'files_with_issues': 0,
            'total_issues': 0,
            'file_reports': []
        }

        for file_path in files:
            issues = self.scan_file(file_path)
            results['file_reports'].append(issues)

            if issues.get('critical_issues'):
                results['files_with_issues'] += 1
                results['total_issues'] += len(issues['critical_issues'])

        return results

    def repair_all(self, unit_filter: Optional[str] = None, dry_run: bool = False) -> Dict:
        """Repair all parquet files"""
        files = self.find_parquet_files(unit_filter)
        results = {
            'total_files': len(files),
            'repaired_files': 0,
            'failed_repairs': 0,
            'details': []
        }

        for file_path in files:
            try:
                success = self.repair_file(file_path, dry_run)
                if success:
                    results['repaired_files'] += 1
                    results['details'].append(f"OK {file_path}")
                else:
                    results['failed_repairs'] += 1
                    results['details'].append(f"FAIL {file_path}")
            except Exception as e:
                results['failed_repairs'] += 1
                results['details'].append(f"ERROR {file_path}: {e}")

        return results


def print_scan_report(results: Dict):
    """Print formatted scan report"""
    print(f"\n[SCAN] DATA SCAN REPORT")
    print(f"{'='*50}")
    print(f"Total files scanned: {results['total_files']}")
    print(f"Files with issues: {results['files_with_issues']}")
    print(f"Total critical issues: {results['total_issues']}")

    if results['files_with_issues'] > 0:
        print(f"\n[WARNING] FILES WITH CRITICAL ISSUES:")
        for report in results['file_reports']:
            if report.get('critical_issues'):
                print(f"\nFile: {report['file']}")
                for issue in report['critical_issues']:
                    print(f"   - {issue}")

                if report.get('none_values'):
                    print(f"   None values: {report['none_values']}")
                if report.get('null_counts'):
                    print(f"   Null counts: {report['null_counts']}")


def print_repair_report(results: Dict):
    """Print formatted repair report"""
    print(f"\n[REPAIR] DATA REPAIR REPORT")
    print(f"{'='*50}")
    print(f"Total files processed: {results['total_files']}")
    print(f"Successfully repaired: {results['repaired_files']}")
    print(f"Failed repairs: {results['failed_repairs']}")

    if results['details']:
        print(f"\nDetails:")
        for detail in results['details']:
            print(f"   {detail}")


def main():
    parser = argparse.ArgumentParser(description="TurboPredict Data Repair Tool")
    parser.add_argument('--scan-all', action='store_true', help='Scan all parquet files for issues')
    parser.add_argument('--unit', type=str, help='Filter by specific unit (e.g., K-12-01)')
    parser.add_argument('--repair', action='store_true', help='Repair identified issues')
    parser.add_argument('--dry-run', action='store_true', help='Show what would be repaired without making changes')
    parser.add_argument('--validate', action='store_true', help='Validate that repairs were successful')
    parser.add_argument('--no-backup', action='store_true', help='Disable backup creation')

    args = parser.parse_args()

    # Create repair engine
    repair_engine = DataRepairEngine(backup_enabled=not args.no_backup)

    if args.scan_all or args.validate:
        # Scan mode
        print("[SCAN] Scanning parquet files for data issues...")
        results = repair_engine.scan_all(args.unit)
        print_scan_report(results)

        if results['files_with_issues'] > 0:
            print(f"\n[INFO] To repair these issues, run:")
            unit_arg = f" --unit {args.unit}" if args.unit else ""
            print(f"   python scripts/data_repair.py --repair{unit_arg}")
        else:
            print(f"\n[OK] All files are healthy! No repairs needed.")

    elif args.repair:
        # Repair mode
        action = "DRY RUN - Would repair" if args.dry_run else "Repairing"
        print(f"[REPAIR] {action} parquet files...")

        if not args.dry_run and not args.no_backup:
            print("[BACKUP] Backups will be created automatically")

        results = repair_engine.repair_all(args.unit, args.dry_run)
        print_repair_report(results)

        if not args.dry_run and results['repaired_files'] > 0:
            print(f"\n[OK] Repair complete! Run the anomaly scanner again to verify fixes.")

    else:
        # Default: quick scan
        print("[SCAN] Quick scan for data issues (use --scan-all for detailed report)...")
        results = repair_engine.scan_all(args.unit)

        if results['files_with_issues'] > 0:
            print(f"[WARNING] Found {results['total_issues']} issues in {results['files_with_issues']} files")
            print("Run with --scan-all for detailed report")
        else:
            print("[OK] No data issues found")


if __name__ == "__main__":
    main()