#!/usr/bin/env python3
"""
Hourly Refresh Script for TURBOPREDICT X PROTEAN
Maintains 1-hour data freshness standard for industrial process control
"""

import os
import sys
import time
from pathlib import Path
from datetime import datetime, timedelta
import schedule
import logging

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from pi_monitor.parquet_auto_scan import ParquetAutoScanner
from pi_monitor.config import Config

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(project_root / 'logs' / 'hourly_refresh.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


class HourlyRefreshService:
    """Service to maintain 1-hour data freshness"""
    
    def __init__(self):
        self.config = Config()
        self.scanner = ParquetAutoScanner()
        
        # Excel files to try (in priority order)
        self.excel_candidates = [
            project_root / 'excel' / 'PCFS_Automation.xlsx',
            project_root / 'excel' / 'PCFS_Automation1.xlsx',
        ]
        
        # Ensure logs directory exists
        (project_root / 'logs').mkdir(exist_ok=True)
        
    def find_excel_file(self):
        """Find the best Excel file to use for refresh"""
        excel_dir = project_root / 'excel'
        
        if excel_dir.exists():
            # Get all Excel files, sorted by modification time (newest first)
            excel_files = sorted(
                excel_dir.glob('*.xlsx'), 
                key=lambda x: x.stat().st_mtime, 
                reverse=True
            )
            
            # Filter out temp/backup files for primary candidates
            primary_files = [f for f in excel_files 
                           if not any(word in f.name.lower() 
                                    for word in ['backup', 'dummy', 'temp', '~'])]
            
            if primary_files:
                return primary_files[0]
            elif excel_files:
                return excel_files[0]
        
        return None
    
    def perform_refresh(self):
        """Perform hourly refresh to maintain data freshness"""
        logger.info("=" * 60)
        logger.info("HOURLY REFRESH CYCLE STARTING")
        logger.info("=" * 60)
        
        try:
            # Check current freshness
            scan_results = self.scanner.scan_all_units(max_age_hours=1.0)
            
            fresh_count = len(scan_results['fresh_units'])
            stale_count = len(scan_results['stale_units'])
            total_count = fresh_count + stale_count
            
            logger.info(f"Current status: {fresh_count}/{total_count} units fresh")
            
            if stale_count == 0:
                logger.info("All units meet 1-hour freshness standard. No refresh needed.")
                return True
            
            # Find Excel file
            excel_file = self.find_excel_file()
            if not excel_file:
                logger.error("No Excel file found for refresh")
                return False
            
            logger.info(f"Using Excel file: {excel_file.name}")
            logger.info(f"Refreshing {stale_count} stale units: {scan_results['stale_units']}")
            
            # Perform refresh
            refresh_start = datetime.now()
            refresh_results = self.scanner.refresh_stale_units_with_progress(
                xlsx_path=excel_file,
                max_age_hours=1.0
            )
            
            # Log results
            refresh_end = datetime.now()
            duration = (refresh_end - refresh_start).total_seconds()
            
            if refresh_results['success']:
                successful = refresh_results['successful_units']
                failed = refresh_results['failed_units']
                
                logger.info(f"Refresh completed in {duration:.1f}s")
                logger.info(f"Success: {successful} units, Failed: {failed} units")
                
                # Log details for each unit
                for unit_name, result in refresh_results.get('unit_results', {}).items():
                    if result['success']:
                        records = result['records_after']
                        logger.info(f"  ✓ {unit_name}: {records:,} records updated")
                    else:
                        error = result.get('error', 'Unknown error')[:50]
                        logger.warning(f"  ✗ {unit_name}: {error}")
                
                return successful > 0
            else:
                logger.error(f"Refresh failed: {refresh_results.get('error', 'Unknown error')}")
                return False
                
        except Exception as e:
            logger.error(f"Refresh cycle failed: {e}")
            return False
        
        finally:
            logger.info("HOURLY REFRESH CYCLE COMPLETED")
            logger.info("=" * 60)
    
    def run_once(self):
        """Run refresh once and exit"""
        logger.info("Running single refresh cycle...")
        success = self.perform_refresh()
        return success
    
    def run_service(self):
        """Run as a continuous service"""
        logger.info("Starting Hourly Refresh Service...")
        logger.info("Data freshness standard: 1 hour")
        
        # Schedule hourly refreshes
        schedule.every().hour.at(":00").do(self.perform_refresh)
        
        # Also run immediately
        logger.info("Performing initial refresh...")
        self.perform_refresh()
        
        logger.info("Service started. Running hourly refreshes...")
        
        try:
            while True:
                schedule.run_pending()
                time.sleep(60)  # Check every minute
        except KeyboardInterrupt:
            logger.info("Service stopped by user")
        except Exception as e:
            logger.error(f"Service error: {e}")


def main():
    """Main entry point"""
    service = HourlyRefreshService()
    
    if len(sys.argv) > 1 and sys.argv[1] == '--once':
        # Run once and exit
        success = service.run_once()
        sys.exit(0 if success else 1)
    else:
        # Run as service
        service.run_service()


if __name__ == "__main__":
    main()