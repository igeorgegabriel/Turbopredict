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
import logging
import schedule

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from scripts.smart_incremental_refresh import run_smart_incremental_refresh
from turbopredict import TurbopredictSystem

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
        self.system: TurbopredictSystem | None = None

        # Ensure logs directory exists
        (project_root / 'logs').mkdir(exist_ok=True)
    
    def cleanup_temp_files(self):
        """Clean up old temporary files"""
        try:
            # Import cleanup function
            sys.path.append(str(Path(__file__).parent))
            from cleanup_dummy_files import cleanup_all_excel_directories

            logger.info("Running automatic cleanup of temporary files...")

            # Run cleanup for files older than 24 hours
            stats = cleanup_all_excel_directories(max_age_hours=24, dry_run=False)

            if stats['files_cleaned'] > 0:
                logger.info(f"Cleaned up {stats['files_cleaned']} temp files, "
                           f"reclaimed {stats['space_reclaimed_mb']:.1f}MB")
            else:
                logger.debug("No old temporary files found to clean")

            return stats

        except Exception as e:
            logger.warning(f"Cleanup cycle failed but continuing: {e}")
            return {"success": False, "error": str(e)}

    def perform_analysis(self, units_to_analyze, anomaly_threshold=50, days_limit=30):
        """Perform analysis on specified units using the updated option [2] workflow."""
        if not units_to_analyze:
            logger.info("No units to analyze after refresh.")
            return False

        if self.system is None:
            try:
                self.system = TurbopredictSystem()
            except Exception as exc:
                logger.error("Failed to initialize TurboPredict system: %s", exc)
                return False

        system = self.system

        if not getattr(system, "data_available", False):
            logger.warning("TurboPredict data systems unavailable - skipping analysis phase.")
            return False

        speed_aware_enabled = getattr(system, "speed_aware_available", False)
        analyses: dict[str, dict] = {}

        print("\n" + "=" * 80)
        print("[PHASE 2/2] OPTION [2] UNIT ANALYSIS")
        print("=" * 80)

        for idx, unit_name in enumerate(units_to_analyze, 1):
            logger.info("Analyzing unit %s (%d/%d)", unit_name, idx, len(units_to_analyze))
            try:
                if hasattr(system, "_analyze_unit_with_speed_awareness"):
                    analysis = system._analyze_unit_with_speed_awareness(unit_name, speed_aware_enabled)
                else:
                    analysis = system.scanner.analyze_unit_data(unit_name, run_anomaly_detection=True)
                analyses[unit_name] = analysis
            except Exception as exc:
                logger.error("Analysis failed for %s: %s", unit_name, exc, exc_info=True)

        if not analyses:
            logger.warning("No analysis results were produced.")
            return False

        try:
            system._display_all_units_analysis(analyses)
        except Exception as exc:
            logger.warning("Rich analysis display failed (%s); falling back to text output.", exc)
            fallback = getattr(system, "_display_all_units_analysis_fallback", None)
            if callable(fallback):
                fallback(analyses)

        return True

    def perform_refresh(self):
        """Perform hourly refresh using the updated option [1] logic."""
        logger.info("=" * 60)
        logger.info("HOURLY REFRESH CYCLE STARTING")
        logger.info("=" * 60)

        # Run cleanup every 6 hours (at 3, 9, 15, 21 o'clock)
        current_hour = datetime.now().hour
        if current_hour % 6 == 3:
            logger.info("Running scheduled cleanup of temporary files...")
            self.cleanup_temp_files()

        try:
            refresh_start = datetime.now()
            refresh_result = run_smart_incremental_refresh(max_age_hours=1.0)
            refresh_end = datetime.now()

            duration = (refresh_end - refresh_start).total_seconds()
            refreshed_units = refresh_result.get("refreshed_units", [])
            failed_units = refresh_result.get("failed_units", [])
            results = refresh_result.get("results", {})

            if not refresh_result.get("stale_units"):
                logger.info("All units met the freshness threshold. No refresh required.")
                return True

            total_rows_added = sum(r.get("rows_added", 0) for r in results.values())
            logger.info(
                "Smart incremental refresh completed in %.1fs: %d succeeded, %d failed, %d rows added.",
                duration,
                len(refreshed_units),
                len(failed_units),
                total_rows_added,
            )

            for unit_key in refreshed_units:
                unit_result = results.get(unit_key, {})
                logger.info(
                    "  [OK] %s → rows_added=%s active_tags=%s/%s",
                    unit_key,
                    f"{unit_result.get('rows_added', 0):,}",
                    unit_result.get("active_tags", 0),
                    unit_result.get("total_tags", 0),
                )

            for unit_key in failed_units:
                logger.warning("  [X] %s failed during refresh", unit_key)

            analysis_success = True
            if refreshed_units:
                logger.info("Launching option [2] analysis for refreshed units...")
                analysis_success = self.perform_analysis(refreshed_units)
                logger.info("Option [2] analysis %s.", "succeeded" if analysis_success else "encountered issues")
            else:
                logger.info("No units refreshed; skipping analysis phase.")

            return analysis_success and not failed_units

        except Exception as e:
            logger.error("Refresh cycle failed: %s", e, exc_info=True)
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
        logger.info(f"Next refresh scheduled at: {schedule.next_run()}")

        cycle_count = 0
        try:
            while True:
                schedule.run_pending()
                cycle_count += 1

                # Log heartbeat every 10 minutes
                if cycle_count % 10 == 0:
                    logger.debug(f"Service alive - Next run: {schedule.next_run()}")

                time.sleep(60)  # Check every minute
        except KeyboardInterrupt:
            logger.info("Service stopped by user")
        except Exception as e:
            logger.error(f"Service error: {e}", exc_info=True)
            raise  # Re-raise to prevent silent failure


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
