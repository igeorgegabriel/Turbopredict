from __future__ import annotations

from pathlib import Path
import time
import logging
import threading

try:
    import xlwings as xw
except Exception as _exc:  # pragma: no cover - optional at scaffold time
    xw = None  # type: ignore

# Import the new file manager
from .excel_file_manager import create_dummy_and_refresh, restore_after_refresh

logger = logging.getLogger(__name__)

# Global PI server access coordination
_pi_server_lock = threading.Lock()
_last_refresh_time = {}
_minimum_interval_seconds = 5  # Minimum time between PI refreshes (reduced for faster throughput)


def refresh_with_xlwings(xlsx: Path, settle_seconds: int = 5) -> None:
    """Headless Excel refresh using xlwings with automatic save handling.

    Steps:
    - Create dummy file to avoid save prompts
    - Open workbook
    - RefreshAll (connections/queries)
    - CalculateFull
    - Save & close (saves to original filename without prompting)
    - Restore file structure
    """
    if xw is None:
        raise RuntimeError("xlwings is not available. Install dependencies first.")

    xlsx = Path(xlsx)
    print(f"[excel] Refreshing via Excel with auto-save handling: {xlsx}")
    logger.info(f"Starting Excel refresh with dummy file strategy: {xlsx}")

    # Create dummy file to avoid save prompts
    original_path, dummy_path = create_dummy_and_refresh(xlsx, settle_seconds=1)

    t0 = time.time()
    app = xw.App(visible=False, add_book=False)
    try:
        # CRITICAL: Disable all Excel alerts and prompts
        app.display_alerts = False
        app.screen_updating = False
        app.interactive = False  # Prevent user interaction

        # Set Excel to not ask about saving changes
        app.api.Application.DisplayAlerts = False
        app.api.Application.EnableEvents = False

        # Check if we need to use dummy file for opening
        file_to_open = dummy_path if dummy_path.exists() else original_path
        logger.info(f"Opening Excel file: {file_to_open}")

        # 1) Open workbook (from dummy if original was renamed)
        wb = app.books.open(str(file_to_open))

        # Disable save prompts on workbook level
        wb.api.Application.DisplayAlerts = False

        # 2) Manual calc while we prep (faster)
        xlCalculationManual = -4135
        xlCalculationAutomatic = -4105
        app.api.Calculation = xlCalculationManual

        # 3) Refresh connections (if any) + recalc
        print("[excel] Refreshing PI DataLink connections...")
        try:
            wb.api.RefreshAll()  # triggers DataLink/queries
            logger.info("PI DataLink refresh completed successfully")
        except Exception as e:
            logger.warning(f"RefreshAll encountered issue: {e}")
            pass

        app.api.CalculateFull()  # lighter than CalculateFullRebuild
        # Robust wait until async queries finish and Excel reports done
        try:
            app.api.CalculateUntilAsyncQueriesDone()
        except Exception:
            pass
        # Poll CalculationState (XlCalculationState: 0=Done, 1=Calculating, 2=Pending)
        import time as _t
        max_wait = max(6.0, float(settle_seconds) * 4.0)
        t0 = _t.time()
        done_cycles = 0
        timed_out = True
        while (_t.time() - t0) < max_wait:
            try:
                state = int(app.api.CalculationState)
            except Exception:
                state = None
            if state == 0:
                done_cycles += 1
                if done_cycles >= 3:
                    timed_out = False
                    break
            else:
                done_cycles = 0
            _t.sleep(0.5)
        if timed_out:
            print(f"[excel] Warning: Refresh wait timed out after {max_wait:.1f}s; continuing...")

        # 4) Back to automatic, save to original path
        app.api.Calculation = xlCalculationAutomatic

        # Save to original filename - this avoids the save prompt
        # because Excel thinks it's creating a new file
        # Use absolute path to avoid Excel path resolution issues
        absolute_original = original_path.resolve()
        print(f"[excel] Saving refreshed data to: {absolute_original}")

        # Strategy: Just save in place, don't use SaveAs at all
        try:
            # The dummy file strategy means Excel will save to the original filename
            # without prompting because it thinks it's a new file
            print(f"[excel] Saving refreshed data (no SaveAs dialog)...")
            wb.save()  # This saves to the current filename (which is the dummy file)

            # Now copy the saved dummy file back to original location
            import shutil
            if dummy_path.exists():
                shutil.copy2(str(dummy_path), str(absolute_original))
                print(f"[excel] Data copied to original location: {absolute_original}")
            else:
                logger.warning("Dummy file not found after save - using SaveAs fallback")
                wb.api.SaveAs(str(absolute_original))

        except Exception as save_error:
            logger.error(f"Save operation failed: {save_error}")
            # Final fallback
            try:
                wb.api.SaveAs(str(absolute_original))
            except Exception as saveas_error:
                logger.error(f"SaveAs fallback also failed: {saveas_error}")
                raise

        wb.close()

    finally:
        try:
            # Restore Excel settings before quitting
            app.display_alerts = True
            app.screen_updating = True
            app.interactive = True
        except:
            pass
        app.quit()

    # Restore file structure and cleanup
    restore_success = restore_after_refresh(original_path, dummy_path, keep_dummy=True)

    elapsed = time.time() - t0
    print(f"[excel] Refresh done in {elapsed:.1f}s")
    logger.info(f"Excel refresh completed successfully in {elapsed:.1f}s")

    if restore_success:
        logger.info("File structure restored successfully")
    else:
        logger.warning("File structure restoration had issues - check manually")


def refresh_with_working_copy(xlsx: Path, settle_seconds: int = 5) -> None:
    """Alternative Excel refresh using working copy strategy.
    
    This approach creates a working copy, refreshes it, then copies back.
    Use this if the dummy file strategy encounters issues.
    """
    if xw is None:
        raise RuntimeError("xlwings is not available. Install dependencies first.")

    from .excel_file_manager import ExcelFileManager
    
    xlsx = Path(xlsx)
    print(f"[excel] Refreshing via working copy strategy: {xlsx}")
    logger.info(f"Starting Excel refresh with working copy: {xlsx}")
    
    manager = ExcelFileManager(xlsx)
    working_copy = None
    
    try:
        # Create working copy
        working_copy = manager.create_working_copy()
        logger.info(f"Created working copy: {working_copy}")
        
        t0 = time.time()
        app = xw.App(visible=False, add_book=False)
        try:
            # CRITICAL: Disable all Excel alerts and prompts
            app.display_alerts = False
            app.screen_updating = False
            app.interactive = False  # Prevent user interaction

            # Set Excel to not ask about saving changes
            app.api.Application.DisplayAlerts = False
            app.api.Application.EnableEvents = False

            # Open working copy
            wb = app.books.open(str(working_copy))

            # Disable save prompts on workbook level
            wb.api.Application.DisplayAlerts = False

            # Manual calc mode
            xlCalculationManual = -4135
            xlCalculationAutomatic = -4105
            app.api.Calculation = xlCalculationManual

            # Refresh connections
            print("[excel] Refreshing PI DataLink connections...")
            try:
                wb.api.RefreshAll()
                logger.info("PI DataLink refresh completed successfully")
            except Exception as e:
                logger.warning(f"RefreshAll encountered issue: {e}")

            app.api.CalculateFull()
            # Robust wait until async queries finish and Excel reports done
            try:
                app.api.CalculateUntilAsyncQueriesDone()
            except Exception:
                pass
            import time as _t
            max_wait = max(6.0, float(settle_seconds) * 4.0)
            t0 = _t.time()
            done_cycles = 0
            timed_out = True
            while (_t.time() - t0) < max_wait:
                try:
                    state = int(app.api.CalculationState)
                except Exception:
                    state = None
                if state == 0:
                    done_cycles += 1
                    if done_cycles >= 3:
                        timed_out = False
                        break
                else:
                    done_cycles = 0
                _t.sleep(0.5)
            if timed_out:
                print(f"[excel] Warning: Working-copy refresh wait timed out after {max_wait:.1f}s; continuing...")

            # Save and close working copy
            app.api.Calculation = xlCalculationAutomatic
            wb.save()
            wb.close()

        finally:
            try:
                # Restore Excel settings before quitting
                app.display_alerts = True
                app.screen_updating = True
                app.interactive = True
            except:
                pass
            app.quit()
        
        # Copy refreshed data back to original
        import shutil
        absolute_xlsx = xlsx.resolve()
        shutil.copy2(str(working_copy), str(absolute_xlsx))
        logger.info(f"Copied refreshed data back to: {xlsx}")
        
        elapsed = time.time() - t0
        print(f"[excel] Working copy refresh done in {elapsed:.1f}s")
        logger.info(f"Working copy refresh completed successfully in {elapsed:.1f}s")
        
    finally:
        # Clean up working copy
        if working_copy and working_copy.exists():
            try:
                working_copy.unlink()
                logger.info(f"Cleaned up working copy: {working_copy}")
            except Exception as e:
                logger.warning(f"Failed to clean up working copy: {e}")


def refresh_excel_safe(xlsx: Path, settle_seconds: int = 5, use_working_copy: bool = True, auto_cleanup: bool = True) -> None:
    """Safe Excel refresh with fallback strategies and improved error handling.

    Args:
        xlsx: Path to Excel file
        settle_seconds: Seconds to wait after operations
        use_working_copy: If True, use working copy strategy instead of dummy file. Default True to avoid file locks/popups.
        auto_cleanup: If True, automatically clean up old temporary files
    """
    xlsx = Path(xlsx)

    # Verify file exists before attempting refresh
    if not xlsx.exists():
        raise FileNotFoundError(f"Excel file not found: {xlsx}")

    # Check if file is currently being accessed by another process
    import psutil
    import time

    def is_file_locked(filepath):
        """Check if file is locked by another process"""
        try:
            for proc in psutil.process_iter(['pid', 'name', 'open_files']):
                try:
                    if proc.info['open_files']:
                        for file_info in proc.info['open_files']:
                            if str(filepath).lower() in file_info.path.lower():
                                return True, proc.info['name'], proc.info['pid']
                except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess):
                    continue
            return False, None, None
        except Exception:
            return False, None, None

    # Wait for file to be unlocked (up to 30 seconds)
    max_wait = 30
    wait_start = time.time()
    while time.time() - wait_start < max_wait:
        locked, process_name, pid = is_file_locked(xlsx)
        if not locked:
            break
        print(f"[excel] File locked by {process_name} (PID {pid}), waiting...")
        time.sleep(2)
    else:
        logger.warning(f"File still locked after {max_wait}s, proceeding anyway...")

    # Automatic cleanup of old temp files before refresh
    if auto_cleanup:
        try:
            from .excel_file_manager import ExcelFileManager
            manager = ExcelFileManager(xlsx)
            cleaned_count = manager.cleanup_temp_files(max_age_hours=24)  # Clean files older than 24 hours
            if cleaned_count > 0:
                logger.info(f"Cleaned up {cleaned_count} old temporary files")
        except Exception as cleanup_error:
            logger.warning(f"Cleanup failed but continuing with refresh: {cleanup_error}")

    # Allow env override: EXCEL_REFRESH_STRATEGY=working|dummy
    import os as _os
    strat = _os.getenv('EXCEL_REFRESH_STRATEGY', '').strip().lower()
    if strat in ('working', 'work', 'copy'):
        use_working_copy = True
    elif strat in ('dummy', 'rename'):
        use_working_copy = False

    try:
        if use_working_copy:
            refresh_with_working_copy(xlsx, settle_seconds)
        else:
            refresh_with_xlwings(xlsx, settle_seconds)
    except Exception as e:
        logger.error(f"Primary refresh strategy failed: {e}")

        # Wait a bit before fallback
        time.sleep(3)

        # Try fallback strategy
        try:
            fallback_strategy = refresh_with_working_copy if not use_working_copy else refresh_with_xlwings
            logger.info("Attempting fallback refresh strategy...")
            fallback_strategy(xlsx, settle_seconds)
        except Exception as fallback_error:
            logger.error(f"Fallback refresh strategy also failed: {fallback_error}")

            # Final attempt: skip refresh but ensure file integrity
            logger.warning("Both refresh strategies failed. Ensuring file integrity...")
            if not xlsx.exists():
                # Try to restore from most recent backup/dummy file
                from .excel_file_manager import ExcelFileManager
                manager = ExcelFileManager(xlsx)

                # Look for recent dummy files
                dummy_pattern = f"{xlsx.stem}_dummy_*.xlsx"
                dummy_files = list(xlsx.parent.glob(dummy_pattern))
                if dummy_files:
                    dummy_files.sort(key=lambda x: x.stat().st_mtime, reverse=True)
                    latest_dummy = dummy_files[0]
                    logger.info(f"Restoring from latest dummy: {latest_dummy}")
                    manager.restore_from_dummy(latest_dummy, keep_dummy=True)
                    return

            raise RuntimeError(f"Both refresh strategies failed. Primary: {e}, Fallback: {fallback_error}")


def refresh_excel_with_pi_coordination(xlsx: Path, settle_seconds: int = 5, use_working_copy: bool = True, auto_cleanup: bool = True) -> None:
    """Safe Excel refresh with PI server coordination to prevent connection conflicts.

    This function coordinates access to the PI server to prevent multiple units
    from trying to refresh simultaneously, which causes alternating fetch failures.

    Args:
        xlsx: Path to Excel file
        settle_seconds: Seconds to wait after operations
        use_working_copy: If True, use working copy strategy
        auto_cleanup: If True, automatically clean up old temporary files
    """
    xlsx = Path(xlsx)

    # Determine unit from excel path for coordination
    unit_key = "UNKNOWN"
    xlsx_str = str(xlsx).upper()
    if "PCFS" in xlsx_str:
        unit_key = "PCFS"
    elif "PCMSB" in xlsx_str:
        unit_key = "PCMSB"
    elif "ABF" in xlsx_str:
        unit_key = "ABFSB"
    elif "MLNG" in xlsx_str:
        unit_key = "MLNG"

    logger.info(f"[{unit_key}] Requesting PI server access...")

    with _pi_server_lock:
        # Check if we need to wait for minimum interval
        now = time.time()
        if _last_refresh_time:
            last_time = max(_last_refresh_time.values())
            time_since_last = now - last_time

            if time_since_last < _minimum_interval_seconds:
                wait_time = _minimum_interval_seconds - time_since_last
                logger.info(f"[{unit_key}] Waiting {wait_time:.1f}s for PI server cooldown...")
                print(f"[{unit_key}] PI server coordination: waiting {wait_time:.1f}s...")
                time.sleep(wait_time)

        # Proceed with coordinated refresh
        logger.info(f"[{unit_key}] Acquired PI server access, starting refresh...")
        print(f"[{unit_key}] Starting coordinated PI refresh...")

        try:
            # Call original refresh function
            refresh_excel_safe(xlsx, settle_seconds, use_working_copy, auto_cleanup)
            _last_refresh_time[unit_key] = time.time()
            logger.info(f"[{unit_key}] PI refresh completed successfully")
            print(f"[{unit_key}] PI refresh completed successfully!")

        except Exception as e:
            logger.error(f"[{unit_key}] PI refresh failed: {e}")
            print(f"[{unit_key}] PI refresh failed: {e}")
            # Still update time to prevent rapid retries
            _last_refresh_time[unit_key] = time.time()
            raise
