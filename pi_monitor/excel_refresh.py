from __future__ import annotations

from pathlib import Path
import time
import logging

try:
    import xlwings as xw
except Exception as _exc:  # pragma: no cover - optional at scaffold time
    xw = None  # type: ignore

# Import the new file manager
from .excel_file_manager import create_dummy_and_refresh, restore_after_refresh

logger = logging.getLogger(__name__)


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
        app.display_alerts = False
        app.screen_updating = False
        
        # Check if we need to use dummy file for opening
        file_to_open = dummy_path if dummy_path.exists() else original_path
        logger.info(f"Opening Excel file: {file_to_open}")

        # 1) Open workbook (from dummy if original was renamed)
        wb = app.books.open(str(file_to_open))

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
        time.sleep(settle_seconds)

        # 4) Back to automatic, save to original path
        app.api.Calculation = xlCalculationAutomatic
        
        # Save to original filename - this avoids the save prompt
        # because Excel thinks it's creating a new file
        print(f"[excel] Saving refreshed data to: {original_path}")
        wb.api.SaveAs(str(original_path))
        wb.close()
        
    finally:
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
            app.display_alerts = False
            app.screen_updating = False

            # Open working copy
            wb = app.books.open(str(working_copy))

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
            time.sleep(settle_seconds)

            # Save and close working copy
            app.api.Calculation = xlCalculationAutomatic
            wb.save()
            wb.close()
            
        finally:
            app.quit()
        
        # Copy refreshed data back to original
        import shutil
        shutil.copy2(str(working_copy), str(xlsx))
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


def refresh_excel_safe(xlsx: Path, settle_seconds: int = 5, use_working_copy: bool = False) -> None:
    """Safe Excel refresh with fallback strategies.
    
    Args:
        xlsx: Path to Excel file
        settle_seconds: Seconds to wait after operations
        use_working_copy: If True, use working copy strategy instead of dummy file
    """
    try:
        if use_working_copy:
            refresh_with_working_copy(xlsx, settle_seconds)
        else:
            refresh_with_xlwings(xlsx, settle_seconds)
    except Exception as e:
        logger.error(f"Primary refresh strategy failed: {e}")
        
        # Try fallback strategy
        try:
            fallback_strategy = refresh_with_working_copy if not use_working_copy else refresh_with_xlwings
            logger.info("Attempting fallback refresh strategy...")
            fallback_strategy(xlsx, settle_seconds)
        except Exception as fallback_error:
            logger.error(f"Fallback refresh strategy also failed: {fallback_error}")
            raise RuntimeError(f"Both refresh strategies failed. Primary: {e}, Fallback: {fallback_error}")

