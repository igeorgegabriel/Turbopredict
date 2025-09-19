"""
Ultra-Fast Excel Optimization for Critical Equipment Monitoring
Maximum performance tweaks for TURBOPREDICT X PROTEAN
"""

from __future__ import annotations

from pathlib import Path
import time
import logging
from typing import Dict, Any, List
import threading
from concurrent.futures import ThreadPoolExecutor
import os

logger = logging.getLogger(__name__)

try:
    import xlwings as xw
except ImportError:
    xw = None

from .excel_file_manager import ExcelFileManager


class UltraFastExcelProcessor:
    """Ultra-optimized Excel processor for critical equipment monitoring"""
    
    def __init__(self):
        """Initialize ultra-fast Excel processor"""
        self.manager = ExcelFileManager if ExcelFileManager else None
        self.performance_metrics = {}
        
    def optimize_excel_app(self, app) -> None:
        """Apply all possible Excel optimizations"""
        try:
            # Disable all visual elements for maximum speed
            app.display_alerts = False
            app.screen_updating = False
            app.enable_events = False
            app.interactive = False
            app.visible = False
            
            # Advanced performance settings
            if hasattr(app.api, 'EnableAnimations'):
                app.api.EnableAnimations = False
            if hasattr(app.api, 'EnableSound'):
                app.api.EnableSound = False
            if hasattr(app.api, 'EnableAutoComplete'):
                app.api.EnableAutoComplete = False
            
            # Set calculation to manual for speed
            xlCalculationManual = -4135
            app.api.Calculation = xlCalculationManual
            
            logger.info("Applied ultra-fast Excel optimizations")
            
        except Exception as e:
            logger.warning(f"Could not apply all optimizations: {e}")
    
    def parallel_unit_refresh(self, xlsx_paths: List[Path], units: List[str], 
                            max_workers: int = 2) -> Dict[str, Any]:
        """Process multiple units in parallel for maximum speed"""
        
        if not xlsx_paths or not units:
            return {"success": False, "error": "No units to process"}
        
        start_time = time.time()
        results = {}
        
        # Split units into batches for parallel processing
        unit_batches = []
        batch_size = max(1, len(units) // max_workers)
        
        for i in range(0, len(units), batch_size):
            batch = units[i:i + batch_size]
            unit_batches.append(batch)
        
        logger.info(f"Processing {len(units)} units in {len(unit_batches)} parallel batches")
        
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            # Submit parallel Excel refresh tasks
            future_to_batch = {}
            for i, batch in enumerate(unit_batches):
                xlsx_path = xlsx_paths[min(i, len(xlsx_paths) - 1)]  # Use available Excel files
                future = executor.submit(self._refresh_unit_batch, xlsx_path, batch, i)
                future_to_batch[future] = batch
            
            # Collect results
            for future in future_to_batch:
                batch = future_to_batch[future]
                try:
                    batch_result = future.result(timeout=600)  # 10 minute timeout per batch
                    results[f"batch_{hash(tuple(batch))}"] = batch_result
                except Exception as e:
                    logger.error(f"Batch {batch} failed: {e}")
                    results[f"batch_{hash(tuple(batch))}"] = {"success": False, "error": str(e)}
        
        total_time = time.time() - start_time
        
        return {
            "success": True,
            "total_time": total_time,
            "units_processed": len(units),
            "batches": len(unit_batches),
            "results": results,
            "performance_metrics": self.performance_metrics
        }
    
    def _refresh_unit_batch(self, xlsx_path: Path, units: List[str], batch_id: int) -> Dict[str, Any]:
        """Refresh a batch of units in a single Excel instance"""
        
        batch_start = time.time()
        
        try:
            # Create unique Excel app for this batch
            app = xw.App(visible=False, add_book=False)
            app.display_alerts = False
            
            # Apply ultra-fast optimizations
            self.optimize_excel_app(app)
            
            # Open workbook
            wb = app.books.open(str(xlsx_path))
            
            # Process each unit in this batch
            unit_results = {}
            for unit in units:
                unit_start = time.time()
                
                # Focus on specific unit data (if workbook has unit-specific sheets)
                try:
                    # Refresh only the data for this unit
                    self._refresh_unit_data(wb, unit)
                    unit_time = time.time() - unit_start
                    unit_results[unit] = {"success": True, "time": unit_time}
                    
                except Exception as e:
                    unit_time = time.time() - unit_start
                    unit_results[unit] = {"success": False, "error": str(e), "time": unit_time}
            
            # Save workbook
            wb.save()
            wb.close()
            app.quit()
            
            batch_time = time.time() - batch_start
            
            return {
                "success": True,
                "batch_id": batch_id,
                "units": unit_results,
                "batch_time": batch_time,
                "units_count": len(units)
            }
            
        except Exception as e:
            batch_time = time.time() - batch_start
            logger.error(f"Batch {batch_id} failed after {batch_time:.1f}s: {e}")
            return {
                "success": False,
                "batch_id": batch_id,
                "error": str(e),
                "batch_time": batch_time
            }
    
    def _refresh_unit_data(self, workbook, unit: str) -> None:
        """Refresh data for a specific unit with targeted approach"""
        
        try:
            # Method 1: Try unit-specific worksheet
            if unit in [ws.name for ws in workbook.sheets]:
                sheet = workbook.sheets[unit]
                sheet.api.RefreshAll()
                logger.debug(f"Refreshed unit-specific sheet: {unit}")
                return
            
            # Method 2: Refresh all if no unit-specific sheets
            workbook.api.RefreshAll()
            logger.debug(f"Performed full refresh for unit: {unit}")
            
        except Exception as e:
            logger.warning(f"Unit-specific refresh failed for {unit}: {e}")
            # Fallback to full refresh
            workbook.api.RefreshAll()
    
    def ultra_fast_single_refresh(self, xlsx_path: Path, target_units: List[str] = None) -> Dict[str, Any]:
        """Ultra-optimized single Excel refresh with all speed tweaks"""
        
        start_time = time.time()
        
        # Create dummy file for save optimization
        if self.manager:
            original_path, dummy_path = self.manager.create_dummy_and_refresh(xlsx_path, settle_seconds=0)
        else:
            original_path, dummy_path = xlsx_path, None
        
        try:
            # Create highly optimized Excel app
            app = xw.App(visible=False, add_book=False)
            
            # Apply all optimizations
            self.optimize_excel_app(app)
            
            # Open file (dummy if available)
            file_to_open = dummy_path if dummy_path and dummy_path.exists() else original_path
            wb = app.books.open(str(file_to_open))
            
            refresh_start = time.time()
            
            if target_units:
                # Targeted refresh for specific units
                for unit in target_units:
                    self._refresh_unit_data(wb, unit)
            else:
                # Full refresh with optimizations
                wb.api.RefreshAll()
            
            refresh_time = time.time() - refresh_start
            
            # Fast calculation
            app.api.CalculateFull()
            
            # Save optimizations
            save_start = time.time()
            if dummy_path:
                wb.api.SaveAs(str(original_path))
            else:
                wb.save()
            save_time = time.time() - save_start
            
            wb.close()
            app.quit()
            
            total_time = time.time() - start_time
            
            # Cleanup dummy file
            if self.manager and dummy_path:
                self.manager.restore_after_refresh(original_path, dummy_path, keep_dummy=False)
            
            return {
                "success": True,
                "total_time": total_time,
                "refresh_time": refresh_time,
                "save_time": save_time,
                "target_units": target_units,
                "optimizations_applied": True
            }
            
        except Exception as e:
            total_time = time.time() - start_time
            logger.error(f"Ultra-fast refresh failed after {total_time:.1f}s: {e}")
            
            # Cleanup on error
            if self.manager and dummy_path:
                try:
                    self.manager.restore_from_dummy(dummy_path, keep_dummy=False)
                except:
                    pass
            
            return {
                "success": False,
                "error": str(e),
                "total_time": total_time
            }
    
    def get_performance_report(self) -> Dict[str, Any]:
        """Get detailed performance metrics"""
        return {
            "processor": "UltraFastExcelProcessor",
            "optimizations": [
                "Disabled Excel animations and sounds",
                "Manual calculation mode",
                "Screen updating disabled",
                "Events disabled",
                "Parallel processing capable",
                "Targeted unit refresh",
                "Dummy file save optimization"
            ],
            "metrics": self.performance_metrics,
            "parallel_capable": True,
            "max_recommended_workers": 4
        }


# High-level interface function
def ultra_fast_excel_refresh(xlsx_path: Path, units: List[str] = None, 
                           parallel: bool = True, max_workers: int = 2) -> Dict[str, Any]:
    """
    Ultra-fast Excel refresh optimized for critical equipment monitoring
    
    Args:
        xlsx_path: Path to Excel file
        units: List of specific units to refresh (optional)
        parallel: Use parallel processing if multiple units
        max_workers: Number of parallel Excel instances
        
    Returns:
        Performance results and timing metrics
    """
    
    processor = UltraFastExcelProcessor()
    
    if parallel and units and len(units) > 1:
        # Use parallel processing for multiple units
        return processor.parallel_unit_refresh([xlsx_path], units, max_workers)
    else:
        # Use single optimized refresh
        return processor.ultra_fast_single_refresh(xlsx_path, units)