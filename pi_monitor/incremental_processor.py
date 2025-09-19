"""
Incremental Processing System for TURBOPREDICT X PROTEAN
Process units individually with real-time status updates
"""

from __future__ import annotations

import pandas as pd
from pathlib import Path
from typing import Dict, Any, List, Optional
from datetime import datetime, timedelta
import logging
import time
import threading

logger = logging.getLogger(__name__)

try:
    import xlwings as xw
except ImportError:
    xw = None

class IncrementalProcessor:
    """Process PI data units incrementally with status updates"""
    
    def __init__(self):
        """Initialize incremental processor"""
        self.processing_status = {}
        self.lock = threading.RLock()
        
    def process_units_incrementally(self, xlsx_path: Path, units: List[str], 
                                  output_dir: Path = None) -> Dict[str, Any]:
        """Process units one by one with individual status updates"""
        
        if output_dir is None:
            output_dir = Path("data/processed")
        
        overall_start = datetime.now()
        results = {
            "overall_start": overall_start.isoformat(),
            "units_to_process": units.copy(),
            "unit_results": {},
            "completed_units": [],
            "failed_units": [],
            "current_unit": None,
            "overall_status": "STARTING"
        }
        
        print(f"\n🚀 INCREMENTAL PROCESSING STARTED")
        print(f"📋 Units to process: {len(units)}")
        print(f"📁 Output directory: {output_dir}")
        print(f"⏰ Started at: {overall_start.strftime('%H:%M:%S')}")
        print("=" * 70)
        
        for i, unit in enumerate(units):
            unit_number = i + 1
            unit_start = datetime.now()
            
            results["current_unit"] = unit
            results["overall_status"] = f"PROCESSING_UNIT_{unit_number}"
            
            print(f"\n[{unit_number}/{len(units)}] 🔄 PROCESSING: {unit}")
            print(f"⏰ Started: {unit_start.strftime('%H:%M:%S')}")
            print("-" * 50)
            
            try:
                # Process individual unit
                unit_result = self._process_single_unit(
                    xlsx_path=xlsx_path,
                    unit=unit,
                    output_dir=output_dir,
                    unit_number=unit_number,
                    total_units=len(units)
                )
                
                unit_end = datetime.now()
                unit_duration = (unit_end - unit_start).total_seconds()
                
                # Update results
                unit_result.update({
                    "unit": unit,
                    "started_at": unit_start.isoformat(),
                    "completed_at": unit_end.isoformat(),
                    "duration_seconds": unit_duration,
                    "unit_number": unit_number
                })
                
                results["unit_results"][unit] = unit_result
                
                if unit_result["success"]:
                    results["completed_units"].append(unit)
                    print(f"✅ [{unit_number}/{len(units)}] {unit} COMPLETED in {unit_duration:.1f}s")
                    
                    if unit_result.get("parquet_file"):
                        size_mb = unit_result["parquet_file"]["size_mb"]
                        records = unit_result["parquet_file"]["records"]
                        print(f"   📊 Generated: {size_mb:.1f}MB, {records:,} records")
                        
                else:
                    results["failed_units"].append(unit)
                    print(f"❌ [{unit_number}/{len(units)}] {unit} FAILED after {unit_duration:.1f}s")
                    print(f"   Error: {unit_result.get('error', 'Unknown error')}")
                
                # Show overall progress
                completed_count = len(results["completed_units"])
                failed_count = len(results["failed_units"]) 
                remaining = len(units) - unit_number
                progress_pct = (unit_number / len(units)) * 100
                
                overall_elapsed = (datetime.now() - overall_start).total_seconds()
                avg_time = overall_elapsed / unit_number
                estimated_remaining = avg_time * remaining
                
                print(f"📈 Progress: {completed_count} completed, {failed_count} failed, {remaining} remaining ({progress_pct:.1f}%)")
                print(f"⏱️  Average: {avg_time:.1f}s/unit, Est. remaining: {estimated_remaining/60:.1f} minutes")
                
            except Exception as e:
                unit_end = datetime.now()
                unit_duration = (unit_end - unit_start).total_seconds()
                
                error_result = {
                    "success": False,
                    "error": str(e),
                    "unit": unit,
                    "started_at": unit_start.isoformat(),
                    "failed_at": unit_end.isoformat(),
                    "duration_seconds": unit_duration,
                    "unit_number": unit_number
                }
                
                results["unit_results"][unit] = error_result
                results["failed_units"].append(unit)
                
                print(f"💥 [{unit_number}/{len(units)}] {unit} CRASHED after {unit_duration:.1f}s")
                print(f"   Error: {str(e)}")
        
        # Final summary
        overall_end = datetime.now()
        overall_duration = (overall_end - overall_start).total_seconds()
        
        results.update({
            "overall_end": overall_end.isoformat(),
            "overall_duration_seconds": overall_duration,
            "overall_status": "COMPLETED",
            "current_unit": None,
            "success_rate": len(results["completed_units"]) / len(units) * 100,
            "total_completed": len(results["completed_units"]),
            "total_failed": len(results["failed_units"])
        })
        
        print("\n" + "=" * 70)
        print("🎯 INCREMENTAL PROCESSING SUMMARY")
        print("=" * 70)
        print(f"⏰ Total time: {overall_duration/60:.1f} minutes ({overall_duration:.1f} seconds)")
        print(f"✅ Successful: {results['total_completed']}/{len(units)} units ({results['success_rate']:.1f}%)")
        print(f"❌ Failed: {results['total_failed']}/{len(units)} units")
        
        if results["completed_units"]:
            print(f"🎉 Completed units: {', '.join(results['completed_units'])}")
        
        if results["failed_units"]:
            print(f"⚠️  Failed units: {', '.join(results['failed_units'])}")
        
        return results
    
    def _process_single_unit(self, xlsx_path: Path, unit: str, output_dir: Path,
                           unit_number: int, total_units: int) -> Dict[str, Any]:
        """Process a single unit with detailed status"""
        
        result = {
            "success": False,
            "unit": unit,
            "steps_completed": [],
            "error": None
        }
        
        try:
            # Step 1: Excel Processing
            print(f"   🔄 Step 1/4: Opening Excel file...")
            result["steps_completed"].append("excel_open")
            
            if xw is None:
                raise RuntimeError("xlwings not available")
            
            app = xw.App(visible=False, add_book=False)
            app.display_alerts = False
            app.screen_updating = False
            
            # Step 2: Data Refresh
            print(f"   🔄 Step 2/4: Refreshing PI data for {unit}...")
            wb = app.books.open(str(xlsx_path))
            
            # Focus on unit-specific data if possible
            try:
                # Try to find unit-specific worksheet
                if unit in [ws.name for ws in wb.sheets]:
                    sheet = wb.sheets[unit]
                    print(f"   📋 Found unit-specific sheet: {unit}")
                    sheet.api.RefreshAll()
                else:
                    print(f"   📋 Using full workbook refresh...")
                    wb.api.RefreshAll()
            except Exception:
                wb.api.RefreshAll()  # Fallback to full refresh
            
            result["steps_completed"].append("data_refresh")
            
            # Step 3: Data Processing
            print(f"   🔄 Step 3/4: Processing data...")
            
            # Load fresh data from Excel
            from .ingest import load_latest_frame
            df = load_latest_frame(xlsx_path, unit=unit)
            
            if df.empty:
                raise RuntimeError(f"No data found for unit {unit}")
            
            result["steps_completed"].append("data_processing")
            
            # Step 4: Save to Parquet
            print(f"   🔄 Step 4/4: Saving to Parquet...")
            
            from .ingest import write_parquet
            parquet_filename = f"{unit}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.parquet"
            parquet_path = output_dir / parquet_filename
            
            output_path = write_parquet(df, parquet_path)
            
            # Get file stats
            file_size = output_path.stat().st_size
            
            parquet_info = {
                "path": str(output_path),
                "filename": parquet_filename,
                "size_bytes": file_size,
                "size_mb": file_size / (1024 * 1024),
                "records": len(df),
                "columns": list(df.columns)
            }
            
            result.update({
                "success": True,
                "parquet_file": parquet_info,
                "records_processed": len(df)
            })
            
            result["steps_completed"].append("parquet_save")
            
            # Cleanup Excel
            wb.save()
            wb.close()
            app.quit()
            
            return result
            
        except Exception as e:
            result["error"] = str(e)
            
            # Cleanup on error
            try:
                if 'wb' in locals():
                    wb.close()
                if 'app' in locals():
                    app.quit()
            except:
                pass
            
            return result


def run_incremental_processing(xlsx_path: Path = None, units: List[str] = None) -> Dict[str, Any]:
    """Run incremental processing with auto-detection"""
    
    if xlsx_path is None:
        # Auto-detect Excel file
        possible_paths = [
            Path("data/raw/Automation.xlsx"),
            Path("data/raw/PCFS_Automation.xlsx"),
            Path("Automation.xlsx")
        ]
        
        xlsx_path = None
        for path in possible_paths:
            if path.exists():
                xlsx_path = path
                break
        
        if xlsx_path is None:
            return {
                "success": False,
                "error": "No Excel file found. Tried: " + ", ".join(str(p) for p in possible_paths)
            }
    
    if units is None:
        # Auto-detect units
        try:
            from .parquet_database import ParquetDatabase
            db = ParquetDatabase()
            units = db.get_all_units()
        except:
            units = ["K-12-01", "K-16-01", "K-19-01", "K-31-01"]
    
    # Run incremental processing
    processor = IncrementalProcessor()
    return processor.process_units_incrementally(xlsx_path, units)


if __name__ == "__main__":
    # Run incremental processing
    result = run_incremental_processing()
    
    if result["success_rate"] > 0:
        print(f"\n🎉 Processing completed with {result['success_rate']:.1f}% success rate")
    else:
        print(f"\n💥 All units failed to process")
    
    print(f"\nTotal time: {result['overall_duration_seconds']/60:.1f} minutes")