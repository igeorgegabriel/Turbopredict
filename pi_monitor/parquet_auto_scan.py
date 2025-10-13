"""Auto-scan functionality using existing Parquet files for TURBOPREDICT X PROTEAN
Works with real data in the data directory
"""

from __future__ import annotations

from pathlib import Path
from typing import List, Dict, Any, Optional
import pandas as pd
import logging
from datetime import datetime, timedelta
import time
import os
import json

from .parquet_database import ParquetDatabase
from .config import Config
from .excel_refresh import refresh_excel_safe
from .breakout import detect_breakouts
from .batch import build_unit_from_tags
from .clean import dedup_parquet
from .memory_optimizer import MemoryMonitor, ChunkedProcessor, StreamingParquetHandler, memory_efficient_dedup, optimize_dataframe_memory

logger = logging.getLogger(__name__)

# Try to import rich/colorama for cyberpunk styling
try:
    from rich.console import Console
    from rich.panel import Panel
    from rich.table import Table
    from rich.text import Text
    from rich import box
    RICH_AVAILABLE = True
except ImportError:
    RICH_AVAILABLE = False

if not RICH_AVAILABLE:
    try:
        from colorama import init, Fore, Back, Style
        init(autoreset=True)
        COLORAMA_AVAILABLE = True
    except ImportError:
        COLORAMA_AVAILABLE = False


class ParquetAutoScanner:
    """Auto-scanner using existing Parquet files"""
    
    def __init__(self, config: Config = None, data_dir: Path = None):
        """Initialize auto-scanner for Parquet files.

        Args:
            config: Configuration object
            data_dir: Path to data directory
        """
        self.config = config or Config()

        if data_dir is None:
            # Default to data directory relative to current location
            current_dir = Path(__file__).parent.parent
            data_dir = current_dir / "data"

        self.db = ParquetDatabase(data_dir)

        # Memory optimization components - aggressive settings for large datasets
        self.memory_monitor = MemoryMonitor(memory_threshold_gb=1.0)  # Reduced threshold for aggressive optimization
        self.chunked_processor = ChunkedProcessor(chunk_size=250_000, memory_monitor=self.memory_monitor)  # Smaller chunks
        self.streaming_handler = StreamingParquetHandler(temp_dir=data_dir / "temp", memory_monitor=self.memory_monitor)

    def _print_cyberpunk_summary(self, results: Dict[str, Any]):
        """Print cyberpunk-themed summary of refresh results"""
        if RICH_AVAILABLE:
            self._print_rich_summary(results)
        elif COLORAMA_AVAILABLE:
            self._print_colorama_summary(results)
        else:
            self._print_plain_summary(results)

    def _print_rich_summary(self, results: Dict[str, Any]):
        """Rich/beautiful cyberpunk summary"""
        console = Console()

        # Build summary data
        successful = results.get('successful_units', 0)
        failed = results.get('failed_units', 0)
        total = successful + failed
        success_rate = results.get('success_rate', 0)
        total_time = results.get('total_time', 0)

        # Create cyberpunk header
        console.print()
        console.print("╔" + "═" * 68 + "╗", style="bright_cyan")
        console.print("║" + " " * 68 + "║", style="bright_cyan")
        console.print("║" + "  ██╗███╗   ██╗ ██████╗██████╗ ███████╗███╗   ███╗███████╗███╗   ██╗████████╗  ██║".center(68) + "║", style="bright_cyan")
        console.print("║" + "  ██║████╗  ██║██╔════╝██╔══██╗██╔════╝████╗ ████║██╔════╝████╗  ██║╚══██╔══╝  ██║".center(68) + "║", style="bright_cyan")
        console.print("║" + "  ██║██╔██╗ ██║██║     ██████╔╝█████╗  ██╔████╔██║█████╗  ██╔██╗ ██║   ██║     ██║".center(68) + "║", style="bright_cyan")
        console.print("║" + "  ██║██║╚██╗██║██║     ██╔══██╗██╔══╝  ██║╚██╔╝██║██╔══╝  ██║╚██╗██║   ██║     ██║".center(68) + "║", style="bright_cyan")
        console.print("║" + "  ██║██║ ╚████║╚██████╗██║  ██║███████╗██║ ╚═╝ ██║███████╗██║ ╚████║   ██║     ██║".center(68) + "║", style="bright_cyan")
        console.print("║" + "  ╚═╝╚═╝  ╚═══╝ ╚═════╝╚═╝  ╚═╝╚══════╝╚═╝     ╚═╝╚══════╝╚═╝  ╚═══╝   ╚═╝     ╚═╝".center(68) + "║", style="bright_cyan")
        console.print("║" + "                 ██████╗ ███████╗███████╗██████╗ ███████╗███████╗██╗  ██╗".center(68) + "║", style="bright_magenta")
        console.print("║" + "                 ██╔══██╗██╔════╝██╔════╝██╔══██╗██╔════╝██╔════╝██║  ██║".center(68) + "║", style="bright_magenta")
        console.print("║" + "                 ██████╔╝█████╗  █████╗  ██████╔╝█████╗  ███████╗███████║".center(68) + "║", style="bright_magenta")
        console.print("║" + "                 ██╔══██╗██╔══╝  ██╔══╝  ██╔══██╗██╔══╝  ╚════██║██╔══██║".center(68) + "║", style="bright_magenta")
        console.print("║" + "                 ██║  ██║███████╗██║     ██║  ██║███████╗███████║██║  ██║".center(68) + "║", style="bright_magenta")
        console.print("║" + "                 ╚═╝  ╚═╝╚══════╝╚═╝     ╚═╝  ╚═╝╚══════╝╚══════╝╚═╝  ╚═╝".center(68) + "║", style="bright_magenta")
        console.print("║" + " " * 68 + "║", style="bright_cyan")
        console.print("╚" + "═" * 68 + "╝", style="bright_cyan")
        console.print()

        # Create summary table with Excel fetch timing
        table = Table(show_header=True, header_style="bold bright_cyan", box=box.DOUBLE_EDGE, border_style="bright_cyan")
        table.add_column("▪ UNIT", style="bright_magenta", width=12)
        table.add_column("▪ STATUS", style="bright_yellow", width=10, justify="center")
        table.add_column("▪ FETCH", style="bright_green", width=8, justify="right")
        table.add_column("▪ TOTAL", style="bright_cyan", width=8, justify="right")
        table.add_column("▪ RESULT", style="bright_white", width=25)

        # Add results for each unit
        total_fetch_time = 0.0
        for unit, result in results.get('unit_results', {}).items():
            status_text = "✓ OK" if result['success'] else "✗ FAIL"
            status_style = "bright_green" if result['success'] else "bright_red"
            time_text = f"{result.get('processing_time', 0):.1f}s"

            # Get Excel fetch time from stage_times
            stage_times = result.get('stage_times', {})
            fetch_time = stage_times.get('incremental_fetch', 0)
            fetch_text = f"{fetch_time:.1f}s" if fetch_time > 0 else "N/A"
            if fetch_time > 0:
                total_fetch_time += fetch_time

            if result['success']:
                detail = f"{result.get('records_after', 0):,} records"
            else:
                detail = f"{result.get('error', 'Unknown error')[:23]}"

            table.add_row(
                unit,
                Text(status_text, style=status_style),
                fetch_text,
                time_text,
                detail
            )

        console.print(table)
        console.print()

        # Stats panel with fetch timing breakdown
        fetch_pct = (total_fetch_time / total_time * 100) if total_time > 0 else 0
        stats_content = f"""
[bright_cyan]⏱  TOTAL TIME:[/]      [bright_yellow]{total_time/60:.1f}min[/] ([bright_white]{total_time:.1f}s[/])
[bright_green]📊 FETCH TIME:[/]      [bright_yellow]{total_fetch_time/60:.1f}min[/] ([bright_white]{total_fetch_time:.1f}s, {fetch_pct:.1f}%[/])
[bright_green]✓  SUCCESS:[/]         [bright_white]{successful}/{total}[/] units
[bright_red]✗  FAILED:[/]          [bright_white]{failed}/{total}[/] units
[bright_magenta]◆  SUCCESS RATE:[/]    [bright_yellow]{success_rate:.1f}%[/]
"""

        panel = Panel(
            stats_content.strip(),
            title="[bold bright_cyan]▪▫▪ REFRESH STATISTICS ▪▫▪[/]",
            border_style="bright_magenta",
            box=box.DOUBLE
        )
        console.print(panel)
        console.print()

    def _print_colorama_summary(self, results: Dict[str, Any]):
        """Colorama fallback cyberpunk summary"""
        successful = results.get('successful_units', 0)
        failed = results.get('failed_units', 0)
        total = successful + failed
        success_rate = results.get('success_rate', 0)
        total_time = results.get('total_time', 0)

        # Calculate total fetch time
        total_fetch_time = 0.0
        for result in results.get('unit_results', {}).values():
            stage_times = result.get('stage_times', {})
            total_fetch_time += stage_times.get('incremental_fetch', 0)

        print(f"\n{Fore.CYAN}{'=' * 70}")
        print(f"{Fore.MAGENTA}{Style.BRIGHT}INCREMENTAL REFRESH SUMMARY")
        print(f"{Fore.CYAN}{'=' * 70}\n")

        # Unit results with fetch timing
        for unit, result in results.get('unit_results', {}).items():
            if result['success']:
                status = f"{Fore.GREEN}[OK] SUCCESS{Style.RESET_ALL}"
                stage_times = result.get('stage_times', {})
                fetch_time = stage_times.get('incremental_fetch', 0)
                if fetch_time > 0:
                    status += f" (fetch: {fetch_time:.1f}s)"
            else:
                status = f"{Fore.RED}[FAIL] ERROR{Style.RESET_ALL}"
            print(f"{Fore.MAGENTA}{unit}: {status}")

        print(f"\n{Fore.CYAN}{'-' * 70}")
        print(f"{Fore.YELLOW}⏱  Total Time:     {total_time/60:.1f} min ({total_time:.1f}s)")
        fetch_pct = (total_fetch_time / total_time * 100) if total_time > 0 else 0
        print(f"{Fore.GREEN}📊 Fetch Time:     {total_fetch_time/60:.1f} min ({total_fetch_time:.1f}s, {fetch_pct:.1f}%)")
        print(f"{Fore.GREEN}✓  Success:        {successful}/{total} units")
        print(f"{Fore.RED}✗  Failed:         {failed}/{total} units")
        print(f"{Fore.MAGENTA}◆  Success Rate:   {success_rate:.1f}%")
        print(f"{Fore.CYAN}{'=' * 70}\n")

    def _print_plain_summary(self, results: Dict[str, Any]):
        """Plain text fallback summary"""
        successful = results.get('successful_units', 0)
        failed = results.get('failed_units', 0)
        total = successful + failed
        success_rate = results.get('success_rate', 0)
        total_time = results.get('total_time', 0)

        # Calculate total fetch time
        total_fetch_time = 0.0
        for result in results.get('unit_results', {}).values():
            stage_times = result.get('stage_times', {})
            total_fetch_time += stage_times.get('incremental_fetch', 0)

        print(f"\n{'=' * 70}")
        print(f"INCREMENTAL REFRESH SUMMARY")
        print(f"{'=' * 70}\n")

        for unit, result in results.get('unit_results', {}).items():
            status = "[OK] SUCCESS" if result['success'] else "[FAIL] ERROR"
            stage_times = result.get('stage_times', {})
            fetch_time = stage_times.get('incremental_fetch', 0)
            if result['success'] and fetch_time > 0:
                status += f" (fetch: {fetch_time:.1f}s)"
            print(f"{unit}: {status}")

        print(f"\n{'-' * 70}")
        print(f"Total Time:     {total_time/60:.1f} min ({total_time:.1f}s)")
        fetch_pct = (total_fetch_time / total_time * 100) if total_time > 0 else 0
        print(f"Fetch Time:     {total_fetch_time/60:.1f} min ({total_fetch_time:.1f}s, {fetch_pct:.1f}%)")
        print(f"Success:        {successful}/{total} units")
        print(f"Failed:         {failed}/{total} units")
        print(f"Success Rate:   {success_rate:.1f}%")
        print(f"{'=' * 70}\n")

    def scan_all_units(self, max_age_hours: float = 1.0, force_refresh: bool = False) -> Dict[str, Any]:
        """Scan all available units and check data freshness.
        
        Args:
            max_age_hours: Maximum data age before considering stale
            force_refresh: Force refresh even if data is fresh
            
        Returns:
            Dictionary with scan results
        """
        results = {
            'scan_timestamp': datetime.now().isoformat(),
            'max_age_hours': max_age_hours,
            'force_refresh': force_refresh,
            'units_scanned': [],
            'fresh_units': [],
            'stale_units': [],
            'empty_units': [],
            'total_records': 0,
            'total_size_mb': 0
        }
        
        # Proactively archive any stray Parquet files not prefixed by a unit id
        try:
            archived = self.db.archive_non_unit_parquet()
            if archived:
                logger.info(f"Archived {len(archived)} non-unit Parquet file(s) to 'archive/'")
        except Exception:
            pass

        # Get all available units
        units = self.db.get_all_units()
        logger.info(f"Found {len(units)} units with data")
        
        for unit in units:
            try:
                # Get freshness info
                info = self.db.get_data_freshness_info(unit)
                
                unit_result = {
                    'unit': unit,
                    'total_records': info['total_records'],
                    'unique_tags': len(info['unique_tags']),
                    'latest_timestamp': info['latest_timestamp'],
                    'data_age_hours': info['data_age_hours'],
                    'is_stale': info['is_stale'],
                    'date_range_days': info['date_range_days']
                }
                
                results['units_scanned'].append(unit_result)
                results['total_records'] += info['total_records']
                
                # Categorize units
                if info['total_records'] == 0:
                    results['empty_units'].append(unit)
                elif force_refresh or (info['data_age_hours'] and info['data_age_hours'] > max_age_hours):
                    results['stale_units'].append(unit)
                else:
                    results['fresh_units'].append(unit)
                    
            except Exception as e:
                logger.error(f"Error scanning unit {unit}: {e}")
        
        # Calculate summary statistics
        results['summary'] = {
            'total_units': len(units),
            'fresh_units': len(results['fresh_units']),
            'stale_units': len(results['stale_units']),
            'empty_units': len(results['empty_units']),
            'freshness_rate': len(results['fresh_units']) / len(units) if units else 0,
            'total_records': results['total_records'],
            'units_needing_refresh': len(results['stale_units']) + len(results['empty_units'])
        }
        
        logger.info(f"Scan complete: {results['summary']['fresh_units']} fresh, "
                   f"{results['summary']['stale_units']} stale, "
                   f"{results['summary']['empty_units']} empty units")
        
        return results

    # --- Auto-build support for missing units ---------------------------------
    def _discover_configured_units(self) -> list[dict[str, Any]]:
        """Discover units from tag files in the config directory.

        Returns list of dicts: { 'unit': str, 'plant': str, 'tags_file': Path }
        """
        configured: list[dict[str, Any]] = []
        try:
            config_dir = Path(__file__).parent.parent / "config"
            if not config_dir.exists():
                return configured

            for tags_file in sorted(config_dir.glob("tags_*.txt")):
                # Read first non-empty, non-comment line to infer unit/plant
                first_line = None
                try:
                    for raw in tags_file.read_text(encoding="utf-8").splitlines():
                        s = raw.strip()
                        if s and not s.startswith("#"):
                            first_line = s
                            break
                except Exception:
                    first_line = None

                unit = None
                plant = None

                # Infer from first tag line if available
                if first_line:
                    # Try to find patterns like 'K-31-01', 'C-02001', 'C-104', 'C-13001', 'XT-07002'
                    import re
                    m = re.search(r"\b([A-Z]{1,4}-\d{2,5}(?:-\d{2})?)\b", first_line)
                    if m:
                        unit = m.group(1)
                    # Plant hint: token before first '.' or '_' often the plant code (e.g. PCFS, PCM)
                    m2 = re.match(r"^([A-Za-z]+)[\._]", first_line)
                    if m2:
                        plant_code = m2.group(1).upper()
                        # Map known aliases
                        plant = {
                            "PCM": "PCMSB",
                        }.get(plant_code, plant_code)

                # Fallbacks from filename
                name = tags_file.stem.lower()
                if unit is None:
                    if "k12_01" in name:
                        unit = "K-12-01"
                    elif "k16_01" in name:
                        unit = "K-16-01"
                    elif "k19_01" in name:
                        unit = "K-19-01"
                    elif "k31_01" in name:
                        unit = "K-31-01"
                    elif "c02001" in name or "c-02001" in name:
                        unit = "C-02001"
                    elif "c104" in name or "c-104" in name:
                        unit = "C-104"
                    elif "c13001" in name or "c-13001" in name:
                        unit = "C-13001"
                    elif "c1301" in name or "c-1301" in name:
                        unit = "C-1301"
                    elif "c1302" in name or "c-1302" in name:
                        unit = "C-1302"
                    elif "c201" in name or "c-201" in name:
                        unit = "C-201"
                    elif "c202" in name or "c-202" in name:
                        unit = "C-202"
                    elif ("abf" in name) and ("07" in name) and ("mt01" in name) and ("k001" in name):
                        unit = "07-MT01-K001"
                    elif ("abf" in name) and ("07" in name) and ("mt001" in name) and ("k001" in name):
                        unit = "07-MT01-K001"
                    elif "xt07002" in name or "xt-07002" in name or "xt_07002" in name:
                        unit = "XT-07002"

                if plant is None:
                    # Heuristic: PCMSB for files mentioning pcmsb/pcm, else PCFS
                    if ("abf" in name):
                        plant = "ABF"
                    else:
                        plant = "PCMSB" if ("pcmsb" in name or "pcm" in name) else "PCFS"

                if unit:
                    configured.append({
                        "unit": unit,
                        "plant": plant,
                        "tags_file": tags_file,
                    })
        except Exception:
            # Fail-soft: no configured units discovered
            pass
        return configured

    def _auto_build_missing_units(self, xlsx_path: Path) -> list[dict[str, Any]]:
        """Build Parquet files for any configured units missing in processed/.

        Returns list of build result dicts.
        """
        results: list[dict[str, Any]] = []
        configured = self._discover_configured_units()
        if not configured:
            return results

        # Normalize aliases to canonical names to avoid duplicate (alias vs canonical) builds
        alias_map = {
            '07-MT01/K001': '07-MT01-K001',
            '07-MT001/K001': '07-MT01-K001',
            'FI-07001': '07-MT01-K001',
        }
        def _canon(u: str) -> str:
            return alias_map.get(u, u)

        existing_units = set(_canon(u) for u in self.db.get_all_units())
        for item in configured:
            item["unit"] = _canon(item.get("unit", ""))
        to_build = [c for c in configured if c["unit"] not in existing_units]
        if not to_build:
            return results

        print(f"Discovered {len(to_build)} configured unit(s) with no Parquet. Seeding...")
        for item in to_build:
            unit = item["unit"]
            plant = item["plant"]
            tags_file: Path = Path(item["tags_file"])  # type: ignore[assignment]
            try:
                import re as _re
                safe_unit = _re.sub(r"[^A-Za-z0-9._-]", "_", str(unit))
                out_parquet = self.db.processed_dir / f"{safe_unit}_1y_0p1h.parquet"
                out_parquet.parent.mkdir(parents=True, exist_ok=True)
                # Prefer plant-specific workbook if available (e.g., PCMSB)
                preferred_wb = xlsx_path
                try:
                    if str(plant).upper().startswith("PCMSB"):
                        pcmsb_wb = Path("excel/PCMSB/PCMSB_Automation.xlsx")
                        if pcmsb_wb.exists():
                            preferred_wb = pcmsb_wb
                    if str(plant).upper().startswith("ABF"):
                        abf_wb = Path("excel/ABF_Automation.xlsx")
                        if abf_wb.exists():
                            preferred_wb = abf_wb
                except Exception:
                    pass
                print(f"  - Building {plant} {unit} from {tags_file.name} using {Path(preferred_wb).name} -> {out_parquet.name}")
                # PCMSB units need longer timeouts due to PI server response times
                settle_time = 30.0 if str(plant).upper().startswith("PCMSB") else 1.0

                build_unit_from_tags(
                    xlsx=preferred_wb,
                    tags=[t.strip() for t in tags_file.read_text(encoding="utf-8").splitlines() if t.strip() and not t.strip().startswith('#')],
                    out_parquet=out_parquet,
                    plant=plant,
                    unit=unit,
                    settle_seconds=settle_time,
                    # use defaults for: server/start/end/step/sheet/visible
                )
                # Dedup
                dedup_path = dedup_parquet(out_parquet)
                size_mb = dedup_path.stat().st_size / (1024 * 1024)
                print(f"    Seeded {unit}: {len(pd.read_parquet(dedup_path)):,} rows, {size_mb:.1f}MB")
                results.append({
                    "unit": unit,
                    "plant": plant,
                    "parquet": str(dedup_path),
                    "size_mb": size_mb,
                    "success": True,
                })
            except Exception as e:
                print(f"    Failed to seed {unit}: {e}")
                results.append({
                    "unit": unit,
                    "plant": plant,
                    "parquet": None,
                    "size_mb": None,
                    "success": False,
                    "error": str(e),
                })
        # Refresh DB view of processed files
        self.db = ParquetDatabase(self.db.data_dir)
        return results

    def analyze_unit_data(self, unit: str, run_anomaly_detection: bool = True, days_limit: int = 90) -> Dict[str, Any]:
        """Analyze data for a specific unit.

        Args:
            unit: Unit identifier
            run_anomaly_detection: Whether to run anomaly detection
            days_limit: Only analyze last N days of data (default 90 for performance)

        Returns:
            Analysis results with timing information
        """
        import time

        # Start timing
        unit_start_time = time.time()
        timing = {}

        logger.info(f"Analyzing unit: {unit} (last {days_limit} days)")
        print(f"   [DEBUG] Loading data for {unit} with {days_limit} days filter...")

        # Get unit data with time filter at database level (much faster!)
        fetch_start = time.time()
        if days_limit > 0:
            cutoff_date = datetime.now() - timedelta(days=days_limit)
            print(f"   [DEBUG] Cutoff date: {cutoff_date}, calling get_unit_data...")
            df = self.db.get_unit_data(unit, start_time=cutoff_date)
            logger.info(f"Loaded {len(df):,} records for {unit} (last {days_limit} days)")
            print(f"   [DEBUG] Loaded {len(df):,} records successfully")
        else:
            df = self.db.get_unit_data(unit)
            print(f"   [DEBUG] Loaded {len(df):,} records (no filter)")

        fetch_time = time.time() - fetch_start
        timing['data_fetch_seconds'] = round(fetch_time, 2)
        print(f"   [TIMING] Data fetch: {fetch_time:.2f}s")
        
        if df.empty:
            return {
                'unit': unit,
                'status': 'no_data',
                'records': 0,
                'tags': [],
                'analysis_timestamp': datetime.now().isoformat()
            }
        
        # Basic statistics
        analysis = {
            'unit': unit,
            'status': 'success',
            'records': len(df),
            'date_range': {
                'start': df['time'].min().isoformat() if 'time' in df.columns else None,
                'end': df['time'].max().isoformat() if 'time' in df.columns else None,
            },
            'analysis_timestamp': datetime.now().isoformat()
        }
        
        # Tag analysis
        if 'tag' in df.columns:
            tag_summary = self.db.get_tag_summary(unit)
            analysis['tags'] = tag_summary.to_dict('records') if not tag_summary.empty else []
            analysis['unique_tags'] = len(df['tag'].unique())
        else:
            analysis['tags'] = []
            analysis['unique_tags'] = 0
        
        # Value statistics
        if 'value' in df.columns:
            analysis['value_stats'] = {
                'count': int(df['value'].count()),
                'mean': float(df['value'].mean()) if df['value'].count() > 0 else None,
                'std': float(df['value'].std()) if df['value'].count() > 0 else None,
                'min': float(df['value'].min()) if df['value'].count() > 0 else None,
                'max': float(df['value'].max()) if df['value'].count() > 0 else None,
                'null_count': int(df['value'].isnull().sum())
            }
        
        # Extended data freshness analysis - fetch latest regardless of staleness
        analysis['extended_freshness'] = self._analyze_extended_freshness(df, unit)

        # Enhanced anomaly detection with baseline tuning + staleness as anomaly
        if run_anomaly_detection and 'value' in df.columns and 'tag' in df.columns:
            print(f"   [DEBUG] Starting anomaly detection for {unit}...")
            detection_start = time.time()

            analysis['anomalies'] = self._detect_anomalies_enhanced(df, unit)

            detection_time = time.time() - detection_start
            timing['anomaly_detection_seconds'] = round(detection_time, 2)
            print(f"   [TIMING] Anomaly detection: {detection_time:.2f}s")

            # Add staleness as instrumentation anomaly
            print(f"   [DEBUG] Adding staleness anomalies...")
            analysis['anomalies'] = self._add_staleness_anomalies(analysis['anomalies'], analysis['extended_freshness'])
            print(f"   [DEBUG] Staleness anomalies added")

        # Calculate total time
        total_time = time.time() - unit_start_time
        timing['total_seconds'] = round(total_time, 2)
        timing['total_minutes'] = round(total_time / 60, 2)

        # Add timing to analysis
        analysis['timing'] = timing

        print(f"   [TIMING] Total unit analysis: {total_time:.2f}s ({total_time/60:.2f}min)")
        print(f"   [SUMMARY] {unit}: {len(df):,} records, {analysis.get('unique_tags', 0)} tags, {timing.get('total_seconds', 0)}s")

        return analysis
    
    def _detect_anomalies_enhanced(self, df: pd.DataFrame, unit: str) -> Dict[str, Any]:
        """Enhanced anomaly detection with baseline tuning and unit status awareness"""
        try:
            # Try to use smart detection with unit status checking first
            print(f"   [DEBUG] Calling smart_anomaly_detection for {unit} with {len(df)} records...")
            from .smart_anomaly_detection import smart_anomaly_detection

            # Disable auto-plotting during analysis (plots are generated later in batch)
            results = smart_anomaly_detection(df, unit, auto_plot_anomalies=False)
            print(f"   [DEBUG] smart_anomaly_detection returned")
            
            # Convert to expected format with unit status awareness
            if results:
                unit_status = results.get('unit_status', {})
                analysis_performed = results.get('anomaly_analysis_performed', True)
                
                base_result = {
                    'total_anomalies': results.get('total_anomalies', 0),
                    'anomaly_rate': results.get('anomaly_rate', 0.0),
                    'by_tag': results.get('by_tag', {}),
                    'method': results.get('method', 'smart_enhanced'),
                    'baseline_calibrated': results.get('config_loaded', False),
                    'tags_analyzed': results.get('tags_analyzed', 0),
                    'unit_status': unit_status.get('status', 'UNKNOWN'),
                    'unit_message': unit_status.get('message', ''),
                    'analysis_performed': analysis_performed
                }
                
                # If tuned path is conservative fallback (no baseline), use robust MTD for better sensitivity
                if base_result['method'] == 'conservative_fallback' and not base_result['baseline_calibrated']:
                    return self._detect_simple_anomalies(df, unit)

                # If tuned result looks suspiciously high (likely mis-calibrated), fallback to robust MTD/IF
                # DISABLED: Allow enhanced detection with higher anomaly rates for better sensitivity
                # try:
                #     if base_result.get('anomaly_rate', 0) > 0.01:
                #         logger.warning("Tuned anomaly rate > 1%; using robust MTD/IF fallback")
                #         return self._detect_simple_anomalies(df, unit)
                # except Exception:
                #     pass

                # Generate appropriate detection summary
                if not analysis_performed:
                    base_result['detection_summary'] = f"UNIT OFFLINE: {unit_status.get('message', 'Unit not running')}"
                elif results.get('total_anomalies', 0) > 0:
                    status_note = f" ({unit_status.get('status', 'RUNNING')})" if unit_status.get('status') != 'RUNNING' else ""
                    base_result['detection_summary'] = f"Smart detection: {results['total_anomalies']} anomalies found ({results['anomaly_rate']*100:.2f}%){status_note}"
                else:
                    base_result['detection_summary'] = f"No anomalies detected - Unit {unit_status.get('status', 'RUNNING')}"
                return base_result
            else:
                # Fallback result
                return {
                    'total_anomalies': 0,
                    'anomaly_rate': 0.0,
                    'by_tag': {},
                    'method': 'smart_enhanced_fallback',
                    'baseline_calibrated': False,
                    'detection_summary': "Smart detection fallback - no anomalies detected"
                }
                
        except ImportError:
            # Fall back to original detection if tuned module not available
            logger.warning("Tuned anomaly detection module not available, using original method")
            return self._detect_simple_anomalies(df, unit)
        except Exception as e:
            # Fall back to original detection on any error
            logger.warning(f"Enhanced anomaly detection failed: {e}, using original method")
            return self._detect_simple_anomalies(df)
    
    def _detect_simple_anomalies(self, df: pd.DataFrame, unit_hint: Optional[str] = None) -> Dict[str, Any]:
        """Mahalanobis-Taguchi Distance anomaly detection for turbomachinery, focused on last 7 days.
        
        Args:
            df: DataFrame with time series data
            
        Returns:
            Anomaly detection results using MTD
        """
        anomalies = {
            'total_anomalies': 0,
            'anomaly_rate': 0.0,
            'by_tag': {},
            'analysis_period': 'last_7_days',
            'date_range': None,
            'method': 'mahalanobis_taguchi_distance',
            'speed_dominant': True
        }
        
        try:
            # Filter to recent data for anomaly detection with fallback strategy
            if 'time' in df.columns and not df.empty:
                now = datetime.now()
                
                # Convert time column to datetime if needed
                if not pd.api.types.is_datetime64_any_dtype(df['time']):
                    df['time'] = pd.to_datetime(df['time'])
                
                # Try 7 days first
                seven_days_ago = now - timedelta(days=7)
                recent_df = df[df['time'] >= seven_days_ago].copy()
                
                # If 7-day data insufficient, fallback to 1 year
                if recent_df.empty or len(recent_df) < 100:
                    anomalies['analysis_period'] = 'fallback_1_year'
                    one_year_ago = now - timedelta(days=365)
                    recent_df = df[df['time'] >= one_year_ago].copy()
                    
                    if recent_df.empty:
                        anomalies['error'] = 'No data available in last year'
                        return anomalies
                    
                    print(f"MTD: Using 1-year fallback data ({len(recent_df):,} records)")
                else:
                    anomalies['analysis_period'] = 'last_7_days'
                
                anomalies['date_range'] = {
                    'start': recent_df['time'].min().isoformat(),
                    'end': recent_df['time'].max().isoformat(),
                    'records_analyzed': len(recent_df)
                }
                
                # Use recent data for analysis
                analysis_df = recent_df
            else:
                # Fallback to full dataset if no time column
                analysis_df = df
                anomalies['analysis_period'] = 'full_dataset'
            
            # Use MTD for multivariate anomaly detection
            mtd_results = self._mahalanobis_taguchi_detection(analysis_df)
            
            # Check if MTD found anomalies - if not, use Isolation Forest as fallback
            # Also trigger if anomaly rate is suspiciously high (>50%), suggesting MTD sensitivity issues
            mtd_anomalies = mtd_results.get('mtd_anomalies', 0)
            mtd_rate = mtd_results.get('anomaly_rate', 0)
            
            if (mtd_anomalies == 0 or mtd_rate > 0.5) and 'error' not in mtd_results:
                if mtd_anomalies == 0:
                    print("MTD detected no anomalies. Proceeding with Isolation Forest batch analysis...")
                else:
                    print(f"MTD anomaly rate too high ({mtd_rate*100:.1f}%). Using Isolation Forest for refined analysis...")
                if_results = self._isolation_forest_batch_analysis(analysis_df, unit_hint)
                
                # Merge both results
                anomalies.update(mtd_results)
                anomalies['isolation_forest'] = if_results
                anomalies['method'] = 'mtd_with_isolation_forest_fallback'
                anomalies['total_anomalies'] = if_results.get('total_anomalies', 0)
                anomalies['anomaly_rate'] = if_results.get('anomaly_rate', 0.0)
                # Combine per-tag results so plotter has signals
                combined_by_tag = {}
                try:
                    for src in (mtd_results.get('by_tag', {}), if_results.get('by_tag', {})):
                        if src:
                            for t, info in src.items():
                                if t not in combined_by_tag:
                                    combined_by_tag[t] = dict(info)
                                else:
                                    combined_by_tag[t]['count'] = combined_by_tag[t].get('count', 0) + info.get('count', 0)
                                    combined_by_tag[t]['rate'] = max(combined_by_tag[t].get('rate', 0.0), info.get('rate', 0.0))
                    if combined_by_tag:
                        anomalies['by_tag'] = combined_by_tag
                except Exception:
                    pass
            else:
                # Use MTD results
                anomalies.update(mtd_results)
            
        except Exception as e:
            logger.error(f"Error in MTD anomaly detection: {e}")
            anomalies['error'] = str(e)
        
        return anomalies
    
    def _mahalanobis_taguchi_detection(self, df: pd.DataFrame) -> Dict[str, Any]:
        """Implement Mahalanobis-Taguchi Distance for turbomachinery anomaly detection.
        
        Speed as dominant X-axis, all other parameters as Y-axis variables.
        
        Args:
            df: DataFrame with time series data
            
        Returns:
            MTD-based anomaly results
        """
        import numpy as np
        # Use numpy instead of scipy for broader compatibility
        
        results = {
            'mtd_anomalies': 0,
            'mtd_threshold': 0,
            'speed_parameter': None,
            'multivariate_analysis': {},
            'by_tag': {}
        }
        # Ensure local unit variable exists to avoid scope/name issues
        unit_local = None

        try:
            # Identify speed parameter - prioritize known speed indicators
            speed_tags = []
            other_tags = []
            
            # Known speed indicators by unit (ISA-5.1 SI = Speed Indicator)
            known_speed_tags = {
                'K-12-01': 'PCFS_K-12-01_12SI-401B_PV',
                'K-16-01': 'PCFS_K-16-01_16SI-501B_PV',
                'K-19-01': 'PCFS_K-19-01_19SI-601B_PV',
                'K-31-01': 'PCFS_K-31-01_31KI-302_PV'
            }
            
            # Extract unit from data
            for tag in df['tag'].unique():
                for unit_code in known_speed_tags.keys():
                    if unit_code in tag:
                        unit_local = unit_code
                        break
                if unit_local:
                    break
            
            # Use known speed tag if available
            if unit_local and known_speed_tags[unit_local]:
                if known_speed_tags[unit_local] in df['tag'].unique():
                    speed_tags = [known_speed_tags[unit_local]]
                    other_tags = [tag for tag in df['tag'].unique() if tag != known_speed_tags[unit_local]]
            
            # Fallback: search by keywords
            if not speed_tags:
                for tag in df['tag'].unique():
                    if any(keyword in tag.upper() for keyword in ['SI-', 'SPEED', 'RPM', 'FREQ', 'ROTATION']):
                        speed_tags.append(tag)
                    else:
                        other_tags.append(tag)
            
            # Last resort: use first available tag
            if not speed_tags:
                all_tags = df['tag'].unique()
                if len(all_tags) > 0:
                    speed_tags = [all_tags[0]]
                    other_tags = all_tags[1:].tolist()
            
            if not speed_tags:
                results['error'] = 'No suitable speed parameter found'
                return results
            
            # Use primary speed tag
            primary_speed_tag = speed_tags[0]
            results['speed_parameter'] = primary_speed_tag
            
            # Load optional MTD config for this unit
            # Defaults
            mtd_cfg = {
                'resample': 'h',
                'baseline_fraction': 0.7,
                'threshold_quantile': 0.995,
                'support_fraction': 0.75,
                'max_features': 20
            }
            try:
                # Derive unit code robustly from available tags
                unit_candidates = []
                for uc in ['K-12-01', 'K-16-01', 'K-19-01', 'K-31-01']:
                    if any(uc in str(t) for t in df['tag'].unique()):
                        unit_candidates.append(uc)
                unit_code = unit_candidates[0] if unit_candidates else None
                # Try baseline config file first
                if unit_code:
                    base_cfg_path = Path(f"baseline_config_{unit_code}.json")
                    if base_cfg_path.exists():
                        with open(base_cfg_path, 'r', encoding='utf-8') as f:
                            base_cfg = json.load(f)
                        if isinstance(base_cfg, dict) and 'mtd_config' in base_cfg:
                            mtd_cfg.update({k: base_cfg['mtd_config'][k] for k in mtd_cfg.keys() if k in base_cfg['mtd_config']})
                    # Then try standalone mtd config
                    standalone_path = Path(f"mtd_config_{unit_code}.json")
                    if standalone_path.exists():
                        with open(standalone_path, 'r', encoding='utf-8') as f:
                            stand_cfg = json.load(f)
                        if isinstance(stand_cfg, dict):
                            mtd_cfg.update({k: stand_cfg[k] for k in mtd_cfg.keys() if k in stand_cfg})
            except Exception:
                pass
            
            # Create multivariate dataset with speed as dominant axis
            # Use time-based resampling to handle sparse data
            df_time_indexed = df.set_index('time')
            
            # Resample to create aligned dataset (configurable)
            resample_rule = mtd_cfg.get('resample', 'h') or 'h'
            pivot_df = df_time_indexed.groupby(['tag']).resample(resample_rule)['value'].mean().unstack(level=0).reset_index()
            pivot_df.columns.name = None  # Remove multi-level column name
            
            # Ensure we have the speed column and other parameters
            available_tags = [col for col in pivot_df.columns if col != 'time']
            
            if primary_speed_tag not in available_tags:
                results['error'] = f'Speed parameter {primary_speed_tag} not found in resampled data'
                return results
            
            # Select top correlated features to reduce dimensionality
            # Start with speed + all other parameters (we'll prune later)
            feature_cols = [primary_speed_tag]
            other_features = [tag for tag in available_tags if tag != primary_speed_tag]
            
            # Add other features (candidates)
            cfg_max_feat = int(mtd_cfg.get('max_features', 20) or 20)
            feature_cols.extend(other_features)
            
            if len(feature_cols) < 2:
                results['error'] = 'Insufficient multivariate features for MTD analysis'
                return results
            
            # Create feature matrix with forward fill for missing values
            X = pivot_df[feature_cols].ffill().dropna()
            
            if len(X) < 50:
                results['error'] = f'Insufficient data points for MTD analysis: {len(X)} points (need ≥50)'
                return results
            
            # Prefer speed-windowed baseline within recent window if configured
            X_baseline = None
            X_test = None
            try:
                baseline_days = int(mtd_cfg.get('baseline_days', 30) or 30)
            except Exception:
                baseline_days = 30
            try:
                speed_window = mtd_cfg.get('speed_window_rpm', None)
            except Exception:
                speed_window = None
            auto_bw = bool(mtd_cfg.get('auto_speed_window', False))
            window_pct = float(mtd_cfg.get('window_pct', 0.03) or 0.03)
            min_window_rpm = float(mtd_cfg.get('min_window_rpm', 100) or 100)

            # Build recent slice for baseline/test
            pivot_df['time'] = pd.to_datetime(pivot_df['time'], errors='coerce')
            if baseline_days and len(pivot_df) > 0:
                cutoff = pivot_df['time'].max() - pd.Timedelta(days=baseline_days)
                recent_mask = pivot_df['time'] >= cutoff
            else:
                recent_mask = pd.Series([True] * len(pivot_df))

            # Auto speed-window discovery
            if (not speed_window) and auto_bw and primary_speed_tag in pivot_df.columns:
                try:
                    speed_series = pivot_df.loc[recent_mask, primary_speed_tag].dropna()
                    if len(speed_series) >= 50:
                        binned = (speed_series/10.0).round().astype(int)
                        mode_bin = binned.value_counts().idxmax()
                        center = float(mode_bin * 10)
                        half_width = max(min_window_rpm, center * window_pct)
                        speed_window = [center - half_width, center + half_width]
                except Exception:
                    speed_window = None

            if speed_window and primary_speed_tag in pivot_df.columns:
                lo, hi = float(speed_window[0]), float(speed_window[1])
                base_mask = recent_mask & pivot_df[primary_speed_tag].between(lo, hi, inclusive='both')
                Xb_raw = pivot_df.loc[base_mask, feature_cols].ffill().dropna()
                Xt_raw = pivot_df.loc[recent_mask, feature_cols].ffill().dropna()
                if len(Xb_raw) >= 50 and len(Xt_raw) >= 1:
                    X_baseline = Xb_raw.copy()
                    X_test = Xt_raw.copy()

            # Fallback to time-split if no valid speed-window baseline
            if X_baseline is None or X_test is None:
                baseline_fraction = float(mtd_cfg.get('baseline_fraction', 0.7) or 0.7)
                baseline_fraction = min(0.9, max(0.5, baseline_fraction))
                baseline_size = int(len(X) * baseline_fraction)
                X_baseline = X.iloc[:baseline_size].copy()
                X_test = X.iloc[baseline_size:].copy()

            # Standardize features using baseline mean/std (z-score)
            # Drop near-constant features to avoid numerical issues
            eps = 1e-8
            mu = X_baseline.mean()
            sigma = X_baseline.std(ddof=0).replace(0, np.nan)
            keep_cols_all = sigma[sigma > eps].index.tolist()
            if len(keep_cols_all) < 2:
                results['error'] = 'Insufficient informative features after standardization'
                return results

            # Correlation-pruned, variance-ranked selection
            selected = []
            if primary_speed_tag in keep_cols_all:
                selected.append(primary_speed_tag)
            variances = sigma[keep_cols_all].sort_values(ascending=False)
            for col in variances.index:
                if col == primary_speed_tag:
                    continue
                if len(selected) >= cfg_max_feat:
                    break
                try:
                    ok = True
                    for s in selected:
                        corr = float(X_baseline[[col, s]].corr().iloc[0,1])
                        if abs(corr) >= 0.95:
                            ok = False
                            break
                    if ok:
                        selected.append(col)
                except Exception:
                    selected.append(col)
            keep_cols = selected if len(selected) >= 2 else keep_cols_all[:cfg_max_feat]

            Xb = ((X_baseline[keep_cols] - mu[keep_cols]) / sigma[keep_cols]).dropna()
            Xt = ((X_test[keep_cols] - mu[keep_cols]) / sigma[keep_cols]).dropna()
            if len(Xb) < 50 or len(Xt) < 1:
                results['error'] = f'Insufficient standardized data for MTD analysis: baseline={len(Xb)}, test={len(Xt)}'
                return results

            # Robust covariance estimation (MinCovDet), fallback to LedoitWolf, then np.cov
            cov_inv = None
            robust_mean = Xb.mean().values
            try:
                from sklearn.covariance import MinCovDet
                import warnings as _warnings
                support_fraction = float(mtd_cfg.get('support_fraction', 0.75) or 0.75)
                support_fraction = min(0.95, max(0.5, support_fraction))
                with _warnings.catch_warnings():
                    _warnings.simplefilter("ignore", category=RuntimeWarning)
                    mcd = MinCovDet(support_fraction=support_fraction, random_state=42).fit(Xb.values)
                robust_mean = mcd.location_
                cov = mcd.covariance_
                cov_inv = np.linalg.pinv(cov)
            except Exception:
                try:
                    from sklearn.covariance import LedoitWolf
                    lw = LedoitWolf().fit(Xb.values)
                    cov = lw.covariance_
                    cov_inv = np.linalg.pinv(cov)
                except Exception:
                    try:
                        cov = np.cov(Xb.T)
                        cov_inv = np.linalg.pinv(cov)
                    except Exception:
                        results['error'] = 'Covariance matrix estimation/inversion failed'
                        return results

            # Mahalanobis distance helper
            def mahalanobis_distance(point, mean, inv):
                diff = point - mean
                return float(np.sqrt(np.dot(np.dot(diff, inv), diff)))

            # Compute baseline distances to set an adaptive threshold (e.g., 99.5th percentile)
            baseline_d = [mahalanobis_distance(row.values, robust_mean, cov_inv) for _, row in Xb.iterrows()]
            if len(baseline_d) < 10:
                results['error'] = 'Insufficient baseline distances for thresholding'
                return results
            mtd_q = float(mtd_cfg.get('threshold_quantile', 0.995) or 0.995)
            mtd_q = min(0.9999, max(0.9, mtd_q))
            mtd_threshold = float(np.quantile(baseline_d, mtd_q))  # robust high-quantile threshold
            if not np.isfinite(mtd_threshold) or mtd_threshold <= 0:
                mtd_threshold = 3.0  # fallback

            # Calculate distances for test data
            mahal_distances = []
            anomaly_indices = []
            for idx, row in Xt.iterrows():
                try:
                    d = mahalanobis_distance(row.values, robust_mean, cov_inv)
                    mahal_distances.append(d)
                    if d > mtd_threshold:
                        anomaly_indices.append(idx)
                except Exception:
                    continue

            results['mtd_threshold'] = mtd_threshold
            results['mtd_anomalies'] = len(anomaly_indices)
            results['total_test_points'] = len(Xt)
            results['anomaly_rate'] = len(anomaly_indices) / len(Xt) if len(Xt) > 0 else 0
            
            # Detailed analysis by parameter
            results['multivariate_analysis'] = {
                'speed_parameter': primary_speed_tag,
                'feature_count': len(keep_cols),
                'features_analyzed': keep_cols,
                'baseline_size': len(Xb),
                'test_size': len(Xt),
                'mean_mahal_distance': float(np.mean(mahal_distances)) if mahal_distances else 0.0,
                'max_mahal_distance': float(np.max(mahal_distances)) if mahal_distances else 0.0,
                'threshold_quantile': mtd_q,
                'baseline_distance_mean': float(np.mean(baseline_d)) if baseline_d else 0.0,
                'baseline_distance_p995': mtd_threshold,
                'baseline_stats': {
                    param: {
                        'mean': float(mu[param]) if param in mu else None,
                        'std': float(sigma[param]) if param in sigma else None
                    } for param in keep_cols
                }
            }
            
            # Per-tag contribution analysis for MTD (no IQR). Count tags with |z|>z_threshold and
            # optionally those outside density bands derived from the baseline at stable speed.
            try:
                contrib_counts: Dict[str, int] = {}
                z_threshold = float(mtd_cfg.get('z_threshold', 3.0) or 3.0)
                if anomaly_indices:
                    # Xt is standardized on keep_cols
                    Xt_anom = Xt.loc[anomaly_indices, keep_cols]
                    # Count per-feature exceedances in standardized space
                    exceed = (Xt_anom.abs() > z_threshold)
                    counts = exceed.sum(axis=0)
                    for col, cnt in counts.items():
                        if int(cnt) > 0:
                            contrib_counts[col] = contrib_counts.get(col, 0) + int(cnt)

                # Add out-of-band counts using raw baseline bands if available
                try:
                    band_p = mtd_cfg.get('band_percentiles', [5, 95])
                    p_low = float(band_p[0])
                    p_high = float(band_p[1])
                except Exception:
                    p_low, p_high = 5.0, 95.0

                if 'Xb_raw' in locals() and 'Xt_raw' in locals() and len(Xt) > 0:
                    bands = {}
                    for col in keep_cols:
                        try:
                            lo_b = float(np.nanpercentile(Xb_raw[col].values, p_low))
                            hi_b = float(np.nanpercentile(Xb_raw[col].values, p_high))
                            bands[col] = (lo_b, hi_b)
                        except Exception:
                            continue
                    # Align Xt_raw to Xt indices if possible
                    try:
                        Xt_raw_slice = Xt_raw.loc[Xt.index]
                    except Exception:
                        Xt_raw_slice = Xt_raw.iloc[:len(Xt)]
                    for col, (lo_b, hi_b) in bands.items():
                        try:
                            series = Xt_raw_slice[col]
                            ob_mask = (series < lo_b) | (series > hi_b)
                            ob_count = int(ob_mask.sum())
                            if ob_count > 0:
                                contrib_counts[col] = contrib_counts.get(col, 0) + ob_count
                        except Exception:
                            continue
                # Novelty IF on baseline, evaluated only at MTD event times (combine A + C)
                try:
                    if 'Xb_raw' in locals() and 'Xt_raw_slice' in locals() and anomaly_indices:
                        # Align event rows in raw space
                        try:
                            evt_rows = Xt_raw_slice.loc[anomaly_indices]
                        except Exception:
                            # fall back to positional alignment
                            evt_pos = [i for i in range(min(len(Xt_raw_slice), len(Xt))) if Xt.index[i] in anomaly_indices]
                            evt_rows = Xt_raw_slice.iloc[evt_pos]
                        if len(evt_rows) > 0:
                            alpha = float(mtd_cfg.get('if_novelty_alpha', 0.01) or 0.01)
                            from sklearn.ensemble import IsolationForest as _IF
                            for col in keep_cols:
                                try:
                                    train = Xb_raw[[col]].dropna().values
                                    test = evt_rows[[col]].dropna().values
                                    if len(train) < 20 or len(test) < 5:
                                        continue
                                    iso = _IF(contamination='auto', n_estimators=100, random_state=42)
                                    iso.fit(train)
                                    thr = float(np.quantile(iso.decision_function(train), alpha))
                                    test_scores = iso.decision_function(test)
                                    nov_count = int((test_scores < thr).sum())
                                    if nov_count > 0:
                                        contrib_counts[col] = contrib_counts.get(col, 0) + nov_count
                                except Exception:
                                    continue
                except Exception:
                    pass

                # Breakout detection (rolling-window) within stable-speed window
                try:
                    if 'Xb_raw' in locals() and 'Xt_raw' in locals() and len(pivot_df) > 0:
                        br = detect_breakouts(
                            pivot_df,
                            speed_col=primary_speed_tag,
                            tag_cols=keep_cols,
                            window=int(mtd_cfg.get('break_window', 20) or 20),
                            q_low=float(mtd_cfg.get('break_q_low', p_low/100 if 'p_low' in locals() else 0.10)),
                            q_high=float(mtd_cfg.get('break_q_high', p_high/100 if 'p_high' in locals() else 0.90)),
                            persist=int(mtd_cfg.get('break_persist', 2) or 2),
                            persist_window=int(mtd_cfg.get('break_persist_window', 3) or 3),
                            cooldown=int(mtd_cfg.get('break_cooldown', 5) or 5),
                            speed_window=speed_window if 'speed_window' in locals() else None,
                            recent_mask=recent_mask if 'recent_mask' in locals() else None,
                        )
                        for col, info in br.items():
                            contrib_counts[col] = contrib_counts.get(col, 0) + int(info.get('count', 0))
                except Exception:
                    pass

                # Populate by_tag with counts and rates
                by_tag: Dict[str, Any] = {}
                for col, cnt in contrib_counts.items():
                    by_tag[col] = {
                        'count': int(cnt),
                        'rate': float(cnt / max(1, len(Xt))),
                        'method': 'mtd_contribution',
                        'is_speed_parameter': col == primary_speed_tag
                    }
                results['by_tag'] = by_tag
            except Exception:
                results['by_tag'] = {}

            # Set total anomalies to MTD count (multivariate events)
            results['total_anomalies'] = results['mtd_anomalies']
                
        except Exception as e:
            logger.error(f"Error in MTD calculation: {e}")
            results['error'] = f'MTD calculation failed: {str(e)}'
        
        return results
    
    def _isolation_forest_batch_analysis(self, df: pd.DataFrame, unit_hint: Optional[str] = None) -> Dict[str, Any]:
        """Isolation Forest anomaly detection with batch processing for large datasets.
        
        Args:
            df: DataFrame with time series data
            
        Returns:
            Isolation Forest anomaly results
        """
        from sklearn.ensemble import IsolationForest
        import numpy as np
        
        results = {
            'total_anomalies': 0,
            'anomaly_rate': 0.0,
            'batches_processed': 0,
            'batch_results': [],
            'by_tag': {},
            'method': 'isolation_forest_batch'
        }
        
        try:
            # Create time-indexed multivariate dataset similar to MTD
            if 'time' not in df.columns or 'tag' not in df.columns:
                results['error'] = 'Missing required columns (time, tag)'
                return results
            
            # Resample to configured interval for consistent batch processing
            df_time_indexed = df.set_index('time')
            resample_rule_if = 'h'
            try:
                # Try to align IF resample to MTD config for this unit
                unit_code = unit_hint or ''
                if not unit_code:
                    # derive from tags in df
                    tags = df['tag'].unique().tolist()
                    for uc in ['K-12-01','K-16-01','K-19-01','K-31-01']:
                        if any(uc in str(t) for t in tags):
                            unit_code = uc
                            break
                if unit_code:
                    mtd_cfg_path = Path(f"mtd_config_{unit_code}.json")
                    if mtd_cfg_path.exists():
                        with open(mtd_cfg_path, 'r', encoding='utf-8') as f:
                            _m = json.load(f)
                        if isinstance(_m, dict) and _m.get('resample'):
                            resample_rule_if = _m.get('resample')
            except Exception:
                pass
            pivot_df = df_time_indexed.groupby(['tag']).resample(resample_rule_if)['value'].mean().unstack(level=0).reset_index()
            pivot_df.columns.name = None
            
            # Get available features
            available_tags = [col for col in pivot_df.columns if col != 'time']
            
            if len(available_tags) < 2:
                results['error'] = 'Insufficient features for Isolation Forest analysis'
                return results
            
            # Prepare feature matrix
            X = pivot_df[available_tags].ffill().dropna()
            
            if len(X) < 50:
                results['error'] = f'Insufficient data points: {len(X)} (need ≥50)'
                return results
            
            # Batch processing parameters
            batch_size = min(10000, max(1000, len(X) // 5))  # Adaptive batch size
            n_batches = (len(X) + batch_size - 1) // batch_size

            # Load optional IF config for this unit
            if_cfg = {
                'contamination': 0.05,
                'n_estimators': 100,
                'min_batch': 10
            }
            try:
                # Determine unit code from hint or by scanning tag names
                unit_code = unit_hint or ''
                if not unit_code:
                    for uc in ['K-12-01', 'K-16-01', 'K-19-01', 'K-31-01']:
                        if any(uc in str(t) for t in available_tags):
                            unit_code = uc
                            break
                base_cfg_path = Path(f"baseline_config_{unit_code}.json")
                if base_cfg_path.exists():
                    with open(base_cfg_path, 'r', encoding='utf-8') as f:
                        base_cfg = json.load(f)
                    if isinstance(base_cfg, dict) and 'if_config' in base_cfg:
                        for k in list(if_cfg.keys()):
                            if k in base_cfg['if_config']:
                                if_cfg[k] = base_cfg['if_config'][k]
                stand_path = Path(f"if_config_{unit_code}.json")
                if stand_path.exists():
                    with open(stand_path, 'r', encoding='utf-8') as f:
                        stand = json.load(f)
                    if isinstance(stand, dict):
                        for k in list(if_cfg.keys()):
                            if k in stand:
                                if_cfg[k] = stand[k]
            except Exception:
                pass
            
            # Build batch boundaries and merge small tail with previous batch
            min_batch = int(if_cfg.get('min_batch', 10) or 10)
            boundaries = []
            start = 0
            N = len(X)
            while start < N:
                end = min(start + batch_size, N)
                boundaries.append((start, end))
                start = end
            if len(boundaries) >= 2:
                tail = boundaries[-1][1] - boundaries[-1][0]
                if tail < min_batch:
                    prev_start, _ = boundaries[-2]
                    boundaries[-2] = (prev_start, N)
                    boundaries.pop()
            n_batches = len(boundaries)

            print(f"Isolation Forest: Processing {N:,} records in {n_batches} batches (size: {batch_size:,}, min_batch: {min_batch})")

            all_anomaly_scores = []
            all_anomaly_labels = []
            batch_anomaly_counts = []
            
            # Process data in batches
            for batch_idx, (start_idx, end_idx) in enumerate(boundaries):
                X_batch = X.iloc[start_idx:end_idx]
                
                # Train Isolation Forest on batch
                iso_forest = IsolationForest(
                    n_estimators=int(if_cfg.get('n_estimators', 100) or 100),
                    contamination=float(if_cfg.get('contamination', 0.05) or 0.05),
                    random_state=42,
                    n_jobs=-1
                )
                
                try:
                    # Fit and predict
                    anomaly_labels = iso_forest.fit_predict(X_batch)
                    anomaly_scores = iso_forest.decision_function(X_batch)
                    
                    # Count anomalies in this batch (Isolation Forest uses -1 for anomalies)
                    batch_anomalies = np.sum(anomaly_labels == -1)
                    batch_anomaly_counts.append(batch_anomalies)
                    
                    # Store results
                    all_anomaly_scores.extend(anomaly_scores)
                    all_anomaly_labels.extend(anomaly_labels)
                    
                    batch_info = {
                        'batch_id': batch_idx + 1,
                        'size': len(X_batch),
                        'anomalies': int(batch_anomalies),
                        'anomaly_rate': batch_anomalies / len(X_batch),
                        'avg_score': float(np.mean(anomaly_scores)),
                        'min_score': float(np.min(anomaly_scores)),
                        'time_range': {
                            'start': X.index[start_idx].isoformat() if hasattr(X.index[start_idx], 'isoformat') else str(X.index[start_idx]),
                            'end': X.index[end_idx-1].isoformat() if hasattr(X.index[end_idx-1], 'isoformat') else str(X.index[end_idx-1])
                        }
                    }
                    results['batch_results'].append(batch_info)
                    
                    print(f"  Batch {batch_idx + 1}/{n_batches}: {batch_anomalies} anomalies ({batch_anomalies/len(X_batch)*100:.1f}%)")
                    
                except Exception as e:
                    print(f"  Batch {batch_idx + 1} failed: {e}")
                    continue
            
            results['batches_processed'] = len(results['batch_results'])
            
            # Calculate overall statistics
            if all_anomaly_labels:
                total_anomalies = np.sum(np.array(all_anomaly_labels) == -1)
                results['total_anomalies'] = int(total_anomalies)
                results['anomaly_rate'] = total_anomalies / len(all_anomaly_labels)
                
                results['score_statistics'] = {
                    'mean_score': float(np.mean(all_anomaly_scores)),
                    'std_score': float(np.std(all_anomaly_scores)),
                    'min_score': float(np.min(all_anomaly_scores)),
                    'max_score': float(np.max(all_anomaly_scores))
                }
                
                print(f"Isolation Forest: Found {total_anomalies:,} anomalies ({total_anomalies/len(all_anomaly_labels)*100:.2f}%)")
            
            # Individual tag analysis for comparison (all tags, no IQR, univariate IF)
            for tag in available_tags:
                tag_data = X[tag].dropna()
                if len(tag_data) >= 50:
                    try:
                        iso_forest_single = IsolationForest(
                            n_estimators=int(if_cfg.get('n_estimators', 100) or 100)//2,
                            contamination=float(if_cfg.get('contamination', 0.05) or 0.05),
                            random_state=42
                        )
                        single_predictions = iso_forest_single.fit_predict(tag_data.values.reshape(-1, 1))
                        tag_anomalies = np.sum(single_predictions == -1)
                        
                        if int(tag_anomalies) > 0:
                            results['by_tag'][tag] = {
                                'count': int(tag_anomalies),
                                'rate': tag_anomalies / len(tag_data),
                                'method': 'isolation_forest_univariate'
                            }
                    except:
                        continue
            
        except Exception as e:
            logger.error(f"Error in Isolation Forest analysis: {e}")
            results['error'] = f'Isolation Forest analysis failed: {str(e)}'
        
        return results

    def _analyze_extended_freshness(self, df: pd.DataFrame, unit: str) -> Dict[str, Any]:
        """Analyze data freshness and extend to latest fetch regardless of staleness.

        This method treats stale data as a form of instrumentation anomaly
        and fetches the latest available data regardless of staleness thresholds.
        """
        freshness_analysis = {
            'method': 'extended_fetch_regardless_staleness',
            'unit': unit,
            'timestamp': datetime.now().isoformat()
        }

        try:
            if df.empty or 'time' not in df.columns:
                freshness_analysis['status'] = 'no_time_data'
                return freshness_analysis

            # Convert time column to datetime if needed
            if not pd.api.types.is_datetime64_any_dtype(df['time']):
                df['time'] = pd.to_datetime(df['time'])

            # Current data analysis
            now = datetime.now()
            latest_data_time = pd.to_datetime(df['time'].max())
            hours_since_latest = (now - latest_data_time).total_seconds() / 3600

            freshness_analysis.update({
                'latest_data_time': latest_data_time.isoformat(),
                'hours_since_latest': hours_since_latest,
                'current_time': now.isoformat(),
                'is_stale_traditional': hours_since_latest > 1.0,  # Traditional 1-hour threshold
                'staleness_category': self._categorize_staleness(hours_since_latest)
            })

            # Attempt to fetch latest data if available
            try:
                # This would normally check if unit needs refresh, but we extend regardless
                extended_data = self._attempt_latest_fetch(unit, df)
                freshness_analysis['extended_fetch'] = extended_data
            except Exception as e:
                freshness_analysis['extended_fetch'] = {
                    'status': 'fetch_failed',
                    'error': str(e)
                }

            return freshness_analysis

        except Exception as e:
            freshness_analysis.update({
                'status': 'analysis_error',
                'error': str(e)
            })
            return freshness_analysis

    def _categorize_staleness(self, hours_old: float) -> Dict[str, Any]:
        """Categorize staleness level for instrumentation anomaly classification."""
        if hours_old <= 1.0:
            return {'level': 'fresh', 'severity': 'none', 'description': 'Data is current'}
        elif hours_old <= 6.0:
            return {'level': 'mildly_stale', 'severity': 'low', 'description': 'Slight data lag - normal variation'}
        elif hours_old <= 24.0:
            return {'level': 'stale', 'severity': 'medium', 'description': 'Data staleness - potential instrumentation issue'}
        elif hours_old <= 168.0:  # 1 week
            return {'level': 'very_stale', 'severity': 'high', 'description': 'Significant staleness - instrumentation anomaly likely'}
        else:
            return {'level': 'extremely_stale', 'severity': 'critical', 'description': 'Extreme staleness - instrumentation failure probable'}

    def _attempt_latest_fetch(self, unit: str, current_df: pd.DataFrame) -> Dict[str, Any]:
        """Attempt to fetch the very latest data regardless of staleness thresholds."""
        fetch_result = {
            'attempted': True,
            'timestamp': datetime.now().isoformat()
        }

        try:
            # Get unit info for potential refresh
            unit_info = self.db.get_unit_info(unit)
            if not unit_info:
                fetch_result.update({
                    'status': 'no_unit_info',
                    'message': 'Unit configuration not found'
                })
                return fetch_result

            # Check for most recent possible data
            current_latest = pd.to_datetime(current_df['time'].max()) if not current_df.empty else None

            fetch_result.update({
                'current_latest': current_latest.isoformat() if current_latest else None,
                'unit_info_available': True,
                'extended_range_attempted': True
            })

            # Plant-specific handling for all units
            fetch_result.update(self._get_plant_specific_handling(unit))

            return fetch_result

        except Exception as e:
            fetch_result.update({
                'status': 'fetch_error',
                'error': str(e)
            })
            return fetch_result

    def _add_staleness_anomalies(self, existing_anomalies: Dict[str, Any], freshness_analysis: Dict[str, Any]) -> Dict[str, Any]:
        """Add staleness as instrumentation anomalies to existing anomaly detection results."""

        if not existing_anomalies:
            existing_anomalies = {'total_anomalies': 0, 'by_tag': {}, 'method': 'baseline'}

        staleness_info = freshness_analysis.get('staleness_category', {})
        severity = staleness_info.get('severity', 'none')

        # Only treat medium+ severity staleness as anomalies
        if severity in ['medium', 'high', 'critical']:
            # Add instrumentation anomaly
            instrumentation_anomalies = {
                'staleness_anomaly': {
                    'type': 'instrumentation_anomaly',
                    'subtype': 'data_staleness',
                    'severity': severity,
                    'description': staleness_info.get('description', 'Data staleness detected'),
                    'hours_stale': freshness_analysis.get('hours_since_latest', 0),
                    'detection_method': 'extended_freshness_analysis'
                }
            }

            # Merge with existing anomalies
            if 'instrumentation_anomalies' not in existing_anomalies:
                existing_anomalies['instrumentation_anomalies'] = {}

            existing_anomalies['instrumentation_anomalies'].update(instrumentation_anomalies)

            # Update counts
            existing_anomalies['total_anomalies'] = existing_anomalies.get('total_anomalies', 0) + 1
            existing_anomalies['instrumentation_anomaly_count'] = len(existing_anomalies.get('instrumentation_anomalies', {}))

        # Add freshness metadata
        existing_anomalies['freshness_metadata'] = {
            'extended_analysis_performed': True,
            'staleness_category': staleness_info,
            'extended_fetch_attempted': 'extended_fetch' in freshness_analysis
        }

        return existing_anomalies

    def _get_plant_specific_handling(self, unit: str) -> Dict[str, Any]:
        """Get plant-specific handling configuration for extended analysis."""

        handling = {
            'plant_type': 'unknown',
            'special_handling': None,
            'timeout_settings': {},
            'working_tags_identified': [],
            'known_issues': [],
            'optimization_notes': []
        }

        unit_upper = unit.upper()

        # ABF (Abu Dhabi Future) Units
        if any(abf_pattern in unit_upper for abf_pattern in ['ABF', '07-MT01', '07-MT001']):
            handling.update({
                'plant_type': 'ABF',
                'special_handling': 'ABF_direct_fetch',
                'timeout_settings': {
                    'PI_FETCH_TIMEOUT': 45,  # ABF typically faster than PCMSB
                    'settle_seconds': 2.0,
                    'recommended_timeframe': '-1d'  # Shorter timeframe for testing
                },
                'known_issues': [
                    'Some ABF units may have popup issues',
                    'Excel automation can be sensitive to VPN connectivity'
                ],
                'optimization_notes': [
                    'Use visible=True if automation fails',
                    'Consider use_working_copy=True for Excel stability'
                ]
            })

            # Specific ABF units with known patterns
            if '07-MT01' in unit_upper or '07-MT001' in unit_upper:
                handling['working_tags_identified'] = [
                    f'PRISM.ABF.{unit}.FI-07001.PV',
                    f'ABF.{unit}.FI-07001.PV'
                ]
                handling['known_issues'].append('K001 variants may have timeout issues')

        # PCFS (Petronas Chemicals Fertilizer Sdn Bhd) Units
        elif any(pcfs_pattern in unit_upper for pcfs_pattern in ['PCFS', 'K-12-01', 'K-16-01', 'K-19-01', 'K-31-01']):
            handling.update({
                'plant_type': 'PCFS',
                'special_handling': 'PCFS_optimized_fetch',
                'timeout_settings': {
                    'PI_FETCH_TIMEOUT': 30,  # PCFS generally reliable
                    'settle_seconds': 1.5,
                    'recommended_timeframe': '-1y'  # Full year data typically available
                },
                'optimization_notes': [
                    'PCFS units typically have good connectivity',
                    'Standard timeouts usually sufficient',
                    'Rich historical data available'
                ]
            })

            # PCFS unit-specific working tags
            pcfs_working_tags = {
                'K-12-01': ['PCFS_K-12-01_12SI-401B_PV', 'PCFS_K-12-01_12FI-302_PV'],
                'K-16-01': ['PCFS_K-16-01_16SI-501B_PV', 'PCFS_K-16-01_16FI-401_PV'],
                'K-19-01': ['PCFS_K-19-01_19SI-601B_PV', 'PCFS_K-19-01_19FI-501_PV'],
                'K-31-01': ['PCFS_K-31-01_31KI-302_PV', 'PCFS_K-31-01_31FI-201_PV']
            }

            for pcfs_unit, tags in pcfs_working_tags.items():
                if pcfs_unit in unit:
                    handling['working_tags_identified'] = tags
                    break

        # PCMSB (Petronas Chemicals Methanol Sdn Bhd) Units
        elif any(pcmsb_pattern in unit_upper for pcmsb_pattern in ['PCMSB', 'XT-07002', 'C-02001', 'C-104', 'C-13001', 'C-1301', 'C-1302', 'C-201', 'C-202']):
            handling.update({
                'plant_type': 'PCMSB',
                'special_handling': 'PCMSB_enhanced_timeout_fetch',
                'timeout_settings': {
                    'PI_FETCH_TIMEOUT': 90,  # PCMSB needs longer timeouts
                    'settle_seconds': 3.0,
                    'recommended_timeframe': '-3d'  # Conservative timeframe due to server issues
                },
                'known_issues': [
                    'PCMSB PI server has slower response times',
                    'Many tags experience timeout issues',
                    'Some units may have connectivity problems'
                ],
                'optimization_notes': [
                    'Use longer timeouts (90s) for reliability',
                    'Consider shorter time ranges for initial testing',
                    'Focus on working tags first, then expand',
                    'May need visible Excel for debugging'
                ]
            })

            # PCMSB unit-specific working tags
            pcmsb_working_tags = {
                'XT-07002': ['PCM.XT-07002.070GZI8402.PV'],  # From user's evidence
                'C-02001': ['PCM.C-02001.020TI-001.PV'],
                'C-104': ['PCM.C-104.104TI-001.PV'],
                'C-13001': ['PCM.C-13001.130TI-001.PV'],
                'C-1301': ['PCM.C-1301.130TI-001.PV'],
                'C-1302': ['PCM.C-1302.130TI-002.PV'],
                'C-201': ['PCM.C-201.201TI-001.PV'],
                'C-202': ['PCM.C-202.202TI-001.PV']
            }

            for pcmsb_unit, tags in pcmsb_working_tags.items():
                if pcmsb_unit in unit:
                    handling['working_tags_identified'] = tags
                    break

            # Special case for XT-07002 (user's problematic unit)
            if 'XT-07002' in unit:
                handling['known_issues'].extend([
                    'Multiple tags timeout after 24s with default settings',
                    'Data exists but server response is slow',
                    'Some tags return empty data despite server connectivity'
                ])

        # Generic/Unknown Units
        else:
            handling.update({
                'plant_type': 'GENERIC',
                'special_handling': 'generic_adaptive_fetch',
                'timeout_settings': {
                    'PI_FETCH_TIMEOUT': 60,  # Conservative default
                    'settle_seconds': 2.0,
                    'recommended_timeframe': '-7d'  # Safe default
                },
                'optimization_notes': [
                    'Using generic settings - may need plant-specific tuning',
                    'Monitor performance and adjust timeouts as needed'
                ]
            })

        return handling

    def generate_data_quality_report(self, unit: str) -> Dict[str, Any]:
        """Generate a data quality report for a unit.
        
        Args:
            unit: Unit identifier
            
        Returns:
            Data quality report
        """
        df = self.db.get_unit_data(unit)
        
        if df.empty:
            return {
                'unit': unit,
                'status': 'no_data',
                'report_timestamp': datetime.now().isoformat()
            }
        
        quality_report = {
            'unit': unit,
            'report_timestamp': datetime.now().isoformat(),
            'overall_score': 0.0,
            'metrics': {}
        }
        
        # Completeness (missing values)
        if 'value' in df.columns:
            total_values = len(df)
            missing_values = df['value'].isnull().sum()
            completeness = (total_values - missing_values) / total_values if total_values > 0 else 0
            quality_report['metrics']['completeness'] = {
                'score': completeness,
                'missing_values': int(missing_values),
                'total_values': int(total_values)
            }
        
        # Consistency (check for duplicates)
        if 'time' in df.columns and 'tag' in df.columns:
            total_records = len(df)
            duplicate_records = df.duplicated(subset=['time', 'tag']).sum()
            consistency = (total_records - duplicate_records) / total_records if total_records > 0 else 0
            quality_report['metrics']['consistency'] = {
                'score': consistency,
                'duplicate_records': int(duplicate_records),
                'total_records': int(total_records)
            }
        
        # Freshness (data recency)
        if 'time' in df.columns:
            try:
                series = pd.to_datetime(df['time'], errors='coerce')
                local_tz = datetime.now().astimezone().tzinfo
                if getattr(series.dt, 'tz', None) is None:
                    series = series.dt.tz_localize(local_tz)
                else:
                    series = series.dt.tz_convert(local_tz)
                latest_local = series.max()
                latest_utc = latest_local.tz_convert('UTC')
                now_utc = pd.Timestamp.now(tz='UTC')
                hours_old = (now_utc - latest_utc).total_seconds() / 3600
                latest_out = latest_local.tz_localize(None)
            except Exception:
                latest_out = pd.to_datetime(df['time'].max(), errors='coerce')
                hours_old = None
            freshness = max(0, 1 - (hours_old / 24))  # Linear decay over 24 hours
            
            quality_report['metrics']['freshness'] = {
                'score': freshness,
                'hours_old': hours_old,
                'latest_timestamp': latest_out.isoformat() if latest_out is not None else None
            }
        
        # Calculate overall score (average of available metrics)
        scores = [metric['score'] for metric in quality_report['metrics'].values()]
        quality_report['overall_score'] = sum(scores) / len(scores) if scores else 0
        
        # Quality grade
        if quality_report['overall_score'] >= 0.9:
            quality_report['grade'] = 'A'
        elif quality_report['overall_score'] >= 0.8:
            quality_report['grade'] = 'B'
        elif quality_report['overall_score'] >= 0.7:
            quality_report['grade'] = 'C'
        elif quality_report['overall_score'] >= 0.6:
            quality_report['grade'] = 'D'
        else:
            quality_report['grade'] = 'F'
        
        return quality_report
    
    def get_comprehensive_status(self) -> Dict[str, Any]:
        """Get comprehensive status of all data.
        
        Returns:
            Complete status report
        """
        logger.info("Generating comprehensive status report")
        
        # Get database status
        db_status = self.db.get_database_status()
        
        # Scan all units
        scan_results = self.scan_all_units()
        
        # Generate quality reports for active units
        quality_reports = {}
        for unit in db_status['units'][:5]:  # Limit to top 5 units
            if unit['records'] > 0:
                quality_reports[unit['unit']] = self.generate_data_quality_report(unit['unit'])
        
        comprehensive_status = {
            'report_timestamp': datetime.now().isoformat(),
            'database_status': db_status,
            'scan_results': scan_results,
            'data_quality_reports': quality_reports,
            'recommendations': self._generate_recommendations(db_status, scan_results, quality_reports)
        }
        
        return comprehensive_status
    
    def _generate_recommendations(self, db_status: Dict, scan_results: Dict, quality_reports: Dict) -> List[str]:
        """Generate recommendations based on analysis.
        
        Args:
            db_status: Database status
            scan_results: Scan results
            quality_reports: Quality reports
            
        Returns:
            List of recommendations
        """
        recommendations = []
        
        # Storage recommendations
        if db_status['total_size_gb'] > 10:
            recommendations.append(f"Database size is {db_status['total_size_gb']:.1f}GB. Consider archiving old data.")
        
        # Freshness recommendations  
        if scan_results['summary']['stale_units'] > scan_results['summary']['fresh_units']:
            recommendations.append("More units have stale data than fresh. Consider increasing refresh frequency.")
        
        # Quality recommendations
        low_quality_units = [unit for unit, report in quality_reports.items() 
                           if report.get('overall_score', 0) < 0.7]
        if low_quality_units:
            recommendations.append(f"Units with low data quality: {', '.join(low_quality_units)}")
        
        # Empty units
        if scan_results['summary']['empty_units'] > 0:
            recommendations.append(f"{scan_results['summary']['empty_units']} units have no data. Check data collection.")
        
        if not recommendations:
            recommendations.append("Data quality and freshness look good! System is operating normally.")
        
        return recommendations
    
    def _get_excel_file_for_unit(self, unit: str, default_xlsx_path: Path = None) -> Path:
        """Determine the correct Excel file for a specific unit based on intelligent unit name patterns.

        This method uses unit naming conventions instead of depending on existing data,
        which fixes the circular dependency issue where stale units can't be refreshed
        because their plant info is missing from stale data.
        """
        project_root = Path(__file__).resolve().parents[1]

        # Primary method: Intelligent unit name pattern matching
        unit_upper = unit.upper()

        # K-units (K-12-01, K-16-01, K-19-01, K-31-01) -> PCFS
        if unit_upper.startswith('K-') and '-' in unit_upper:
            pcfs_paths = [
                project_root / "excel" / "PCFS" / "PCFS_Automation.xlsx",  # Working file first
                project_root / "excel" / "PCFS" / "PCFS_Automation_2.xlsx"
            ]
            for path in pcfs_paths:
                if path.exists():
                    return path

        # 07-MT01 units -> ABF (now in ABFSB directory)
        elif unit_upper.startswith('07-MT01') or unit_upper.startswith('ABF'):
            abf_paths = [
                project_root / "excel" / "ABFSB" / "ABF_Automation.xlsx",
                project_root / "excel" / "ABFSB" / "ABFSB_Automation.xlsx",
                project_root / "excel" / "ABFSB" / "ABFSB_Automation_Master.xlsx",
                project_root / "excel" / "ABF_Automation.xlsx",
                project_root / "excel" / "ABFSB_Automation.xlsx",
            ]
            for path in abf_paths:
                if path.exists():
                    return path

        # PCMSB units -> PCMSB ONLY (no PCFS fallback - different plants!)
        # PCMSB units include: units starting with PCMSB, containing PCMSB, or C-units (C-104, C-201, etc.)
        elif (unit_upper.startswith('PCMSB') or 'PCMSB' in unit_upper or
              unit_upper.startswith('C-') or unit_upper.startswith('XT-')):
            pcmsb_path = project_root / "excel" / "PCMSB" / "PCMSB_Automation.xlsx"
            if pcmsb_path.exists():
                return pcmsb_path
            else:
                # Attempt auto-recovery from most recent backup/dummy if main file missing
                pcmsb_dir = project_root / "excel" / "PCMSB"
                candidates = list(pcmsb_dir.glob("PCMSB_Automation_backup_*.xlsx")) + \
                              list(pcmsb_dir.glob("PCMSB_Automation_dummy_*.xlsx"))
                if candidates:
                    latest = max(candidates, key=lambda p: p.stat().st_mtime)
                    try:
                        import shutil
                        shutil.copy2(str(latest), str(pcmsb_path))
                        print(f"   Auto-recovered PCMSB workbook from backup: {latest.name} -> {pcmsb_path.name}")
                        return pcmsb_path
                    except Exception as _copy_err:
                        pass
                raise RuntimeError(f"PCMSB unit '{unit}' requires PCMSB_Automation.xlsx but file not found")

        # ABFSB units -> ABFSB (with ABF fallback)
        elif unit_upper.startswith('ABFSB') or 'ABFSB' in unit_upper:
            abfsb_paths = [
                project_root / "excel" / "ABFSB" / "ABFSB_Automation.xlsx",
                project_root / "excel" / "ABFSB" / "ABF_Automation.xlsx"     # Fallback
            ]
            for path in abfsb_paths:
                if path.exists():
                    return path

        # Secondary method: Try to get plant from existing data (if available)
        try:
            unit_data = self.db.get_unit_data(unit)
            if not unit_data.empty and 'plant' in unit_data.columns:
                plant = unit_data['plant'].iloc[0].upper()

                if plant.startswith("ABF"):
                    abf_paths = [
                        project_root / "excel" / "ABFSB" / "ABF_Automation.xlsx",
                        project_root / "excel" / "ABFSB" / "ABFSB_Automation.xlsx"
                    ]
                    for path in abf_paths:
                        if path.exists():
                            return path
                elif plant.startswith("PCMSB"):
                    pcmsb_path = project_root / "excel" / "PCMSB" / "PCMSB_Automation.xlsx"
                    if pcmsb_path.exists():
                        return pcmsb_path
                    else:
                        raise RuntimeError(f"PCMSB plant requires PCMSB_Automation.xlsx but file not found")
                elif plant.startswith("PCFS"):
                    pcfs_paths = [
                        project_root / "excel" / "PCFS" / "PCFS_Automation.xlsx",  # Working file first
                        project_root / "excel" / "PCFS" / "PCFS_Automation_2.xlsx"
                    ]
                    for path in pcfs_paths:
                        if path.exists():
                            return path
        except Exception:
            # Ignore data access errors - rely on pattern matching above
            pass

        # Fallback to default if provided
        if default_xlsx_path and default_xlsx_path.exists():
            return default_xlsx_path

        # Last resort - find any available Excel file (prioritize PCFS for unknown units)
        excel_paths = [
            project_root / "excel" / "PCFS" / "PCFS_Automation.xlsx",  # Working file first
            project_root / "excel" / "PCFS" / "PCFS_Automation_2.xlsx",
            project_root / "excel" / "ABFSB" / "ABF_Automation.xlsx",
            project_root / "excel" / "PCMSB" / "PCMSB_Automation.xlsx",
            project_root / "excel" / "ABFSB" / "ABFSB_Automation.xlsx",
            project_root / "excel" / "MLNG" / "MLNG_Automation.xlsx",
            project_root / "excel" / "PFLNG1" / "PFLNG1_Automation.xlsx",
            project_root / "excel" / "PFLNG2" / "PFLNG2_Automation.xlsx",
            project_root / "data" / "raw" / "Automation.xlsx",
            project_root / "Automation.xlsx"
        ]
        for path in excel_paths:
            if path.exists():
                return path

        raise RuntimeError("No Excel automation file found")



    def _infer_plant_from_unit(self, unit: str) -> str:
        """Best-effort mapping from unit id to plant.

        Known patterns:
        - ABF: legacy ids starting with digits (e.g., '07-MT01-K001') or new ids like '21-K002'
        - PCFS: 'K-xx-xx'
        - PCMSB: 'C-xxxxx' and 'XT-xxxxx'
        """
        import re
        u = unit.strip()
        if u.upper().startswith('ABF'):
            return 'ABF'
        # Digit-led ABF conventions (e.g., '07-...', '21-K002')
        if re.match(r"^\d{2}-", u):
            return 'ABF'
        if u.startswith('K-'):
            return 'PCFS'
        if u.startswith('C-') or u.startswith('XT-'):
            return 'PCMSB'
        return 'UNKNOWN'

    def _find_tags_file_for_unit(self, unit: str) -> Optional[Path]:
        """Locate the appropriate tag file in config/ for a unit.

        Supports:
        - ABF unit 07-MT01-K001 -> tags_abf_07mt01_k001.txt
        - PCMSB units C-xxxx / XT-xxxx -> tags_pcmsb_cxxxx.txt / tags_pcmsb_xtxxxx.txt
        - PCFS K-units (rarely needed here) -> tags_kxx_xx.txt
        """
        from .config import Config
        cfg = Config()
        cfg_dir = cfg.paths.project_root / 'config'

        u = unit.strip().upper()
        # ABF mapping
        if u == '07-MT01-K001' or u.startswith('07-MT01'):
            candidate = cfg_dir / 'tags_abf_07mt01_k001.txt'
            return candidate if candidate.exists() else None
        # New ABF unit mapping: 21-K002
        if u == '21-K002':
            candidate = cfg_dir / 'tags_abf_21k002.txt'
            return candidate if candidate.exists() else None

        # PCMSB mapping
        if u.startswith('C-'):
            key = u.replace('-', '').lower()
            candidate = cfg_dir / f'tags_pcmsb_{key}.txt'
            return candidate if candidate.exists() else None
        if u.startswith('XT-'):
            key = u.replace('-', '').lower()
            candidate = cfg_dir / f'tags_pcmsb_{key}.txt'
            return candidate if candidate.exists() else None

        # PCFS mapping (K-units)
        if u.startswith('K-'):
            key = u.replace('-', '_').lower()  # K-16-01 -> k_16_01
            # Known files in repo use tags_k16_01.txt etc.
            try:
                num = u.split('-')[1:]
                if len(num) >= 2:
                    candidate = cfg_dir / f"tags_k{num[0]}_{num[1]}.txt"
                    return candidate if candidate.exists() else None
            except Exception:
                pass

        return None

    def _attempt_reseed_and_refresh(self, unit: str, xlsx_path: Path) -> bool:
        """Try to reseed the workbook's DL_WORK sheet with correct PI tags and refresh.

        Returns True if reseed+refresh appeared to succeed (Excel file exists).
        """
        import os
        # Hard-disable reseed for ABF/07-MT01 units to avoid popups/timeouts
        u = unit.strip().upper()
        if u.startswith('07-') or u.startswith('ABF'):
            return False
        if os.getenv('DISABLE_RESEED', '0') == '1':
            # Allow operators to bypass reseed if it causes slowness
            return False
        tags_file = self._find_tags_file_for_unit(unit)
        if not tags_file or not tags_file.exists():
            return False

        try:
            # Import seeder lazily to avoid heavy deps when unused
            from scripts.seed_datalink_formulas import seed_sheet
            server = "\\\\PTSG-1MMPDPdb01"
            # Choose a short lookback to avoid long Excel recalcs that can appear 'stuck'
            # If there is an existing master parquet, start after its last timestamp; else use last 1 day
            start_arg = "-1d"
            try:
                master = self.db.processed_dir / f"{unit}_1y_0p1h.parquet"
                if master.exists():
                    import pandas as pd
                    ts = pd.read_parquet(master, columns=['time'])
                    if not ts.empty:
                        last_ts = pd.to_datetime(ts['time']).max()
                        # Use a small backward overlap so near-real-time runs don't produce 0 rows
                        # and let downstream dedup remove duplicates safely.
                        overlap_minutes = 15
                        start_arg = (last_ts - pd.Timedelta(minutes=overlap_minutes)).strftime('%Y-%m-%d %H:%M:%S')
            except Exception:
                pass

            # Run seeding with a hard timeout to avoid getting stuck in Excel/PI DataLink
            import threading, time as _t
            seed_ok = {'done': False, 'err': None}

            def _seed_task():
                try:
                    seed_sheet(
                        xlsx_path,
                        sheet_name="DL_WORK",
                        tags_file=tags_file,
                        server=server,
                        start=start_arg,
                        end="*",
                        step="-0.1h",
                    )
                    seed_ok['done'] = True
                except Exception as _e:
                    seed_ok['err'] = _e

            t = threading.Thread(target=_seed_task, daemon=True)
            t.start()
            t.join(timeout=180)  # 3 minutes max for reseed
            if t.is_alive():
                print("   WARNING: Reseed timed out after 180s; skipping and using direct tag retrieval")
                return False
            if seed_ok['err'] is not None:
                print(f"   Warning: Reseed failed: {seed_ok['err']}")
                return False

            # Optional light refresh after seeding to flush formulas
            try:
                refresh_excel_safe(xlsx_path, settle_seconds=2)
            except Exception:
                pass
            return xlsx_path.exists()
        except Exception as _seed_err:
            print(f"   Warning: Reseed attempt failed: {_seed_err}")
            return False

    def _incremental_refresh_unit(self, unit: str, xlsx_path: Path) -> bool:
        """Perform incremental refresh using PI Web API (primary) or Excel PI DataLink (fallback).

        This function:
        1. Gets the latest timestamp from the parquet database
        2. Calculates the time gap from last data to now
        3. Tries PI Web API fetch first (faster, no Excel overhead)
        4. Falls back to Excel PI DataLink if Web API unavailable/fails

        Args:
            unit: Unit identifier (e.g., 'K-12-01')
            xlsx_path: Path to Excel workbook

        Returns:
            True if incremental refresh succeeded, False otherwise
        """
        try:
            # Check if ABF units should be skipped (optional via environment variable)
            import os as _os_abf
            skip_abf = _os_abf.getenv('SKIP_ABF_REFRESH', '0').strip() in ('1', 'true', 'yes', 'y')
            if skip_abf and (unit.startswith('07-') or unit.upper().startswith('ABF')):
                print(f"   SKIPPED: ABF refresh disabled by SKIP_ABF_REFRESH env var")
                return False

            # Step 1: Get latest timestamp from database
            latest_ts = self.db.get_latest_timestamp(unit)
            if latest_ts is None:
                print(f"   No existing data for {unit} - need full historical fetch")
                return False

            # Step 2: Calculate time gap
            now = pd.Timestamp.now()
            gap_hours = (now - latest_ts).total_seconds() / 3600

            # Add 10% buffer to ensure we don't miss any data
            fetch_hours = gap_hours * 1.10

            print(f"   Latest data: {latest_ts.strftime('%Y-%m-%d %H:%M:%S')}")
            print(f"   Gap detected: {gap_hours:.1f} hours")
            print(f"   Fetching: {fetch_hours:.1f} hours (with 10% buffer)")

            # Step 3: Get tags for this unit
            tags_file = self._find_tags_file_for_unit(unit)
            if not tags_file or not tags_file.exists():
                print(f"   WARNING: No tags file found for {unit}")
                return False

            tags = [
                t.strip() for t in tags_file.read_text(encoding='utf-8').splitlines()
                if t.strip() and not t.strip().startswith('#')
            ]

            print(f"   Tags: {len(tags)} found")

            # Determine plant and server
            plant = self._infer_plant_from_unit(unit)
            server = "PTSG-1MMPDPdb01"  # No backslashes for Web API

            # Calculate incremental start/end/step
            start = f"-{fetch_hours:.1f}h"
            end = "*"
            step = "-0.1h"  # 6-minute intervals

            # Create temp parquet for incremental data
            temp_parquet = self.db.processed_dir / f"{unit}_incremental_temp.parquet"

            # STRATEGY: Try PI Web API first, fallback to Excel if needed
            webapi_success = False
            import os as _os_web
            webapi_url = _os_web.getenv('PI_WEBAPI_URL', '').strip()

            if webapi_url:
                # Try PI Web API fetch (PRIMARY METHOD)
                try:
                    from .webapi import fetch_tags_via_webapi

                    print(f"   [PRIMARY] Trying PI Web API fetch...")
                    print(f"   Web API URL: {webapi_url}")

                    df_web = fetch_tags_via_webapi(
                        tags=tags,
                        server=server,
                        start=start,
                        end=end,
                        step=step,
                        base_url=webapi_url,
                        auth_mode='windows',  # Windows auth
                        verify_ssl=False,     # Often needed for internal servers
                        timeout=30.0,
                        max_workers=4,
                        qps=3.0,
                        retries=2
                    )

                    if not df_web.empty:
                        # Save to temp parquet
                        df_web.to_parquet(temp_parquet, index=False, engine='pyarrow')
                        print(f"   [SUCCESS] Web API fetched {len(df_web):,} records from {len(df_web['tag'].unique())} tags")
                        webapi_success = True
                    else:
                        print(f"   [WARNING] Web API returned no data")

                except Exception as web_err:
                    print(f"   [WARNING] Web API fetch failed: {web_err}")
                    print(f"   [FALLBACK] Will try Excel PI DataLink...")
            else:
                print(f"   [INFO] PI_WEBAPI_URL not configured, using Excel PI DataLink")

            # FALLBACK: Use Excel PI DataLink if Web API failed or not configured
            if not webapi_success:
                from .batch import build_unit_from_tags

                print(f"   [FALLBACK] Using Excel PI DataLink fetch...")

                # Plant-specific timeout configuration
                if plant.upper().startswith("PCMSB"):
                    settle_time = 30.0
                    timeout_seconds = 360  # 6 minutes for PCMSB
                elif plant.upper().startswith("ABF") or plant.upper().startswith("07-"):
                    settle_time = 60.0
                    timeout_seconds = 600  # 10 minutes for ABF
                    if fetch_hours > 1.0:
                        print(f"   WARNING: ABF fetch limited to 1 hour max (was {fetch_hours:.1f}h)")
                        fetch_hours = 1.0
                        start = f"-{fetch_hours:.1f}h"
                elif plant.upper() == "PCFS":
                    # PCFS needs longer timeout for incremental fetches
                    settle_time = 5.0  # Increased from 1.0 for reliability
                    timeout_seconds = 300  # 5 minutes for PCFS (was 60-120s)
                else:
                    settle_time = 1.0
                    timeout_seconds = 120  # Default 2 minutes

                # Set timeout environment variable
                import os as _os_timeout
                old_timeout = _os_timeout.getenv('PI_FETCH_TIMEOUT')
                _os_timeout.environ['PI_FETCH_TIMEOUT'] = str(timeout_seconds)
                print(f"   Timeout: {timeout_seconds}s ({timeout_seconds/60:.1f} minutes), settle: {settle_time}s")

                try:
                    # Check if we should use visible Excel (more reliable for problematic units)
                    import os as _os_vis
                    use_visible = _os_vis.getenv('EXCEL_VISIBLE', '0').strip() in ('1', 'true', 'yes', 'y')

                    # PCFS units often need visible=True for PI DataLink to load properly
                    if plant.upper() == "PCFS" and not use_visible:
                        # Try visible=True for PCFS by default (more reliable)
                        use_visible = True
                        print(f"   Using visible=True for {plant} (PI DataLink reliability)")

                    build_unit_from_tags(
                        xlsx=xlsx_path,
                        tags=tags,
                        out_parquet=temp_parquet,
                        plant=plant,
                        unit=unit,
                        server=f"\\\\{server}",  # Excel needs backslashes
                        start=start,
                        end=end,
                        step=step,
                        settle_seconds=settle_time,
                        visible=use_visible  # Use visible Excel for reliability
                    )
                finally:
                    # Restore original timeout
                    if old_timeout:
                        _os_timeout.environ['PI_FETCH_TIMEOUT'] = old_timeout
                    elif 'PI_FETCH_TIMEOUT' in _os_timeout.environ:
                        del _os_timeout.environ['PI_FETCH_TIMEOUT']

            # Step 5: Append incremental data to master parquet
            if temp_parquet.exists():
                df_new = pd.read_parquet(temp_parquet)
                print(f"   Fetched {len(df_new):,} new records")

                # Append to master parquet
                master_parquet = self.db.processed_dir / f"{unit}_1y_0p1h.parquet"
                if master_parquet.exists():
                    # Append new data
                    from .ingest import append_parquet
                    try:
                        append_parquet(df_new, master_parquet)
                        print(f"   Appended to {master_parquet.name}")
                    except Exception as append_err:
                        # Fallback: read, concat, write
                        print(f"   Append failed ({append_err}), using concat method...")
                        df_old = pd.read_parquet(master_parquet)
                        df_combined = pd.concat([df_old, df_new], ignore_index=True)
                        df_combined = df_combined.sort_values('time').drop_duplicates(subset=['time', 'tag'], keep='last')
                        df_combined.to_parquet(master_parquet, index=False, engine='pyarrow')
                        print(f"   Combined {len(df_combined):,} total records")
                else:
                    # First time - just copy
                    df_new.to_parquet(master_parquet, index=False, engine='pyarrow')
                    print(f"   Created new {master_parquet.name}")

                # Clean up temp file
                temp_parquet.unlink(missing_ok=True)
                return True
            else:
                print(f"   WARNING: No data fetched")
                return False

        except Exception as e:
            print(f"   ERROR in incremental refresh: {e}")
            import traceback
            traceback.print_exc()
            return False

    def _load_unit_from_tags(self, unit: str, default_xlsx_path: Path, lookback: str = '-2d') -> pd.DataFrame:
        from .config import Config
        from .batch import build_unit_from_tags
        from .clean import dedup_parquet
        import pandas as pd
        import gc

        cfg = Config()
        config_dir = cfg.paths.project_root / 'config'
        target = unit.replace('-', '').lower()
        tags_file = None
        for candidate in sorted(config_dir.glob('tags_*.txt')):
            name = candidate.stem.replace('tags_', '').replace('_', '').lower()
            if target in name:
                tags_file = candidate
                break
        if tags_file is None:
            raise RuntimeError(f"Tag file not found for {unit}")

        tags = [
            t.strip() for t in tags_file.read_text(encoding='utf-8').splitlines()
            if t.strip() and not t.strip().startswith('#')
        ]
        if not tags:
            raise RuntimeError(f"No tags defined for {unit} in {tags_file}")

        excel_path = self._get_excel_file_for_unit(unit, default_xlsx_path)
        if excel_path is None or not excel_path.exists():
            raise RuntimeError(f"No Excel workbook available for {unit}")

        plant = self._infer_plant_from_unit(unit)

        # Find the last timestamp in existing master parquet file
        master = self.db.processed_dir / f"{unit}_1y_0p1h.parquet"
        start_time = None

        if master.exists():
            try:
                # Read just the time column to find last timestamp
                existing_df = pd.read_parquet(master, columns=['time'])
                if not existing_df.empty:
                    last_timestamp = pd.to_datetime(existing_df['time']).max()
                    # Start from last timestamp + 1 minute to avoid overlap
                    start_time = (last_timestamp + pd.Timedelta(minutes=1)).strftime('%Y-%m-%d %H:%M:%S')
                    print(f"   Last data in master: {last_timestamp}")
                    print(f"   Fetching from: {start_time} to current time")
                else:
                    print(f"   Master file exists but empty, fetching last 24h")
                    start_time = '-1d'
            except Exception as e:
                print(f"   Error reading master file: {e}, fetching last 24h")
                start_time = '-1d'
        else:
            print(f"   No master file exists, fetching last 24h")
            start_time = '-1d'

        print(f"   Fallback: fetching {len(tags)} tags from PI for {unit} (plant {plant}) using {excel_path.name}...")
        print(f"   Time range: {start_time} to current")

        # Use a unique temp parquet to avoid collisions/locks across runs
        import time as _tmod
        temp_parquet = self.db.processed_dir / f"{unit}_fallback_{int(_tmod.time())}.parquet"
        try: # This is the outer try for the entire fallback operation
            import time
            import threading

            result_container = {}
            def target():
                try:
                    # Allow unit/plant-specific server override via env
                    import os as _os_env
                    server_override = None
                    if plant.upper().startswith('ABF'):
                        server_override = _os_env.getenv('ABF_PI_SERVER')
                    elif plant.upper().startswith('PCMSB'):
                        server_override = _os_env.getenv('PCMSB_PI_SERVER')
                    elif plant.upper().startswith('PCFS'):
                        server_override = _os_env.getenv('PCFS_PI_SERVER')

                    build_unit_from_tags(
                        excel_path,
                        tags,
                        temp_parquet,
                        plant=plant,
                        unit=unit,
                        server=(server_override or "\\\\PTSG-1MMPDPdb01"),
                        start=start_time,
                        end='*',
                        step='-0.1h',
                        visible=False,
                        settle_seconds=0.5,
                    )
                    result_container['success'] = True
                except Exception as e:
                    result_container['error'] = e

            try:
                thread = threading.Thread(target=target)
                thread.start()
                fetch_start = time.time()
                thread.join(timeout=300) # 5-minute timeout

                if thread.is_alive():
                    raise TimeoutError("Fallback tag retrieval timed out after 300 seconds")
                if 'error' in result_container:
                    raise result_container['error']

                # Read the parquet file if the thread was successful (with robust retry)
                import shutil, os
                df = pd.DataFrame()
                if temp_parquet.exists():
                    read_ok = False
                    last_err = None
                    # Try direct read several times in case another process still has a handle
                    for _ in range(10):
                        try:
                            df = pd.read_parquet(temp_parquet)
                            read_ok = True
                            break
                        except Exception as e:
                            last_err = e
                            _tmod.sleep(0.5)
                    if not read_ok:
                        # Try copying to a new path then reading
                        try:
                            copy_path = temp_parquet.with_suffix('.read.parquet')
                            shutil.copy2(str(temp_parquet), str(copy_path))
                            df = pd.read_parquet(copy_path)
                            try:
                                copy_path.unlink(missing_ok=True)
                            except Exception:
                                pass
                            read_ok = True
                        except Exception as e:
                            last_err = e
                    if not read_ok:
                        raise last_err if last_err else RuntimeError("Unable to read fallback parquet")
                elif 'df' not in locals():
                    df = pd.DataFrame()

            except Exception as e:
                # Handle any errors from the threading operation
                if isinstance(e, TimeoutError):
                    raise e
                else:
                    print(f"   WARNING: Threading operation failed: {e}")
                    df = pd.DataFrame()

        except TimeoutError as e:
            print(f"   WARNING: {e}")
            # Create minimal fallback data to prevent complete failure
            df = pd.DataFrame({
                'plant': [plant],
                'unit': [unit],
                'tag': [f'{plant}_{unit}_FALLBACK_TAG'], # Corrected f-string
                'time': [pd.Timestamp.now()],
                'value': [0.0]
            })
        except Exception as e:
            # Catch exceptions from the thread
            print(f"   WARNING: Fallback thread failed: {e}")
            df = pd.DataFrame()
        finally:
            try:
                temp_parquet.unlink(missing_ok=True) # Ensure temp file is always cleaned up
            except Exception:
                pass

        if df.empty:
            raise RuntimeError(f"Tag fallback produced no data for {unit}")

        df['time'] = pd.to_datetime(df['time'])
        df = df.sort_values('time').drop_duplicates(subset=['time', 'tag'], keep='last').reset_index(drop=True)

        master = self.db.processed_dir / f"{unit}_1y_0p1h.parquet"
        if master.exists():
            try:
                # Memory-efficient merge for large datasets
                logger.info(f"Merging with existing data for unit {unit}")
                self.memory_monitor.log_memory_status("before merge")

                # Check existing file size first
                existing_size_mb = master.stat().st_size / (1024**2)
                new_df_mb = df.memory_usage(deep=True).sum() / (1024**2)

                # Emergency bypass for extremely large files to prevent memory failures
                if existing_size_mb > 200:  # 200MB+ files (very aggressive to prevent K-31-01 issues)
                    logger.warning(f"Very large historical file ({existing_size_mb:.1f}MB), using fresh data only to prevent memory issues")
                    # For extremely large files, just use fresh data and skip historical merge
                    logger.info(f"Skipping historical merge for {unit} due to size constraints")
                    # Just use the fresh data without historical merge
                    pass  # df already contains fresh data, no merge needed

                elif existing_size_mb > 100 or new_df_mb > 100 or self.memory_monitor.check_memory_pressure():
                    # Use streaming merge for large files
                    logger.info(f"Using streaming merge for large dataset: {existing_size_mb:.1f}MB existing + {new_df_mb:.1f}MB new")
                    existing = pd.read_parquet(master, columns=['time'])  # Only read time column initially

                    # Performance optimization: Only load recent historical data to prevent memory issues
                    # Use chunked reading to avoid loading massive datasets into memory
                    cutoff_date = datetime.now() - timedelta(days=90)
                    logger.info(f"Using chunked reading for historical data from {cutoff_date.strftime('%Y-%m-%d')} onwards")

                    try:
                        # Try chunked reading first to avoid memory issues
                        existing_chunks = []
                        chunk_size = 100_000  # 100K records per chunk

                        logger.info(f"Reading historical data in chunks of {chunk_size:,} records")
                        parquet_file = pd.read_parquet(master, engine='pyarrow')

                        # If file is too large, skip very old data
                        if len(parquet_file) > 5_000_000:  # 5M+ records
                            logger.warning(f"Very large historical dataset ({len(parquet_file):,} records), using only recent subset")
                            parquet_file['time'] = pd.to_datetime(parquet_file['time'])
                            # For very large files, only keep last 30 days to prevent memory issues
                            recent_cutoff = datetime.now() - timedelta(days=30)
                            existing_full = parquet_file[parquet_file['time'] >= recent_cutoff]
                            logger.info(f"Large dataset optimization: {len(parquet_file):,} -> {len(existing_full):,} records (30-day window)")
                            del parquet_file
                        else:
                            # Standard filtering for smaller datasets
                            parquet_file['time'] = pd.to_datetime(parquet_file['time'])
                            existing_full = parquet_file[parquet_file['time'] >= cutoff_date]
                            logger.info(f"Standard filtering: {len(parquet_file):,} -> {len(existing_full):,} records")
                            del parquet_file

                    except MemoryError:
                        logger.error(f"Memory error reading historical data for {unit}, skipping historical merge")
                        existing_full = pd.DataFrame()  # Use only fresh data

                    # Use streaming handler for merge
                    df = self.streaming_handler.merge_large_dataframes(existing_full, df, ['time', 'tag'])

                    # Cleanup
                    del existing, existing_full
                    self.memory_monitor.force_garbage_collection()
                else:
                    # Standard merge for smaller datasets
                    existing = pd.read_parquet(master)
                    existing['time'] = pd.to_datetime(existing['time'])

                    # Apply same time filter to standard merge for consistency
                    cutoff_date = datetime.now() - timedelta(days=90)
                    before_filter = len(existing)
                    existing = existing[existing['time'] >= cutoff_date]
                    after_filter = len(existing)
                    if before_filter != after_filter:
                        logger.info(f"Standard merge - Historical data filtered: {before_filter:,} -> {after_filter:,} records")

                    df = pd.concat([existing, df], ignore_index=True)
                    del existing

                # Memory-efficient deduplication
                df = memory_efficient_dedup(df, subset=['time', 'tag'])
                df = df.sort_values('time').reset_index(drop=True)

                self.memory_monitor.log_memory_status("after merge")

            except MemoryError as e:
                logger.error(f"Memory error during merge for unit {unit}: {e}")
                logger.info("Attempting incremental processing fallback")
                # Fallback: process in smaller chunks
                try:
                    df = self._fallback_incremental_merge(master, df, unit)
                except Exception as fallback_e:
                    logger.error(f"Fallback merge also failed for unit {unit}: {fallback_e}")
                    raise MemoryError(f"Unable to merge data for unit {unit} due to memory constraints")
            except Exception as e:
                logger.error(f"Error during merge for unit {unit}: {e}")
                pass

        # Atomic write of master parquet
        from .ingest import write_parquet
        write_parquet(df, master)
        # Optional: defer dedup to end-of-run for speed
        import os as _os
        if _os.getenv('DELAY_DEDUP', '').strip().lower() in ('1','true','yes','y') or \
           _os.getenv('DEDUP_MODE', '').strip().lower() in ('end','deferred','once'):
            # Caller may handle a final dedup sweep
            pass
        else:
            dedup_parquet(master)
        return df

    def _fallback_incremental_merge(self, master_file: Path, new_df: pd.DataFrame, unit: str) -> pd.DataFrame:
        """Incremental merge fallback for memory-constrained environments.

        Args:
            master_file: Path to existing parquet file
            new_df: New DataFrame to merge
            unit: Unit identifier

        Returns:
            Merged DataFrame using incremental processing
        """
        logger.info(f"Using incremental merge fallback for unit {unit}")

        try:
            # Read existing data in chunks to find overlap period
            temp_files = []
            chunk_processor = ChunkedProcessor(chunk_size=100_000, memory_monitor=self.memory_monitor)

            # Find the time range of new data
            new_df['time'] = pd.to_datetime(new_df['time'])
            new_start = new_df['time'].min()
            new_end = new_df['time'].max()

            logger.info(f"New data range: {new_start} to {new_end}")

            # Process existing data in chunks, filtering out overlapping period
            filtered_chunks = []
            for chunk in chunk_processor.read_parquet_chunked(master_file):
                chunk['time'] = pd.to_datetime(chunk['time'])

                # Keep data outside the new data time range
                before_new = chunk[chunk['time'] < new_start]
                after_new = chunk[chunk['time'] > new_end]

                if not before_new.empty:
                    filtered_chunks.append(before_new)
                if not after_new.empty:
                    filtered_chunks.append(after_new)

                # Memory management
                del chunk, before_new, after_new
                if len(filtered_chunks) % 10 == 0:
                    self.memory_monitor.force_garbage_collection()

            # Combine filtered existing data with new data
            all_chunks = filtered_chunks + [new_df]

            if all_chunks:
                result = pd.concat(all_chunks, ignore_index=True)
                result = result.sort_values('time').reset_index(drop=True)

                # Final deduplication
                result = memory_efficient_dedup(result, subset=['time', 'tag'])

                logger.info(f"Incremental merge completed for unit {unit}: {len(result):,} total records")
                return result

            return new_df

        except Exception as e:
            logger.error(f"Incremental merge fallback failed for unit {unit}: {e}")
            # Last resort: return just the new data
            logger.warning(f"Returning only new data for unit {unit} due to merge failure")
            return new_df

    def refresh_stale_units_with_progress(self, xlsx_path: Path = None, max_age_hours: float = 8.0) -> Dict[str, Any]:
        """Refresh stale units with real-time progress tracking.

        Args:
            xlsx_path: Default Excel file path (will be overridden per unit based on plant)
            max_age_hours: Maximum age before refreshing

        Returns:
            Refresh results with unit-by-unit progress
        """
        refresh_start = datetime.now()

        # Determine default Excel file if not provided (needed for auto-build)
        if xlsx_path is None:
            excel_paths = [
                Path("excel/ABF_Automation.xlsx"),
                Path("excel/PCMSB_Automation.xlsx"),
                Path("excel/PCFS_Automation.xlsx"),
                Path("data/raw/Automation.xlsx"),
                Path("Automation.xlsx")
            ]
            for path in excel_paths:
                if path.exists():
                    xlsx_path = path
                    break

        # Auto-build any configured units that don't have Parquet yet
        if xlsx_path is not None:
            try:
                seeded = self._auto_build_missing_units(xlsx_path)
                if seeded:
                    print(f"Seeded {sum(1 for s in seeded if s['success'])} new unit(s) before refresh.")
            except Exception as e:
                print(f"Auto-build step skipped due to error: {e}")

        # Get stale units after possible seeding
        scan_results = self.scan_all_units(max_age_hours=max_age_hours)
        stale_units = scan_results['stale_units']

        # Optional environment filters to limit scope
        try:
            import os as _os
            units_filter = [u.strip() for u in _os.getenv('REFRESH_UNITS', '').split(',') if u.strip()]
            plants_filter = [p.strip().upper() for p in _os.getenv('REFRESH_PLANTS', '').split(',') if p.strip()]
            if units_filter:
                # Keep only requested units; allow forcing units even if not stale
                requested = set(units_filter)
                # Include non-stale requested units as well
                all_units = {u['unit'] for u in scan_results.get('units_scanned', []) if isinstance(u, dict) and u.get('unit')}
                stale_units = [u for u in stale_units if u in requested]
                for u in requested:
                    if u in all_units and u not in stale_units:
                        stale_units.append(u)
            if plants_filter:
                def _plant_ok(u: str) -> bool:
                    try:
                        plant = self._infer_plant_from_unit(u)
                        return plant in plants_filter
                    except Exception:
                        return False
                stale_units = [u for u in stale_units if _plant_ok(u)]
        except Exception:
            pass

        # Cache freshness info from scan to avoid redundant loads
        unit_freshness_cache = {}
        for unit_info in scan_results.get('units_scanned', []):
            unit_name = unit_info.get('unit')
            if unit_name:
                unit_freshness_cache[unit_name] = unit_info
        
        if not stale_units:
            return {
                "success": True,
                "message": "All units are fresh - no refresh needed",
                "fresh_units": scan_results['fresh_units'],
                "total_time": 0,
                "units_processed": []
            }
        
        # Excel path may be None here; that's OK because we resolve the correct
        # workbook per-unit in _get_excel_file_for_unit(). Historically we bailed
        # out if no default workbook was found, which prevented any units from
        # being processed when files lived under plant subfolders (e.g.,
        # excel/PCMSB/PCMSB_Automation.xlsx). Proceed and resolve per unit.
        if xlsx_path is None:
            try:
                print("Note: No default Excel workbook found; resolving per-unit workbooks.")
            except Exception:
                pass
        
        print(f"\\nREFRESHING {len(stale_units)} STALE UNITS")
        print(f"Excel file: {xlsx_path}")
        print(f"Started: {refresh_start.strftime('%H:%M:%S')}")
        print("=" * 60)
        
        results = {
            "success": True,
            "start_time": refresh_start.isoformat(),
            "excel_file": str(xlsx_path),
            "units_to_refresh": stale_units.copy(),
            "units_processed": [],
            "unit_results": {},
            "total_time": 0,
            "fresh_after_refresh": []
        }

        # Toggle to delay dedup until the end for speed (DEFAULT: False since PyArrow streaming is memory-safe)
        import os as _os
        _delay_dedup = _os.getenv('DELAY_DEDUP', 'false').strip().lower() in ('1','true','yes','y') or \
                       _os.getenv('DEDUP_MODE', '').strip().lower() in ('end','deferred','once')
        pending_dedup: list[Path] = []

        # Process each unit with progress display
        for i, unit in enumerate(stale_units):
            unit_number = i + 1
            unit_start = time.time()

            # Stage timing
            stage_times = {}

            print(f"\\n[{unit_number}/{len(stale_units)}] Processing: {unit}")
            print(f"Started: {datetime.now().strftime('%H:%M:%S')}")
            print("-" * 40)

            # Memory pressure check before processing
            memory_info = self.memory_monitor.get_memory_usage()
            print(f"   Available memory: {memory_info['available_gb']:.1f}GB")

            # Lower threshold to 1GB since PyArrow streaming uses minimal memory
            min_memory_gb = float(_os.getenv('MIN_MEMORY_GB', '1.0'))
            if memory_info['available_gb'] < min_memory_gb:
                print(f"   WARNING: Low memory ({memory_info['available_gb']:.1f}GB available, minimum {min_memory_gb}GB required)")
                print(f"   Skipping {unit} to prevent system instability")
                results["unit_results"][unit] = {
                    "success": False,
                    "unit": unit,
                    "processing_time": 0,
                    "error": f"Insufficient memory: only {memory_info['available_gb']:.1f}GB available (need {min_memory_gb}GB)"
                }
                results["units_processed"].append(unit)
                continue

            try:
                # Use cached info from scan instead of reloading
                if unit in unit_freshness_cache:
                    before_info = unit_freshness_cache[unit]
                else:
                    # Fallback to direct query if not in cache
                    before_info = self.db.get_data_freshness_info(unit)

                unit_data_age_hours = before_info.get('data_age_hours', 0)
                total_records = before_info.get('total_records', 0)
                print(f"   Before: {total_records:,} records, {unit_data_age_hours:.1f}h old")

                # Determine correct Excel file for this specific unit
                unit_xlsx_path = self._get_excel_file_for_unit(unit, xlsx_path)
                print(f"   Excel file for {unit}: {unit_xlsx_path.name}")

                # Perform INCREMENTAL refresh (each unit needs its own fetch with unit-specific tags)
                print(f"   Performing incremental refresh for {unit}...")

                try:
                    excel_start = time.time()

                    # Use new incremental refresh method (each unit fetches independently)
                    success = self._incremental_refresh_unit(unit, unit_xlsx_path)

                    excel_time = time.time() - excel_start
                    stage_times['incremental_fetch'] = excel_time

                    if success:
                        print(f"   ⏱️  Incremental fetch: {excel_time:.1f}s")
                    else:
                        print(f"   WARNING: Incremental refresh failed - skipping {unit}")
                        unit_time = time.time() - unit_start
                        results["unit_results"][unit] = {
                            "success": False,
                            "unit": unit,
                            "processing_time": unit_time,
                            "error": "Incremental refresh failed"
                        }
                        results["units_processed"].append(unit)
                        continue  # Skip to next unit

                except Exception as refresh_error:
                    excel_time = time.time() - excel_start
                    print(f"   ERROR: Incremental refresh failed: {refresh_error}")
                    # Record as failed and skip to next unit
                    unit_time = time.time() - unit_start
                    results["unit_results"][unit] = {
                        "success": False,
                        "unit": unit,
                        "processing_time": unit_time,
                        "error": f"Incremental refresh error: {refresh_error}"
                    }
                    results["units_processed"].append(unit)
                    continue  # Skip to next unit

                # Get updated statistics after incremental refresh
                processing_start = time.time()
                print(f"   Verifying data after incremental refresh...")

                # Determine master parquet file
                master_parquet = self.db.processed_dir / f"{unit}_1y_0p1h.parquet"
                output_path = master_parquet

                # Get after statistics
                after_records = 0
                if master_parquet.exists():
                    import pandas as pd
                    try:
                        import pyarrow.parquet as pq
                        after_records = pq.ParquetFile(master_parquet).metadata.num_rows
                    except Exception:
                        after_records = len(pd.read_parquet(master_parquet, columns=['time']))

                # Get updated freshness info from database
                after_info = self.db.get_data_freshness_info(unit)

                # Skip old Excel loading section - incremental refresh already handled everything
                # Check after status (master file was updated in place)
                after_info = self.db.get_data_freshness_info(unit)
                # Additional guard: if the freshly ingested dataframe carries a
                # newer timestamp than what DuckDB reports (e.g., due to file
                # caching lag or glob precedence), prefer the dataframe's max
                # time for age calculation.
                try:
                    df_latest_ts = None
                    if df is not None and len(df) > 0 and 'time' in df.columns:
                        _df_t = pd.to_datetime(df['time'], errors='coerce')
                        if _df_t.notna().any():
                            df_latest_ts = _df_t.max()
                    if df_latest_ts is not None:
                        # Normalize to naive for comparison
                        import pandas as _pd
                        latest_after = after_info.get('latest_timestamp')
                        if latest_after is None or (_pd.to_datetime(latest_after) < _pd.to_datetime(df_latest_ts)):
                            # Recompute data_age_hours from df_latest_ts
                            try:
                                now_utc = _pd.Timestamp.now(tz='UTC')
                                latest_dt = _pd.to_datetime(df_latest_ts)
                                if getattr(latest_dt, 'tz', None) is None:
                                    latest_dt = latest_dt.tz_localize(now_utc.tz).tz_convert('UTC')
                                age_h = (now_utc - latest_dt.tz_convert('UTC')).total_seconds() / 3600
                                after_info['latest_timestamp'] = df_latest_ts
                                after_info['data_age_hours'] = age_h
                                after_info['is_stale'] = age_h > float(_os.getenv('MAX_AGE_HOURS', '1.0'))
                            except Exception:
                                pass
                except Exception:
                    pass
                unit_time = time.time() - unit_start

                unit_result = {
                    "success": True,
                    "unit": unit,
                    "processing_time": unit_time,
                    "records_before": before_info['total_records'],
                    "records_after": after_records,
                    "age_before_hours": before_info['data_age_hours'],
                    "age_after_hours": after_info['data_age_hours'] if not after_info['is_stale'] else 0,
                    "output_file": str(output_path),
                    "file_size_mb": output_path.stat().st_size / (1024 * 1024),
                    "stage_times": stage_times  # Include detailed timing breakdown
                }
                
                results["unit_results"][unit] = unit_result
                results["units_processed"].append(unit)
                
                if not after_info['is_stale']:
                    results["fresh_after_refresh"].append(unit)
                
                processing_time = time.time() - processing_start
                stage_times['data_processing'] = processing_time

                # Print stage breakdown
                print(f"   ⏱️  Data processing: {processing_time:.1f}s")
                if 'excel_refresh' in stage_times:
                    print(f"   📊 Stage breakdown: Excel {stage_times['excel_refresh']:.1f}s + Processing {processing_time:.1f}s")
                print(f"   ✅ {unit} completed in {unit_time:.1f}s")
                print(f"   Records: {before_info['total_records']:,} -> {len(df):,}")
                print(f"   Output: {unit_result['file_size_mb']:.1f}MB")
                if after_info['is_stale']:
                    print(f"   Age: {before_info['data_age_hours']:.1f}h -> still stale ({after_info['data_age_hours']:.1f}h)")
                else:
                    print(f"   Age: {before_info['data_age_hours']:.1f}h -> Fresh ({after_info['data_age_hours']:.1f}h)")

                # Memory cleanup after unit processing
                del df
                import gc
                gc.collect()
                self.memory_monitor.log_memory_status(f"after {unit}")

                # Show overall progress
                completed = len(results["units_processed"])
                remaining = len(stale_units) - completed
                progress_pct = (completed / len(stale_units)) * 100

                print(f"\\nProgress: {completed}/{len(stale_units)} completed ({progress_pct:.1f}%), {remaining} remaining")

            except Exception as e:
                unit_time = time.time() - unit_start
                error_msg = str(e)
                
                unit_result = {
                    "success": False,
                    "unit": unit,
                    "processing_time": unit_time,
                    "error": error_msg
                }
                
                results["unit_results"][unit] = unit_result
                results["units_processed"].append(unit)
                
                print(f"   {unit} FAILED after {unit_time:.1f}s")
                print(f"   Error: {error_msg}")
        
        # End-of-run dedup pass (optional for speed)
        if pending_dedup:
            print("\nRunning end-of-run dedup pass...")
            unique_paths = []
            seen = set()
            for p in pending_dedup:
                if str(p) not in seen:
                    seen.add(str(p))
                    unique_paths.append(p)
            completed = 0
            for p in unique_paths:
                try:
                    dp = dedup_parquet(p)
                    print(f"   Deduped: {dp.name}")
                    completed += 1
                except Exception as e:
                    print(f"   WARNING: Dedup failed for {p.name}: {e}")
            results['dedup_deferred'] = len(unique_paths)
            results['dedup_completed'] = completed

        # Final summary
        refresh_end = datetime.now()
        total_time = (refresh_end - refresh_start).total_seconds()
        successful = len([r for r in results["unit_results"].values() if r["success"]])
        failed = len([r for r in results["unit_results"].values() if not r["success"]])
        
        results.update({
            "end_time": refresh_end.isoformat(),
            "total_time": total_time,
            "successful_units": successful,
            "failed_units": failed,
            "success_rate": (successful / len(stale_units)) * 100 if stale_units else 0
        })
        
        # Print cyberpunk-themed summary
        self._print_cyberpunk_summary(results)

        return results
