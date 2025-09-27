#!/usr/bin/env python3
"""
TURBOPREDICT X PROTEAN - Unified Real Data System
Single entry point for all functionality with real Parquet data integration
"""

import os
import sys
import time
from pathlib import Path
from datetime import datetime, timedelta
import pandas as pd

# Add current directory to Python path
current_dir = Path(__file__).parent

# Ensure AE + 2.5-sigma primary analysis; require AE before verification by default.
import os as _os_patch
_os_patch.environ.setdefault('REQUIRE_AE','1')
_os_patch.environ.setdefault('ENABLE_AE_LIVE', _os_patch.getenv('ENABLE_AE_LIVE','1'))
# Rich imports for beautiful interface
try:
    from rich.console import Console
    from rich.panel import Panel
    from rich.prompt import Prompt, Confirm
    from rich.table import Table
    from rich.progress import Progress, SpinnerColumn, TextColumn, BarColumn
    from rich import box
    RICH_AVAILABLE = False  # Disable Rich to avoid Windows Unicode issues
except ImportError:
    RICH_AVAILABLE = False

# Colorama fallback
try:
    from colorama import init, Fore, Back, Style
    init(autoreset=True)
    COLORAMA_AVAILABLE = True
except ImportError:
    COLORAMA_AVAILABLE = False

# Import real data modules
try:
    from pi_monitor.parquet_database import ParquetDatabase
    from pi_monitor.parquet_auto_scan import ParquetAutoScanner
    from pi_monitor.config import Config
    from pi_monitor.cli import main as original_cli_main
    DATA_MODULES_AVAILABLE = True
except ImportError as e:
    print(f"Warning: Data modules not available: {e}")
    DATA_MODULES_AVAILABLE = False


class TurbopredictSystem:
    """Unified TURBOPREDICT X PROTEAN system"""
    
    def __init__(self):
        """Initialize the unified system"""
        self.console = self._setup_console()
        self.data_available = False
        
        # Initialize data systems if available
        if DATA_MODULES_AVAILABLE:
            try:
                self.config = Config()
                self.db = ParquetDatabase()
                self.scanner = ParquetAutoScanner(self.config)
                self.data_available = True
                print("Real data systems initialized successfully")
            except Exception as e:
                print(f"Warning: Data systems failed to initialize: {e}")
                self.data_available = False
        
    def _setup_console(self):
        """Setup Rich console with fallback"""
        if RICH_AVAILABLE:
            try:
                return Console(force_terminal=True, width=80)
            except:
                return None
        return None
    
    def get_system_banner(self):
        """Get the unified system banner"""
        return """
+========================================================================+
|  TURBOPREDICT X PROTEAN - UNIFIED NEURAL INTERFACE                    |
|                                                                        |
|  TTTTT U   U RRRR  BBBB   OOO  PPPP  RRRR  EEEEE DDDD  III  CCCC TTTTT|
|    T   U   U R   R B   B O   O P   P R   R E     D   D  I  C   C  T   |
|    T   U   U RRRR  BBBB  O   O PPPP  RRRR  EEEE  D   D  I  C      T   |
|    T   U   U R  R  B   B O   O P     R  R  E     D   D  I  C   C  T   |
|    T    UUU  R   R BBBB   OOO  P     R   R EEEEE DDDD  III  CCCC  T   |
|                                                                        |
|               >>> UNIFIED QUANTUM NEURAL MATRIX <<<                   |
|          >>> REAL DATA + INTELLIGENT AUTO-SCAN SYSTEM <<<             |
+========================================================================+
        """
    
    def display_startup_banner(self):
        """Display startup banner with system status"""
        if self.console:
            try:
                # System status
                data_status = "ONLINE - REAL DATA CONNECTED" if self.data_available else "OFFLINE - NO DATA CONNECTION"
                status_style = "bright_green" if self.data_available else "bright_red"
                
                banner_panel = Panel(
                    self.get_system_banner(),
                    title="[bold cyan]TURBOPREDICT X PROTEAN INITIALIZATION[/]",
                    subtitle=f"[{status_style}]>>> {data_status} <<<[/]",
                    style="bright_blue"
                )
                
                self.console.clear()
                self.console.print(banner_panel)
                
                # System loading animation
                with Progress(
                    SpinnerColumn(),
                    TextColumn("Initializing quantum neural matrix..."),
                    transient=True,
                    console=self.console
                ) as progress:
                    task = progress.add_task("", total=50)
                    for i in range(50):
                        time.sleep(0.02)
                        progress.update(task, advance=1)
                
                # Show system capabilities
                status_text = ">>> NEURAL MATRIX ONLINE <<<" if self.data_available else ">>> LIMITED MODE ACTIVE <<<"
                style = "bold green" if self.data_available else "bold yellow"
                self.console.print(f"[{style}]{status_text}[/]")
                
                # Data summary if available
                if self.data_available:
                    try:
                        db_status = self.db.get_database_status()
                        self.console.print(f"[dim cyan]Data: {db_status['total_files']} files, "
                                         f"{db_status['total_size_gb']:.1f}GB, "
                                         f"{len(db_status['units'])} units available[/]")
                    except:
                        pass
                
                self.console.print("")
                return
                
            except Exception as e:
                print(f"Rich display failed: {e}")
        
        # Fallback display
        self._display_fallback_banner()
    
    def _display_fallback_banner(self):
        """Fallback banner display"""
        os.system('cls' if os.name == 'nt' else 'clear')
        
        if COLORAMA_AVAILABLE:
            print(Fore.CYAN + Style.BRIGHT + self.get_system_banner())
            if self.data_available:
                print(Fore.GREEN + ">>> NEURAL MATRIX ONLINE <<<")
            else:
                print(Fore.YELLOW + ">>> LIMITED MODE ACTIVE <<<")
            print(Style.RESET_ALL)
        else:
            print(self.get_system_banner())
            print(">>> NEURAL MATRIX ONLINE <<<" if self.data_available else ">>> LIMITED MODE ACTIVE <<<")
    
    def show_main_menu(self):
        """Display unified main menu"""
        if self.console:
            try:
                menu_table = Table(title="[bold magenta]>>> TURBOPREDICT X PROTEAN NEURAL MATRIX <<<[/]")
                menu_table.add_column("CMD", style="bold cyan", width=5)
                menu_table.add_column("System", style="bold green", width=25)
                menu_table.add_column("Description", style="yellow", width=40)
                
                # Real data options (always shown)
                menu_options = [
                    ("1", "AUTO-REFRESH SCAN", "Auto-update stale data (no prompt)"),
                    ("2", "UNIT DEEP ANALYSIS", "Smart anomaly detection with auto-triggered plots"),
                    ("3", "HOURLY AUTO LOOP", "Every hour: [1] then [2], until Ctrl+C"),
                    ("4", "AUTO-SCAN SYSTEM", "Intelligent freshness-based scanning"),
                    ("5", "DATA QUALITY AUDIT", "Comprehensive quality analysis"),
                    ("6", "UNIT EXPLORER", "Browse and explore all units"),
                    ("7", "ORIGINAL CLI", "Access original command interface"),
                    ("8", "SYSTEM DIAGNOSTICS", "Neural matrix health check"),
                    ("9", "CONDITIONAL PLOTTING", "Smart plots: only if data + minimal change marks"),
                    ("A", "TAG STATE DASHBOARD", "Comprehensive tag health monitoring"),
                    ("B", "INCIDENT REPORTER", "WHO-WHAT-WHEN-WHERE detailed reports"),
                    ("0", "NEURAL DISCONNECT", "Terminate all connections")
                ]
                
                for cmd, system, desc in menu_options:
                    # Mark unavailable options if no data
                    if not self.data_available and cmd in ["1", "2", "3", "4", "5", "6", "9", "A", "B"]:
                        menu_table.add_row(cmd, f"[dim]{system}[/]", f"[dim red]{desc} (DATA OFFLINE)[/]")
                    else:
                        menu_table.add_row(cmd, system, desc)
                
                self.console.print(menu_table)
                
                if not self.data_available:
                    self.console.print(Panel(
                        "[bold red]âš  DATA SYSTEMS OFFLINE âš [/]\n"
                        "Real data features unavailable.\n"
                        "Options 1-6 require data connection.",
                        title="[bold red]WARNING[/]",
                        border_style="red"
                    ))
                
                choice = Prompt.ask(
                    "[bold magenta]>>> SELECT NEURAL PATHWAY[/]",
                    choices=["0","1","2","3","4","5","6","7","8","9","A","B"],
                    default="3" if self.data_available else "7",
                    console=self.console
                )
                return choice
                
            except Exception:
                pass
        
        # Fallback menu
        return self._show_fallback_menu()
    
    def _show_fallback_menu(self):
        """Fallback text menu"""
        menu = """
+================================================================+
|         TURBOPREDICT X PROTEAN NEURAL COMMAND MATRIX          |
+================================================================+
| 1. AUTO-REFRESH SCAN    - Auto-update stale data (no prompt) |
| 2. UNIT DEEP ANALYSIS   - Smart anomaly detection + plots    |
| 3. HOURLY AUTO LOOP     - Every hour: [1] then [2]           |
| 4. AUTO-SCAN SYSTEM     - Intelligent scanning system        |
| 5. DATA QUALITY AUDIT   - Quality analysis reports           |
| 6. UNIT EXPLORER        - Browse all available units         |
| 7. ORIGINAL CLI         - Access original command interface   |
| 8. SYSTEM DIAGNOSTICS   - System health check                |
+----------------------------------------------------------------+
| ENHANCED ANALYSIS SUITE                                       |
+----------------------------------------------------------------+
| 9. CONDITIONAL PLOTTING - Smart plots + minimal change marks |
| A. TAG STATE DASHBOARD  - Comprehensive tag health monitor   |
| B. INCIDENT REPORTER    - WHO-WHAT-WHEN-WHERE reports        |
+----------------------------------------------------------------+
| ANOMALY DIAGNOSTIC PLOTS                                      |
+----------------------------------------------------------------+
| C. AUTO-PLOT STATUS     - Show anomaly-triggered plot status  |
| D. CLEANUP REPORTS      - Clean old reports and reclaim space|
+================================================================+
| 0. NEURAL DISCONNECT    - Exit system                        |
+================================================================+
        """
        
        if COLORAMA_AVAILABLE:
            print(Fore.CYAN + menu + Style.RESET_ALL)
            if not self.data_available:
                print(Fore.RED + "WARNING: Data systems offline (options 1-6 limited)" + Style.RESET_ALL)
            try:
                choice = input(Fore.MAGENTA + ">>> SELECT PATHWAY: " + Style.RESET_ALL)
            except EOFError:
                return "0"  # Exit gracefully
        else:
            print(menu)
            if not self.data_available:
                print("WARNING: Data systems offline")
            try:
                choice = input(">>> SELECT PATHWAY: ")
            except EOFError:
                return "0"  # Exit gracefully
        
        return choice.strip()
    
    def execute_option(self, choice):
        """Execute the selected menu option"""
        if choice == "0":
            self.shutdown_system()
            return False
            
        elif choice == "1":
            self.run_real_data_scanner(auto_refresh=True)
            
        elif choice == "2":
            self.run_unit_analysis()
            
        elif choice == "3":
            self.run_hourly_auto_loop()
            
        elif choice == "4":
            self.run_auto_scan_system()
            
        elif choice == "5":
            self.run_quality_audit()
            
        elif choice == "6":
            self.run_unit_explorer()
            
        elif choice == "7":
            self.launch_original_cli()
            
        elif choice == "8":
            self.run_system_diagnostics()

        elif choice == "9":
            self.run_conditional_plotting()

        elif choice.upper() == "A":
            self.run_tag_state_dashboard()

        elif choice.upper() == "B":
            self.run_incident_reporter()

        elif choice.upper() == "C":
            self.run_controlled_plots()

        elif choice.upper() == "D":
            self.cleanup_reports()

        else:
            self._show_invalid_choice()

        return True
    
    def run_real_data_scanner(self, auto_refresh=False):
        """Run the real data scanner with PI DataLink fetching capability
        
        Args:
            auto_refresh: If True, automatically refresh stale data without prompting
        """
        if not self._check_data_available():
            return
            
        try:
            if COLORAMA_AVAILABLE:
                print(Fore.GREEN + ">>> INITIATING REAL DATA SCAN <<<" + Style.RESET_ALL)
                print(Fore.CYAN + "Scanning Parquet neural matrix..." + Style.RESET_ALL)
            else:
                print(">>> INITIATING REAL DATA SCAN <<<")
                print("Scanning Parquet neural matrix...")
            
            # Get all units from database
            units = self.db.get_all_units()
            
            if COLORAMA_AVAILABLE:
                print(Fore.YELLOW + f"Found {len(units)} units in database: {', '.join(units)}" + Style.RESET_ALL)
                print(Fore.CYAN + "Analyzing data freshness..." + Style.RESET_ALL)
            else:
                print(f"Found {len(units)} units in database: {', '.join(units)}")
                print("Analyzing data freshness...")
            
            # Use consistent max-age threshold across scan and refresh
            try:
                max_age_hours = float(getattr(self, 'config', None).max_age_hours) if hasattr(self, 'config') else float(os.getenv('MAX_AGE_HOURS', '1.0'))
            except Exception:
                max_age_hours = 1.0

            # Scan each unit for data freshness
            scan_results = []
            stale_units = []
            
            for unit in units:
                freshness_info = self.db.get_data_freshness_info(unit)
                # Determine stale consistently using configured threshold
                is_stale = (freshness_info['data_age_hours'] is not None and freshness_info['data_age_hours'] > max_age_hours)

                result = {
                    'unit': unit,
                    'records': freshness_info['total_records'],
                    'latest_data': freshness_info['latest_timestamp'],
                    'data_age_hours': freshness_info['data_age_hours'],
                    'is_stale': is_stale,
                    'unique_tags': len(freshness_info['unique_tags'])
                }
                scan_results.append(result)
                
                # Track stale units for potential PI DataLink refresh
                if result['is_stale'] and result['records'] > 0:  # Only consider units with existing data
                    stale_units.append(unit)
            
            # Display initial scan results
            print()
            if COLORAMA_AVAILABLE:
                print(Fore.GREEN + "=" * 80)
                print("                         REAL DATA SCAN RESULTS")
                print("=" * 80 + Style.RESET_ALL)
            else:
                print("=" * 80)
                print("                         REAL DATA SCAN RESULTS") 
                print("=" * 80)
            
            print(f"{'Unit':<12} {'Records':<10} {'Age (hrs)':<10} {'Status':<12} {'Tags':<8}")
            print("-" * 60)
            
            for result in scan_results:
                unit = result['unit']
                records = f"{result['records']:,}" if result['records'] else "0"
                age = f"{result['data_age_hours']:.1f}" if result['data_age_hours'] else "N/A"
                status = "STALE" if result['is_stale'] else "FRESH"
                tags = str(result['unique_tags'])
                
                if COLORAMA_AVAILABLE:
                    status_color = Fore.RED if result['is_stale'] else Fore.GREEN
                    print(f"{unit:<12} {records:<10} {age:<10} {status_color}{status:<12}{Style.RESET_ALL} {tags:<8}")
                else:
                    print(f"{unit:<12} {records:<10} {age:<10} {status:<12} {tags:<8}")
            
            if COLORAMA_AVAILABLE:
                print(Fore.GREEN + "=" * 80 + Style.RESET_ALL)
            else:
                print("=" * 80)
            
            # Check for stale data and offer PI DataLink refresh
            main_stale_units = [u for u in stale_units if u.startswith('K-') or u.startswith('07-MT01-')]  # Focus on main K-units and ABF units
            if main_stale_units:
                print()
                if COLORAMA_AVAILABLE:
                    print(Fore.YELLOW + f">>> DETECTED {len(main_stale_units)} STALE MAIN UNITS <<<" + Style.RESET_ALL)
                    print(Fore.CYAN + f"Stale units: {', '.join(main_stale_units)}" + Style.RESET_ALL)
                    print(Fore.MAGENTA + "PI DataLink refresh available to fetch fresh data..." + Style.RESET_ALL)
                else:
                    print(f">>> DETECTED {len(main_stale_units)} STALE MAIN UNITS <<<")
                    print(f"Stale units: {', '.join(main_stale_units)}")
                    print("PI DataLink refresh available to fetch fresh data...")
                
                # Auto-refresh or ask user
                if auto_refresh:
                    if COLORAMA_AVAILABLE:
                        print(Fore.GREEN + "AUTO-REFRESH MODE: Automatically fetching fresh PI data..." + Style.RESET_ALL)
                    else:
                        print("AUTO-REFRESH MODE: Automatically fetching fresh PI data...")
                    self._fetch_fresh_data_from_pi(main_stale_units, max_age_hours=max_age_hours)
                    
                    # Reload database to see updated data
                    if COLORAMA_AVAILABLE:
                        print(Fore.CYAN + "Reloading database with fresh data..." + Style.RESET_ALL)
                    else:
                        print("Reloading database with fresh data...")
                    self._reload_database()
                    
                    # Re-scan to show fresh results
                    if COLORAMA_AVAILABLE:
                        print(Fore.CYAN + "Re-scanning with fresh database..." + Style.RESET_ALL)
                    else:
                        print("Re-scanning with fresh database...")
                    self._show_fresh_scan_results()
                else:
                    # Ask user if they want to fetch fresh data
                    try:
                        refresh = input("Fetch fresh data from PI DataLink? (y/n): ").strip().lower()
                        if refresh in ['y', 'yes']:
                            self._fetch_fresh_data_from_pi(main_stale_units, max_age_hours=max_age_hours)
                            
                            # Reload database to see updated data
                            if COLORAMA_AVAILABLE:
                                print(Fore.CYAN + "Reloading database with fresh data..." + Style.RESET_ALL)
                            else:
                                print("Reloading database with fresh data...")
                            self._reload_database()
                            
                            # Re-scan to show fresh results  
                            if COLORAMA_AVAILABLE:
                                print(Fore.CYAN + "Re-scanning with fresh database..." + Style.RESET_ALL)
                            else:
                                print("Re-scanning with fresh database...")
                            self._show_fresh_scan_results()
                    except EOFError:
                        if COLORAMA_AVAILABLE:
                            print(Fore.YELLOW + "Non-interactive mode: Skipping PI DataLink refresh" + Style.RESET_ALL)
                        else:
                            print("Non-interactive mode: Skipping PI DataLink refresh")
            else:
                if COLORAMA_AVAILABLE:
                    print(Fore.GREEN + ">>> ALL MAIN UNITS ARE FRESH <<<" + Style.RESET_ALL)
                else:
                    print(">>> ALL MAIN UNITS ARE FRESH <<<")
                
        except Exception as e:
            if COLORAMA_AVAILABLE:
                print(Fore.RED + f">>> SCANNER FAILED: {e} <<<" + Style.RESET_ALL)
            else:
                print(f">>> SCANNER FAILED: {e} <<<")

    def run_hourly_auto_loop(self, interval_hours: float = 1.0):
        """Continuously run [1] Auto-Refresh Scan then [2] Unit Deep Analysis every hour.

        Press Ctrl+C to stop the loop and return to the menu.
        """
        if not self._check_data_available():
            return

        if COLORAMA_AVAILABLE:
            print(Fore.GREEN + "=" * 80)
            print("          HOURLY AUTO LOOP: [1] AUTO-REFRESH -> [2] UNIT ANALYSIS")
            print("=" * 80 + Style.RESET_ALL)
            print(Fore.CYAN + f"Interval: {interval_hours} hour(s). Press Ctrl+C to stop." + Style.RESET_ALL)
        else:
            print("=" * 80)
            print("          HOURLY AUTO LOOP: [1] AUTO-REFRESH -> [2] UNIT ANALYSIS")
            print("=" * 80)
            print(f"Interval: {interval_hours} hour(s). Press Ctrl+C to stop.")

        cycle = 1
        try:
            while True:
                # Step 1: Auto-refresh scan (option [1])
                if COLORAMA_AVAILABLE:
                    print(Fore.MAGENTA + f"\n>>> CYCLE #{cycle}: RUNNING [1] AUTO-REFRESH SCAN <<<" + Style.RESET_ALL)
                else:
                    print(f"\n>>> CYCLE #{cycle}: RUNNING [1] AUTO-REFRESH SCAN <<<")
                self.run_real_data_scanner(auto_refresh=True)

                # Step 2: Unit deep analysis (option [2])
                if COLORAMA_AVAILABLE:
                    print(Fore.MAGENTA + f"\n>>> CYCLE #{cycle}: RUNNING [2] UNIT DEEP ANALYSIS <<<" + Style.RESET_ALL)
                else:
                    print(f"\n>>> CYCLE #{cycle}: RUNNING [2] UNIT DEEP ANALYSIS <<<")
                self.run_unit_analysis()

                # Sleep until next cycle
                wait_seconds = int(interval_hours * 3600)
                next_time = datetime.now() + timedelta(seconds=wait_seconds)
                if COLORAMA_AVAILABLE:
                    print(Fore.CYAN + f"Waiting {interval_hours} hour(s). Next cycle at: {next_time.strftime('%Y-%m-%d %H:%M:%S')}" + Style.RESET_ALL)
                else:
                    print(f"Waiting {interval_hours} hour(s). Next cycle at: {next_time.strftime('%Y-%m-%d %H:%M:%S')}")
                time.sleep(wait_seconds)
                cycle += 1

        except KeyboardInterrupt:
            if COLORAMA_AVAILABLE:
                print(Fore.YELLOW + "\n>>> HOURLY AUTO LOOP STOPPED BY USER <<<" + Style.RESET_ALL)
            else:
                print("\n>>> HOURLY AUTO LOOP STOPPED BY USER <<<")
            return
    
    def _fetch_fresh_data_from_pi(self, stale_units, max_age_hours: float | None = None):
        """Fetch fresh data from PI DataLink using progress tracking system"""
        try:
            if COLORAMA_AVAILABLE:
                print(Fore.GREEN + ">>> INITIATING PI DATALINK REFRESH WITH PROGRESS TRACKING <<<" + Style.RESET_ALL)
                print(Fore.CYAN + f"Refreshing data for units: {', '.join(stale_units)}" + Style.RESET_ALL)
            else:
                print(">>> INITIATING PI DATALINK REFRESH WITH PROGRESS TRACKING <<<")
                print(f"Refreshing data for units: {', '.join(stale_units)}")
            
            # Look for Excel automation workbooks (supporting all plants)
            project_root = Path(__file__).parent
            # Expanded search: include plant subfolders and master files
            excel_candidates = [
                # Root-level common names
                project_root / "excel" / "ABF_Automation.xlsx",
                project_root / "excel" / "ABFSB_Automation.xlsx",
                project_root / "excel" / "PCFS_Automation_2.xlsx",
                project_root / "excel" / "PCFS_Automation.xlsx",
                project_root / "excel" / "PCMSB_Automation.xlsx",
                # Plant subfolders
                project_root / "excel" / "ABFSB" / "ABF_Automation.xlsx",
                project_root / "excel" / "ABFSB" / "ABFSB_Automation.xlsx",
                project_root / "excel" / "ABFSB" / "ABFSB_Automation_Master.xlsx",
                project_root / "excel" / "PCFS" / "PCFS_Automation_2.xlsx",
                project_root / "excel" / "PCFS" / "PCFS_Automation.xlsx",
                project_root / "excel" / "PCMSB" / "PCMSB_Automation.xlsx",
                # Generic fallbacks
                project_root / "data" / "raw" / "Automation.xlsx",
                project_root / "Automation.xlsx",
            ]
            # Last resort: glob any Automation*.xlsx under excel/
            if not any(p.exists() for p in excel_candidates):
                for path in (project_root / "excel").rglob("*Automation*.xlsx"):
                    excel_candidates.append(path)
            main_excel = next((path for path in excel_candidates if path.exists()), None)

            if main_excel is None:
                if COLORAMA_AVAILABLE:
                    print(Fore.RED + ">>> NO PI DATALINK EXCEL FILES FOUND <<<" + Style.RESET_ALL)
                    print(Fore.YELLOW + "Checked locations:" + Style.RESET_ALL)
                    for candidate in excel_candidates:
                        print(Fore.YELLOW + f"  - {candidate}" + Style.RESET_ALL)
                else:
                    print(">>> NO PI DATALINK EXCEL FILES FOUND <<<")
                    print("Checked locations:")
                    for candidate in excel_candidates:
                        print(f"  - {candidate}")
                return

            if COLORAMA_AVAILABLE:
                print(Fore.CYAN + "Using plant-specific PI DataLink files for each unit" + Style.RESET_ALL)
                print(Fore.YELLOW + "Starting unit-by-unit progress tracking..." + Style.RESET_ALL)
            else:
                print("Using plant-specific PI DataLink files for each unit")
                print("Starting unit-by-unit progress tracking...")
            
            # Use the new progress tracking system
            from pi_monitor.parquet_auto_scan import ParquetAutoScanner
            scanner = ParquetAutoScanner()
            
            # Call the progress tracking refresh method
            # Use provided threshold or configuration/env fallback
            if max_age_hours is None:
                try:
                    max_age_hours = float(getattr(self, 'config', None).max_age_hours) if hasattr(self, 'config') else float(os.getenv('MAX_AGE_HOURS', '1.0'))
                except Exception:
                    max_age_hours = 1.0

            results = scanner.refresh_stale_units_with_progress(
                xlsx_path=main_excel,
                max_age_hours=max_age_hours
            )
            
            if COLORAMA_AVAILABLE:
                print(Fore.GREEN + f">>> PROGRESS TRACKING REFRESH COMPLETED <<<" + Style.RESET_ALL)
                if results.get("units_processed"):
                    print(Fore.CYAN + f"Successfully processed {results['units_processed']} units" + Style.RESET_ALL)
                if results.get("total_records_refreshed"):
                    print(Fore.CYAN + f"Total records refreshed: {results['total_records_refreshed']:,}" + Style.RESET_ALL)
            else:
                print(">>> PROGRESS TRACKING REFRESH COMPLETED <<<")
                if results.get("units_processed"):
                    print(f"Successfully processed {results['units_processed']} units")
                if results.get("total_records_refreshed"):
                    print(f"Total records refreshed: {results['total_records_refreshed']:,}")
            
            return results
            
        except Exception as e:
            if COLORAMA_AVAILABLE:
                print(Fore.RED + f">>> PI DATALINK REFRESH FAILED: {e} <<<" + Style.RESET_ALL)
            else:
                print(f">>> PI DATALINK REFRESH FAILED: {e} <<<")
    
    def _reload_database(self):
        """Reload the database to pick up fresh data after PI refresh"""
        try:
            # Reinitialize the ParquetDatabase to pick up updated Parquet files
            self.db = ParquetDatabase()
            
            if COLORAMA_AVAILABLE:
                print(Fore.GREEN + "Database reloaded successfully with fresh data!" + Style.RESET_ALL)
            else:
                print("Database reloaded successfully with fresh data!")
                
        except Exception as e:
            if COLORAMA_AVAILABLE:
                print(Fore.RED + f">>> DATABASE RELOAD FAILED: {e} <<<" + Style.RESET_ALL)
            else:
                print(f">>> DATABASE RELOAD FAILED: {e} <<<")
    
    def _show_fresh_scan_results(self):
        """Display fresh scan results after database reload"""
        try:
            # Get fresh scan results
            units = self.db.get_all_units()
            fresh_scan_results = []
            
            for unit in units:
                freshness_info = self.db.get_data_freshness_info(unit)
                result = {
                    'unit': unit,
                    'records': freshness_info['total_records'],
                    'latest_data': freshness_info['latest_timestamp'],
                    'data_age_hours': freshness_info['data_age_hours'],
                    'is_stale': freshness_info['is_stale'],
                    'unique_tags': len(freshness_info['unique_tags'])
                }
                fresh_scan_results.append(result)
            
            # Display fresh results
            print()
            if COLORAMA_AVAILABLE:
                print(Fore.GREEN + "=" * 80)
                print("                    FRESH DATA SCAN RESULTS (AFTER REFRESH)")
                print("=" * 80 + Style.RESET_ALL)
            else:
                print("=" * 80)
                print("                    FRESH DATA SCAN RESULTS (AFTER REFRESH)")
                print("=" * 80)
            
            print(f"{'Unit':<12} {'Records':<10} {'Age (hrs)':<10} {'Status':<12} {'Tags':<8}")
            print("-" * 60)
            
            fresh_count = 0
            for result in fresh_scan_results:
                unit = result['unit']
                records = f"{result['records']:,}" if result['records'] else "0"
                age = f"{result['data_age_hours']:.1f}" if result['data_age_hours'] else "N/A"
                status = "STALE" if result['is_stale'] else "FRESH"
                tags = str(result['unique_tags'])
                
                if not result['is_stale']:
                    fresh_count += 1
                
                if COLORAMA_AVAILABLE:
                    status_color = Fore.RED if result['is_stale'] else Fore.GREEN
                    print(f"{unit:<12} {records:<10} {age:<10} {status_color}{status:<12}{Style.RESET_ALL} {tags:<8}")
                else:
                    print(f"{unit:<12} {records:<10} {age:<10} {status:<12} {tags:<8}")
            
            if COLORAMA_AVAILABLE:
                print(Fore.GREEN + "=" * 80 + Style.RESET_ALL)
                if fresh_count == len(fresh_scan_results):
                    print(Fore.GREEN + f">>> ALL {fresh_count} UNITS ARE NOW FRESH! <<<" + Style.RESET_ALL)
                else:
                    print(Fore.YELLOW + f">>> {fresh_count}/{len(fresh_scan_results)} UNITS ARE FRESH <<<" + Style.RESET_ALL)
            else:
                print("=" * 80)
                if fresh_count == len(fresh_scan_results):
                    print(f">>> ALL {fresh_count} UNITS ARE NOW FRESH! <<<")
                else:
                    print(f">>> {fresh_count}/{len(fresh_scan_results)} UNITS ARE FRESH <<<")
                    
        except Exception as e:
            if COLORAMA_AVAILABLE:
                print(Fore.RED + f">>> FRESH SCAN FAILED: {e} <<<" + Style.RESET_ALL)
            else:
                print(f">>> FRESH SCAN FAILED: {e} <<<")
    
    def _display_all_units_analysis(self, all_analyses):
        """Display comprehensive analysis results for all units with Rich formatting"""
        try:
            if not self.console:
                return self._display_all_units_analysis_fallback(all_analyses)
                
            from rich.table import Table
            from rich.panel import Panel
            
            # Summary statistics
            total_records = sum(analysis.get('records', 0) for analysis in all_analyses.values())
            total_anomalies = sum(analysis.get('anomalies', {}).get('total_anomalies', 0) for analysis in all_analyses.values())
            avg_anomaly_rate = sum(analysis.get('anomalies', {}).get('anomaly_rate', 0) for analysis in all_analyses.values()) / len(all_analyses)
            
            # Display summary
            summary_text = f"""[bold cyan]COMPREHENSIVE UNIT ANALYSIS SUMMARY[/]
            
ðŸ“Š [green]Total Records Analyzed:[/] {total_records:,}
ðŸš¨ [red]Total Anomalies Found:[/] {total_anomalies:,}  
ðŸ“ˆ [yellow]Average Anomaly Rate:[/] {avg_anomaly_rate:.2%}
ðŸ­ [blue]Units Analyzed:[/] {len(all_analyses)}"""
            
            self.console.print(Panel(summary_text, title="ðŸ” ANALYSIS COMPLETE", border_style="green"))
            
            # Detailed results table
            table = Table(title="ðŸ“‹ DETAILED UNIT ANALYSIS RESULTS", show_header=True, header_style="bold magenta")
            table.add_column("Unit", style="cyan", width=12)
            table.add_column("Records", justify="right", style="green", width=12)
            table.add_column("Tags", justify="right", style="blue", width=8)
            table.add_column("Anomalies", justify="right", style="red", width=12)
            table.add_column("Rate", justify="right", style="yellow", width=10)
            table.add_column("Date Range", style="white", width=25)
            
            for unit, analysis in all_analyses.items():
                records = f"{analysis.get('records', 0):,}"
                tags = str(len(analysis.get('tags', [])))
                anomalies_info = analysis.get('anomalies', {})
                anomaly_count = f"{anomalies_info.get('total_anomalies', 0):,}"
                anomaly_rate = f"{anomalies_info.get('anomaly_rate', 0):.2%}"
                
                date_range = "N/A"
                if 'date_range' in analysis:
                    start = analysis['date_range'].get('start', '')[:10]  # Just date part
                    end = analysis['date_range'].get('end', '')[:10]
                    if start and end:
                        date_range = f"{start} to {end}"
                
                table.add_row(unit, records, tags, anomaly_count, anomaly_rate, date_range)
            
            self.console.print(table)
            
            # Latest anomalies and top anomalous tags
            self._display_latest_anomalies(all_analyses)
            self._display_top_anomalous_tags(all_analyses)
            
            # DETAILED PROBLEMATIC TAGS LIST
            self._display_detailed_problematic_tags(all_analyses)
            
            # AUTOMATICALLY GENERATE PLOTS WITH PROPER FOLDER STRUCTURE
            self._generate_enhanced_plots(all_analyses)
            
        except Exception as e:
            self.console.print(f"[red]Display error: {e}[/]")
    
    def _display_all_units_analysis_fallback(self, all_analyses):
        """Display analysis results for all units with fallback text formatting"""
        try:
            # Summary statistics
            total_records = sum(analysis.get('records', 0) for analysis in all_analyses.values())
            total_anomalies = sum(analysis.get('anomalies', {}).get('total_anomalies', 0) for analysis in all_analyses.values())
            avg_anomaly_rate = sum(analysis.get('anomalies', {}).get('anomaly_rate', 0) for analysis in all_analyses.values()) / len(all_analyses)
            
            print("\\n" + "=" * 80)
            print("                    COMPREHENSIVE UNIT ANALYSIS SUMMARY")
            print("=" * 80)
            print(f"Total Records Analyzed: {total_records:,}")
            print(f"Total Anomalies Found: {total_anomalies:,}")
            print(f"Average Anomaly Rate: {avg_anomaly_rate:.2%}")
            print(f"Units Analyzed: {len(all_analyses)}")
            
            print("\\n" + "=" * 80)
            print("                      DETAILED UNIT ANALYSIS RESULTS")
            print("=" * 80)
            print(f"{'Unit':<12} {'Records':<12} {'Tags':<8} {'Anomalies':<12} {'Rate':<10} {'Date Range':<25}")
            print("-" * 80)
            
            for unit, analysis in all_analyses.items():
                records = f"{analysis.get('records', 0):,}"
                tags = str(len(analysis.get('tags', [])))
                anomalies_info = analysis.get('anomalies', {})
                anomaly_count = f"{anomalies_info.get('total_anomalies', 0):,}"
                anomaly_rate = f"{anomalies_info.get('anomaly_rate', 0):.2%}"
                
                date_range = "N/A"
                if 'date_range' in analysis:
                    start = analysis['date_range'].get('start', '')[:10]
                    end = analysis['date_range'].get('end', '')[:10]
                    if start and end:
                        date_range = f"{start} to {end}"
                
                print(f"{unit:<12} {records:<12} {tags:<8} {anomaly_count:<12} {anomaly_rate:<10} {date_range:<25}")
            
            print("=" * 80)
            
            # Also display detailed problematic tags for fallback
            self._display_detailed_problematic_tags_fallback(all_analyses)
            
            # AUTOMATICALLY GENERATE PLOTS WITH PROPER FOLDER STRUCTURE
            self._generate_enhanced_plots(all_analyses)
            
        except Exception as e:
            print(f"Display error: {e}")
    
    def _display_latest_anomalies(self, all_analyses):
        """Display the latest anomalies with specific tag pinpointing"""
        try:
            if not self.console:
                return self._display_latest_anomalies_fallback(all_analyses)
                
            from rich.table import Table
            from rich.panel import Panel
            from datetime import datetime, timedelta
            import pandas as pd
            
            # Collect all recent anomalies (last 2 hours for fresh alerts)
            current_time = datetime.now()
            recent_threshold = current_time - timedelta(hours=2)
            
            latest_anomalies = []
            
            # Get detailed anomaly data for each unit
            for unit, analysis in all_analyses.items():
                unit_data = self.db.get_unit_data(unit)
                if not unit_data.empty and 'time' in unit_data.columns and 'tag' in unit_data.columns and 'value' in unit_data.columns:
                    # Get anomalies from the analysis
                    anomalies_info = analysis.get('anomalies', {})
                    by_tag = anomalies_info.get('by_tag', {})
                    
                    for tag, tag_info in by_tag.items():
                        if tag_info.get('count', 0) > 0:
                            # Get tag-specific data and find recent anomalies
                            tag_data = unit_data[unit_data['tag'] == tag].copy()
                            if len(tag_data) > 10:
                                # Apply same IQR logic to identify specific anomalous points
                                Q1 = tag_data['value'].quantile(0.25)
                                Q3 = tag_data['value'].quantile(0.75)
                                IQR = Q3 - Q1
                                lower_bound = Q1 - 1.5 * IQR
                                upper_bound = Q3 + 1.5 * IQR
                                
                                # Find anomalous points with timestamps
                                anomalous_points = tag_data[
                                    (tag_data['value'] < lower_bound) | 
                                    (tag_data['value'] > upper_bound)
                                ].copy()
                                
                                # Get recent anomalies (last 24h)
                                if not anomalous_points.empty and 'time' in anomalous_points.columns:
                                    anomalous_points['time'] = pd.to_datetime(anomalous_points['time'])
                                    recent_anomalies = anomalous_points[
                                        anomalous_points['time'] >= recent_threshold
                                    ]
                                    
                                    # Add to latest anomalies list
                                    for _, row in recent_anomalies.iterrows():
                                        latest_anomalies.append({
                                            'unit': unit,
                                            'tag': tag,
                                            'timestamp': row['time'],
                                            'value': row['value'],
                                            'lower_bound': lower_bound,
                                            'upper_bound': upper_bound,
                                            'severity': abs(row['value'] - ((lower_bound + upper_bound) / 2))
                                        })
            
            # Sort by timestamp (most recent first) and limit to top 15
            latest_anomalies.sort(key=lambda x: x['timestamp'], reverse=True)
            latest_anomalies = latest_anomalies[:15]
            
            if latest_anomalies:
                # Display latest anomalies
                alert_text = f"ðŸš¨ [bold red]FRESH ANOMALIES DETECTED[/] ({len(latest_anomalies)} in last 2h)"
                self.console.print(Panel(alert_text, border_style="red", title="âš ï¸ REAL-TIME ALERTS"))
                
                table = Table(title="ðŸ“ PINPOINTED ANOMALOUS TAGS (Most Recent First)", show_header=True, header_style="bold red")
                table.add_column("â° Timestamp", width=20, style="yellow")
                table.add_column("ðŸ­ Unit", width=10, style="cyan") 
                table.add_column("ðŸ·ï¸ Tag", width=25, style="white")
                table.add_column("ðŸ“Š Value", width=12, justify="right", style="red")
                table.add_column("ðŸ“ Normal Range", width=20, justify="center", style="green")
                table.add_column("ðŸš¨ Severity", width=10, justify="right", style="magenta")
                
                for anomaly in latest_anomalies:
                    timestamp = anomaly['timestamp'].strftime("%Y-%m-%d %H:%M:%S")
                    tag_name = anomaly['tag'].split('_')[-1] if '_' in anomaly['tag'] else anomaly['tag']  # Shorten tag
                    value = f"{anomaly['value']:.2f}"
                    range_str = f"{anomaly['lower_bound']:.1f} - {anomaly['upper_bound']:.1f}"
                    severity = f"{anomaly['severity']:.2f}"
                    
                    table.add_row(timestamp, anomaly['unit'], tag_name, value, range_str, severity)
                
                self.console.print(table)
            else:
                good_news = "âœ… [bold green]NO RECENT ANOMALIES[/] - All systems operating within normal parameters"
                self.console.print(Panel(good_news, border_style="green", title="ðŸ‘ ALL CLEAR"))
                
        except Exception as e:
            self.console.print(f"[red]Latest anomalies display error: {e}[/]")
            import traceback
            traceback.print_exc()
    
    def _display_latest_anomalies_fallback(self, all_analyses):
        """Fallback display for latest anomalies"""
        try:
            from datetime import datetime, timedelta
            import pandas as pd
            
            print("\\n" + "=" * 80)  
            print("                        FRESH ANOMALIES DETECTED")
            print("=" * 80)
            
            # Collect recent anomalies (last 2 hours)
            current_time = datetime.now()
            recent_threshold = current_time - timedelta(hours=2)
            latest_anomalies = []
            
            for unit, analysis in all_analyses.items():
                unit_data = self.db.get_unit_data(unit)
                if not unit_data.empty and 'time' in unit_data.columns and 'tag' in unit_data.columns:
                    # Get anomalies from analysis
                    anomalies_info = analysis.get('anomalies', {})
                    by_tag = anomalies_info.get('by_tag', {})
                    
                    for tag, tag_info in by_tag.items():
                        if tag_info.get('count', 0) > 0:
                            tag_data = unit_data[unit_data['tag'] == tag].copy()
                            if len(tag_data) > 10:
                                # Find recent anomalous points
                                Q1 = tag_data['value'].quantile(0.25)
                                Q3 = tag_data['value'].quantile(0.75)
                                IQR = Q3 - Q1
                                lower_bound = Q1 - 1.5 * IQR
                                upper_bound = Q3 + 1.5 * IQR
                                
                                anomalous_points = tag_data[
                                    (tag_data['value'] < lower_bound) | 
                                    (tag_data['value'] > upper_bound)
                                ]
                                
                                if not anomalous_points.empty:
                                    anomalous_points['time'] = pd.to_datetime(anomalous_points['time'])
                                    recent_anomalies = anomalous_points[
                                        anomalous_points['time'] >= recent_threshold
                                    ]
                                    
                                    for _, row in recent_anomalies.iterrows():
                                        latest_anomalies.append({
                                            'unit': unit,
                                            'tag': tag,
                                            'timestamp': row['time'],
                                            'value': row['value'],
                                            'lower_bound': lower_bound,
                                            'upper_bound': upper_bound
                                        })
            
            # Sort by timestamp (most recent first)  
            latest_anomalies.sort(key=lambda x: x['timestamp'], reverse=True)
            latest_anomalies = latest_anomalies[:10]
            
            if latest_anomalies:
                print(f"FOUND {len(latest_anomalies)} FRESH ANOMALIES (Last 2 Hours)")
                print("=" * 80)
                print(f"{'Timestamp':<20} {'Unit':<10} {'Tag':<30} {'Value':<12} {'Normal Range':<20}")
                print("-" * 80)
                
                for anomaly in latest_anomalies:
                    timestamp = anomaly['timestamp'].strftime("%Y-%m-%d %H:%M:%S")
                    tag_short = anomaly['tag'].split('_')[-1] if '_' in anomaly['tag'] else anomaly['tag'][:25]
                    value = f"{anomaly['value']:.2f}"
                    range_str = f"{anomaly['lower_bound']:.1f} - {anomaly['upper_bound']:.1f}"
                    
                    print(f"{timestamp:<20} {anomaly['unit']:<10} {tag_short:<30} {value:<12} {range_str:<20}")
                    
                print("=" * 80)
            else:
                print("NO FRESH ANOMALIES - All systems operating within normal parameters")
                print("=" * 80)
                
        except Exception as e:
            print(f"Latest anomalies fallback error: {e}")
            import traceback
            traceback.print_exc()
    
    def _display_top_anomalous_tags(self, all_analyses):
        """Display top anomalous tags across all units"""
        try:
            if not self.console:
                return
                
            from rich.table import Table
            
            # Collect all tag anomalies
            all_tag_anomalies = []
            for unit, analysis in all_analyses.items():
                anomalies = analysis.get('anomalies', {})
                by_tag = anomalies.get('by_tag', {})
                for tag, tag_info in by_tag.items():
                    all_tag_anomalies.append({
                        'unit': unit,
                        'tag': tag,
                        'count': tag_info.get('count', 0),
                        'rate': tag_info.get('rate', 0)
                    })
            
            # Sort by anomaly count and get top 10
            top_anomalies = sorted(all_tag_anomalies, key=lambda x: x['count'], reverse=True)[:10]
            
            if top_anomalies:
                table = Table(title="ðŸš¨ TOP 10 MOST ANOMALOUS TAGS", show_header=True, header_style="bold red")
                table.add_column("Rank", width=6, justify="center")
                table.add_column("Unit", width=10, style="cyan")
                table.add_column("Tag", width=30, style="white")
                table.add_column("Anomalies", width=12, justify="right", style="red")
                table.add_column("Rate", width=10, justify="right", style="yellow")
                
                for i, tag_info in enumerate(top_anomalies, 1):
                    table.add_row(
                        str(i),
                        tag_info['unit'],
                        tag_info['tag'][-30:],  # Truncate long tag names
                        f"{tag_info['count']:,}",
                        f"{tag_info['rate']:.2%}"
                    )
                
                self.console.print(table)
            
        except Exception as e:
            self.console.print(f"[red]Top anomalies display error: {e}[/]")
    
    def _display_detailed_problematic_tags(self, all_analyses):
        """Display detailed list of all problematic tags for troubleshooting"""
        try:
            if not self.console:
                return self._display_detailed_problematic_tags_fallback(all_analyses)
                
            from rich.table import Table
            from rich.panel import Panel
            
            # Collect all problematic tags across all units
            all_problematic_tags = []
            
            for unit, analysis in all_analyses.items():
                anomalies = analysis.get('anomalies', {})
                by_tag = anomalies.get('by_tag', {})
                
                for tag, tag_info in by_tag.items():
                    count = tag_info.get('count', 0)
                    rate = tag_info.get('rate', 0) * 100
                    
                    if count > 0:  # Only include tags with actual anomalies
                        # Extract detection method information
                        method = tag_info.get('method', 'Unknown')
                        mtd_count = tag_info.get('mtd_count', 0)
                        iso_count = tag_info.get('isolation_forest_count', 0)
                        
                        # Create detection method summary
                        if mtd_count > 0 and iso_count > 0:
                            detection_method = f"MTD({mtd_count}) + IF({iso_count})"
                        elif mtd_count > 0:
                            detection_method = f"MTD({mtd_count})"
                        elif iso_count > 0:
                            detection_method = f"IF({iso_count})"
                        else:
                            detection_method = method
                        
                        all_problematic_tags.append({
                            'unit': unit,
                            'tag': tag,
                            'count': count,
                            'rate': rate,
                            'confidence': tag_info.get('confidence', 'UNKNOWN'),
                            'baseline_tuned': tag_info.get('baseline_tuned', False),
                            'detection_method': detection_method
                        })
            
            if not all_problematic_tags:
                self.console.print(Panel(
                    "[green]No problematic tags found! All units operating normally.[/]",
                    title="[bold green]EXCELLENT NEWS[/]",
                    border_style="green"
                ))
                return
                
            # Sort by anomaly count (highest first)
            all_problematic_tags.sort(key=lambda x: x['count'], reverse=True)
            
            # Create detailed table
            table = Table(
                title=f"[bold red]DETAILED PROBLEMATIC TAGS - {len(all_problematic_tags)} TAGS REQUIRE ATTENTION[/]",
                show_header=True,
                header_style="bold red"
            )
            table.add_column("RANK", width=4, justify="center", style="yellow")
            table.add_column("UNIT", width=8, style="cyan")
            table.add_column("PROBLEMATIC TAG", width=30, style="white")
            table.add_column("ANOMALIES", width=9, justify="right", style="red")
            table.add_column("RATE %", width=7, justify="right", style="yellow")
            table.add_column("DETECTION METHOD", width=18, style="green")
            table.add_column("CONFIDENCE", width=10, style="magenta")
            
            # Add rows for top 50 problematic tags
            for i, tag_info in enumerate(all_problematic_tags[:50], 1):
                rank = str(i)
                unit = tag_info['unit']
                tag = tag_info['tag']
                count = f"{tag_info['count']:,}"
                rate = f"{tag_info['rate']:.2f}"
                detection_method = tag_info['detection_method']
                confidence = tag_info['confidence']
                
                # Truncate long tag names
                if len(tag) > 30:
                    tag = tag[-30:]  # Show last 30 chars
                    
                # Color code by severity
                if tag_info['rate'] >= 50:
                    rank_style = "bold red"
                elif tag_info['rate'] >= 10:
                    rank_style = "bold yellow"
                else:
                    rank_style = "white"
                    
                table.add_row(rank, unit, tag, count, rate, detection_method, confidence)
            
            self.console.print(table)
            
            # Show summary by severity
            critical = len([t for t in all_problematic_tags if t['rate'] >= 50])
            high = len([t for t in all_problematic_tags if 10 <= t['rate'] < 50])
            medium = len([t for t in all_problematic_tags if 5 <= t['rate'] < 10])
            low = len([t for t in all_problematic_tags if t['rate'] < 5])
            
            severity_text = f"""[bold white]SEVERITY BREAKDOWN:[/]
            
[bold red]CRITICAL (â‰¥50% failure):[/] {critical} tags - [red]IMMEDIATE ACTION REQUIRED[/]
[bold yellow]HIGH (10-49% failure):[/] {high} tags - [yellow]URGENT MAINTENANCE[/]
[bold orange3]MEDIUM (5-9% failure):[/] {medium} tags - [orange3]SCHEDULED MAINTENANCE[/]
[white]LOW (<5% failure):[/] {low} tags - Monitor closely"""
            
            self.console.print(Panel(
                severity_text,
                title="[bold red]MAINTENANCE PRIORITY[/]",
                border_style="red"
            ))
            
            if len(all_problematic_tags) > 50:
                self.console.print(f"[yellow]... and {len(all_problematic_tags) - 50} more problematic tags[/]")
                
        except Exception as e:
            self.console.print(f"[red]Detailed tags display error: {e}[/]")
    
    def _display_detailed_problematic_tags_fallback(self, all_analyses):
        """Fallback display for detailed problematic tags"""
        try:
            # Collect all problematic tags
            all_problematic_tags = []
            
            for unit, analysis in all_analyses.items():
                anomalies = analysis.get('anomalies', {})
                by_tag = anomalies.get('by_tag', {})
                
                for tag, tag_info in by_tag.items():
                    count = tag_info.get('count', 0)
                    rate = tag_info.get('rate', 0) * 100
                    
                    if count > 0:
                        # Extract detection method information
                        method = tag_info.get('method', 'Unknown')
                        mtd_count = tag_info.get('mtd_count', 0)
                        iso_count = tag_info.get('isolation_forest_count', 0)
                        
                        # Create detection method summary
                        if mtd_count > 0 and iso_count > 0:
                            detection_method = f"MTD({mtd_count}) + IF({iso_count})"
                        elif mtd_count > 0:
                            detection_method = f"MTD({mtd_count})"
                        elif iso_count > 0:
                            detection_method = f"IF({iso_count})"
                        else:
                            detection_method = method
                        
                        all_problematic_tags.append({
                            'unit': unit,
                            'tag': tag,
                            'count': count,
                            'rate': rate,
                            'detection_method': detection_method
                        })
            
            if not all_problematic_tags:
                print("\\n" + "=" * 80)
                print("                    EXCELLENT NEWS - NO PROBLEMATIC TAGS")
                print("=" * 80)
                print("All units operating normally! No anomalous tags detected.")
                return
                
            # Sort by count
            all_problematic_tags.sort(key=lambda x: x['count'], reverse=True)
            
            print("\\n" + "=" * 100)
            print(f"              DETAILED PROBLEMATIC TAGS - {len(all_problematic_tags)} TAGS REQUIRE ATTENTION")
            print("=" * 100)
            print(f"{'RANK':<4} {'UNIT':<8} {'PROBLEMATIC TAG':<35} {'ANOMALIES':<10} {'RATE %':<8} {'DETECTION METHOD':<20}")
            print("-" * 100)
            
            # Show top 50 problematic tags
            for i, tag_info in enumerate(all_problematic_tags[:50], 1):
                rank = str(i)
                unit = tag_info['unit']
                tag = tag_info['tag']
                count = f"{tag_info['count']:,}"
                rate = f"{tag_info['rate']:.2f}"
                detection_method = tag_info['detection_method']
                
                # Truncate long tag names
                if len(tag) > 35:
                    tag = tag[-35:]
                    
                print(f"{rank:<4} {unit:<8} {tag:<35} {count:<10} {rate:<8} {detection_method:<20}")
            
            if len(all_problematic_tags) > 50:
                print(f"... and {len(all_problematic_tags) - 50} more problematic tags")
                
            # Severity summary
            critical = len([t for t in all_problematic_tags if t['rate'] >= 50])
            high = len([t for t in all_problematic_tags if 10 <= t['rate'] < 50])
            medium = len([t for t in all_problematic_tags if 5 <= t['rate'] < 10])
            low = len([t for t in all_problematic_tags if t['rate'] < 5])
            
            print("\\n" + "=" * 80)
            print("                           MAINTENANCE PRIORITY")
            print("=" * 80)
            print(f"CRITICAL (>=50% failure): {critical} tags - IMMEDIATE ACTION REQUIRED")
            print(f"HIGH (10-49% failure):    {high} tags - URGENT MAINTENANCE")
            print(f"MEDIUM (5-9% failure):    {medium} tags - SCHEDULED MAINTENANCE") 
            print(f"LOW (<5% failure):        {low} tags - Monitor closely")
            print("=" * 80)
            
        except Exception as e:
            print(f"Detailed tags display error: {e}")
    
    def _refresh_excel_silently(self, xlsx_path):
        """Refresh Excel file completely silently without any prompts or popups"""
        import time
        
        try:
            import xlwings as xw
        except ImportError:
            raise RuntimeError("xlwings not available - install with: pip install xlwings")
        
        if COLORAMA_AVAILABLE:
            print(Fore.CYAN + "Opening Excel in headless mode with maximum dialog suppression..." + Style.RESET_ALL)
        else:
            print("Opening Excel in headless mode with maximum dialog suppression...")
        
        app = None
        wb = None
        try:
            # Create completely invisible Excel app with extreme suppression
            app = xw.App(visible=False, add_book=False)
            
            # Primary xlwings settings
            app.display_alerts = False
            app.screen_updating = False
            app.interactive = False
            app.enable_events = False
            
            # Get direct COM object for maximum control
            xl = app.api
            
            # Comprehensive COM properties to suppress ALL dialogs
            try:
                # Core dialog suppression
                xl.DisplayAlerts = False
                xl.AlertBeforeOverwriting = False
                xl.AskToUpdateLinks = False
                xl.DisplayStatusBar = False
                xl.EnableSound = False
                xl.Interactive = False
                xl.UserControl = False
                
                # File operation suppression  
                xl.DisplayFileAccessWarnings = False
                xl.DisplayCommentIndicator = 0
                xl.DisplayFormulaBar = False
                xl.DisplayFullScreen = False
                xl.DisplayNoteIndicator = False
                xl.DisplayRecentFiles = False
                xl.DisplayScrollBars = False
                
                # Advanced suppression
                xl.EnableCancelKey = 0  # Disable interruption
                xl.EnableEvents = False
                xl.ScreenUpdating = False
                xl.Cursor = -4143  # xlDefault
                
                if COLORAMA_AVAILABLE:
                    print(Fore.GREEN + "âœ“ Maximum dialog suppression activated" + Style.RESET_ALL)
                else:
                    print("âœ“ Maximum dialog suppression activated")
                    
            except Exception as e:
                if COLORAMA_AVAILABLE:
                    print(Fore.YELLOW + f"Some COM settings unavailable: {e}" + Style.RESET_ALL)
                else:
                    print(f"Some COM settings unavailable: {e}")
            
            # Open workbook with no update links to prevent dialogs
            if COLORAMA_AVAILABLE:
                print(Fore.CYAN + f"Loading PI DataLink file: {xlsx_path.name}" + Style.RESET_ALL)
            else:
                print(f"Loading PI DataLink file: {xlsx_path.name}")
            
            # Open with comprehensive dialog prevention
            wb = app.books.open(str(xlsx_path), 
                              update_links=False,      # Don't update links automatically
                              read_only=False,         # Allow modifications
                              format=None,             # Don't ask about format
                              password="",             # No password prompt
                              write_res_password="",   # No write password prompt  
                              ignore_read_only_recommended=True)  # Ignore read-only recommendation
            
            # Immediately mark as saved to prevent any save dialogs
            wb.api.Saved = True
            wb.api.ReadOnly = False
            wb.api.HasPassword = False
            wb.api.WritePassword = ""
            
            # Set manual calculation for speed
            xlCalculationManual = -4135
            xlCalculationAutomatic = -4105
            xl.Calculation = xlCalculationManual
            
            # Refresh PI DataLink connections
            if COLORAMA_AVAILABLE:
                print(Fore.YELLOW + "Refreshing PI DataLink connections..." + Style.RESET_ALL)
                print(Fore.CYAN + "This process will be completely automated - no user action needed!" + Style.RESET_ALL)
            else:
                print("Refreshing PI DataLink connections...")
                print("This process will be completely automated - no user action needed!")
            
            # Perform the actual refresh
            wb.api.RefreshAll()
            time.sleep(20)  # Extended wait for PI server
            
            # Recalculate with fresh data
            xl.CalculateFull()
            time.sleep(5)
            
            # Force save without any prompts
            wb.api.Saved = False  # Mark as needing save
            wb.save()             # Save automatically
            wb.api.Saved = True   # Mark as saved again
            
            # Restore automatic calculation
            xl.Calculation = xlCalculationAutomatic
            
            if COLORAMA_AVAILABLE:
                print(Fore.GREEN + "âœ“ PI DataLink refresh and save completed silently!" + Style.RESET_ALL)
            else:
                print("âœ“ PI DataLink refresh and save completed silently!")
            
        finally:
            # Ultra-safe shutdown with maximum dialog suppression
            try:
                if wb:
                    wb.api.Saved = True  # Final save state
                    wb.close()
                if app:
                    app.api.DisplayAlerts = False
                    app.quit()
                    
                # Force cleanup
                import gc
                gc.collect()
                
            except Exception as cleanup_error:
                if COLORAMA_AVAILABLE:
                    print(Fore.YELLOW + f"Cleanup note: {cleanup_error}" + Style.RESET_ALL)
                else:
                    print(f"Cleanup note: {cleanup_error}")
        
        if COLORAMA_AVAILABLE:
            print(Fore.GREEN + "Excel automation completed - all dialogs suppressed!" + Style.RESET_ALL)
        else:
            print("Excel automation completed - all dialogs suppressed!")
    
    def run_unit_analysis(self):
        """Run deep unit analysis"""
        if not self._check_data_available():
            return
            
        try:
            units = self.db.get_all_units()
            if not units:
                self._show_message("No units found in database", "warning")
                return
            
            if self.console:
                self.console.print(f"[cyan]Found {len(units)} units: {', '.join(units)}[/]")
                self.console.print(f"[bold green]>>> ANALYZING ALL UNITS AUTOMATICALLY <<<[/]")
                
                with Progress(
                    SpinnerColumn(),
                    TextColumn("[progress.description]{task.description}"),
                    BarColumn(),
                    MofNCompleteColumn(),
                    TimeElapsedColumn(),
                    console=self.console
                ) as progress:
                    main_task = progress.add_task("Deep scanning all neural pathways...", total=len(units))
                    
                    all_analyses = {}
                    for i, unit in enumerate(units):
                        progress.update(main_task, description=f"Analyzing unit {unit}...")
                        analysis = self.scanner.analyze_unit_data(unit, run_anomaly_detection=True)
                        all_analyses[unit] = analysis
                        progress.advance(main_task)
                
                self._display_all_units_analysis(all_analyses)
                try:
                    self._generate_enhanced_option2_plots(all_analyses)
                except Exception as _plot_exc:
                    self._show_message(f"Enhanced plots skipped: {_plot_exc}", "warning")
            else:
                print(f"Found {len(units)} units: {', '.join(units)}")
                print(">>> ANALYZING ALL UNITS AUTOMATICALLY <<<")
                
                all_analyses = {}
                for i, unit in enumerate(units):
                    print(f"[{i+1}/{len(units)}] Smart anomaly scanning {unit}...")
                    # Use enhanced anomaly detection pipeline for Option [2]
                    analysis = self.scanner.analyze_unit_data(unit, run_anomaly_detection=True)
                    all_analyses[unit] = analysis
                    # Show enhanced detection status
                    an = analysis.get('anomalies', {}) if isinstance(analysis, dict) else {}
                    method = an.get('method', 'unknown')
                    total = an.get('total_anomalies', 0)
                    rate = an.get('anomaly_rate', 0.0)
                    if method in ('hybrid_anomaly_detection', 'smart_enhanced'):
                        print(f"  -> Enhanced Pipeline: anomalies={total} ({rate*100:.2f}%)")
                    elif method == 'mtd_with_isolation_forest_fallback':
                        print(f"  -> Fallback: MTD/IF, anomalies={total} ({rate*100:.2f}%)")
                    elif method in ('mahalanobis_taguchi_distance','baseline_tuned'):
                        print(f"  -> {method}: anomalies={total} ({rate*100:.2f}%)")
                    else:
                        print(f"  -> Detection: {method}, anomalies={total} ({rate*100:.2f}%)")
                
                self._display_all_units_analysis_fallback(all_analyses)
                try:
                    self._generate_enhanced_option2_plots(all_analyses)
                except Exception as _plot_exc:
                    print(f"Enhanced plots skipped: {_plot_exc}")
                
        except Exception as e:
            self._show_error(f"Analysis failed: {e}")

    def _generate_enhanced_option2_plots(self, all_analyses: dict):
        """Generate anomaly-triggered plots for Option [2] using smart detection pipeline.

        Only generates 3-month historical plots when verified anomalies are detected
        by the detection pipeline: 2.5-sigma + Autoencoder -> MTD + Isolation Forest
        """
        from datetime import datetime, timedelta
        import pandas as pd
        from pi_monitor.smart_anomaly_detection import smart_anomaly_detection
        from pi_monitor.anomaly_triggered_plots import generate_anomaly_plots

        print("\n>>> ANOMALY-TRIGGERED PLOTTING SYSTEM <<<")
        print("Only generating plots for verified anomalous tags")
        print("Detection Pipeline: 2.5-Sigma + Autoencoder -> MTD + Isolation Forest")

        # Collect all detection results for anomaly plotting
        detection_results = {}
        verified_anomalies_found = 0

        cutoff = datetime.now() - timedelta(days=90)  # 3-month analysis window

        units = list(all_analyses.keys())
        for unit in units:
            print(f"\n[ANOMALY SCAN] {unit}:")

            # Load unit data for anomaly detection
            df = self.db.get_unit_data(unit)
            if df.empty:
                print(f"  No data available for {unit}")
                continue

            try:
                # Filter to analysis window
                df['time'] = pd.to_datetime(df['time'])
                df_recent = df[df['time'] >= cutoff].copy()

                if df_recent.empty:
                    print(f"  No data in 3-month analysis window")
                    continue

                # Run smart anomaly detection with auto-plotting enabled
                results = smart_anomaly_detection(df_recent, unit, auto_plot_anomalies=False)

                # Store results for batch plotting
                detection_results[unit] = results

                # Report status
                unit_status = results.get('unit_status', {})
                total_anomalies = results.get('total_anomalies', 0)
                by_tag = results.get('by_tag', {})

                print(f"  Status: {unit_status.get('status', 'UNKNOWN')}")
                print(f"  Total anomalies: {total_anomalies:,}")
                print(f"  Problematic tags: {len(by_tag)}")

                # Count verified anomalies
                unit_verified = 0
                for tag, tag_info in by_tag.items():
                    sigma_count = tag_info.get('sigma_2_5_count', 0)
                    ae_count = tag_info.get('autoencoder_count', 0)
                    mtd_count = tag_info.get('mtd_count', 0)
                    iso_count = tag_info.get('isolation_forest_count', 0)
                    confidence = tag_info.get('confidence', 'LOW')

                    # Check verification criteria
                    primary_detected = sigma_count > 0 or ae_count > 0
                    verification_detected = mtd_count > 0 or iso_count > 0
                    high_confidence = confidence in ['HIGH', 'MEDIUM']

                    if primary_detected and verification_detected and high_confidence:
                        unit_verified += 1

                verified_anomalies_found += unit_verified
                print(f"  Verified anomalies: {unit_verified}")

            except Exception as e:
                print(f"  ERROR in anomaly detection: {e}")
                continue

        # Generate plots only if verified anomalies are found
        if verified_anomalies_found > 0:
            print(f"\n>>> GENERATING ANOMALY DIAGNOSTIC PLOTS <<<")
            print(f"Found {verified_anomalies_found} verified anomalies across {len(detection_results)} units")

            try:
                # Generate anomaly-triggered plots
                plot_session_dir = generate_anomaly_plots(detection_results)

                print(f"\n>>> ANOMALY DIAGNOSTIC PLOTS COMPLETED <<<")
                print(f"Location: {plot_session_dir}")
                print(f"Only verified anomalous tags have been plotted")

            except Exception as e:
                print(f"ERROR in plot generation: {e}")
        else:
            print(f"\n>>> NO VERIFIED ANOMALIES DETECTED <<<")
            print("All monitored tags are operating within normal parameters")
            print("No diagnostic plots generated")
    
    def show_database_overview(self):
        """Show complete database overview"""
        if not self._check_data_available():
            return
            
        try:
            if COLORAMA_AVAILABLE:
                print(Fore.GREEN + ">>> DATABASE OVERVIEW SCAN <<<" + Style.RESET_ALL)
                print(Fore.CYAN + "Scanning quantum storage matrix..." + Style.RESET_ALL)
            else:
                print(">>> DATABASE OVERVIEW SCAN <<<")
                print("Scanning quantum storage matrix...")
            
            status = self.db.get_database_status()
            
            # Main database information
            print()
            if COLORAMA_AVAILABLE:
                print(Fore.GREEN + "+" + "=" * 78 + "+")
                print("|" + " " * 22 + "QUANTUM STORAGE MATRIX" + " " * 33 + "|")
                print("+" + "=" * 78 + "+" + Style.RESET_ALL)
                print(Fore.CYAN + f"Directory:    {status['processed_directory']}" + Style.RESET_ALL)
                print(Fore.CYAN + f"Total Files:  {status['total_files']}" + Style.RESET_ALL)
                print(Fore.CYAN + f"Storage Size: {status['total_size_gb']:.2f} GB" + Style.RESET_ALL)
                print(Fore.CYAN + f"DuckDB:       {'Available' if status['duckdb_available'] else 'Unavailable'}" + Style.RESET_ALL)
                print(Fore.CYAN + f"Active Units: {len(status['units'])}" + Style.RESET_ALL)
            else:
                print("+" + "=" * 78 + "+")
                print("|" + " " * 22 + "QUANTUM STORAGE MATRIX" + " " * 33 + "|")
                print("+" + "=" * 78 + "+")
                print(f"Directory:    {status['processed_directory']}")
                print(f"Total Files:  {status['total_files']}")
                print(f"Storage Size: {status['total_size_gb']:.2f} GB")
                print(f"DuckDB:       {'Available' if status['duckdb_available'] else 'Unavailable'}")
                print(f"Active Units: {len(status['units'])}")
            
            # Units table
            if status['units']:
                print()
                if COLORAMA_AVAILABLE:
                    print(Fore.YELLOW + "+" + "=" * 78 + "+")
                    print("|" + " " * 27 + "UNIT STATUS MATRIX" + " " * 32 + "|")
                    print("+" + "=" * 78 + "+" + Style.RESET_ALL)
                else:
                    print("+" + "=" * 78 + "+")
                    print("|" + " " * 27 + "UNIT STATUS MATRIX" + " " * 32 + "|")
                    print("+" + "=" * 78 + "+")
                
                print(f"{'Unit':<12} {'Files':<6} {'Size(MB)':<10} {'Records':<12} {'Age(hrs)':<10} {'Status':<8}")
                print("-" * 78)
                
                for unit in status['units']:
                    age_str = f"{unit['data_age_hours']:.1f}" if unit['data_age_hours'] else "N/A"
                    status_str = "FRESH" if not unit['is_stale'] else "STALE"
                    
                    if COLORAMA_AVAILABLE:
                        status_color = Fore.GREEN if not unit['is_stale'] else Fore.RED
                        print(f"{unit['unit']:<12} {unit['files']:<6} {unit['size_mb']:<10.1f} "
                              f"{unit['records']:<12,} {age_str:<10} "
                              f"{status_color}{status_str:<8}{Style.RESET_ALL}")
                    else:
                        print(f"{unit['unit']:<12} {unit['files']:<6} {unit['size_mb']:<10.1f} "
                              f"{unit['records']:<12,} {age_str:<10} {status_str:<8}")
            
            if COLORAMA_AVAILABLE:
                print(Fore.GREEN + "+" + "=" * 78 + "+" + Style.RESET_ALL)
            else:
                print("+" + "=" * 78 + "+")
                
        except Exception as e:
            if COLORAMA_AVAILABLE:
                print(Fore.RED + f">>> DATABASE SCAN FAILED: {e} <<<" + Style.RESET_ALL)
            else:
                print(f">>> DATABASE SCAN FAILED: {e} <<<")
    
    def run_auto_scan_system(self):
        """Run the intelligent auto-scan system"""
        if not self._check_data_available():
            return
            
        self._show_message("AUTO-SCAN SYSTEM", "info")
        self._show_message("This would run the intelligent auto-scan with PI DataLink integration", "info")
        self._wait_for_continue()
    
    def run_quality_audit(self):
        """Run data quality audit"""
        if not self._check_data_available():
            return
            
        self._show_message("DATA QUALITY AUDIT", "info")
        self._show_message("This would generate comprehensive quality reports", "info")
        self._wait_for_continue()
    
    def run_unit_explorer(self):
        """Run unit explorer"""
        if not self._check_data_available():
            return
            
        try:
            units = self.db.get_all_units()
            
            if COLORAMA_AVAILABLE:
                print(Fore.MAGENTA + f">>> UNIT EXPLORER ({len(units)} units) <<<" + Style.RESET_ALL)
            else:
                print(f">>> UNIT EXPLORER ({len(units)} units) <<<")
            
            if units:
                print()
                for i, unit in enumerate(units, 1):
                    if COLORAMA_AVAILABLE:
                        print(Fore.CYAN + f"{i:2d}. {unit}" + Style.RESET_ALL)
                    else:
                        print(f"{i:2d}. {unit}")
            else:
                if COLORAMA_AVAILABLE:
                    print(Fore.RED + ">>> NO UNITS DETECTED <<<" + Style.RESET_ALL)
                else:
                    print(">>> NO UNITS DETECTED <<<")
                
        except Exception as e:
            self._show_error(f"Unit explorer failed: {e}")
    
    def launch_original_cli(self):
        """Launch the original CLI system"""
        if COLORAMA_AVAILABLE:
            print(Fore.YELLOW + ">>> LAUNCHING ORIGINAL CLI SYSTEM <<<" + Style.RESET_ALL)
        else:
            print(">>> LAUNCHING ORIGINAL CLI SYSTEM <<<")
        time.sleep(1)
        
        try:
            # Launch original CLI
            if DATA_MODULES_AVAILABLE:
                original_cli_main()
            else:
                print("Original CLI not available - data modules missing")
                self._wait_for_continue()
        except Exception as e:
            self._show_error(f"Original CLI failed: {e}")
    
    def run_system_diagnostics(self):
        """Run system diagnostics"""
        if COLORAMA_AVAILABLE:
            print(Fore.GREEN + ">>> SYSTEM DIAGNOSTICS <<<" + Style.RESET_ALL)
        else:
            print(">>> SYSTEM DIAGNOSTICS <<<")
        
        print()
        print("+" + "=" * 70 + "+")
        print("|" + " " * 22 + "NEURAL MATRIX DIAGNOSTICS" + " " * 23 + "|")
        print("+" + "=" * 70 + "+")
        
        # Check components
        components = [
            ("Rich Interface", RICH_AVAILABLE, "Beautiful UI available" if RICH_AVAILABLE else "Fallback mode"),
            ("Colorama Support", COLORAMA_AVAILABLE, "Color support available" if COLORAMA_AVAILABLE else "No color support"),  
            ("Data Modules", DATA_MODULES_AVAILABLE, "Real data access" if DATA_MODULES_AVAILABLE else "No data access"),
            ("Parquet Database", self.data_available, "Connected to data" if self.data_available else "No data connection"),
        ]
        
        print(f"{'Component':<20} {'Status':<12} {'Details':<35}")
        print("-" * 70)
        
        for name, status, details in components:
            status_text = "ONLINE" if status else "OFFLINE"
            if COLORAMA_AVAILABLE:
                status_color = Fore.GREEN if status else Fore.RED
                print(f"{name:<20} {status_color}{status_text:<12}{Style.RESET_ALL} {details:<35}")
            else:
                print(f"{name:<20} {status_text:<12} {details:<35}")
        
        print("+" + "=" * 70 + "+")

    def run_conditional_plotting(self):
        """Run conditional plotting with minimal change detection"""
        try:
            if COLORAMA_AVAILABLE:
                print(Fore.GREEN + ">>> LAUNCHING CONDITIONAL PLOTTING SYSTEM <<<" + Style.RESET_ALL)
            else:
                print(">>> LAUNCHING CONDITIONAL PLOTTING SYSTEM <<<")

            # Import the conditional plotting module
            import importlib
            try:
                conditional_plot = importlib.import_module('enhanced_plot_conditional')

                # Get available units
                units = self.db.get_all_units()

                if not units:
                    print("No units available for plotting")
                    return

                # Ask user for change threshold
                print(f"\nAvailable units: {', '.join(units)}")
                try:
                    threshold = input("Enter minimal change threshold % (default 0.5): ").strip()
                    if not threshold:
                        threshold = 0.5
                    else:
                        threshold = float(threshold)

                    print(f"\nGenerating conditional plots with {threshold}% change threshold...")
                    output_dir = conditional_plot.create_conditional_enhanced_plots(change_threshold=threshold)
                    print(f"\nConditional plotting completed!")
                    print(f"Output directory: {output_dir}")
                    print("Check reports/ directory for plots with minimal change markers.")

                except EOFError:
                    print("Non-interactive mode: using default 0.5% threshold")
                    output_dir = conditional_plot.create_conditional_enhanced_plots(change_threshold=0.5)
                    print(f"Conditional plots generated in: {output_dir}")
                except ValueError:
                    print("Invalid threshold value, using default 0.5%")
                    output_dir = conditional_plot.create_conditional_enhanced_plots(change_threshold=0.5)

            except ImportError as e:
                print(f"Enhanced conditional plotting module not available: {e}")
                print("Please ensure enhanced_plot_conditional.py is in the project directory")

        except Exception as e:
            print(f"Conditional plotting failed: {e}")

    def run_tag_state_dashboard(self):
        """Run comprehensive tag state dashboard"""
        try:
            if COLORAMA_AVAILABLE:
                print(Fore.GREEN + ">>> LAUNCHING TAG STATE DASHBOARD <<<" + Style.RESET_ALL)
            else:
                print(">>> LAUNCHING TAG STATE DASHBOARD <<<")

            # Import the tag state dashboard module
            try:
                from pi_monitor.tag_state_dashboard import TagStateDashboard

                dashboard = TagStateDashboard()

                # Get available units
                units = self.db.get_all_units()

                if not units:
                    print("No units available for dashboard")
                    return

                # Ask user to select unit
                print(f"\nAvailable units: {', '.join(units)}")
                try:
                    unit = input("Enter unit for tag state analysis: ").strip()
                    if unit not in units:
                        print(f"Unit '{unit}' not found")
                        return

                    # Get comprehensive tag states
                    print(f"\nAnalyzing tag states for {unit}...")
                    tag_states = dashboard.get_comprehensive_tag_states(unit, hours_back=24)

                    # Display results
                    print(f"\n=== TAG STATE DASHBOARD: {unit} ===")
                    print(f"Analysis period: Last 24 hours")
                    print(f"Total tags analyzed: {tag_states.get('total_tags', 0)}")
                    print(f"Active tags: {tag_states.get('active_tags', 0)}")
                    print(f"Stale tags: {tag_states.get('stale_tags', 0)}")
                    print(f"Anomalous tags: {tag_states.get('anomalous_tags', 0)}")

                    # Show tag details
                    if 'tag_details' in tag_states:
                        print(f"\nTOP TAG ISSUES:")
                        for i, tag_info in enumerate(tag_states['tag_details'][:10], 1):
                            print(f"{i:2d}. {tag_info['tag']}")
                            print(f"     Status: {tag_info.get('status', 'Unknown')}")
                            print(f"     Last update: {tag_info.get('last_update', 'Unknown')}")
                            if 'anomaly_rate' in tag_info:
                                print(f"     Anomaly rate: {tag_info['anomaly_rate']:.1%}")

                except EOFError:
                    print("Non-interactive mode: analyzing first unit")
                    unit = units[0]
                    tag_states = dashboard.get_comprehensive_tag_states(unit, hours_back=24)
                    print(f"Tag state analysis completed for {unit}")

            except ImportError as e:
                print(f"Tag state dashboard module not available: {e}")
                print("Please ensure pi_monitor/tag_state_dashboard.py is available")

        except Exception as e:
            print(f"Tag state dashboard failed: {e}")

    def run_incident_reporter(self):
        """Run WHO-WHAT-WHEN-WHERE incident reporting system"""
        try:
            if COLORAMA_AVAILABLE:
                print(Fore.GREEN + ">>> LAUNCHING INCIDENT REPORTING SYSTEM <<<" + Style.RESET_ALL)
            else:
                print(">>> LAUNCHING INCIDENT REPORTING SYSTEM <<<")

            # Import the incident reporter module
            import importlib
            import subprocess
            try:
                # Get available units
                units = self.db.get_all_units()

                if not units:
                    print("No units available for incident reporting")
                    return

                # Ask user to select unit and timeframe
                print(f"\nAvailable units: {', '.join(units)}")
                try:
                    unit = input("Enter unit for incident report: ").strip()
                    if unit not in units:
                        print(f"Unit '{unit}' not found")
                        return

                    hours = input("Enter hours back to analyze (default 24): ").strip()
                    if not hours:
                        hours = "24"

                    # Run the incident reporter script
                    print(f"\nGenerating incident report for {unit} (last {hours} hours)...")
                    result = subprocess.run([
                        'python', 'scripts/anomaly_incident_reporter.py',
                        '--unit', unit,
                        '--hours', hours
                    ], capture_output=True, text=True)

                    if result.returncode == 0:
                        print("Incident report generated successfully!")
                        print("Check reports/ directory for the detailed incident report.")
                        if result.stdout:
                            print("\nReport summary:")
                            print(result.stdout)
                    else:
                        print(f"Incident report generation failed: {result.stderr}")

                except EOFError:
                    print("Non-interactive mode: generating report for first unit")
                    unit = units[0]
                    result = subprocess.run([
                        'python', 'scripts/anomaly_incident_reporter.py',
                        '--unit', unit,
                        '--hours', '24'
                    ], capture_output=True, text=True)
                    print(f"Incident report generated for {unit}")

            except Exception as e:
                print(f"Failed to run incident reporter: {e}")
                print("Please ensure scripts/anomaly_incident_reporter.py is available")

        except Exception as e:
            print(f"Incident reporting failed: {e}")

    def shutdown_system(self):
        """Shutdown the unified system"""
        if self.console:
            try:
                with Progress(
                    SpinnerColumn(),
                    TextColumn("Terminating neural matrix connections..."),
                    transient=True,
                    console=self.console
                ) as progress:
                    task = progress.add_task("", total=30)
                    for i in range(30):
                        time.sleep(0.03)
                        progress.update(task, advance=1)
                
                self.console.print(Panel(
                    "[bold red]>>> TURBOPREDICT X PROTEAN OFFLINE <<<[/]\n\n"
                    "Neural matrix disconnected\n"
                    "Quantum processors shutdown\n"
                    "All systems terminated",
                    title="[bold red]SYSTEM SHUTDOWN[/]"
                ))
                return
            except:
                pass
        
        if COLORAMA_AVAILABLE:
            print(Fore.RED + ">>> TURBOPREDICT X PROTEAN OFFLINE <<<" + Style.RESET_ALL)
        else:
            print(">>> TURBOPREDICT X PROTEAN OFFLINE <<<")
        print("Thank you for using the neural matrix!")
    
    # Helper methods
    def _check_data_available(self):
        """Check if data systems are available"""
        if not self.data_available:
            if self.console:
                self.console.print(Panel(
                    "[bold red]>>> DATA SYSTEMS OFFLINE <<<[/]\n\n"
                    "This feature requires data connection.\n"
                    "Please ensure Parquet files are accessible.",
                    title="[bold red]ERROR[/]"
                ))
                self._wait_for_continue()
            else:
                print(">>> DATA SYSTEMS OFFLINE <<<")
                print("This feature requires data connection.")
                input("Press Enter to continue...")
            return False
        return True
    
    def _show_error(self, message):
        """Show error message"""
        if self.console:
            self.console.print(f"[bold red]>>> ERROR: {message} <<<[/]")
            self._wait_for_continue()
        else:
            print(f">>> ERROR: {message} <<<")
            input("Press Enter to continue...")
    
    def _show_message(self, message, msg_type="info"):
        """Show a message with optional type styling"""
        colors = {"info": "cyan", "warning": "yellow", "error": "red", "success": "green"}
        color = colors.get(msg_type, "white")
        
        if self.console:
            self.console.print(f"[bold {color}]>>> {message} <<<[/]")
        else:
            print(f">>> {message} <<<")
    
    def _wait_for_continue(self):
        """Wait for user to continue"""
        if self.console:
            Prompt.ask("[dim]Press Enter to continue...[/]", default="", console=self.console)
        else:
            input("Press Enter to continue...")
    
    def _show_invalid_choice(self):
        """Show invalid choice message"""
        self._show_message("INVALID NEURAL PATHWAY", "error")
    
    def _display_scan_results(self, results):
        """Display scan results with rich formatting"""
        # Implementation would show scan results in rich format
        summary = results.get('summary', {})
        self.console.print(f"[bold green]Scan complete: {summary.get('fresh_units', 0)} fresh, {summary.get('stale_units', 0)} stale units[/]")
        self._wait_for_continue()
    
    def _display_scan_results_fallback(self, results):
        """Display scan results in fallback format"""
        summary = results.get('summary', {})
        print(f"Scan complete: {summary.get('fresh_units', 0)} fresh, {summary.get('stale_units', 0)} stale units")
        input("Press Enter to continue...")
    
    def _display_unit_analysis(self, analysis):
        """Display unit analysis with rich formatting"""
        self.console.print(f"[bold green]Analysis complete for {analysis.get('unit', 'unknown')}[/]")
        self.console.print(f"Records: {analysis.get('records', 0):,}")
        self._wait_for_continue()
    
    def _display_unit_analysis_fallback(self, analysis):
        """Display unit analysis in fallback format"""
        print(f"Analysis complete for {analysis.get('unit', 'unknown')}")
        print(f"Records: {analysis.get('records', 0):,}")
        input("Press Enter to continue...")
    
    def _generate_enhanced_plots(self, all_analyses):
        """Generate enhanced plots with proper folder structure after anomaly analysis"""
        try:
            if self.console:
                self.console.print("\n[bold cyan]ðŸ“Š GENERATING ENHANCED PLOTS...[/]")
            else:
                print("\n" + "=" * 60)
                print("GENERATING ENHANCED PLOTS...")
                print("=" * 60)
            
            import matplotlib
            matplotlib.use('Agg')  # Use non-interactive backend
            import matplotlib.pyplot as plt
            import numpy as np
            from datetime import datetime, timedelta
            from pathlib import Path
            
            # Setup proper folder structure
            base_reports_dir = Path("C:/Users/george.gabrielujai/Documents/CodeX/reports")
            cutoff_date = datetime.now() - timedelta(days=90)
            analysis_date = f"{cutoff_date.strftime('%Y-%m-%d')}_to_{datetime.now().strftime('%Y-%m-%d')}_Analysis"
            plotting_time = datetime.now().strftime('%H-%M-%S_PlottingTime')
            
            main_output_dir = base_reports_dir / analysis_date / plotting_time
            main_output_dir.mkdir(parents=True, exist_ok=True)
            
            if self.console:
                self.console.print(f"[green]ðŸ“ Plots will be saved to:[/] {main_output_dir}")
            else:
                print(f"Plots will be saved to: {main_output_dir}")
            
            plot_count = 0
            
            for unit, analysis in all_analyses.items():
                try:
                    if self.console:
                        self.console.print(f"[yellow]ðŸ”„ Processing unit: {unit}[/]")
                    else:
                        print(f"Processing unit: {unit}")
                    
                    # Create unit directory
                    unit_dir = main_output_dir / unit
                    unit_dir.mkdir(exist_ok=True)
                    
                    # Get anomaly data
                    anomalies = analysis.get('anomalies', {})
                    by_tag = anomalies.get('by_tag', {})
                    
                    if not by_tag:
                        if self.console:
                            self.console.print(f"[green]âœ“ {unit}: No problematic tags - unit operating normally[/]")
                        else:
                            print(f"[OK] {unit}: No problematic tags - unit operating normally")
                        continue
                    
                    # Sort tags by anomaly count and plot ALL problematic tags
                    sorted_tags = sorted(by_tag.items(), key=lambda x: x[1].get('count', 0), reverse=True)
                    top_tags = sorted_tags
                    
                    # Get unit data for plotting
                    unit_data = self.db.get_unit_data(unit)
                    if unit_data.empty:
                        continue
                    
                    # Filter to last 3 months for plotting
                    unit_data['time'] = pd.to_datetime(unit_data['time'])
                    recent_data = unit_data[unit_data['time'] >= cutoff_date].copy()
                    
                    if recent_data.empty:
                        continue
                    
                    # Create plots for top problematic tags
                    for i, (tag, tag_info) in enumerate(top_tags):
                        try:
                            self._create_enhanced_tag_plot(unit, tag, tag_info, recent_data, unit_dir)
                            plot_count += 1
                            
                            if self.console:
                                self.console.print(f"  [green]âœ“[/] Created plot {i+1}/{len(top_tags)}: {tag[:50]}...")
                            else:
                                print(f"  [OK] Created plot {i+1}/{len(top_tags)}: {tag[:50]}...")
                                
                        except Exception as e:
                            if self.console:
                                self.console.print(f"  [red]âœ—[/] Plot failed for {tag[:30]}: {e}")
                            else:
                                print(f"  [FAIL] Plot failed for {tag[:30]}: {e}")
                    
                    # Create unit summary report
                    self._create_unit_summary_report(unit, unit_dir, recent_data, anomalies, top_tags)
                    
                except Exception as e:
                    if self.console:
                        self.console.print(f"[red]âœ— Error processing {unit}: {e}[/]")
                    else:
                        print(f"[FAIL] Error processing {unit}: {e}")
            
            # Create main overview
            self._create_main_overview_report(main_output_dir, all_analyses, analysis_date, plotting_time)
            
            if self.console:
                self.console.print(f"\n[bold green]ðŸŽ‰ PLOT GENERATION COMPLETE![/]")
                self.console.print(f"[green]ðŸ“Š Total plots created: {plot_count}[/]")
                self.console.print(f"[green]ðŸ“ Location: {main_output_dir}[/]")
                self.console.print(f"[cyan]ðŸ’¡ Folder structure: Analysis_Period/Plotting_Time/Units/[/]")
            else:
                print(f"\n" + "=" * 60)
                print("PLOT GENERATION COMPLETE!")
                print(f"Total plots created: {plot_count}")
                print(f"Location: {main_output_dir}")
                print("Folder structure: Analysis_Period/Plotting_Time/Units/")
                print("=" * 60)
                
        except Exception as e:
            if self.console:
                self.console.print(f"[red]âœ— Plot generation failed: {e}[/]")
            else:
                print(f"[FAIL] Plot generation failed: {e}")
    
    def _create_enhanced_tag_plot(self, unit, tag, tag_info, data, unit_dir):
        """Create enhanced plot for a specific tag"""
        import matplotlib.pyplot as plt
        
        # Get tag data
        tag_data = data[data['tag'] == tag].copy()
        if tag_data.empty or len(tag_data) < 10:
            return
            
        tag_data = tag_data.sort_values('time')
        
        # Extract detection information
        count = tag_info.get('count', 0)
        rate = tag_info.get('rate', 0) * 100
        method = tag_info.get('method', 'Unknown')
        mtd_count = tag_info.get('mtd_count', 0)
        iso_count = tag_info.get('isolation_forest_count', 0)
        thresholds = tag_info.get('thresholds', {})
        
        # Create plot
        fig, ((ax1, ax2), (ax3, ax4)) = plt.subplots(2, 2, figsize=(16, 12))
        
        # Main time series (top left)
        ax1.plot(tag_data['time'], tag_data['value'], 'b-', alpha=0.7, linewidth=1.5, label='Values')
        
        if thresholds:
            upper_limit = thresholds.get('upper', tag_data['value'].quantile(0.95))
            lower_limit = thresholds.get('lower', tag_data['value'].quantile(0.05))
            
            anomalies = tag_data[(tag_data['value'] > upper_limit) | (tag_data['value'] < lower_limit)]
            if not anomalies.empty:
                ax1.scatter(anomalies['time'], anomalies['value'], c='red', s=30, alpha=0.8, 
                           label=f'Anomalies ({len(anomalies)})', zorder=5)
            
            ax1.axhline(y=upper_limit, color='orange', linestyle='--', alpha=0.7, label='Upper Threshold')
            ax1.axhline(y=lower_limit, color='orange', linestyle='--', alpha=0.7, label='Lower Threshold')
        
        ax1.set_title(f'{unit} - {tag[:40]}\nSmart Detection: {method}', fontweight='bold', fontsize=10)
        ax1.set_ylabel('Value')
        ax1.grid(True, alpha=0.3)
        ax1.legend(fontsize=8)
        ax1.tick_params(axis='x', rotation=45)
        
        # Distribution (top right)
        ax2.hist(tag_data['value'], bins=30, alpha=0.7, color='skyblue', edgecolor='black')
        ax2.axvline(x=tag_data['value'].mean(), color='red', linestyle='-', linewidth=2, label='Mean')
        ax2.set_title('Value Distribution', fontweight='bold')
        ax2.set_xlabel('Value')
        ax2.set_ylabel('Frequency')
        ax2.legend()
        ax2.grid(True, alpha=0.3)
        
        # Detection methods (bottom left)
        if mtd_count > 0 or iso_count > 0:
            methods = []
            counts = []
            if mtd_count > 0:
                methods.append('MTD')
                counts.append(mtd_count)
            if iso_count > 0:
                methods.append('Isolation\nForest')
                counts.append(iso_count)
            
            bars = ax3.bar(methods, counts, color=['blue', 'green'][:len(methods)], alpha=0.7)
            ax3.set_title('Detection Method Breakdown', fontweight='bold')
            ax3.set_ylabel('Anomaly Count')
            
            for bar, count in zip(bars, counts):
                ax3.text(bar.get_x() + bar.get_width()/2., bar.get_height() + 0.1,
                        str(count), ha='center', va='bottom', fontweight='bold')
        else:
            ax3.text(0.5, 0.5, 'No Anomalies\nDetected', ha='center', va='center', 
                    transform=ax3.transAxes, fontsize=12, fontweight='bold')
            ax3.set_title('Detection Method Breakdown', fontweight='bold')
        
        ax3.grid(True, alpha=0.3)
        
        # Statistics (bottom right)
        stats_text = f"""Statistics:
Mean: {tag_data['value'].mean():.3f}
Std: {tag_data['value'].std():.3f}
Min: {tag_data['value'].min():.3f}
Max: {tag_data['value'].max():.3f}

Detection Results:
Total Anomalies: {count}
Anomaly Rate: {rate:.2f}%
MTD Count: {mtd_count}
IF Count: {iso_count}"""
        
        ax4.text(0.1, 0.9, stats_text, transform=ax4.transAxes, fontsize=10, 
                verticalalignment='top', fontfamily='monospace',
                bbox=dict(boxstyle='round', facecolor='wheat', alpha=0.8))
        ax4.set_xlim(0, 1)
        ax4.set_ylim(0, 1)
        ax4.axis('off')
        ax4.set_title('Analysis Summary', fontweight='bold')
        
        plt.tight_layout()
        
        # Save with safe filename
        safe_tag_name = tag.replace('/', '_').replace('\\', '_').replace(':', '_').replace('*', '_')[:50]
        filename = f'{safe_tag_name}_SMART_ANALYSIS.png'
        plt.savefig(unit_dir / filename, dpi=200, bbox_inches='tight')
        plt.close()
    
    def _create_unit_summary_report(self, unit, unit_dir, data, anomalies, top_tags):
        """Create unit summary report"""
        summary_file = unit_dir / f"{unit}_ANALYSIS_REPORT.txt"
        
        with open(summary_file, 'w') as f:
            f.write(f"SMART ANOMALY DETECTION ANALYSIS REPORT\n")
            f.write(f"=" * 60 + "\n")
            f.write(f"Unit: {unit}\n")
            f.write(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
            f.write(f"Analysis Period: Last 3 months\n")
            f.write(f"Detection System: MTD + Isolation Forest + Unit Status\n")
            f.write(f"=" * 60 + "\n\n")
            
            f.write(f"SUMMARY:\n")
            f.write(f"Total Anomalies: {anomalies.get('total_anomalies', 0):,}\n")
            f.write(f"Anomaly Rate: {anomalies.get('anomaly_rate', 0)*100:.3f}%\n")
            f.write(f"Detection Method: {anomalies.get('method', 'Unknown')}\n")
            f.write(f"Total Data Points: {len(data):,}\n")
            f.write(f"Unique Tags: {data['tag'].nunique()}\n")
            f.write(f"Problematic Tags: {len(anomalies.get('by_tag', {}))}\n\n")
            
            if top_tags:
                f.write(f"TOP PROBLEMATIC TAGS:\n")
                f.write(f"-" * 40 + "\n")
                for i, (tag, tag_info) in enumerate(top_tags, 1):
                    f.write(f"{i}. {tag}\n")
                    f.write(f"   Anomalies: {tag_info.get('count', 0):,} ({tag_info.get('rate', 0)*100:.2f}%)\n")
                    f.write(f"   Method: {tag_info.get('method', 'Unknown')}\n")
                    f.write(f"   MTD: {tag_info.get('mtd_count', 0)}, IF: {tag_info.get('isolation_forest_count', 0)}\n\n")
    
    def _create_main_overview_report(self, output_dir, all_analyses, analysis_date, plotting_time):
        """Create main overview report"""
        overview_file = output_dir / "ANALYSIS_OVERVIEW.txt"
        
        with open(overview_file, 'w') as f:
            f.write(f"COMPREHENSIVE ANOMALY ANALYSIS OVERVIEW\n")
            f.write(f"=" * 70 + "\n")
            f.write(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
            f.write(f"Analysis Period: {analysis_date}\n")
            f.write(f"Plotting Time: {plotting_time}\n")
            f.write(f"Detection: MTD + Isolation Forest + Unit Status Awareness\n")
            f.write(f"=" * 70 + "\n\n")
            
            total_anomalies = sum(analysis.get('anomalies', {}).get('total_anomalies', 0) for analysis in all_analyses.values())
            
            f.write(f"FLEET SUMMARY:\n")
            f.write(f"Units Analyzed: {len(all_analyses)}\n")
            f.write(f"Total Anomalies: {total_anomalies:,}\n")
            f.write(f"Units with Issues: {sum(1 for a in all_analyses.values() if a.get('anomalies', {}).get('total_anomalies', 0) > 0)}\n\n")
            
            f.write(f"UNIT BREAKDOWN:\n")
            f.write(f"-" * 40 + "\n")
            for unit, analysis in all_analyses.items():
                anomalies = analysis.get('anomalies', {})
                f.write(f"{unit}: {anomalies.get('total_anomalies', 0):,} anomalies\n")
            
            f.write(f"\nThis analysis uses the same detection as CLI option [2].\n")
    
    def run(self):
        """Main system loop"""
        try:
            self.display_startup_banner()
            
            while True:
                choice = self.show_main_menu()
                if not self.execute_option(choice):
                    break
                    
        except KeyboardInterrupt:
            self.shutdown_system()
        except Exception as e:
            try:
                if self.console:
                    self.console.print(f"[bold red]>>> CRITICAL SYSTEM ERROR: {e} <<<[/]")
                else:
                    print(f">>> CRITICAL SYSTEM ERROR: {e} <<<")
            except UnicodeEncodeError:
                # Fallback for Windows encoding issues
                print(f">>> CRITICAL SYSTEM ERROR: {str(e).encode('ascii', 'replace').decode('ascii')} <<<")


def run_auto_refresh_scan():
    """API function for automated loops - returns status"""
    try:
        system = TurbopredictSystem()
        system.run_real_data_scanner(auto_refresh=True)
        return True
    except Exception as e:
        print(f"Auto-refresh failed: {e}")
        return False


def run_continuous_monitoring(interval_hours=1):
    """Run continuous monitoring loop with auto-refresh"""
    import time
    
    if COLORAMA_AVAILABLE:
        print(Fore.GREEN + "=" * 80)
        print("          TURBOPREDICT X PROTEAN - CONTINUOUS MONITORING MODE")
        print("=" * 80 + Style.RESET_ALL)
        print(Fore.CYAN + f"Monitoring interval: {interval_hours} hour(s)" + Style.RESET_ALL)
        print(Fore.YELLOW + "Press Ctrl+C to stop monitoring" + Style.RESET_ALL)
    else:
        print("=" * 80)
        print("          TURBOPREDICT X PROTEAN - CONTINUOUS MONITORING MODE")
        print("=" * 80)
        print(f"Monitoring interval: {interval_hours} hour(s)")
        print("Press Ctrl+C to stop monitoring")
    
    cycle_count = 1
    
    try:
        while True:
            if COLORAMA_AVAILABLE:
                print(Fore.MAGENTA + f"\n>>> MONITORING CYCLE #{cycle_count} - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} <<<" + Style.RESET_ALL)
            else:
                print(f"\n>>> MONITORING CYCLE #{cycle_count} - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} <<<")
            
            # Run auto-refresh scan
            success = run_auto_refresh_scan()
            
            if success:
                if COLORAMA_AVAILABLE:
                    print(Fore.GREEN + f"âœ“ Cycle #{cycle_count} completed successfully" + Style.RESET_ALL)
                else:
                    print(f"âœ“ Cycle #{cycle_count} completed successfully")
            else:
                if COLORAMA_AVAILABLE:
                    print(Fore.RED + f"âœ— Cycle #{cycle_count} failed" + Style.RESET_ALL)
                else:
                    print(f"âœ— Cycle #{cycle_count} failed")
            
            # Wait for next cycle
            wait_seconds = int(interval_hours * 3600)  # Convert hours to seconds
            
            if COLORAMA_AVAILABLE:
                print(Fore.CYAN + f"Waiting {interval_hours} hour(s) until next cycle..." + Style.RESET_ALL)
                print(Fore.YELLOW + f"Next scan at: {(datetime.now() + timedelta(hours=interval_hours)).strftime('%Y-%m-%d %H:%M:%S')}" + Style.RESET_ALL)
            else:
                print(f"Waiting {interval_hours} hour(s) until next cycle...")
                print(f"Next scan at: {(datetime.now() + timedelta(hours=interval_hours)).strftime('%Y-%m-%d %H:%M:%S')}")
            
            time.sleep(wait_seconds)
            cycle_count += 1
            
    except KeyboardInterrupt:
        if COLORAMA_AVAILABLE:
            print(Fore.YELLOW + f"\n>>> MONITORING STOPPED BY USER AFTER {cycle_count} CYCLES <<<" + Style.RESET_ALL)
        else:
            print(f"\n>>> MONITORING STOPPED BY USER AFTER {cycle_count} CYCLES <<<")
    except Exception as e:
        if COLORAMA_AVAILABLE:
            print(Fore.RED + f"\n>>> MONITORING FAILED: {e} <<<" + Style.RESET_ALL)
        else:
            print(f"\n>>> MONITORING FAILED: {e} <<<")
    def run_controlled_plots(self):
        """Run controlled plotting with smart limits"""
        try:
            print("CONTROLLED PLOT GENERATION")
            print("=" * 50)
            print("This will create plots with intelligent limits:")
            print("• Maximum 5 plots per unit")
            print("• Maximum 8 priority units per report")
            print("• Focus on most problematic tags only")
            print("• Automatic cleanup of old reports")
            print()

            # Import and run controlled plotting
            from controlled_anomaly_plots import create_controlled_plots
            from pi_monitor.plot_controls import PlotController

            controller = PlotController()

            if COLORAMA_AVAILABLE:
                print(Fore.CYAN + "Starting controlled analysis..." + Style.RESET_ALL)
            else:
                print("Starting controlled analysis...")

            report_path = create_controlled_plots(controller)

            if COLORAMA_AVAILABLE:
                print(Fore.GREEN + f"Controlled analysis completed!" + Style.RESET_ALL)
                print(Fore.YELLOW + f"Report location: {report_path}" + Style.RESET_ALL)
            else:
                print("Controlled analysis completed!")
                print(f"Report location: {report_path}")

        except Exception as e:
            print(f"Error in controlled plotting: {e}")
            import traceback
            traceback.print_exc()

        input("Press Enter to continue...")

    def cleanup_reports(self):
        """Clean up old report directories"""
        try:
            print("REPORT CLEANUP UTILITY")
            print("=" * 50)

            from pi_monitor.plot_controls import PlotController
            from pathlib import Path

            controller = PlotController()
            reports_dir = Path("reports")

            if not reports_dir.exists():
                print("No reports directory found.")
                input("Press Enter to continue...")
                return

            # Check current usage
            excessive = controller.check_disk_usage_alert(reports_dir)
            if excessive:
                if COLORAMA_AVAILABLE:
                    print(Fore.RED + "Report disk usage is excessive!" + Style.RESET_ALL)
                else:
                    print("Report disk usage is excessive!")

            print("Current report directories:")
            report_dirs = [d for d in reports_dir.iterdir() if d.is_dir()]
            report_dirs.sort(key=lambda x: x.stat().st_mtime, reverse=True)

            for i, report_dir in enumerate(report_dirs[:10]):
                try:
                    files = list(report_dir.glob('**/*'))
                    file_count = len([f for f in files if f.is_file()])
                    size_mb = sum(f.stat().st_size for f in files if f.is_file()) / (1024 * 1024)
                    mtime = datetime.fromtimestamp(report_dir.stat().st_mtime)
                    print(f"  {i+1:2d}. {report_dir.name}")
                    print(f"      Created: {mtime.strftime('%Y-%m-%d %H:%M')}")
                    print(f"      Files: {file_count:,} | Size: {size_mb:.1f}MB")
                except:
                    print(f"  {i+1:2d}. {report_dir.name} (error reading)")

            if len(report_dirs) > 10:
                print(f"      ... and {len(report_dirs) - 10} more directories")

            print()
            try:
                response = input("Run cleanup? (y/N): ").strip().lower()
            except EOFError:
                return

            if response == 'y':
                print("Running cleanup...")
                stats = controller.cleanup_old_reports(reports_dir)

                print(f"Cleanup completed!")
                print(f"  Directories removed: {stats['cleaned']}")
                print(f"  Space reclaimed: {stats['space_reclaimed_mb']:.1f}MB")

                if stats['errors']:
                    print(f"  Errors: {len(stats['errors'])}")
            else:
                print("Cleanup cancelled.")

        except Exception as e:
            print(f"Error in cleanup: {e}")
            import traceback
            traceback.print_exc()

        input("Press Enter to continue...")


def main():
    """Main entry point"""
    import sys
    
    # Check for command line arguments
    if len(sys.argv) > 1:
        if sys.argv[1] == "--auto-refresh":
            # Single auto-refresh scan
            return run_auto_refresh_scan()
        elif sys.argv[1] == "--loop":
            # Continuous monitoring loop (1 hour intervals)
            interval = 1.0  # Default 1 hour
            if len(sys.argv) > 2:
                try:
                    interval = float(sys.argv[2])
                except ValueError:
                    print("Invalid interval. Using default 1 hour.")
            return run_continuous_monitoring(interval)
        elif sys.argv[1] == "--hourly-loop":
            # New: Combined loop [1] then [2] each interval hour
            interval = 1.0
            if len(sys.argv) > 2:
                try:
                    interval = float(sys.argv[2])
                except ValueError:
                    print("Invalid interval. Using default 1 hour.")
            system = TurbopredictSystem()
            system.run_hourly_auto_loop(interval_hours=interval)
            return
        elif sys.argv[1] == "--hourly-once":
            # New: Single cycle of [1] then [2]
            system = TurbopredictSystem()
            system.run_real_data_scanner(auto_refresh=True)
            system.run_unit_analysis()
            return
        elif sys.argv[1] == "--help":
            print("TURBOPREDICT X PROTEAN - Command Line Options:")
            print("  python turbopredict.py                    # Interactive mode")
            print("  python turbopredict.py --auto-refresh     # Single auto-refresh")
            print("  python turbopredict.py --loop [hours]     # Continuous loop (default: 1 hour)")
            print("  python turbopredict.py --hourly-loop [h]  # Hourly loop: [1] then [2] until Ctrl+C")
            print("  python turbopredict.py --hourly-once      # Run [1] then [2] once and exit")
            print("  python turbopredict.py --loop 0.5         # Every 30 minutes")
            print("  python turbopredict.py --loop 2           # Every 2 hours")
            return
    
    # Interactive mode
    system = TurbopredictSystem()
    system.run()


def main_loop_auto_refresh(max_age_hours=1.0, single_run=True):
    """Main loop for auto refresh with age threshold and single run option.

    Args:
        max_age_hours (float): Maximum age in hours for data freshness
        single_run (bool): If True, run once and exit; if False, run continuously

    Returns:
        bool: Success status
    """
    try:
        system = TurbopredictSystem()

        if single_run:
            # Run auto-refresh scan once
            system.run_real_data_scanner(auto_refresh=True)
            print(f"Auto-refresh completed (max age: {max_age_hours}h)")
            return True
        else:
            # Run continuous monitoring
            run_continuous_monitoring(interval_hours=max_age_hours)
            return True

    except Exception as e:
        print(f"main_loop_auto_refresh failed: {e}")
        return False


if __name__ == "__main__":
    main()



