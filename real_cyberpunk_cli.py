#!/usr/bin/env python3
"""
TURBOPREDICT X PROTEAN - Real Data Cyberpunk CLI
Works with actual Parquet files in your data directory
"""

import time
import os
import sys
from pathlib import Path

# Add current directory to Python path
current_dir = Path(__file__).parent
sys.path.insert(0, str(current_dir))

try:
    from rich.console import Console
    from rich.panel import Panel
    from rich.prompt import Prompt, Confirm
    from rich.table import Table
    from rich.progress import Progress, SpinnerColumn, TextColumn
    from rich import box
    RICH_AVAILABLE = True
except ImportError:
    RICH_AVAILABLE = False

try:
    from colorama import init, Fore, Back, Style
    init(autoreset=True)
    COLORAMA_AVAILABLE = True
except ImportError:
    COLORAMA_AVAILABLE = False

# Import our real data modules
try:
    from pi_monitor.parquet_database import ParquetDatabase
    from pi_monitor.parquet_auto_scan import ParquetAutoScanner
    from pi_monitor.config import Config
    DATA_MODULES_AVAILABLE = True
except ImportError as e:
    print(f"Warning: Could not import data modules: {e}")
    DATA_MODULES_AVAILABLE = False

class RealCyberpunkCLI:
    """Real cyberpunk interface using actual Parquet data"""
    
    def __init__(self):
        self.console = None
        if RICH_AVAILABLE:
            try:
                self.console = Console(force_terminal=True, no_color=False, width=80)
            except:
                self.console = None
        
        # Initialize data systems
        if DATA_MODULES_AVAILABLE:
            try:
                self.config = Config()
                self.db = ParquetDatabase()
                self.scanner = ParquetAutoScanner(self.config)
                self.data_available = True
            except Exception as e:
                print(f"Warning: Could not initialize data systems: {e}")
                self.data_available = False
        else:
            self.data_available = False
    
    def get_banner(self):
        """ASCII-safe banner"""
        return """
+========================================================================+
|  TURBOPREDICT X PROTEAN - REAL DATA NEURAL INTERFACE                  |
|                                                                        |
|  TTTTT U   U RRRR  BBBB   OOO  PPPP  RRRR  EEEEE DDDD  III  CCCC TTTTT|
|    T   U   U R   R B   B O   O P   P R   R E     D   D  I  C   C  T   |
|    T   U   U RRRR  BBBB  O   O PPPP  RRRR  EEEE  D   D  I  C      T   |
|    T   U   U R  R  B   B O   O P     R  R  E     D   D  I  C   C  T   |
|    T    UUU  R   R BBBB   OOO  P     R   R EEEEE DDDD  III  CCCC  T   |
|                                                                        |
|                  >>> REAL DATA QUANTUM PROCESSORS <<<                 |
|                  >>> CONNECTED TO PARQUET MATRIX <<<                  |
+========================================================================+
        """
    
    def display_banner(self):
        """Display banner with system status"""
        if self.console:
            try:
                status_text = "ONLINE - PARQUET DATA DETECTED" if self.data_available else "OFFLINE - NO DATA CONNECTION"
                status_style = "bright_green" if self.data_available else "bright_red"
                
                banner_panel = Panel(
                    self.get_banner(),
                    title="[bold cyan]SYSTEM INITIALIZATION[/]",
                    subtitle=f"[{status_style}]>>> {status_text} <<<[/]",
                    style="bright_blue"
                )
                self.console.clear()
                self.console.print(banner_panel)
                
                # Loading animation
                with Progress(
                    SpinnerColumn(),
                    TextColumn("Connecting to Parquet matrix..."),
                    transient=True,
                    console=self.console
                ) as progress:
                    task = progress.add_task("", total=50)
                    for i in range(50):
                        time.sleep(0.02)
                        progress.update(task, advance=1)
                
                status_msg = ">>> PARQUET MATRIX ONLINE <<<" if self.data_available else ">>> DATA CONNECTION FAILED <<<"
                style = "bold green" if self.data_available else "bold red"
                self.console.print(f"[{style}]{status_msg}[/]\n")
                return
            except:
                pass
        
        # Fallback display
        if COLORAMA_AVAILABLE:
            os.system('cls' if os.name == 'nt' else 'clear')
            print(Fore.CYAN + Style.BRIGHT + self.get_banner())
            if self.data_available:
                print(Fore.GREEN + ">>> PARQUET MATRIX ONLINE <<<" + Style.RESET_ALL)
            else:
                print(Fore.RED + ">>> DATA CONNECTION FAILED <<<" + Style.RESET_ALL)
        else:
            os.system('cls' if os.name == 'nt' else 'clear')
            print(self.get_banner())
            print(">>> PARQUET MATRIX ONLINE <<<" if self.data_available else ">>> DATA CONNECTION FAILED <<<")
    
    def show_menu(self):
        """Display menu"""
        if self.console:
            try:
                menu_table = Table(title="[bold magenta]REAL DATA NEURAL COMMAND MATRIX[/]")
                menu_table.add_column("CMD", style="bold cyan", width=5)
                menu_table.add_column("Operation", style="bold green", width=25)
                menu_table.add_column("Description", style="yellow", width=35)
                
                menu_options = [
                    ("1", "SCAN ALL UNITS", "Scan all units in Parquet data"),
                    ("2", "UNIT ANALYSIS", "Analyze specific unit data"),
                    ("3", "DATABASE STATUS", "Real Parquet database status"),
                    ("4", "DATA QUALITY", "Generate quality reports"),
                    ("5", "COMPREHENSIVE SCAN", "Full system analysis"),
                    ("6", "UNIT LIST", "Show all available units"),
                    ("7", "FILE EXPLORER", "Browse Parquet files"),
                    ("0", "DISCONNECT", "Terminate neural link")
                ]
                
                for cmd, op, desc in menu_options:
                    menu_table.add_row(cmd, op, desc)
                
                self.console.print(menu_table)
                
                if not self.data_available:
                    self.console.print("[bold red]WARNING: Data systems offline. Some features unavailable.[/]")
                
                choice = Prompt.ask(
                    "[bold magenta]>>> SELECT NEURAL PATHWAY[/]",
                    choices=["0","1","2","3","4","5","6","7"],
                    default="3",
                    console=self.console
                )
                return choice
            except:
                pass
        
        # Fallback display
        menu_text = """
+============================================================+
|              REAL DATA NEURAL COMMAND MATRIX              |
+============================================================+
| 1. SCAN ALL UNITS    - Scan all units in Parquet data   |
| 2. UNIT ANALYSIS     - Analyze specific unit data       |
| 3. DATABASE STATUS   - Real Parquet database status     |
| 4. DATA QUALITY      - Generate quality reports         |
| 5. COMPREHENSIVE SCAN - Full system analysis            |
| 6. UNIT LIST         - Show all available units         |
| 7. FILE EXPLORER     - Browse Parquet files             |
| 0. DISCONNECT        - Terminate neural link            |
+============================================================+
        """
        
        if COLORAMA_AVAILABLE:
            print(Fore.CYAN + menu_text + Style.RESET_ALL)
            if not self.data_available:
                print(Fore.RED + "WARNING: Data systems offline." + Style.RESET_ALL)
            choice = input(Fore.MAGENTA + ">>> SELECT PATHWAY: " + Style.RESET_ALL)
        else:
            print(menu_text)
            if not self.data_available:
                print("WARNING: Data systems offline.")
            choice = input(">>> SELECT PATHWAY: ")
        
        return choice.strip()
    
    def scan_all_units(self):
        """Scan all units in the database"""
        if not self.data_available:
            self._show_no_data_message()
            return
        
        try:
            # Auto-seed any configured-but-missing units (e.g., PCMSB C-02001)
            try:
                from pi_monitor.parquet_auto_scan import ParquetAutoScanner
                from pathlib import Path as _P
                # Prefer plant-specific workbook for PCMSB if present
                xlsx_candidates = [
                    _P("excel/ABF_Automation.xlsx"),
                    _P("excel/PCMSB_Automation.xlsx"),
                    _P("excel/PCFS_Automation.xlsx"),
                    _P("data/raw/Automation.xlsx"),
                    _P("Automation.xlsx"),
                ]
                xlsx_path = next((p for p in xlsx_candidates if p.exists()), None)
                if xlsx_path is not None:
                    # Use the scanner's auto-build to seed missing units before scanning
                    try:
                        seeded = self.scanner._auto_build_missing_units(xlsx_path)  # type: ignore[attr-defined]
                        if seeded:
                            # Recreate DB to pick up new files
                            from pi_monitor.parquet_database import ParquetDatabase
                            self.db = ParquetDatabase()
                    except Exception:
                        pass
            except Exception:
                pass

            if self.console:
                self.console.print("[bold green]>>> SCANNING ALL UNITS <<<[/]")
                
                with Progress(
                    SpinnerColumn(),
                    TextColumn("Scanning Parquet matrix..."),
                    console=self.console
                ) as progress:
                    task = progress.add_task("", total=None)
                    results = self.scanner.scan_all_units()
                
                # Display results
                summary_table = Table(title="[bold magenta]UNIT SCAN RESULTS[/]")
                summary_table.add_column("Metric", style="bold cyan")
                summary_table.add_column("Value", style="bold green")
                
                summary = results['summary']
                summary_table.add_row("Total Units", str(summary['total_units']))
                summary_table.add_row("Fresh Units", str(summary['fresh_units']))
                summary_table.add_row("Stale Units", str(summary['stale_units']))
                summary_table.add_row("Empty Units", str(summary['empty_units']))
                summary_table.add_row("Total Records", f"{summary['total_records']:,}")
                summary_table.add_row("Freshness Rate", f"{summary['freshness_rate']:.1%}")
                
                self.console.print(summary_table)
                
                # Show unit details (all units)
                if results['units_scanned']:
                    unit_table = Table(title="[bold cyan]UNIT DETAILS[/]")
                    unit_table.add_column("Unit", style="cyan")
                    unit_table.add_column("Records", style="green")
                    unit_table.add_column("Tags", style="yellow")
                    unit_table.add_column("Age (hrs)", style="red")
                    unit_table.add_column("Status", style="white")
                    
                    for unit_info in results['units_scanned']:
                        age_str = f"{unit_info['data_age_hours']:.1f}" if unit_info['data_age_hours'] else "N/A"
                        status = "FRESH" if not unit_info['is_stale'] else "STALE"
                        
                        unit_table.add_row(
                            unit_info['unit'],
                            f"{unit_info['total_records']:,}",
                            str(unit_info['unique_tags']),
                            age_str,
                            status
                        )
                    
                    self.console.print(unit_table)
                
                Prompt.ask("[dim]Press Enter to continue...[/]", default="", console=self.console)
            else:
                print(">>> SCANNING ALL UNITS <<<")
                results = self.scanner.scan_all_units()
                summary = results['summary']
                print(f"Total Units: {summary['total_units']}")
                print(f"Fresh Units: {summary['fresh_units']}")
                print(f"Stale Units: {summary['stale_units']}")
                print(f"Total Records: {summary['total_records']:,}")
                input("Press Enter to continue...")
                
        except Exception as e:
            if self.console:
                self.console.print(f"[bold red]>>> ERROR: {e} <<<[/]")
            else:
                print(f"ERROR: {e}")
            if self.console:
                Prompt.ask("[dim]Press Enter to continue...[/]", default="", console=self.console)
            else:
                input("Press Enter to continue...")
    
    def analyze_unit(self):
        """Analyze a specific unit"""
        if not self.data_available:
            self._show_no_data_message()
            return
        
        try:
            # Get available units
            units = self.db.get_all_units()
            
            if not units:
                if self.console:
                    self.console.print("[bold red]>>> NO UNITS FOUND <<<[/]")
                else:
                    print(">>> NO UNITS FOUND <<<")
                return
            
            # Show available units
            if self.console:
                unit_list = ", ".join(units[:5])
                if len(units) > 5:
                    unit_list += f" (and {len(units)-5} more)"
                
                self.console.print(f"[bold cyan]Available units: {unit_list}[/]")
                unit = Prompt.ask("[bold magenta]Enter unit to analyze[/]", 
                                 choices=units, default=units[0], console=self.console)
            else:
                print(f"Available units: {', '.join(units[:5])}")
                unit = input("Enter unit to analyze: ").strip() or units[0]
            
            # Analyze the unit (auto-seed if missing)
            if self.console:
                self.console.print(f"[bold green]>>> ANALYZING UNIT: {unit} <<<[/]")
                with Progress(
                    SpinnerColumn(),
                    TextColumn("Analyzing unit data..."),
                    console=self.console
                ) as progress:
                    task = progress.add_task("", total=None)
                    analysis = self.scanner.analyze_unit_data(unit)
            else:
                print(f">>> ANALYZING UNIT: {unit} <<<")
                analysis = self.scanner.analyze_unit_data(unit)

            # If no data yet, try to auto-build from config and retry once
            if analysis.get('status') == 'no_data':
                try:
                    from pathlib import Path as _P
                    xlsx_candidates = [
                        _P("excel/ABF_Automation.xlsx"),
                        _P("excel/PCMSB_Automation.xlsx"),
                        _P("excel/PCFS_Automation.xlsx"),
                        _P("data/raw/Automation.xlsx"),
                        _P("Automation.xlsx"),
                    ]
                    xlsx_path = next((p for p in xlsx_candidates if p.exists()), None)
                    if xlsx_path is not None:
                        try:
                            seeded = self.scanner._auto_build_missing_units(xlsx_path)  # type: ignore[attr-defined]
                            if seeded:
                                from pi_monitor.parquet_database import ParquetDatabase
                                self.db = ParquetDatabase()
                                analysis = self.scanner.analyze_unit_data(unit)
                        except Exception:
                            pass
                except Exception:
                    pass
            
            # Display results
            self._display_unit_analysis(analysis)
            
        except Exception as e:
            if self.console:
                self.console.print(f"[bold red]>>> ERROR: {e} <<<[/]")
                Prompt.ask("[dim]Press Enter to continue...[/]", default="", console=self.console)
            else:
                print(f"ERROR: {e}")
                input("Press Enter to continue...")
    
    def _display_unit_analysis(self, analysis):
        """Display unit analysis results"""
        if analysis['status'] == 'no_data':
            if self.console:
                self.console.print(f"[bold red]>>> NO DATA FOR UNIT: {analysis['unit']} <<<[/]")
                Prompt.ask("[dim]Press Enter to continue...[/]", default="", console=self.console)
            else:
                print(f">>> NO DATA FOR UNIT: {analysis['unit']} <<<")
                input("Press Enter to continue...")
            return
        
        if self.console:
            # Basic info
            info_table = Table(title=f"[bold magenta]UNIT ANALYSIS: {analysis['unit']}[/]")
            info_table.add_column("Metric", style="bold cyan")
            info_table.add_column("Value", style="bold green")
            
            info_table.add_row("Records", f"{analysis['records']:,}")
            info_table.add_row("Unique Tags", str(analysis['unique_tags']))
            
            if analysis['date_range']['start']:
                info_table.add_row("Start Date", analysis['date_range']['start'])
                info_table.add_row("End Date", analysis['date_range']['end'])
            
            if 'value_stats' in analysis:
                stats = analysis['value_stats']
                info_table.add_row("Value Count", f"{stats['count']:,}")
                if stats['mean'] is not None:
                    info_table.add_row("Mean Value", f"{stats['mean']:.2f}")
                    info_table.add_row("Std Deviation", f"{stats['std']:.2f}")
                    info_table.add_row("Min Value", f"{stats['min']:.2f}")
                    info_table.add_row("Max Value", f"{stats['max']:.2f}")
                info_table.add_row("Null Values", f"{stats['null_count']:,}")
            
            self.console.print(info_table)
            
            # Tag details
            if analysis['tags']:
                tag_table = Table(title="[bold cyan]TAG STATISTICS[/]")
                tag_table.add_column("Tag", style="cyan", width=20)
                tag_table.add_column("Count", style="green")
                tag_table.add_column("Mean", style="yellow")
                tag_table.add_column("Std", style="blue")
                tag_table.add_column("Hours Since Last", style="red")
                
                for tag in analysis['tags'][:10]:  # Show top 10 tags
                    tag_table.add_row(
                        tag.get('tag', '')[:18],  # Truncate long tag names
                        f"{tag.get('value_count', 0):,}",
                        f"{tag.get('value_mean', 0):.2f}",
                        f"{tag.get('value_std', 0):.2f}",
                        f"{tag.get('hours_since_last', 0):.1f}" if tag.get('hours_since_last') else "N/A"
                    )
                
                self.console.print(tag_table)
            
            # Anomalies
            if 'anomalies' in analysis and analysis['anomalies']['total_anomalies'] > 0:
                anom = analysis['anomalies']
                self.console.print(Panel(
                    f"[bold red]ANOMALIES DETECTED[/]\n\n"
                    f"[yellow]Total Anomalies: {anom['total_anomalies']}[/]\n"
                    f"[yellow]Anomaly Rate: {anom['anomaly_rate']:.2%}[/]",
                    title="[bold red]ALERT[/]"
                ))
            
            Prompt.ask("[dim]Press Enter to continue...[/]", default="", console=self.console)
        else:
            print(f"Unit: {analysis['unit']}")
            print(f"Records: {analysis['records']:,}")
            print(f"Unique Tags: {analysis['unique_tags']}")
            if 'anomalies' in analysis:
                print(f"Anomalies: {analysis['anomalies']['total_anomalies']}")
            input("Press Enter to continue...")
    
    def show_database_status(self):
        """Show real database status"""
        if not self.data_available:
            self._show_no_data_message()
            return
        
        try:
            if self.console:
                self.console.print("[bold green]>>> SCANNING PARQUET DATABASE <<<[/]")
                
                with Progress(
                    SpinnerColumn(),
                    TextColumn("Scanning Parquet files..."),
                    console=self.console
                ) as progress:
                    task = progress.add_task("", total=None)
                    status = self.db.get_database_status()
                
                # Database overview
                db_panel = Panel(
                    f"[bold green]>>> PARQUET DATABASE STATUS <<<[/]\n\n"
                    f"[cyan]Data Directory:[/] {status['processed_directory']}\n"
                    f"[cyan]Total Files:[/] {status['total_files']}\n"
                    f"[cyan]Total Size:[/] {status['total_size_gb']:.2f} GB\n"
                    f"[cyan]DuckDB Available:[/] {'Yes' if status['duckdb_available'] else 'No'}",
                    title="[bold magenta]QUANTUM STORAGE MATRIX[/]"
                )
                self.console.print(db_panel)
                
                # Unit status
                if status['units']:
                    unit_table = Table(title="[bold cyan]UNIT STATUS[/]")
                    unit_table.add_column("Unit", style="cyan")
                    unit_table.add_column("Files", style="green")
                    unit_table.add_column("Size (MB)", style="yellow")
                    unit_table.add_column("Records", style="blue")
                    unit_table.add_column("Age (hrs)", style="red")
                    unit_table.add_column("Status", style="white")
                    
                    for unit in status['units'][:10]:  # Show top 10
                        age_str = f"{unit['data_age_hours']:.1f}" if unit['data_age_hours'] else "N/A"
                        status_str = "FRESH" if not unit['is_stale'] else "STALE"
                        
                        unit_table.add_row(
                            unit['unit'],
                            str(unit['files']),
                            f"{unit['size_mb']:.1f}",
                            f"{unit['records']:,}",
                            age_str,
                            status_str
                        )
                    
                    self.console.print(unit_table)
                
                Prompt.ask("[dim]Press Enter to continue...[/]", default="", console=self.console)
            else:
                print(">>> PARQUET DATABASE STATUS <<<")
                status = self.db.get_database_status()
                print(f"Total Files: {status['total_files']}")
                print(f"Total Size: {status['total_size_gb']:.2f} GB")
                print(f"Units: {len(status['units'])}")
                input("Press Enter to continue...")
                
        except Exception as e:
            if self.console:
                self.console.print(f"[bold red]>>> ERROR: {e} <<<[/]")
                Prompt.ask("[dim]Press Enter to continue...[/]", default="", console=self.console)
            else:
                print(f"ERROR: {e}")
                input("Press Enter to continue...")
    
    def show_unit_list(self):
        """Show all available units"""
        if not self.data_available:
            self._show_no_data_message()
            return
        
        try:
            units = self.db.get_all_units()
            
            if self.console:
                if units:
                    self.console.print(Panel(
                        "\n".join([f"• {unit}" for unit in units]),
                        title=f"[bold magenta]AVAILABLE UNITS ({len(units)})[/]"
                    ))
                else:
                    self.console.print("[bold red]>>> NO UNITS FOUND <<<[/]")
                
                Prompt.ask("[dim]Press Enter to continue...[/]", default="", console=self.console)
            else:
                print(f"Available Units ({len(units)}):")
                for unit in units:
                    print(f"  - {unit}")
                input("Press Enter to continue...")
                
        except Exception as e:
            if self.console:
                self.console.print(f"[bold red]>>> ERROR: {e} <<<[/]")
                Prompt.ask("[dim]Press Enter to continue...[/]", default="", console=self.console)
            else:
                print(f"ERROR: {e}")
                input("Press Enter to continue...")
    
    def _show_no_data_message(self):
        """Show no data available message"""
        if self.console:
            self.console.print(Panel(
                "[bold red]>>> DATA SYSTEMS OFFLINE <<<[/]\n\n"
                "Cannot access Parquet database.\n"
                "Please ensure:\n"
                "• Python modules are installed\n"
                "• Data directory exists\n"
                "• Parquet files are accessible",
                title="[bold red]ERROR[/]"
            ))
            Prompt.ask("[dim]Press Enter to continue...[/]", default="", console=self.console)
        else:
            print(">>> DATA SYSTEMS OFFLINE <<<")
            print("Cannot access Parquet database.")
            input("Press Enter to continue...")
    
    def _show_coming_soon(self, feature):
        """Show coming soon message"""
        if self.console:
            self.console.print(Panel(
                f"[bold yellow]>>> {feature} <<<[/]\n\n"
                "Neural pathways under construction...\n"
                "Feature available in next quantum update",
                title="[bold magenta]COMING SOON[/]"
            ))
            Prompt.ask("[dim]Press Enter to continue...[/]", default="", console=self.console)
        else:
            print(f">>> {feature} - COMING SOON <<<")
            input("Press Enter to continue...")
    
    def shutdown(self):
        """Shutdown sequence"""
        if self.console:
            try:
                with Progress(
                    SpinnerColumn(),
                    TextColumn("Terminating Parquet matrix link..."),
                    transient=True,
                    console=self.console
                ) as progress:
                    task = progress.add_task("", total=30)
                    for i in range(30):
                        time.sleep(0.03)
                        progress.update(task, advance=1)
                
                self.console.print(Panel(
                    "[bold red]>>> PARQUET MATRIX LINK TERMINATED <<<[/]\n\n"
                    "Quantum processors offline\n"
                    "Neural pathways disconnected\n"
                    "Matrix interface shutdown complete",
                    title="[bold red]SYSTEM OFFLINE[/]"
                ))
                return
            except:
                pass
        
        # Fallback
        if COLORAMA_AVAILABLE:
            print(Fore.RED + ">>> PARQUET MATRIX LINK TERMINATED <<<" + Style.RESET_ALL)
        else:
            print(">>> PARQUET MATRIX LINK TERMINATED <<<")
        print("Matrix interface shutdown complete")
    
    def run(self):
        """Main application loop"""
        try:
            self.display_banner()
            
            while True:
                choice = self.show_menu()
                
                if choice == "0":
                    self.shutdown()
                    break
                elif choice == "1":
                    self.scan_all_units()
                elif choice == "2":
                    self.analyze_unit()
                elif choice == "3":
                    self.show_database_status()
                elif choice == "4":
                    self._show_coming_soon("DATA QUALITY REPORTS")
                elif choice == "5":
                    self._show_coming_soon("COMPREHENSIVE SYSTEM SCAN")
                elif choice == "6":
                    self.show_unit_list()
                elif choice == "7":
                    self._show_coming_soon("FILE EXPLORER")
                else:
                    if COLORAMA_AVAILABLE:
                        print(Fore.RED + ">>> INVALID NEURAL PATHWAY <<<" + Style.RESET_ALL)
                    else:
                        print(">>> INVALID PATHWAY <<<")
                    time.sleep(1)
                    
        except KeyboardInterrupt:
            self.shutdown()
        except Exception as e:
            if self.console:
                self.console.print(f"[bold red]>>> SYSTEM ERROR: {e} <<<[/]")
            else:
                print(f">>> SYSTEM ERROR: {e} <<<")

def main():
    """Entry point"""
    cli = RealCyberpunkCLI()
    cli.run()

if __name__ == "__main__":
    main()
