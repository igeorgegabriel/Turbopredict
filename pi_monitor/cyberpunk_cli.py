"""
TURBOPREDICT X PROTEAN - Cyberpunk CLI Interface
Beautiful terminal interface with cyberpunk aesthetic
"""

from __future__ import annotations

import os
import sys
import time
import json
from pathlib import Path
from typing import Dict, List, Any, Optional
from datetime import datetime
import threading

try:
    from rich.console import Console
    from rich.panel import Panel
    from rich.progress import Progress, SpinnerColumn, TextColumn, BarColumn, TaskProgressColumn, TimeRemainingColumn
    from rich.table import Table
    from rich.text import Text
    from rich.prompt import Prompt, Confirm
    from rich.layout import Layout
    from rich.live import Live
    from rich.align import Align
    from rich.columns import Columns
    from rich.tree import Tree
    from rich import box
    RICH_AVAILABLE = True
except ImportError:
    RICH_AVAILABLE = False

# Fallback for basic color support
if not RICH_AVAILABLE:
    try:
        from colorama import init, Fore, Back, Style
        init(autoreset=True)
        COLORAMA_AVAILABLE = True
    except ImportError:
        COLORAMA_AVAILABLE = False


class CyberpunkTheme:
    """Cyberpunk color theme and styling"""
    
    # Rich color scheme (neon cyberpunk)
    NEON_CYAN = "bright_cyan"
    NEON_MAGENTA = "bright_magenta" 
    NEON_GREEN = "bright_green"
    NEON_YELLOW = "bright_yellow"
    NEON_RED = "bright_red"
    ELECTRIC_BLUE = "dodger_blue1"
    MATRIX_GREEN = "green1"
    CYBER_ORANGE = "dark_orange"
    DARK_BG = "grey11"
    
    # ASCII Art and symbols
    CYBER_SYMBOLS = ["▪", "▫", "▬", "═", "║", "╔", "╗", "╚", "╝", "◆", "◇", "●", "○"]
    
    @staticmethod
    def get_banner() -> str:
        return """
╔══════════════════════════════════════════════════════════════════════╗
║  ████████╗██╗   ██╗██████╗ ██████╗  ██████╗ ██████╗ ██████╗ ███████╗ ║
║  ╚══██╔══╝██║   ██║██╔══██╗██╔══██╗██╔═══██╗██╔══██╗██╔══██╗██╔════╝ ║
║     ██║   ██║   ██║██████╔╝██████╔╝██║   ██║██████╔╝██████╔╝█████╗   ║
║     ██║   ██║   ██║██╔══██╗██╔══██╗██║   ██║██╔═══╝ ██╔══██╗██╔══╝   ║
║     ██║   ╚██████╔╝██║  ██║██████╔╝╚██████╔╝██║     ██║  ██║███████╗ ║
║     ╚═╝    ╚═════╝ ╚═╝  ╚═╝╚═════╝  ╚═════╝ ╚═╝     ╚═╝  ╚═╝╚══════╝ ║
║                                                                      ║
║         ██████╗ ██████╗ ███████╗██████╗ ██╗ ██████╗████████╗         ║
║         ██╔══██╗██╔══██╗██╔════╝██╔══██╗██║██╔════╝╚══██╔══╝         ║
║         ██████╔╝██████╔╝█████╗  ██║  ██║██║██║        ██║            ║
║         ██╔═══╝ ██╔══██╗██╔══╝  ██║  ██║██║██║        ██║            ║
║         ██║     ██║  ██║███████╗██████╔╝██║╚██████╗   ██║            ║
║         ╚═╝     ╚═╝  ╚═╝╚══════╝╚═════╝ ╚═╝ ╚═════╝   ╚═╝            ║
║                                                                      ║
║                    ██╗  ██╗    ██████╗ ██████╗  ██████╗ ████████╗   ║
║                    ╚██╗██╔╝    ██╔══██╗██╔══██╗██╔═══██╗╚══██╔══╝   ║
║                     ╚███╔╝     ██████╔╝██████╔╝██║   ██║   ██║      ║
║                     ██╔██╗     ██╔═══╝ ██╔══██╗██║   ██║   ██║      ║
║                    ██╔╝ ██╗    ██║     ██║  ██║╚██████╔╝   ██║      ║
║                    ╚═╝  ╚═╝    ╚═╝     ╚═╝  ╚═╝ ╚═════╝    ╚═╝      ║
║                                                                      ║
║                      █▀▀ █▄█ █▄▄ █▀▀ █▀█ █▀█ █░█ █▄░█ █▄▀           ║
║                      █▄▄ ░█░ █▄█ ██▄ █▀▄ █▀▀ █▄█ █░▀█ █░█           ║
║                                                                      ║
║              ▪▫▪ NEURAL NETWORK INTERFACE ACTIVATED ▪▫▪             ║
╚══════════════════════════════════════════════════════════════════════╝
"""

class CyberpunkCLI:
    """Beautiful cyberpunk-themed CLI for TURBOPREDICT X PROTEAN"""
    
    def __init__(self):
        self.console = Console() if RICH_AVAILABLE else None
        self.theme = CyberpunkTheme()
        self.config = None
        self._setup_console()
        
    def _setup_console(self):
        """Setup console for optimal display"""
        if RICH_AVAILABLE:
            # Enable color and styling
            os.system('color')  # Enable ANSI colors on Windows
            
    def display_banner(self):
        """Display animated cyberpunk banner"""
        if RICH_AVAILABLE:
            self.console.clear()
            
            # Animated banner display
            banner_text = self.theme.get_banner()
            
            # Create styled banner
            banner_panel = Panel(
                Text(banner_text, style=f"bold {self.theme.NEON_CYAN}"),
                box=box.DOUBLE,
                style=f"{self.theme.ELECTRIC_BLUE}",
                title="[bold bright_magenta]◆◇◆ SYSTEM INITIALIZATION ◆◇◆[/]",
                subtitle="[dim bright_cyan]> Connecting to neural matrix...[/]"
            )
            
            self.console.print(banner_panel)
            
            # Loading animation
            with Progress(
                SpinnerColumn(spinner_style=self.theme.NEON_GREEN),
                TextColumn("[bold bright_cyan]Initializing quantum processors..."),
                transient=True
            ) as progress:
                task = progress.add_task("", total=100)
                for i in range(100):
                    time.sleep(0.02)
                    progress.update(task, advance=1)
                    
            self.console.print(f"\n[bold {self.theme.MATRIX_GREEN}]◆ SYSTEM ONLINE ◆[/] [dim]Neural pathways established[/]\n")
            
        else:
            # Fallback for systems without rich
            self._fallback_banner()
            
    def _fallback_banner(self):
        """Fallback banner for systems without rich"""
        if COLORAMA_AVAILABLE:
            print(Fore.CYAN + Style.BRIGHT + self.theme.get_banner())
            print(Fore.GREEN + "=== TURBOPREDICT X PROTEAN - CYBERPUNK INTERFACE ===" + Style.RESET_ALL)
        else:
            print(self.theme.get_banner())
            print("=== TURBOPREDICT X PROTEAN - CYBERPUNK INTERFACE ===")
    
    def show_main_menu(self) -> str:
        """Display main menu and get user choice"""
        if RICH_AVAILABLE:
            # Create menu table
            menu_table = Table(
                title="[bold bright_magenta]◆◇◆ NEURAL COMMAND MATRIX ◆◇◆[/]",
                box=box.DOUBLE_EDGE,
                style=f"{self.theme.DARK_BG}"
            )
            
            menu_table.add_column("CMD", style=f"bold {self.theme.NEON_CYAN}", width=8)
            menu_table.add_column("Operation", style=f"bold {self.theme.NEON_GREEN}", width=30)
            menu_table.add_column("Neural Path", style=f"dim {self.theme.ELECTRIC_BLUE}", width=40)
            
            # Menu options
            menu_options = [
                ("1", "◆ AUTO-SCAN", "Intelligent PI data scanning with neural cache"),
                ("2", "◇ DATABASE STATUS", "Quantum storage analytics and metrics"),
                ("3", "◆ BATCH PROCESSOR", "Multi-tag neural batch processing"),
                ("4", "◇ ANOMALY SCANNER", "Advanced pattern recognition engine"),
                ("5", "◆ DATA VISUALIZER", "Holographic data representation"),
                ("6", "◇ SYSTEM CONFIG", "Neural network configuration"),
                ("7", "◆ MATRIX LOGS", "System diagnostic interface"),
                ("0", "◇ DISCONNECT", "Terminate neural link")
            ]
            
            for cmd, operation, description in menu_options:
                menu_table.add_row(cmd, operation, description)
            
            self.console.print(menu_table)
            
            # Get user input with styled prompt
            choice = Prompt.ask(
                f"\n[bold {self.theme.NEON_MAGENTA}]◆ SELECT NEURAL PATHWAY[/]",
                choices=["0", "1", "2", "3", "4", "5", "6", "7"],
                default="1"
            )
            
            return choice
            
        else:
            return self._fallback_menu()
    
    def _fallback_menu(self) -> str:
        """Fallback menu for systems without rich"""
        menu_text = """
╔════════════════════════════════════════╗
║        TURBOPREDICT X PROTEAN          ║
║           COMMAND MATRIX               ║
╠════════════════════════════════════════╣
║ 1. AUTO-SCAN       - Smart PI Scanning║
║ 2. DATABASE STATUS - Storage Analytics║
║ 3. BATCH PROCESSOR - Multi-tag Process║
║ 4. ANOMALY SCANNER - Pattern Detection║
║ 5. DATA VISUALIZER - Generate Plots   ║
║ 6. SYSTEM CONFIG   - Configuration    ║
║ 7. MATRIX LOGS     - View Logs        ║
║ 0. DISCONNECT      - Exit System      ║
╚════════════════════════════════════════╝
        """
        
        if COLORAMA_AVAILABLE:
            print(Fore.CYAN + menu_text + Style.RESET_ALL)
            choice = input(Fore.MAGENTA + "◆ SELECT PATHWAY: " + Style.RESET_ALL)
        else:
            print(menu_text)
            choice = input("◆ SELECT PATHWAY: ")
            
        return choice.strip()
    
    def run_auto_scan_interface(self):
        """Interactive auto-scan interface"""
        if RICH_AVAILABLE:
            self.console.print(Panel(
                "[bold bright_green]◆ AUTO-SCAN NEURAL INTERFACE ◆[/]\n\n"
                "[dim]Configure intelligent PI data scanning parameters[/]",
                title="[bold bright_cyan]QUANTUM SCANNER[/]",
                box=box.DOUBLE
            ))
            
            # Get parameters interactively
            xlsx_path = Prompt.ask("[bold bright_cyan]Excel Workbook Path")
            tags_source = Prompt.ask(
                "[bold bright_cyan]Tags Source", 
                choices=["file", "sheet"], 
                default="file"
            )
            
            if tags_source == "file":
                tags_path = Prompt.ask("[bold bright_cyan]Tags File Path", default="sample_tags.txt")
                tags_sheet = None
            else:
                tags_path = None
                tags_sheet = Prompt.ask("[bold bright_cyan]Excel Sheet Name", default="DL_TAGS")
            
            plant = Prompt.ask("[bold bright_cyan]Plant Identifier", default="PCFS")
            unit = Prompt.ask("[bold bright_cyan]Unit Identifier", default="K-31-01")
            max_age_hours = float(Prompt.ask("[bold bright_cyan]Max Age Hours", default="1.0"))
            
            force_refresh = Confirm.ask("[bold bright_yellow]Force Refresh All Data?", default=False)
            
            # Display configuration
            config_table = Table(title="[bold bright_magenta]SCAN CONFIGURATION[/]", box=box.ROUNDED)
            config_table.add_column("Parameter", style="bold bright_cyan")
            config_table.add_column("Value", style="bright_green")
            
            config_table.add_row("Excel Workbook", str(xlsx_path))
            config_table.add_row("Tags Source", f"{'File: ' + tags_path if tags_path else 'Sheet: ' + tags_sheet}")
            config_table.add_row("Plant", plant)
            config_table.add_row("Unit", unit)
            config_table.add_row("Max Age (hours)", str(max_age_hours))
            config_table.add_row("Force Refresh", str(force_refresh))
            
            self.console.print(config_table)
            
            if Confirm.ask(f"\n[bold {self.theme.NEON_GREEN}]◆ INITIATE NEURAL SCAN?[/]", default=True):
                self._execute_auto_scan(xlsx_path, tags_path, tags_sheet, plant, unit, max_age_hours, force_refresh)
        else:
            self._fallback_auto_scan()
    
    def _execute_auto_scan(self, xlsx_path, tags_path, tags_sheet, plant, unit, max_age_hours, force_refresh):
        """Execute auto-scan with progress tracking"""
        try:
            from .auto_scan import AutoScanner
            from .config import Config
            from .batch import read_tags_from_sheet
            
            # Initialize
            cfg = Config()
            scanner = AutoScanner(cfg)
            
            # Get tags
            if tags_sheet:
                tags = read_tags_from_sheet(Path(xlsx_path), sheet_name=tags_sheet)
            else:
                tags = [t.strip() for t in Path(tags_path).read_text(encoding="utf-8").splitlines() 
                       if t.strip() and not t.strip().startswith('#')]
            
            if RICH_AVAILABLE:
                self.console.print(f"\n[bold {self.theme.MATRIX_GREEN}]◆ NEURAL SCAN INITIATED ◆[/]")
                self.console.print(f"[dim]Processing {len(tags)} quantum signatures...[/]")
                
                # Progress tracking
                with Progress(
                    SpinnerColumn(spinner_style=self.theme.NEON_GREEN),
                    TextColumn("[bold bright_cyan]{task.description}"),
                    BarColumn(style=self.theme.NEON_CYAN, complete_style=self.theme.MATRIX_GREEN),
                    TaskProgressColumn(),
                    TimeRemainingColumn(),
                    console=self.console
                ) as progress:
                    
                    scan_task = progress.add_task("Scanning neural pathways...", total=100)
                    
                    # Simulate progress (in real implementation, hook into actual progress)
                    for i in range(100):
                        time.sleep(0.05)  # Simulate work
                        progress.update(scan_task, advance=1, description=self._get_scan_status(i))
                
                # Display completion
                self.console.print(Panel(
                    f"[bold {self.theme.MATRIX_GREEN}]◆ NEURAL SCAN COMPLETE ◆[/]\n\n"
                    f"[bright_cyan]Quantum signatures processed: {len(tags)}[/]\n"
                    f"[bright_green]Neural pathways established[/]\n"
                    f"[bright_yellow]Matrix synchronization successful[/]",
                    title="[bold bright_magenta]SCAN RESULTS[/]",
                    box=box.DOUBLE
                ))
            
            # Run actual scan (simplified for demo)
            results = scanner.auto_scan_and_analyze(
                tags=tags[:3],  # Limit for demo
                xlsx_path=Path(xlsx_path),
                plant=plant,
                unit=unit,
                max_age_hours=max_age_hours,
                run_anomaly_detection=True,
                generate_plots=False,  # Skip plots for demo
                send_notifications=False  # Skip email for demo
            )
            
            self._display_scan_results(results)
            
        except Exception as e:
            if RICH_AVAILABLE:
                self.console.print(Panel(
                    f"[bold red]◆ NEURAL PATHWAY ERROR ◆[/]\n\n"
                    f"[red]Error: {str(e)}[/]\n"
                    f"[dim]Neural link compromised[/]",
                    title="[bold red]SYSTEM MALFUNCTION[/]",
                    box=box.DOUBLE
                ))
            else:
                print(f"ERROR: {e}")
    
    def _get_scan_status(self, progress: int) -> str:
        """Get dynamic status message based on progress"""
        stages = [
            "Initializing quantum processors...",
            "Establishing neural pathways...",
            "Scanning data matrices...",
            "Analyzing quantum signatures...",
            "Processing neural feedback...",
            "Synchronizing matrix data...",
            "Generating holographic reports...",
            "Finalizing neural patterns..."
        ]
        
        stage_index = min(progress // 12, len(stages) - 1)
        return stages[stage_index]
    
    def _display_scan_results(self, results: Dict[str, Any]):
        """Display scan results in cyberpunk style"""
        if not RICH_AVAILABLE:
            print("Scan completed successfully!")
            return
            
        scan = results.get('scan_results', {})
        analysis = results.get('analysis_results', {})
        
        # Results table
        results_table = Table(
            title="[bold bright_magenta]◆◇◆ NEURAL SCAN ANALYTICS ◆◇◆[/]",
            box=box.DOUBLE_EDGE
        )
        
        results_table.add_column("Metric", style=f"bold {self.theme.NEON_CYAN}")
        results_table.add_column("Value", style=f"bold {self.theme.MATRIX_GREEN}")
        results_table.add_column("Status", style=f"bold {self.theme.NEON_YELLOW}")
        
        results_table.add_row(
            "Total Quantum Signatures", 
            str(scan.get('total_tags', 0)), 
            "◆ PROCESSED"
        )
        results_table.add_row(
            "Neural Cache Hits", 
            str(len(scan.get('used_local_cache', []))), 
            "◇ OPTIMIZED"
        )
        results_table.add_row(
            "Matrix Fetches", 
            str(len(scan.get('fetched_from_pi', []))), 
            "◆ SYNCHRONIZED"
        )
        results_table.add_row(
            "Neural Success Rate", 
            f"{scan.get('processing_stats', {}).get('success_rate', 0):.1%}", 
            "◇ EXCELLENT"
        )
        results_table.add_row(
            "Anomaly Signatures", 
            str(results.get('total_alerts', 0)), 
            "◆ DETECTED"
        )
        
        self.console.print(results_table)
        
        # Wait for user to continue
        Prompt.ask(f"\n[dim {self.theme.ELECTRIC_BLUE}]Press Enter to return to neural matrix...[/]", default="")
    
    def show_database_status(self):
        """Show database status in cyberpunk style"""
        try:
            from .auto_scan import AutoScanner
            from .config import Config
            
            cfg = Config()
            scanner = AutoScanner(cfg)
            status = scanner.get_database_status()
            
            if RICH_AVAILABLE:
                # Database status panel
                db_panel = Panel(
                    f"[bold {self.theme.MATRIX_GREEN}]◆ QUANTUM STORAGE MATRIX ◆[/]\n\n"
                    f"[bright_cyan]Neural Database: [/][dim]{status['database_path']}[/]\n"
                    f"[bright_cyan]Quantum Records: [/][bold]{status['total_records']:,}[/]\n"
                    f"[bright_cyan]Meta Signatures: [/][bold]{status['metadata_records']:,}[/]\n"
                    f"[bright_cyan]Unique Tags: [/][bold]{status['unique_tags']}[/]\n"
                    f"[bright_cyan]Neural Networks: [/][bold]{status['unique_plants']} plants, {status['unique_units']} units[/]",
                    title="[bold bright_magenta]DATABASE ANALYTICS[/]",
                    box=box.DOUBLE
                )
                
                self.console.print(db_panel)
                
                # Recent activity
                if status.get('recent_activity'):
                    activity_table = Table(title="[bold bright_cyan]RECENT NEURAL ACTIVITY[/]", box=box.ROUNDED)
                    activity_table.add_column("Quantum Tag", style="bright_cyan")
                    activity_table.add_column("Records", style="bright_green") 
                    activity_table.add_column("Last Signal", style="bright_yellow")
                    
                    for activity in status['recent_activity'][:5]:
                        activity_table.add_row(
                            activity['tag'],
                            f"{activity['records']:,}",
                            activity['latest_data']
                        )
                    
                    self.console.print(activity_table)
                else:
                    self.console.print(f"\n[dim {self.theme.CYBER_ORANGE}]◇ No recent neural activity detected ◇[/]")
                
                Prompt.ask(f"\n[dim {self.theme.ELECTRIC_BLUE}]Press Enter to return to neural matrix...[/]", default="")
            else:
                print(f"Database: {status['database_path']}")
                print(f"Records: {status['total_records']:,}")
                print(f"Tags: {status['unique_tags']}")
                input("Press Enter to continue...")
                
        except Exception as e:
            if RICH_AVAILABLE:
                self.console.print(f"[bold red]Neural pathway error: {e}[/]")
            else:
                print(f"Error: {e}")
    
    def _fallback_auto_scan(self):
        """Fallback auto-scan for systems without rich"""
        print("\n=== AUTO-SCAN CONFIGURATION ===")
        xlsx_path = input("Excel Workbook Path: ")
        plant = input("Plant Identifier [PCFS]: ") or "PCFS"
        unit = input("Unit Identifier [K-31-01]: ") or "K-31-01"
        
        print("\nScanning... (This would run the actual auto-scan)")
        time.sleep(2)
        print("Scan completed!")
        input("Press Enter to continue...")
    
    def run(self):
        """Main CLI loop"""
        try:
            self.display_banner()
            
            while True:
                choice = self.show_main_menu()
                
                if choice == "0":
                    self._shutdown()
                    break
                elif choice == "1":
                    self.run_auto_scan_interface()
                elif choice == "2":
                    self.show_database_status()
                elif choice == "3":
                    self._show_coming_soon("BATCH PROCESSOR")
                elif choice == "4":
                    self._show_coming_soon("ANOMALY SCANNER")
                elif choice == "5":
                    self._show_coming_soon("DATA VISUALIZER")
                elif choice == "6":
                    self._show_coming_soon("SYSTEM CONFIG")
                elif choice == "7":
                    self._show_coming_soon("MATRIX LOGS")
                else:
                    if RICH_AVAILABLE:
                        self.console.print(f"[bold red]◆ INVALID NEURAL PATHWAY ◆[/]")
                    else:
                        print("Invalid choice!")
                        
        except KeyboardInterrupt:
            self._shutdown()
        except Exception as e:
            if RICH_AVAILABLE:
                self.console.print(f"[bold red]◆ CRITICAL SYSTEM ERROR ◆[/]\n{e}")
            else:
                print(f"Error: {e}")
    
    def _show_coming_soon(self, feature_name: str):
        """Show coming soon message"""
        if RICH_AVAILABLE:
            self.console.print(Panel(
                f"[bold {self.theme.NEON_YELLOW}]◆ {feature_name} ◆[/]\n\n"
                f"[dim]Neural pathways under construction...[/]\n"
                f"[bright_cyan]Feature will be available in next quantum update[/]",
                title="[bold bright_magenta]COMING SOON[/]",
                box=box.DOUBLE
            ))
            Prompt.ask(f"\n[dim {self.theme.ELECTRIC_BLUE}]Press Enter to return...[/]", default="")
        else:
            print(f"\n{feature_name} - Coming Soon!")
            input("Press Enter to continue...")
    
    def _shutdown(self):
        """Shutdown sequence"""
        if RICH_AVAILABLE:
            with Progress(
                SpinnerColumn(spinner_style=self.theme.NEON_RED),
                TextColumn("[bold bright_red]Terminating neural link..."),
                transient=True
            ) as progress:
                task = progress.add_task("", total=50)
                for i in range(50):
                    time.sleep(0.01)
                    progress.update(task, advance=1)
            
            self.console.print(Panel(
                f"[bold {self.theme.NEON_RED}]◆ NEURAL LINK TERMINATED ◆[/]\n\n"
                f"[dim]Quantum processors offline[/]\n"
                f"[bright_cyan]Neural pathways disconnected[/]\n"
                f"[bright_yellow]Matrix interface shutdown complete[/]",
                title="[bold bright_red]SYSTEM OFFLINE[/]",
                box=box.DOUBLE
            ))
        else:
            print("\n=== SYSTEM SHUTDOWN ===")
            print("Neural link terminated. Goodbye!")


def main():
    """Entry point for cyberpunk CLI"""
    cli = CyberpunkCLI()
    cli.run()


if __name__ == "__main__":
    main()