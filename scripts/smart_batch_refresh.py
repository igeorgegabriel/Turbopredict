"""Smart batch refresh: Uses batch.py to fetch data for each stale unit"""
from pathlib import Path
import sys
sys.path.insert(0, str(Path(__file__).parent.parent))

from datetime import datetime, timedelta
import pandas as pd

# Import from existing modules
from scripts.smart_incremental_refresh import check_if_stale, UNITS
from pi_monitor.batch import build_unit_from_tags

try:
    from colorama import init, Fore, Style
    init(autoreset=True)
    COLORS_AVAILABLE = True
except ImportError:
    COLORS_AVAILABLE = False
    class Fore:
        GREEN = CYAN = YELLOW = RED = MAGENTA = WHITE = ""
    class Style:
        BRIGHT = RESET_ALL = ""

PROJECT_ROOT = Path(__file__).parent.parent

# Map units to their primary tag (you'll need to configure this)
UNIT_TAGS = {
    "PCFS": {
        "K-12-01": ["PCFS.K-12-01.12PI-007.PV"],
        "K-16-01": ["PCFS.K-16-01.16PI-XXX.PV"],  # TODO: Find correct tag
        "K-19-01": ["PCFS.K-19-01.19PI-XXX.PV"],  # TODO: Find correct tag
        "K-31-01": ["PCFS.K-31-01.31PI-XXX.PV"],  # TODO: Find correct tag
    }
}

print(f"{Fore.RED}{Style.BRIGHT}")
print("=" * 80)
print("ERROR: Unit tag mapping incomplete")
print("=" * 80)
print(f"{Style.RESET_ALL}")
print("\nThis approach requires knowing the exact PI tags for each unit.")
print("The Excel file only has configuration for K-12-01.")
print("\nRecommended solutions:")
print("  1. Create separate Excel files for each unit")
print("  2. Use the existing Excel Sheet1 which has all units' data")
print("  3. Manually configure tags in UNIT_TAGS dictionary above")
print("\nFor now, only K-12-01 can be refreshed.")
sys.exit(1)
