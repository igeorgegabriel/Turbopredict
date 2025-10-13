"""Smart incremental refresh: Only refresh units that are actually stale"""
from pathlib import Path
import sys
sys.path.insert(0, str(Path(__file__).parent))

from simple_incremental_refresh import simple_refresh_unit, get_latest_timestamp, count_tags_in_parquet, check_tag_freshness, PROJECT_ROOT
from datetime import datetime, timedelta

# Try to use colorama for colors, fallback to plain text
try:
    from colorama import init, Fore, Style, Back
    init(autoreset=True)
    COLORS_AVAILABLE = True
except ImportError:
    COLORS_AVAILABLE = False
    class Fore:
        GREEN = CYAN = YELLOW = RED = MAGENTA = WHITE = ""
    class Style:
        BRIGHT = RESET_ALL = ""
    class Back:
        BLACK = ""

def check_if_stale(unit: str, plant: str, max_age_hours: float = 1.0) -> tuple[bool, datetime | None, timedelta | None, int, int]:
    """Check if a unit's data is stale using PER-TAG freshness validation.

    NEW: Requires at least 50% of tags to have fresh data (< max_age_hours).
    This prevents false positives where only 1 tag is fresh but others are stale.

    Returns:
        (is_stale, latest_time, age, total_tags, active_tags)
    """
    parquet_file = PROJECT_ROOT / "data" / "processed" / f"{unit}_1y_0p1h.dedup.parquet"

    if not parquet_file.exists():
        return (False, None, None, 0, 0)  # No file = skip

    latest_time = get_latest_timestamp(parquet_file)
    if not latest_time:
        return (False, None, None, 0, 0)

    age = datetime.now() - latest_time.replace(tzinfo=None)

    # NEW: Use per-tag freshness check instead of just overall latest timestamp
    is_fresh, fresh_tag_count, total_tag_count = check_tag_freshness(parquet_file, max_age_hours=max_age_hours)
    is_stale = not is_fresh  # Stale if NOT fresh (i.e., < 50% tags are fresh)

    # Count tags (check partitioned dataset first, then flat file)
    total_tags, active_tags = count_tags_in_parquet(parquet_file, unit=unit, plant=plant)

    return (is_stale, latest_time, age, total_tags, active_tags)

DEFAULT_UNITS: dict[str, list[str]] = {
    "PCFS": ["K-12-01", "K-16-01", "K-19-01", "K-31-01"],
    "ABFSB": ["07-MT01-K001"],
    "PCMSB": ["C-02001", "C-104", "C-13001", "C-1301", "C-1302", "C-201", "C-202", "XT-07002"],
}


def run_smart_incremental_refresh(
    max_age_hours: float = 1.0,
    units: dict[str, list[str]] | None = None,
) -> dict:
    """Execute the smart incremental refresh workflow and return detailed results."""

    units_map = units or DEFAULT_UNITS

    print(Fore.CYAN + "=" * 80)
    print(Fore.CYAN + Style.BRIGHT + "SMART INCREMENTAL REFRESH - STALE UNITS ONLY")
    print(Fore.CYAN + "=" * 80 + Style.RESET_ALL)

    # First, check which units are stale
    stale_units = []
    fresh_units = []

    for plant, plant_units in units_map.items():
        for unit in plant_units:
            is_stale, latest_time, age, total_tags, active_tags = check_if_stale(
                unit, plant, max_age_hours=max_age_hours
            )

            if is_stale:
                stale_units.append((plant, unit, age, latest_time, total_tags, active_tags))
            elif latest_time:
                fresh_units.append((plant, unit, age, latest_time, total_tags, active_tags))

    def print_status_table(title, units_list, status_color, status_text, show_rows_added=False):
        if not units_list:
            return

        if show_rows_added:
            # Include ROWS ADDED column
            print(f"\n{Fore.WHITE}{Style.BRIGHT}+{'-' * 114}+")
            print(f"| {title.center(112)} |")
            print(f"+{'-' * 14}+{'-' * 18}+{'-' * 15}+{'-' * 28}+{'-' * 20}+{'-' * 16}+")
            print(f"| {'STATUS'.center(12)} | {'PLANT'.center(16)} | {'UNIT'.center(13)} | {'DATA AGE'.center(26)} | {'TAGS (Active/Total)'.center(18)} | {'ROWS ADDED'.center(14)} |")
            print(f"+{'-' * 14}+{'-' * 18}+{'-' * 15}+{'-' * 28}+{'-' * 20}+{'-' * 16}+")
        else:
            print(f"\n{Fore.WHITE}{Style.BRIGHT}+{'-' * 98}+")
            print(f"| {title.center(96)} |")
            print(f"+{'-' * 14}+{'-' * 18}+{'-' * 15}+{'-' * 28}+{'-' * 20}+")
            print(f"| {'STATUS'.center(12)} | {'PLANT'.center(16)} | {'UNIT'.center(13)} | {'DATA AGE'.center(26)} | {'TAGS (Active/Total)'.center(18)} |")
            print(f"+{'-' * 14}+{'-' * 18}+{'-' * 15}+{'-' * 28}+{'-' * 20}+")

        for plant, unit, age, latest_time, total_tags, active_tags in units_list:
            hours = age.total_seconds() / 3600
            days = int(hours // 24)
            remaining_hours = hours % 24

            if days > 0:
                age_str = f"{days}d {remaining_hours:.1f}h"
            else:
                age_str = f"{hours:.1f}h"

            # Color codes for display
            if status_text == "OK FRESH":
                status_colored = f"{Fore.GREEN}{status_text}{Style.RESET_ALL}"
            else:
                status_colored = f"{Fore.YELLOW}{status_text}{Style.RESET_ALL}"

            plant_colored = f"{Fore.CYAN}{plant}{Style.RESET_ALL}"
            unit_colored = f"{Fore.YELLOW}{unit}{Style.RESET_ALL}"

            if hours < 1:
                age_colored = f"{Fore.GREEN}{age_str}{Style.RESET_ALL}"
            elif hours < 24:
                age_colored = f"{Fore.YELLOW}{age_str}{Style.RESET_ALL}"
            else:
                age_colored = f"{Fore.RED}{age_str}{Style.RESET_ALL}"

            # Format tags column
            tag_str = f"{active_tags}/{total_tags}"
            if total_tags == 0:
                tag_colored = f"{Fore.RED}{tag_str}{Style.RESET_ALL}"
            elif active_tags == total_tags:
                tag_colored = f"{Fore.GREEN}{tag_str}{Style.RESET_ALL}"
            elif active_tags / total_tags >= 0.9:
                tag_colored = f"{Fore.GREEN}{tag_str}{Style.RESET_ALL}"
            elif active_tags / total_tags >= 0.7:
                tag_colored = f"{Fore.YELLOW}{tag_str}{Style.RESET_ALL}"
            else:
                tag_colored = f"{Fore.RED}{tag_str}{Style.RESET_ALL}"

            if show_rows_added:
                rows_str = "0"  # Fresh units have 0 rows added
                rows_colored = f"{Fore.YELLOW}{rows_str}{Style.RESET_ALL}"
                print(f"| {status_colored.ljust(12 + len(status_colored) - len(status_text))} | {plant_colored.ljust(16 + len(plant_colored) - len(plant))} | {unit_colored.ljust(13 + len(unit_colored) - len(unit))} | {age_colored.ljust(26 + len(age_colored) - len(age_str))} | {tag_colored.ljust(18 + len(tag_colored) - len(tag_str))} | {rows_colored.ljust(14 + len(rows_colored) - len(rows_str))} |")
            else:
                print(f"| {status_colored.ljust(12 + len(status_colored) - len(status_text))} | {plant_colored.ljust(16 + len(plant_colored) - len(plant))} | {unit_colored.ljust(13 + len(unit_colored) - len(unit))} | {age_colored.ljust(26 + len(age_colored) - len(age_str))} | {tag_colored.ljust(18 + len(tag_colored) - len(tag_str))} |")

        if show_rows_added:
            print(f"+{'-' * 14}+{'-' * 18}+{'-' * 15}+{'-' * 28}+{'-' * 20}+{'-' * 16}+{Style.RESET_ALL}")
        else:
            print(f"+{'-' * 14}+{'-' * 18}+{'-' * 15}+{'-' * 28}+{'-' * 20}+{Style.RESET_ALL}")

    # Show fresh units
    if fresh_units:
        print_status_table(
            f"FRESH UNITS ({len(fresh_units)}) - Data < 1 hour old",
            fresh_units,
            Fore.GREEN,
            "OK FRESH",
            show_rows_added=True,  # Show rows added column (will be 0 for fresh units)
        )

    # Show stale units
    if stale_units:
        print_status_table(
            f"STALE UNITS ({len(stale_units)}) - Data > 1 hour old",
            stale_units,
            Fore.YELLOW,
            "!! STALE",
        )

    if not stale_units:
        print(f"\n{Fore.GREEN}{Style.BRIGHT}+{'-' * 98}+")
        print(f"| {'[OK] ALL UNITS FRESH - NO REFRESH NEEDED!'.center(96)} |")
        print(f"+{'-' * 98}+{Style.RESET_ALL}")
        return {
            "fresh_units": fresh_units,
            "stale_units": stale_units,
            "results": {},
            "refreshed_units": [],
            "failed_units": [],
            "success": True,
        }

    # Refresh only stale units
    print(f"\n{Fore.MAGENTA}{'='*80}")
    print(f"{Style.BRIGHT}>>> REFRESHING {len(stale_units)} STALE UNITS <<<{Style.RESET_ALL}")
    print(f"{Fore.MAGENTA}{'='*80}{Style.RESET_ALL}")

    results: dict[str, dict] = {}
    for plant, unit, age, latest_time, total_tags, active_tags in stale_units:
        print()
        result = simple_refresh_unit(unit, plant)
        results[f"{plant}/{unit}"] = result

    # Summary table
    print(f"\n{Fore.CYAN}{'=' * 80}")
    print(f"{Style.BRIGHT}REFRESH SUMMARY{Style.RESET_ALL}")
    print(f"{Fore.CYAN}{'=' * 80}{Style.RESET_ALL}")

    print(f"\n{Fore.WHITE}{Style.BRIGHT}+{'-' * 98}+")
    print(f"| {'REFRESH RESULTS'.center(96)} |")
    print(f"+{'-' * 14}+{'-' * 40}+{'-' * 20}+{'-' * 21}+")
    print(f"| {'STATUS'.center(12)} | {'UNIT'.center(38)} | {'TAGS (Active/Total)'.center(18)} | {'ROWS ADDED'.center(19)} |")
    print(f"+{'-' * 14}+{'-' * 40}+{'-' * 20}+{'-' * 21}+")

    for key, result in results.items():
        if result["success"]:
            status_text = "[OK]"
            status_colored = f"{Fore.GREEN}{status_text}{Style.RESET_ALL}"
        else:
            status_text = "[FAIL]"
            status_colored = f"{Fore.RED}{status_text}{Style.RESET_ALL}"

        tag_str = f"{result['active_tags']}/{result['total_tags']}"
        rows_str = f"{result['rows_added']:,}" if result['rows_added'] > 0 else "0"

        unit_colored = f"{Fore.CYAN}{key}{Style.RESET_ALL}"
        tag_colored = f"{Fore.GREEN}{tag_str}{Style.RESET_ALL}" if result['active_tags'] > 0 else f"{Fore.RED}{tag_str}{Style.RESET_ALL}"
        rows_colored = f"{Fore.GREEN}{rows_str}{Style.RESET_ALL}" if result['rows_added'] > 0 else f"{Fore.YELLOW}{rows_str}{Style.RESET_ALL}"

        print(f"| {status_colored.ljust(12 + len(status_colored) - len(status_text))} | {unit_colored.ljust(38 + len(unit_colored) - len(key))} | {tag_colored.ljust(18 + len(tag_colored) - len(tag_str))} | {rows_colored.ljust(19 + len(rows_colored) - len(rows_str))} |")

    print(f"+{'-' * 14}+{'-' * 40}+{'-' * 20}+{'-' * 21}+{Style.RESET_ALL}")

    total = len(results)
    success_keys = [key for key, value in results.items() if value.get("success")]
    failed_keys = [key for key in results.keys() if key not in success_keys]

    if len(success_keys) == total:
        summary_color = Fore.GREEN
        summary_icon = "[OK]"
    elif len(success_keys) > 0:
        summary_color = Fore.YELLOW
        summary_icon = "[WARN]"
    else:
        summary_color = Fore.RED
        summary_icon = "[FAIL]"

    print(
        f"\n{summary_color}{Style.BRIGHT}{summary_icon} Refreshed: "
        f"{len(success_keys)}/{total} units successfully{Style.RESET_ALL}"
    )

    return {
        "fresh_units": fresh_units,
        "stale_units": stale_units,
        "results": results,
        "refreshed_units": success_keys,
        "failed_units": failed_keys,
        "success": len(success_keys) == total and total > 0,
    }


if __name__ == "__main__":
    outcome = run_smart_incremental_refresh()
    sys.exit(0 if outcome.get("success", False) else 1)
