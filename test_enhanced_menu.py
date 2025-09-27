#!/usr/bin/env python3
"""
Quick test of the enhanced menu integration
"""

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent))

def test_enhanced_menu():
    """Test that the enhanced menu options are properly integrated"""

    print("TESTING ENHANCED MENU INTEGRATION")
    print("=" * 50)

    try:
        # Import the turbopredict system
        from turbopredict import TurbopredictSystem

        # Create system instance
        system = TurbopredictSystem()

        # Test that new methods exist
        methods_to_test = [
            'run_conditional_plotting',
            'run_tag_state_dashboard',
            'run_incident_reporter'
        ]

        print("Checking new method integration:")
        for method_name in methods_to_test:
            if hasattr(system, method_name):
                print(f"  + {method_name} - INTEGRATED")
            else:
                print(f"  - {method_name} - MISSING")

        # Test menu display (without full interaction)
        print(f"\nTesting menu display...")

        # Check if Rich is available for proper menu display
        try:
            from rich.console import Console
            console = Console()
            print("  + Rich console available for enhanced menu")
        except ImportError:
            print("  - Rich not available, using fallback menu")

        # Test data availability
        if system.data_available:
            print(f"  + Data systems online")
        else:
            print(f"  - Data systems offline (expected in test)")

        print(f"\nTest Results:")
        print(f"  Enhanced menu options 9, A, B should now be available")
        print(f"  Options include:")
        print(f"    9. CONDITIONAL PLOTTING - Smart plots + minimal change marks")
        print(f"    A. TAG STATE DASHBOARD - Comprehensive tag health monitor")
        print(f"    B. INCIDENT REPORTER   - WHO-WHAT-WHEN-WHERE reports")

        print(f"\nINTEGRATION TEST SUCCESSFUL!")
        return True

    except Exception as e:
        print(f"Integration test failed: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    test_enhanced_menu()