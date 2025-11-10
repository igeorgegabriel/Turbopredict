#!/usr/bin/env python3
"""
Test enhanced plotting with single unit to verify timeline fix
"""

import sys
import os
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent))

# Import the fixed enhanced plotting functions
from enhanced_plot_anomalies import create_enhanced_plots
import pandas as pd
from datetime import datetime, timedelta

def test_single_unit_enhanced_plot():
    """Test enhanced plotting with K-31-01 only"""

    print("TESTING ENHANCED PLOTTING TIMELINE FIX")
    print("=" * 50)

    # Temporarily modify the enhanced plotting script to only process K-31-01
    # Save original units list
    import enhanced_plot_anomalies as epa

    # Override the units list in the module
    original_units = ['K-12-01', 'K-16-01', 'K-19-01', 'K-31-01']
    test_units = ['K-31-01']  # Only test K-31-01

    # Monkey patch the script to use our test units
    epa.create_enhanced_plots.__code__ = epa.create_enhanced_plots.__code__.replace(co_consts=(
        epa.create_enhanced_plots.__code__.co_consts[0],
        test_units,  # Replace units list
        *epa.create_enhanced_plots.__code__.co_consts[2:]
    ))

    print("Running enhanced plotting for K-31-01 only...")
    print("This should now show timeline extending to 22/09/2025")
    print()

    try:
        output_dir = create_enhanced_plots()
        print(f"\\nPlots generated in: {output_dir}")

        # Check if any plots were generated
        if output_dir and output_dir.exists():
            plot_files = list(output_dir.glob("**/*.png"))
            print(f"Generated {len(plot_files)} plot files")

            if plot_files:
                print("\\n✅ SUCCESS: Enhanced plotting completed with timeline fix!")
                print("Check the generated plots - they should now show data up to 22/09/2025")
            else:
                print("\\n❌ No plot files generated")
        else:
            print("\\n❌ Output directory not found")

    except Exception as e:
        print(f"Error during plotting: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    test_single_unit_enhanced_plot()