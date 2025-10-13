#!/usr/bin/env python3
"""
Plot current XT-07002 data - show latest fetch without staleness filtering
"""

import os
import sys
from pathlib import Path
import matplotlib.pyplot as plt
import pandas as pd

# Set timeout
os.environ['PI_FETCH_TIMEOUT'] = '60'

PROJECT_ROOT = Path(__file__).parent
sys.path.insert(0, str(PROJECT_ROOT))

from pi_monitor.batch import _fetch_single
import xlwings as xw

def plot_xt07002_current():
    """Fetch and plot current XT-07002 data - no staleness filtering"""

    print("=== Plotting XT-07002 Current Data ===")

    # The tag you showed with data
    tag = 'PCM.XT-07002.070GZI8402.PV'
    xlsx = PROJECT_ROOT / "excel" / "PCMSB" / "PCMSB_Automation.xlsx"

    print(f"Fetching latest data for: {tag}")

    with xw.App(visible=False) as app:
        wb = app.books.open(xlsx)

        try:
            # Fetch last 7 days of data
            df = _fetch_single(
                wb, 'DL_WORK', tag, r"\\PTSG-1MMPDPdb01",
                '-7d', '*', '-0.1h', settle_seconds=2.0
            )

            print(f"Retrieved {len(df)} data points")

            if len(df) > 0:
                print(f"Data range: {df['time'].min()} to {df['time'].max()}")
                print(f"Value range: {df['value'].min():.3f} to {df['value'].max():.3f}")

                # Create plot
                plt.figure(figsize=(12, 6))
                plt.plot(df['time'], df['value'], 'b-', linewidth=1, alpha=0.7)
                plt.scatter(df['time'].iloc[-1:], df['value'].iloc[-1:],
                          color='red', s=50, zorder=5, label=f'Latest: {df["value"].iloc[-1]:.3f}')

                plt.title(f'XT-07002 Current Data - {tag}\nLatest Fetch - All Available Data')
                plt.xlabel('Time')
                plt.ylabel('Value')
                plt.grid(True, alpha=0.3)
                plt.legend()
                plt.xticks(rotation=45)
                plt.tight_layout()

                # Save plot
                plot_file = PROJECT_ROOT / "reports" / "xt07002_current_data.png"
                plot_file.parent.mkdir(exist_ok=True)
                plt.savefig(plot_file, dpi=150, bbox_inches='tight')
                print(f"Plot saved: {plot_file}")

                plt.show()

                # Show latest 10 points
                print(f"\nLatest 10 data points:")
                print(df.tail(10).to_string(index=False))

            else:
                print("No data retrieved")

        except Exception as e:
            print(f"Error: {e}")

        wb.close()

if __name__ == "__main__":
    plot_xt07002_current()