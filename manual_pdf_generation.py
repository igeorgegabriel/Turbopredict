#!/usr/bin/env python3
"""
Manually generate PDF for the 12PM session to see any errors
"""

from pathlib import Path
from pi_monitor.anomaly_triggered_plots import AnomalyTriggeredPlotter

session_dir = Path("reports/12-10-2025/12PM")

print(f"Generating PDF for session: {session_dir}")
print(f"Session exists: {session_dir.exists()}")

if session_dir.exists():
    plotter = AnomalyTriggeredPlotter()
    try:
        pdf_path = plotter._generate_consolidated_pdf(session_dir)
        if pdf_path:
            print(f"\n[SUCCESS] PDF generated: {pdf_path}")
            print(f"File size: {pdf_path.stat().st_size / (1024*1024):.2f} MB")
        else:
            print("\n[ERROR] PDF generation returned None")
    except Exception as e:
        print(f"\n[ERROR] PDF generation failed:")
        print(f"  Error: {e}")
        import traceback
        traceback.print_exc()
else:
    print("[ERROR] Session directory does not exist")
