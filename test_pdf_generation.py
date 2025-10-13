#!/usr/bin/env python3
"""
Quick test for PDF generation functionality
Tests the consolidated PDF creation from existing anomaly plots
"""

from pathlib import Path
from pi_monitor.anomaly_triggered_plots import AnomalyTriggeredPlotter
import logging

logging.basicConfig(level=logging.INFO)

def test_pdf_generation():
    """Test PDF generation on most recent anomaly report session"""

    # Find most recent anomaly report session
    reports_dir = Path("reports")

    if not reports_dir.exists():
        print("[ERROR] No reports directory found")
        return

    # Find session directories - look for date-based directories
    session_dirs = []
    for d in reports_dir.iterdir():
        if not d.is_dir():
            continue
        # Check for subdirectories like "12-10-2025/7AM"
        for subdir in d.iterdir():
            if subdir.is_dir():
                session_dirs.append(subdir)

    if not session_dirs:
        # Fallback to top-level directories
        session_dirs = sorted([d for d in reports_dir.iterdir() if d.is_dir()], reverse=True)

    if not session_dirs:
        print("[ERROR] No session directories found")
        return

    # Use most recent session
    session_dirs = sorted(session_dirs, key=lambda x: x.stat().st_mtime, reverse=True)
    latest_session = session_dirs[0]
    print(f"[PDF TEST] Testing PDF generation on session: {latest_session}")

    # Count PNG files
    png_count = 0
    for unit_dir in latest_session.iterdir():
        if unit_dir.is_dir():
            png_count += len(list(unit_dir.glob("ANOMALY_*.png")))

    print(f"[PNG COUNT] Found {png_count} PNG plot files")

    if png_count == 0:
        print("[WARNING] No PNG files to compile into PDF")
        return

    # Create plotter and generate PDF
    plotter = AnomalyTriggeredPlotter()
    pdf_path = plotter._generate_consolidated_pdf(latest_session)

    if pdf_path and pdf_path.exists():
        file_size_mb = pdf_path.stat().st_size / (1024 * 1024)
        print(f"[SUCCESS] PDF generated successfully!")
        print(f"   File: {pdf_path}")
        print(f"   Size: {file_size_mb:.2f} MB")
        print(f"   Contains: {png_count} plots")
    else:
        print("[ERROR] PDF generation failed")

if __name__ == "__main__":
    test_pdf_generation()
