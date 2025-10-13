#!/usr/bin/env python3
"""
Scan for unintegrated modules that could be added to the main system
"""

import os
import sys
from pathlib import Path
import ast
import importlib.util

def scan_unintegrated_modules():
    """Scan for modules that aren't integrated into turbopredict.py"""

    print("SCANNING FOR UNINTEGRATED MODULES")
    print("=" * 60)

    # Read turbopredict.py to see what's already integrated
    turbo_path = Path("turbopredict.py")
    if not turbo_path.exists():
        print("ERROR: turbopredict.py not found")
        return

    with open(turbo_path, 'r', encoding='utf-8') as f:
        turbo_content = f.read()

    # Categories of modules to check
    categories = {
        'Enhanced Plotting': [
            'enhanced_plot_anomalies.py',
            'enhanced_plot_conditional.py',
            'final_anomaly_plots.py',
            'simple_anomaly_plots.py',
            'create_unit_plots.py'
        ],
        'Analysis & Detection': [
            'pi_monitor/tag_state_dashboard.py',
            'pi_monitor/stale_data_detector.py',
            'pi_monitor/hybrid_anomaly_detection.py',
            'pi_monitor/tuned_anomaly_detection.py'
        ],
        'Incident & Monitoring': [
            'scripts/anomaly_incident_reporter.py',
            'scripts/anomaly_validator.py',
            'scripts/freshness_monitor.py',
            'scripts/unit_status_detector.py'
        ],
        'PCMSB Specific': [
            'diagnose_pcmsb_data_source.py',
            'populate_pcmsb_excel_sheets.py',
            'test_pcmsb_multi_unit_fix.py'
        ],
        'Architecture Management': [
            'organize_excel_by_plant.py',
            'check_main_excel_files.py',
            'test_plant_architecture.py'
        ],
        'Optimization & Performance': [
            'scripts/mtd_auto_optimize.py',
            'scripts/baseline_tuning_system.py',
            'pi_monitor/polars_optimizer.py',
            'pi_monitor/ultra_fast_excel.py'
        ],
        'Data Management': [
            'scripts/archive_stray_parquet.py',
            'scripts/merge_into_master.py',
            'scripts/build_duckdb_from_processed.py'
        ]
    }

    print("Checking integration status:")
    print("-" * 60)

    unintegrated = {}

    for category, modules in categories.items():
        print(f"\n{category}:")
        category_unintegrated = []

        for module_path in modules:
            # Check if module exists
            if not Path(module_path).exists():
                print(f"  X {module_path} - FILE NOT FOUND")
                continue

            # Extract module name for checking
            module_name = Path(module_path).stem

            # Check if referenced in turbopredict.py
            if module_name in turbo_content or module_path in turbo_content:
                print(f"  + {module_path} - INTEGRATED")
            else:
                print(f"  - {module_path} - NOT INTEGRATED")
                category_unintegrated.append(module_path)

        if category_unintegrated:
            unintegrated[category] = category_unintegrated

    # Analyze important standalone scripts
    print(f"\n\nIMPORTANT STANDALONE SCRIPTS:")
    print("-" * 60)

    important_scripts = {
        'enhanced_plot_conditional.py': 'Conditional plotting with minimal change detection',
        'pi_monitor/tag_state_dashboard.py': 'Comprehensive tag health monitoring',
        'scripts/anomaly_incident_reporter.py': 'WHO-WHAT-WHEN-WHERE incident reporting',
        'scripts/freshness_monitor.py': 'Real-time data freshness monitoring',
        'diagnose_pcmsb_data_source.py': 'PCMSB-specific diagnostics',
        'populate_pcmsb_excel_sheets.py': 'PCMSB Excel population tool'
    }

    for script, description in important_scripts.items():
        if Path(script).exists():
            if script in turbo_content or Path(script).stem in turbo_content:
                status = "INTEGRATED"
            else:
                status = "PENDING INTEGRATION"
            print(f"  {script}")
            print(f"    Description: {description}")
            print(f"    Status: {status}")
            print()

    # Summary
    print("\nINTEGRATION OPPORTUNITIES:")
    print("=" * 60)

    if unintegrated:
        for category, modules in unintegrated.items():
            if modules:
                print(f"\n{category}:")
                for module in modules:
                    print(f"  • {module}")
    else:
        print("All major modules appear to be integrated!")

    # Recommendations
    print(f"\n\nRECOMMENDATIONS:")
    print("=" * 60)

    recommendations = [
        ("HIGH PRIORITY", [
            "enhanced_plot_conditional.py - Conditional plotting functionality",
            "pi_monitor/tag_state_dashboard.py - Tag health dashboard",
            "scripts/anomaly_incident_reporter.py - Incident reporting system"
        ]),
        ("MEDIUM PRIORITY", [
            "scripts/freshness_monitor.py - Real-time monitoring",
            "pi_monitor/stale_data_detector.py - Enhanced stale detection",
            "scripts/unit_status_detector.py - Unit status monitoring"
        ]),
        ("PLANT-SPECIFIC", [
            "diagnose_pcmsb_data_source.py - PCMSB diagnostics",
            "populate_pcmsb_excel_sheets.py - PCMSB data population"
        ])
    ]

    for priority, items in recommendations:
        print(f"\n{priority}:")
        for item in items:
            print(f"  • {item}")

    return unintegrated

if __name__ == "__main__":
    scan_unintegrated_modules()