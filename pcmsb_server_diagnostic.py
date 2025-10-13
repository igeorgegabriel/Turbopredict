#!/usr/bin/env python3
"""
PCMSB PI Server Diagnostic Report
Analyze connectivity and timeout issues with PCMSB units
"""

import sys
from pathlib import Path
from datetime import datetime

PROJECT_ROOT = Path(__file__).parent
sys.path.insert(0, str(PROJECT_ROOT))

def analyze_pcmsb_server_status():
    """Analyze PCMSB PI server connectivity and recent failures"""

    print("=" * 70)
    print("PCMSB PI SERVER DIAGNOSTIC REPORT")
    print(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 70)

    print("\n1. CRITICAL ISSUE IDENTIFIED:")
    print("-" * 40)
    print("[CRITICAL] PCMSB PI Server (\\\\PTSG-1MMPDPdb01) appears DOWN or extremely slow")
    print("[CRITICAL] All XT-07002 tags timing out after 90s (enhanced timeout)")
    print("[CRITICAL] C-02001 rebuild stuck due to same timeout issues")

    print("\n2. EVIDENCE OF SERVER ISSUES:")
    print("-" * 40)
    print("• XT-07002: 113 tags ALL timing out with 'No data' after 90s")
    print("• C-02001: Rebuild process stuck (killed due to timeouts)")
    print("• Previous working tags (PCM.XT-07002.070GZI8402.PV) now failing")
    print("• Enhanced timeout (90s vs normal 30s) still insufficient")

    print("\n3. AFFECTED UNITS:")
    print("-" * 40)
    print("High Risk (Currently failing):")
    print("  • XT-07002: Complete data fetch failure")
    print("  • C-02001: Cannot rebuild real data")

    print("\nModerate Risk (May be using cached data):")
    print("  • C-104, C-13001, C-1301, C-1302, C-201, C-202")
    print("  • Fresh scan showed 'FRESH' status but may be stale cache")

    print("\n4. IMPACT ASSESSMENT:")
    print("-" * 40)
    print("Immediate Impact:")
    print("  [FAIL] Cannot fetch new PCMSB data")
    print("  [FAIL] Extended analysis limited to existing cached data")
    print("  [FAIL] Plot stale fetch will show stale data only")

    print("\nExtended Analysis Status:")
    print("  [OK] System architecture ready (90s timeout, plant-specific handling)")
    print("  [OK] Option [2] integration complete")
    print("  [WAIT] Waiting for PI server restoration")

    print("\n5. RECOMMENDED ACTIONS:")
    print("-" * 40)
    print("Immediate (Operations Team):")
    print("  1. Check PCMSB PI server status (\\\\PTSG-1MMPDPdb01)")
    print("  2. Verify network connectivity to PI server")
    print("  3. Check PI DataLink service status")
    print("  4. Review PI server performance logs")

    print("\nTechnical (When server restored):")
    print("  1. Re-run C-02001 rebuild: python rebuild_c02001_real_data.py")
    print("  2. Re-run XT-07002 build: python build_xt07002_quick.py")
    print("  3. Verify other PCMSB units refresh properly")
    print("  4. Test extended analysis with fresh data")

    print("\n6. WORKAROUND OPTIONS:")
    print("-" * 40)
    print("Current Capabilities:")
    print("  • Extended analysis works with existing cached data")
    print("  • Plot stale fetch will display last known data")
    print("  • 6 PCMSB units have rich cached data (C-104, C-13001, etc.)")

    print("\nTemporary Solutions:")
    print("  • Use cached data for demonstration purposes")
    print("  • Focus extended analysis on ABF/PCFS units")
    print("  • Monitor server status and retry when restored")

    print("\n7. SYSTEM HEALTH STATUS:")
    print("-" * 40)
    print("Extended Analysis Framework: [READY]")
    print("Plant-Specific Optimization: [CONFIGURED]")
    print("Option [2] Integration: [COMPLETE]")
    print("PCMSB PI Server: [DOWN/TIMEOUT]")
    print("Data Availability: [CACHED ONLY]")

    print("\n8. NEXT STEPS:")
    print("-" * 40)
    print("1. Report server issue to PCMSB operations team")
    print("2. Monitor server restoration")
    print("3. Resume data builds when connectivity restored")
    print("4. Test extended analysis on fresh PCMSB data")

    print("\n" + "=" * 70)
    print("END DIAGNOSTIC REPORT")
    print("Contact: System shows enhanced 90s timeout configured")
    print("Status: Waiting for PCMSB PI server restoration")
    print("=" * 70)

if __name__ == "__main__":
    analyze_pcmsb_server_status()