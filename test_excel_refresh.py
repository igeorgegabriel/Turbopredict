#!/usr/bin/env python3
"""
Test script for Excel refresh with automated save handling
Tests the dummy file strategy to avoid Excel save prompts
"""

import logging
from pathlib import Path
import sys

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Add current directory to path
sys.path.insert(0, str(Path(__file__).parent))

from pi_monitor.excel_refresh import refresh_excel_safe
from pi_monitor.excel_file_manager import ExcelFileManager


def test_excel_file_manager():
    """Test the ExcelFileManager functionality"""
    print("=== Testing ExcelFileManager ===")
    
    # Create a test path (doesn't need to exist for this test)
    test_path = Path("data/raw/Automation.xlsx")
    
    if not test_path.exists():
        print(f"⚠️  Test file not found: {test_path}")
        print("This test requires an existing Excel file to demonstrate functionality")
        return False
    
    try:
        manager = ExcelFileManager(test_path)
        
        # Test backup creation
        print("Creating backup...")
        backup_path = manager.create_backup()
        print(f"✅ Backup created: {backup_path}")
        
        # Test cleanup
        print("Testing cleanup...")
        cleanup_count = manager.cleanup_temp_files(max_age_hours=0.1)  # Very short age for testing
        print(f"✅ Cleaned up {cleanup_count} temp files")
        
        return True
        
    except Exception as e:
        print(f"❌ ExcelFileManager test failed: {e}")
        return False


def test_excel_refresh():
    """Test the Excel refresh functionality"""
    print("\n=== Testing Excel Refresh ===")
    
    # Look for Excel file
    possible_paths = [
        Path("data/raw/Automation.xlsx"),
        Path("data/Automation.xlsx"),
        Path("Automation.xlsx")
    ]
    
    excel_path = None
    for path in possible_paths:
        if path.exists():
            excel_path = path
            break
    
    if not excel_path:
        print("⚠️  No Excel file found for testing")
        print("Expected paths:")
        for path in possible_paths:
            print(f"   - {path}")
        return False
    
    print(f"Found Excel file: {excel_path}")
    
    try:
        # Test dummy file strategy
        print("\n--- Testing dummy file strategy ---")
        refresh_excel_safe(excel_path, settle_seconds=2, use_working_copy=False)
        print("✅ Dummy file refresh completed")
        
        # Test working copy strategy
        print("\n--- Testing working copy strategy ---")
        refresh_excel_safe(excel_path, settle_seconds=2, use_working_copy=True)
        print("✅ Working copy refresh completed")
        
        return True
        
    except Exception as e:
        print(f"❌ Excel refresh test failed: {e}")
        logger.exception("Excel refresh test error details:")
        return False


def main():
    """Run all tests"""
    print("TURBOPREDICT X PROTEAN - Excel Refresh Test Suite")
    print("=" * 50)
    
    results = []
    
    # Test file manager
    results.append(test_excel_file_manager())
    
    # Test Excel refresh
    results.append(test_excel_refresh())
    
    # Summary
    print("\n" + "=" * 50)
    print("TEST SUMMARY")
    print("=" * 50)
    
    passed = sum(results)
    total = len(results)
    
    print(f"Passed: {passed}/{total}")
    
    if passed == total:
        print("🎉 All tests passed!")
        print("\nThe Excel refresh system is ready to use!")
        print("Key features implemented:")
        print("  ✅ Dummy file strategy to avoid save prompts")
        print("  ✅ Working copy fallback strategy")
        print("  ✅ Automatic file management and cleanup")
        print("  ✅ Robust error handling with fallbacks")
    else:
        print(f"⚠️  {total - passed} tests failed")
        print("Check the error messages above for details")
    
    return passed == total


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)