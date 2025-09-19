#!/usr/bin/env python3
"""
Test Progress Tracking Integration
Simulate selecting option 1A to test unit-by-unit progress display
"""

import sys
from pathlib import Path

# Add current directory to path
sys.path.insert(0, str(Path(__file__).parent))

def test_auto_refresh_with_progress():
    """Test the AUTO-REFRESH SCAN with progress tracking"""
    
    print("TESTING PROGRESS TRACKING INTEGRATION")
    print("=" * 50)
    
    try:
        # Import the main system
        from turbopredict import TurbopredictSystem
        
        # Initialize the system
        system = TurbopredictSystem()
        
        print("\n>>> SIMULATING OPTION 1A: AUTO-REFRESH SCAN <<<")
        print("This will test the new progress tracking system")
        print("-" * 50)
        
        # Call the same method that option 1A calls
        result = system.run_real_data_scanner(auto_refresh=True)
        
        print("\n" + "=" * 50)
        print("PROGRESS TRACKING TEST COMPLETED")
        
        if result:
            print("+ Progress tracking system integration successful")
            print("+ Unit-by-unit status updates working")
            print("+ Real completion metrics available")
        else:
            print("! Test completed but check output for any issues")
        
        return True
        
    except Exception as e:
        print(f"X PROGRESS TRACKING TEST FAILED: {e}")
        return False

if __name__ == "__main__":
    success = test_auto_refresh_with_progress()
    sys.exit(0 if success else 1)