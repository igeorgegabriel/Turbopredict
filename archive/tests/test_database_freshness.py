#!/usr/bin/env python3
"""
Test Database Freshness Recognition
Check if the database now recognizes refreshed files as fresh
"""

import sys
from pathlib import Path

# Add current directory to path
sys.path.insert(0, str(Path(__file__).parent))

def test_database_freshness():
    """Test if database recognizes fresh data"""
    
    print("TESTING DATABASE FRESHNESS RECOGNITION")
    print("=" * 50)
    
    try:
        from pi_monitor.parquet_database import ParquetDatabase
        
        # Initialize database
        db = ParquetDatabase()
        print("+ Database initialized")
        
        # Test units
        units = ["K-12-01", "K-16-01", "K-19-01", "K-31-01"]
        
        for unit in units:
            print(f"\nUnit: {unit}")
            print("-" * 20)
            
            # Get unit data to see which file is being used
            df = db.get_unit_data(unit)
            print(f"   Records loaded: {len(df)}")
            
            # Get freshness info
            info = db.get_data_freshness_info(unit)
            print(f"   Data age: {info['data_age_hours']:.1f} hours")
            print(f"   Is stale: {info['is_stale']}")
            print(f"   Latest timestamp: {info['latest_timestamp']}")
        
        return True
        
    except Exception as e:
        print(f"TEST FAILED: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    test_database_freshness()