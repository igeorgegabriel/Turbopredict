#!/usr/bin/env python3
"""
Scheduling Optimization Analysis for TURBOPREDICT X PROTEAN
Provides recommendations for different fetch/analysis patterns
"""

import sys
from pathlib import Path

# Add current directory to path
sys.path.insert(0, str(Path(__file__).parent))

def analyze_scheduling_patterns():
    """Analyze different scheduling patterns and their performance impact"""
    
    print("TURBOPREDICT X PROTEAN - SCHEDULING OPTIMIZATION ANALYSIS")
    print("=" * 70)
    
    print("\n1. CURRENT SITUATION:")
    print("-" * 30)
    print("• Data staleness threshold: 1.0 hours")
    print("• Typical fetch time: 5-10 minutes")
    print("• Analysis time: < 5 seconds")
    print("• Total records: 42+ million")
    
    print("\n2. SCHEDULING PATTERN ANALYSIS:")
    print("-" * 40)
    
    patterns = [
        {
            "name": "CURRENT (1-hour threshold)",
            "stale_threshold": 1.0,
            "schedule_interval": 1.0,
            "fetch_frequency": "Every hour",
            "performance": "Poor - Always fetching"
        },
        {
            "name": "BALANCED (4-hour threshold)",
            "stale_threshold": 4.0,
            "schedule_interval": 1.0,
            "fetch_frequency": "Every 4th hour",
            "performance": "Good - 75% fast, 25% slow"
        },
        {
            "name": "PERFORMANCE (8-hour threshold)",
            "stale_threshold": 8.0,
            "schedule_interval": 1.0,
            "fetch_frequency": "Every 8th hour",
            "performance": "Excellent - 87.5% fast, 12.5% slow"
        },
        {
            "name": "DAILY (24-hour threshold)",
            "stale_threshold": 24.0,
            "schedule_interval": 1.0,
            "fetch_frequency": "Once daily",
            "performance": "Maximum - 95.8% fast, 4.2% slow"
        }
    ]
    
    for pattern in patterns:
        print(f"\n• {pattern['name']}:")
        print(f"  Staleness: {pattern['stale_threshold']} hours")
        print(f"  Fetch frequency: {pattern['fetch_frequency']}")
        print(f"  Performance: {pattern['performance']}")
        
        # Calculate hourly performance
        hours_per_day = 24
        fetch_hours = hours_per_day / pattern['stale_threshold'] if pattern['stale_threshold'] <= 24 else 1
        fast_hours = hours_per_day - fetch_hours
        fast_percentage = (fast_hours / hours_per_day) * 100
        
        print(f"  Fast diagnostics: {fast_percentage:.1f}% of the time")
        print(f"  Slow fetches: {hours_per_day - fast_hours:.1f} times per day")
    
    print("\n3. RECOMMENDED OPTIMIZATION STRATEGIES:")
    print("-" * 50)
    
    strategies = [
        {
            "strategy": "HYBRID SCHEDULING",
            "description": "Fast diagnostics + Scheduled background fetching",
            "implementation": [
                "• Set staleness threshold to 4-8 hours",
                "• Run diagnostics anytime (will use cached data)",
                "• Schedule background fetch during off-hours",
                "• Best of both worlds: Fast + Fresh"
            ]
        },
        {
            "strategy": "DEMAND-BASED FETCHING",
            "description": "Only fetch when absolutely needed",
            "implementation": [
                "• Use current data for routine diagnostics",
                "• Fetch fresh data only for critical analysis",
                "• Manual refresh option available",
                "• Maximize performance for regular use"
            ]
        },
        {
            "strategy": "INCREMENTAL UPDATES",
            "description": "Fetch only new data since last update",
            "implementation": [
                "• Modify PI DataLink to fetch incrementally",
                "• Append new records to existing Parquet files", 
                "• Reduce fetch time significantly",
                "• Requires development work"
            ]
        }
    ]
    
    for strategy in strategies:
        print(f"\n• {strategy['strategy']}:")
        print(f"  {strategy['description']}")
        for item in strategy['implementation']:
            print(f"  {item}")
    
    print("\n4. IMMEDIATE CONFIGURATION OPTIONS:")
    print("-" * 45)
    
    print("\nEnvironment Variable Method:")
    print("• Set MAX_AGE_HOURS=4.0  # 4-hour threshold")
    print("• Set MAX_AGE_HOURS=8.0  # 8-hour threshold")
    print("• Set MAX_AGE_HOURS=24.0 # Daily refresh")
    
    print("\nCommand Line Method:")
    print("• python -m pi_monitor.cli auto-scan --max-age-hours 4")
    print("• python turbopredict.py  # Uses environment setting")
    
    print("\nScheduling Examples:")
    print("• Diagnostics: Run anytime - always fast")
    print("• Background fetch: Schedule during low usage")
    print("• Critical analysis: Force refresh if needed")
    
    print("\n5. PERFORMANCE PREDICTION:")
    print("-" * 35)
    
    scenarios = [
        ("Current (1h)", 1.0, 10.0, 0.083),
        ("Balanced (4h)", 4.0, 10.0, 0.083), 
        ("Performance (8h)", 8.0, 10.0, 0.083),
        ("Daily (24h)", 24.0, 10.0, 0.083)
    ]
    
    print(f"{'Scenario':<15} {'Avg Time':<10} {'Fast %':<8} {'Fetch/Day':<10}")
    print("-" * 45)
    
    for name, threshold, fetch_time, analysis_time in scenarios:
        fetches_per_day = 24 / threshold if threshold <= 24 else 1
        fast_percentage = ((24 - fetches_per_day) / 24) * 100
        avg_time = (fetches_per_day * fetch_time + (24 - fetches_per_day) * analysis_time) / 24
        
        print(f"{name:<15} {avg_time:<10.1f}min {fast_percentage:<8.1f}% {fetches_per_day:<10.1f}")
    
    print("\n" + "=" * 70)
    print("RECOMMENDATION: Use 4-8 hour threshold for optimal balance")
    print("• 75-87% of diagnostics will be instant")
    print("• Fresh data every few hours")
    print("• Predictable performance pattern")

def create_optimized_config():
    """Create an optimized configuration file"""
    config_content = """# TURBOPREDICT X PROTEAN - OPTIMIZED CONFIGURATION
# Performance-optimized settings for scheduled operations

# Data freshness threshold (hours)
# 1.0 = Always fetch (slow but fresh)
# 4.0 = Balanced performance (recommended)
# 8.0 = High performance (good for routine diagnostics)
# 24.0 = Daily refresh (maximum performance)
MAX_AGE_HOURS=4.0

# Excel file path
XLSX_PATH=data/raw/Automation.xlsx

# Plant and unit defaults
PLANT=PCFS
UNIT=K-31-01

# Email notifications (optional)
EMAIL_SENDER=turbopredict@yourcompany.com
EMAIL_RECIPIENTS=operations@yourcompany.com,analytics@yourcompany.com

# SMTP settings (optional)
SMTP_HOST=smtp.yourcompany.com
SMTP_PORT=587
SMTP_USE_TLS=true
"""
    
    config_path = Path(".env.optimized")
    with open(config_path, 'w') as f:
        f.write(config_content)
    
    print(f"\n✅ Created optimized configuration: {config_path}")
    print("To use: Copy to .env or set environment variables")

if __name__ == "__main__":
    analyze_scheduling_patterns()
    create_optimized_config()