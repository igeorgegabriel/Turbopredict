#!/usr/bin/env python3
"""
Fix vacuum pressure engineering limits in baseline configuration
"""

import json
from pathlib import Path

def fix_vacuum_pressure_limits():
    """Fix engineering limits for vacuum pressure tags"""
    
    config_file = "baseline_config_K-31-01.json"
    
    if not Path(config_file).exists():
        print(f"ERROR: {config_file} not found")
        return
        
    # Load current configuration
    with open(config_file, 'r') as f:
        config = json.load(f)
        
    print("FIXING VACUUM PRESSURE ENGINEERING LIMITS")
    print("=" * 50)
    
    # Vacuum pressure tags (condenser/exhaust steam)
    vacuum_tags = [
        'PCFS_K-31-01_31PIA308A_PV',
        'PCFS_K-31-01_31PIA308B_PV', 
        'PCFS_K-31-01_31PIA308C_PV'
    ]
    
    fixes_made = 0
    
    for tag in vacuum_tags:
        if tag in config['tag_configurations']:
            current_config = config['tag_configurations'][tag]
            current_lower = current_config['thresholds']['lower_limit']
            current_upper = current_config['thresholds']['upper_limit']
            
            print(f"\nTag: {tag}")
            print(f"  Current limits: {current_lower:.3f} to {current_upper:.3f}")
            
            # Fix for vacuum system (typical condenser vacuum range)
            # Normal operating range: -1.0 to -0.5 bar
            # Allow some margin: -1.2 to +0.1 bar
            new_lower = -1.2  # Allow deeper vacuum
            new_upper = 0.1   # Allow slight positive pressure during startup
            
            # Update the configuration
            config['tag_configurations'][tag]['thresholds']['lower_limit'] = new_lower
            config['tag_configurations'][tag]['thresholds']['upper_limit'] = new_upper
            
            # Add tag type specification for vacuum
            config['tag_configurations'][tag]['tag_type'] = 'VACUUM_PRESSURE'
            config['tag_configurations'][tag]['process_type'] = 'CONDENSER_VACUUM'
            
            print(f"  Updated limits: {new_lower:.3f} to {new_upper:.3f}")
            print(f"  Tag type: VACUUM_PRESSURE")
            
            fixes_made += 1
        else:
            print(f"\nWARNING: {tag} not found in configuration")
    
    if fixes_made > 0:
        # Save updated configuration
        backup_file = f"{config_file}.backup"
        with open(backup_file, 'w') as f:
            json.dump(config, f, indent=2)
        print(f"\nBackup saved to: {backup_file}")
        
        with open(config_file, 'w') as f:
            json.dump(config, f, indent=2)
        print(f"Updated configuration saved to: {config_file}")
        
        print(f"\nSUCCESS: Fixed {fixes_made} vacuum pressure tags")
        print("\nVacuum pressure tags will now correctly recognize:")
        print("- Negative pressure as NORMAL operating condition")
        print("- Range: -1.2 to +0.1 bar (vacuum to slight positive)")
        print("- No more false positive anomalies for condenser vacuum")
        
    else:
        print("\nNo vacuum tags found to fix")

if __name__ == "__main__":
    fix_vacuum_pressure_limits()