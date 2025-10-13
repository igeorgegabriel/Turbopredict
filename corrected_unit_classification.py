#!/usr/bin/env python3
"""
Corrected unit classification logic that handles operational exceptions like XT-07002.
"""

def get_corrected_unit_classification():
    """
    Return corrected unit classifications based on operational reality, not just naming patterns.

    Key correction: XT-07002 is operationally confirmed to be under PCMSB plant,
    even though it doesn't follow the C-xxx naming pattern.
    """

    # Corrected classifications based on operational confirmation
    classifications = {
        'pcfs': ['K-12-01', 'K-16-01', 'K-19-01', 'K-31-01'],
        'pcmsb': ['C-02001', 'C-104', 'C-13001', 'C-1301', 'C-1302', 'C-201', 'C-202', 'XT-07002'],  # XT-07002 added
        'abfsb': ['07-MT01-K001'],  # XT-07002 removed
        'mlng': []  # If any MLNG units exist
    }

    return classifications

def classify_unit_by_name(unit_name):
    """
    Classify a unit by name, handling operational exceptions.

    Args:
        unit_name: Unit identifier (e.g., 'K-31-01', 'XT-07002')

    Returns:
        Plant classification: 'PCFS', 'PCMSB', 'ABFSB', or 'UNKNOWN'
    """

    # Get corrected classifications
    classifications = get_corrected_unit_classification()

    # Check each plant classification
    if unit_name in classifications['pcfs']:
        return 'PCFS'
    elif unit_name in classifications['pcmsb']:
        return 'PCMSB'
    elif unit_name in classifications['abfsb']:
        return 'ABFSB'
    elif unit_name in classifications['mlng']:
        return 'MLNG'

    # Fallback for unknown units - try pattern matching
    if unit_name.startswith('K-'):
        return 'PCFS'
    elif unit_name.startswith('C-'):
        return 'PCMSB'
    else:
        return 'UNKNOWN'

def get_plant_units(plant_name):
    """
    Get all units for a specific plant.

    Args:
        plant_name: 'PCFS', 'PCMSB', 'ABFSB', or 'MLNG'

    Returns:
        List of unit names for that plant
    """
    classifications = get_corrected_unit_classification()
    plant_key = plant_name.lower()

    return classifications.get(plant_key, [])

def validate_classification():
    """Validate the corrected classification and show changes."""

    print("CORRECTED UNIT CLASSIFICATION")
    print("=" * 50)

    classifications = get_corrected_unit_classification()

    for plant, units in classifications.items():
        plant_upper = plant.upper()
        unit_count = len(units)
        print(f"{plant_upper:5} ({unit_count} units): {units}")

        # Highlight XT-07002 correction
        if plant == 'pcmsb' and 'XT-07002' in units:
            print(f"      ✅ XT-07002 CORRECTED: Moved from ABFSB → PCMSB")

    print()
    print("Total units:", sum(len(units) for units in classifications.values()))

    # Test the classification function
    print("\nTesting unit classification:")
    test_units = ['K-31-01', 'C-202', 'XT-07002', '07-MT01-K001']

    for unit in test_units:
        plant = classify_unit_by_name(unit)
        print(f"  {unit:12} → {plant}")

        if unit == 'XT-07002' and plant == 'PCMSB':
            print(f"  {' ':12}   ✅ CORRECTLY classified as PCMSB")

if __name__ == "__main__":
    validate_classification()