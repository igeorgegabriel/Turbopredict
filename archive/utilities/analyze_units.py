#!/usr/bin/env python3
"""
Analyze all units and classify them by plant (PCFS, PCMSB, ABFSB)
"""

from pi_monitor.parquet_database import ParquetDatabase
from pathlib import Path

def analyze_units():
    db = ParquetDatabase(Path('data'))
    units = db.get_all_units()

    print('Unit-Plant Analysis:')
    print('=' * 60)

    pcfs_units = []
    pcmsb_units = []
    abf_units = []
    other_units = []

    for unit in sorted(units):
        try:
            df = db.get_unit_data(unit)
            if not df.empty and 'tag' in df.columns:
                sample_tags = df['tag'].head(3).tolist()

                print(f'{unit:12} | Sample: {sample_tags}')

                # Classify by unit naming patterns
                if unit.startswith('K-'):
                    pcfs_units.append(unit)
                    plant = 'PCFS'
                elif unit.startswith('C-'):
                    pcmsb_units.append(unit)
                    plant = 'PCMSB'
                elif unit in ['07-MT01-K001', 'XT-07002']:
                    abf_units.append(unit)
                    plant = 'ABFSB'
                else:
                    other_units.append(unit)
                    plant = 'UNKNOWN'

                print(f'{"":12} | Plant: {plant}')
                print()

        except Exception as e:
            print(f'{unit:12} | Error: {e}')
            other_units.append(unit)

    print('=' * 60)
    print('PLANT CLASSIFICATION RESULTS:')
    print('=' * 60)

    print(f'\nPCFS Units ({len(pcfs_units)}):')
    for unit in pcfs_units:
        print(f'  - {unit}')

    print(f'\nPCMSB Units ({len(pcmsb_units)}):')
    for unit in pcmsb_units:
        print(f'  - {unit}')

    print(f'\nABFSB Units ({len(abf_units)}):')
    for unit in abf_units:
        print(f'  - {unit}')

    print(f'\nOther/Unknown ({len(other_units)}):')
    for unit in other_units:
        print(f'  - {unit}')

    return {
        'pcfs': pcfs_units,
        'pcmsb': pcmsb_units,
        'abfsb': abf_units,
        'other': other_units
    }

if __name__ == "__main__":
    analyze_units()