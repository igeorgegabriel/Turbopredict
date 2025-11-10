#!/usr/bin/env python3
"""
Quick script to check if data is stable vs anomalous
"""

from pi_monitor.parquet_database import ParquetDatabase
import pandas as pd
import numpy as np

def check_data_stability():
    print('CHECKING DATA STABILITY VS ANOMALY DETECTION')
    print('=' * 60)
    
    db = ParquetDatabase()
    df = db.get_unit_data('K-31-01')
    print(f'Total records: {len(df):,}')
    
    # Check the reported high-anomaly tag
    problem_tag = 'PCFS_K-31-01_31TIA-321A_PV'
    tag_df = df[df['tag'] == problem_tag].copy()
    
    if tag_df.empty:
        print(f'No data for {problem_tag}')
        return
        
    print(f'\nAnalyzing: {problem_tag}')
    print(f'Records: {len(tag_df):,}')
    
    values = tag_df['value'].dropna()
    
    # Basic stats
    mean_val = values.mean()
    std_val = values.std()
    min_val = values.min()
    max_val = values.max()
    
    print(f'\nSTATISTICS:')
    print(f'Mean: {mean_val:.3f}')
    print(f'Std Dev: {std_val:.3f}')
    print(f'Min: {min_val:.3f}')
    print(f'Max: {max_val:.3f}')
    print(f'Range: {max_val - min_val:.3f}')
    
    # Coefficient of variation
    cv = std_val / mean_val * 100 if mean_val > 0 else 0
    print(f'Coefficient of Variation: {cv:.2f}%')
    
    # Stability assessment
    if cv < 5:
        stability = 'VERY STABLE (< 5% variation)'
    elif cv < 10:
        stability = 'STABLE (< 10% variation)'
    else:
        stability = 'VARIABLE (>= 10% variation)'
        
    print(f'Assessment: {stability}')
    
    # Check for actual outliers
    Q1 = values.quantile(0.25)
    Q3 = values.quantile(0.75)
    IQR = Q3 - Q1
    
    outliers = ((values < Q1 - 1.5*IQR) | (values > Q3 + 1.5*IQR)).sum()
    outlier_rate = outliers / len(values) * 100
    
    print(f'\nOUTLIER ANALYSIS:')
    print(f'Outliers: {outliers} / {len(values)}')
    print(f'Real outlier rate: {outlier_rate:.2f}%')
    
    # Show sample values
    print(f'\nSAMPLE VALUES (first 20):')
    for i, val in enumerate(values.head(20)):
        print(f'{val:.3f}', end='  ')
        if (i + 1) % 5 == 0:
            print()
    print()
    
    # Verdict
    print(f'\nVERDICT:')
    print(f'Reported anomaly rate: 43.7%')
    print(f'Actual outlier rate: {outlier_rate:.2f}%')
    print(f'Data stability: {stability}')
    
    if outlier_rate < 5 and cv < 10:
        print('\n*** CONCLUSION: FALSE POSITIVE ***')
        print('The data is stable. 43.7% anomaly rate is wrong.')
        print('Anomaly detection algorithm is too sensitive.')
    else:
        print('\n*** CONCLUSION: MAY BE GENUINE ***')
        print('Data shows enough variation to justify some anomalies.')

if __name__ == '__main__':
    check_data_stability()