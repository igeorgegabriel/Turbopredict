#!/usr/bin/env python3
"""
Comprehensive Machine Learning Training System for Industrial Units
Provides baseline data, training data for ML models, and historical trends.
"""

import pandas as pd
import numpy as np
from pathlib import Path
import time
from sklearn.preprocessing import StandardScaler, MinMaxScaler
from sklearn.model_selection import train_test_split
from sklearn.ensemble import IsolationForest, RandomForestRegressor
from sklearn.cluster import DBSCAN
from sklearn.decomposition import PCA
from sklearn.metrics import classification_report, mean_squared_error
import warnings
warnings.filterwarnings('ignore')

class MLTrainingSystem:
    """Complete ML training system for all industrial units."""

    def __init__(self, processed_dir=None):
        if processed_dir is None:
            self.processed_dir = Path(r"C:\Users\george.gabrielujai\Documents\CodeX\data\processed")
        else:
            self.processed_dir = Path(processed_dir)

        self.units = []
        self.unit_data = {}
        self.ml_models = {}
        self.baselines = {}

    def discover_units(self):
        """Discover all available units from parquet files."""

        print("=== DISCOVERING INDUSTRIAL UNITS ===")

        # Find all main parquet files (not dedup or backup)
        parquet_files = list(self.processed_dir.glob("*_1y_0p1h.parquet"))

        for file in parquet_files:
            if not any(x in file.name for x in ['.dedup', '.backup']):
                unit_name = file.name.split('_')[0]
                self.units.append(unit_name)
                print(f"✓ Found unit: {unit_name}")

        print(f"\nTotal units discovered: {len(self.units)}")
        return self.units

    def load_unit_data(self, unit_name, sample_size=None):
        """Load and prepare data for a specific unit."""

        file_path = self.processed_dir / f"{unit_name}_1y_0p1h.parquet"

        try:
            df = pd.read_parquet(file_path)

            # Sample data if requested for faster processing
            if sample_size and len(df) > sample_size:
                df = df.sample(n=sample_size, random_state=42).sort_values('time')

            # Basic data quality checks
            total_rows = len(df)
            non_null_values = df['value'].count()
            unique_tags = df['tag'].nunique()

            print(f"\n{unit_name} Data Summary:")
            print(f"  Rows: {total_rows:,}")
            print(f"  Non-null values: {non_null_values:,} ({non_null_values/total_rows*100:.1f}%)")
            print(f"  Unique tags: {unique_tags}")
            print(f"  Time range: {df['time'].min()} to {df['time'].max()}")

            self.unit_data[unit_name] = df
            return df

        except Exception as e:
            print(f"Error loading {unit_name}: {e}")
            return None

    def create_baseline_models(self, unit_name):
        """Create baseline models for normal behavior patterns."""

        if unit_name not in self.unit_data:
            print(f"No data loaded for {unit_name}")
            return None

        df = self.unit_data[unit_name]

        print(f"\nCreating baseline models for {unit_name}...")

        # Prepare data for ML
        # Pivot to get tag-based features
        pivot_df = df.pivot_table(
            index='time',
            columns='tag',
            values='value',
            aggfunc='mean'
        ).fillna(method='ffill').fillna(method='bfill')

        if len(pivot_df) < 10:
            print(f"Insufficient data for {unit_name}")
            return None

        # Remove columns with too many nulls
        valid_columns = pivot_df.columns[pivot_df.isnull().sum() < len(pivot_df) * 0.5]
        X = pivot_df[valid_columns].fillna(0)

        models = {}

        # 1. Anomaly Detection Model (Isolation Forest)
        iso_forest = IsolationForest(contamination=0.1, random_state=42)
        iso_forest.fit(X)
        models['anomaly_detector'] = iso_forest

        # 2. Clustering Model (DBSCAN)
        scaler = StandardScaler()
        X_scaled = scaler.fit_transform(X)

        # Use PCA for dimensionality reduction if too many features
        if X_scaled.shape[1] > 20:
            pca = PCA(n_components=20)
            X_pca = pca.fit_transform(X_scaled)
            models['pca'] = pca
        else:
            X_pca = X_scaled

        dbscan = DBSCAN(eps=0.5, min_samples=5)
        clusters = dbscan.fit_predict(X_pca)
        models['clustering'] = dbscan
        models['scaler'] = scaler

        # 3. Baseline Statistics
        baseline_stats = {
            'mean': X.mean(),
            'std': X.std(),
            'min': X.min(),
            'max': X.max(),
            'q25': X.quantile(0.25),
            'q75': X.quantile(0.75)
        }

        models['baseline_stats'] = baseline_stats
        models['feature_columns'] = valid_columns.tolist()

        # 4. Time Series Trends
        trends = {}
        for col in valid_columns[:10]:  # Analyze top 10 tags
            values = X[col].values
            if len(values) > 1:
                # Simple trend analysis
                time_index = np.arange(len(values))
                trend_coef = np.polyfit(time_index, values, 1)[0]
                trends[col] = {
                    'trend_coefficient': trend_coef,
                    'mean_value': np.mean(values),
                    'volatility': np.std(values)
                }

        models['trends'] = trends

        # Store models
        self.ml_models[unit_name] = models

        print(f"✓ Created baseline models for {unit_name}")
        print(f"  Features: {len(valid_columns)}")
        print(f"  Clusters found: {len(set(clusters)) - (1 if -1 in clusters else 0)}")
        print(f"  Anomaly detection ready")

        return models

    def train_predictive_models(self, unit_name):
        """Train predictive models for forecasting."""

        if unit_name not in self.unit_data:
            return None

        df = self.unit_data[unit_name]

        print(f"\nTraining predictive models for {unit_name}...")

        # Focus on key tags for prediction
        tag_counts = df['tag'].value_counts()
        top_tags = tag_counts.head(5).index.tolist()

        predictive_models = {}

        for tag in top_tags:
            tag_data = df[df['tag'] == tag].copy()
            if len(tag_data) < 50:
                continue

            # Sort by time
            tag_data = tag_data.sort_values('time')

            # Create lagged features
            tag_data['value_lag1'] = tag_data['value'].shift(1)
            tag_data['value_lag2'] = tag_data['value'].shift(2)
            tag_data['value_lag3'] = tag_data['value'].shift(3)
            tag_data['rolling_mean_5'] = tag_data['value'].rolling(5).mean()
            tag_data['rolling_std_5'] = tag_data['value'].rolling(5).std()

            # Remove rows with NaN
            tag_data = tag_data.dropna()

            if len(tag_data) < 20:
                continue

            # Prepare features and target
            feature_cols = ['value_lag1', 'value_lag2', 'value_lag3', 'rolling_mean_5', 'rolling_std_5']
            X = tag_data[feature_cols]
            y = tag_data['value']

            # Train-test split
            X_train, X_test, y_train, y_test = train_test_split(
                X, y, test_size=0.2, random_state=42
            )

            # Random Forest Regressor
            rf_model = RandomForestRegressor(n_estimators=50, random_state=42)
            rf_model.fit(X_train, y_train)

            # Evaluate
            y_pred = rf_model.predict(X_test)
            mse = mean_squared_error(y_test, y_pred)

            predictive_models[tag] = {
                'model': rf_model,
                'features': feature_cols,
                'mse': mse,
                'feature_importance': dict(zip(feature_cols, rf_model.feature_importances_))
            }

        # Store predictive models
        if unit_name not in self.ml_models:
            self.ml_models[unit_name] = {}

        self.ml_models[unit_name]['predictive'] = predictive_models

        print(f"✓ Trained predictive models for {len(predictive_models)} tags")

        return predictive_models

    def generate_training_datasets(self, unit_name):
        """Generate structured training datasets for external ML use."""

        if unit_name not in self.unit_data:
            return None

        df = self.unit_data[unit_name]

        print(f"\nGenerating training datasets for {unit_name}...")

        # 1. Anomaly Detection Dataset
        pivot_df = df.pivot_table(
            index='time',
            columns='tag',
            values='value',
            aggfunc='mean'
        ).fillna(method='ffill').fillna(method='bfill')

        # Add synthetic anomalies for training
        anomaly_df = pivot_df.copy()
        n_anomalies = int(len(anomaly_df) * 0.05)  # 5% anomalies

        anomaly_indices = np.random.choice(anomaly_df.index, n_anomalies, replace=False)
        labels = np.zeros(len(anomaly_df))

        for idx in anomaly_indices:
            # Inject anomalies
            for col in anomaly_df.columns[:3]:  # Modify first 3 columns
                original_value = anomaly_df.loc[idx, col]
                if not pd.isna(original_value):
                    # Add noise or shift
                    anomaly_df.loc[idx, col] = original_value * np.random.uniform(1.5, 3.0)
            labels[anomaly_df.index.get_loc(idx)] = 1

        # 2. Time Series Forecasting Dataset
        forecasting_datasets = {}
        top_tags = df['tag'].value_counts().head(5).index

        for tag in top_tags:
            tag_data = df[df['tag'] == tag].sort_values('time')
            if len(tag_data) > 100:
                forecasting_datasets[tag] = tag_data[['time', 'value']].copy()

        # 3. Classification Dataset (Normal vs Abnormal periods)
        classification_df = pivot_df.copy()

        # Define "abnormal" as values beyond 2 standard deviations
        abnormal_mask = np.zeros(len(classification_df))
        for col in classification_df.columns[:10]:  # Check first 10 columns
            values = classification_df[col].dropna()
            if len(values) > 10:
                mean_val = values.mean()
                std_val = values.std()
                threshold_high = mean_val + 2 * std_val
                threshold_low = mean_val - 2 * std_val

                abnormal_indices = classification_df[
                    (classification_df[col] > threshold_high) |
                    (classification_df[col] < threshold_low)
                ].index

                for idx in abnormal_indices:
                    abnormal_mask[classification_df.index.get_loc(idx)] = 1

        datasets = {
            'anomaly_detection': {
                'features': anomaly_df.fillna(0),
                'labels': labels,
                'description': 'Features with synthetic anomalies for anomaly detection training'
            },
            'forecasting': {
                'datasets': forecasting_datasets,
                'description': 'Time series data for forecasting models'
            },
            'classification': {
                'features': classification_df.fillna(0),
                'labels': abnormal_mask,
                'description': 'Normal vs abnormal period classification'
            },
            'raw_pivot': {
                'data': pivot_df,
                'description': 'Raw pivoted data for custom ML applications'
            }
        }

        print(f"✓ Generated training datasets:")
        print(f"  Anomaly detection: {len(anomaly_df)} samples, {len(anomaly_df.columns)} features")
        print(f"  Forecasting: {len(forecasting_datasets)} tag datasets")
        print(f"  Classification: {len(classification_df)} samples")

        return datasets

    def train_all_units(self, sample_size=50000):
        """Train ML models for all discovered units."""

        print("=" * 70)
        print("COMPREHENSIVE ML TRAINING FOR ALL UNITS")
        print("=" * 70)

        # Discover units
        units = self.discover_units()

        results = {}

        for unit in units:
            print(f"\n{'='*50}")
            print(f"TRAINING UNIT: {unit}")
            print(f"{'='*50}")

            # Load data
            df = self.load_unit_data(unit, sample_size=sample_size)
            if df is None:
                continue

            # Create baseline models
            baseline_models = self.create_baseline_models(unit)

            # Train predictive models
            predictive_models = self.train_predictive_models(unit)

            # Generate training datasets
            training_datasets = self.generate_training_datasets(unit)

            results[unit] = {
                'baseline_models': baseline_models,
                'predictive_models': predictive_models,
                'training_datasets': training_datasets,
                'data_summary': {
                    'total_rows': len(df),
                    'unique_tags': df['tag'].nunique(),
                    'time_range': (df['time'].min(), df['time'].max()),
                    'data_quality': df['value'].count() / len(df)
                }
            }

        print(f"\n{'='*70}")
        print("ML TRAINING SUMMARY")
        print(f"{'='*70}")

        for unit, result in results.items():
            if result['baseline_models']:
                print(f"\n{unit}:")
                print(f"  ✓ Baseline models: Anomaly detection, clustering, statistics")
                if result['predictive_models']:
                    print(f"  ✓ Predictive models: {len(result['predictive_models'])} tag forecasters")
                print(f"  ✓ Training datasets: Anomaly, forecasting, classification")
                print(f"  ✓ Data quality: {result['data_summary']['data_quality']*100:.1f}%")

        return results

def main():
    """Main training execution."""

    # Initialize ML training system
    ml_system = MLTrainingSystem()

    # Train all units
    results = ml_system.train_all_units(sample_size=50000)

    print(f"\n{'='*70}")
    print("ML TRAINING COMPLETE!")
    print(f"{'='*70}")
    print(f"✓ Trained models for {len(results)} units")
    print("✓ Baseline models for normal behavior patterns")
    print("✓ Predictive models for forecasting")
    print("✓ Training datasets for external ML use")
    print("✓ Historical trend analysis")

    # Save results summary
    summary_file = Path(r"C:\Users\george.gabrielujai\Documents\CodeX\ml_training_summary.txt")
    with open(summary_file, 'w') as f:
        f.write("ML TRAINING SUMMARY\n")
        f.write("="*50 + "\n\n")

        for unit, result in results.items():
            f.write(f"{unit}:\n")
            if result['baseline_models']:
                f.write(f"  - Features: {len(result['baseline_models'].get('feature_columns', []))}\n")
                f.write(f"  - Data quality: {result['data_summary']['data_quality']*100:.1f}%\n")
                f.write(f"  - Unique tags: {result['data_summary']['unique_tags']}\n")
            f.write("\n")

    print(f"\nSummary saved to: {summary_file}")

if __name__ == "__main__":
    main()