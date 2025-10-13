# ChatGPT Prompt for Industrial ML Implementation

## Complete Prompt to Send to ChatGPT:

```
I need a comprehensive Python machine learning system for industrial process monitoring with the following requirements:

**DATA STRUCTURE:**
- 12 industrial units: K-12-01, K-16-01, K-19-01, K-31-01, C-02001, C-104, C-201, C-202, C-1301, C-1302, C-13001, 07-MT01-K001, XT-07002
- Data format: Parquet files with columns ['plant', 'unit', 'tag', 'time', 'value']
- Each unit has 50-80 PI tags (sensors: FI, TI, PI, PDI, HC, PC, SI, SV, VI, ZI)
- Time series data with 0.1 hour intervals over 1-1.5 years
- Files located in: data/processed/{unit}_1y_0p1h.parquet

**MACHINE LEARNING REQUIREMENTS:**

1. **Baseline Model Training:**
   - Anomaly detection using Isolation Forest and DBSCAN clustering
   - Statistical baselines (mean, std, quartiles) for each tag
   - Normal behavior pattern recognition
   - Trend analysis and seasonality detection

2. **Predictive Models:**
   - Time series forecasting for critical tags
   - Multi-step ahead prediction (1-hour, 6-hour, 24-hour)
   - Feature engineering with lagged values and rolling statistics
   - Model validation with proper train/test splits

3. **Training Data Generation:**
   - Synthetic anomaly injection for supervised learning
   - Balanced datasets for classification tasks
   - Time series datasets for forecasting
   - Feature engineering pipelines

4. **Model Performance:**
   - Cross-validation strategies for time series
   - Performance metrics (MSE, MAE, F1-score, AUC)
   - Model interpretability and feature importance
   - Automated hyperparameter tuning

**TECHNICAL SPECIFICATIONS:**
- Use scikit-learn, pandas, numpy, and optionally xgboost/lightgbm
- Memory-efficient processing for large datasets (50,000+ rows per unit)
- Robust error handling for missing data and outliers
- Scalable architecture to handle all 12 units automatically
- Export trained models using joblib or pickle
- Generate comprehensive training reports

**OUTPUT REQUIREMENTS:**
1. Complete Python script with all ML functionality
2. Individual unit training capability
3. Batch processing for all units
4. Model persistence and loading functions
5. Performance evaluation and visualization
6. Training dataset export for external use

**INDUSTRY CONTEXT:**
This is for industrial process monitoring where:
- Speed compensation is critical (PCM.{unit}.020SI6601.PV tags)
- Anomaly detection helps prevent equipment failures
- Predictive maintenance requires forecasting capabilities
- Historical patterns inform operational decisions

Please provide a complete, production-ready Python implementation that can train ML models across all 12 industrial units, with proper data handling, model validation, and export capabilities.
```

## How to Use This Prompt:

1. **Copy the entire prompt above** (everything between the triple quotes)
2. **Paste it into ChatGPT**
3. **ChatGPT will generate** a comprehensive Python ML system
4. **Save the output** as a .py file in your CodeX directory
5. **Run the script** to train models for all your units

## Expected ChatGPT Output:

ChatGPT should provide you with:
- ✅ Complete ML training pipeline
- ✅ Anomaly detection models
- ✅ Predictive forecasting models
- ✅ Training data generation
- ✅ Model evaluation and metrics
- ✅ Batch processing for all 12 units
- ✅ Model persistence (save/load)
- ✅ Performance reporting

## Alternative Shorter Prompt:

If you want a more concise prompt, use this instead:

```
Create a Python ML system for 12 industrial units with Parquet data (format: plant, unit, tag, time, value). Need: 1) Anomaly detection with Isolation Forest, 2) Time series forecasting, 3) Training data generation, 4) Batch processing all units from data/processed/*.parquet files, 5) Model export/import. Units: K-12-01, K-16-01, K-19-01, K-31-01, C-02001, C-104, C-201, C-202, C-1301, C-1302, C-13001, 07-MT01-K001, XT-07002. Focus on industrial process monitoring with 50-80 PI tags per unit.
```

## What This Will Give You:

**Machine Learning Capabilities:**
- 🤖 **Baseline Models** - Normal behavior patterns for each unit
- 📈 **Predictive Models** - Forecast future values for critical tags
- 🚨 **Anomaly Detection** - Identify unusual patterns early
- 📊 **Training Data** - Structured datasets for further ML development
- 📋 **Historical Trends** - Pattern recognition across time periods

**Business Value:**
- ⚡ **Predictive Maintenance** - Prevent equipment failures
- 🎯 **Process Optimization** - Identify efficiency opportunities
- 🔍 **Quality Control** - Detect deviations before they impact production
- 📈 **Performance Monitoring** - Track KPIs and trends
- 🛡️ **Risk Management** - Early warning system for critical issues