# PI DataLink Setup Guide for C-02001 Full Dataset

## Overview
This guide helps you configure PI DataLink to fetch 1.5 years of historical data for all 80 C-02001 PI tags.

## Prerequisites
1. **PI DataLink Add-in** installed in Excel
2. **PI Server connection** configured
3. **Network access** to PI Server
4. **Excel file** with all 80 tag columns prepared

## Files Created
- `PCMSB_C02001_Full_Structure.xlsx` - Excel with all 80 tag columns
- `configure_pi_datalink.py` - Automated configuration script
- `automated_pi_data_fetch.py` - Batch processing script

## Step-by-Step Manual Setup

### Step 1: Open Excel File
```
File: C:\Users\george.gabrielujai\Documents\CodeX\excel\PCMSB\PCMSB_C02001_Full_Structure.xlsx
```

### Step 2: Configure PI DataLink Connection
1. Open Excel and go to **Data** tab
2. Look for **PI DataLink** ribbon/section
3. Click **PI Server** to configure connection
4. Enter your PI Server details:
   - Server: `your_pi_server_name`
   - User credentials if required

### Step 3: Set Up Time Parameters
In the Excel file, configure:
```
Start Time: 03/30/2024 00:00:00  (1.5 years ago)
End Time:   09/28/2025 23:59:59  (now)
Interval:   6m                   (6 minutes = 0.1 hours)
```

### Step 4: Configure PI Formulas
For each of the 80 PI tags, use one of these formula patterns:

#### Option A: PIValue (Most Common)
```excel
=PIValue("PCM.C-02001.020FI0101.PV", A8)
```

#### Option B: PIArchive
```excel
=PIArchive("PCM.C-02001.020FI0101.PV", A8)
```

#### Option C: PIInterp
```excel
=PIInterp("PCM.C-02001.020FI0101.PV", A8)
```

### Step 5: Set Up Time Series
1. In column A (starting row 8), create time series:
   ```excel
   A8:  03/30/2024 00:00:00
   A9:  03/30/2024 00:06:00
   A10: 03/30/2024 00:12:00
   ... (continue with 6-minute intervals)
   ```

2. Or use formula for automatic time series:
   ```excel
   A8: =DATE(2024,3,30)+TIME(0,0,0)
   A9: =A8+TIME(0,6,0)
   ```
   Then copy down for ~131,000 rows (1.5 years at 6-minute intervals)

### Step 6: Apply PI Formulas to All Tags
1. **For each tag column (B through CC):**
   - Enter the PI formula in row 8
   - Copy the formula down for all time points
   - Wait for data to populate

2. **Example for tag in column B:**
   ```excel
   B8: =PIValue("PCM.C-02001.020FI0101.PV", $A8)
   ```
   Copy this formula down to B131000+ (all time points)

### Step 7: Refresh PI DataLink Data
1. **Manual Refresh:**
   - Press `Ctrl + Alt + F9`
   - Or use PI DataLink → Refresh All

2. **Automatic Refresh:**
   - Data → Refresh All
   - Set refresh intervals if needed

## Expected Results

### Data Volume
- **Rows:** ~131,281 (1.5 years at 6-minute intervals)
- **Columns:** 83 (1 timestamp + 80 PI tags + 2 metadata)
- **File Size:** 15-20 MB (parquet compressed)

### Data Quality Checks
- All 80 tags should have data
- No completely empty columns
- Reasonable value ranges for each tag type
- Continuous time series with 6-minute intervals

## Automated Scripts Available

### 1. Configuration Script
```bash
python configure_pi_datalink.py
```
- Sets up basic structure
- Adds configuration parameters
- Creates formula templates

### 2. Batch Processing Script
```bash
python automated_pi_data_fetch.py
```
- Handles large dataset processing
- Manages Excel memory efficiently
- Monitors data population

### 3. Final Parquet Generation
```bash
python build_pcmsb_parquet.py
```
- Converts Excel data to parquet
- Validates all 80 tags
- Creates final compressed dataset

## Troubleshooting

### Common Issues

#### 1. PI Server Connection Failed
- Verify PI Server name and network access
- Check PI DataLink installation
- Verify user permissions on PI Server

#### 2. Formulas Not Working
- Try different formula patterns (PIValue, PIArchive, etc.)
- Check tag names for typos
- Verify PI tag exists on server

#### 3. Excel Performance Issues
- Process in smaller batches (10,000 rows at a time)
- Close other applications to free memory
- Use Excel 64-bit version for large datasets

#### 4. Data Not Populating
- Allow more time for refresh (large datasets take time)
- Check PI Server performance
- Verify date ranges are valid

### Performance Tips
1. **Batch Processing:** Process 10-20 tags at a time
2. **Memory Management:** Save frequently and close/reopen Excel
3. **Network:** Use wired connection for stability
4. **Timing:** Run during off-peak hours

## Validation Steps

### 1. Check Data Population
```python
# Run this to verify data quality
python analyze_parquet_data.py
```

### 2. Expected Output
```
Shape: (131281, 83)
Columns: 83
Tag columns with data: 80/80
File size: 15-20 MB
```

### 3. Quality Metrics
- All 80 tags should have non-null data
- Time series should be continuous
- Values should be within expected ranges

## Final Steps

Once PI DataLink is fully configured and data is populated:

1. **Generate Final Parquet:**
   ```bash
   python build_pcmsb_parquet.py
   ```

2. **Verify Results:**
   ```bash
   python analyze_parquet_data.py
   ```

3. **Expected Final File:**
   - File: `PCMSB_C-02001_Full_80tags.parquet`
   - Size: 15-20 MB
   - Rows: ~131,281
   - Columns: 83 (all 80 tags + metadata)

This will give you the complete 1.5-year dataset with all 80 PI tags at 0.1-hour intervals as originally requested.