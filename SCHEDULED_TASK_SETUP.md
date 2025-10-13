# Windows Scheduled Task Setup - TURBOPREDICT Hourly Analysis

This guide explains how to set up automatic hourly analysis that runs even when your laptop is locked.

## Why Use Scheduled Task Instead of Option [3]?

**Problem with Option [3]:**
- Windows suspends console applications when laptop is locked/sleeping
- Process stops running overnight when you lock your laptop
- No reliable background operation

**Solution - Windows Scheduled Task:**
- Runs as SYSTEM account (not affected by user lock/logout)
- Continues running even when laptop is locked
- Survives sleep/wake cycles
- Proper system-level background service

## Quick Setup (Recommended)

1. **Right-click** `setup_scheduled_task.bat` and select **"Run as Administrator"**
2. Press any key when setup is complete
3. Done! The task will run every hour automatically

## Manual Setup (Alternative)

If the quick setup doesn't work, follow these steps:

### Method 1: Using XML Import

1. Open **Task Scheduler** (press Windows key, type "Task Scheduler")
2. Click **"Import Task..."** in the right panel
3. Select `TurboPredictTask.xml` from this folder
4. Click **OK**
5. Enter administrator password if prompted

### Method 2: Using Command Line

Open **Command Prompt as Administrator** and run:

```cmd
schtasks /create /tn "TurboPredictHourlyAnalysis" /xml "C:\Users\george.gabrielujai\Documents\CodeX\TurboPredictTask.xml"
```

## What Gets Installed?

**Task Name:** TurboPredictHourlyAnalysis
**Schedule:** Every hour, 24/7
**Runs as:** SYSTEM (doesn't require user login)
**Action:** Executes `scripts/scheduled_analysis.ps1`

The PowerShell script:
1. Runs hourly refresh (single cycle) to fetch fresh PI data
2. Analyzes all 13 units with anomaly detection
3. Generates plots and reports
4. Logs everything to `logs/scheduled_analysis.log`

## Testing the Task

### Run Immediately (Don't Wait for Next Hour)

```cmd
schtasks /run /tn "TurboPredictHourlyAnalysis"
```

### Check Task Status

```cmd
schtasks /query /tn "TurboPredictHourlyAnalysis" /v
```

### View Logs

```cmd
type logs\scheduled_analysis.log
```

Or open the file in Notepad:
```cmd
notepad logs\scheduled_analysis.log
```

## Managing the Task

### Disable (Pause) the Task

```cmd
schtasks /change /tn "TurboPredictHourlyAnalysis" /disable
```

### Enable the Task

```cmd
schtasks /change /tn "TurboPredictHourlyAnalysis" /enable
```

### Delete the Task

```cmd
schtasks /delete /tn "TurboPredictHourlyAnalysis" /f
```

## Troubleshooting

### Task Not Running?

1. Check if task is enabled:
   ```cmd
   schtasks /query /tn "TurboPredictHourlyAnalysis"
   ```

2. Check last run result:
   - Open Task Scheduler
   - Find "TurboPredictHourlyAnalysis" in the list
   - Check "Last Run Result" column (should be "0x0" for success)

3. Check logs:
   ```cmd
   type logs\scheduled_analysis.log
   ```

### Python Not Found Error?

The task runs as SYSTEM account, which may not have Python in its PATH.

**Fix:** Edit `scripts/scheduled_analysis.ps1` and change line:
```powershell
$PythonCmd = Get-Command python -ErrorAction SilentlyContinue
```
to:
```powershell
$PythonCmd = "C:\Python\python.exe"  # Replace with your Python path
```

To find your Python path:
```cmd
where python
```

### Permission Errors?

Make sure you ran the setup as Administrator. Re-run `setup_scheduled_task.bat` with Administrator privileges.

## What About the Background Process (bash 93f446)?

The current background process running `hourly_refresh.py` will stop when you lock your laptop. You can safely stop it:

1. Press Ctrl+C in the terminal window
2. Or close the terminal window

The scheduled task will take over and provide more reliable background operation.

## Recommended Workflow

1. **Install the scheduled task** (one-time setup)
2. **Let it run in the background** (automatic hourly)
3. **Use turbopredict.py interactively** when you want to:
   - Check specific units (Option [2])
   - View database overview (Option [1])
   - Generate plots on demand (Option [4])

**Don't use Option [3] anymore** - the scheduled task does the same thing but works when laptop is locked.

## File Locations

- **PowerShell Script:** `scripts/scheduled_analysis.ps1`
- **Batch Installer:** `setup_scheduled_task.bat`
- **XML Definition:** `TurboPredictTask.xml`
- **Logs:** `logs/scheduled_analysis.log`
- **Per-unit logs:** `logs/analysis_<UNIT>_stdout.log`

## Next Steps

After setup:
1. Run test: `schtasks /run /tn "TurboPredictHourlyAnalysis"`
2. Check log: `type logs\scheduled_analysis.log`
3. Lock your laptop and go home
4. Come back tomorrow and verify logs show hourly execution

The system will now maintain fresh data and analysis automatically, even when your laptop is locked overnight.
