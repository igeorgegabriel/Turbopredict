# TURBOPREDICT Continuous Service
# PowerShell script for Windows Task Scheduler
# Runs continuous hourly refresh and analysis loop (works when laptop is locked)

# Set working directory
$WorkDir = "C:\Users\george.gabrielujai\Documents\CodeX"
Set-Location $WorkDir

# Configure encoding for Python output
$env:PYTHONIOENCODING = "utf-8"

# Log file path
$LogFile = Join-Path $WorkDir "logs\scheduled_service.log"

# Ensure logs directory exists
New-Item -ItemType Directory -Force -Path (Join-Path $WorkDir "logs") | Out-Null

# Function to log messages
function Write-Log {
    param($Message)
    $Timestamp = Get-Date -Format "yyyy-MM-dd HH:mm:ss"
    $LogMessage = "[$Timestamp] $Message"
    Write-Output $LogMessage
    Add-Content -Path $LogFile -Value $LogMessage
}

Write-Log "========================================"
Write-Log "TURBOPREDICT Continuous Service Started"
Write-Log "========================================"

try {
    # Check if Python is available
    $PythonCmd = Get-Command python -ErrorAction SilentlyContinue
    if (-not $PythonCmd) {
        Write-Log "ERROR: Python not found in PATH"
        exit 1
    }

    Write-Log "Python found: $($PythonCmd.Source)"

    # Run continuous hourly refresh service
    # This script runs forever, doing refresh + analysis every hour
    Write-Log "Starting continuous hourly refresh and analysis service..."
    Write-Log "Service will run indefinitely (hourly cycles)"

    # Run the hourly refresh service (continuous mode)
    # Python script handles its own logging to logs\hourly_refresh.log
    python scripts\hourly_refresh.py

    # If we get here, the service stopped unexpectedly
    Write-Log "WARNING: Hourly refresh service stopped unexpectedly"
    exit 1

} catch {
    Write-Log "ERROR: $($_.Exception.Message)"
    Write-Log "Stack trace: $($_.ScriptStackTrace)"
    exit 1
}
