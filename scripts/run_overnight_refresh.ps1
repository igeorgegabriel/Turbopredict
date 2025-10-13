param(
  [string]$MaxAgeHours = '1.0',
  [int]$Passes = 10,
  [int]$PauseSecs = 60
)

$ErrorActionPreference = 'Stop'

# Move to project root (two levels up from this script)
$root = (Split-Path -Parent (Split-Path -Parent $MyInvocation.MyCommand.Path))
Set-Location $root

# Ensure logs dir
$logs = Join-Path $root 'logs'
New-Item -ItemType Directory -Path $logs -Force | Out-Null

# Robust defaults for PI/Excel and dedup
if (-not $env:PI_FETCH_TIMEOUT) { $env:PI_FETCH_TIMEOUT = 60 }
if (-not $env:PI_FETCH_LINGER) { $env:PI_FETCH_LINGER = 20 }
if (-not $env:EXCEL_CALC_MODE) { $env:EXCEL_CALC_MODE = 'full' }
if (-not $env:NO_VISIBLE_FALLBACK) { $env:NO_VISIBLE_FALLBACK = 0 }
if (-not $env:DEDUP_MODE) { $env:DEDUP_MODE = 'end' }

# Overnight control
$env:MAX_AGE_HOURS = $MaxAgeHours
$env:REFRESH_PASSES = $Passes
$env:REFRESH_PAUSE_SECS = $PauseSecs

$ts = Get-Date -Format 'yyyyMMdd_HHmmss'
$log = Join-Path $logs "overnight_refresh_$ts.log"

Write-Host "Starting overnight refresh at $(Get-Date)" -ForegroundColor Cyan
Write-Host "Logging to: $log" -ForegroundColor Yellow
Write-Host "Env: PI_FETCH_TIMEOUT=$($env:PI_FETCH_TIMEOUT) PI_FETCH_LINGER=$($env:PI_FETCH_LINGER) EXCEL_CALC_MODE=$($env:EXCEL_CALC_MODE) DEDUP_MODE=$($env:DEDUP_MODE)" -ForegroundColor DarkGray

# Stream output to both console and file
python -u scripts/overnight_refresh_all.py *>&1 | Tee-Object -FilePath $log

