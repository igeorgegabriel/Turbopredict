# PowerShell script to enable PI DataLink (RUN AS ADMINISTRATOR)
Write-Host "Enabling PI DataLink..." -ForegroundColor Cyan

# Kill any existing Excel
Get-Process EXCEL -ErrorAction SilentlyContinue | Stop-Process -Force

# Wait a moment
Start-Sleep -Seconds 2

# Start Excel as administrator
$excel = New-Object -ComObject Excel.Application
$excel.Visible = $true

Write-Host "Checking COM Add-ins..." -ForegroundColor Yellow

$piDataLinkFound = $false
foreach ($addin in $excel.COMAddIns) {
    if ($addin.Description -like "*PI DataLink*" -or $addin.Description -like "*PI*") {
        Write-Host "Found: $($addin.Description) - Connected: $($addin.Connect)" -ForegroundColor Green
        $piDataLinkFound = $true

        if (-not $addin.Connect) {
            Write-Host "  Enabling PI DataLink..." -ForegroundColor Yellow
            try {
                $addin.Connect = $true
                Write-Host "  SUCCESS: PI DataLink enabled!" -ForegroundColor Green
            } catch {
                Write-Host "  ERROR: $($_.Exception.Message)" -ForegroundColor Red
            }
        }
    }
}

if (-not $piDataLinkFound) {
    Write-Host "WARNING: PI DataLink not found in COM Add-ins!" -ForegroundColor Red
}

Write-Host "`nPI DataLink should now appear in Excel ribbon." -ForegroundColor Cyan
Write-Host "Press any key to close Excel..."
$null = $Host.UI.RawUI.ReadKey("NoEcho,IncludeKeyDown")

$excel.Quit()
[System.Runtime.Interopservices.Marshal]::ReleaseComObject($excel) | Out-Null
Remove-Variable excel

Write-Host "Done!" -ForegroundColor Green
