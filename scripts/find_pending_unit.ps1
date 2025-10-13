param(
    [Parameter(Mandatory=$true)][string]$Unit,
    [int]$DaysBack = 180,
    [string[]]$FolderPaths = @('Inbox')
)

function Get-OutlookFolderByPath {
    param(
        [object]$Root,
        [string]$Path
    )
    $f = $Root
    foreach ($part in $Path -split '[\\/]') {
        if (-not [string]::IsNullOrWhiteSpace($part)) {
            $f = $f.Folders.Item($part)
            if (-not $f) { throw "Folder not found: $Path (missing '$part')" }
        }
    }
    return $f
}

function Get-MsgLinesWithPendingInfo {
    param(
        [string]$Text
    )
    $lines = $Text -split "\r?\n"
    $hits = @()
    foreach ($ln in $lines) {
        if ($ln -match '(?i)\b(pending|awaiting|back.?order|on hold|TBA)\b') {
            $trim = ($ln -replace '\s+', ' ').Trim()
            if ($trim.Length -gt 0) { $hits += $trim }
        }
    }
    return $hits
}

$null = Add-Type -AssemblyName "Microsoft.Office.Interop.Outlook" -ErrorAction SilentlyContinue
try {
    $outlook = [Runtime.InteropServices.Marshal]::GetActiveObject("Outlook.Application")
} catch {
    try {
        $outlook = New-Object -ComObject Outlook.Application
    } catch {
        Write-Warning "Unable to attach to Outlook via COM. Make sure classic Outlook is running (not New Outlook)."
        $outlook = $null
    }
}
$ns = $outlook.GetNamespace('MAPI')

$root = $ns.Session.DefaultStore.GetRootFolder()
$since = (Get-Date).AddDays(-1 * $DaysBack)
$sinceStr = $since.ToString('MM/dd/yyyy hh:mm tt')

$results = @()

foreach ($fp in $FolderPaths) {
    try {
        $folder = Get-OutlookFolderByPath -Root $root -Path $fp
    } catch {
        Write-Warning $_
        continue
    }

    $items = $folder.Items
    $items.Sort('[ReceivedTime]', $true)
    $restriction = "[ReceivedTime] >= '$sinceStr'"
    $recent = $items.Restrict($restriction)

    foreach ($m in $recent) {
        try {
            $subject = ''+$m.Subject
            $body = ''+$m.Body
            $received = $m.ReceivedTime

            if ($subject -match [Regex]::Escape($Unit) -or $body -match [Regex]::Escape($Unit)) {
                $lines = Get-MsgLinesWithPendingInfo -Text $body
                if ($lines.Count -eq 0) {
                    # also check subject
                    $lines = Get-MsgLinesWithPendingInfo -Text $subject
                }
                if ($lines.Count -gt 0) {
                    $results += [pscustomobject]@{
                        Folder = $fp
                        Subject = $subject
                        ReceivedTime = $received
                        PendingLines = ($lines -join ' | ')
                    }
                }
            }
        } catch {
            continue
        }
    }
}

$results = $results | Sort-Object ReceivedTime -Descending

if ($results.Count -eq 0) {
    Write-Output "No 'pending' info found for unit '$Unit' in folders: $($FolderPaths -join ', ') within last $DaysBack days."
    exit 0
}

$results | Select-Object ReceivedTime, Folder, Subject, PendingLines | Format-List
