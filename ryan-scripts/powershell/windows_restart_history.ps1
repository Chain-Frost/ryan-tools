<#
.SYNOPSIS
Summarises recent Windows startups and their likely shutdown or restart causes.

.DESCRIPTION
Correlates Windows System event-log records for operating-system startup, clean
shutdown, user or service restart requests, unexpected shutdown, Kernel-Power,
bugcheck and Windows Update events.

An unexpected shutdown without a bugcheck cannot be attributed reliably to a
specific cause. It may represent power loss, a forced reset, a held power
button, or a system hang. The script reports that limitation instead of
guessing.

.PARAMETER Since
Only report startups at or after this local date and time.

.PARAMETER MaxStartups
Maximum number of startups to report, starting with the most recent.

.PARAMETER CorrelationLookbackDays
Number of days before Since to search for a shutdown associated with the first
reported startup.

.PARAMETER CsvPath
Optional CSV output path. No file is written when this is omitted.

.PARAMETER Force
Allow an existing CsvPath to be replaced.

.EXAMPLE
.\windows_restart_history.ps1

.EXAMPLE
.\windows_restart_history.ps1 -Since (Get-Date).AddDays(-90) -MaxStartups 50

.EXAMPLE
.\windows_restart_history.ps1 -CsvPath .\restart-history.csv
#>

[CmdletBinding()]
param(
    [datetime]$Since = (Get-Date).AddDays(-30),
    [ValidateRange(1, 1000)]
    [int]$MaxStartups = 20,
    [ValidateRange(1, 365)]
    [int]$CorrelationLookbackDays = 7,
    [string]$CsvPath,
    [switch]$Force
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$systemLogName = "System"
$kernelGeneralProvider = "Microsoft-Windows-Kernel-General"
$kernelPowerProvider = "Microsoft-Windows-Kernel-Power"
$windowsUpdateProvider = "Microsoft-Windows-WindowsUpdateClient"
$unexpectedEvidenceWindowMinutes = 5
$updateEvidenceWindowMinutes = 15

function Get-EventPropertyValue {
    param(
        [Parameter(Mandatory)]
        $EventRecord,
        [Parameter(Mandatory)]
        [int]$Index
    )

    $values = @($EventRecord.Properties | Select-Object -ExpandProperty Value)
    if ($Index -lt 0 -or $Index -ge $values.Count) {
        return $null
    }
    return $values[$Index]
}

function Format-Duration {
    param(
        [Parameter(Mandatory)]
        [timespan]$Duration
    )

    if ($Duration.TotalDays -ge 1.0) {
        return "{0}d {1}h {2}m" -f $Duration.Days, $Duration.Hours, $Duration.Minutes
    }
    if ($Duration.TotalHours -ge 1.0) {
        return "{0}h {1}m {2}s" -f $Duration.Hours, $Duration.Minutes, $Duration.Seconds
    }
    return "{0}m {1}s" -f $Duration.Minutes, $Duration.Seconds
}

function Get-ShortProcessName {
    param(
        [AllowNull()]
        [string]$ProcessDescription
    )

    if ([string]::IsNullOrWhiteSpace($ProcessDescription)) {
        return ""
    }
    $executable = $ProcessDescription -replace "\s+\([^)]*\)$", ""
    return Split-Path -Leaf $executable
}

$queryStart = $Since.AddDays(-$CorrelationLookbackDays)
$eventIds = 12, 19, 41, 1001, 1074, 6005, 6006, 6008
$events = @(
    Get-WinEvent -FilterHashtable @{
        LogName  = $systemLogName
        StartTime = $queryStart
        Id        = $eventIds
    }
)

$allStartups = @(
    $events |
        Where-Object { $_.Id -eq 12 -and $_.ProviderName -eq $kernelGeneralProvider } |
        Sort-Object TimeCreated
)
$reportedStartups = @(
    $allStartups |
        Where-Object { $_.TimeCreated -ge $Since } |
        Select-Object -Last $MaxStartups |
        Sort-Object TimeCreated -Descending
)

if ($reportedStartups.Count -eq 0) {
    Write-Output "No Windows startup events were found on or after $($Since.ToString('yyyy-MM-dd HH:mm:ss'))."
    exit 0
}

$results = @()
foreach ($startup in $reportedStartups) {
    $startupTime = $startup.TimeCreated
    $previousStartup = @(
        $allStartups |
            Where-Object { $_.TimeCreated -lt $startupTime } |
            Select-Object -Last 1
    )
    $correlationStart = $queryStart
    if ($previousStartup.Count -gt 0) {
        $correlationStart = $previousStartup[0].TimeCreated
    }

    $shutdownRequest = @(
        $events |
            Where-Object {
                $_.Id -eq 1074 -and
                $_.TimeCreated -gt $correlationStart -and
                $_.TimeCreated -lt $startupTime
            } |
            Sort-Object TimeCreated |
            Select-Object -Last 1
    )
    $cleanStop = @(
        $events |
            Where-Object {
                $_.Id -eq 6006 -and
                $_.TimeCreated -gt $correlationStart -and
                $_.TimeCreated -lt $startupTime
            } |
            Sort-Object TimeCreated |
            Select-Object -Last 1
    )
    $unexpectedEvents = @(
        $events |
            Where-Object {
                $_.Id -in 41, 6008 -and
                $_.TimeCreated -ge $startupTime.AddSeconds(-10) -and
                $_.TimeCreated -le $startupTime.AddMinutes($unexpectedEvidenceWindowMinutes)
            } |
            Sort-Object TimeCreated
    )
    $bugcheckEvents = @(
        $events |
            Where-Object {
                $_.Id -eq 1001 -and
                $_.ProviderName -match "BugCheck|WER-SystemErrorReporting" -and
                $_.TimeCreated -ge $startupTime.AddSeconds(-10) -and
                $_.TimeCreated -le $startupTime.AddMinutes($unexpectedEvidenceWindowMinutes)
            } |
            Sort-Object TimeCreated
    )
    $updates = @(
        $events |
            Where-Object {
                $_.Id -eq 19 -and
                $_.ProviderName -eq $windowsUpdateProvider -and
                $_.TimeCreated -ge $startupTime.AddMinutes(-$updateEvidenceWindowMinutes) -and
                $_.TimeCreated -le $startupTime.AddMinutes($updateEvidenceWindowMinutes)
            } |
            Sort-Object TimeCreated
    )

    $classification = "Unclassified startup"
    $initiator = ""
    $reason = "No correlated shutdown request was found"
    $evidenceParts = @()
    $downtime = ""

    if ($unexpectedEvents.Count -gt 0) {
        $classification = "Unexpected shutdown"
        $reason = "Windows recorded an improper shutdown"
        $eventIdsFound = @($unexpectedEvents | Select-Object -ExpandProperty Id -Unique)
        $evidenceParts += "Event IDs: $($eventIdsFound -join ', ')"

        $kernelPowerEvent = @(
            $unexpectedEvents |
                Where-Object { $_.Id -eq 41 -and $_.ProviderName -eq $kernelPowerProvider } |
                Select-Object -First 1
        )
        $bugcheckCode = 0
        if ($kernelPowerEvent.Count -gt 0) {
            $rawBugcheckCode = Get-EventPropertyValue -EventRecord $kernelPowerEvent[0] -Index 0
            if ($null -ne $rawBugcheckCode) {
                $bugcheckCode = [int64]$rawBugcheckCode
            }
        }
        if ($bugcheckCode -ne 0 -or $bugcheckEvents.Count -gt 0) {
            if ($bugcheckCode -ne 0) {
                $reason = "Unexpected shutdown with bugcheck 0x{0:X}" -f $bugcheckCode
            }
            else {
                $reason = "Unexpected shutdown with a recorded bugcheck"
            }
            $evidenceParts += "A bugcheck record was found"
        }
        else {
            $evidenceParts += "No bugcheck was recorded; power loss, forced reset, power-button shutdown, or a system hang cannot be distinguished"
        }
    }
    elseif ($shutdownRequest.Count -gt 0) {
        $request = $shutdownRequest[0]
        $processDescription = [string](Get-EventPropertyValue -EventRecord $request -Index 0)
        $windowsReason = [string](Get-EventPropertyValue -EventRecord $request -Index 2)
        $shutdownType = [string](Get-EventPropertyValue -EventRecord $request -Index 4)
        $user = [string](Get-EventPropertyValue -EventRecord $request -Index 6)
        $processName = Get-ShortProcessName -ProcessDescription $processDescription
        $initiator = $user
        if ($processName) {
            $initiator = "$user via $processName"
        }
        $reason = $windowsReason

        if ($shutdownType -match "power off") {
            $classification = "Normal power-on after clean shutdown"
        }
        elseif (
            $windowsReason -match "\(Planned\)" -or
            $processName -match "^(MoUsoCoreWorker|TrustedInstaller)\.exe$"
        ) {
            $classification = "Planned system restart"
        }
        else {
            $classification = "Manual clean restart"
        }
        $evidenceParts += "Event ID 1074 at $($request.TimeCreated.ToString('yyyy-MM-dd HH:mm:ss'))"
    }
    elseif ($cleanStop.Count -gt 0) {
        $classification = "Clean startup after shutdown"
        $reason = "The event-log service stopped cleanly, but no shutdown requester was recorded"
    }

    if ($cleanStop.Count -gt 0) {
        $downtime = Format-Duration -Duration ($startupTime - $cleanStop[0].TimeCreated)
        $evidenceParts += "Clean event-log stop (6006)"
    }
    if ($updates.Count -gt 0) {
        $updateNames = @(
            $updates |
                ForEach-Object { [string](Get-EventPropertyValue -EventRecord $_ -Index 0) } |
                Where-Object { $_ } |
                Select-Object -Unique
        )
        if ($updateNames.Count -gt 0) {
            $evidenceParts += "Nearby updates: $($updateNames -join '; ')"
        }
    }

    $results += New-Object -TypeName PSObject -Property ([ordered]@{
        Startup       = $startupTime.ToString("dddd, yyyy-MM-dd HH:mm:ss zzz")
        Classification = $classification
        Initiator     = $initiator
        Reason        = $reason
        Downtime      = $downtime
        Evidence      = $evidenceParts -join ". "
    })
}

$results | Format-List Startup, Classification, Initiator, Reason, Downtime, Evidence

if ($CsvPath) {
    if ((Test-Path -LiteralPath $CsvPath) -and -not $Force) {
        throw "CSV output already exists. Use -Force to replace it: $CsvPath"
    }
    $parent = Split-Path -Parent $CsvPath
    if ($parent -and -not (Test-Path -LiteralPath $parent -PathType Container)) {
        throw "CSV output folder does not exist: $parent"
    }
    $results | Export-Csv -LiteralPath $CsvPath -NoTypeInformation -Encoding UTF8
    Write-Output "Exported $($results.Count) startup records to $CsvPath"
}
