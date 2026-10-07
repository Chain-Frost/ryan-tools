<#
.SYNOPSIS
Repairs permissions on named directories.

.DESCRIPTION
Finds directories matching FolderName directly below RootPath and resets their
ACLs recursively so they inherit the same permissions as neighbouring
directories. The invoking Windows account is used for ownership when an owner
repair is needed; no user name is hard-coded.

The script uses Windows icacls.exe and is compatible with PowerShell
Constrained Language Mode. FolderName accepts an exact directory name, a
wildcard pattern, or multiple names or patterns. Use -WhatIf to preview the
matching directories.

.EXAMPLE
powershell.exe -NoLogo -NoProfile -ExecutionPolicy Bypass -File '.\ryan-scripts\repair-folder-permissions.ps1' -FolderName 'some-folder'

Repairs one folder directly below the repository root.

.EXAMPLE
powershell.exe -NoLogo -NoProfile -ExecutionPolicy Bypass -File '.\ryan-scripts\repair-folder-permissions.ps1' -FolderName 'pytest-cache-files-*'

Repairs every directly contained folder matching the wildcard pattern.

.EXAMPLE
powershell.exe -NoLogo -NoProfile -ExecutionPolicy Bypass -File '.\ryan-scripts\repair-folder-permissions.ps1' -FolderName 'some-folder' -WhatIf

Previews the repair without changing permissions or ownership.

.EXAMPLE
powershell.exe -NoLogo -NoProfile -ExecutionPolicy Bypass -Command "& '.\ryan-scripts\repair-folder-permissions.ps1' -FolderName 'folder-one', 'folder-two'"

Repairs multiple named folders. PowerShell's Command mode is used so the
FolderName array is passed correctly.

.EXAMPLE
powershell.exe -NoLogo -NoProfile -ExecutionPolicy Bypass -File 'Q:\BGER\PER\RPRT\ryan-tools\ryan-scripts\repair-folder-permissions.ps1' -FolderName 'some-folder' -RootPath 'Q:\BGER\PER\RPRT\ryan-tools'

Uses explicit absolute script and root paths.
#>

[CmdletBinding()]
param(
    [Parameter(Mandatory = $true, Position = 0)]
    [ValidateNotNullOrEmpty()]
    [string[]]$FolderName,

    [Parameter()]
    [string]$RootPath,

    [Parameter()]
    [switch]$WhatIf
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

function Invoke-Icacls {
    param(
        [Parameter(Mandatory = $true)]
        [string[]]$Arguments
    )

    $output = @(& icacls.exe @Arguments 2>&1)
    $exitCode = $LASTEXITCODE
    $output | ForEach-Object { Write-Host $_ }

    $reportedFailures = @($output | Where-Object { "$_" -match 'Failed processing [1-9][0-9]* files' })
    if (($exitCode -ne 0) -or ($reportedFailures.Count -gt 0)) {
        throw "icacls.exe failed with exit code $exitCode."
    }
}

function Invoke-Takeown {
    param(
        [Parameter(Mandatory = $true)]
        [string]$TargetPath
    )

    $output = @(& takeown.exe /F $TargetPath 2>&1)
    $exitCode = $LASTEXITCODE
    $output | ForEach-Object { Write-Host $_ }

    if ($exitCode -ne 0) {
        throw "takeown.exe failed with exit code $exitCode on '$TargetPath'."
    }
}

try {
    if (-not $RootPath) {
        $RootPath = Join-Path -Path $PSScriptRoot -ChildPath '..'
    }

    $resolvedRoot = (Resolve-Path -LiteralPath $RootPath).Path
    $rootItem = Get-Item -LiteralPath $resolvedRoot -Force
    if (-not $rootItem.PSIsContainer) {
        throw "RootPath is not a directory: $resolvedRoot"
    }

    $currentUser = "$env:USERDOMAIN\$env:USERNAME"
    $expectedOwner = (Get-Acl -LiteralPath $resolvedRoot).Owner

    $childDirectories = @(Get-ChildItem -LiteralPath $resolvedRoot -Directory -Force)
    $matchedDirectories = @()

    foreach ($requestedName in $FolderName) {
        if (($requestedName -match '[\\/]') -or ($requestedName -eq '.') -or ($requestedName -eq '..')) {
            throw "FolderName must be a name or wildcard pattern, not a path: '$requestedName'. Use RootPath for the parent directory."
        }

        $matchedDirectories += @($childDirectories | Where-Object { $_.Name -like $requestedName })
    }

    $targets = @($matchedDirectories | Sort-Object -Property FullName -Unique)

    if ($targets.Count -eq 0) {
        throw "No directories matching '$($FolderName -join ', ')' were found directly below '$resolvedRoot'."
    }

    Write-Host "Found $($targets.Count) matching directories below '$resolvedRoot'."
    Write-Host "Current Windows user: $currentUser"
    Write-Host "Current user's file-server owner identity: $expectedOwner"

    foreach ($target in $targets) {
        if ($WhatIf) {
            Write-Host "What if: Reset inherited permissions recursively and verify owner on '$($target.FullName)'"
            continue
        }

        Write-Host "Repairing $($target.FullName)"

        # Repair the root before enumeration, because its protected ACL can
        # prevent PowerShell from seeing otherwise accessible descendants.
        Invoke-Icacls -Arguments @($target.FullName, '/reset', '/C', '/Q')

        $descendants = @(Get-ChildItem -LiteralPath $target.FullName -Force -Recurse)
        if ($descendants.Count -gt 0) {
            Invoke-Icacls -Arguments @($target.FullName, '/reset', '/T', '/C', '/Q')
        }

        $repairedItems = @($target) + $descendants
        foreach ($item in $repairedItems) {
            $acl = Get-Acl -LiteralPath $item.FullName
            if ($acl.Owner -ne $expectedOwner) {
                Invoke-Takeown -TargetPath $item.FullName
                $acl = Get-Acl -LiteralPath $item.FullName
                if ($acl.Owner -ne $expectedOwner) {
                    throw "Owner mismatch on '$($item.FullName)': expected '$expectedOwner', found '$($acl.Owner)'."
                }
            }
            if ($acl.AreAccessRulesProtected) {
                throw "Permission inheritance remains disabled on '$($item.FullName)'."
            }

            $inheritedFullControl = @(
                $acl.Access | Where-Object {
                    $_.IsInherited -and
                    $_.AccessControlType -eq 'Allow' -and
                    "$($_.FileSystemRights)" -match 'FullControl'
                }
            )
            if ($inheritedFullControl.Count -eq 0) {
                throw "No inherited Full Control permission was found on '$($item.FullName)'."
            }
        }
    }

    Write-Host 'Permission repair completed successfully.'
    exit 0
}
catch {
    Write-Error $_
    exit 1
}
