
# GIS file type descriptions for Windows Explorer
# PowerShell Constrained Language Mode compatible
# No administrator privileges required

$fileTypes = @{
    ".shp" = @{
        ProgID      = "GIS.Shapefile.Geometry"
        Description = "SHP Shapefile Geometry"
    }
    ".shx" = @{
        ProgID      = "GIS.Shapefile.Index"
        Description = "SHX Shapefile Index"
    }
    ".prj" = @{
        ProgID      = "GIS.CoordinateReference"
        Description = "PRJ Projection / CRS"
    }
    ".dbf" = @{
        ProgID      = "GIS.AttributeTable"
        Description = "DBF Attribute Table"
    }
}

foreach ($extension in $fileTypes.Keys) {
    $info = $fileTypes[$extension]

    # Register file type description
    $typePath = "HKCU:\Software\Classes\$($info.ProgID)"

    New-Item -Path $typePath -Force | Out-Null

    # Remove incorrectly configured FriendlyTypeName
    # This must reference a string resource, not plain text
    Remove-ItemProperty `
        -Path $typePath `
        -Name "FriendlyTypeName" `
        -ErrorAction SilentlyContinue

    # Set the actual description
    Set-Item -Path $typePath -Value $info.Description

    # Register extension
    $extPath = "HKCU:\Software\Classes\$extension"

    New-Item -Path $extPath -Force | Out-Null
    Set-Item -Path $extPath -Value $info.ProgID

    Write-Host "$extension -> $($info.Description)"

    # Check for Windows UserChoice overrides
    $userChoice = "HKCU:\Software\Microsoft\Windows\CurrentVersion\Explorer\FileExts\$extension\UserChoice"

    if (Test-Path $userChoice) {
        $currentProgID = (
            Get-ItemProperty `
                -Path $userChoice `
                -Name ProgId `
                -ErrorAction SilentlyContinue
        ).ProgId

        if ($currentProgID -and $currentProgID -ne $info.ProgID) {
            Write-Warning "$extension uses override: $currentProgID"
        }
    }
}

# Refresh Windows Explorer
Write-Host "Restarting Windows Explorer..."

Stop-Process -Name explorer -Force -ErrorAction SilentlyContinue
Start-Process explorer.exe

Write-Host "GIS file type registration complete."
