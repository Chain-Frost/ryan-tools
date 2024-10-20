param (
    [string]$Directory = "C:\folder\folder",
    [string]$Extension = "*.msg"
)

# https://www.powershellgallery.com/packages/ReadMsgFile/1.2
# Install-Module -Name ReadMsgFile -RequiredVersion 1.2 

# if no admin:
# Install-Module -Name ReadMsgFile -RequiredVersion 1.2 -Scope CurrentUser

# e.g.
# dir C:\folder\folder\*.msg | % { ren $_ "$((Read-MsgFile $_).Sent.toString('yyyy.MM.dd.HHmm')) - $($_.Name)" }

# Runs fast
# https://chatgpt.com/share/73790793-a13e-4ca7-8614-db37a7980e09

# Ensure the ReadMsgFile module is installed
if (!(Get-Module -ListAvailable -Name ReadMsgFile)) {
    Install-Module -Name ReadMsgFile -RequiredVersion 1.2 -Scope CurrentUser
}

# Initialize a hashtable to keep track of filenames
$filenameTracker = @{}

# Process each file
Get-ChildItem -Path $Directory -Filter $Extension | ForEach-Object {
    $file = $_
    $msgFile = Read-MsgFile $file.FullName

    # Print the current file name being operated on
    Write-Host "Processing file: $($file.Name)"

    # Generate the base new name using the Sent date
    $baseNewName = "$($msgFile.Sent.ToString('yyyy.MM.dd.HHmm')) - $($file.Name)"
    $newName = $baseNewName

    # Check for conflicts and append a counter if necessary
    $counter = 1
    while (Test-Path (Join-Path $file.DirectoryName $newName)) {
        $newName = "$baseNewName ($counter)"
        $counter++
    }

    # Rename the file
    Rename-Item -Path $file.FullName -NewName $newName
}

Write-Host "Processing complete."
