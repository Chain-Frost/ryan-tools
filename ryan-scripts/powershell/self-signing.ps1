# =============================================================================
# PowerShell Script to:
# 1. Create a Self-Signed Code Signing Certificate
# 2. Trust the Certificate on the System
# 3. Digitally Sign the '7zipper.ps1' Script
# =============================================================================

# -------------------------------
# 1. Configuration Section
# -------------------------------

# Define variables
$CertSubject = "CN=PowerShellScriptSigning"  # Certificate Subject Name
$CertStoreLocation = "Cert:\CurrentUser\My"  # Certificate Store Location
$ExportCertPath = "$env:TEMP\PowerShellScriptSigning.cer"  # Temporary Path to Export Certificate
$ScriptToSign = "7zipper2.ps1"  # Name of the script to sign
$ScriptPath = Join-Path -Path $PSScriptRoot -ChildPath $ScriptToSign  # Full Path to the Script
$TrustedPublishersStore = "Cert:\CurrentUser\TrustedPublisher"
$TrustedRootStore = "Cert:\CurrentUser\Root"

# -------------------------------
# 2. Create a Self-Signed Code Signing Certificate
# -------------------------------

Write-Host "Creating a self-signed code signing certificate..." -ForegroundColor Cyan

try {
    # Check if certificate already exists
    $existingCert = Get-ChildItem -Path $CertStoreLocation | Where-Object { $_.Subject -eq $CertSubject }
    if ($existingCert) {
        Write-Host "A certificate with subject '$CertSubject' already exists. Skipping creation." -ForegroundColor Yellow
    }
    else {
        # Create the certificate
        $cert = New-SelfSignedCertificate -Type CodeSigningCert `
            -Subject $CertSubject `
            -CertStoreLocation $CertStoreLocation `
            -KeyExportPolicy Exportable `
            -KeySpec Signature

        Write-Host "Certificate created successfully." -ForegroundColor Green
    }
}
catch {
    Write-Host "ERROR: Failed to create the certificate." -ForegroundColor Red
    Write-Host $_.Exception.Message -ForegroundColor Red
    Exit 1
}

# -------------------------------
# 3. Export the Certificate
# -------------------------------

Write-Host "Exporting the certificate..." -ForegroundColor Cyan

try {
    # Retrieve the certificate
    $cert = Get-ChildItem -Path $CertStoreLocation | Where-Object { $_.Subject -eq $CertSubject }

    # Export the certificate to a .cer file
    Export-Certificate -Cert $cert -FilePath $ExportCertPath -Type CERT

    Write-Host "Certificate exported to '$ExportCertPath'." -ForegroundColor Green
}
catch {
    Write-Host "ERROR: Failed to export the certificate." -ForegroundColor Red
    Write-Host $_.Exception.Message -ForegroundColor Red
    Exit 1
}

# -------------------------------
# 4. Import the Certificate into Trusted Stores
# -------------------------------

Write-Host "Importing the certificate into Trusted Publishers and Trusted Root stores..." -ForegroundColor Cyan

try {
    # Import into Trusted Publishers
    Import-Certificate -FilePath $ExportCertPath -CertStoreLocation $TrustedPublishersStore

    # Import into Trusted Root Certification Authorities
    Import-Certificate -FilePath $ExportCertPath -CertStoreLocation $TrustedRootStore

    Write-Host "Certificate imported into Trusted Publishers and Trusted Root stores." -ForegroundColor Green
}
catch {
    Write-Host "ERROR: Failed to import the certificate into trusted stores." -ForegroundColor Red
    Write-Host $_.Exception.Message -ForegroundColor Red
    Exit 1
}

# -------------------------------
# 5. Remove the Exported Certificate File
# -------------------------------

Write-Host "Cleaning up temporary certificate file..." -ForegroundColor Cyan

try {
    Remove-Item -Path $ExportCertPath -ErrorAction SilentlyContinue
    Write-Host "Temporary certificate file removed." -ForegroundColor Green
}
catch {
    Write-Host "WARNING: Could not remove temporary certificate file." -ForegroundColor Yellow
}

# -------------------------------
# 6. Sign the PowerShell Script
# -------------------------------

Write-Host "Signing the PowerShell script '$ScriptToSign'..." -ForegroundColor Cyan

try {
    # Check if the script exists
    if (-Not (Test-Path -Path $ScriptPath)) {
        Write-Host "ERROR: Script '$ScriptToSign' not found at path '$ScriptPath'." -ForegroundColor Red
        Exit 1
    }

    # Retrieve the certificate
    $cert = Get-ChildItem -Path $CertStoreLocation | Where-Object { $_.Subject -eq $CertSubject }

    # Sign the script
    Set-AuthenticodeSignature -FilePath $ScriptPath -Certificate $cert -TimestampServer "http://timestamp.digicert.com" | Out-Null

    # Verify the signature
    $signature = Get-AuthenticodeSignature -FilePath $ScriptPath

    if ($signature.Status -eq 'Valid') {
        Write-Host "Script '$ScriptToSign' signed successfully." -ForegroundColor Green
    }
    else {
        Write-Host "ERROR: Failed to sign the script. Status: $($signature.Status)" -ForegroundColor Red
        Write-Host "Details: $($signature.StatusMessage)" -ForegroundColor Red
        Exit 1
    }
}
catch {
    Write-Host "ERROR: An exception occurred while signing the script." -ForegroundColor Red
    Write-Host $_.Exception.Message -ForegroundColor Red
    Exit 1
}

# -------------------------------
# 7. Final Output
# -------------------------------

Write-Host "===============================================" -ForegroundColor Cyan
Write-Host "All tasks completed successfully!" -ForegroundColor Green
Write-Host "You can now run your signed script:" -ForegroundColor Green
Write-Host "& '$ScriptPath'" -ForegroundColor Yellow
Write-Host "===============================================" -ForegroundColor Cyan
