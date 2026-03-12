# Argus ODBC Driver - Silent Installation Script for Microsoft Intune
# Exit code 0 = success, 1 = failure

$ErrorActionPreference = "Stop"

$installerName = "argus-odbc-installer.exe"
$installerPath = Join-Path $PSScriptRoot $installerName

if (-not (Test-Path $installerPath)) {
    Write-Error "Installer not found: $installerPath"
    exit 1
}

# Run the NSIS installer in silent mode
$process = Start-Process -FilePath $installerPath -ArgumentList "/S" -Wait -PassThru

if ($process.ExitCode -ne 0) {
    Write-Error "Installer exited with code $($process.ExitCode)"
    exit 1
}

# Verify the ODBC driver was registered (64-bit registry)
$regPath = "HKLM:\SOFTWARE\ODBC\ODBCINST.INI\Argus ODBC Driver"
if (-not (Test-Path $regPath)) {
    Write-Error "ODBC driver registry key not found after installation"
    exit 1
}

Write-Output "Argus ODBC Driver installed successfully"
exit 0
