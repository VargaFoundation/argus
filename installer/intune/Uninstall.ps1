# Argus ODBC Driver - Silent Uninstallation Script for Microsoft Intune
# Exit code 0 = success, 1 = failure

$ErrorActionPreference = "Stop"

# Read install directory from registry
$regKey = "HKLM:\Software\Argus ODBC Driver"
if (-not (Test-Path $regKey)) {
    Write-Output "Argus ODBC Driver is not installed (registry key not found)"
    exit 0
}

$installDir = (Get-ItemProperty -Path $regKey -Name "InstallDir").InstallDir
$uninstallerPath = Join-Path $installDir "uninstall.exe"

if (-not (Test-Path $uninstallerPath)) {
    Write-Error "Uninstaller not found: $uninstallerPath"
    exit 1
}

# Run the NSIS uninstaller in silent mode
$process = Start-Process -FilePath $uninstallerPath -ArgumentList "/S" -Wait -PassThru

if ($process.ExitCode -ne 0) {
    Write-Error "Uninstaller exited with code $($process.ExitCode)"
    exit 1
}

# Verify the ODBC driver registration was removed
$odbcRegPath = "HKLM:\SOFTWARE\ODBC\ODBCINST.INI\Argus ODBC Driver"
if (Test-Path $odbcRegPath) {
    Write-Error "ODBC driver registry key still present after uninstallation"
    exit 1
}

Write-Output "Argus ODBC Driver uninstalled successfully"
exit 0
