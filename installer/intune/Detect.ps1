# Argus ODBC Driver - Detection Script for Microsoft Intune
# Intune detection rules:
#   - stdout output + exit 0 = application detected (installed)
#   - no stdout + exit 0     = application not detected (not installed)

$regPath = "HKLM:\SOFTWARE\ODBC\ODBCINST.INI\Argus ODBC Driver"

# Check if the ODBC driver registry key exists
if (-not (Test-Path $regPath)) {
    exit 0
}

# Check if the driver DLL exists at the registered path
$driverPath = (Get-ItemProperty -Path $regPath -Name "Driver" -ErrorAction SilentlyContinue).Driver
if (-not $driverPath -or -not (Test-Path $driverPath)) {
    exit 0
}

# Application is installed
Write-Output "Argus ODBC Driver detected"
exit 0
