# Deploying Argus ODBC Driver via Microsoft Intune

## Prerequisites

- [IntuneWinAppUtil.exe](https://github.com/microsoft/Microsoft-Win32-Content-Prep-Tool) to package the installer as `.intunewin`
- Microsoft Intune admin access

## Generate the .intunewin package

Place the signed `argus-odbc-installer.exe` alongside the PowerShell scripts in a staging folder, then run:

```powershell
# From the repository root
Copy-Item installer/intune/*.ps1 installer/staging/
IntuneWinAppUtil.exe -c installer/staging -s argus-odbc-installer.exe -o .
```

This produces `argus-odbc-installer.intunewin`.

> **Note:** The CI pipeline (`.github/workflows/release.yml`) generates and uploads this package automatically on tagged releases.

## Intune portal configuration

Add a **Windows app (Win32)** in the [Intune admin center](https://intune.microsoft.com) with these settings:

| Field | Value |
|-------|-------|
| **Name** | Argus ODBC Driver |
| **Publisher** | Varga Foundation |
| **App package file** | `argus-odbc-installer.intunewin` |
| **Install command** | `powershell.exe -ExecutionPolicy Bypass -File Install.ps1` |
| **Uninstall command** | `powershell.exe -ExecutionPolicy Bypass -File Uninstall.ps1` |
| **Install behavior** | System |
| **Detection rules** | Use a custom detection script → upload `Detect.ps1` |
| **Return codes** | 0 = Success, 1 = Failed, 3010 = Soft reboot |
| **Requirements** | Windows 10 64-bit (minimum) |

## Testing locally

```powershell
# Install
powershell.exe -ExecutionPolicy Bypass -File installer\intune\Install.ps1

# Verify driver appears in ODBC Data Source Administrator
odbcad32.exe

# Detect
powershell.exe -ExecutionPolicy Bypass -File installer\intune\Detect.ps1

# Uninstall
powershell.exe -ExecutionPolicy Bypass -File installer\intune\Uninstall.ps1
```
