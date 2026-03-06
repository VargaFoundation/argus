; Argus ODBC Driver - NSIS Windows Installer
; Installs the driver DLL and registers it with the ODBC subsystem

!include "MUI2.nsh"
!include "x64.nsh"

; ── Version ──────────────────────────────────────────────────────
; Pass -DVERSION=x.y.z on the makensis command line to set the version.
; Falls back to "0.0.0" if not provided.
!ifndef VERSION
    !define VERSION "0.0.0"
!endif

; ── General ──────────────────────────────────────────────────────
Name "Argus ODBC Driver ${VERSION}"
OutFile "argus-odbc-installer.exe"
InstallDir "$PROGRAMFILES64\Argus ODBC Driver"
InstallDirRegKey HKLM "Software\Argus ODBC Driver" "InstallDir"
RequestExecutionLevel admin

; ── Interface ────────────────────────────────────────────────────
!define MUI_ABORTWARNING
!insertmacro MUI_PAGE_WELCOME
!insertmacro MUI_PAGE_DIRECTORY
!insertmacro MUI_PAGE_INSTFILES
!insertmacro MUI_PAGE_FINISH

!insertmacro MUI_UNPAGE_CONFIRM
!insertmacro MUI_UNPAGE_INSTFILES

!insertmacro MUI_LANGUAGE "English"

; ── Install Section ──────────────────────────────────────────────
Section "Argus ODBC Driver" SecDriver
    SectionIn RO

    ; Force 64-bit registry view so the driver is visible to 64-bit
    ; applications (PowerBI, Excel, odbcad32.exe, etc.)
    SetRegView 64

    SetOutPath "$INSTDIR"

    ; Copy driver DLL and all bundled dependencies
    File "argus_odbc.dll"
    File /nonfatal "*.dll"

    ; Store install directory
    WriteRegStr HKLM "Software\Argus ODBC Driver" "InstallDir" "$INSTDIR"

    ; ── Register ODBC driver (64-bit registry) ────────────────────
    ; Driver registration in ODBCINST.INI registry keys
    WriteRegStr HKLM "SOFTWARE\ODBC\ODBCINST.INI\Argus ODBC Driver" \
        "Driver" "$INSTDIR\argus_odbc.dll"
    WriteRegStr HKLM "SOFTWARE\ODBC\ODBCINST.INI\Argus ODBC Driver" \
        "Setup" "$INSTDIR\argus_odbc.dll"
    WriteRegStr HKLM "SOFTWARE\ODBC\ODBCINST.INI\Argus ODBC Driver" \
        "Description" "Argus ODBC Driver for Hive, Impala, Trino, Phoenix, and Kudu"
    WriteRegStr HKLM "SOFTWARE\ODBC\ODBCINST.INI\Argus ODBC Driver" \
        "CompanyName" "Varga Foundation"
    WriteRegDWORD HKLM "SOFTWARE\ODBC\ODBCINST.INI\Argus ODBC Driver" \
        "UsageCount" 1

    ; Add to the installed drivers list
    WriteRegStr HKLM "SOFTWARE\ODBC\ODBCINST.INI\ODBC Drivers" \
        "Argus ODBC Driver" "Installed"

    ; ── Start Menu shortcuts ──────────────────────────────────────
    CreateDirectory "$SMPROGRAMS\Argus ODBC Driver"
    CreateShortcut "$SMPROGRAMS\Argus ODBC Driver\ODBC Data Sources.lnk" \
        "$WINDIR\System32\odbcad32.exe"
    CreateShortcut "$SMPROGRAMS\Argus ODBC Driver\Uninstall.lnk" \
        "$INSTDIR\uninstall.exe"

    ; ── Create uninstaller ────────────────────────────────────────
    WriteUninstaller "$INSTDIR\uninstall.exe"

    ; Add/Remove Programs entry
    WriteRegStr HKLM "Software\Microsoft\Windows\CurrentVersion\Uninstall\ArgusODBC" \
        "DisplayName" "Argus ODBC Driver"
    WriteRegStr HKLM "Software\Microsoft\Windows\CurrentVersion\Uninstall\ArgusODBC" \
        "UninstallString" "$\"$INSTDIR\uninstall.exe$\""
    WriteRegStr HKLM "Software\Microsoft\Windows\CurrentVersion\Uninstall\ArgusODBC" \
        "InstallLocation" "$INSTDIR"
    WriteRegStr HKLM "Software\Microsoft\Windows\CurrentVersion\Uninstall\ArgusODBC" \
        "Publisher" "Varga Foundation"
    WriteRegStr HKLM "Software\Microsoft\Windows\CurrentVersion\Uninstall\ArgusODBC" \
        "DisplayVersion" "${VERSION}"
    WriteRegDWORD HKLM "Software\Microsoft\Windows\CurrentVersion\Uninstall\ArgusODBC" \
        "NoModify" 1
    WriteRegDWORD HKLM "Software\Microsoft\Windows\CurrentVersion\Uninstall\ArgusODBC" \
        "NoRepair" 1
SectionEnd

; ── Uninstall Section ────────────────────────────────────────────
Section "Uninstall"
    ; Force 64-bit registry view for cleanup
    SetRegView 64

    ; Remove ODBC driver registration
    DeleteRegKey HKLM "SOFTWARE\ODBC\ODBCINST.INI\Argus ODBC Driver"
    DeleteRegValue HKLM "SOFTWARE\ODBC\ODBCINST.INI\ODBC Drivers" \
        "Argus ODBC Driver"

    ; Remove Start Menu shortcuts
    Delete "$SMPROGRAMS\Argus ODBC Driver\ODBC Data Sources.lnk"
    Delete "$SMPROGRAMS\Argus ODBC Driver\Uninstall.lnk"
    RMDir "$SMPROGRAMS\Argus ODBC Driver"

    ; Remove files
    Delete "$INSTDIR\*.dll"
    Delete "$INSTDIR\uninstall.exe"
    RMDir "$INSTDIR"

    ; Remove registry keys
    DeleteRegKey HKLM "Software\Microsoft\Windows\CurrentVersion\Uninstall\ArgusODBC"
    DeleteRegKey HKLM "Software\Argus ODBC Driver"
SectionEnd
