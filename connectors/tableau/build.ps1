<#
.SYNOPSIS
  Build (and optionally sign) the Argus Tableau connectors (.taco).

.DESCRIPTION
  Packages each connector directory under connectors/tableau/ into a .taco using
  Tableau's own connector-packager (from the connector-plugin-sdk), which
  validates the XML against the SDK schemas, zips it as a JAR and — when a
  keystore is supplied — signs it with jarsigner.

  Signing note: a .taco is functionally a JAR, so Tableau verifies it with the
  JRE's default keystore and requires a code-signing certificate from a public
  CA **and a valid timestamp** (Tableau rejects an untimestamped .taco). That is
  a third, separate signing path: the driver uses Azure Trusted Signing and the
  Power BI connector uses a MakePQX .pfx. Neither can produce a signed .taco.

  Without a certificate this still produces a working, unsigned .taco, but
  Tableau will only load it when signature verification is disabled
  (-DDisableVerifyConnectorPluginSignature=true on Desktop, or the TSM setting
  native_api.disable_verify_connector_plugin_signature on Server). That is fine
  for development and not acceptable for release.

.PARAMETER OutDir
  Directory to receive the .taco files. Created if absent.

.PARAMETER Connector
  Build only this connector directory name (e.g. argus-trino). Default: all.

.PARAMETER KeystorePath
  Java keystore holding the code-signing certificate. Defaults to
  $env:TACO_KEYSTORE. Empty => build unsigned.

.PARAMETER KeystoreAlias
  Alias of the signing key inside the keystore. Defaults to $env:TACO_ALIAS.

.PARAMETER SdkRef
  connector-plugin-sdk git ref to package with. Pinned so a packager change
  cannot silently alter release artifacts.

.EXAMPLE
  pwsh connectors/tableau/build.ps1 -OutDir dist
  # -> dist/argus-trino.taco (unsigned; needs the Tableau signature check off)

.EXAMPLE
  $env:TACO_KEYSTORE = 'C:\secure\codesign.jks'
  $env:TACO_ALIAS    = 'varga'
  pwsh connectors/tableau/build.ps1 -OutDir dist
  # -> dist/argus-trino.taco (signed + timestamped)
#>
[CmdletBinding()]
param(
    [string]$OutDir = "connector-dist",
    [string]$Connector = "",
    [string]$KeystorePath = $env:TACO_KEYSTORE,
    [string]$KeystoreAlias = $env:TACO_ALIAS,
    [string]$SdkRef = "v2024.2.0",
    [string]$ToolsCacheDir = (Join-Path $env:TEMP "argus-tableau-sdk")
)

$ErrorActionPreference = "Stop"
$scriptDir = $PSScriptRoot

function Write-Section($m) { Write-Host "==> $m" -ForegroundColor Cyan }

# The packager insists on being run from connector-plugin-sdk/connector-packager,
# so the SDK is cloned rather than pip-installed from a URL. Pinned to $SdkRef.
function Get-Packager {
    New-Item -ItemType Directory -Force $ToolsCacheDir | Out-Null
    $sdk = Join-Path $ToolsCacheDir "connector-plugin-sdk"

    if (-not (Test-Path (Join-Path $sdk "connector-packager"))) {
        Write-Section "Cloning connector-plugin-sdk @ $SdkRef"
        & git clone --depth 1 --branch $SdkRef `
            https://github.com/tableau/connector-plugin-sdk.git $sdk
        if ($LASTEXITCODE -ne 0) { throw "connector-plugin-sdk clone failed" }
    }

    $packagerDir = Join-Path $sdk "connector-packager"
    $venvPy = Join-Path $packagerDir ".venv\Scripts\python.exe"
    if (-not (Test-Path $venvPy)) {
        $venvPy = Join-Path $packagerDir ".venv/bin/python"
    }

    if (-not (Test-Path $venvPy)) {
        Write-Section "Installing connector-packager"
        Push-Location $packagerDir
        try {
            & python -m venv .venv
            if ($LASTEXITCODE -ne 0) { throw "venv creation failed" }
            $venvPy = if (Test-Path ".venv\Scripts\python.exe") {
                (Resolve-Path ".venv\Scripts\python.exe").Path
            } else {
                (Resolve-Path ".venv/bin/python").Path
            }
            & $venvPy -m pip install --quiet --upgrade pip setuptools wheel
            & $venvPy -m pip install --quiet .
            if ($LASTEXITCODE -ne 0) { throw "connector-packager install failed" }
        } finally { Pop-Location }
    }

    return [pscustomobject]@{ Python = $venvPy; Dir = $packagerDir }
}

$plugins = Get-ChildItem $scriptDir -Directory |
    Where-Object { Test-Path (Join-Path $_.FullName "manifest.xml") }
if ($Connector) {
    $plugins = $plugins | Where-Object { $_.Name -eq $Connector }
    if (-not $plugins) { throw "No connector directory named '$Connector'" }
}
if (-not $plugins) { throw "No connector directories found under $scriptDir" }

$packager = Get-Packager
New-Item -ItemType Directory -Force $OutDir | Out-Null
$OutDir = (Resolve-Path $OutDir).Path

$signing = [bool]$KeystorePath
if ($signing -and -not $KeystoreAlias) {
    throw "KeystoreAlias is required when KeystorePath is set"
}

Push-Location $packager.Dir
try {
    foreach ($p in $plugins) {
        Write-Section "Packaging $($p.Name)"
        $taco = Join-Path $OutDir "$($p.Name).taco"
        if (Test-Path $taco) { Remove-Item $taco -Force }

        $packArgs = @("-m", "connector_packager.package", $p.FullName, "-d", $OutDir)
        if ($signing) {
            # jarsigner runs inside the packager; it must add a timestamp or
            # Tableau will refuse the .taco once the certificate expires.
            $packArgs += @("-a", $KeystoreAlias, "-ks", $KeystorePath)
        } else {
            $packArgs += "--package-only"
        }

        & $packager.Python @packArgs
        if ($LASTEXITCODE -ne 0) { throw "packaging $($p.Name) failed" }
        if (-not (Test-Path $taco)) { throw "packager produced no $taco" }

        $size = (Get-Item $taco).Length
        $state = if ($signing) { "signed" } else { "UNSIGNED" }
        Write-Host ("   built $($p.Name).taco ({0:N0} bytes, $state)" -f $size) -ForegroundColor Green
    }
} finally { Pop-Location }

if (-not $signing) {
    Write-Host "::warning::Tableau connectors built UNSIGNED; Tableau loads them only with signature verification disabled."
}

Write-Section "Artifacts in $OutDir"
Get-ChildItem $OutDir -Filter *.taco | ForEach-Object {
    Write-Host ("   {0,10:N0}  {1}" -f $_.Length, $_.Name)
}
