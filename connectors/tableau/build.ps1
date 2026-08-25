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
  cannot silently alter release artifacts. Must be a tag or branch that really
  exists upstream: the repo publishes tdvt-* tags (plus v1.4* and 2020.1) and
  has never had a v2024.x, which is what the previous pin asked for.

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
    [string]$SdkRef = "tdvt-2.13.7",
    # $env:TEMP only exists on Windows; under PowerShell Core on the Linux and
    # macOS runners it is null and Join-Path throws on a null -Path. GetTempPath
    # resolves on all three (TMPDIR, then /tmp).
    [string]$ToolsCacheDir = (Join-Path ([System.IO.Path]::GetTempPath()) "argus-tableau-sdk")
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

        # The packager names the file from the manifest's class and version
        # (argus_postgres-v0.1.0.taco), not from the directory, so package into
        # a scratch directory and take whatever single file comes out rather
        # than guessing the name.
        $stage = Join-Path ([System.IO.Path]::GetTempPath()) ("argus-taco-" + $p.Name)
        if (Test-Path $stage) { Remove-Item $stage -Recurse -Force }
        New-Item -ItemType Directory -Force $stage | Out-Null

        # The packager no longer signs, and no longer takes --package-only:
        # it always emits an unsigned .taco and leaves jarsigner to the caller,
        # which is why signing is a separate step below (and in ci.yml).
        & $packager.Python -m connector_packager.package $p.FullName -d $stage
        if ($LASTEXITCODE -ne 0) { throw "packaging $($p.Name) failed" }

        $produced = @(Get-ChildItem $stage -Filter *.taco)
        if ($produced.Count -ne 1) {
            throw "packaging $($p.Name) produced $($produced.Count) .taco files, expected 1"
        }
        $taco = Join-Path $OutDir $produced[0].Name
        Move-Item $produced[0].FullName $taco -Force
        Remove-Item $stage -Recurse -Force

        if ($signing) {
            # -tsa is not optional: without a timestamp Tableau refuses the
            # .taco as soon as the certificate expires.
            & jarsigner -tsa http://timestamp.digicert.com `
                -keystore $KeystorePath $taco $KeystoreAlias
            if ($LASTEXITCODE -ne 0) { throw "signing $($p.Name) failed" }
            & jarsigner -verify -strict $taco
            if ($LASTEXITCODE -ne 0) { throw "signature check for $($p.Name) failed" }
        }

        $size = (Get-Item $taco).Length
        $state = if ($signing) { "signed" } else { "UNSIGNED" }
        Write-Host ("   built $($produced[0].Name) ({0:N0} bytes, $state)" -f $size) -ForegroundColor Green
    }
} finally { Pop-Location }

if (-not $signing) {
    Write-Host "::warning::Tableau connectors built UNSIGNED; Tableau loads them only with signature verification disabled."
}

Write-Section "Artifacts in $OutDir"
Get-ChildItem $OutDir -Filter *.taco | ForEach-Object {
    Write-Host ("   {0,10:N0}  {1}" -f $_.Length, $_.Name)
}
