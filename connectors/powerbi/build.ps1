<#
.SYNOPSIS
  Build (and optionally sign) the Argus Power BI custom connector.

.DESCRIPTION
  Compiles connectors/powerbi/Argus.pq into Argus.mez using Microsoft's MakePQX
  (shipped in the Microsoft.PowerQuery.SdkTools NuGet package). When a code-signing
  certificate is supplied, it additionally packs and signs Argus.pqx - the format
  Power BI can load without lowering its security setting, provided the signing
  thumbprint is trusted on the client.

  Windows-only: MakePQX is a Windows .NET Framework tool. On some .NET runtimes it
  throws a benign serialization exception AFTER writing its output; this script
  tolerates the exit code / exception and validates the produced file instead
  (the same approach documented in tests/pqtest/run-pqtest.ps1).

  Signing note: MakePQX signs a .pqx only with a .pfx code-signing certificate
  (-c/--certificate + -p/--password). It has no native Azure Trusted Signing /
  Key Vault path, so connector signing uses its own PFX secret and is deliberately
  independent of the driver's Azure Trusted Signing.

.PARAMETER OutDir
  Directory to receive Argus.mez (and Argus.pqx when signing). Created if absent.

.PARAMETER CertBase64
  Base64 of a code-signing .pfx. When set (with CertPassword) the script also
  produces a signed Argus.pqx. Defaults to $env:PQ_SIGNING_CERT_BASE64.
  Empty => build the unsigned .mez only.

.PARAMETER CertPassword
  Password for the .pfx. Defaults to $env:PQ_SIGNING_CERT_PASSWORD.

.PARAMETER SdkToolsVersion
  Microsoft.PowerQuery.SdkTools NuGet version that provides MakePQX.

.PARAMETER ToolsCacheDir
  Where the SdkTools nupkg is cached and extracted. Keep this path short: the
  package nests up to ~93 chars internally, so a long base risks MAX_PATH.

.EXAMPLE
  pwsh connectors/powerbi/build.ps1 -OutDir dist
  # -> dist/Argus.mez (unsigned)

.EXAMPLE
  $env:PQ_SIGNING_CERT_BASE64 = [Convert]::ToBase64String([IO.File]::ReadAllBytes('cert.pfx'))
  $env:PQ_SIGNING_CERT_PASSWORD = 'secret'
  pwsh connectors/powerbi/build.ps1 -OutDir dist
  # -> dist/Argus.mez + dist/Argus.pqx (signed) + dist/Argus.pqx.thumbprint.txt
#>
[CmdletBinding()]
param(
    [string]$OutDir = "connector-dist",
    [string]$CertBase64 = $env:PQ_SIGNING_CERT_BASE64,
    [string]$CertPassword = $env:PQ_SIGNING_CERT_PASSWORD,
    [string]$SdkToolsVersion = "2.155.2",
    [string]$ToolsCacheDir = (Join-Path $env:TEMP "argus-pqsdk")
)

$ErrorActionPreference = "Stop"
$scriptDir = $PSScriptRoot

function Write-Section($m) { Write-Host "==> $m" -ForegroundColor Cyan }

# Acquire MakePQX from the Microsoft.PowerQuery.SdkTools NuGet package. The nupkg
# is a plain .zip; MakePQX.exe lives under tools/. Cached by version so repeated
# builds (and CI with actions/cache) skip the ~128 MB download.
function Get-MakePQX {
    New-Item -ItemType Directory -Force $ToolsCacheDir | Out-Null
    $nupkg = Join-Path $ToolsCacheDir "sdktools-$SdkToolsVersion.nupkg"
    $tools = Join-Path $ToolsCacheDir "tools-$SdkToolsVersion"
    $exe = Join-Path $tools "tools\MakePQX.exe"
    if (Test-Path $exe) { return $exe }

    if (-not ((Test-Path $nupkg) -and ((Get-Item $nupkg).Length -gt 100MB))) {
        Write-Section "Downloading Microsoft.PowerQuery.SdkTools $SdkToolsVersion"
        $url = "https://api.nuget.org/v3-flatcontainer/microsoft.powerquery.sdktools/" +
               "$SdkToolsVersion/microsoft.powerquery.sdktools.$SdkToolsVersion.nupkg"
        # curl.exe resumes (-C -) and retries; robust over flaky links.
        & curl.exe -L --fail --retry 10 --retry-delay 5 --retry-all-errors `
            --connect-timeout 30 -C - -o $nupkg $url
        if ($LASTEXITCODE -ne 0) { throw "SdkTools download failed (curl exit $LASTEXITCODE)" }
    }

    Write-Section "Extracting MakePQX"
    if (Test-Path $tools) { Remove-Item $tools -Recurse -Force }
    Add-Type -AssemblyName System.IO.Compression.FileSystem
    [System.IO.Compression.ZipFile]::ExtractToDirectory($nupkg, $tools)
    if (-not (Test-Path $exe)) { throw "MakePQX.exe not found under $tools after extraction" }
    return $exe
}

# Run a MakePQX verb, tolerating the benign post-write serialization exception and
# MakePQX's noisy native stderr, then assert the expected output file was written.
function Invoke-MakePQX {
    param([string]$Exe, [string[]]$MakeArgs, [string]$ExpectFile, [switch]$InPlace)
    # Create ops (compile/pack): clear stale output first so a present file proves
    # freshness. In-place ops (sign) take the file as input - never pre-delete it.
    if (-not $InPlace -and $ExpectFile -and (Test-Path $ExpectFile)) { Remove-Item $ExpectFile -Force }
    $prev = $ErrorActionPreference
    $ErrorActionPreference = "Continue"
    try {
        & $Exe @MakeArgs 2>&1 | ForEach-Object { Write-Host "   $_" }
    } catch {
        Write-Host "   (MakePQX $($MakeArgs[0]) threw after writing output: $($_.Exception.Message))"
    } finally {
        $ErrorActionPreference = $prev
    }
    if ($ExpectFile -and -not (Test-Path $ExpectFile)) {
        throw "MakePQX '$($MakeArgs[0])' did not produce expected output: $ExpectFile"
    }
}

$MakePQX = Get-MakePQX
Write-Host "MakePQX: $MakePQX"

# Stage a clean source dir: exactly the files the connector references (Argus.pq +
# resources.resx + icons). Excludes Argus.query.pq / README / .mproj so MakePQX
# compiles only the connector section.
Write-Section "Staging connector source"
$work = Join-Path $env:TEMP "argus-connbuild"
$src = Join-Path $work "src"
if (Test-Path $work) { Remove-Item $work -Recurse -Force }
New-Item -ItemType Directory -Force $src | Out-Null
Copy-Item (Join-Path $scriptDir "Argus.pq") $src
Copy-Item (Join-Path $scriptDir "resources.resx") $src
Copy-Item (Join-Path $scriptDir "Argus*.png") $src
Get-ChildItem $src | ForEach-Object { Write-Host "   $($_.Name)" }

New-Item -ItemType Directory -Force $OutDir | Out-Null
$OutDir = (Resolve-Path $OutDir).Path

# Compile Argus.mez - a real M parse/compile; a syntax error aborts before any
# .mez is written, so a present file means the connector compiled.
Write-Section "Compiling Argus.mez"
$mez = Join-Path $OutDir "Argus.mez"
Invoke-MakePQX -Exe $MakePQX -MakeArgs @("compile", $src, "-d", $OutDir, "-t", "Argus") -ExpectFile $mez
Write-Host ("   built Argus.mez ({0:N0} bytes)" -f (Get-Item $mez).Length) -ForegroundColor Green

# Optionally produce a signed Argus.pqx. Signing degrades gracefully: a broken or
# missing certificate leaves the unsigned .mez in place and emits a warning rather
# than failing the release (mirrors the driver's continue-on-error signing).
$CertBase64 = ($CertBase64 -replace '\s', '')
if ($CertBase64) {
    try {
        Write-Section "Packing + signing Argus.pqx"
        $pfx = Join-Path $work "codesign.pfx"
        [IO.File]::WriteAllBytes($pfx, [Convert]::FromBase64String($CertBase64))
        $pqx = Join-Path $OutDir "Argus.pqx"

        Invoke-MakePQX -Exe $MakePQX -MakeArgs @("pack", "-mz", $mez, "-t", $pqx) -ExpectFile $pqx
        # sign modifies the .pqx in place; existence can't prove signing (the packed
        # .pqx already exists), so validate by the embedded signature part below.
        Invoke-MakePQX -Exe $MakePQX `
            -MakeArgs @("sign", $pqx, "--certificate", $pfx, "--password", $CertPassword) -ExpectFile $pqx -InPlace

        # Confirm the signature was actually embedded - machine-independent, and
        # unlike MakePQX verify it does not depend on JSON output that a runtime
        # assembly conflict can corrupt. A signed .pqx carries OPC digital-signature
        # parts under package/services/digital-signature/.
        Add-Type -AssemblyName System.IO.Compression.FileSystem
        $zip = [System.IO.Compression.ZipFile]::OpenRead($pqx)
        $signed = @($zip.Entries | Where-Object { $_.FullName -like 'package/services/digital-signature/*' }).Count -gt 0
        $zip.Dispose()
        if (-not $signed) { throw "MakePQX sign did not embed a signature into Argus.pqx" }

        # The trust thumbprint comes straight from the signing certificate.
        $thumb = (New-Object System.Security.Cryptography.X509Certificates.X509Certificate2($pfx, $CertPassword)).Thumbprint
        Set-Content (Join-Path $OutDir "Argus.pqx.thumbprint.txt") $thumb
        Write-Host "   signed; trust thumbprint: $thumb" -ForegroundColor Green

        # Best-effort: also log MakePQX's own signature/cert status when available.
        $prev = $ErrorActionPreference; $ErrorActionPreference = "Continue"
        try { & $MakePQX verify $pqx 2>&1 | ForEach-Object { Write-Host "   $_" } } catch {}
        finally { $ErrorActionPreference = $prev }

        Remove-Item $pfx -Force -ErrorAction SilentlyContinue
    } catch {
        # Do not block the release on a signing failure - ship the unsigned .mez.
        Remove-Item (Join-Path $OutDir "Argus.pqx") -Force -ErrorAction SilentlyContinue
        Write-Host "::warning::Power BI connector signing failed; shipping unsigned Argus.mez. $($_.Exception.Message)"
        Write-Warning "Signing failed: $($_.Exception.Message)"
    }
} else {
    Write-Section "No signing certificate configured - built unsigned Argus.mez only"
}

Write-Section "Artifacts in $OutDir"
Get-ChildItem $OutDir | ForEach-Object { Write-Host ("   {0,10:N0}  {1}" -f $_.Length, $_.Name) }

# MakePQX leaves a non-zero $LASTEXITCODE from its post-write serialization quirk,
# and GitHub's pwsh shell exits with $LASTEXITCODE - which would fail the job after
# a successful build. Success is validated above by output-file existence, so exit
# cleanly here; hard failures (e.g. a compile error) throw above and never reach it.
exit 0
