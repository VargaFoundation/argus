<#
.SYNOPSIS
  Tests headless Power BI (moteur mashup reel) du driver ODBC Argus, via PQTest.

.DESCRIPTION
  Valide que le driver fonctionne a travers le moteur Power Query / mashup de Power BI
  (pas seulement au niveau ODBC brut) : credentials, navigation (Navigateur), et
  execution de requetes. Chemin 100% headless — aucun clic Power BI Desktop requis.

  Sequence par requete :
    credential-template -> set-credential (store chiffre) -> compare (vs golden .pqout)

  Prerequis :
    - Windows (PQTest et le moteur mashup sont Windows-only), .NET, 7-Zip ou tar.
    - Le driver "Argus ODBC Driver" installe (l'installeur signe des releases).
    - Un backend joignable. Defaut : Trino sur localhost:8080, catalogue tpch
      (docker : trinodb/trino, cf. tests/integration/docker-compose.yml).

  PQTest est recupere automatiquement depuis NuGet (Microsoft.PowerQuery.SdkTools)
  s'il n'est pas deja fourni via -PqToolDir.

.EXAMPLE
  pwsh -File run-pqtest.ps1
  pwsh -File run-pqtest.ps1 -TrinoHost 10.0.0.5 -TrinoPort 8080
#>
[CmdletBinding()]
param(
    [string]$TrinoHost = "localhost",
    [int]$TrinoPort = 8080,
    [string]$PqToolDir,                                   # dossier contenant PQTest.exe/MakePQX.exe
    [string]$SdkToolsVersion = "2.155.2",
    [string]$WorkDir = (Join-Path $env:TEMP "argus-pqtest")
)
$ErrorActionPreference = "Stop"
$root = $PSScriptRoot
New-Item -ItemType Directory -Force $WorkDir | Out-Null

# --- 1. Acquerir PQTest / MakePQX ------------------------------------------------
function Get-PqTools {
    if ($PqToolDir -and (Test-Path (Join-Path $PqToolDir "PQTest.exe"))) { return $PqToolDir }
    $tools = Join-Path $WorkDir "pqtool"
    if (Test-Path (Join-Path $tools "tools\PQTest.exe")) { return (Join-Path $tools "tools") }
    $nupkg = Join-Path $WorkDir "sdktools.nupkg"
    $url = "https://api.nuget.org/v3-flatcontainer/microsoft.powerquery.sdktools/$SdkToolsVersion/microsoft.powerquery.sdktools.$SdkToolsVersion.nupkg"
    Write-Host "Telechargement PQTest ($SdkToolsVersion)..."
    Invoke-WebRequest -Uri $url -OutFile $nupkg
    Expand-Archive -Path $nupkg -DestinationPath $tools -Force   # .nupkg = .zip
    return (Join-Path $tools "tools")
}
$toolsDir = Get-PqTools
$PQTest = Join-Path $toolsDir "PQTest.exe"
$MakePQX = Join-Path $toolsDir "MakePQX.exe"

# --- 2. Compiler les connecteurs (.pq -> .mez) -----------------------------------
# NB : MakePQX peut lever une exception de serialisation en fin d'execution sur
# certains runtimes .NET ; le .mez est ecrit AVANT, donc on tolere le code de sortie
# et on verifie la presence du .mez.
function Build-Connector([string]$pqFile, [string]$targetName) {
    $bd = Join-Path $WorkDir $targetName
    New-Item -ItemType Directory -Force $bd | Out-Null
    Copy-Item $pqFile (Join-Path $bd (Split-Path $pqFile -Leaf)) -Force
    $out = Join-Path $bd "out"
    New-Item -ItemType Directory -Force $out | Out-Null
    Push-Location $bd
    try { & $MakePQX compile -d $out -t $targetName 2>$null | Out-Null } catch {}
    Pop-Location
    $mez = Join-Path $out "$targetName.mez"
    if (-not (Test-Path $mez)) { throw "Echec compilation connecteur : $pqFile" }
    return $mez
}
$mezQuery = Build-Connector (Join-Path $root "connectors\Argus.pq")     "Argus"
$mezNav   = Build-Connector (Join-Path $root "connectors\ArgusNav.pq")  "ArgusNav"
$mezFold  = Build-Connector (Join-Path $root "connectors\ArgusFold.pq") "ArgusFold"

# --- 3. Executer chaque requete golden -------------------------------------------
$store = Join-Path $WorkDir "credstore.bin"
if (Test-Path $store) { Remove-Item $store }
$queries = Get-ChildItem (Join-Path $root "queries") -Filter "*.query.pq"
$pass = 0; $fail = 0
foreach ($q in $queries) {
    $name = $q.BaseName
    $src = Get-Content $q.FullName -Raw
    # substitution de l'hote/port si demande
    $src = $src.Replace("HOST=localhost;PORT=8080", "HOST=$TrinoHost;PORT=$TrinoPort")
    $tmpQ = Join-Path $WorkDir $q.Name
    Set-Content $tmpQ $src -Encoding utf8
    # connecteur selon la fonction data-source utilisee ; les tests de folding
    # (nom *fold*) passent par le connecteur qui declare LimitClauseKind.
    $mez = if ($src -match "Argus\.Feed") {
               if ($name -match "fold") { $mezFold } else { $mezNav }
           } else { $mezQuery }
    # credential Anonymous -> store
    $tmpl = (& $PQTest credential-template -q $tmpQ -e $mez -ak "Anonymous") -join ''
    $tmpl | & $PQTest set-credential -q $tmpQ -e $mez -cfp $store | Out-Null
    # compare : la sortie reelle est ecrite dans WorkDir (-ofp), puis diff vs golden
    & $PQTest compare -q $tmpQ -e $mez -cfp $store -ofp $WorkDir 2>&1 | Out-Null
    $actual = Get-Content (Join-Path $WorkDir ($q.BaseName + ".pqout")) -Raw -ErrorAction SilentlyContinue
    $golden = Get-Content (Join-Path $q.DirectoryName ($q.BaseName + ".pqout")) -Raw -ErrorAction SilentlyContinue
    if ($actual -and $golden -and ($actual.Trim() -eq $golden.Trim())) {
        Write-Host "  PASS  $name" -ForegroundColor Green; $pass++
    } else {
        Write-Host "  FAIL  $name" -ForegroundColor Red
        Write-Host "    attendu : $($golden  -replace '\r?\n',' ')"
        Write-Host "    obtenu  : $($actual -replace '\r?\n',' ')"
        $fail++
    }
}
Write-Host ""
$summaryColor = if ($fail) { "Red" } else { "Green" }
Write-Host "PQTest Argus : $pass pass, $fail fail" -ForegroundColor $summaryColor
exit $fail
