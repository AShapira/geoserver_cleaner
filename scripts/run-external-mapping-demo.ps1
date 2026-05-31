param(
    [switch]$Down,
    [int]$TimeoutSeconds = 300
)

$ErrorActionPreference = "Stop"

$root = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
$compose = Join-Path $root "docker-compose.external-mapping-demo.yml"
$fixtureScript = Join-Path $root "geoserver_test\populate_external_mapping_demo.py"
$geoserverUrl = "http://127.0.0.1:8081/geoserver"
$appUrl = "http://127.0.0.1:8000"
$appDataDir = Join-Path $root "app_data\external_mapping_demo"
$logPath = Join-Path $appDataDir "logs\geoserver_cleaner.log"

function Invoke-Compose {
    param([string[]]$Arguments)
    & docker compose -f $compose @Arguments
    if ($LASTEXITCODE -ne 0) {
        throw "docker compose failed: $($Arguments -join ' ')"
    }
}

function Wait-HttpReady {
    param(
        [Parameter(Mandatory = $true)]
        [string]$Name,
        [Parameter(Mandatory = $true)]
        [string]$Url,
        [Parameter(Mandatory = $true)]
        [datetime]$Deadline,
        [switch]$AcceptAnyHttpResponse
    )

    while ((Get-Date) -lt $Deadline) {
        try {
            $response = Invoke-WebRequest -Uri $Url -UseBasicParsing -TimeoutSec 5
            if ($AcceptAnyHttpResponse -or ($response.StatusCode -ge 200 -and $response.StatusCode -lt 400)) {
                Write-Host "$Name ready: $Url"
                return
            }
        }
        catch {
            if ($AcceptAnyHttpResponse -and $_.Exception.Response) {
                Write-Host "$Name reachable: $Url"
                return
            }
        }
        Start-Sleep -Seconds 2
    }

    throw "Timed out waiting for $Name at $Url"
}

function Get-ResponsePath {
    param([Parameter(Mandatory = $true)]$Response)

    if ($Response.BaseResponse -and $Response.BaseResponse.ResponseUri) {
        return $Response.BaseResponse.ResponseUri.AbsolutePath
    }
    if ($Response.BaseResponse -and $Response.BaseResponse.RequestMessage -and $Response.BaseResponse.RequestMessage.RequestUri) {
        return $Response.BaseResponse.RequestMessage.RequestUri.AbsolutePath
    }
    return ""
}

function Wait-JobComplete {
    param(
        [Parameter(Mandatory = $true)]
        [int]$JobId,
        [Parameter(Mandatory = $true)]
        [datetime]$Deadline
    )

    while ((Get-Date) -lt $Deadline) {
        $response = Invoke-WebRequest -Uri "$appUrl/jobs/$JobId/status" -UseBasicParsing -TimeoutSec 10
        $content = [string]$response.Content
        $statusMatch = [regex]::Match($content, "<dt>Status</dt><dd>([^<]+)</dd>")
        $status = if ($statusMatch.Success) { $statusMatch.Groups[1].Value.Trim() } else { "" }
        if ($status -eq "completed") {
            return $content
        }
        if ($status -eq "failed") {
            throw "Job $JobId failed. Status fragment:`n$content"
        }
        Start-Sleep -Seconds 2
    }

    throw "Timed out waiting for job $JobId to complete"
}

function Get-StoreId {
    param(
        [Parameter(Mandatory = $true)]
        [string]$Html,
        [Parameter(Mandatory = $true)]
        [string]$StoreName
    )

    $rows = [regex]::Matches($Html, "<tr[^>]*>.*?</tr>", "Singleline")
    foreach ($rowMatch in $rows) {
        $row = $rowMatch.Value
        if ($row -like "*<td>$StoreName</td>*") {
            $idMatch = [regex]::Match($row, 'value="(\d+)"')
            if ($idMatch.Success) {
                return [int]$idMatch.Groups[1].Value
            }
        }
    }
    throw "Could not find store id for $StoreName"
}

if ($Down) {
    Invoke-Compose -Arguments @("down", "--remove-orphans")
    Write-Host "Stopped external mapping demo."
    exit 0
}

$deadline = (Get-Date).AddSeconds($TimeoutSeconds)

if ((Test-Path $appDataDir) -and ((Resolve-Path $appDataDir).Path -like "$root*")) {
    Remove-Item -LiteralPath $appDataDir -Recurse -Force
}
New-Item -ItemType Directory -Force -Path $appDataDir | Out-Null
Invoke-Compose -Arguments @("up", "-d", "geoserver_external_mapping_demo")
Wait-HttpReady -Name "GeoServer external mapping demo" -Url "$geoserverUrl/web/" -Deadline $deadline

& python $fixtureScript --base-dir $root --reload-geoserver --geoserver-url $geoserverUrl
if ($LASTEXITCODE -ne 0) {
    throw "Fixture generation failed."
}

Invoke-Compose -Arguments @("up", "-d", "--build", "geoserver-cleaner-external-mapping-demo")
Wait-HttpReady -Name "GeoServer Cleaner UI" -Url "$appUrl/stores" -Deadline $deadline

$scanResponse = Invoke-WebRequest -Uri "$appUrl/scan" -Method Post -Body @{ exclude_workspaces = "" } -UseBasicParsing -TimeoutSec 30
$scanPath = Get-ResponsePath -Response $scanResponse
if ($scanPath -notmatch "/jobs/(\d+)") {
    throw "Could not find scan job id from response path: $scanPath"
}
$scanJobId = [int]$Matches[1]
$scanStatus = Wait-JobComplete -JobId $scanJobId -Deadline ((Get-Date).AddSeconds($TimeoutSeconds))
if ($scanStatus -notmatch "<dt>Snapshot run</dt><dd>(\d+)</dd>") {
    throw "Could not find snapshot run id in scan job status."
}
$runId = [int]$Matches[1]

$storesHtml = (Invoke-WebRequest -Uri "$appUrl/stores?workspace=external_mapping_demo&page_size=100" -UseBasicParsing -TimeoutSec 30).Content
$internalId = Get-StoreId -Html $storesHtml -StoreName "internal_raster"
$windowsId = Get-StoreId -Html $storesHtml -StoreName "windows_external_raster"
$posixId = Get-StoreId -Html $storesHtml -StoreName "posix_external_raster"
$selectedIds = @($internalId, $windowsId, $posixId) -join ","

$preview = Invoke-WebRequest -Uri "$appUrl/delete/preview" -Method Post -Body @{ selected_ids = $selectedIds } -UseBasicParsing -TimeoutSec 30
$previewContent = [string]$preview.Content
if ($previewContent -notmatch "GeoServer will delete store configuration and internal data\.") {
    throw "Delete preview did not mark the internal store as data-deletable."
}
if ($previewContent -notmatch "GeoServer will delete store configuration only; data is outside data_dir\.") {
    throw "Delete preview did not mark mapped external stores as configuration-only."
}
if ($previewContent -notmatch 'name="run_id" value="(\d+)"') {
    throw "Delete preview did not include a run id."
}
$previewRunId = [int]$Matches[1]
if ($previewRunId -ne $runId) {
    throw "Preview run id $previewRunId did not match scan run id $runId."
}

$deleteResponse = Invoke-WebRequest -Uri "$appUrl/delete/execute" -Method Post -Body @{ selected_ids = $selectedIds; run_id = $runId } -UseBasicParsing -TimeoutSec 30
$deletePath = Get-ResponsePath -Response $deleteResponse
if ($deletePath -notmatch "/jobs/(\d+)") {
    throw "Could not find delete job id from response path: $deletePath"
}
$deleteJobId = [int]$Matches[1]
Wait-JobComplete -JobId $deleteJobId -Deadline ((Get-Date).AddSeconds($TimeoutSeconds)) | Out-Null

$windowsExternalFile = Join-Path $root "geoserver_test\external_data\windows\mapped_windows.tif"
$posixExternalFile = Join-Path $root "geoserver_test\external_data\posix\mapped_posix.tif"
if (-not (Test-Path $windowsExternalFile)) {
    throw "Mapped Windows external file was removed unexpectedly: $windowsExternalFile"
}
if (-not (Test-Path $posixExternalFile)) {
    throw "Mapped POSIX external file was removed unexpectedly: $posixExternalFile"
}
if (-not (Test-Path $logPath)) {
    throw "Expected cleaner log was not written: $logPath"
}
$logText = Get-Content -Path $logPath -Raw
if ($logText -notmatch "purge=all") {
    throw "Cleaner log did not show an internal coverage delete with purge=all."
}
if ($logText -notmatch "purge=none") {
    throw "Cleaner log did not show an external coverage delete with purge=none."
}

Write-Host ""
Write-Host "External mapping demo validation passed."
Write-Host "Web UI: $appUrl/stores"
Write-Host "Scan job: $appUrl/jobs/$scanJobId"
Write-Host "Delete job: $appUrl/jobs/$deleteJobId"
