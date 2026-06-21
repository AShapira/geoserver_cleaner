param(
    [switch]$Down,
    [int]$TimeoutSeconds = 180
)

$ErrorActionPreference = "Stop"

$root = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
$testCompose = Join-Path $root "geoserver_test\docker-compose.geoserver-test.yml"
$productionCompose = Join-Path $root "docker-compose.production.yml"
$envFile = Join-Path $root ".env.production.local"

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

if (-not (Test-Path $envFile)) {
    throw "Missing env file: $envFile"
}

if ($Down) {
    docker compose --env-file $envFile -f $productionCompose down
    docker compose -f $testCompose down
    Write-Host "Stopped local production flow."
    exit 0
}

docker compose -f $testCompose up -d
docker compose --env-file $envFile -f $productionCompose up -d

$deadline = (Get-Date).AddSeconds($TimeoutSeconds)

Wait-HttpReady -Name "GeoServer test fixture" -Url "http://127.0.0.1:8081/geoserver/web/" -Deadline $deadline
Wait-HttpReady -Name "GeoServer Cleaner UI" -Url "http://127.0.0.1:8000/stores" -Deadline $deadline

Write-Host ""
Write-Host "Local production flow is running."
Write-Host "Web UI: http://127.0.0.1:8000/stores"
