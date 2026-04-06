param(
    [string]$ImageTag = "geoserver-cleaner:security",
    [string]$Dockerfile = "docker/Dockerfile.app",
    [string]$Severity = "critical,high",
    [switch]$SkipBuild
)

$ErrorActionPreference = "Stop"

if (-not (Get-Command docker -ErrorAction SilentlyContinue)) {
    throw "Docker CLI is required."
}

$null = docker version
$null = docker scout version

if (-not $SkipBuild) {
    docker build -f $Dockerfile -t $ImageTag .
}

docker scout quickview "local://$ImageTag"
docker scout cves "local://$ImageTag" --only-severity $Severity --only-fixed --exit-code
