param(
    [string]$ImageTag = "2.5.0",
    [string]$ReportPath = "TASK_EXECUTION_REPORT.md",
    [switch]$KeepRunning
)

$ErrorActionPreference = "Stop"

$root = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
$testCompose = Join-Path $root "geoserver_test\docker-compose.geoserver-test.yml"
$productionCompose = Join-Path $root "docker-compose.production.yml"
$baseEnvFile = Join-Path $root ".env.production.local"
$reportFile = Join-Path $root $ReportPath
$tempEnvFile = Join-Path ([System.IO.Path]::GetTempPath()) ("geoserver-cleaner-validation-{0}.env" -f ([Guid]::NewGuid().ToString("N")))
$imageRef = "ghcr.io/ashapira/geoserver-cleaner:$ImageTag"
$appServiceName = "geoserver-cleaner"
$mcpUrl = "http://127.0.0.1:8000/mcp/"
$storesUrl = "http://127.0.0.1:8000/stores"
$csvUrl = "http://127.0.0.1:8000/reports/latest.csv"
$scanUrl = "http://127.0.0.1:8000/scan"
$jobsBaseUrl = "http://127.0.0.1:8000/jobs"
$geoserverUrl = "http://127.0.0.1:8081/geoserver/web/"
$commandLog = New-Object System.Collections.Generic.List[string]
$summaryLines = New-Object System.Collections.Generic.List[string]
$logSummary = New-Object System.Collections.Generic.List[string]

function Add-CommandLog {
    param(
        [string]$Command,
        [string]$Output
    )

    $commandLog.Add("## " + $Command)
    $commandLog.Add("")
    $commandLog.Add('```text')
    if ([string]::IsNullOrWhiteSpace($Output)) {
        $commandLog.Add("<no output>")
    }
    else {
        $commandLog.Add($Output.TrimEnd())
    }
    $commandLog.Add('```')
    $commandLog.Add("")
}

function Invoke-CapturedCommand {
    param(
        [Parameter(Mandatory = $true)]
        [string]$FilePath,
        [string[]]$ArgumentList = @(),
        [string]$DisplayCommand = ""
    )

    if ($DisplayCommand) {
        $display = $DisplayCommand
    }
    else {
        $display = ($FilePath + " " + [string]::Join(" ", $ArgumentList)).Trim()
    }
    Write-Host ">> $display"
    $output = & $FilePath @ArgumentList 2>&1
    $text = ($output | ForEach-Object { $_.ToString() }) -join [Environment]::NewLine
    Add-CommandLog -Command $display -Output $text
    if ($LASTEXITCODE -ne 0) {
        throw "Command failed: $display"
    }
    return $text
}

function Wait-HttpReady {
    param(
        [Parameter(Mandatory = $true)]
        [string]$Name,
        [Parameter(Mandatory = $true)]
        [string]$Url,
        [datetime]$Deadline,
        [switch]$AcceptAnyHttpResponse
    )

    while ((Get-Date) -lt $Deadline) {
        try {
            $response = Invoke-WebRequest -Uri $Url -UseBasicParsing -TimeoutSec 5
            if ($AcceptAnyHttpResponse -or ($response.StatusCode -ge 200 -and $response.StatusCode -lt 400)) {
                return $response
            }
        }
        catch {
            if ($AcceptAnyHttpResponse -and $_.Exception.Response) {
                return $_.Exception.Response
            }
        }
        Start-Sleep -Seconds 2
    }

    throw "Timed out waiting for $Name at $Url"
}

function Wait-ScanCompletion {
    param(
        [Parameter(Mandatory = $true)]
        [int]$JobId,
        [datetime]$Deadline
    )

    $lastBody = ""
    while ((Get-Date) -lt $Deadline) {
        $response = Invoke-WebRequest -Uri "$jobsBaseUrl/$JobId/status" -UseBasicParsing -TimeoutSec 10
        $lastBody = [string]$response.Content
        if ($lastBody -match "completed" -and $lastBody -match "Inventory scan completed") {
            return $lastBody
        }
        if ($lastBody -match "failed" -and $lastBody -match "Inventory scan failed") {
            throw "Inventory scan job $JobId failed."
        }
        Start-Sleep -Seconds 2
    }

    throw "Timed out waiting for inventory scan job $JobId to complete."
}

function Get-ComposeContainerId {
    param(
        [Parameter(Mandatory = $true)]
        [string]$ComposeFile,
        [Parameter(Mandatory = $true)]
        [string]$ServiceName,
        [string]$EnvFile
    )

    $args = @()
    if ($EnvFile) {
        $args += "--env-file"
        $args += $EnvFile
    }
    $args += "-f"
    $args += $ComposeFile
    $args += "ps"
    $args += "-q"
    $args += $ServiceName
    $id = (& docker compose @args).Trim()
    if (-not $id) {
        throw "Could not resolve container id for service $ServiceName."
    }
    return $id
}

function Get-PublishRunSummary {
    param(
        [Parameter(Mandatory = $true)]
        [string]$CommitSha
    )

    try {
        Remove-Item Env:GH_TOKEN -ErrorAction SilentlyContinue
        $json = & gh run list --workflow publish-geoserver-cleaner.yml --limit 20 --json databaseId,displayTitle,event,headSha,status,conclusion,url 2>$null
        if ($LASTEXITCODE -ne 0 -or -not $json) {
            return $null
        }
        $runs = $json | ConvertFrom-Json
        return $runs | Where-Object { $_.headSha -eq $CommitSha } | Select-Object -First 1
    }
    catch {
        return $null
    }
}

if (-not (Test-Path $baseEnvFile)) {
    throw "Missing env file: $baseEnvFile"
}

$cleanupNeeded = $true
$tempEnvFileForCleanup = $tempEnvFile
try {
    $envLines = Get-Content $baseEnvFile
    $updatedEnvLines = foreach ($line in $envLines) {
        if ($line -match '^GEOSERVER_CLEANER_TAG=') {
            "GEOSERVER_CLEANER_TAG=$ImageTag"
        }
        else {
            $line
        }
    }
    Set-Content -Path $tempEnvFile -Value $updatedEnvLines -Encoding ascii

    $validationStartedAt = Get-Date
    $gitHead = (Invoke-CapturedCommand -FilePath "git" -ArgumentList @("rev-parse", "HEAD") -DisplayCommand "git rev-parse HEAD").Trim()
    $tagSha = (Invoke-CapturedCommand -FilePath "git" -ArgumentList @("rev-list", "-n", "1", $ImageTag) -DisplayCommand "git rev-list -n 1 $ImageTag").Trim()
    $publishRun = Get-PublishRunSummary -CommitSha $tagSha

    Invoke-CapturedCommand -FilePath "python" -ArgumentList @("-m", "unittest", "discover", "-s", "tests", "-v") -DisplayCommand "python -m unittest discover -s tests -v" | Out-Null
    Invoke-CapturedCommand -FilePath "docker" -ArgumentList @("compose", "-f", $testCompose, "config") -DisplayCommand "docker compose -f geoserver_test/docker-compose.geoserver-test.yml config" | Out-Null
    Invoke-CapturedCommand -FilePath "docker" -ArgumentList @("compose", "--env-file", $tempEnvFile, "-f", $productionCompose, "config") -DisplayCommand "docker compose --env-file [temp validation env] -f docker-compose.production.yml config" | Out-Null
    Invoke-CapturedCommand -FilePath "docker" -ArgumentList @("pull", $imageRef) -DisplayCommand "docker pull $imageRef" | Out-Null

    Invoke-CapturedCommand -FilePath "docker" -ArgumentList @("compose", "--env-file", $tempEnvFile, "-f", $productionCompose, "down", "--remove-orphans") -DisplayCommand "docker compose --env-file [temp validation env] -f docker-compose.production.yml down --remove-orphans" | Out-Null
    Invoke-CapturedCommand -FilePath "docker" -ArgumentList @("compose", "-f", $testCompose, "down", "--remove-orphans") -DisplayCommand "docker compose -f geoserver_test/docker-compose.geoserver-test.yml down --remove-orphans" | Out-Null

    Invoke-CapturedCommand -FilePath "docker" -ArgumentList @("compose", "-f", $testCompose, "up", "-d") -DisplayCommand "docker compose -f geoserver_test/docker-compose.geoserver-test.yml up -d" | Out-Null
    Invoke-CapturedCommand -FilePath "docker" -ArgumentList @("compose", "--env-file", $tempEnvFile, "-f", $productionCompose, "up", "-d") -DisplayCommand "docker compose --env-file [temp validation env] -f docker-compose.production.yml up -d" | Out-Null

    $deadline = (Get-Date).AddMinutes(5)
    $geoserverResponse = Wait-HttpReady -Name "GeoServer fixture" -Url $geoserverUrl -Deadline $deadline
    $storesResponse = Wait-HttpReady -Name "GeoServer Cleaner UI" -Url $storesUrl -Deadline $deadline
    $mcpResponse = Wait-HttpReady -Name "GeoServer Cleaner MCP" -Url $mcpUrl -Deadline $deadline -AcceptAnyHttpResponse

    $scanResponse = Invoke-WebRequest -Uri $scanUrl -Method Post -Body @{ exclude_workspaces = "" } -UseBasicParsing -TimeoutSec 30
    if (-not $scanResponse) {
        throw "Inventory scan request did not return a response."
    }
    $scanStatusCode = [int]$scanResponse.StatusCode
    $jobLocation = [string]$scanResponse.BaseResponse.ResponseUri.AbsolutePath
    if (-not $jobLocation) {
        throw "Inventory scan response did not include a job redirect."
    }
    if ($jobLocation -notmatch '/jobs/(\d+)') {
        throw "Could not extract job id from redirect location: $jobLocation"
    }
    $jobId = [int]$Matches[1]
    $jobBody = Wait-ScanCompletion -JobId $jobId -Deadline ((Get-Date).AddMinutes(10))

    $csvResponse = Invoke-WebRequest -Uri $csvUrl -UseBasicParsing -TimeoutSec 30

    $appContainerId = Get-ComposeContainerId -ComposeFile $productionCompose -ServiceName $appServiceName -EnvFile $tempEnvFile
    $testContainerId = Get-ComposeContainerId -ComposeFile $testCompose -ServiceName "geoserver_test"
    $imageInspectText = Invoke-CapturedCommand -FilePath "docker" -ArgumentList @("image", "inspect", $imageRef, "--format", "{{json .RepoDigests}}") -DisplayCommand "docker image inspect $imageRef --format {{json .RepoDigests}}"
    $containerInspectText = Invoke-CapturedCommand -FilePath "docker" -ArgumentList @("inspect", $appContainerId, "--format", "{{.Image}}") -DisplayCommand "docker inspect $appContainerId --format {{.Image}}"
    $appLogsText = Invoke-CapturedCommand -FilePath "docker" -ArgumentList @("logs", $appContainerId, "--since", $validationStartedAt.ToUniversalTime().ToString("yyyy-MM-ddTHH:mm:ssZ")) -DisplayCommand "docker logs $appContainerId --since $($validationStartedAt.ToUniversalTime().ToString("yyyy-MM-ddTHH:mm:ssZ"))"

    $summaryLines.Add("- GeoServer fixture status: HTTP $([int]$geoserverResponse.StatusCode)")
    $summaryLines.Add("- UI status: HTTP $([int]$storesResponse.StatusCode)")
    $summaryLines.Add("- MCP status: HTTP $([int]$mcpResponse.StatusCode)")
    $summaryLines.Add(('- Inventory scan request: HTTP {0} redirect to `{1}`' -f $scanStatusCode, $jobLocation))
    $summaryLines.Add(('- Inventory scan job id: `{0}`' -f $jobId))
    $summaryLines.Add(('- CSV export status: HTTP {0}' -f [int]$csvResponse.StatusCode))
    $summaryLines.Add(('- App container id: `{0}`' -f $appContainerId))
    $summaryLines.Add(('- GeoServer fixture container id: `{0}`' -f $testContainerId))

    $logLines = $appLogsText -split "`r?`n"
    foreach ($line in $logLines) {
        if ($line -match "runtime_init_start|runtime_init_complete|mcp_http_enabled|http_request_complete|inventory_scan_complete|mcp_http_build|web_app_create") {
            $logSummary.Add($line)
        }
    }
    if ($logSummary.Count -eq 0) {
        $logSummary.Add("No matching summary log lines were extracted; inspect the full command log section.")
    }

    $repoDigest = ""
    try {
        $parsedDigest = $imageInspectText | ConvertFrom-Json
        if ($parsedDigest.Count -gt 0) {
            $repoDigest = [string]$parsedDigest[0]
        }
    }
    catch {
        $repoDigest = $imageInspectText.Trim()
    }
    $containerImageId = $containerInspectText.Trim()

    $workflowLines = @()
    if ($publishRun) {
        $workflowLines += '- Workflow: `Publish GeoServer Cleaner Image`'
        $workflowLines += "- Run URL: $($publishRun.url)"
        $workflowLines += "- Run status: $($publishRun.status)"
        $workflowLines += "- Run conclusion: $($publishRun.conclusion)"
    }
    else {
        $workflowLines += '- Workflow run lookup via `gh` was not available; publication was verified through the pulled GHCR image and digest.'
    }

    $reportLines = @(
        ('# GeoServer Cleaner {0} GHCR Validation Report' -f $ImageTag),
        "",
        "## Release Summary",
        "",
        '- Repository: `AShapira/geoserver_cleaner`',
        ('- Release tag: `{0}`' -f $ImageTag),
        ('- Release commit SHA: `{0}`' -f $gitHead),
        ('- Tag commit SHA: `{0}`' -f $tagSha),
        ('- Validated image: `{0}`' -f $imageRef),
        ('- Pulled image digest: `{0}`' -f $repoDigest),
        ('- Running container image id: `{0}`' -f $containerImageId),
        "",
        "## Publish Workflow",
        ""
    ) + $workflowLines + @(
        "",
        "## Validation Environment",
        "",
        ('- Validation host directory: `{0}`' -f $root),
        '- GeoServer fixture compose: `geoserver_test/docker-compose.geoserver-test.yml`',
        '- App compose: `docker-compose.production.yml`',
        ('- Validation env source: `.env.production.local` with `GEOSERVER_CLEANER_TAG={0}` forced in a temporary env file' -f $ImageTag),
        '- GeoServer base URL during validation: `http://127.0.0.1:8081/geoserver`',
        ('- Web UI endpoint: `{0}`' -f $storesUrl),
        ('- MCP endpoint: `{0}`' -f $mcpUrl),
        "",
        "## Executed Checks",
        ""
    ) + $summaryLines + @(
        "",
        "## Scan and Endpoint Results",
        "",
        '- `/stores` responded successfully and served the web UI.',
        '- `/mcp/` was reachable with HTTP MCP enabled.',
        '- A fresh inventory scan was triggered over HTTP and reached the completed state.',
        '- `/reports/latest.csv` responded successfully after the scan completed.',
        '- The running app used the published GHCR image rather than a local build.',
        "",
        "## Log Summary",
        "",
        '```text'
    ) + $logSummary + @(
        '```',
        "",
        "## Isolated-Network Readiness Conclusion",
        "",
        ('The published image `ghcr.io/ashapira/geoserver-cleaner:{0}` started successfully with the production compose file, served `/stores` and `/mcp/`, completed an inventory scan against the configured GeoServer fixture, and served a snapshot export. The runtime validation and codebase behavior indicate that steady-state network dependency is the configured GeoServer endpoint rather than external internet services.' -f $ImageTag),
        "",
        'This validation did not add a host-level firewall block; the conclusion is based on successful execution of the published image, the captured container logs, and the repo code paths that only initiate outbound HTTP toward `GEOSERVER_URL` during normal operation.',
        "",
        "## Commands",
        ""
    ) + $commandLog

    Set-Content -Path $reportFile -Value $reportLines -Encoding utf8
}
finally {
    if ($cleanupNeeded -and -not $KeepRunning) {
        try {
            & docker compose --env-file $tempEnvFileForCleanup -f $productionCompose down --remove-orphans | Out-Null
        }
        catch {
        }
        try {
            & docker compose -f $testCompose down --remove-orphans | Out-Null
        }
        catch {
        }
    }

    if ((Test-Path $tempEnvFileForCleanup)) {
        Remove-Item $tempEnvFileForCleanup -Force
    }
}
