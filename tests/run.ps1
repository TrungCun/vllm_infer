param(
    [switch]$Live,
    [int]$Requests = 30,
    [int]$Concurrency = 8,
    [int]$IdleSeconds = 10,
    [int]$Cycles = 2
)
$ErrorActionPreference = "Stop"
if ($Requests -lt 1 -or $Concurrency -lt 1 -or $Cycles -lt 1 -or $IdleSeconds -lt 3) {
    throw "Requests/Concurrency/Cycles >= 1; IdleSeconds >= 3."
}
$repoRoot = Split-Path -Parent $PSScriptRoot
$composeArgs = @("compose", "--project-directory", $repoRoot, "-f", (Join-Path $repoRoot "docker-compose.yml"))
$reportRoot = Join-Path $PSScriptRoot ("results\" + (Get-Date -Format "yyyyMMdd-HHmmss"))
New-Item -ItemType Directory -Force -Path $reportRoot | Out-Null
$image = "vllm-infer-tests:local"
function Invoke-Docker {
    param([string[]]$DockerArgs)
    & docker @DockerArgs
    if ($LASTEXITCODE -ne 0) { throw "Docker failed (exit $LASTEXITCODE): $($DockerArgs -join ' ')" }
}
Write-Host "Build test image (khong thay doi container dang chay)..."
Invoke-Docker -DockerArgs @("build", "--build-context", "gateway_source=$(Join-Path $repoRoot 'gateway')",
    "-f", (Join-Path $PSScriptRoot "Dockerfile"), "-t", $image, $PSScriptRoot)
Invoke-Docker -DockerArgs @("run", "--rm", "--mount", "type=bind,source=$reportRoot,target=/results",
    $image, "python", "-m", "unittest", "-v", "test_client")
Invoke-Docker -DockerArgs @("run", "--rm", "--mount", "type=bind,source=$reportRoot,target=/results", $image)

if ($Live) {
    Write-Host "Test vLLM that: tam dung cac agent/client khac truoc khi chay."
    $gatewayId = (Invoke-Docker -DockerArgs ($composeArgs + @("ps", "-q", "gateway")) | Out-String).Trim()
    $backendId = (Invoke-Docker -DockerArgs ($composeArgs + @("ps", "-q", "vllm")) | Out-String).Trim()
    if (-not $gatewayId -or -not $backendId) { throw "Gateway va vLLM phai dang chay." }
    $gatewayInfo = (Invoke-Docker -DockerArgs @("inspect", $gatewayId) | Out-String | ConvertFrom-Json)[0]
    $backendInfo = (Invoke-Docker -DockerArgs @("inspect", $backendId) | Out-String | ConvertFrom-Json)[0]
    $network = @($gatewayInfo.NetworkSettings.Networks.PSObject.Properties.Name |
        Where-Object { $_ -in $backendInfo.NetworkSettings.Networks.PSObject.Properties.Name }) |
        Select-Object -First 1
    if (-not $network) { throw "Khong tim thay Docker network chung." }
    $savedIdle = @($gatewayInfo.Config.Env | Where-Object { $_ -like "IDLE_TIMEOUT=*" })[0]
    $savedControl = @($gatewayInfo.Config.Env | Where-Object { $_ -like "CONTROL_TIMEOUT=*" })[0]
    if (-not $savedIdle -or -not $savedControl) { throw "Khong doc duoc runtime timeout cua gateway." }
    $savedIdle = $savedIdle.Substring("IDLE_TIMEOUT=".Length)
    $savedControl = $savedControl.Substring("CONTROL_TIMEOUT=".Length)
    $oldShellIdle = [Environment]::GetEnvironmentVariable("IDLE_TIMEOUT", "Process")
    $oldShellControl = [Environment]::GetEnvironmentVariable("CONTROL_TIMEOUT", "Process")
    $changed = $false
    try {
        # Test vLLM that phai dung gateway cung source voi suite mock.
        Invoke-Docker -DockerArgs ($composeArgs + @("build", "gateway"))
        $env:IDLE_TIMEOUT = "0"
        $env:CONTROL_TIMEOUT = $savedControl
        $changed = $true
        Invoke-Docker -DockerArgs ($composeArgs + @("up", "-d", "--no-deps", "--force-recreate", "gateway"))
        $runArgs = @("run", "--rm", "--network", $network,
            "--mount", "type=bind,source=$reportRoot,target=/results")
        if ($env:OPENAI_API_KEY) { $runArgs += @("-e", "OPENAI_API_KEY") }
        $runArgs += @($image, "python", "test_live.py", "--idle-seconds", "$IdleSeconds",
            "--requests", "$Requests", "--concurrency", "$Concurrency", "--cycles", "$Cycles")
        Invoke-Docker -DockerArgs ($runArgs + @("--suite", "benchmark"))
        try {
            & docker @composeArgs logs --no-color gateway 2>&1 |
                Out-File -FilePath (Join-Path $reportRoot "benchmark-gateway.log") -Encoding utf8
        } catch { Write-Warning "Khong luu duoc benchmark log: $_" }
        $env:IDLE_TIMEOUT = "$($IdleSeconds)s"
        Invoke-Docker -DockerArgs ($composeArgs + @("up", "-d", "--no-deps", "--force-recreate", "gateway"))
        Invoke-Docker -DockerArgs ($runArgs + @("--suite", "lifecycle"))
    }
    finally {
        try {
            try {
                & docker @composeArgs logs --no-color gateway 2>&1 |
                    Out-File -FilePath (Join-Path $reportRoot "live-gateway.log") -Encoding utf8
            } catch { Write-Warning "Khong luu duoc live log: $_" }
            if ($changed) {
                $env:IDLE_TIMEOUT = $savedIdle
                $env:CONTROL_TIMEOUT = $savedControl
                Invoke-Docker -DockerArgs ($composeArgs + @("up", "-d", "--no-deps", "--force-recreate", "gateway"))
                Write-Host "Da khoi phuc IDLE_TIMEOUT=$savedIdle, CONTROL_TIMEOUT=$savedControl."
            }
        }
        finally {
            [Environment]::SetEnvironmentVariable("IDLE_TIMEOUT", $oldShellIdle, "Process")
            [Environment]::SetEnvironmentVariable("CONTROL_TIMEOUT", $oldShellControl, "Process")
            Write-Host "Bao cao: $reportRoot"
        }
    }
}
Write-Host "Bao cao: $reportRoot"
