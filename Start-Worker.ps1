param([string]$StateDir = $env:GPU_QUEUE_STATE_DIR)
$ErrorActionPreference = 'Stop'
$queueScript = Join-Path $PSScriptRoot 'gpu_queue.py'
$pythonExe = (Get-Command python.exe).Source
if (-not $StateDir) { $StateDir = Join-Path $env:LOCALAPPDATA 'gpu-queue' }
$queueState = [System.IO.Path]::GetFullPath($StateDir)
New-Item -ItemType Directory -Force $queueState | Out-Null
$queueStatus = & $pythonExe $queueScript --state-dir $queueState status --json | ConvertFrom-Json
if ($queueStatus.worker_running) {
    Write-Output 'GPU queue worker is already running.'
    exit 0
}
Start-Process -FilePath $pythonExe -ArgumentList @('-u', ('"' + $queueScript + '"'), '--state-dir', ('"' + $queueState + '"'), 'worker') -WindowStyle Hidden -RedirectStandardOutput (Join-Path $queueState 'worker.log') -RedirectStandardError (Join-Path $queueState 'worker-error.log')
Start-Sleep -Milliseconds 700
& $pythonExe $queueScript --state-dir $queueState status
