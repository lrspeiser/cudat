$ErrorActionPreference = 'Stop'
[Console]::OutputEncoding = [System.Text.UTF8Encoding]::new($false)
$issues = @()
$samples = @()
foreach ($counter in @('\GPU Engine(*)\Utilization Percentage', '\GPU Process Memory(*)\Dedicated Usage', '\GPU Process Memory(*)\Shared Usage')) {
    try {
        $result = Get-Counter $counter -ErrorAction Stop
        $samples += @($result.CounterSamples | Where-Object { $_.Status -eq 0 -and $_.CookedValue -gt 0 } | ForEach-Object {
            @{ instance=$_.InstanceName; value=$_.CookedValue; kind=($counter.Split('\')[-1]) }
        })
    } catch { $issues += "Counter unavailable: $counter" }
}
$processes = @()
try {
    $processes = @(Get-CimInstance Win32_Process | ForEach-Object {
        @{ pid=[int]$_.ProcessId; parent=[int]$_.ParentProcessId; name=$_.Name;
           created= $(if ($_.CreationDate) { ([DateTimeOffset]$_.CreationDate).ToUnixTimeMilliseconds()/1000.0 } else { 0 }) }
    })
} catch { $issues += 'Windows process inventory unavailable.' }
@{ samples=$samples; processes=$processes; issues=$issues } | ConvertTo-Json -Depth 6 -Compress
