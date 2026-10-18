$ErrorActionPreference = 'Stop'
$projectRoot = Split-Path -Parent $PSScriptRoot
$demoJar = Join-Path $projectRoot 'cs324-A2\target\cs324-A2-1.0-SNAPSHOT.jar'
if (-not (Test-Path -LiteralPath $demoJar)) { throw 'Build first: .\mvnw.cmd -f cs324-A2\pom.xml clean verify' }
$demoLogDir = Join-Path $projectRoot ('.demo\integrated-' + [guid]::NewGuid().ToString('N'))
New-Item -ItemType Directory -Path $demoLogDir -Force | Out-Null
$javaPath = (Get-Command java).Source
$previousPreference = $ErrorActionPreference
$ErrorActionPreference = 'Continue'
try { $runtimeSettings = & $javaPath -XshowSettings:properties -version 2>&1 }
finally { $ErrorActionPreference = $previousPreference }
$homeLine = $runtimeSettings | ForEach-Object { $_.ToString() } | Where-Object { $_ -match '^\s*java.home\s*=' } | Select-Object -First 1
if (-not $homeLine) { throw 'Cannot identify the real Java executable' }
$javaPath = Join-Path (($homeLine -split '=', 2)[1].Trim()) 'bin\java.exe'
$listener = [System.Net.Sockets.TcpListener]::new([System.Net.IPAddress]::Loopback, 0)
$listener.Start(); $bootstrapPort = $listener.LocalEndpoint.Port; $listener.Stop()
$processes = [System.Collections.Generic.List[object]]::new()
$workerProcesses = @{}
function Invoke-Cluster([string[]]$clientArgs) {
    $previousPreference = $ErrorActionPreference
    $ErrorActionPreference = 'Continue'
    try {
        $output = & $javaPath -cp $demoJar cs324.assignment2.worker.ClusterDemo @clientArgs 2>> (Join-Path $demoLogDir 'client.err.log')
        $script:clientExitCode = $LASTEXITCODE
        return $output
    } finally { $ErrorActionPreference = $previousPreference }
}
function Start-ClusterProcess([string]$name, [string]$className, [string[]]$programArgs) {
    $arguments = @('-cp', ('"' + $demoJar + '"'), $className) + $programArgs
    $process = Start-Process -FilePath $javaPath -ArgumentList $arguments -WindowStyle Hidden -PassThru `
        -RedirectStandardOutput (Join-Path $demoLogDir "$name.out.log") `
        -RedirectStandardError (Join-Path $demoLogDir "$name.err.log")
    $processes.Add($process)
    return $process
}
function Start-Worker([string]$name) {
    $process = Start-ClusterProcess -name $name -className 'cs324.assignment2.worker.WorkerNode' -programArgs @('localhost', "$bootstrapPort", '0', '0')
    $deadline = [DateTime]::UtcNow.AddSeconds(20)
    do {
        $workerLog = Get-Content -LiteralPath (Join-Path $demoLogDir "$name.out.log") -Raw -ErrorAction SilentlyContinue
        if ($workerLog -match '\[Worker (\d+)\] Registered;') {
            $workerProcesses[[int]$Matches[1]] = $process
            return
        }
        if ($process.HasExited) { throw "$name failed to start; see $demoLogDir" }
        Start-Sleep -Milliseconds 150
    } while ([DateTime]::UtcNow -lt $deadline)
    throw "$name did not register; see $demoLogDir"
}
function Wait-Ready([int]$leader, [int]$activeCount) {
    $deadline = [DateTime]::UtcNow.AddSeconds(40)
    do {
        $statusLines = Invoke-Cluster -clientArgs @('status', 'localhost', "$bootstrapPort")
        $statusText = $statusLines -join "`n"
        if ($script:clientExitCode -eq 0 -and $statusText -match ('(?m)^COORDINATOR=' + $leader + '$') `
            -and $statusText -match '(?m)^READY=true$' -and $statusText -match ('(?m)^ACTIVE_WORKERS=' + $activeCount + '$')) { return }
        Start-Sleep -Milliseconds 200
    } while ([DateTime]::UtcNow -lt $deadline)
    throw "Coordinator $leader did not become ready; see $demoLogDir"
}
try {
    $bootstrapProcess = Start-ClusterProcess -name 'bootstrap' -className 'cs324.assignment2.bootstrap.BootstrapNode' -programArgs @("$bootstrapPort")
    $deadline = [DateTime]::UtcNow.AddSeconds(20)
    do {
        $bootstrapLog = Get-Content -LiteralPath (Join-Path $demoLogDir 'bootstrap.out.log') -Raw -ErrorAction SilentlyContinue
        if ($bootstrapLog -match 'gRPC server started') { break }
        if ($bootstrapProcess.HasExited) { throw "Bootstrap startup failed; see $demoLogDir" }
        if ([DateTime]::UtcNow -ge $deadline) { throw 'Bootstrap startup timeout' }
        Start-Sleep -Milliseconds 150
    } while ($true)
    Start-Worker 'worker-a'; Start-Worker 'worker-b'; Start-Worker 'worker-c'
    Wait-Ready 3 3
    $saved = Invoke-Cluster -clientArgs @('checkpoint', 'localhost', "$bootstrapPort", 'demo-job', 'demo-only;task-A=done;task-B=pending')
    if ($script:clientExitCode -ne 0 -or ($saved -join '') -notmatch 'CHECKPOINT_ACCEPTED=true') { throw 'Checkpoint replication failed' }
    Stop-Process -Id $workerProcesses[3].Id -Force
    Wait-Ready 2 2
    $recovered = Invoke-Cluster -clientArgs @('read', 'localhost', "$bootstrapPort")
    if ($script:clientExitCode -ne 0 -or ($recovered -join '') -notmatch 'demo-job = demo-only;task-A=done;task-B=pending') { throw 'Recovery after coordinator crash failed' }
    Write-Output 'PASS: real bootstrap registration, coordinator crash detection, replacement election and checkpoint recovery.'
    Start-Worker 'worker-d'
    Wait-Ready 4 3
    $joinedState = Invoke-Cluster -clientArgs @('read', 'localhost', "$bootstrapPort")
    if ($script:clientExitCode -ne 0 -or ($joinedState -join '') -notmatch 'demo-job = demo-only;task-A=done;task-B=pending') { throw 'State did not survive the joining-worker transition' }
    Write-Output 'PASS: new worker joined the running cluster and recovered existing state through the new configuration.'
    Write-Output "Logs: $demoLogDir"
} finally {
    foreach ($process in $processes) {
        if (-not $process.HasExited) { Stop-Process -Id $process.Id -Force -ErrorAction SilentlyContinue }
    }
}
