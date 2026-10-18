$ErrorActionPreference = 'Stop'
$projectRoot = Split-Path -Parent $PSScriptRoot
$demoJar = Join-Path $projectRoot 'election\target\election-1.0-SNAPSHOT.jar'
if (-not (Test-Path -LiteralPath $demoJar)) { throw 'Build first: .\mvnw.cmd clean verify' }
$demoLogDir = Join-Path $projectRoot ('.demo\' + [guid]::NewGuid().ToString('N'))
New-Item -ItemType Directory -Path $demoLogDir -Force | Out-Null
$javaPath = (Get-Command java).Source
# Oracle's javapath java.exe may be a launcher that spawns a child JVM. Resolve
# java.home so Stop-Process later targets the real JVM, not only that launcher.
$previousPreference = $ErrorActionPreference
$ErrorActionPreference = 'Continue'
try { $runtimeSettings = & $javaPath -XshowSettings:properties -version 2>&1 }
finally { $ErrorActionPreference = $previousPreference }
$homeLine = $runtimeSettings | ForEach-Object { $_.ToString() } | Where-Object { $_ -match '^\s*java.home\s*=' } | Select-Object -First 1
if (-not $homeLine) { throw 'Could not identify the Java runtime directory' }
$javaPath = Join-Path (($homeLine -split '=', 2)[1].Trim()) 'bin\java.exe'
# Java 24 emits Netty deprecation warnings on stderr. Windows PowerShell 5 treats
# native stderr as ErrorRecord objects; redirect it to per-command log files.
function Invoke-DemoClient([string[]]$clientArgs) {
    $previousPreference = $ErrorActionPreference
    $ErrorActionPreference = 'Continue'
    try {
        $output = & $javaPath -jar $demoJar @clientArgs 2>> (Join-Path $demoLogDir 'client.err.log')
        $script:clientExitCode = $LASTEXITCODE
        return $output
    } finally { $ErrorActionPreference = $previousPreference }
}
$processes = @()
$ports = @()
for ($i = 0; $i -lt 3; $i++) {
    $listener = [System.Net.Sockets.TcpListener]::new([System.Net.IPAddress]::Loopback, 0)
    $listener.Start()
    $ports += $listener.LocalEndpoint.Port
    $listener.Stop()
}
$membership = (1..3 | ForEach-Object { '{0}:localhost:{1}' -f $_, $ports[$_ - 1] }) -join ','
function Wait-ForLeader([int]$port, [int]$expectedId) {
    $deadline = [DateTime]::UtcNow.AddSeconds(30)
    do {
        $nodeStatus = Invoke-DemoClient -clientArgs @('status', 'localhost', "$port")
        if ($script:clientExitCode -eq 0) {
            $textStatus = $nodeStatus -join "`n"
            if ($textStatus -match ('(?s)coordinator\s*\{\s*id:\s*' + $expectedId + '\b') -and $textStatus -match 'ready: true') { return }
        }
        Start-Sleep -Milliseconds 300
    } while ([DateTime]::UtcNow -lt $deadline)
    throw "Coordinator $expectedId did not become ready on port $port. Logs: $demoLogDir"
}
try {
    foreach ($id in 1..3) {
        $neighbours = ((1..3 | Where-Object { $_ -ne $id }) -join ',')
        $arguments = @('-jar', ('"' + $demoJar + '"'), $id, $membership, $neighbours)
        $processes += Start-Process -FilePath $javaPath -ArgumentList $arguments -WindowStyle Hidden -PassThru `
            -RedirectStandardOutput (Join-Path $demoLogDir "worker-$id.out.log") `
            -RedirectStandardError (Join-Path $demoLogDir "worker-$id.err.log")
    }
    Wait-ForLeader $ports[2] 3
    $saved = Invoke-DemoClient -clientArgs @('save-demo', 'localhost', "$($ports[2])", 'job-demo')
    if ($script:clientExitCode -ne 0 -or ($saved -join '') -notmatch 'replicated to a majority') { throw 'Checkpoint replication failed' }
    Stop-Process -Id $processes[2].Id -Force
    Wait-ForLeader $ports[1] 2
    $recovered = Invoke-DemoClient -clientArgs @('read', 'localhost', "$($ports[1])")
    if ($script:clientExitCode -ne 0 -or ($recovered -join '') -notmatch 'job-demo = STUB_ONLY;operation=PRIMESUM;input=1\.\.100;task-1=done;task-2=pending') { throw 'Checkpoint recovery failed' }
    Write-Output 'PASS: three independent JVMs elected worker 3; abrupt coordinator kill elected worker 2; checkpoint survived.'
    Write-Output "Worker logs: $demoLogDir"
} finally {
    foreach ($workerProcess in $processes) {
        if (-not $workerProcess.HasExited) { Stop-Process -Id $workerProcess.Id -Force -ErrorAction SilentlyContinue }
    }
}
