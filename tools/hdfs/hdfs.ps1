# DTSMS — start / stop / status of the real HDFS running in WSL2 Ubuntu
# (installed once by tools\hdfs\setup_wsl_hdfs.sh).
#
#   .\tools\hdfs\hdfs.ps1 start     # NameNode + DataNode; WebHDFS on http://127.0.0.1:9870, RPC hdfs://127.0.0.1:9000
#   .\tools\hdfs\hdfs.ps1 stop
#   .\tools\hdfs\hdfs.ps1 status
param(
    [ValidateSet('start', 'stop', 'status')][string]$Action = 'status',
    [string]$Distro = 'Ubuntu'
)

function Test-WebHdfs {
    try { (Invoke-WebRequest 'http://127.0.0.1:9870/webhdfs/v1/?op=GETFILESTATUS' -UseBasicParsing -TimeoutSec 2).StatusCode -eq 200 }
    catch { $false }
}

wsl.exe -d $Distro -- test -x /usr/local/bin/dtsms-hdfs 2>$null
if ($LASTEXITCODE -ne 0) {
    Write-Host "HDFS is not set up in WSL distro '$Distro'. Once, from the project folder:" -ForegroundColor Yellow
    Write-Host "  wsl -d $Distro -u root -- bash tools/hdfs/setup_wsl_hdfs.sh"
    exit 1
}

switch ($Action) {
    'start' {
        if (Test-WebHdfs) { Write-Host 'HDFS     : already running (http://127.0.0.1:9870)'; exit 0 }
        # start daemons inside WSL (dtsms-hdfs uses nohup)
        wsl.exe -d $Distro -- dtsms-hdfs start
        # a hidden wsl.exe keeps the WSL distro running while HDFS is up
        Start-Process wsl.exe -ArgumentList "-d $Distro -- sleep infinity" -WindowStyle Hidden
        for ($i = 0; $i -lt 90; $i++) {
            if (Test-WebHdfs) { Write-Host 'HDFS     : running (NameNode http://127.0.0.1:9870, hdfs://127.0.0.1:9000)'; exit 0 }
            Start-Sleep -Seconds 1
        }
        Write-Host 'HDFS did not answer within 90 s; logs are in /var/lib/dtsms-hdfs/logs inside WSL' -ForegroundColor Red
        exit 1
    }
    'stop' {
        wsl.exe -d $Distro -- dtsms-hdfs stop
        Get-CimInstance Win32_Process -Filter "Name='wsl.exe'" |
            Where-Object { $_.CommandLine -like '*sleep infinity*' -or $_.CommandLine -like '*dtsms-hdfs start*' } |
            ForEach-Object { Stop-Process -Id $_.ProcessId -Force }
    }
    'status' {
        wsl.exe -d $Distro -- dtsms-hdfs status
        if (Test-WebHdfs) { 'WebHDFS: answering on http://127.0.0.1:9870' } else { 'WebHDFS: not reachable' }
    }
}
