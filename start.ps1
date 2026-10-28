# Starts DTSMS on Windows: checks Python, installs missing packages, runs the backend
# and opens the app in the browser.
#
#   .\start.ps1                 # start on http://127.0.0.1:5000/
#   .\start.ps1 -Port 8080      # another port
#   .\start.ps1 -NoBrowser      # don't open the browser
#   .\start.ps1 -Install        # force a reinstall of the Python packages
#
# HDFS (Apache Hadoop in WSL2 Ubuntu) is required: DTSMS keeps every dataset in it.
# First time on a new PC: run install.ps1 (or double-click install.cmd).
# If PowerShell blocks the script: double-click start.cmd, or  powershell -ExecutionPolicy Bypass -File .\start.ps1
param(
    [int]$Port = 5000,
    [switch]$NoBrowser,
    [switch]$Install
)
$ErrorActionPreference = 'Stop'
Set-Location $PSScriptRoot

# runs Python code; False when it fails. Its stderr must not stop the script (Windows PowerShell 5.1 turns a
# native command's redirected stderr into errors, which 'Stop' makes fatal)
function Test-Python([string]$exe, [string]$code) {
    $ErrorActionPreference = 'Continue'
    & $exe -c $code 2>$null | Out-Null
    return $LASTEXITCODE -eq 0
}

# 1. Python ("python" can be the Microsoft Store alias, which is not Python: try each candidate)
$python = @(Get-Command python, py -ErrorAction SilentlyContinue | ForEach-Object Source) +
    @(Get-ChildItem "$env:LOCALAPPDATA\Programs\Python\Python3*\python.exe" -ErrorAction SilentlyContinue | Sort-Object FullName -Descending | ForEach-Object FullName) |
    Where-Object { Test-Python $_ 'import sys; sys.exit(sys.version_info < (3, 10))' } | Select-Object -First 1
if (-not $python) {
    Write-Host 'Python 3.10+ is required: run .\install.ps1 (or https://www.python.org/downloads/)' -ForegroundColor Red
    exit 1
}
Write-Host "Python   : $(& $python --version)"

# 2. Packages (only when something is missing, or with -Install)
if ($Install -or -not (Test-Python $python 'import flask, flask_cors, requests, dotenv')) {
    Write-Host 'Installing Python packages...'
    & $python -m pip install -r backend\requirements.txt --quiet
    if ($LASTEXITCODE -ne 0) { Write-Host 'pip install failed' -ForegroundColor Red; exit 1 }
}

# 3. Spark is optional: without PySpark + Java 17/21 (found through JAVA_HOME) the pipeline runs its Python fallback
$hasPyspark = Test-Python $python 'import pyspark'
$hasJava = ($env:JAVA_HOME -and (Test-Path (Join-Path $env:JAVA_HOME 'bin\java.exe'))) -or [bool](Get-Command java -ErrorAction SilentlyContinue)
if ($hasPyspark -and $hasJava) {
    Write-Host 'Spark    : PySpark + Java found (pipeline runs on Apache Spark)'
} else {
    Write-Host 'Spark    : not available - the pipeline will use its Python fallback' -ForegroundColor Yellow
    Write-Host '           (optional: .\install.ps1 adds Java 17 + PySpark)' -ForegroundColor Yellow
}

# 4. HDFS (NameNode + DataNode in WSL2 Ubuntu, set up once by install.ps1). DTSMS keeps every dataset in
#    HDFS, so it does not start without it.
& "$PSScriptRoot\tools\hdfs\hdfs.ps1" start
if ($LASTEXITCODE -ne 0) {
    Write-Host 'HDFS is required and did not start. Run install.cmd to set it up or repair it, then start.cmd again.' -ForegroundColor Red
    exit 1
}

# 5. Port: when DTSMS already answers there, open it; when another program has it, take the next free port
function Test-PortBusy([int]$p) { [bool](Get-NetTCPConnection -LocalPort $p -State Listen -ErrorAction SilentlyContinue) }
function Test-Dtsms([int]$p) {
    try { (Invoke-RestMethod "http://127.0.0.1:$p/api/health" -TimeoutSec 3).status -eq 'ok' } catch { $false }
}
if (Test-PortBusy $Port) {
    if (Test-Dtsms $Port) {
        Write-Host "DTSMS is already running on http://127.0.0.1:$Port/ - opening it" -ForegroundColor Green
        if (-not $NoBrowser) { Start-Process "http://127.0.0.1:$Port/" }
        exit 0
    }
    $free = ($Port + 1)..($Port + 20) | Where-Object { -not (Test-PortBusy $_) } | Select-Object -First 1
    if (-not $free) {
        Write-Host "Ports $Port-$($Port + 20) are all in use. Start with another one: start.cmd -Port 8080" -ForegroundColor Red
        exit 1
    }
    Write-Host "Port $Port is used by another program - using port $free instead" -ForegroundColor Yellow
    $Port = $free
}

# 6. Open the browser once the server answers (in the background)
$url = "http://127.0.0.1:$Port/"
if (-not $NoBrowser) {
    Start-Job -ArgumentList $url -ScriptBlock {
        param($u)
        for ($i = 0; $i -lt 60; $i++) {
            try { Invoke-WebRequest "$($u)api/health" -UseBasicParsing -TimeoutSec 2 | Out-Null; Start-Process $u; return } catch { Start-Sleep 1 }
        }
    } | Out-Null
}

# 7. Run the backend (Ctrl+C to stop). The first start with an empty database
#    generates a simulated week of 12-hour surveys and processes it (about a minute).
Write-Host "Starting DTSMS on $url  (Ctrl+C to stop)" -ForegroundColor Green
$env:FLASK_HOST = '127.0.0.1'
$env:FLASK_PORT = "$Port"
[Console]::OutputEncoding = [System.Text.Encoding]::UTF8     # the server log has arrows and dashes in it
$env:PYTHONIOENCODING = 'utf-8'
& $python backend\server.py
