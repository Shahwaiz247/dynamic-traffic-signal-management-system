# DTSMS - one-time setup on a new Windows 10 (2004+) / 11 PC. Safe to re-run: whatever is already there is skipped.
#
#   .\install.ps1            # everything: Python + packages, Java 17 + PySpark (Apache Spark), WSL2 Ubuntu + real HDFS
#   .\install.ps1 -Check     # only report what is installed and what is missing; changes nothing
#   .\install.ps1 -NoSpark   # skip Java/PySpark: the pipeline then runs its Python fallback
#
# HDFS is required (DTSMS keeps every dataset in it), so WSL2 Ubuntu + Hadoop are always set up.
# Double-click install.cmd, or if PowerShell blocks scripts:  powershell -ExecutionPolicy Bypass -File .\install.ps1
# Downloads: Python 3.12 (~25 MB) and Java 17 (~190 MB) with winget, PySpark 4.0.1 (~430 MB) with pip; for HDFS,
# WSL2 + Ubuntu (~1 GB, first time only), then OpenJDK 11 and Hadoop 3.4.3 (~515 MB, SHA-512 checked) inside Ubuntu.
# Windows asks for administrator rights for Java and WSL. Afterwards start the app with start.cmd (or .\start.ps1).
param(
    [switch]$Check,
    [switch]$NoSpark,
    [string]$Distro = 'Ubuntu'
)
# native tools write progress to stderr: results are judged by exit codes, so stderr must not stop the script
$ErrorActionPreference = 'Continue'
$ProgressPreference = 'SilentlyContinue'
Set-Location $PSScriptRoot

$PYTHON_MIN = [version]'3.10'
$PYSPARK_VERSION = '4.0.1'      # the Spark release the pipeline is tested with; Spark 4 runs on Java 17 or 21
$JAVA_OK = 17, 21
$status = [ordered]@{}
$COLOR = @{ ok = 'Green'; skipped = 'DarkGray'; missing = 'Yellow'; pending = 'Yellow'; failed = 'Red' }

function Step($title) { Write-Host ''; Write-Host "== $title" -ForegroundColor Cyan }

function Report($part, $state, $msg) {
    $status[$part] = $state
    Write-Host ('   {0,-8} {1}' -f $state, $msg) -ForegroundColor $COLOR[$state]
}

function Update-SessionPath {
    $env:Path = [Environment]::GetEnvironmentVariable('Path', 'Machine') + ';' + [Environment]::GetEnvironmentVariable('Path', 'User')
}

function Install-WithWinget($id, [string[]]$extra = @()) {
    if (-not (Get-Command winget -ErrorAction SilentlyContinue)) {
        Write-Host "   winget (App Installer, from the Microsoft Store) is missing: install $id by hand" -ForegroundColor Red
        return
    }
    Write-Host "   installing $id with winget..."
    winget install --exact --id $id --silent --accept-package-agreements --accept-source-agreements @extra
    Update-SessionPath
}

function Find-Python {
    # "python" can be the Microsoft Store alias (WindowsApps\python.exe), which is not Python: run each candidate
    $candidates = @(Get-Command python, py -ErrorAction SilentlyContinue | ForEach-Object Source)
    $candidates += @(Get-ChildItem "$env:LOCALAPPDATA\Programs\Python\Python3*\python.exe", "$env:ProgramFiles\Python3*\python.exe" -ErrorAction SilentlyContinue |
        Sort-Object FullName -Descending | ForEach-Object FullName)
    foreach ($exe in $candidates) {
        $v = & $exe -c "import sys; print('%d.%d' % sys.version_info[:2])" 2>$null
        if ($LASTEXITCODE -eq 0 -and $v -and [version]"$v" -ge $PYTHON_MIN) { return [pscustomobject]@{ Exe = $exe; Version = "$v" } }
    }
}

function Get-JavaMajor($javaExe) {
    $psi = New-Object System.Diagnostics.ProcessStartInfo($javaExe, '-version')
    $psi.UseShellExecute = $false; $psi.RedirectStandardError = $true; $psi.CreateNoWindow = $true
    $p = [System.Diagnostics.Process]::Start($psi)
    $text = $p.StandardError.ReadToEnd(); $p.WaitForExit()
    if ($text -match 'version "1\.(\d+)') { return [int]$Matches[1] }      # Java 8 and older: "1.8.0_..."
    if ($text -match 'version "(\d+)') { return [int]$Matches[1] }
    return 0
}

function Find-Jdk {
    # Spark finds Java through JAVA_HOME, so that must name a Java 17 or 21 installation
    $homes = @()
    if ($env:JAVA_HOME) { $homes += "$env:JAVA_HOME".TrimEnd('\') }
    $homes += @(Get-ChildItem "$env:ProgramFiles\Eclipse Adoptium\jdk-*", "$env:ProgramFiles\Microsoft\jdk-*", "$env:ProgramFiles\Java\jdk-*",
        "$env:ProgramFiles\Zulu\zulu-*" -Directory -ErrorAction SilentlyContinue | Sort-Object Name | ForEach-Object FullName)
    $java = Get-Command java -ErrorAction SilentlyContinue
    if ($java) { $homes += Split-Path (Split-Path $java.Source) }
    foreach ($h in $homes) {
        $exe = Join-Path $h 'bin\java.exe'
        if (Test-Path $exe) {
            $major = Get-JavaMajor $exe
            if ($JAVA_OK -contains $major) { return [pscustomobject]@{ Home = $h; Major = $major } }
        }
    }
}

function Test-WebHdfs {
    try { (Invoke-WebRequest 'http://127.0.0.1:9870/webhdfs/v1/?op=GETFILESTATUS' -UseBasicParsing -TimeoutSec 3).StatusCode -eq 200 }
    catch { $false }
}

Write-Host "DTSMS setup in $PSScriptRoot" -ForegroundColor Green
if ($Check) { Write-Host '(check only: nothing is installed or changed)' }

# 1. Python ----------------------------------------------------------------------------------------
Step "Python $PYTHON_MIN or newer"
$py = Find-Python
if (-not $py -and -not $Check) {
    Install-WithWinget 'Python.Python.3.12' @('--scope', 'user')
    $py = Find-Python
}
if ($py) { Report python ok "Python $($py.Version)  $($py.Exe)" }
elseif ($Check) { Report python missing 'not found (install.ps1 installs Python 3.12 with winget)' }
else { Report python failed 'not found after installing: get it from https://www.python.org/downloads/ and run install.ps1 again' }

# 2. Python packages -------------------------------------------------------------------------------
Step 'Python packages (backend\requirements.txt)'
if ($py) {
    & $py.Exe -c 'import flask, flask_cors, requests, dotenv' 2>$null
    $have = $LASTEXITCODE -eq 0
    if (-not $have -and -not $Check) {
        & $py.Exe -m pip install --disable-pip-version-check -r backend\requirements.txt
        & $py.Exe -c 'import flask, flask_cors, requests, dotenv' 2>$null
        $have = $LASTEXITCODE -eq 0
    }
    if ($have) { Report packages ok 'Flask, flask-cors, requests, python-dotenv' }
    elseif ($Check) { Report packages missing 'not installed (install.ps1 runs pip install -r backend\requirements.txt)' }
    else { Report packages failed 'pip install -r backend\requirements.txt failed (see above)' }
} else { Report packages skipped 'needs Python' }

# 3. Apache Spark: Java 17 + PySpark ---------------------------------------------------------------
Step "Apache Spark (Java 17 + PySpark $PYSPARK_VERSION)"
if ($NoSpark) {
    Report java skipped '-NoSpark: the pipeline runs its Python fallback'
    Report spark skipped '-NoSpark'
} else {
    $jdk = Find-Jdk
    if (-not $jdk -and -not $Check) {
        Install-WithWinget 'EclipseAdoptium.Temurin.17.JDK'
        $jdk = Find-Jdk
    }
    if ($jdk) {
        if ("$env:JAVA_HOME".TrimEnd('\') -ne $jdk.Home) {
            if ($Check) { Report java missing "Java $($jdk.Major) found, but JAVA_HOME does not point to it ($($jdk.Home))" }
            else {
                [Environment]::SetEnvironmentVariable('JAVA_HOME', $jdk.Home, 'User')
                $env:JAVA_HOME = $jdk.Home
                Report java ok "Java $($jdk.Major)  JAVA_HOME set to $($jdk.Home)"
            }
        } else { Report java ok "Java $($jdk.Major)  $($jdk.Home)" }
    } elseif ($Check) { Report java missing 'no Java 17/21 (install.ps1 installs Eclipse Temurin 17 with winget)' }
    else { Report java failed 'no Java 17/21 after installing: get Temurin 17 from https://adoptium.net and run install.ps1 again' }

    if ($py) {
        $probe = "import pyspark, sys; sys.exit(0 if pyspark.__version__.startswith('4.') else 1)"
        & $py.Exe -c $probe 2>$null
        $hasPyspark = $LASTEXITCODE -eq 0
        if (-not $hasPyspark -and -not $Check) {
            Write-Host "   installing pyspark==$PYSPARK_VERSION (about 430 MB)..."
            & $py.Exe -m pip install --disable-pip-version-check "pyspark==$PYSPARK_VERSION"
            & $py.Exe -c $probe 2>$null
            $hasPyspark = $LASTEXITCODE -eq 0
        }
        if (-not $hasPyspark) { Report spark ($(if ($Check) { 'missing' } else { 'failed' })) "PySpark 4.x is not installed" }
        elseif ($status.java -ne 'ok') { Report spark missing 'PySpark is installed, but Spark needs Java 17/21 through JAVA_HOME' }
        else {
            # a real Spark job: start a local session and sum 0..999
            Write-Host '   starting a local Spark session to test it (about 15 s)...'
            $test = Join-Path $env:TEMP 'dtsms_spark_check.py'
            @(
                'from pyspark.sql import SparkSession'
                "s = SparkSession.builder.master('local[2]').appName('dtsms-install-check').config('spark.ui.enabled', 'false').getOrCreate()"
                "s.sparkContext.setLogLevel('ERROR')"
                "n = s.range(1000).selectExpr('sum(id) AS n').first()['n']"
                "print('DTSMS_SPARK', s.version, s.sparkContext._jvm.System.getProperty('java.version'), n)"
                's.stop()'
            ) | Set-Content -Path $test -Encoding ASCII
            $out = (& $py.Exe $test 2>$null) -join "`n"
            Remove-Item $test -ErrorAction SilentlyContinue
            if ($out -match 'DTSMS_SPARK (\S+) (\S+) 499500') { Report spark ok "Apache Spark $($Matches[1]) on Java $($Matches[2]): test job passed" }
            else { Report spark failed 'the Spark test job did not finish (the pipeline will fall back to Python)' }
        }
    } else { Report spark skipped 'needs Python' }
}

# 4. Real HDFS: WSL2 Ubuntu + Hadoop ---------------------------------------------------------------
Step "Real HDFS (WSL2 $Distro + Apache Hadoop)"
$env:WSL_UTF8 = '1'                                  # plain UTF-8 from "wsl -l" instead of UTF-16
$distros = @()
if (Get-Command wsl.exe -ErrorAction SilentlyContinue) {
    $list = wsl.exe -l -q 2>$null
    if ($LASTEXITCODE -eq 0) { $distros = @($list | ForEach-Object { ($_ -replace "`0", '').Trim() } | Where-Object { $_ }) }
}
if ($distros -notcontains $Distro) {
    if ($Check) { Report hdfs missing "WSL distro '$Distro' is not installed (install.ps1 installs it)" }
    else {
        Write-Host "   installing WSL2 + $Distro (Windows asks for administrator rights; about 1 GB)..."
        try {
            $p = Start-Process wsl.exe -ArgumentList @('--install', '-d', $Distro) -Verb RunAs -Wait -PassThru
            Report hdfs pending ("WSL setup ran (exit $($p.ExitCode)). Restart Windows if it asked, open '$Distro' from the Start menu " +
                'once to create your Linux user, then run install.ps1 again to finish HDFS.')
        } catch {
            Report hdfs failed ("WSL was not installed ($($_.Exception.Message)). DTSMS needs it for HDFS: allow the administrator " +
                'prompt, or turn on virtualization in the BIOS/UEFI if Windows reports it is off, then run install.cmd again.')
        }
    }
} else {
    wsl.exe -d $Distro -- test -x /usr/local/bin/dtsms-hdfs
    $ready = $LASTEXITCODE -eq 0
    if (-not $ready -and -not $Check) {
        Write-Host '   installing Java 11 + Hadoop inside WSL (as root; Hadoop is ~515 MB)...'
        # a copy without Windows line endings, in case the project was copied with CRLF conversion
        wsl.exe -d $Distro -u root -- bash -c "tr -d '\r' < tools/hdfs/setup_wsl_hdfs.sh > /tmp/dtsms_setup.sh && bash /tmp/dtsms_setup.sh"
        wsl.exe -d $Distro -- test -x /usr/local/bin/dtsms-hdfs
        $ready = $LASTEXITCODE -eq 0
    }
    if (-not $ready) {
        if ($Check) { Report hdfs missing "Hadoop is not set up in '$Distro' (install.ps1 runs tools/hdfs/setup_wsl_hdfs.sh)" }
        else { Report hdfs failed "Hadoop setup in '$Distro' failed (see above). Run install.cmd again: finished steps are skipped." }
    } elseif (Test-WebHdfs) { Report hdfs ok 'running: NameNode http://127.0.0.1:9870, hdfs://127.0.0.1:9000' }
    elseif ($Check) { Report hdfs ok "set up in '$Distro', not running now (start.ps1 starts it)" }
    else {
        & "$PSScriptRoot\tools\hdfs\hdfs.ps1" start -Distro $Distro
        if (Test-WebHdfs) { Report hdfs ok 'started: NameNode http://127.0.0.1:9870, hdfs://127.0.0.1:9000' }
        else { Report hdfs failed 'set up, but did not start: logs are in /var/lib/dtsms-hdfs/logs inside WSL' }
    }
}

# Summary ------------------------------------------------------------------------------------------
Step 'Summary'
foreach ($k in $status.Keys) { Write-Host ('   {0,-9} {1}' -f $k, $status[$k]) -ForegroundColor $COLOR[$status[$k]] }
Write-Host '   database: SQLite in backend\ (no server needed); a MySQL server is used instead when backend\.env has its login'
$bad = @($status.Values | Where-Object { $_ -in 'failed', 'missing' })
$pending = @($status.Values | Where-Object { $_ -eq 'pending' })
Write-Host ''
if ($bad.Count -eq 0 -and $pending.Count -eq 0) {
    Write-Host 'Ready. Start DTSMS with start.cmd (or .\start.ps1): it opens http://127.0.0.1:5000/' -ForegroundColor Green
    Write-Host 'The first start with an empty database simulates a week of surveys and processes it (about a minute).'
} elseif ($bad.Count -eq 0) {
    Write-Host 'Almost there: finish the pending step above, then run install.ps1 again.' -ForegroundColor Yellow
} elseif ($Check) {
    Write-Host 'Run install.ps1 (or double-click install.cmd) to install what is missing.' -ForegroundColor Yellow
}
exit $(if ($bad.Count) { 1 } else { 0 })
