#!/usr/bin/env bash
# Starts DTSMS (Git Bash on Windows, macOS, Linux): checks Python, installs missing
# packages, runs the backend and opens the app in the browser.
#
#   ./start.sh                  # start on http://127.0.0.1:5000/
#   PORT=8080 ./start.sh        # another port
#   NO_BROWSER=1 ./start.sh     # don't open the browser
#   INSTALL=1 ./start.sh        # force a reinstall of the Python packages
#
# HDFS (Apache Hadoop, WebHDFS on :9870) is required: DTSMS keeps every dataset in it.
set -euo pipefail
cd "$(dirname "$0")"
PORT="${PORT:-5000}"
URL="http://127.0.0.1:${PORT}/"

# 1. Python 3 — prefer an interpreter that already has PySpark (several may be installed)
PY=""
for c in python python3 py; do
  command -v "$c" >/dev/null || continue
  "$c" -c 'import sys; sys.exit(sys.version_info < (3, 9))' 2>/dev/null || continue
  [ -z "$PY" ] && PY="$c"
  if "$c" -c 'import pyspark' 2>/dev/null; then PY="$c"; break; fi
done
if [ -z "$PY" ]; then
  echo "Python 3.10+ is required: https://www.python.org/downloads/" >&2
  exit 1
fi
echo "Python   : $("$PY" --version 2>&1)"

# 2. Packages (only when something is missing, or with INSTALL=1)
if [ -n "${INSTALL:-}" ] || ! "$PY" -c 'import flask, flask_cors, requests, dotenv' 2>/dev/null; then
  echo "Installing Python packages..."
  "$PY" -m pip install -r backend/requirements.txt --quiet
fi

# 3. Spark is optional: without PySpark + Java the pipeline runs its Python fallback
if "$PY" -c 'import pyspark' 2>/dev/null && command -v java >/dev/null; then
  echo "Spark    : PySpark + Java found (pipeline runs on Apache Spark)"
else
  echo "Spark    : not available - the pipeline will use its Python fallback"
  echo "           (optional: pip install pyspark, and install Java 17)"
fi

# 4. HDFS, required. On Windows (Git Bash) it runs in WSL2 Ubuntu (install.ps1 sets it up); on Ubuntu/Debian
#    set it up once with  sudo bash tools/hdfs/setup_wsl_hdfs.sh  and this script starts it.
hdfs_up() { curl -s -o /dev/null "http://127.0.0.1:9870/webhdfs/v1/?op=GETFILESTATUS"; }
case "$(uname -s)" in
  MINGW*|MSYS*|CYGWIN*) powershell.exe -NoProfile -ExecutionPolicy Bypass -File tools/hdfs/hdfs.ps1 start || true ;;
  *) hdfs_up || { command -v dtsms-hdfs >/dev/null && dtsms-hdfs start; } || true ;;
esac
if ! hdfs_up; then
  echo "HDFS is required and is not running on :9870." >&2
  echo "  Windows: run install.cmd.  Ubuntu/Debian: sudo bash tools/hdfs/setup_wsl_hdfs.sh, then run this again." >&2
  exit 1
fi
echo "HDFS     : running (http://127.0.0.1:9870)"

# 5. Open the browser once the server answers (in the background)
if [ -z "${NO_BROWSER:-}" ]; then
  (
    for _ in $(seq 1 60); do
      if curl -s -o /dev/null "${URL}api/health"; then
        case "$(uname -s)" in
          Darwin) open "$URL" ;;
          MINGW*|MSYS*|CYGWIN*) start "" "$URL" ;;
          *) xdg-open "$URL" >/dev/null 2>&1 || true ;;
        esac
        break
      fi
      sleep 1
    done
  ) &
fi

# 6. Run the backend (Ctrl+C to stop). The first start with an empty database
#    generates a simulated week of 12-hour surveys and processes it (about a minute).
echo "Starting DTSMS on ${URL}  (Ctrl+C to stop)"
export FLASK_HOST=127.0.0.1 FLASK_PORT="$PORT" PYTHONIOENCODING=utf-8
exec "$PY" backend/server.py
