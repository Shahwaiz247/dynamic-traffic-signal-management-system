# DTSMS - Dynamic Traffic Signal Management System

Weekly signal timetables for the 47 signalised junctions of Thiruvananthapuram district, planned from
traffic surveys: CSV upload → HDFS → Apache Spark → SQL → Webster's method → a web app with the
timetable, a live map of every junction and a 3D view of the selected one.

## Install (once)

Needs Windows 10 version 2004 or later, or Windows 11, and an internet connection.

**Easiest:** run `DTSMS-Setup-<version>.exe` (in the zip, next to this folder). It installs DTSMS for your
user with Start-menu shortcuts and an uninstaller, then runs the steps below for you.

**Or by hand:**

1. Unzip this folder somewhere you can write to, for example `Documents\DTSMS`.
2. Double-click `install.cmd`. It installs only what is missing:
   - Python 3.12 and the Python packages
   - Java 17 and Apache Spark (PySpark 4.0.1)
   - WSL2 Ubuntu with Apache Hadoop, for the real HDFS

   Windows asks for administrator rights for Java and WSL. If WSL is new on this PC, restart when
   asked, open Ubuntu once from the Start menu to create a Linux user, then run `install.cmd` again.

HDFS is required: DTSMS stores every dataset in it, and does not start without it. `install.cmd -Check`
only reports what is installed. `install.cmd -NoSpark` skips Java and Spark; the pipeline then runs its
Python fallback.

## Start

Double-click `start.cmd`. It starts HDFS and the server, then opens <http://127.0.0.1:5000/>.
The first start simulates a week of traffic surveys and processes it, which takes about a minute.
Close the window (or press Ctrl+C in it) to stop.

On Ubuntu or Debian, set up HDFS once with `sudo bash tools/hdfs/setup_wsl_hdfs.sh`, then run
`./start.sh` (Python 3.10+, optionally Java 17 and PySpark).

## Your own survey data

Open **Data and pipeline** → **Upload a survey**. The page has a template CSV to download and a
table of the columns it reads. A `To` column (where the vehicles went) is optional; with it, the
turning split uses your counts.

## Settings

Optional: copy `backend\.env.example` to `backend\.env` to use a MySQL server or change the port.
Without it the app keeps its data in a built-in SQLite database.

## Rebuilding the installer

`installer\` has what builds the setup.exe and this zip. With [Inno Setup 6](https://jrsoftware.org/isinfo.php)
installed, run `python installer\build_release.py`; the results are in `installer\Output\`.
