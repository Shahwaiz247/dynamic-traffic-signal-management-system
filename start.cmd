@echo off
rem Starts DTSMS on http://127.0.0.1:5000/ (see start.ps1). Double-click it, or: start.cmd -Port 8080
powershell -NoProfile -ExecutionPolicy Bypass -File "%~dp0start.ps1" %*
rem keep the window open when something went wrong, so the message can be read
if errorlevel 1 pause
