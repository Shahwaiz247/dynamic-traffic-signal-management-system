@echo off
rem DTSMS one-time setup on a new PC (see install.ps1). Double-click it, or: install.cmd -Check
powershell -NoProfile -ExecutionPolicy Bypass -File "%~dp0install.ps1" %*
echo.
pause
