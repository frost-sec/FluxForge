@echo off
rem Process-local script policy; never changes the user's global PowerShell policy.
powershell.exe -NoLogo -NoProfile -ExecutionPolicy Bypass -File "%~dp0ff.ps1" %*
