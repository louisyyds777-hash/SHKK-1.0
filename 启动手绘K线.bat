@echo off
rem Launcher: start local server in background (no window), then auto-open browser.
rem NOTE: keep this file ASCII-only. Chinese comments break cmd (GBK codepage) parsing.
cd /d "%~dp0"
where pythonw >nul 2>nul
if %errorlevel%==0 (
  start "" pythonw server.py
) else (
  start /min "" python server.py
)
