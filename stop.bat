@echo off
setlocal
REM Stop script for data-fetcher - stops process by PID saved in run.pid

set SCRIPT_DIR=%~dp0
cd /d "%SCRIPT_DIR%"

if not exist "%SCRIPT_DIR%run.pid" (
  echo PID file not found: %SCRIPT_DIR%run.pid
  exit /b 1
)

for /f %%A in ('type "%SCRIPT_DIR%run.pid"') do set PID=%%A

echo Stopping data-fetcher with PID %PID% and all child processes...

REM Use taskkill with /T flag to kill process tree (parent + all children)
taskkill /PID %PID% /T /F
if %ERRORLEVEL% equ 0 (
  echo Successfully stopped PID %PID% and child processes
  if exist "%SCRIPT_DIR%run.pid" (
    del /q "%SCRIPT_DIR%run.pid"
    echo Cleaned up run.pid
  )
) else (
  echo Failed to stop process tree for PID %PID%
  exit /b 1
)

endlocal
