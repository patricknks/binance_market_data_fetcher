@echo off
setlocal
REM Start script for data-fetcher
REM Usage: run this from the deploy folder containing the JAR and config\application.properties

set SCRIPT_DIR=%~dp0
cd /d "%SCRIPT_DIR%"

REM Locate JAR - picks the most recently modified binance_data_fetcher-*.jar, so the
REM version number never has to be edited here. target\ wins over the script folder.
set "JAR="
set "JARCOUNT=0"
for /f "delims=" %%F in ('dir /b /o-d "target\binance_data_fetcher-*.jar" 2^>nul') do (
    set /a JARCOUNT+=1
    if not defined JAR set "JAR=target\%%F"
)
if not defined JAR (
    for /f "delims=" %%F in ('dir /b /o-d "binance_data_fetcher-*.jar" 2^>nul') do (
        set /a JARCOUNT+=1
        if not defined JAR set "JAR=%%F"
    )
)
if not defined JAR (
    echo ERROR: no binance_data_fetcher-*.jar found in %SCRIPT_DIR% or %SCRIPT_DIR%target\
    exit /b 1
)
if not "%JARCOUNT%"=="1" echo Warning: %JARCOUNT% binance_data_fetcher JARs found, using the newest one.
echo Using JAR: %JAR%

if not exist "config\application.properties" (
  echo Warning: config\application.properties not found in %SCRIPT_DIR%config\
)

REM config\application.properties reads the DB password from this variable.
if not defined DB_PASSWORD (
  echo Warning: DB_PASSWORD is not set, the database connection will fail at startup.
)

if not exist "logs" mkdir logs

echo Starting data-fetcher from %SCRIPT_DIR%

REM Start Java process via PowerShell and write PID to run.pid, redirect stdout/stderr to logs
powershell -NoProfile -Command ^
  "$args = @('-jar','%JAR%','--spring.config.location=file:./config/application.properties');" ^
  "$out = Join-Path '%CD%' 'logs\stdout.log';" ^
  "$err = Join-Path '%CD%' 'logs\stderr.log';" ^
  "$p = Start-Process -FilePath 'java' -ArgumentList $args -WorkingDirectory '%CD%' -RedirectStandardOutput $out -RedirectStandardError $err -WindowStyle Hidden -PassThru; $p.Id" > "%SCRIPT_DIR%run.pid"

if %ERRORLEVEL% neq 0 (
  echo Failed to start process. Check logs\stderr.log for errors.
  type "%SCRIPT_DIR%logs\stderr.log"
  exit /b 1
)

echo Started. PID:
type "%SCRIPT_DIR%run.pid"

endlocal