
@echo off

rem Set PostgreSQL connection parameters
set "PGHOST=localhost"
set "PGPORT=5432"
set "PGUSER=postgres"
set "PGPASSWORD=postgres"
set "PGDATABASE=abd_stack_overflow"

set "QUERY=Q1"
if "%~1" neq "" (set "QUERY=%~1")

rem Set PostgreSQL script file name
set "SCRIPT_FILE=%QUERY%.sql"

rem Set output dir name
set "OUTPUT_DIR="
if "%~2" neq "" (set "OUTPUT_DIR=%~2")

rem Set output file name
set "OUTPUT_FILE=%OUTPUT_DIR%%QUERY%.txt"

rem Set output file name
set "OUTPUT_CSV=%OUTPUT_DIR%%QUERY%.csv"

rem Set number of times to execute the script
set "NUM_EXECUTIONS=20"

setlocal enabledelayedexpansion

rem Extracting the last three rows from the output and saving them in CSV format
echo PLANNING_TIME,EXECUTION_TIME,ROWS > %OUTPUT_CSV%

rem Loop to execute the script multiple times
for /l %%i in (1, 1, %NUM_EXECUTIONS%) do (
    echo Executing script, iteration %%i...
    psql -h %PGHOST% -p %PGPORT% -U %PGUSER% -d %PGDATABASE% -f %SCRIPT_FILE% > %OUTPUT_FILE%
    rem echo EXPLAIN ANALYSE $(cat %SCRIPT_FILE%) | psql -h %PGHOST% -p %PGPORT% -U %PGUSER% -d %PGDATABASE% > %OUTPUT_FILE%

    set "PLANNING_TIME="
    set "EXECUTION_TIME="
    set "ROWS="

    for /f "tokens=2 delims=:" %%a in ('findstr /R /C:"Planning Time:" %OUTPUT_FILE%') do (
        for /f "tokens=1" %%b in ("%%a") do (set "PLANNING_TIME=%%b")
    )

    for /f "tokens=2 delims=:" %%a in ('findstr /R /C:"Execution Time:" %OUTPUT_FILE%') do (
        for /f "tokens=1" %%b in ("%%a") do (set "EXECUTION_TIME=%%b")
    )

    for /f "tokens=1" %%a in ('findstr /R /C:"(.*rows)" %OUTPUT_FILE%') do (
        for /f "tokens=1 delims=(" %%b in ("%%a") do (set "ROWS=%%b")
    )

    rem echo\ !PLANNING_TIME!
    rem echo\ !EXECUTION_TIME!
    rem echo\ !ROWS!
    rem echo\ !PLANNING_TIME!,!EXECUTION_TIME!,!ROWS!

    echo !PLANNING_TIME!,!EXECUTION_TIME!,!ROWS! >> %OUTPUT_CSV%

)

endlocal

echo %OUTPUT_CSV% saved!