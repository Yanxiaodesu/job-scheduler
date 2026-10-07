@echo off
REM ============================================================
REM  Job Scheduler launcher
REM
REM  Usage:
REM    run.cmd admin    8080
REM    run.cmd executor 9001
REM    run.cmd both     8080
REM
REM  Args:
REM    %1 role        admin ^| executor ^| both
REM    %2 port        http port
REM    %3 instanceId  optional, defaults to ^<role^>-^<port^>
REM    %4 appName     optional, defaults to demo
REM
REM  Notes:
REM    - ASCII only on purpose: Chinese text inside a .cmd file
REM      breaks under the default GBK console codepage.
REM    - One jar, three roles. To reproduce the "no duplicate
REM      dispatch" test, start three admins on 8080/8081/8082.
REM ============================================================
setlocal

set ROLE=%~1
set PORT=%~2
set INSTANCE_ID=%~3
set APP_NAME=%~4

if "%ROLE%"=="" goto usage
if "%PORT%"=="" goto usage
if "%INSTANCE_ID%"=="" set INSTANCE_ID=%ROLE%-%PORT%
if "%APP_NAME%"=="" set APP_NAME=demo

set JAVA_HOME=D:\java\jdk17
if not exist "%JAVA_HOME%\bin\java.exe" (
    echo [ERROR] JDK not found at %JAVA_HOME%
    exit /b 1
)

set ROOT=%~dp0..
set JAR=%ROOT%\target\job-scheduler-0.0.1-SNAPSHOT.jar
if not exist "%JAR%" (
    echo [ERROR] jar not found: %JAR%
    echo         Build it first:  mvnw.cmd package -DskipTests
    exit /b 1
)

if not exist "%ROOT%\logs" mkdir "%ROOT%\logs"

REM SCAN_INTERVAL_MS controls scheduling precision:
REM   average delay is roughly half of this value.
REM   1000 -> ~560ms P50 ; 200 -> ~110ms P50
if "%SCAN_INTERVAL_MS%"=="" set SCAN_INTERVAL_MS=1000

set ADDRESS=http://127.0.0.1:%PORT%
set CALLBACK_URL=http://127.0.0.1:%PORT%/api/internal/callback

echo ============================================================
echo   role       : %ROLE%
echo   port       : %PORT%
echo   instanceId : %INSTANCE_ID%
echo   appName    : %APP_NAME%
echo ============================================================

"%JAVA_HOME%\bin\java.exe" -jar "%JAR%"
exit /b %ERRORLEVEL%

:usage
echo Usage: run.cmd ^<admin^|executor^|both^> ^<port^> [instanceId] [appName]
exit /b 1
