@echo off

SETLOCAL EnableDelayedExpansion

REM Find the location of the bin directory and change to the root of kairosdb
set KAIROSDB_BIN_DIR=%~dp0
cd "%KAIROSDB_BIN_DIR%\.."

set KAIROSDB_LIB_DIR=lib
set KAIROSDB_LOG_DIR=log

if exist "%KAIROSDB_BIN_DIR%kairosdb-env.bat" (
    "%KAIROSDB_BIN_DIR%kairosdb-env.bat"
)

if NOT exist "%KAIROSDB_LOG_DIR%" (
    mkdir "%KAIROSDB_LOG_DIR%"
)

REM Use JAVA_HOME if set, otherwise look for java in PATH
if NOT "%JAVA_HOME%" == "" (
  set JAVA=%JAVA_HOME%\bin\java
) else (
  set JAVA=java
)

REM Load up the classpath
set CLASSPATH=conf\logging

for %%j in ("%KAIROSDB_LIB_DIR%\*.jar") do set CLASSPATH=!CLASSPATH!;%%j


if /i "%1" == "run" (
    "%JAVA%" %JAVA_OPTS% -cp %CLASSPATH% org.kairosdb.core.Main -c run -p conf\kairosdb.properties
) else (
    echo Unrecognized command.
)
