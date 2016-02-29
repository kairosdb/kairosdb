@echo off
set SERVICE_NAME=KairosDB

REM Find the location of the bin directory and change to the root of kairosdb
set KAIROSDB_BIN=%~dp0
set KAIROSDB_HOME=%KAIROSDB_BIN%..
 
set PR_INSTALL=%KAIROSDB_HOME%\bin\daemon\prunsrv.exe

 
@REM Service Log Configuration
set PR_LOGPREFIX=%SERVICE_NAME%
set PR_LOGPATH=%KAIROSDB_HOME%\mainlogs
set PR_KAIROS_PROCESS_FILE_DIR=%KAIROSDB_HOME%\tempProcessFiles
set PR_PID_FILE=pid.txt
set PR_STDOUTPUT=auto
set PR_STDERROR=auto
set PR_LOGLEVEL=Debug
 
@REM Path to Java Installation
set PR_JVM=%JAVA_HOME%\jre\bin\server\jvm.dll
 
set PR_CLASSPATH=%KAIROSDB_HOME%\lib\;%KAIROSDB_HOME%\conf\logging

REM For each jar in the CASSANDRA_HOME lib directory call append to build the CLASSPATH variable.
for %%i in ("%KAIROSDB_HOME%\lib\*.jar") do call :append "%%i"
goto okKairos

:append
set PR_CLASSPATH=%PR_CLASSPATH%;%1
goto :eof

REM -----------------------------------------------------------------------------
:okKairos
 
@REM Startup Configuration
set KairosDb_START_CLASS=org.kairosdb.core.daemon.WindowsService
set PR_JVMOPTIONS=-Xdebug
 
set PR_STARTUP=auto
set PR_STARTMODE=java
set PR_STARTCLASS=%KairosDb_START_CLASS%
set PR_STARTPARAMS=processFileDir=%PR_KAIROS_PROCESS_FILE_DIR%;-c;run;-p;%KAIROSDB_HOME%\conf\kairosdb.properties
 
@REM Shutdown Configuration
set PR_STOPMODE=java
set PR_STOPCLASS=%KairosDb_START_CLASS%


set PR_STOPPARAMS=stop;processFileDir=%PR_KAIROS_PROCESS_FILE_DIR%
 
"%PR_INSTALL%" //IS/%SERVICE_NAME% ^
  --DisplayName="%SERVICE_NAME%" ^
  --Install="%PR_INSTALL%" ^
  --Startup="%PR_STARTUP%" ^
  --LogPath="%PR_LOGPATH%" ^
  --LogPrefix="%PR_LOGPREFIX%" ^
  --LogLevel="%PR_LOGLEVEL%" ^
  --StdOutput="%PR_STDOUTPUT%" ^
  --StdError="%PR_STDERROR%" ^
  --JavaHome="%JAVA_HOME%" ^
  --Jvm="%PR_JVM%" ^
  --JvmOptions="%PR_JVMOPTIONS%" ^
  --Classpath="%PR_CLASSPATH%" ^
  --StartMode="%PR_STARTMODE%" ^
  --StartClass="%KairosDb_START_CLASS%" ^
  --StartParams="%PR_STARTPARAMS%" ^
  --StopMode="%PR_STOPMODE%" ^
  --StopClass="%PR_STOPCLASS%" ^
  --StopParams="%PR_STOPPARAMS%" ^
  --PidFile="%PR_PID_FILE%"
 
if not errorlevel 1 goto installed
echo Failed to install "%SERVICE_NAME%" service.  Refer to log in %PR_LOGPATH%
goto end
 
:installed
echo The Service "%SERVICE_NAME%" has been installed
 
:end

