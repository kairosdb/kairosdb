@echo off
set SERVICE_NAME=KairosDB

REM Find the location of the bin directory and change to the root of kairosdb
set KAIROSDB_BIN=%~dp0
set KAIROSDB_HOME=%KAIROSDB_BIN%..
 
set PR_INSTALL=%KAIROSDB_HOME%\bin\daemon\prunsrv.exe

"%PR_INSTALL%" //DS//%SERVICE_NAME%
 
