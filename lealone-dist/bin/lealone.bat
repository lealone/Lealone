@REM
@REM  Copyright Lealone Database Group.
@REM  Licensed under the Server Side Public License, v 1.
@REM  Initial Developer: zhh

@echo off
if "%OS%" == "Windows_NT" setlocal

if NOT DEFINED JAVA_HOME goto :err

pushd %~dp0..
if NOT DEFINED LEALONE_HOME set LEALONE_HOME=%CD%
popd

REM ***** JAVA options *****
REM set JAVA_OPTS=-ea^
REM  -Xms10M^
REM  -Xmx1G^
REM  -XX:+HeapDumpOnOutOfMemoryError^
REM  -XX:+UseParNewGC^
REM  -XX:+UseConcMarkSweepGC^
REM  -XX:+CMSParallelRemarkEnabled^
REM  -XX:SurvivorRatio=8^
REM  -XX:MaxTenuringThreshold=1^
REM  -XX:CMSInitiatingOccupancyFraction=75^
REM  -XX:+UseCMSInitiatingOccupancyOnly

set JAVA_OPTS=%JAVA_OPTS%

set str=%1
if "%str%"=="-debug" (
    set JAVA_OPTS=%JAVA_OPTS% -agentlib:jdwp=transport=dt_socket,address=8000,server=y,suspend=y
    goto exec
)
if "%str%"=="" (
    goto exec
)

:exec
set CLASSPATH="%LEALONE_HOME%\conf;%LEALONE_HOME%\lib\*"
"%JAVA_HOME%\bin\java" %JAVA_OPTS% -cp %CLASSPATH% org.lealone.main.Lealone %*
goto finally

:err
echo JAVA_HOME environment variable must be set!
pause

:finally

ENDLOCAL
