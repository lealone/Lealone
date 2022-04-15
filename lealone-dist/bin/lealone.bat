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

if NOT DEFINED LEALONE_MAIN set LEALONE_MAIN=org.lealone.main.Lealone


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

set logdir=%LEALONE_HOME%\logs
set args=""

set JAVA_OPTS=-Xms10M^
 -Dlealone.logdir="%logdir%"

set str=%1
if "%str%"=="-nodes" (
    goto nodes
)
if "%str%"=="-cluster" (
    goto cluster
)
if "%str%"=="-debug" (
    set JAVA_OPTS=%JAVA_OPTS% -agentlib:jdwp=transport=dt_socket,address=8000,server=y,suspend=y
    goto exec
)
if "%str%"=="" (
    goto exec
)

goto usage

:usage
echo usage: "lealone [-nodes <value>]"
goto finally

:nodes
for /L %%i in (1,1,%2) do start "Node"%%i lealone.bat -cluster %%i
goto finally

:cluster
set logdir="%logdir%\cluster\node%2"
set args="-cluster %2"
goto exec

:exec
set CLASSPATH="%LEALONE_HOME%\conf;%LEALONE_HOME%\lib\*"

REM echo Starting Lealone Server
"%JAVA_HOME%\bin\java" %JAVA_OPTS% -cp %CLASSPATH% %LEALONE_MAIN% %args%
goto finally

:err
echo JAVA_HOME environment variable must be set!
pause

:finally

ENDLOCAL
