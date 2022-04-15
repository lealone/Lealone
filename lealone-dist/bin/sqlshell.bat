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

if NOT DEFINED LEALONE_MAIN set LEALONE_MAIN=org.lealone.main.Shell

set JAVA_OPTS=-Xms10M^
 -Dlealone.logdir="%LEALONE_HOME%\logs"

set CLASSPATH="%LEALONE_HOME%\conf;%LEALONE_HOME%\lib\*"

REM set LEALONE_PARAMS=%1 %2
REM goto runShell

set LEALONE_PARAMS=

:param
set str=%1
if "%str%"=="" (
    goto end
)
set LEALONE_PARAMS=%LEALONE_PARAMS% %str%
shift /0
goto param

:end
if "%LEALONE_PARAMS%"=="" (
    goto runShell
)

:runShell
"%JAVA_HOME%\bin\java" %JAVA_OPTS% -cp %CLASSPATH% "%LEALONE_MAIN%" %LEALONE_PARAMS%
goto finally

:err
echo JAVA_HOME environment variable must be set!
pause

:finally

ENDLOCAL
