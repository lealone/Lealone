@REM
@REM  Copyright Lealone Database Group.
@REM  Licensed under the Server Side Public License, v 1.
@REM  Initial Developer: zhh

@echo off
if "%OS%" == "Windows_NT" setlocal

set ARG=%1

if /i "%ARG%" == "" goto usage
if /i "%ARG%" == "-e" goto e
if /i "%ARG%" == "-ec" goto ec
if /i "%ARG%" == "-es" goto es
if /i "%ARG%" == "-p" goto p
if /i "%ARG%" == "-pc" goto pc
if /i "%ARG%" == "-pd" goto pd
if /i "%ARG%" == "-i" goto i
if /i "%ARG%" == "-c" goto c
if /i "%ARG%" == "-dt" goto dt
if /i "%ARG%" == "-vu" goto vu

goto usage

:usage
echo usage: build [options]
echo    options:
echo    -e            mvn eclipse:eclipse
echo    -ec           mvn eclipse:clean
echo    -es           mvn eclipse:eclipse -DdownloadSources=true
echo    -p            mvn package assembly:assembly -Dmaven.test.skip=true
echo    -pc           mvn clean package assembly:assembly -Dmaven.test.skip=true
echo    -pd           mvn package assembly:assembly -Dmaven.test.skip=true -P database
echo    -i            mvn install -Dmaven.test.skip=true
echo    -c            mvn clean
echo    -dt           mvn dependency:tree
echo    -vu version   pom.xml version update
goto end

:e
call mvn eclipse:eclipse
goto end

:ec
call mvn eclipse:clean
goto end

:es
call mvn eclipse:eclipse -DdownloadSources=true
goto end

:p
call mvn package assembly:assembly -Dmaven.test.skip=true
goto end

:pc
call mvn clean package assembly:assembly -Dmaven.test.skip=true
goto end

:pd
call mvn package assembly:assembly -Dmaven.test.skip=true -P database
goto end

:c
call mvn clean
goto end

:i
call mvn install -Dmaven.test.skip=true
goto end

:dt
call mvn dependency:tree
goto end

:vu
set VERSION=%2
if /i "%VERSION%" == "" goto usage
call mvn versions:set -DnewVersion=%VERSION%
call mvn versions:commit
echo lealoneVersion=%VERSION%>lealone-common\src\main\resources\org\lealone\common\resources\version.properties
goto end

:end
