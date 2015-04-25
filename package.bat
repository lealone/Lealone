@echo off

set ARG=%1
if /i "%ARG%" == "clean" goto clean
call mvn package assembly:assembly -Dmaven.test.skip=true
goto end

:clean
call mvn clean package assembly:assembly -Dmaven.test.skip=true
goto end

:end
