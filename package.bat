@echo off

call mvn clean package assembly:assembly -Dmaven.test.skip=true
