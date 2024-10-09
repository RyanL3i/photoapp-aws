@echo off
REM
REM Windows BATCH script to build docker container
REM
@echo on
docker rmi project01
docker build -t project01 .
