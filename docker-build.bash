#!/bin/bash
#
# Linux/Mac BASH script to build docker container
#
docker rmi project01
docker build -t project01 .
