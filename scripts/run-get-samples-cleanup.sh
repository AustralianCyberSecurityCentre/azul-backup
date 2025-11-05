#!/bin/bash

# this script will retrieve testing data from azul dispatcher, so that backup program can be verified.
# check source code for more details

set -e
# Remove old binary to ensure running latest build (in case build fails.)
[ -e ./bin/get-samples-cleanup ] && rm ./bin/get-samples-cleanup
go build -v -tags static_all -o ./bin/get-samples-cleanup ./get-samples-cleanup/main.go

chmod +x ./bin/*

./bin/get-samples-cleanup

