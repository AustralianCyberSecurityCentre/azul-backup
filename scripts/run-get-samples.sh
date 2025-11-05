#!/bin/bash

# this script will retrieve testing data from azul dispatcher, so that backup program can be verified.
# check source code for more details

set -e
# Remove old binary to ensure running latest build (in case build fails.)
[ -e ./bin/get-samples ] && rm ./bin/get-samples
go build -v -tags static_all -o ./bin/get-samples ./get-samples/main.go

chmod +x ./bin/*

# Add dispatcher URL environment variable to enable sample collection.
#BK__DISPATCHER_STREAMS="https://dispatcher.internal" BK__DISPATCHER_EVENTS="https://dispatcher.internal"
BK__BACKUP_ID="local1" ./bin/get-samples

