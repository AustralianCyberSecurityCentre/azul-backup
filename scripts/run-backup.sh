#!/bin/bash
# Compile the azul-backup golang binary and execute it to backup the defined sources and events.

# raises an error code even though it works
read -r -d '' BK__SOURCES << EOM
{"sources": 
    {
    "assemblyline": {
        "exclude_from_backup": true
    },
    "incidents": {
        "exclude_from_backup": true
    },
    "reporting": {
        "exclude_from_backup": true
    },
    "samples": {
    },
    "tasking": {
    },
    "testing": {
        "exclude_from_backup": true
    },
    "virustotal": {
        "exclude_from_backup": true
    },
    "vthunts": {
        "exclude_from_backup": true
    },
    "watch": {
        "exclude_from_backup": true
    }
    }
}

EOM
export BK__SOURCES

set -e
# Remove old binary to ensure running latest build (in case build fails.)
[ -e ./bin/backup ] && rm ./bin/backup
go build -v -tags static_all -o ./bin/backup main.go

chmod +x ./bin/*

# Using dispatcher local minio for testing
export BK__EXTERNAL_BACKUP__ENDPOINT=localhost:9000
export BK__EXTERNAL_BACKUP__ACCESS_KEY=minio-root-user
export BK__EXTERNAL_BACKUP__SECRET_KEY=minio-root-password
export BK__EXTERNAL_BACKUP__SECURE=false

BK__BACKUP_ID="local1" ./bin/backup backup
