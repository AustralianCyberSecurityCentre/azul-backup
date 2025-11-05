#!/bin/bash
# Compile the azul-backup golang binary and execute it to restore the defined sources and events.

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

time BK__BACKUP_ID="local1" BK__RESTORE_TYPE="all" ./bin/backup restore

rm /tmp/azbackup/restoreevents -rf
rm /tmp/azbackup/restorestreams -rf