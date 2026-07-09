package common

import (
	"fmt"

	"github.com/AustralianCyberSecurityCentre/azul-backup.git/prom"
	"github.com/AustralianCyberSecurityCentre/azul-bedrock/v12/gosrc/store"
)

func NewObjectS3Store(settings *RecoverySettings) store.FileStorage {
	streamBucket := fmt.Sprintf("%s%s-streams", settings.BucketNamePrefix, settings.BackupID)
	streamStore, err := store.NewS3Store(settings.ExternalBackup.Endpoint, settings.ExternalBackup.AccessKey, settings.ExternalBackup.SecretKey, settings.ExternalBackup.Secure, streamBucket, settings.ExternalBackup.Region, prom.StreamsOperationDuration, store.AutomaticAgeOffSettings{
		SourceConf:              &settings.Sources,
		EnableAutomaticAgeOff:   settings.EnableAutomaticAgeOff,
		EnableCleanupAutoAgeOff: settings.EnableCleanupAutoAgeOff,
	})
	if err != nil {
		panic(err)
	}
	if settings.ExternalBackup.BucketInEndpoint {
		// The bucket lives in the endpoint hostname, so streamBucket is a folder
		// (key prefix) inside it - make listing look under that folder.
		return NewFolderPrefixStore(streamStore, streamBucket)
	}
	return streamStore
}

func NewEventS3Store(settings *RecoverySettings) store.FileStorage {
	eventBucket := fmt.Sprintf("%s%s-events", settings.BucketNamePrefix, settings.BackupID)
	eventStore, err := store.NewS3Store(settings.ExternalBackup.Endpoint, settings.ExternalBackup.AccessKey, settings.ExternalBackup.SecretKey, settings.ExternalBackup.Secure, eventBucket, settings.ExternalBackup.Region, prom.StreamsOperationDuration, store.AutomaticAgeOffSettings{
		SourceConf:              &settings.Sources,
		EnableAutomaticAgeOff:   settings.EnableAutomaticAgeOff,
		EnableCleanupAutoAgeOff: settings.EnableCleanupAutoAgeOff,
	})
	if err != nil {
		panic(err)
	}
	if settings.ExternalBackup.BucketInEndpoint {
		// The bucket lives in the endpoint hostname, so eventBucket is a folder
		// (key prefix) inside it - make listing look under that folder.
		return NewFolderPrefixStore(eventStore, eventBucket)
	}
	return eventStore
}
