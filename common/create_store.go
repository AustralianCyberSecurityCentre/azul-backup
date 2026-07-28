package common

import (
	"fmt"

	bedSet "github.com/AustralianCyberSecurityCentre/azul-bedrock/v12/gosrc/settings"

	"github.com/AustralianCyberSecurityCentre/azul-backup.git/prom"
	"github.com/AustralianCyberSecurityCentre/azul-bedrock/v12/gosrc/store"
)

// newS3Store builds an S3 store for one data type. dedicatedBucket is the bucket
// name used when each data type has its own bucket (the default). When
// ExternalBackup.Bucket is set, all data shares that single bucket and,
// dedicatedBucket is folded down to a key prefix instead.
func newS3Store(settings *RecoverySettings, dedicatedBucket string) (store.FileStorage, error) {
	bucket := dedicatedBucket
	folder := ""
	if settings.ExternalBackup.Bucket != "" {
		bucket = settings.ExternalBackup.Bucket
		folder = dedicatedBucket
	}

	inner, err := store.NewS3Store(
		settings.ExternalBackup.Endpoint,
		settings.ExternalBackup.AccessKey,
		settings.ExternalBackup.SecretKey,
		settings.ExternalBackup.Secure,
		bucket,
		settings.ExternalBackup.Region,
		prom.StreamsOperationDuration,
		store.AutomaticAgeOffSettings{
			SourceConf:              &settings.Sources,
			EnableAutomaticAgeOff:   settings.EnableAutomaticAgeOff,
			EnableCleanupAutoAgeOff: settings.EnableCleanupAutoAgeOff,
		},
	)
	if err != nil {
		return nil, err
	}

	switch(settings.ExternalBackup.AesState){
	case AesStateEnabled:
		inner = store.NewAESCtrStore(inner, settings.ExternalBackup.AesKey, true)
		bedSet.Logger.Info().Msg("AES encryption is enabled and AES is being used for reading and writing files.")
	case AesStateReadOnly:
		inner = store.NewAESCtrStore(inner, settings.ExternalBackup.AesKey, false)
		bedSet.Logger.Info().Msg("AES encryption is being read but no new encryption is being done.")
	case AesStateDisabled:
	}

	// NewFolderPrefixStore returns inner unchanged when folder is empty.
	return NewFolderPrefixStore(inner, folder), nil
}

func NewObjectS3Store(settings *RecoverySettings) store.FileStorage {
	streamBucket := fmt.Sprintf("%s%s-streams", settings.BucketNamePrefix, settings.BackupID)
	streamStore, err := newS3Store(settings, streamBucket)
	if err != nil {
		panic(err)
	}
	return streamStore
}

func NewEventS3Store(settings *RecoverySettings) store.FileStorage {
	eventBucket := fmt.Sprintf("%s%s-events", settings.BucketNamePrefix, settings.BackupID)
	eventStore, err := newS3Store(settings, eventBucket)
	if err != nil {
		panic(err)
	}
	return eventStore
}
