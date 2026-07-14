package common

import (
	"fmt"
	"strings"

	"github.com/AustralianCyberSecurityCentre/azul-backup.git/prom"
	"github.com/AustralianCyberSecurityCentre/azul-bedrock/v12/gosrc/store"
)

// splitBucketFromEndpoint splits a host style S3 endpoint that has
// the bucket in its hostname (e.g. "azul-backup-bucket.s3.ap-southeast-1.amazonaws.com")
// into the real bucket name and the plain regional endpoint
// ("azul-backup-bucket", "s3.ap-southeast-1.amazonaws.com").
func splitBucketFromEndpoint(endpoint string) (bucket, regionalEndpoint string) {
	host := strings.TrimPrefix(strings.TrimPrefix(endpoint, "https://"), "http://")
	if i := strings.Index(host, ".s3."); i > 0 {
		return host[:i], host[i+1:] // "azul-backup-bucket", "s3.<region>.amazonaws.com"
	}
	return "", endpoint
}

// newS3Store builds an S3 store for the given per-store bucket. When the backup
// bucket is embedded in the endpoint hostname, it addresses the real bucket via
// the regional endpoint and folds bucketOrFolder down to a key prefix so every
// operation - including List - works against the shared bucket.
func newS3Store(settings *RecoverySettings, bucketOrFolder string) (store.FileStorage, error) {
	endpoint := settings.ExternalBackup.Endpoint
	bucket := bucketOrFolder
	folder := ""

	if settings.ExternalBackup.BucketInEndpoint {
		realBucket, regionalEndpoint := splitBucketFromEndpoint(endpoint)
		if realBucket == "" {
			return nil, fmt.Errorf("bucket_in_endpoint is set but could not extract a bucket from endpoint %q", endpoint)
		}
		endpoint = regionalEndpoint
		bucket = realBucket
		folder = bucketOrFolder
	}

	inner, err := store.NewS3Store(
		endpoint,
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
