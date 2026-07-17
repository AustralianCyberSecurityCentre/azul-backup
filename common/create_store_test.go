package common

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/AustralianCyberSecurityCentre/azul-bedrock/v12/gosrc/store"
	"github.com/stretchr/testify/require"
)

// s3StubSettings starts an S3 stub that answers the BucketExists probe NewS3Store
// makes, and returns settings pointed at it plus a pointer to the bucket that
// probe asked for.
func s3StubSettings(t *testing.T) (*RecoverySettings, *string) {
	t.Helper()

	var probedBucket string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		probedBucket = r.URL.Path
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(srv.Close)

	return &RecoverySettings{
		ExternalBackup: StreamsS3Settings{
			Endpoint:  srv.Listener.Addr().String(),
			AccessKey: "access",
			SecretKey: "secret",
			Region:    "us-east-1",
		},
	}, &probedBucket
}

func TestNewS3StoreUsesDedicatedBucketWhenNoSharedBucketSet(t *testing.T) {
	settings, probedBucket := s3StubSettings(t)

	s, err := newS3Store(settings, "azul-backup-1-streams")
	require.NoError(t, err)

	require.Equal(t, "/azul-backup-1-streams/", *probedBucket)
	_, prefixed := s.(*FolderPrefixStore)
	require.False(t, prefixed, "dedicated bucket should not be folded under a folder")
}

func TestNewS3StoreFoldsDedicatedBucketIntoFolderWhenSharedBucketSet(t *testing.T) {
	settings, probedBucket := s3StubSettings(t)
	settings.ExternalBackup.Bucket = "azul-backup-bucket"

	s, err := newS3Store(settings, "azul-backup-1-streams")
	require.NoError(t, err)

	require.Equal(t, "/azul-backup-bucket/", *probedBucket)
	prefixed, ok := s.(*FolderPrefixStore)
	require.True(t, ok, "shared bucket should fold the data type down to a folder")
	require.Equal(t, "azul-backup-1-streams/", prefixed.prefix)
}

var _ store.FileStorage = (*FolderPrefixStore)(nil)
