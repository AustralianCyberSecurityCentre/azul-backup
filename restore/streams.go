package restore

import (
	"bytes"
	"compress/gzip"
	"fmt"
	"log"
	"strings"

	"github.com/AustralianCyberSecurityCentre/azul-bedrock/v9/gosrc/events"

	"github.com/AustralianCyberSecurityCentre/azul-backup.git/store"
	bedclient "github.com/AustralianCyberSecurityCentre/azul-bedrock/v9/gosrc/client"
	"github.com/minio/minio-go/v7"
)

type RestoreStreams struct {
	dp         bedclient.ClientInterface
	obj        store.StoreS3Interface
	Source     string
	maxRetries int
}

func NewRestoreStreams(dpclient bedclient.ClientInterface, objStore store.StoreS3Interface, source string, maxRetries int) *RestoreStreams {
	return &RestoreStreams{dp: dpclient, obj: objStore, Source: source, maxRetries: maxRetries}
}

func (rs *RestoreStreams) GetBucketIterator(startingKey string) <-chan minio.ObjectInfo {
	settings := minio.ListObjectsOptions{Prefix: rs.Source + "/", Recursive: true, WithMetadata: false, WithVersions: false}
	if startingKey != "" {
		settings.StartAfter = startingKey
		log.Printf("streams - resuming operation after '%s'", startingKey)
	}
	return rs.obj.List(settings)
}

/*Restore S3 raw binaries from the backup S3 to dispatcher.*/
func (rs *RestoreStreams) RestoreStreams(objInfo minio.ObjectInfo) (bool, error) {
	// Assumes format is source/label/sha256
	objectKeyParts := strings.Split(objInfo.Key, "/")
	// Unexpected object key, ignore the file because it can't be restored.
	if len(objectKeyParts) != 3 {
		log.Printf("WARNING - key %s for source %s is not in the expected format 'source/label/sha256'.", objInfo.Key, rs.Source)
		return false, fmt.Errorf("the object key %s for source %s is not in the expected format 'source/label/sha256'", objInfo.Key, rs.Source)
	}
	source := objectKeyParts[0]
	label := objectKeyParts[1]
	sha256 := objectKeyParts[2]

	var err error
	var zipDecompressReader *gzip.Reader
	// Retry downloading and uploading the object.
	var data []byte
	data, err = rs.obj.Fetch(objInfo.Key)
	if err != nil {
		// Error connecting to S3
		return false, fmt.Errorf("s3 fetch: %w", err)
	}
	reader := bytes.NewReader([]byte(data))
	zipDecompressReader, err = gzip.NewReader(reader)
	if err != nil {
		// Malformed data can't be decompressed by Gzip.
		log.Printf("WARNING - stream to restore was invalid gzip: %s", objInfo.Key)
		return false, nil
	}

	if source != rs.Source {
		log.Printf("WARNING - resource source in key does not match worker: %s but worker has %s", objInfo.Key, rs.Source)
	}

	// Prefer using source and label information extracted from key.
	// Skip the identify step to speed up restore operations.
	resp, err := rs.dp.PostStream(
		source,
		events.DatastreamLabel(label),
		zipDecompressReader,
		&bedclient.PostStreamStruct{SkipIdentify: true, ExpectedSha256: sha256},
	)
	if err != nil {
		// Error contacting dispatcher / dispatcher can't process the file.
		return false, fmt.Errorf("UploadBinaryAny: %w", err)
	}

	if resp.Sha256 != sha256 {
		log.Printf("WARNING - dp calculated sha256 does not match stored '%s' '%s' - '%s' but dispatcher calculated '%s'", source, label, sha256, resp.Sha256)
	}
	return true, nil
}
