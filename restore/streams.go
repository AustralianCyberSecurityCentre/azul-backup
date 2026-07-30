package restore

import (
	"context"
	"fmt"
	"io"

	bkupcom "github.com/AustralianCyberSecurityCentre/azul-backup.git/common"
	bedclient "github.com/AustralianCyberSecurityCentre/azul-bedrock/v12/gosrc/client"
	"github.com/AustralianCyberSecurityCentre/azul-bedrock/v12/gosrc/events"
	bedSet "github.com/AustralianCyberSecurityCentre/azul-bedrock/v12/gosrc/settings"
	"github.com/AustralianCyberSecurityCentre/azul-bedrock/v12/gosrc/store"
)

type RestoreStreams struct {
	dp         bedclient.ClientInterface
	obj        store.FileStorage
	Source     string
	maxRetries int
}

func NewRestoreStreams(dpclient bedclient.ClientInterface, objStore store.FileStorage, source string, maxRetries int) *RestoreStreams {
	return &RestoreStreams{dp: dpclient, obj: objStore, Source: source, maxRetries: maxRetries}
}

func (rs *RestoreStreams) GetBucketIterator(ctx context.Context, startingKey string) <-chan store.FileStorageObjectListInfo {
	return rs.obj.List(ctx, rs.Source+"/", startingKey)
}

/*Restore S3 raw binaries from the backup S3 to dispatcher.*/
func (rs *RestoreStreams) RestoreStreams(objInfo store.FileStorageObjectListInfo) (bool, error) {
	var err error
	var zipDecompressReader io.Reader
	// Retry downloading and uploading the object.
	dataSlice, err := rs.obj.Fetch(objInfo.Source, objInfo.Label, objInfo.Id)
	if err != nil {
		// Error connecting to S3
		return false, fmt.Errorf("s3 fetch: %w", err)
	}
	zipDecompressReader, err = bkupcom.NewGzipDecompressReaderAsReader(dataSlice.DataReader)
	if err != nil {
		// Malformed data can't be decompressed by Gzip.
		bedSet.Logger.Warn().Err(err).Msgf("stream to restore was invalid gzip: %s", objInfo.Key)
		return false, nil
	}

	if objInfo.Source != rs.Source {
		bedSet.Logger.Warn().Msgf("resource source in key does not match worker: %s but worker has %s", objInfo.Key, rs.Source)
	}

	// Prefer using source and label information extracted from key.
	// Skip the identify step to speed up restore operations.
	resp, err := rs.dp.PostStream(
		objInfo.Source,
		events.DatastreamLabel(objInfo.Label),
		zipDecompressReader,
		&bedclient.PostStreamStruct{SkipIdentify: true, ExpectedSha256: objInfo.Id},
	)
	if err != nil {
		// Error contacting dispatcher / dispatcher can't process the file.
		return false, fmt.Errorf("UploadBinaryAny: %w", err)
	}

	if resp.Sha256 != objInfo.Id {
		bedSet.Logger.Warn().Msgf("dp calculated sha256 does not match stored '%s' '%s' - '%s' but dispatcher calculated '%s'", objInfo.Source, objInfo.Label, objInfo.Id, resp.Sha256)
	}
	return true, nil
}
