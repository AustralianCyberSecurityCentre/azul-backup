package restore

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"os"
	"strings"

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
	proxiedChannel := make(chan store.FileStorageObjectListInfo)
	go func() {
		defer func() { close(proxiedChannel) }()
		parentChannel := rs.obj.List(ctx, rs.Source+"/", startingKey)
		var parentObject store.FileStorageObjectListInfo
		var ok bool
		for {
			// Get parent object.
			select {
			case <-ctx.Done():
				return
			case parentObject, ok = <-parentChannel:
				// The source channel is closed so exit.
				if !ok {
					return
				}
			}

			// Remove any extensions e.g XOR or AES attached to the sha256 (Id).
			result := strings.Split(parentObject.Id, ".")
			parentObject.Id = result[0]
			// Feed it into new channel.
			select {
			case <-ctx.Done():
				return
			case proxiedChannel <- parentObject:
				continue
			}
		}
	}()

	return proxiedChannel
}

func (rs *RestoreStreams) GetLocalBucketIterator(ctx context.Context, startingKey string, filePath string) <-chan store.FileStorageObjectListInfo {
	proxiedChannel := make(chan store.FileStorageObjectListInfo)
	go func() {
		defer func() { close(proxiedChannel) }()
		file, err := os.Open(filePath)
		if err != nil {
			bedSet.Logger.Fatal().Msgf("Could not find the S3 listing file %s", filePath)
			panic("Listing file not found")
		}
		defer file.Close()

		scanner := bufio.NewScanner(file)
		startingKeyFound := false
		for scanner.Scan() {
			line := scanner.Text()
			line = strings.TrimSpace(line)
			if startingKey != "" && !startingKeyFound {
				if startingKey == line {
					startingKeyFound = true
				} else {
					continue
				}
			}
			splitKey := strings.Split(line, "/")
			if len(splitKey) != 3 {
				bedSet.Logger.Fatal().Msgf("Failed to read interpret line: %s", line)
				panic("FAILED to read line")
			}

			var nextObject = store.FileStorageObjectListInfo{
				Key:    line,
				Source: splitKey[0],
				Label:  splitKey[1],
				Id:     splitKey[2],
				Err:    nil,
			}
			// Remove any extensions e.g XOR or AES attached to the sha256 (Id).
			cleanedId := strings.Split(nextObject.Id, ".")
			nextObject.Id = cleanedId[0]
			// Feed it into new channel.
			select {
			case <-ctx.Done():
				return
			case proxiedChannel <- nextObject:
				continue
			}
		}
		if err := scanner.Err(); err != nil {
			bedSet.Logger.Fatal().Msgf("Failure occurred when reading from file listing with error %s", err)
		}
	}()

	return proxiedChannel
}

/*Restore S3 raw binaries from the backup S3 to dispatcher.*/
func (rs *RestoreStreams) RestoreStream(objInfo store.FileStorageObjectListInfo, restoreSkipExistingStreams bool, ignoreRestoreNotFound bool) (bool, error) {
	var err error

	if restoreSkipExistingStreams {
		// Skip restore if the file already exists
		exists, err := rs.obj.Exists(objInfo.Source, objInfo.Label, objInfo.Id)
		if err == nil && exists {
			return true, nil
		}
	}

	var zipDecompressReader io.Reader
	// Retry downloading and uploading the object.
	dataSlice, err := rs.obj.Fetch(objInfo.Source, objInfo.Label, objInfo.Id)
	if err != nil {
		if ignoreRestoreNotFound {
			bedSet.Logger.Info().Msgf("Skipping file that could not be found in backup %s/%s/%s", objInfo.Source, objInfo.Label, objInfo.Id)
			return true, nil
		}
		// Error connecting to S3
		return false, fmt.Errorf("s3 fetch on file %s/%s/%s: %w", objInfo.Source, objInfo.Label, objInfo.Id, err)
	}
	defer dataSlice.DataReader.Close()

	zipDecompressReader, err = bkupcom.NewGzipDecompressReaderAsReader(dataSlice.DataReader)
	if err != nil {
		// Malformed data can't be decompressed by Gzip.
		bedSet.Logger.Warn().Err(err).Msgf("stream to restore was invalid gzip: %s/%s/%s", objInfo.Source, objInfo.Label, objInfo.Id)
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
