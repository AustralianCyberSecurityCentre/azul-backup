package backup

import (
	"errors"
	"fmt"
	"io"

	bkupcom "github.com/AustralianCyberSecurityCentre/azul-backup.git/common"
	bedclient "github.com/AustralianCyberSecurityCentre/azul-bedrock/v9/gosrc/client"
	"github.com/AustralianCyberSecurityCentre/azul-bedrock/v9/gosrc/store"
)

type BackupStreams struct {
	dp         bedclient.ClientInterface
	obj        store.FileStorage
	objChannel *chan bkupcom.StreamBackupRequest
}

func NewBackupStreams(dpclient bedclient.ClientInterface, objStore store.FileStorage, objChannel *chan bkupcom.StreamBackupRequest) *BackupStreams {
	return &BackupStreams{dp: dpclient, obj: objStore, objChannel: objChannel}
}

/*Backup a S3 raw binary file from dispatcher to the backup server.*/
func (bks *BackupStreams) BackupStream(obr *bkupcom.StreamBackupRequest) (bool, error) {
	key := obr.GetDestS3Path()
	raw, err := bks.dp.DownloadBinary(obr.Source, obr.Label, obr.Sha256)
	if err != nil {
		var httpError *bedclient.HttpError
		if errors.As(err, &httpError) && httpError.StatusCode == 404 {
			return false, nil
		}
		return false, fmt.Errorf("failed to download the file %v because of an unexpected error: %v", key, err)
	}
	// compress resource
	compressor := bkupcom.NewGzipCompressReader(raw)
	copressorReadCloser := io.NopCloser(compressor)
	// add to remove s3 storage
	err = bks.obj.Put(obr.Source, obr.Label.Str(), obr.Sha256, copressorReadCloser, -1)
	if err != nil {
		return false, err
	}
	return true, nil
}
