package backup

import (
	"errors"
	"fmt"
	"io"
	"log"

	bkupcom "github.com/AustralianCyberSecurityCentre/azul-backup.git/common"
	"github.com/AustralianCyberSecurityCentre/azul-backup.git/store"
	bedclient "github.com/AustralianCyberSecurityCentre/azul-bedrock/v9/gosrc/client"
)

type BackupStreams struct {
	dp         bedclient.ClientInterface
	obj        store.StoreS3Interface
	objChannel *chan bkupcom.StreamBackupRequest
}

func NewBackupStreams(dpclient bedclient.ClientInterface, objStore store.StoreS3Interface, objChannel *chan bkupcom.StreamBackupRequest) *BackupStreams {
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

	// move to remote s3 storage
	compressed, err := io.ReadAll(compressor)
	if err != nil {
		log.Printf("WARNING - compression failed with error %s", err)
		return false, err
	}

	// add to remove s3 storage
	err = bks.obj.Put(key, compressed)
	if err != nil {
		return false, err
	}
	return true, nil
}
