package common

import (
	"fmt"

	"github.com/AustralianCyberSecurityCentre/azul-bedrock/v11/gosrc/events"
)

/*
Holds a request to download a specific binary stream from Dispatcher.
Used in the channel between events backup and streams backup.
*/
type StreamBackupRequest struct {
	// Original event ID that is requesting to backup the stream in S3
	EventId string
	// information required to download the sha256 from dispatcher and save it in backup s3.
	Source string
	Label  events.DatastreamLabel
	Sha256 string
}

func (obr *StreamBackupRequest) GetDestS3Path() string {
	return fmt.Sprintf("%s/%s/%s", obr.Source, obr.Label, obr.Sha256)
}

func NewObjectBackupRequest(eid string, source string, label events.DatastreamLabel, sha256 string) StreamBackupRequest {
	return StreamBackupRequest{
		EventId: eid,
		Source:  source,
		Label:   label,
		Sha256:  sha256,
	}
}
