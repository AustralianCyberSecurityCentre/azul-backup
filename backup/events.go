package backup

import (
	"bytes"
	"fmt"
	"io"
	"time"

	bkupcom "github.com/AustralianCyberSecurityCentre/azul-backup.git/common"
	"github.com/AustralianCyberSecurityCentre/azul-bedrock/v13/gosrc/events"
	"github.com/AustralianCyberSecurityCentre/azul-bedrock/v13/gosrc/models"
	"github.com/AustralianCyberSecurityCentre/azul-bedrock/v13/gosrc/msginflight"
	bedSet "github.com/AustralianCyberSecurityCentre/azul-bedrock/v13/gosrc/settings"
	"github.com/AustralianCyberSecurityCentre/azul-bedrock/v13/gosrc/store"

	"github.com/AustralianCyberSecurityCentre/azul-backup.git/prom"
	bedclient "github.com/AustralianCyberSecurityCentre/azul-bedrock/v13/gosrc/client"
)

// If has been x minutes ignore the batch size and save anyway.
const minutesToWaitWithBatchBeforeSaving = 10

type BackupEvents struct {
	source         string
	modelAndAction string
	action         events.BinaryAction
	model          events.Model

	// Events stored on BackupEvents structure to ensure the Events aren't lost during errors.
	partialEvents       []*msginflight.MsgInFlight
	timeSinceLastUpdate time.Time

	dp         bedclient.ClientInterface
	ev         store.FileStorage
	objChannel chan bkupcom.StreamBackupRequest
	fileCache  *bkupcom.LocalData
}

func NewBackupEvents(
	dpclient bedclient.ClientInterface,
	evStore store.FileStorage,
	objChannel chan bkupcom.StreamBackupRequest,
	fileCache *bkupcom.LocalData,
	source string,
	model events.Model,
	action events.BinaryAction,
) *BackupEvents {
	if model != events.ModelBinary {
		action = "default"
	}
	return &BackupEvents{
		dp:                  dpclient,
		ev:                  evStore,
		objChannel:          objChannel,
		fileCache:           fileCache,
		source:              source,
		model:               model,
		action:              events.BinaryAction(action),
		modelAndAction:      string(model) + "/" + string(action),
		timeSinceLastUpdate: time.Now(),
	}
}

func (bke *BackupEvents) GetAuthorInfo() string {
	return fmt.Sprintf("%v-%v-%v", bke.source, bke.model, bke.action)
}

func (bke *BackupEvents) FetchEvents() ([]*msginflight.MsgInFlight, *models.EventResponseInfo, error) {
	st := bkupcom.Settings
	var bulk []byte
	var info *models.EventResponseInfo
	var err error

	for range st.RetryCount {
		if bke.model != events.ModelBinary {
			// non-binary model, no action
			bulk, info, err = bke.dp.GetEventsBytes(&bedclient.FetchEventsStruct{
				AvroFormat:      true,
				Model:           bke.model,
				Count:           st.EventBatchSize, // large amount of collected events
				Deadline:        30,                // wait some time for events
				IsTask:          false,             // not a task, do not track state or expect published statuses
				RequireHistoric: true,              // track ONLY historical data, not live
			})
		} else {
			// binary model
			bulk, info, err = bke.dp.GetEventsBytes(&bedclient.FetchEventsStruct{
				AvroFormat:      true,
				Model:           bke.model,                         // only backup binary docs
				Count:           st.EventBatchSize,                 // large amount of collected events
				Deadline:        30,                                // wait some time for events
				RequireHistoric: true,                              // track ONLY historical data, not live or expedite
				IsTask:          false,                             // not a task, do not track state or expect published statuses
				RequireSources:  []string{bke.source},              // back up a specific source
				RequireActions:  []events.BinaryAction{bke.action}, // back up a specific type of event
			})
		}
		if err == nil {
			break
		}
	}
	if err != nil {
		return nil, nil, err
	}
	// parse bulk
	msgs, err := msginflight.AvroBulkToMsgInFlights(bulk, bke.model)
	return msgs, info, err
}

/*Attempt a backup from cached events, if it succeeds clear the cache.*/
func (bke *BackupEvents) RecoverLostBackupFromDisk() (*BackupStats, error) {
	bulk, err := bke.fileCache.BackupEventsLoad(bke.source, bke.model.Str(), bke.action.Str())
	if err != nil {
		return nil, err
	}
	events, err := msginflight.AvroBulkToMsgInFlights(bulk, bke.model)
	if err != nil {
		return nil, err
	}
	ret, err := bke.innerBackupEvents(events)
	if err != nil {
		return nil, err
	}
	bke.fileCache.BackupEventsDelete(bke.source, bke.model.Str(), bke.action.Str())
	return ret, nil
}

/*Cache the current events to disk.*/
func (bke *BackupEvents) eventsToDisk(bulk []byte) error {
	return bke.fileCache.BackupEventsStash(bke.source, bke.model.Str(), bke.action.Str(), bulk)
}

func (bke *BackupEvents) innerBackupEvents(evs []*msginflight.MsgInFlight) (*BackupStats, error) {
	var err error
	// don't backup nothing
	if len(evs) == 0 {
		bke.timeSinceLastUpdate = time.Now()
		return &BackupStats{}, nil
	}
	numStreams := 0
	numInvalid := 0

	// backup any streams linked in event
	// iterate through events and request binary data get backed up.
	lastTimestamp := time.Time{}
	lastId := ""
	for _, tmp := range evs {
		err = tmp.Event.CheckValid()
		if err != nil {
			bedSet.Logger.Error().Err(err).Msgf("CheckValid failed to process an event in the backup %s with event \n%v", bke.GetAuthorInfo(), tmp.Event)
			prom.BackupEventsError.WithLabelValues(bke.source, bke.model.Str(), "CheckValid").Inc()
			numInvalid += 1
			continue
		}

		lastTimestamp = *tmp.Base.Timestamp
		lastId = *tmp.Base.KafkaKey

		// only get datastreams from binary events
		binary, ok := tmp.GetBinary()
		if !ok {
			continue
		}

		for _, d := range binary.Entity.Datastreams {
			bke.objChannel <- bkupcom.NewObjectBackupRequest(binary.KafkaKey, binary.Source.Name, d.Label, d.Sha256)
			numStreams += 1
		}
	}

	// Datetime is formatted for lexicographical order to allow oldest -> newest restore to occur.
	// This relies on the storage provider 'list' operation returning keys in lexicographical order.
	// Timestamp used is last document timestamp in the bundle, Id is used for uniqueness of the bundle name.
	// As we read from oldest to newest, this can be used to work out age off.
	// FUTURE implement a maintenance goroutine that deletes old events from s3
	bulk, err := msginflight.MsgInFlightsToAvroBulk(evs, bke.model)
	if err != nil {
		return nil, err
	}

	st := bkupcom.Settings

	// Forward events to dispatcher
	for range st.RetryCount {
		r := bytes.NewReader(bulk)
		// compress resource
		compressor, err := bkupcom.NewGzipCompressReader(r)
		if err != nil {
			return nil, err
		}
		compressorReadCloser := io.NopCloser(compressor)

		// move to remote s3 storage
		customId := fmt.Sprintf("%s-%d-%s", lastTimestamp.Format(time.RFC3339Nano), len(evs), lastId)
		err = bke.ev.Put(bke.source, bke.modelAndAction, customId, compressorReadCloser, -1)
		// exit on success
		if err == nil {
			break
		}
		bkupcom.SleepRandDuration(st.RetryAverageDelayMs)
	}
	if err != nil {
		return nil, err
	}

	bke.timeSinceLastUpdate = time.Now()

	return &BackupStats{EventsOk: len(evs) - numInvalid, EventsInvalid: numInvalid}, nil
}

/*
Perform a backup of the events of the BackupEvents source and event_type.

Events are batched up into large chunks to get better compression ratios.

Finalise is used when the plugin is terminating to ensure that memory is cleared.
*/
func (bke *BackupEvents) PerformBackup(finalise bool) (*BackupStats, error) {
	var err error
	events := []*msginflight.MsgInFlight{}

	// grab new events if we are not finalising
	if !finalise {
		events, _, err = bke.FetchEvents()
		if err != nil {
			// continue processing rather than aborting whole backup
			bedSet.Logger.Warn().Err(err).Msgf("events - author %s failed get events from dispatcher", bke.GetAuthorInfo())
			prom.BackupEventsError.WithLabelValues(bke.source, bke.modelAndAction, "FetchEvents").Inc()
			return &BackupStats{}, nil
		}
	}
	events = append(bke.partialEvents[:], events[:]...)

	// ignore size requirements and run the backup no matter what.
	if !finalise {
		st := bkupcom.Settings
		// Only want backup once there are at least y events or if has been x minutes.
		// Return early if this is not the case.
		if len(events) < st.EventBatchSize && time.Since(bke.timeSinceLastUpdate).Minutes() < float64(minutesToWaitWithBatchBeforeSaving) {
			bke.partialEvents = events
			return &BackupStats{}, nil
		}
	}

	// Clear the current events because they will be saved.
	bke.partialEvents = []*msginflight.MsgInFlight{}

	ret, err := bke.innerBackupEvents(events)
	if err != nil {
		// failed to backup events properly
		// convert list of events into bulk avro bytes
		bulk, err := msginflight.MsgInFlightsToAvroBulk(events, bke.model)
		err2 := bke.eventsToDisk(bulk)
		if err2 != nil {
			bedSet.Logger.Error().Err(err2).Msg("Data lost, could not backup events to filecache.")
		}
		return &BackupStats{EventsFail: len(events)}, err
	}
	return ret, nil
}
