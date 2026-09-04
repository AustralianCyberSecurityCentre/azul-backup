package main

import (
	"encoding/json"
	"fmt"
	"os"

	bkupcom "github.com/AustralianCyberSecurityCentre/azul-backup.git/common"
	bedclient "github.com/AustralianCyberSecurityCentre/azul-bedrock/v13/gosrc/client"
	"github.com/AustralianCyberSecurityCentre/azul-bedrock/v13/gosrc/events"
	"github.com/AustralianCyberSecurityCentre/azul-bedrock/v13/gosrc/models"
	"github.com/AustralianCyberSecurityCentre/azul-bedrock/v13/gosrc/msginflight"
	bedSet "github.com/AustralianCyberSecurityCentre/azul-bedrock/v13/gosrc/settings"
)

const directoryPerms = 0755
const filePerms = 0766

// NewAuthor generates a backup/restore author
func NewAuthor(name string) *events.PluginEntity {
	return &events.PluginEntity{
		Name:        name,
		Version:     bkupcom.RECOVERY_VERSION,
		Contact:     "azul@asd.gov.au",
		Category:    "plugin",
		Description: "Backup critical Azul data.",
		Features:    []events.PluginEntityFeature{},
	}
}

func getEvents(source string, model events.Model, action events.BinaryAction, dp *bedclient.Client) ([]*msginflight.MsgInFlight, *models.EventResponseInfo, error) {
	st := bkupcom.Settings
	st.EventBatchSize = 100 // override batch size
	var bulk []byte
	var info *models.EventResponseInfo
	var err error
	if model != events.ModelBinary {
		// non-binary model, no action
		bulk, info, err = dp.GetEventsBytes(&bedclient.FetchEventsStruct{
			AvroFormat:      true,
			Model:           model,
			Count:           st.EventBatchSize, // large amount of collected events
			RequireHistoric: true,              // track ONLY historical data, not live, retry or expedite
			Deadline:        30,                // wait some time for events
			IsTask:          false,             // not a task, do not track state or expect published statuses
		})
	} else {
		// binary model
		bulk, info, err = dp.GetEventsBytes(&bedclient.FetchEventsStruct{
			AvroFormat:      true,
			Model:           model,                         // only backup binary docs
			Count:           st.EventBatchSize,             // large amount of collected events
			Deadline:        30,                            // wait some time for events
			RequireHistoric: true,                          // track ONLY historical data, not live, retry or expedite
			IsTask:          false,                         // not a task, do not track state or expect published statuses
			RequireSources:  []string{source},              // back up a specific source
			RequireActions:  []events.BinaryAction{action}, // back up a specific type of event
		})
	}
	if err != nil {
		return nil, nil, err
	}
	// parse bulk
	msgs, err := msginflight.AvroBulkToMsgInFlights(bulk, model)
	return msgs, info, err
}

// sample Azul events and streams to file for test development
func main() {
	st := bkupcom.Settings

	author := NewAuthor("recovery-get-samples-1")
	dp := bedclient.NewClient(st.DispatcherEvents, st.DispatcherStreams, *author, st.DeploymentKey)

	store := "./testdata/data/samples"
	err := os.MkdirAll(store, directoryPerms)
	if err != nil {
		panic(err)
	}

	mapthings := map[string][]string{
		"samples": {string(events.ActionExtracted), string(events.ActionSourced)},
		"testing": {string(events.ActionSourced)},
		"system":  {string(events.ModelDelete), string(events.ModelInsert)},
	}

	for source, actionsOrModels := range mapthings {
		for _, modelOrAction := range actionsOrModels {
			model, action := bkupcom.GetModelAndAction(source, modelOrAction)
			bedSet.Logger.Info().Msgf("start retrieve of events for %s-%s", source, modelOrAction)
			success := 0
			for success < 1 {
				events, info, err := getEvents(source, model, action, dp)
				if err != nil {
					bedSet.Logger.Error().Err(err).Msgf("failed to fetch events from dp, %v", err)
					continue
				}
				if info.Ready {
					success += 1
				} else {
					bedSet.Logger.Info().Msgf("topics not ready yet %s %s", source, modelOrAction)
					continue
				}
				bulk, err := msginflight.MsgInFlightsToAvroBulk(events, model)
				if err != nil {
					bedSet.Logger.Warn().Err(err).Msg("failed to bulk encode")
					continue
				}

				filename := fmt.Sprintf("%s/%s-%s-%s-%d.avro", store, source, model, action, len(events))

				// Convert all the event to Json and write them to a neighboring file for transparency.
				rawMsgsInFlight, err := json.Marshal(events)
				if err != nil {
					bedSet.Logger.Error().Err(err).Msg("failed to convert to JSON!")
					continue
				}
				err = os.WriteFile(filename+".json", rawMsgsInFlight, filePerms)
				if err != nil {
					bedSet.Logger.Error().Err(err).Msg("failed to write events to json file")
					continue
				}

				// store bulk events to disk as Avro
				err = os.WriteFile(filename, bulk, filePerms)
				if err != nil {
					bedSet.Logger.Error().Err(err).Msg("failed to fetch write file")
					continue
				}
				bedSet.Logger.Info().Msgf("wrote %s", filename)
			}
		}
	}
	bedSet.Logger.Info().Msg("finished")
}
