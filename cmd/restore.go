package cmd

import (
	"context"
	"fmt"
	"slices"
	"sync"
	"time"

	bkupcom "github.com/AustralianCyberSecurityCentre/azul-backup.git/common"
	"github.com/AustralianCyberSecurityCentre/azul-backup.git/prom"
	restore "github.com/AustralianCyberSecurityCentre/azul-backup.git/restore"
	bedclient "github.com/AustralianCyberSecurityCentre/azul-bedrock/v12/gosrc/client"
	bedSet "github.com/AustralianCyberSecurityCentre/azul-bedrock/v12/gosrc/settings"
	"github.com/AustralianCyberSecurityCentre/azul-bedrock/v12/gosrc/store"
	"github.com/spf13/cobra"
)

const MAX_RETRIES = 5

// Maximum minutes to wait for prometheus to scrape the server after restore is completed.
const MAX_MIN_WAIT_FOR_PROM = 5

type Restore struct {
	ctx       context.Context
	CtxCancel context.CancelFunc
	LocalData *bkupcom.LocalData
}

func NewRestore() *Restore {
	st := bkupcom.Settings
	ctx, cancel := context.WithCancel(context.Background())
	fileCache := bkupcom.NewLocal(st.LocalData)
	return &Restore{ctx: ctx, CtxCancel: cancel, LocalData: fileCache}
}

// restartStreamRoutines recreates routines, channels and waitgroups for restoring streams
func (rt *Restore) restartStreamRoutines(
	restoreStream *restore.RestoreStreams, chStats chan map[string]*restore.RestoreStats,
) (*sync.WaitGroup, chan store.FileStorageObjectListInfo) {
	st := bkupcom.Settings
	streamsWg := sync.WaitGroup{}
	chToRestore := make(chan store.FileStorageObjectListInfo, st.StreamChannelSize)
	// start workers
	for i := 0; i < st.StreamRoutineCount; i++ {
		streamsWg.Add(1)
		go func() {
			var ok, fail int
			defer func() {
				// write to channel with results
				// this ignores any mislabeled source artifacts
				chStats <- map[string]*restore.RestoreStats{restoreStream.Source: {StreamsOk: ok, StreamsFail: fail}}
				// signal that the worker is done
				streamsWg.Done()
			}()
			for objInfo := range chToRestore {
				if ok%100 == 0 {
					chStats <- map[string]*restore.RestoreStats{restoreStream.Source: {StreamsOk: ok, StreamsFail: fail}}
					// reset stats
					ok = 0
					fail = 0
				}
				select {
				case <-rt.ctx.Done():
					// Application has been cancelled.
					bedSet.Logger.Info().Msgf("Stopped stream restore for source %s after restoring %d events", restoreStream.Source, ok)
					return
				default:
					wasUploaded, err := restoreStream.RestoreStreams(objInfo)
					if wasUploaded {
						ok += 1
					} else {
						fail += 1
					}
					if err != nil {
						// Can't carry on because of a transient failure (service unavailable) so kill the application.
						bedSet.Logger.Error().Err(err).Msgf("Fatal error during Streams restore")
						rt.CtxCancel()
						return
					}
				}
			}
		}()
	}
	return &streamsWg, chToRestore
}

func (rt *Restore) restoreStreams(
	streamStore store.FileStorage,
	dpStreamsClient bedclient.ClientInterface,
	backup_sources []string,
	chStats chan map[string]*restore.RestoreStats,
) error {
	// restore streams from each sources sequentially
	for _, source := range backup_sources {
		bedSet.Logger.Info().Msgf("streams - restoring for source %s", source)

		restoreStream := restore.NewRestoreStreams(dpStreamsClient, streamStore, source, MAX_RETRIES)
		startFromObjectWithKey, err := rt.LocalData.LastStreamKeyRestoredLoad(source)
		if err != nil {
			return err
		}

		// start the initial workers
		wgStreamsRestore, chToRestore := rt.restartStreamRoutines(restoreStream, chStats)

		// use checkpointing to save state every x binaries
		maxRestorePerCycle := 1000
		restoreCount := 0
		cycleCount := 0
		var objInfo store.FileStorageObjectListInfo
		for objInfo = range restoreStream.GetBucketIterator(rt.ctx, startFromObjectWithKey) {
			cycleCount += 1
			chToRestore <- objInfo
			if cycleCount >= maxRestorePerCycle {
				select {
				case <-rt.ctx.Done():
					// Application has been cancelled.
					bedSet.Logger.Info().Msgf("Stopping after restoring %d streams to source %s", restoreCount, source)
					break
				default:
					// close, wait for workers, mark checkpoint, then reopen and continue
					// close channel
					close(chToRestore)
					// wait for workers to complete
					wgStreamsRestore.Wait()
					// must checkpoint the last binary saved
					err = rt.LocalData.LastStreamKeyRestoredStash(source, objInfo.Key)
					if err != nil {
						bedSet.Logger.Warn().Err(err).Msgf("couldn't save the last backed up stream Key %s to filecache.", objInfo.Key)
					}
					restoreCount += cycleCount
					// restart workers until next checkpoint reached
					wgStreamsRestore, chToRestore = rt.restartStreamRoutines(restoreStream, chStats)
					cycleCount = 0
				}
			}
		}
		// close channel
		close(chToRestore)
		// wait for workers to complete
		wgStreamsRestore.Wait()
		select {
		case <-rt.ctx.Done():
			// Application has been cancelled.
			bedSet.Logger.Info().Msgf("Restore cancelled, stopping after restoring %d streams to source %s", restoreCount, source)
		default:
			// must checkpoint the last binary saved
			err = rt.LocalData.LastStreamKeyRestoredStash(source, objInfo.Key)
			if err != nil {
				bedSet.Logger.Warn().Err(err).Msgf("couldn't save the last backed up stream Key %s to filecache.", objInfo.Key)
			}
			restoreCount += cycleCount
			bedSet.Logger.Info().Msgf("streams - restored all streams to source %s", source)
			chStats <- map[string]*restore.RestoreStats{restoreStream.Source: {StreamsComplete: true}}
		}
	}
	return nil
}

func (rt *Restore) createEventRoutine(
	source string,
	modelOrAction string,
	eventStore store.FileStorage,
	dpclient bedclient.ClientInterface,
	chStats chan map[string]*restore.RestoreStats,
	wgEventsRestore *sync.WaitGroup,
) {
	defer func() {
		// signal that the worker is done
		wgEventsRestore.Done()
	}()
	var err error
	model, action := bkupcom.GetModelAndAction(source, modelOrAction)

	sourceEvent := fmt.Sprintf("%s/%s", source, modelOrAction)
	restoreEvents := restore.NewRestoreEvents(dpclient, eventStore, rt.LocalData, MAX_RETRIES, source, model, action)

	lastSuccesfulEvent, err := restoreEvents.LoadLastEventSavedKey()
	if err != nil {
		bedSet.Logger.Error().Err(err).Msgf("Failed to load event state for restore event %s. Stopping now.", sourceEvent)
		rt.CtxCancel()
		return
	}

	for _, prefix := range restoreEvents.GetSortedBucketPrefixesOldestToNewest() {
		for objInfo := range restoreEvents.Ev.List(rt.ctx, prefix, "") {
			select {
			case <-rt.ctx.Done():
				bedSet.Logger.Info().Msgf("stopped event restore for source %s", sourceEvent)
				return
			default:
				// Skip all keys until we get up to the one we successfully backed up last.
				// FUTURE use StartAfter with minio
				if lastSuccesfulEvent != "" {
					if lastSuccesfulEvent == objInfo.Key {
						lastSuccesfulEvent = ""
					}
					continue
				}
				resp, err := restoreEvents.RestoreEvent(objInfo)
				if err != nil {
					bedSet.Logger.Error().Err(err).Msgf("Cancelling restore for source %s", sourceEvent)
					rt.CtxCancel()
					return
				}
				if resp.TotalFailures > 0 {
					// print the bad messages to stdout
					// FUTURE should be put in LocalData storage for manual inspection
					bedSet.Logger.Warn().Msgf("Events were rejected by dispatcher (%d ok, %d bad) from %s", resp.TotalOk, resp.TotalFailures, objInfo.Key)
					for _, failure := range resp.Failures {
						bedSet.Logger.Warn().Msgf("%s\n%s", failure.Event, failure.Error)
					}
				}
				chStats <- map[string]*restore.RestoreStats{source: {EventsOk: resp.TotalOk, EventsFail: resp.TotalFailures}}
			}
		}
	}
	bedSet.Logger.Info().Msgf("events - restored all %s/%s events", source, modelOrAction)
	chStats <- map[string]*restore.RestoreStats{source: {EventTypesComplete: 1}}
}

// createEventRoutines restores events to dispatcher, prioritising system events
func (rt *Restore) createEventRoutines(
	eventStore store.FileStorage,
	dpEventClients map[string]map[string]bedclient.ClientInterface,
	chStats chan map[string]*restore.RestoreStats,
) *sync.WaitGroup {
	wgEventsRestore := sync.WaitGroup{}
	// restore system events - these have side-effects that delete or modify other events
	systemSource := "system"
	for event, dpclient := range dpEventClients[systemSource] {
		bedSet.Logger.Info().Msgf("events - restoring events for %s/%s", systemSource, event)
		wgEventsRestore.Add(1)
		go func() {
			rt.createEventRoutine(systemSource, event, eventStore, dpclient, chStats, &wgEventsRestore)
		}()
		// Count total number of event restore services running so when they are all done it can be verified.
		prom.RestoreTotalEventTypesForSource.WithLabelValues(systemSource).Inc()
	}

	// wait until system events are restored
	// FUTURE add minimum sleep time here for dispatcher.
	wgEventsRestore.Wait()

	// restore non-system events
	for source, eventClients := range dpEventClients {
		if source == systemSource {
			continue
		}
		for event, dpclient := range eventClients {
			bedSet.Logger.Info().Msgf("events - restoring events for %s/%s", source, event)
			wgEventsRestore.Add(1)
			go func() {
				rt.createEventRoutine(source, event, eventStore, dpclient, chStats, &wgEventsRestore)
			}()
			// Count total number of event restore services running so when they are all done it can be verified.
			prom.RestoreTotalEventTypesForSource.WithLabelValues(source).Inc()
		}

	}
	return &wgEventsRestore
}

// DoRestore restores all streams and then all events to dispatcher.
func (rt *Restore) DoRestore(
	streamStore store.FileStorage,
	eventStore store.FileStorage,
	dpStreamsClient bedclient.ClientInterface,
	dpEventClients map[string]map[string]bedclient.ClientInterface,
) map[string]*restore.RestoreStats {
	st := bkupcom.Settings
	stats := map[string]*restore.RestoreStats{}
	startTime := time.Now()
	defer restore.PrintRestoreState(startTime, stats)

	bedSet.Logger.Info().Msgf(
		"starting restore: type=%s backup_id=%q bucket_prefix=%q endpoint=%q region=%q sources=%d",
		st.RestoreType, st.BackupID, st.BucketNamePrefix, st.ExternalBackup.Endpoint, st.ExternalBackup.Region,
		len(dpEventClients),
	)

	// create stats monitor routine
	chStats := make(chan map[string]*restore.RestoreStats)
	wgStats := sync.WaitGroup{}
	wgStats.Add(1)
	go func() {
		defer wgStats.Done()
		for res := range chStats {
			for k, v := range res {
				stats[k].Add(v)
			}
		}
	}()

	// print status as we go
	go func() {
		// stats every minute
		ticker := time.NewTicker(time.Minute * 1)
		for {
			select {
			case <-rt.ctx.Done():
				return
			case <-ticker.C:
				restore.PrintRestoreState(startTime, stats)
			}
		}
	}()

	// gather needed source directories
	backup_sources := []string{}
	for source := range dpEventClients {
		backup_sources = append(backup_sources, source)
		stats[source] = &restore.RestoreStats{}
	}
	slices.Sort(backup_sources)
	rt.LocalData.CreateRestoreDirectories()

	// restore all streams
	if st.RestoreType == "all" || st.RestoreType == "streams" {
		// note that this blocks until all streams are restored
		err := rt.restoreStreams(streamStore, dpStreamsClient, backup_sources, chStats)
		if err != nil {
			bedSet.Logger.Error().Err(err).Msg("Failed to load stream state due to error. Stopping now.")
			rt.CtxCancel()
			return stats
		}
	}

	// restore all events
	if st.RestoreType == "all" || st.RestoreType == "events" {
		wgEventsRestore := rt.createEventRoutines(eventStore, dpEventClients, chStats)
		wgEventsRestore.Wait()
	}
	bedSet.Logger.Info().Msg("waiting for workers to finish")
	rt.CtxCancel()

	// ensure that all results are processed
	close(chStats)
	wgStats.Wait()
	return stats
}

var restoreCmd = &cobra.Command{
	Use:   "restore",
	Short: "Restore Azul events and streams Data from a backup S3 server.",
	Long:  `Restore Azul events and streams Data from a backup S3 server.`,
	Args:  cobra.NoArgs,
	Run: func(cmd *cobra.Command, args []string) {
		go prom.StartStandalonePromServer()
		st := bkupcom.Settings

		author := NewAuthor("restore-streams", "all", "all", st.BackupID)
		dpStreamsClient := bedclient.NewClient(st.DispatcherEvents, st.DispatcherEvents, *author, st.DeploymentKey)

		dpEventClients := prepareSources("restore", nil)

		// streams bucket creation
		streamStore := bkupcom.NewObjectS3Store(st)
		// events bucket creation
		eventStore := bkupcom.NewEventS3Store(st)
		rt := NewRestore()
		rt.DoRestore(streamStore, eventStore, dpStreamsClient, dpEventClients)

		// If prometheus has been scraping the restore process wait for it to scrape one last time before exiting.
		if prom.PrometheusScrapeCount > 0 {
			startTime := time.Now()
			startingPromCount := prom.PrometheusScrapeCount
			bedSet.Logger.Info().Msg("Giving prometheus a chance to scrape before shutting down.")
			for {
				time.Sleep(time.Duration(10) * time.Second)
				// Wait for at least one more prometheus scrape.
				if prom.PrometheusScrapeCount > startingPromCount {
					bedSet.Logger.Info().Msg("Prometheus successfully scraped metrics prior to shutdown.")
					// Give a small amount of time for scrape to occur.
					time.Sleep(time.Duration(1) * time.Second)
					break
				}
				// Give up if scrape took too long.
				if time.Since(startTime) > time.Duration(MAX_MIN_WAIT_FOR_PROM)*time.Minute {
					bedSet.Logger.Info().Msg("Prometheus took too long to scrape metrics.")
					break
				}
			}
		}
	},
}

func init() {
	rootCmd.AddCommand(restoreCmd)
}
