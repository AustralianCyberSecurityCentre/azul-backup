package cmd

import (
	"context"
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/AustralianCyberSecurityCentre/azul-backup.git/backup"
	bkupcom "github.com/AustralianCyberSecurityCentre/azul-backup.git/common"
	"github.com/AustralianCyberSecurityCentre/azul-backup.git/prom"
	bedclient "github.com/AustralianCyberSecurityCentre/azul-bedrock/v13/gosrc/client"
	"github.com/AustralianCyberSecurityCentre/azul-bedrock/v13/gosrc/events"
	bedSet "github.com/AustralianCyberSecurityCentre/azul-bedrock/v13/gosrc/settings"
	"github.com/AustralianCyberSecurityCentre/azul-bedrock/v13/gosrc/store"
	lru "github.com/hashicorp/golang-lru/v2"
	"github.com/spf13/cobra"
)

// Keyword label to indicate an error occurred that resulted in a stream failing to even be saved to disk.
// This happens if a hash can't be uploaded to S3 and it's S3 path also couldn't be saved to disk.
const BACKUP_STREAM_STASH_LABEL_VALUE = "BackupStreamStashAppend1"

const MANUAL_BACKUP_SIMPLE_FAILED_FILES_RESTORE_FLAG = "include-failed-simple"
const MANUAL_BACKUP_FAILED_FILES_RESTORE_FLAG = "not-failed-complex"
const CLEAR_FAILED_FILES_FLAG = "confirm"

type ReattemptBackupStream string

// All enums added here need to be added to Python's exception_enums.py as well.
const (
	ReattemptBackupStreamSimpleFails  = ReattemptBackupStream("simple-fails")
	ReattemptBackupStreamComplexFails = ReattemptBackupStream("complex-fails")
	ReattemptBackupStreamBoth         = ReattemptBackupStream("both")
)

type Backup struct {
	ctxEvents           context.Context
	CtxEventsCancel     context.CancelFunc
	ctxStreams          context.Context
	CtxStreamsCancel    context.CancelFunc
	CtxSigWatcher       context.Context
	CtxSigWatcherCancel context.CancelFunc
	// Used to trigger the fastest shutdown possible.
	RapidShutdown bool

	LocalData *bkupcom.LocalData
}

func NewBackup() *Backup {
	st := bkupcom.Settings
	fileCache := bkupcom.NewLocal(st.LocalData)

	ctxEvents, ctxEventsCancel := context.WithCancel(context.Background())
	ctxStreams, ctxStreamsCancel := context.WithCancel(context.Background())
	ctxSigWatcher, ctxSigWatcherCancel := context.WithCancel(context.Background())
	return &Backup{
		ctxEvents:           ctxEvents,
		CtxEventsCancel:     ctxEventsCancel,
		ctxStreams:          ctxStreams,
		CtxStreamsCancel:    ctxStreamsCancel,
		LocalData:           fileCache,
		CtxSigWatcher:       ctxSigWatcher,
		CtxSigWatcherCancel: ctxSigWatcherCancel,
		RapidShutdown:       false,
	}
}

// createReattemptRoutines reload all of the previously failed streams
// and put them back into the bkupObject channel ready to be backed up.
func (bk *Backup) createReattemptRoutines(
	chBackupStreams chan bkupcom.StreamBackupRequest,
	chBackupStreamsDeduped chan bkupcom.StreamBackupRequest,
	reAttemptStreamChoice ReattemptBackupStream,
) *sync.WaitGroup {
	var wgReattempt sync.WaitGroup
	wgReattempt.Add(1)
	go func() {
		defer wgReattempt.Done()
		var previouslyFailedStreams []bkupcom.StreamBackupRequest
		var err error
		if reAttemptStreamChoice == ReattemptBackupStreamSimpleFails || reAttemptStreamChoice == ReattemptBackupStreamBoth {
			previouslyFailedStreams, err = bk.LocalData.BackupStreamLoad()
			if err != nil {
				// Failed to load stream data, crash the application.
				bk.CtxEventsCancel()
				bk.CtxStreamsCancel()
				log.Fatalf("Failed to load")
				return
			}
		}
		if reAttemptStreamChoice == ReattemptBackupStreamComplexFails || reAttemptStreamChoice == ReattemptBackupStreamBoth {
			additionalFails, err := bk.LocalData.FailedBackupStreamLoad()
			if err != nil {
				// Failed to load stream data, crash the application.
				bk.CtxEventsCancel()
				bk.CtxStreamsCancel()
				log.Fatalf("Failed to load failed streams")
				return
			}
			previouslyFailedStreams = append(previouslyFailedStreams, additionalFails...)
		}
		// Delete old backup streams file to prevent multiple copies being appended to the same file.
		if reAttemptStreamChoice == ReattemptBackupStreamSimpleFails || reAttemptStreamChoice == ReattemptBackupStreamBoth {
			bk.LocalData.BackupStreamDelete()
		}
		if reAttemptStreamChoice == ReattemptBackupStreamComplexFails || reAttemptStreamChoice == ReattemptBackupStreamBoth {
			bk.LocalData.FailedBackupStreamDeleteFile()
		}
		for _, obr := range previouslyFailedStreams {
			chBackupStreams <- obr
		}
		// Verify that all the stream channels are emptied.
		MAX_SECONDS := 1000
		ticker := time.NewTicker(1 * time.Second)
		totalTicks := 0
		for range ticker.C {
			if totalTicks > MAX_SECONDS {
				return
			}
			totalTicks += 1
			if len(chBackupStreams) == 0 && len(chBackupStreamsDeduped) == 0 {
				bedSet.Logger.Info().Msg("Backup streams that needed re-attempts now complete!")
				return
			}
		}
	}()
	return &wgReattempt
}

// createDedupeRoutines continually look for new events to backup for this source/event combination.
func (bk *Backup) createDedupeRoutines(
	chBackupStreams chan bkupcom.StreamBackupRequest,
	chBackupStreamsDeduped chan bkupcom.StreamBackupRequest,
	ctx context.Context,
) *sync.WaitGroup {
	var wgCacheObjBkup sync.WaitGroup
	wgCacheObjBkup.Add(1)
	go func() {
		defer wgCacheObjBkup.Done()
		// FUTURE - smaller hitcache's per source/eventType may be substantially more useful.
		hitCache, err := lru.New[string, int](16000)
		if err != nil {
			log.Fatalf("Failed to startup hitcache for reason %v", err)
		}

		for {
			select {
			case streamFile := <-chBackupStreams: // Keep streaming files while they are in the channel.
				key := streamFile.GetDestS3Path()
				_, ok := hitCache.Get(key)
				if ok {
					continue
				}
				hitCache.Add(key, 0)
				chBackupStreamsDeduped <- streamFile
			case <-ctx.Done():
				// Keep processing until the buffered channel is empty.
				if len(chBackupStreams) > 0 {
					continue
				}
				return
			}
		}
	}()
	return &wgCacheObjBkup
}

// createStreamRoutines consumes jobs and backs up the corresponding streams
func (bk *Backup) createStreamRoutines(
	dpEventClients map[string]map[string]bedclient.ClientInterface,
	chBackupStreams chan bkupcom.StreamBackupRequest,
	backupStreams *backup.BackupStreams,
	chStats chan map[string]*backup.BackupStats,
) *sync.WaitGroup {
	st := bkupcom.Settings
	var wgObjBkup sync.WaitGroup
	for i := 0; i < st.StreamRoutineCount; i++ {
		wgObjBkup.Add(1)
		go func(workerId int) {
			var stats map[string]*backup.BackupStats
			defer func() {
				chStats <- stats
				// signal that the worker is done
				wgObjBkup.Done()
			}()
			consecutiveStreamFails := 0
			backupCount := 0
			var err error
			// Keep streaming files while they are in the channel.
			for {
				if backupCount%100 == 0 {
					chStats <- stats
					// reset stats
					stats = map[string]*backup.BackupStats{}
					for source := range dpEventClients {
						stats[source] = &backup.BackupStats{}
					}
				}
				select {
				case streamFile := <-chBackupStreams:
					backupCount += 1
					// Too many consecutive file errors, something is wrong so save files to disk and crash.
					if consecutiveStreamFails > st.ConsecutiveBackupStreamFailures {
						err := bk.LocalData.BackupStreamStashAppend(streamFile)
						if err != nil {
							bedSet.Logger.Warn().Err(err).Msgf("streams - failed to backup stream %s", streamFile.GetDestS3Path())
							prom.BackupObjectError.WithLabelValues(BACKUP_STREAM_STASH_LABEL_VALUE).Inc()
						}
						// Kill events, once they have stopped streams can stop.
						bk.CtxEventsCancel()
						continue
					}
					var backedUp bool
					// Retry backing up the stream a few times
					for range st.RetryCount {
						backedUp, err = backupStreams.BackupStream(&streamFile)
						if err == nil {
							break
						}
					}
					if err == nil {
						consecutiveStreamFails = 0
						if backedUp {
							stats[streamFile.Source].StreamsOk += 1
						} else {
							stats[streamFile.Source].StreamsMissing += 1
						}
					} else {
						stats[streamFile.Source].StreamsFail += 1
						bedSet.Logger.Error().Err(err).Msg("streams - backup streams failed.")
						consecutiveStreamFails += 1
						innerserr := bk.LocalData.BackupStreamStashAppend(streamFile)
						if innerserr != nil {
							bedSet.Logger.Error().Err(innerserr).Msgf("data lost failed to backup a stream with dest path %s", streamFile.GetDestS3Path())
							prom.BackupObjectError.WithLabelValues("BackupStreamStashAppend2").Inc()
						}
						// sleep in the hope that the issue will be resolved soon
						time.Sleep(time.Duration(1) * time.Second)
					}
				case <-bk.ctxStreams.Done(): // Exit if context is done.
					// Keep forwarding to S3 until the buffered channel is empty (unless a rapid shutdown is requested).
					if len(chBackupStreams) > 0 && !bk.RapidShutdown {
						continue
					}
					// Save all metadata of streams that couldn't be saved to S3 to local (useful for rapid shutdowns)
					for len(chBackupStreams) > 0 {
						select {
						case streamFile := <-chBackupStreams:
							err := bk.LocalData.BackupStreamStashAppend(streamFile)
							if err != nil {
								bedSet.Logger.Warn().Err(err).Msgf("streams - failed to cache stream during shutdown %s", streamFile.GetDestS3Path())
								prom.BackupObjectError.WithLabelValues(BACKUP_STREAM_STASH_LABEL_VALUE).Inc()
							}
						default:
						}
					}
					select {
					case <-bk.ctxEvents.Done():
						return
					default:
						// If streams has exited and events hasn't had it's context cancelled yet there is a problem.
						// Because events will keep backing up without a streams to handle the objects it's backing up.
						log.Panicf("streams - processing has finished before events this should never happen!")
					}
				}
			}
		}(i)
	}
	return &wgObjBkup
}

func (bk *Backup) createEventRoutines(
	eventStore store.FileStorage,
	dpEventClients map[string]map[string]bedclient.ClientInterface,
	chBackupStreams chan bkupcom.StreamBackupRequest,
	chStats chan map[string]*backup.BackupStats,
) *sync.WaitGroup {
	var wgEvents sync.WaitGroup

	for source, eventClients := range dpEventClients {
		for modelOrAction, dpclient := range eventClients {
			model, action := bkupcom.GetModelAndAction(source, modelOrAction)
			wgEvents.Add(1)
			// Continually look for new events to backup for this source/event combination.
			go func() {
				defer wgEvents.Done()
				// signal that the worker is done
				var err error
				bkevents := backup.NewBackupEvents(dpclient, eventStore, chBackupStreams, bk.LocalData, source, model, action)
				tmpStats, err := bkevents.RecoverLostBackupFromDisk()
				if err != nil {
					bedSet.Logger.Error().Err(err).Msgf("Failed to recover backup events from cache for source/event %s", bkevents.GetAuthorInfo())
					bk.CtxEventsCancel()
					return
				}
				chStats <- map[string]*backup.BackupStats{source: tmpStats}
				for {
					select {
					case <-bk.ctxEvents.Done():
						// Perform a backup on all events currently stored.
						tmpStats, eerr := bkevents.PerformBackup(true)
						if eerr != nil {
							bedSet.Logger.Error().Err(eerr).Msg("events - failed to perform forceful backup")
						}
						chStats <- map[string]*backup.BackupStats{source: tmpStats}
						return
					default:
						tmpStats, eerr := bkevents.PerformBackup(false)
						if eerr != nil {
							bedSet.Logger.Error().Err(eerr).Msgf("events - author %s failed, killing the plugin", bkevents.GetAuthorInfo())
							// Cancel all event collectors.
							bk.CtxEventsCancel()
						}
						chStats <- map[string]*backup.BackupStats{source: tmpStats}
					}
				}
			}()
		}
	}
	return &wgEvents
}

// do_backup will perform a backup with the provided dispatcher clients.
func (bk *Backup) DoBackup(
	streamStore store.FileStorage,
	eventStore store.FileStorage,
	dpStreamsClient bedclient.ClientInterface,
	dpEventClients map[string]map[string]bedclient.ClientInterface,
) map[string]*backup.BackupStats {
	st := bkupcom.Settings
	stats := map[string]*backup.BackupStats{}
	startTime := time.Now()
	defer backup.UpdateBackupStats(startTime, stats, true)

	chBackupStreams := make(chan bkupcom.StreamBackupRequest, st.StreamChannelSize)
	chBackupStreamsDeduped := make(chan bkupcom.StreamBackupRequest, st.StreamChannelSize)
	chStats := make(chan map[string]*backup.BackupStats)

	// gather needed source directories
	for source := range dpEventClients {
		stats[source] = &backup.BackupStats{}
	}

	// create stats monitor routine
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
		// print output for the first 30 minutes and then reduce printing to once an hour.
		TICKER_MAX_FAST_PRINTS := 31
		tickerCount := 0
		TICKER_FREQUENCY := 60
		tickerFast := time.NewTicker(time.Minute * 1)
		for {
			select {
			case <-bk.ctxStreams.Done():
				return
			case <-tickerFast.C:
				tickerCount += 1
				if tickerCount == TICKER_MAX_FAST_PRINTS {
					bedSet.Logger.Info().Msg("reducing stat printing frequency to every hour")
				}
				if tickerCount < TICKER_MAX_FAST_PRINTS {
					// Print + stats
					backup.UpdateBackupStats(startTime, stats, true)
				} else if (tickerCount % TICKER_FREQUENCY) == 0 {
					// Print + stats
					backup.UpdateBackupStats(startTime, stats, true)
				} else {
					// Stats only
					backup.UpdateBackupStats(startTime, stats, false)
				}
			}
		}
	}()

	// create SIGINT capture routine
	// Based on the Kubernetes shutdown process https://medium.com/@meng.yan/what-happens-when-deleting-a-pod-d1219c7e1b53
	// Sigterm sent, then after 30seconds sigkill is sent.
	// This exits after giving at most 45seconds to save all data before giving up with a forceful shutdown and data loss.
	go func() {
		sigExitHappened := false
		chSignals := make(chan os.Signal, 10)
		signal.Notify(chSignals, os.Interrupt, syscall.SIGINT, syscall.SIGTERM)
		for {
			select {
			case <-chSignals:
				bk.RapidShutdown = true
				// First Sigterm with grace period of 30seconds from kubernetes sent.
				if !sigExitHappened {
					bedSet.Logger.Info().Msg("Cancel or kill called, ensuring all workers terminate without data loss")
					// do last actions and wait for all write operations to end
					bk.CtxEventsCancel()
					sigExitHappened = true
				} else {
					// SigKill has been sent after that 30 second grace period.
					// Wait another 15seconds and then shutdown with data loss.
					maxWaitAfterSigkill := time.NewTicker(time.Duration(15) * time.Second)
					select {
					case <-bk.CtxSigWatcher.Done():
						// Yay gracefully exited in time!
						return
					case <-maxWaitAfterSigkill.C:
						bedSet.Logger.Warn().Msg("Cancel or kill called again, terminating immediately (data loss)")
						os.Exit(99)
					}
				}
			case <-bk.CtxSigWatcher.Done(): // Top listening for signals once everything else is finished.
				return
			}
		}
	}()

	bk.LocalData.CreateBackupDirectories()

	cacheObjBkupContext, cacheObjBkupCtxCancel := context.WithCancel(context.Background())

	backupStreams := backup.NewBackupStreams(dpStreamsClient, streamStore, &chBackupStreams)

	wgReattempt := bk.createReattemptRoutines(chBackupStreams, chBackupStreamsDeduped, ReattemptBackupStreamSimpleFails)
	wgDedupe := bk.createDedupeRoutines(chBackupStreams, chBackupStreamsDeduped, cacheObjBkupContext)
	wgStreamBackup := bk.createStreamRoutines(dpEventClients, chBackupStreamsDeduped, backupStreams, chStats)

	// Wait until all previously failed streams are processed before allowing events to start up.
	wgReattempt.Wait()

	select {
	case <-bk.ctxEvents.Done():
		bk.checkForSecondTimeBadBackups(backupStreams)
	default:
		// Stream restore didn't cause a failure carry on to events.
	}

	wgEventBackup := bk.createEventRoutines(eventStore, dpEventClients, chBackupStreams, chStats)
	bedSet.Logger.Info().Msg("all backup routines started")

	// Wait for all event backup routines to exit
	// Under normal operation, backup is continuous so these routines will rarely end
	wgEventBackup.Wait()
	bk.CtxEventsCancel()
	bedSet.Logger.Info().Msg("all event routines stopped, waiting for stream routines")
	// Trigger stream dedupe cache to close and then wait for it, it will only close once the queue empties.
	cacheObjBkupCtxCancel()
	wgDedupe.Wait()
	// Trigger streams to close and then wait for it to finish, it will only close once the queue empties.
	bk.CtxStreamsCancel()
	wgStreamBackup.Wait()
	bedSet.Logger.Info().Msg("all stream routines stopped")

	// ensure that all results are processed
	close(chStats)
	wgStats.Wait()

	// Once everything is closed stop waiting for signals (SigTerm/SigKill)
	bk.CtxSigWatcherCancel()

	return stats
}

func (bk *Backup) checkForSecondTimeBadBackups(backupStreams *backup.BackupStreams) {
	reloadedPreviouslyFailedStreams, err := bk.LocalData.BackupStreamLoad()
	if err != nil {
		// Failed to load stream data, crash the application.
		log.Fatalf("Failed to load backup streams during second check")
		return
	}
	// Delete the backed up streams and only store files that have a chance of succeeding in there.
	bk.LocalData.BackupStreamDelete()
	// Place all files in appropriate storage.
	for _, stream := range reloadedPreviouslyFailedStreams {
		exists, err := backupStreams.StreamExistsInDispatcher(&stream)
		if err == nil && !exists {
			// Show full stream in resulting label as it's important enough to take the prometheus performance hit.
			prom.BackupFailedStream.WithLabelValues(stream.GetDestS3Path())
			bedSet.Logger.Warn().Msgf("giving up trying to restore the file '%s'", stream.GetDestS3Path())
			err = bk.LocalData.FailedBackupStreamStashAppend(stream)
			if err != nil {
				bedSet.Logger.Error().Err(err).Msgf("streams - failed to stash stream backup to failed stream backup location for stream %s", stream.GetDestS3Path())
				prom.BackupObjectError.WithLabelValues("BackupStreamStashAppend3").Inc()
				err = bk.LocalData.BackupStreamStashAppend(stream)
				if err != nil {
					bedSet.Logger.Error().Err(err).Msgf("streams - failed to stash stream backup in secondary location for stream %s", stream.GetDestS3Path())
				}
			}
		} else {
			err = bk.LocalData.BackupStreamStashAppend(stream)
			if err != nil {
				bedSet.Logger.Error().Err(err).Msgf("streams - failed to stash stream backup to failed stream backup location for stream %s", stream.GetDestS3Path())
				prom.BackupObjectError.WithLabelValues("BackupStreamStashAppend4").Inc()
			}
		}
	}
}

var backupCmd = &cobra.Command{
	Use:   "backup",
	Short: "Backup Azul events and streams Data",
	Long:  `Backup Azul events and streams Data`,
	Args:  cobra.NoArgs,
	Run: func(cmd *cobra.Command, args []string) {
		go prom.StartStandalonePromServer()
		st := bkupcom.Settings
		bk := NewBackup()

		author := NewAuthor("backup-streams", "all", "all", st.BackupID)
		dpStreamsClient := bedclient.NewClient(st.DispatcherEvents, st.DispatcherStreams, *author, st.DeploymentKey)
		dpEventClients := prepareSources("backup", nil)

		startTime := time.Now()
		// streams bucket creation
		streamStore := bkupcom.NewObjectS3Store(st)
		// events bucket creation
		eventStore := bkupcom.NewEventS3Store(st)
		// Startup.
		bk.DoBackup(streamStore, eventStore, dpStreamsClient, dpEventClients)

		runtime := time.Since(startTime).Seconds()
		log.Printf("Backup terminated after %.2fs\n", runtime)
	},
}

var clearBadBackupStreams = &cobra.Command{
	Use:   "clear-backup-streams",
	Short: "Clear backup streams that are considered un-recoverable.",
	Long:  `Clear backup streams that are considered un-recoverable.`,
	Args:  cobra.NoArgs,
	Run: func(cmd *cobra.Command, args []string) {
		log.SetFlags(0)
		bk := NewBackup()
		confirmDeletion, err := cmd.Flags().GetBool(CLEAR_FAILED_FILES_FLAG)
		if err != nil {
			bedSet.Logger.Fatal().Msgf("Failed to clear failed fiels because flag '%s' didn't even have a default.", CLEAR_FAILED_FILES_FLAG)
			return
		}
		if confirmDeletion {
			bedSet.Logger.Warn().Msg("Deleting all the streams that have failed to backup more than once.")
			bk.LocalData.FailedBackupStreamDeleteFile()
		} else {
			log.Print("Not deleting failed streams as you need to add the --confirm flag to confirm data loss.")
		}
	},
}

var listBadBackupStreams = &cobra.Command{
	Use:   "list-failed-backup-streams",
	Short: "List failed backup streams.",
	Long:  `List backup streams that have failed once and separately more than once and they don't exist in dispatcher.`,
	Args:  cobra.NoArgs,
	Run: func(cmd *cobra.Command, args []string) {
		log.SetFlags(0)
		bk := NewBackup()
		streams, err := bk.LocalData.BackupStreamLoad()
		if err != nil {
			bedSet.Logger.Error().Err(err).Msg("failed to load bad streams.")
		}
		log.Print("Streams that have failed to backup.")
		for _, curStream := range streams {
			log.Print(curStream.GetDestS3Path())
		}

		failedStreams, err := bk.LocalData.FailedBackupStreamLoad()
		if err != nil {
			bedSet.Logger.Error().Err(err).Msg("failed to load bad streams.")
			return
		}
		log.Print("Streams that have failed to backup and don't appear to exist in dispatcher.")
		for _, curStream := range failedStreams {
			log.Print(curStream.GetDestS3Path())
		}
	},
}

var manualRetryBadBackupFiles = &cobra.Command{
	Use:   "trigger-backup-failed-streams",
	Short: "Manually re-attempt to backup failed streams.",
	Long:  `Manually re-attempt to backup failed streams.`,
	Args:  cobra.NoArgs,
	Run: func(cmd *cobra.Command, args []string) {
		log.SetFlags(0)
		simpleStreamRestore, err := cmd.Flags().GetBool(MANUAL_BACKUP_SIMPLE_FAILED_FILES_RESTORE_FLAG)
		if err != nil {
			bedSet.Logger.Fatal().Msgf("Failed to perform restore because flag '%s' didn't even have a default.", MANUAL_BACKUP_SIMPLE_FAILED_FILES_RESTORE_FLAG)
			return
		}
		notInDispatcherStreamRestore, err := cmd.Flags().GetBool(MANUAL_BACKUP_FAILED_FILES_RESTORE_FLAG)
		if err != nil {
			bedSet.Logger.Fatal().Msgf("Failed to perform restore because flag '%s' didn't even have a default.", MANUAL_BACKUP_FAILED_FILES_RESTORE_FLAG)
			return
		}

		// Check what streams should have a restore attempted.
		var reAttempt ReattemptBackupStream
		if simpleStreamRestore && notInDispatcherStreamRestore {
			reAttempt = ReattemptBackupStreamBoth
			log.Print("Re-attempting to restore all failed streams.")
		} else if simpleStreamRestore {
			reAttempt = ReattemptBackupStreamSimpleFails
			log.Print("Re-attempting just the simple files that have only failed to restore once.")
		} else if notInDispatcherStreamRestore {
			reAttempt = ReattemptBackupStreamComplexFails
			log.Print("Re-attempting just the complex files that aren't in dispatcher and have failed more than once.")
		} else {
			log.Print("Nothing selected to restore so nothing to restore.")
			return
		}

		st := bkupcom.Settings
		bk := NewBackup()
		author := NewAuthor("backup-streams", "all", "all", st.BackupID)
		dpStreamsClient := bedclient.NewClient(st.DispatcherEvents, st.DispatcherStreams, *author, st.DeploymentKey)
		dpEventClients := prepareSources("backup", nil)

		// streams bucket creation
		streamStore := bkupcom.NewObjectS3Store(st)

		// Channel stats (discard them all)
		chStats := make(chan map[string]*backup.BackupStats)

		// Setup all the stats for collection.
		stats := map[string]*backup.BackupStats{}
		// gather needed source directories
		for source := range dpEventClients {
			stats[source] = &backup.BackupStats{}
		}
		// create stats monitor routine
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

		// Prevent signal termination from going early as the streams might be lost.
		chSignals := make(chan os.Signal, 10)
		signal.Notify(chSignals, os.Interrupt, syscall.SIGINT, syscall.SIGTERM)
		go func() {
			safeGuardCounter := 0
			for {
				select {
				case <-chSignals:
					bk.RapidShutdown = true
					safeGuardCounter += 1
					bedSet.Logger.Warn().Msgf("User has requested process exit, exit has begun. If you retry cancellation data loss will occur after 5 attempts attempt number '%d'", safeGuardCounter)
					if safeGuardCounter > 4 {
						bedSet.Logger.Warn().Msgf("Potentially lost streams that were attempted to be backed up.")
						os.Exit(99)
					}
				case <-bk.CtxSigWatcher.Done(): // Top listening for signals once everything else is finished.
					return
				}

			}
		}()

		chBackupStreams := make(chan bkupcom.StreamBackupRequest)
		backupStreams := backup.NewBackupStreams(dpStreamsClient, streamStore, &chBackupStreams)
		wgReattempt := bk.createReattemptRoutines(chBackupStreams, chBackupStreams, reAttempt)
		wgStreamBackup := bk.createStreamRoutines(dpEventClients, chBackupStreams, backupStreams, chStats)

		// Wait until all previously failed streams are processed before allowing events to start up.
		wgReattempt.Wait()

		// Cancel everything else.
		bk.CtxEventsCancel()
		bk.CtxStreamsCancel()
		wgStreamBackup.Wait()

		// Kill stats channel
		close(chStats)
		wgStats.Wait()

		// Re-check what failures are complex and not.
		bk.checkForSecondTimeBadBackups(backupStreams)

		backup.UpdateBackupStats(time.Time{}, stats, true)
		bk.CtxSigWatcherCancel()
	},
}

var addBadBackupStream = &cobra.Command{
	Use:   "add-failed-stream [args]",
	Short: "Add a new failed stream into the files to restore.",
	Long:  `Add a new failed stream into the files to restore.`,
	Args:  cobra.ArbitraryArgs,
	Run: func(cmd *cobra.Command, args []string) {
		log.SetFlags(0)
		bk := NewBackup()
		for _, path := range args {
			source, label, id := bkupcom.SplitLastThree(path)
			backupRequest := bkupcom.StreamBackupRequest{
				EventId: "userSupplied",
				Source:  source,
				Label:   events.DatastreamLabel(label),
				Sha256:  id,
			}
			err := bk.LocalData.FailedBackupStreamStashAppend(backupRequest)
			if err != nil {
				bedSet.Logger.Warn().Err(err).Msgf("failed to add backup stream '%s' as a backup stream", path)
			} else {
				log.Printf("Successfully added %s", path)
			}
		}
	},
}

func init() {
	rootCmd.AddCommand(backupCmd)
	manualRetryBadBackupFiles.Flags().Bool(MANUAL_BACKUP_SIMPLE_FAILED_FILES_RESTORE_FLAG, true, "Manually trigger a backup of files that have failed to backup once.")
	manualRetryBadBackupFiles.Flags().Bool(MANUAL_BACKUP_FAILED_FILES_RESTORE_FLAG, true, "Manually retry to backup the files that have failed more than once and don't exist in dispatcher.")
	rootCmd.AddCommand(manualRetryBadBackupFiles)
	clearBadBackupStreams.Flags().Bool(CLEAR_FAILED_FILES_FLAG, false, "Confirmation to prevent accidental deletion of files")
	rootCmd.AddCommand(clearBadBackupStreams)
	rootCmd.AddCommand(listBadBackupStreams)
	rootCmd.AddCommand(addBadBackupStream)
}
