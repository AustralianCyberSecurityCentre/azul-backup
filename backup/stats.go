package backup

import (
	"slices"
	"time"

	bedSet "github.com/AustralianCyberSecurityCentre/azul-bedrock/v9/gosrc/settings"

	"github.com/AustralianCyberSecurityCentre/azul-backup.git/prom"
)

type BackupStats struct {
	EventsOk       int
	EventsInvalid  int
	EventsFail     int
	StreamsOk      int
	StreamsMissing int
	StreamsFail    int
}

// Add will merge stats into another
func (bk *BackupStats) Add(v *BackupStats) {
	bk.EventsOk += v.EventsOk
	bk.EventsInvalid += v.EventsInvalid
	bk.EventsFail += v.EventsFail
	bk.StreamsOk += v.StreamsOk
	bk.StreamsMissing += v.StreamsMissing
	bk.StreamsFail += v.StreamsFail
}

// PrintBackupState prints stats for each source
func PrintBackupState(startTime time.Time, stats map[string]*BackupStats) {
	// sort sources alphabetically
	keys := []string{}
	for k := range stats {
		keys = append(keys, k)
	}
	slices.Sort(keys)
	var totalEvents, totalStreams int
	runtime := time.Since(startTime).Seconds()
	bedSet.Logger.Info().Msg("Backup stats:\nsource\tevents-ok\tevents-fail\tstreams-ok\tstreams-missing\tstreams-fail\t\n")
	for _, source := range keys {
		stat := stats[source]
		totalEvents += stat.EventsOk
		totalStreams += stat.StreamsOk
		bedSet.Logger.Info().Msgf("%s\t %d\t %d\t %d\t %d\t %d\t\n",
			source, stat.EventsOk, stat.EventsFail, stat.StreamsOk, stat.StreamsMissing, stat.StreamsFail)

		// Set prometheus stat for the printed data.
		prom.BackupEventStatus.WithLabelValues(source, "events-ok").Set(float64(stat.EventsOk))
		prom.BackupEventStatus.WithLabelValues(source, "events-fail").Set(float64(stat.EventsFail))
		prom.BackupEventStatus.WithLabelValues(source, "streams-ok").Set(float64(stat.StreamsOk))
		prom.BackupEventStatus.WithLabelValues(source, "streams-missing").Set(float64(stat.StreamsMissing))
		prom.BackupEventStatus.WithLabelValues(source, "streams-fail").Set(float64(stat.StreamsFail))
	}
	bedSet.Logger.Info().Msgf("Processed %d events (%.2f/s) and %d streams (%.2f/s)\n", totalEvents, float64(totalEvents)/runtime, totalStreams, float64(totalStreams)/runtime)
}
