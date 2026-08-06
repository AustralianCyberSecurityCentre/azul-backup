package backup

import (
	"bytes"
	"fmt"
	"slices"
	"text/tabwriter"
	"time"

	bedSet "github.com/AustralianCyberSecurityCentre/azul-bedrock/v12/gosrc/settings"

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

// UpdateBackupStats prints stats for each source
func UpdateBackupStats(startTime time.Time, stats map[string]*BackupStats, printStats bool) {
	// sort sources alphabetically
	keys := []string{}
	for k := range stats {
		keys = append(keys, k)
	}
	slices.Sort(keys)
	var totalEvents, totalStreams int
	runtime := time.Since(startTime).Seconds()

	var statLogBuffer bytes.Buffer
	statLogWriter := tabwriter.NewWriter(&statLogBuffer, 1, 1, 2, ' ', 0)
	fmt.Fprintln(statLogWriter, "source\tevents-ok\tevents-fail\tstreams-ok\tstreams-missing\tstreams-fail\t")

	for _, source := range keys {
		stat := stats[source]
		totalEvents += stat.EventsOk
		totalStreams += stat.StreamsOk
		fmt.Fprintf(statLogWriter, "%s\t %d\t %d\t %d\t %d\t %d\t\n",
			source, stat.EventsOk, stat.EventsFail, stat.StreamsOk, stat.StreamsMissing, stat.StreamsFail)

		// Set prometheus stat for the printed data.
		prom.BackupEventStatus.WithLabelValues(source, "events-ok").Set(float64(stat.EventsOk))
		prom.BackupEventStatus.WithLabelValues(source, "events-fail").Set(float64(stat.EventsFail))
		prom.BackupEventStatus.WithLabelValues(source, "streams-ok").Set(float64(stat.StreamsOk))
		prom.BackupEventStatus.WithLabelValues(source, "streams-missing").Set(float64(stat.StreamsMissing))
		prom.BackupEventStatus.WithLabelValues(source, "streams-fail").Set(float64(stat.StreamsFail))
	}
	if printStats {
		statLogWriter.Flush()
		bedSet.Logger.Info().Msgf("Backup stats:\n%s", statLogBuffer.String())
		bedSet.Logger.Info().Msgf("Processed %d events (%.2f/s) and %d streams (%.2f/s)\n", totalEvents, float64(totalEvents)/runtime, totalStreams, float64(totalStreams)/runtime)
	}

}
