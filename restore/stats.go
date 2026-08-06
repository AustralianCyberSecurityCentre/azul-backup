package restore

import (
	"bytes"
	"fmt"
	"slices"
	"text/tabwriter"
	"time"

	"github.com/AustralianCyberSecurityCentre/azul-backup.git/prom"
	bedSet "github.com/AustralianCyberSecurityCentre/azul-bedrock/v12/gosrc/settings"
)

type RestoreStats struct {
	EventsOk           int
	EventsFail         int
	EventTypesComplete int
	StreamsOk          int
	StreamsFail        int
	StreamsComplete    bool
}

// Add will merge stats into another
func (rt *RestoreStats) Add(v *RestoreStats) {
	rt.EventsOk += v.EventsOk
	rt.EventsFail += v.EventsFail
	rt.EventTypesComplete += v.EventTypesComplete
	rt.StreamsOk += v.StreamsOk
	rt.StreamsFail += v.StreamsFail
	rt.StreamsComplete = rt.StreamsComplete || v.StreamsComplete
}

// printRestoreState prints stats for each source
func UpdateRestoreStats(startTime time.Time, stats map[string]*RestoreStats, printStats bool) {
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

	fmt.Fprintln(statLogWriter, "source\tevents-ok\tevents-fail\tevent-types-complete\tstreams-ok\tstreams-fail\tstreams-complete")

	for _, source := range keys {
		stat := stats[source]
		totalEvents += stat.EventsOk
		totalStreams += stat.StreamsOk
		fmt.Fprintf(statLogWriter, "%s\t %d\t %d\t %d\t %d\t %d\t %t\t\n",
			source, stat.EventsOk, stat.EventsFail, stat.EventTypesComplete, stat.StreamsOk, stat.StreamsFail, stat.StreamsComplete)

		// Set prometheus stat for the printed data.
		prom.RestoreEventStatus.WithLabelValues(source, "events-ok").Set(float64(stat.EventsOk))
		prom.RestoreEventStatus.WithLabelValues(source, "events-fail").Set(float64(stat.EventsFail))
		prom.RestoreEventStatus.WithLabelValues(source, "streams-ok").Set(float64(stat.StreamsOk))
		prom.RestoreEventStatus.WithLabelValues(source, "streams-fail").Set(float64(stat.StreamsFail))

		prom.RestoreCompletedEventTypesForSource.WithLabelValues(source).Set(float64(stat.EventTypesComplete))

		if stat.StreamsComplete {
			prom.RestoreStreamRestoreComplete.WithLabelValues(source).Set(1)
		} else {
			prom.RestoreStreamRestoreComplete.WithLabelValues(source).Set(0)
		}

	}
	if printStats {
		statLogWriter.Flush()
		bedSet.Logger.Info().Msgf("Restore stats:\n%s", statLogBuffer.String())
		bedSet.Logger.Info().Msgf("Processed %d events (%.2f/s) and %d streams (%.2f/s)\n", totalEvents, float64(totalEvents)/runtime, totalStreams, float64(totalStreams)/runtime)
	}
}
