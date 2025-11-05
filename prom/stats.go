package prom

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	BackupObjectError = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "recovery_backup_object_error",
		Help: "Non-critical error when backing up object.",
	}, []string{"keyword"})
	BackupEventsError = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "recovery_backup_events_error",
		Help: "Non-critical error when backing up events.",
	}, []string{"source", "event", "keyword"})
	BackupEventStatus = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "recovery_backup_stats",
		Help: "Stats about the state of the backup, will update as slow as once every 30minutes.",
	}, []string{"source", "stat"})
	RestoreEventStatus = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "recovery_restore_stats",
		Help: "Stats about the state of the backup, will update as slow as once every 30minutes.",
	}, []string{"source", "stat"})
	RestoreStreamRestoreComplete = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "recovery_stream_restore_complete",
		Help: "Set to a value of 1 when a stream for a source has completed it's restore",
	}, []string{"source"})
	RestoreTotalEventTypesForSource = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "recovery_total_event_types_for_source",
		Help: "Total number of event types for a given source, used to determine when all event types are done.",
	}, []string{"source"})
	RestoreCompletedEventTypesForSource = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "recovery_completed_event_types_for_source",
		Help: "Number of event types completed for a given source, used to determine when all event types are done.",
	}, []string{"source"})
)
