package common

import (
	"log"

	"github.com/AustralianCyberSecurityCentre/azul-bedrock/v12/gosrc/models"
	bedsettings "github.com/AustralianCyberSecurityCentre/azul-bedrock/v12/gosrc/settings"
	"github.com/go-viper/mapstructure/v2"
)

const RECOVERY_VERSION = "3.0.0"

type AesStateEnum string

// All enums added here need to be added to Python's exception_enums.py as well.
const (
	AesStateEnabled  = AesStateEnum("enabled")
	AesStateDisabled = AesStateEnum("disabled")
	// Allows for reading of AES encrypted events but no writing of new aes events.
	AesStateReadOnly = AesStateEnum("readonly")
)

type LocalDataSettings struct {
	RootDir string `koanf:"rootdir"`
}

type StreamsS3Settings struct {
	Endpoint  string `koanf:"endpoint"`
	AccessKey string `koanf:"access_key"`
	SecretKey string `koanf:"secret_key"`
	Secure    bool   `koanf:"secure"`
	Region    string `koanf:"region"`
	// Bucket, when set, is the single shared bucket that holds all backup data.
	// Streams and events live under folders within it
	// Leave empty for deployments that use a dedicated bucket per data type.
	Bucket   string       `koanf:"bucket"`
	AesKey   string       `koanf:"aes_key"`
	AesState AesStateEnum `koanf:"aes_state"`
}

type RecoverySettings struct {
	// Backup Prefix
	BucketNamePrefix string `koanf:"bucket_name_prefix"`
	// If the backup id is changed, a full backup will be started into a new set of buckets
	// bucket format is `<bucket_name_prefix><backup_id>-(streams|events)`
	BackupID string `koanf:"backup_id"`
	// Dispatcher URL to source events from
	DispatcherEvents  string            `koanf:"dispatcher_events"`
	DispatcherStreams string            `koanf:"dispatcher_streams"`
	ExternalBackup    StreamsS3Settings `koanf:"external_backup"`
	// Controls size of the stream channel during backup and restore
	StreamChannelSize int `koanf:"stream_channel_size"`
	// Restore checkpoint binary distance (distance between backup checkpoints), note the larger this is
	// The more performant Restore will be but at the cost of if there is an error it will take longer to get back to where it was up to.
	RestoreCheckpointBinaryCount int `koanf:"checkpoint_binary_count"`
	// Number of times to retry a listing operation if it fails.
	RestoreListRetryCount int `koanf:"restore_list_retry_count"`
	// Number of times to attempt to retry and restore a stream.
	RestoreStreamRetryCount int `koanf:"restore_stream_retry_count"`
	// Controls number of concurrent routines for stream backup and restore
	StreamRoutineCount int `koanf:"stream_routine_count"`
	// local data to allow recovery from failures
	LocalData LocalDataSettings `koanf:"local_data"`
	// Restore S3 and events by setting RestoreType to `all` (default). Otherwise set to `streams` or `events`
	RestoreType string `koanf:"restore_type"`
	// the deployment key, an ID grouping together instances of a plugin for the purposes of scaling
	DeploymentKey string `koanf:"deployment_key"`
	// azul sources to be backed up
	RawSources string             `koanf:"sources"`
	Sources    models.SourcesConf // parsed outside of koanf
	// event batch size
	// There must be at least this many events before saving the events.
	EventBatchSize int `koanf:"event_batch_size"`

	// Enable automatic deletion of data that is older than what the source would keep.
	EnableAutomaticAgeOff bool `koanf:"enable_automatic_ageoff"`
	// If automatic ageoff is disabled, should the old rules be cleaned up?
	EnableCleanupAutoAgeOff bool `koanf:"enable_automatic_ageoff_cleanup"`
	// Check if streams exist in dispatcher and don't restore it if it already exists.
	RestoreSkipExistingStreams bool `koanf:"restore_skip_existing_streams"`
	// Path to file which has listing of files to restore.
	ListingFilePath       string `koanf:"listing_file_path"`
	IgnoreRestoreNotFound bool   `koanf:"ignore_restore_not_found"`
}

var Settings *RecoverySettings

var defaults RecoverySettings = RecoverySettings{
	BucketNamePrefix:             "azul-backup-",
	BackupID:                     "1",
	StreamChannelSize:            500,
	RestoreCheckpointBinaryCount: 1000,
	RestoreStreamRetryCount:      5,
	RestoreListRetryCount:        5,
	StreamRoutineCount:           20,
	LocalData:                    LocalDataSettings{RootDir: "/tmp/azul_recovery"},
	RestoreType:                  "all",
	DeploymentKey:                "recovery",
	EventBatchSize:               1000,
	EnableAutomaticAgeOff:        false,
	EnableCleanupAutoAgeOff:      false,
	RestoreSkipExistingStreams:   false,
	ListingFilePath:              "",
	IgnoreRestoreNotFound:        false,
}

func parseSettings() *RecoverySettings {
	out := bedsettings.ParseSettings(defaults, "BK", []mapstructure.DecodeHookFunc{mapstructure.StringToSliceHookFunc(",")})

	// parse the raw source string into an actual data structure
	var err error
	out.Sources, err = models.ParseSourcesYaml(out.RawSources)
	if err != nil {
		log.Fatalf("bad sources load: %s", err.Error())
	}
	return out
}

func ResetSettings() {
	Settings = parseSettings()
}
