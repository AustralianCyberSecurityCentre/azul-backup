package restore

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"

	bkupcom "github.com/AustralianCyberSecurityCentre/azul-backup.git/common"
	bedclient "github.com/AustralianCyberSecurityCentre/azul-bedrock/v10/gosrc/client"
	"github.com/AustralianCyberSecurityCentre/azul-bedrock/v10/gosrc/events"
	"github.com/AustralianCyberSecurityCentre/azul-bedrock/v10/gosrc/models"
	bedSet "github.com/AustralianCyberSecurityCentre/azul-bedrock/v10/gosrc/settings"
	"github.com/AustralianCyberSecurityCentre/azul-bedrock/v10/gosrc/store"
)

type RestoreEvents struct {
	dp             bedclient.ClientInterface
	Ev             store.FileStorage
	source         string
	model          events.Model
	action         events.BinaryAction
	modelAndAction string
	fileCache      *bkupcom.LocalData
	maxRetries     int
}

func NewRestoreEvents(
	dpclient bedclient.ClientInterface,
	ev store.FileStorage,
	fileCache *bkupcom.LocalData,
	maxRetries int,
	source string,
	model events.Model,
	action events.BinaryAction,
) *RestoreEvents {
	if model != events.ModelBinary {
		action = "default"
	}
	return &RestoreEvents{
		dp:             dpclient,
		Ev:             ev,
		fileCache:      fileCache,
		maxRetries:     maxRetries,
		source:         source,
		model:          model,
		action:         action,
		modelAndAction: string(model) + "/" + string(action),
	}
}

/*Get the time field from an event name.*/
func extractNamePrefixKey(objKey string) (string, error) {
	keyParts := strings.Split(objKey, "/")
	if len(keyParts) == 4 {
		name := keyParts[3]
		nameParts := strings.Split(name, "T")
		return fmt.Sprintf("%s/%s/%s/%s", keyParts[0], keyParts[1], keyParts[2], nameParts[0]), nil
	}
	return "", fmt.Errorf("failed to parse key %s due to unexpected format should be <source>/<model>/<action>/<dateNano>-<count>", objKey)
}

// Used because the format of a event object is 4 parts not 3 like most others.
func convertEventPathToSourceLabelIdFromKey(objKey string) (string, string, string, error) {
	keyParts := strings.Split(objKey, "/")
	if len(keyParts) == 4 {
		modelSlashAction := fmt.Sprintf("%s/%s", keyParts[1], keyParts[2])
		return keyParts[0], modelSlashAction, keyParts[3], nil
	}
	return "", "", "", fmt.Errorf("failed to parse key %s due to unexpected format should be <source>/<model>/<action>/<dateNano>-<count>", objKey)
}

func (re *RestoreEvents) CountObjectsPerPrefix() map[string]int {
	prefix := fmt.Sprintf("%s/%s/", re.source, re.modelAndAction)
	counts := map[string]int{}
	ctx, cancelFunc := context.WithCancel(context.Background())
	defer func() { cancelFunc() }()

	for obj := range re.Ev.List(ctx, prefix, "") {
		dayPrefix, err := extractNamePrefixKey(obj.Key)
		if err != nil {
			bedSet.Logger.Warn().Err(err).Msgf("Ignoring file with key %s when counting number of events per day.", obj.Key)
		}
		counts[dayPrefix] += 1
	}
	return counts
}

func (re *RestoreEvents) GetSortedBucketPrefixesOldestToNewest() []string {
	// If the bucket path doesn't exist because there is no data there is nothing to restore.
	dailyPrefixObjectCount := re.CountObjectsPerPrefix()
	if len(dailyPrefixObjectCount) == 0 {
		return []string{}
	}

	sortedPrefixes := make([]string, 0, len(dailyPrefixObjectCount))
	// FUTURE - if count is high enough split daily to hourly or even minutely, for the day with the high count.
	for prefix := range dailyPrefixObjectCount {
		sortedPrefixes = append(sortedPrefixes, prefix)
	}
	// Sort keys into order
	sort.Strings(sortedPrefixes)

	return sortedPrefixes
}

/*Restore a bundle of events from the backup storage to dispatcher.*/
func (re *RestoreEvents) RestoreEvent(objInfo store.FileStorageObjectListInfo) (*models.ResponsePostEvent, error) {
	var err error
	var notFoundError *store.NotFoundError
	restoreSource, restoreLabel, restoreId, err := convertEventPathToSourceLabelIdFromKey(objInfo.Key)
	if err != nil {
		return nil, fmt.Errorf("s3 fetch parsing key: %w", err)
	}
	compressedData, err := re.Ev.Fetch(restoreSource, restoreLabel, restoreId)
	if errors.As(err, &notFoundError) {
		bedSet.Logger.Warn().Msgf("events to restore were not found in key: %s", objInfo.Key)
	}
	if err != nil {
		return nil, fmt.Errorf("s3 fetch: %w", err)
	}

	bulk, err := bkupcom.NewGzipDecompressReader(compressedData.DataReader)
	if err != nil {
		// blob was corrupted
		return nil, fmt.Errorf("bad gzip: %w", err)
	}

	// dispatcher validates these events, so we can send them through as is
	resp, err := re.dp.PostEventsBytes(bulk, &bedclient.PublishBytesOptions{Sync: true, PausePlugins: true, AvroFormat: true, Model: re.model})
	if err != nil {
		// dispatcher is down or we used it wrong
		// note - dispatcher ignores corrupted events so that doesn't raise here
		return nil, fmt.Errorf("dispatcher publish: %w", err)
	}

	err = re.fileCache.LastEventKeyRestoredSave(re.source, re.model.Str(), re.action.Str(), objInfo.Key)
	if err != nil {
		bedSet.Logger.Warn().Err(err).Msgf("couldn't save the last event Key %s to filecache.", objInfo.Key)
		err = fmt.Errorf("save last event: %w", err)
	}
	return resp, err
}

func (re *RestoreEvents) LoadLastEventSavedKey() (string, error) {
	return re.fileCache.LastEventKeyRestoredLoad(re.source, re.model.Str(), re.action.Str())
}
