package common

import (
	"bytes"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/AustralianCyberSecurityCentre/azul-bedrock/v12/gosrc/events"
	bedSet "github.com/AustralianCyberSecurityCentre/azul-bedrock/v12/gosrc/settings"
)

// 7=rwx, 6=rw, 5=rx, 4=r
const directoryPerms = 0755
const filePerms = 0766

/*LocalData for thread safe saving and loading of backup and restore state.*/
type LocalData struct {
	// Root directory for all local files
	rootDir string
	// Backup state directory
	backupDir string
	// Restore state directory
	restoreDir string

	// Locks - to prevent CRUD operations happening to the same file at the same time in different go routines.
	backupStreamsLock sync.Mutex
	backupEventsLock  sync.Mutex

	restoreStreamsLock sync.Mutex
	restoreEventsLock  sync.Mutex
}

/*NewLocal Creates a new area for thread safe saving and loading of backup and restore state.*/
func NewLocal(st LocalDataSettings) *LocalData {
	return &LocalData{
		rootDir:    st.RootDir,
		backupDir:  filepath.Join(st.RootDir, "backup"),
		restoreDir: filepath.Join(st.RootDir, "restore"),
	}
}

func (fc *LocalData) Reset() error {
	return os.RemoveAll(fc.rootDir)
}

/* Create directory structure for either backup or restore.*/
func (fc *LocalData) createLocalDirs() {
	for _, parentDir := range []string{fc.backupDir, fc.restoreDir} {
		testFile := filepath.Join(parentDir, "test-file-to-delete")
		err := os.MkdirAll(parentDir, directoryPerms)
		if err != nil {
			log.Fatalf("Failed to create the local data directory %s", parentDir)
		}
		err = fc.stashFile(testFile, []byte("test file contents."))
		if err != nil {
			log.Fatalf("Failed to create temporary file when creating temp dir with error %v", err)
		}
		err = os.Remove(testFile)
		if err != nil {
			log.Fatalf("Failed to delete temporary file when creating temp dir with error %v", err)
		}
	}
}

/* Creates all backup directories if they don't exist and creates a test file to test permissions.*/
func (fc *LocalData) CreateBackupDirectories() {
	fc.createLocalDirs()
}

/* Creates all backup directories if they don't exist and creates a test file to test permissions.*/
func (fc *LocalData) CreateRestoreDirectories() {
	fc.createLocalDirs()
}

// --- Backup Events ---

// fpBackupEvents returns the file path to temporarily store bulk events
func (fc *LocalData) fpBackupEvents(source, model, action string) string {
	filename := fmt.Sprintf("%s-%s-%s.events", source, model, action)
	return filepath.Join(fc.backupDir, filename)
}

/*Overrides the details of bulk events that need to be backed up but can't be right now.*/
func (fc *LocalData) BackupEventsStash(source, model, action string, bulk []byte) error {
	fc.backupEventsLock.Lock()
	defer fc.backupEventsLock.Unlock()
	return fc.stashFile(fc.fpBackupEvents(source, model, action), bulk)
}

func (fc *LocalData) BackupEventsLoad(source, model, action string) ([]byte, error) {
	fc.backupEventsLock.Lock()
	defer fc.backupEventsLock.Unlock()
	bulk, err := fc.loadFile(fc.fpBackupEvents(source, model, action))
	if err != nil {
		return []byte{}, err
	}
	return bulk, nil
}
func (fc *LocalData) BackupEventsDelete(source, model, action string) {
	fc.backupEventsLock.Lock()
	defer fc.backupEventsLock.Unlock()
	fc.deleteFile(fc.fpBackupEvents(source, model, action))
}

// --- Backup Streams ---

// fpStreamBackup returns the file path to temporarily store streams
func (fc *LocalData) fpStreamBackup() string {
	return filepath.Join(fc.backupDir, "backup.stream")
}
func (fc *LocalData) fpStreamBackupFailed() string {
	return filepath.Join(fc.backupDir, "backup-failed.stream")
}

/*Appends the info about a file that needs to be backed up but can't be right now.*/
func (fc *LocalData) BackupStreamStashAppend(obr StreamBackupRequest) error {
	fc.backupStreamsLock.Lock()
	defer fc.backupStreamsLock.Unlock()
	data := fmt.Sprintf("%s/%s/%s/%s\n", obr.EventId, obr.Source, obr.Label, obr.Sha256)
	return fc.stashAppendToFile(fc.fpStreamBackup(), []byte(data))
}

// Stash a stream that has failed multiple times and won't be retried.
func (fc *LocalData) BackupFailedStreamStashAppend(obr StreamBackupRequest) error {
	fc.backupStreamsLock.Lock()
	defer fc.backupStreamsLock.Unlock()
	data := fmt.Sprintf("%s/%s/%s/%s\n", obr.EventId, obr.Source, obr.Label, obr.Sha256)
	bedSet.Logger.Warn().Msgf("giving up trying to restore the file '%s'", data)
	return fc.stashAppendToFile(fc.fpStreamBackupFailed(), []byte(data))
}

/*Load the streams that were not correctly saved during the last run.*/
func (fc *LocalData) BackupStreamLoad() ([]StreamBackupRequest, error) {
	fc.backupStreamsLock.Lock()
	defer fc.backupStreamsLock.Unlock()
	var result []StreamBackupRequest
	content, err := fc.loadFile(fc.fpStreamBackup())
	if err != nil {
		return result, err
	}
	if len(content) == 0 {
		return result, nil
	}
	for _, val := range bytes.Split(content, []byte{'\n'}) {
		if len(val) == 0 {
			continue
		}
		streamData := string(val)
		sls := strings.Split(streamData, "/")
		// This would only happen if the format was manually changed and that shouldn't happen.
		if len(sls) != 4 {
			bedSet.Logger.Error().Msgf("could not load the stream %s, it's format was invalid ignoring stream for restore.", streamData)
			continue
		}
		loadedRequest := NewObjectBackupRequest(sls[0], sls[1], events.DatastreamLabel(sls[2]), sls[3])
		result = append(result, loadedRequest)
	}
	return result, nil
}
func (fc *LocalData) BackupStreamDelete() {
	fc.backupStreamsLock.Lock()
	defer fc.backupStreamsLock.Unlock()
	fc.deleteFile(fc.fpStreamBackup())
}

// --- Restore Events ---
// fpEventRestored returns the file path to temporarily restore events
func (fc *LocalData) fpEventRestored(source, model, action string) string {
	filename := fmt.Sprintf("%s-%s-%s.events", source, model, action)
	return filepath.Join(fc.restoreDir, filename)
}

/*Store the last event that was successfully backed up.*/
func (fc *LocalData) LastEventKeyRestoredSave(source, model, action string, key string) error {
	fc.restoreEventsLock.Lock()
	defer fc.restoreEventsLock.Unlock()
	return fc.stashFile(fc.fpEventRestored(source, model, action), []byte(key))
}

/*Load the last event that was successfully backed up.*/
func (fc *LocalData) LastEventKeyRestoredLoad(source, model, action string) (string, error) {
	fc.restoreEventsLock.Lock()
	defer fc.restoreEventsLock.Unlock()
	content, err := fc.loadFile(fc.fpEventRestored(source, model, action))
	if err != nil {
		return "", err
	}
	return string(content), nil
}
func (fc *LocalData) LastEventKeyRestoredDelete(source, model, action string) {
	fc.restoreEventsLock.Lock()
	defer fc.restoreEventsLock.Unlock()
	fc.deleteFile(fc.fpEventRestored(source, model, action))
}

// --- Restore Streams ---
// fpstreamRestored returns the file path to temporarily restore streams
func (fc *LocalData) fpstreamRestored(source string) string {
	return filepath.Join(fc.restoreDir, source+".stream")
}

/*Store the last stream that was successfully backed up.*/
func (fc *LocalData) LastStreamKeyRestoredStash(source string, key string) error {
	fc.restoreStreamsLock.Lock()
	defer fc.restoreStreamsLock.Unlock()
	return fc.stashFile(fc.fpstreamRestored(source), []byte(key))
}

/*Load the last stream that was successfully backed up.*/
func (fc *LocalData) LastStreamKeyRestoredLoad(source string) (string, error) {
	fc.restoreStreamsLock.Lock()
	defer fc.restoreStreamsLock.Unlock()
	content, err := fc.loadFile(fc.fpstreamRestored(source))
	if err != nil {
		return "", err
	}
	return string(content), nil
}
func (fc *LocalData) LastStreamKeyRestoredDelete(source string) {
	fc.restoreStreamsLock.Lock()
	defer fc.restoreStreamsLock.Unlock()
	fc.deleteFile(fc.fpstreamRestored(source))
}

// --- Generic CRUD operations ---
// FUTURE these should guard against jumping around the file system

/*Base method for stashing a file to the operating system.*/
func (fc *LocalData) stashFile(path string, content []byte) error {
	return os.WriteFile(path, content, filePerms)
}

/*Base method for appending content to a stash file to the operating system.*/
func (fc *LocalData) stashAppendToFile(path string, content []byte) error {
	f, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, filePerms)
	if err != nil {
		return err
	}
	defer f.Close()
	_, err = f.Write(content)
	return err
}

/*Base method for deleting a stash file that is no longer needed.*/
func (fc *LocalData) deleteFile(path string) {
	if _, err := os.Stat(path); err == nil {
		err := os.Remove(path)
		if err != nil {
			bedSet.Logger.Warn().Err(err).Msgf("could not delete file with path %s even though it exists.", path)
		}
	}
	// File doesn't exist so do nothing.
}

/*Base method for loading content from a stash file.*/
func (fc *LocalData) loadFile(path string) ([]byte, error) {
	if _, err := os.Stat(path); err != nil {
		// File doesn't exist so nothing to load.
		return []byte{}, nil
	}

	f, err := os.OpenFile(path, os.O_RDONLY, filePerms)
	if err != nil {
		return []byte{}, err
	}
	defer f.Close()
	return io.ReadAll(f)
}
