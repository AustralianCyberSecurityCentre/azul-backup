package common

import (
	"context"
	"io"
	"strings"

	bedSet "github.com/AustralianCyberSecurityCentre/azul-bedrock/v12/gosrc/settings"
	"github.com/AustralianCyberSecurityCentre/azul-bedrock/v12/gosrc/store"
)

// FolderPrefixStore folds a logical store into a folder (key prefix) inside a
// larger shared bucket. It is used when the backup lives in one real bucket
// (e.g. azul-backup-bucket) with the streams/events data under folders such as
// "azul-backup-1-streams/..." and "azul-backup-1-events/...".
//
// It prepends the folder to the key of every operation and strips it back off
// the keys returned by List, so the rest of the codebase works with keys exactly
// as it would against a dedicated per-store bucket. Because bedrock builds object
// keys as "<source>/<label>/<id>", prefixing the source is equivalent to
// prefixing the whole key.
//
// Note: the inner store must address the real bucket directly (regional endpoint
// + real bucket name). Pointing the inner store at an endpoint that embeds the
// bucket in its hostname does NOT work for List - minio then uses path-style
// addressing and pushes the folder into the request path, which AWS treats as a
// malformed bucket listing and returns empty. See NewObjectS3Store.
type FolderPrefixStore struct {
	inner  store.FileStorage
	prefix string // folder including a single trailing slash, e.g. "azul-backup-1-streams/"
}

// NewFolderPrefixStore wraps inner so that every operation is folded under folder.
// If folder is empty the inner store is returned unchanged.
func NewFolderPrefixStore(inner store.FileStorage, folder string) store.FileStorage {
	folder = strings.TrimSuffix(folder, "/")
	if folder == "" {
		return inner
	}
	return &FolderPrefixStore{inner: inner, prefix: folder + "/"}
}

// The key is "<source>/<label>/<id>", so prepending s.prefix to source prefixes
// the whole key with the folder.
func (s *FolderPrefixStore) Put(source, label, id string, data io.ReadCloser, fileSize int64) error {
	return s.inner.Put(s.prefix+source, label, id, data, fileSize)
}

func (s *FolderPrefixStore) Fetch(source, label, id string, opts ...store.FileStorageFetchOption) (store.DataSlice, error) {
	return s.inner.Fetch(s.prefix+source, label, id, opts...)
}

func (s *FolderPrefixStore) Exists(source, label, id string) (bool, error) {
	return s.inner.Exists(s.prefix+source, label, id)
}

func (s *FolderPrefixStore) Delete(source, label, id string, opts ...store.FileStorageDeleteOption) (bool, error) {
	return s.inner.Delete(s.prefix+source, label, id, opts...)
}

func (s *FolderPrefixStore) Copy(sourceOld, labelOld, idOld, sourceNew, labelNew, idNew string) error {
	return s.inner.Copy(s.prefix+sourceOld, labelOld, idOld, s.prefix+sourceNew, labelNew, idNew)
}

func (s *FolderPrefixStore) List(ctx context.Context, prefix string, startAfter string) <-chan store.FileStorageObjectListInfo {
	// Qualify the list prefix and the StartAfter checkpoint (a previously returned
	// - and therefore already stripped - key) with the folder before handing them
	// to the inner store.
	innerStartAfter := startAfter
	if innerStartAfter != "" {
		innerStartAfter = s.prefix + innerStartAfter
	}

	innerPrefix := s.prefix + prefix
	// DEBUG: show the exact physical prefix and checkpoint handed to S3.
	bedSet.Logger.Info().Msgf("DEBUG FolderPrefixStore.List innerPrefix=%q innerStartAfter=%q", innerPrefix, innerStartAfter)

	out := make(chan store.FileStorageObjectListInfo)
	go func() {
		defer close(out)
		count := 0
		for obj := range s.inner.List(ctx, innerPrefix, innerStartAfter) {
			// DEBUG: log the first few RAW physical keys (before the folder is
			// stripped) so the real on-disk layout is visible.
			if count < 3 {
				bedSet.Logger.Info().Msgf("DEBUG FolderPrefixStore.List innerPrefix=%q rawKey=%q", innerPrefix, obj.Key)
			}
			count++
			obj.Key = strings.TrimPrefix(obj.Key, s.prefix)
			// Re-derive source/label/id from the stripped key so callers see the
			// same values they would against a dedicated bucket.
			obj.Source, obj.Label, obj.Id = splitLastThree(obj.Key)
			select {
			case <-ctx.Done():
				bedSet.Logger.Info().Msgf("DEBUG FolderPrefixStore.List innerPrefix=%q cancelled after %d objects", innerPrefix, count)
				return
			case out <- obj:
			}
		}
		// DEBUG: total objects the physical prefix yielded.
		bedSet.Logger.Info().Msgf("DEBUG FolderPrefixStore.List innerPrefix=%q yielded %d objects", innerPrefix, count)
	}()
	return out
}

// splitLastThree mirrors the store package's key splitting: the trailing three
// "/"-separated segments of a key are its source, label and id.
func splitLastThree(key string) (source, label, id string) {
	parts := strings.Split(key, "/")
	n := len(parts)
	if n >= 1 {
		id = parts[n-1]
	}
	if n >= 2 {
		label = parts[n-2]
	}
	if n >= 3 {
		source = parts[n-3]
	}
	return source, label, id
}
