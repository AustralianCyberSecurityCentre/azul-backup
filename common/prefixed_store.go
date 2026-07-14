package common

import (
	"context"
	"io"
	"log"
	"strings"

	"github.com/AustralianCyberSecurityCentre/azul-bedrock/v12/gosrc/store"
)

// FolderPrefixStore is used when the backup S3 endpoint embeds the real bucket in its
// hostname, e.g. azul-backup-bucket.s3.ap-southeast-1.amazonaws.com.
// In this case, objects land at "<real-bucket>/azul-backup-1-streams/...
//
// Writes (PutObject) and single-object reads (GetObject) tolerate this because
// the client puts the bucket segment in the request path, which AWS appends to
// the key. ListObjects does not: the list prefix it sends omits that segment,
// so it matches none of the stored keys and restore finds nothing.
//
// FolderPrefixStore fixes listing by prepending the folder to the list prefix
// (and to the StartAfter checkpoint marker) and stripping it back off the
// returned keys, so the rest of the codebase sees keys exactly as it would
// against a dedicated bucket. Every other operation delegates unchanged - the
// client's own bucket->key fold already places them under the folder.
type FolderPrefixStore struct {
	inner  store.FileStorage
	prefix string // folder including a single trailing slash, e.g. "azul-backup-1-streams/"
}

// NewFolderPrefixStore wraps inner so that List operates under folder. If folder
// is empty the inner store is returned unchanged.
func NewFolderPrefixStore(inner store.FileStorage, folder string) store.FileStorage {
	folder = strings.TrimSuffix(folder, "/")
	if folder == "" {
		return inner
	}
	return &FolderPrefixStore{inner: inner, prefix: folder + "/"}
}

func (s *FolderPrefixStore) Put(source, label, id string, data io.ReadCloser, fileSize int64) error {
	return s.inner.Put(source, label, id, data, fileSize)
}

func (s *FolderPrefixStore) Fetch(source, label, id string, opts ...store.FileStorageFetchOption) (store.DataSlice, error) {
	return s.inner.Fetch(source, label, id, opts...)
}

func (s *FolderPrefixStore) Exists(source, label, id string) (bool, error) {
	return s.inner.Exists(source, label, id)
}

func (s *FolderPrefixStore) Delete(source, label, id string, opts ...store.FileStorageDeleteOption) (bool, error) {
	return s.inner.Delete(source, label, id, opts...)
}

func (s *FolderPrefixStore) Copy(sourceOld, labelOld, idOld, sourceNew, labelNew, idNew string) error {
	return s.inner.Copy(sourceOld, labelOld, idOld, sourceNew, labelNew, idNew)
}

func (s *FolderPrefixStore) List(ctx context.Context, prefix string, startAfter string) <-chan store.FileStorageObjectListInfo {
	// When one bucket is used, files are located under a key such as
	// `azul-backup-1-streams/source/label`, however the list query sends the prefix as `source/label`
	innerStartAfter := startAfter
	if innerStartAfter != "" {
		// Add -streams or -events prefix
		innerStartAfter = s.prefix + innerStartAfter
		// This becomes something like azul-backup-1-streams/source/label/01
	}

	log.Printf("[FolderPrefixStore.List] folder=%q caller prefix=%q startAfter=%q -> inner prefix=%q inner startAfter=%q",
		s.prefix, prefix, startAfter, s.prefix+prefix, innerStartAfter)

	out := make(chan store.FileStorageObjectListInfo)
	go func() {
		defer close(out)
		var count int
		// s.prefix+prefix becomes "azul-backup-1-streams/source/label/"
		for obj := range s.inner.List(ctx, s.prefix+prefix, innerStartAfter) {
			rawKey := obj.Key
			// Strip the folder back off
			obj.Key = strings.TrimPrefix(obj.Key, s.prefix)
			// Derive source/label/id from the stripped key so callers see the
			// same values they would against a dedicated bucket.
			obj.Source, obj.Label, obj.Id = splitLastThree(obj.Key)
			count++
			log.Printf("[FolderPrefixStore.List] object %d: raw key=%q -> stripped key=%q (source=%q label=%q id=%q)",
				count, rawKey, obj.Key, obj.Source, obj.Label, obj.Id)
			select {
			case <-ctx.Done():
				log.Printf("[FolderPrefixStore.List] context cancelled after %d objects (inner prefix=%q)", count, s.prefix+prefix)
				return
			case out <- obj:
			}
		}
		if count == 0 {
			// Diagnostic: the qualified prefix matched nothing. Probe the real key
			// layout so we can tell whether keys actually live under s.prefix or
			// whether the request is failing/empty (BucketInEndpoint misconfig?).
			logSampleKeys(ctx, s.inner, "", "bucket root (no prefix)")
			logSampleKeys(ctx, s.inner, s.prefix, "folder root (s.prefix)")
		}
		log.Printf("[FolderPrefixStore.List] done: emitted %d objects for inner prefix=%q", count, s.prefix+prefix)
	}()
	return out
}

// logSampleKeys lists up to a handful of raw keys under probePrefix and logs
// them, to reveal the true key layout when a list unexpectedly returns nothing.
// Diagnostic only - remove once the listing issue is resolved.
func logSampleKeys(ctx context.Context, inner store.FileStorage, probePrefix, note string) {
	probeCtx, cancel := context.WithCancel(ctx)
	defer cancel() // unblock the inner goroutine if we stop early

	const maxKeys = 10
	var n int
	for obj := range inner.List(probeCtx, probePrefix, "") {
		log.Printf("[FolderPrefixStore.List] PROBE %s: raw key=%q", note, obj.Key)
		if n++; n >= maxKeys {
			log.Printf("[FolderPrefixStore.List] PROBE %s: stopped after %d keys", note, maxKeys)
			return
		}
	}
	if n == 0 {
		log.Printf("[FolderPrefixStore.List] PROBE %s: returned NO keys", note)
	}
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
