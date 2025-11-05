package store

import (
	"fmt"
	"slices"
	"strings"
	"sync"

	"github.com/minio/minio-go/v7"
)

/* Store files via s3 provider. */
type StoreMem struct {
	Data map[string][]byte
	mu   sync.Mutex
}

func NewStoreMem() *StoreMem {
	return &StoreMem{
		Data: map[string][]byte{},
	}
}

func (s *StoreMem) Put(id string, data []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.Data[id] = data
	return nil
}

func (s *StoreMem) Fetch(id string) ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	data, ok := s.Data[id]
	if !ok {
		return nil, fmt.Errorf("id %s not found", id)
	}
	return data, nil
}

func (s *StoreMem) Exists(id string) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, ok := s.Data[id]
	return ok, nil
}

func (s *StoreMem) Delete(id string) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, ok := s.Data[id]
	delete(s.Data, id)
	return ok, nil
}

/*List the contents of the current S3 Bucket.*/
func (s *StoreMem) List(options minio.ListObjectsOptions) <-chan minio.ObjectInfo {
	s.mu.Lock()
	defer s.mu.Unlock()
	// implement minimum needed for testing
	out := make(chan minio.ObjectInfo, 1000)

	// sort alphabetically
	keys := []string{}
	for k := range s.Data {
		keys = append(keys, k)
	}
	slices.Sort(keys)

	hitStartAfter := false

	for _, key := range keys {
		if len(options.StartAfter) > 0 && !hitStartAfter {
			if key == options.StartAfter {
				hitStartAfter = true
			}
			continue
		}
		if len(options.Prefix) > 0 && !strings.HasPrefix(key, options.Prefix) {
			continue
		}
		data := s.Data[key]
		out <- minio.ObjectInfo{
			Key:  key,
			Size: int64(len(data)),
		}
	}

	close(out)
	return out
}
