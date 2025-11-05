package store

import "github.com/minio/minio-go/v7"

type StoreS3Interface interface {
	Put(id string, data []byte) error
	Fetch(id string) ([]byte, error)
	Exists(id string) (bool, error)
	Delete(id string) (bool, error)
	List(options minio.ListObjectsOptions) <-chan minio.ObjectInfo
}
