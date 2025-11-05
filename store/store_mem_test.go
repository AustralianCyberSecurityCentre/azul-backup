package store

import (
	"fmt"
	"testing"

	"github.com/minio/minio-go/v7"
	"github.com/stretchr/testify/require"
)

func TestS3MemProvider(t *testing.T) {
	s3 := NewStoreMem()
	var has bool
	var err error
	var data []byte

	// not exist
	has, err = s3.Exists("a")
	require.Nil(t, err)
	require.Equal(t, has, false)
	has, err = s3.Exists("b")
	require.Nil(t, err)
	require.Equal(t, has, false)
	has, err = s3.Exists("c")
	require.Nil(t, err)
	require.Equal(t, has, false)

	// put
	err = s3.Put("a", []byte("apple"))
	require.Nil(t, err)

	err = s3.Put("b", []byte("banana"))
	require.Nil(t, err)

	// exist
	has, err = s3.Exists("a")
	require.Nil(t, err)
	require.Equal(t, has, true)
	has, err = s3.Exists("b")
	require.Nil(t, err)
	require.Equal(t, has, true)
	has, err = s3.Exists("c")
	require.Nil(t, err)
	require.Equal(t, has, false)

	// fetch
	data, err = s3.Fetch("a")
	require.Nil(t, err)
	require.Equal(t, string(data), "apple")
	data, err = s3.Fetch("b")
	require.Nil(t, err)
	require.Equal(t, string(data), "banana")
	_, err = s3.Fetch("c")
	require.NotNil(t, err)

	// delete
	ok, err := s3.Delete("a")
	require.Nil(t, err)
	require.Equal(t, ok, true)
	ok, err = s3.Delete("c")
	require.Nil(t, err)
	require.Equal(t, ok, false)

	// put in unsorted order
	for i := range 10 {
		txt := fmt.Sprintf("%d", i)
		err = s3.Put("pathway/z"+txt, []byte("z"+txt))
		require.Nil(t, err)
		err = s3.Put("pathway/n"+txt, []byte("n"+txt))
		require.Nil(t, err)
	}

	// list all
	ch := s3.List(minio.ListObjectsOptions{})
	require.NotNil(t, ch)
	listed := []string{}
	for obj := range ch {
		listed = append(listed, obj.Key)
	}
	require.Equal(t, len(listed), 21)

	// list start after
	ch = s3.List(minio.ListObjectsOptions{StartAfter: "pathway/n9"})
	require.NotNil(t, ch)
	listed = []string{}
	for obj := range ch {
		listed = append(listed, obj.Key)
	}
	require.Equal(t, len(listed), 10)

	// list prefix
	ch = s3.List(minio.ListObjectsOptions{Prefix: "pathway/n"})
	require.NotNil(t, ch)
	listed = []string{}
	for obj := range ch {
		listed = append(listed, obj.Key)
	}
	require.Equal(t, len(listed), 10)

}
