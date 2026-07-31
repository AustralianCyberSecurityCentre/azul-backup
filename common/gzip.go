package common

import (
	"bytes"
	"compress/gzip"
	"io"
)

// Need to read a stream and compress
// https://gist.github.com/tomcatzh/cf8040820962e0f8c04700eb3b2f26be
func NewGzipCompressReader(source io.Reader) (io.Reader, error) {
	r, w := io.Pipe()
	go func() {
		defer w.Close()

		zip, err := gzip.NewWriterLevel(w, gzip.BestSpeed)
		if err != nil {
			w.CloseWithError(err)
		}
		defer zip.Close()
		_, err = io.Copy(zip, source)
		if err != nil {
			w.CloseWithError(err)
		}
		zip.Flush()
		err = zip.Close()
		if err != nil {
			w.CloseWithError(err)
		}
	}()
	return r, nil
}

func NewGzipDecompressBytes(source []byte) ([]byte, error) {
	reader := bytes.NewReader([]byte(source))
	return NewGzipDecompressReader(reader)
}

func NewGzipDecompressReader(reader io.Reader) ([]byte, error) {
	zipRead, err := gzip.NewReader(reader)
	if err != nil {
		return nil, err
	}
	return io.ReadAll(zipRead)
}

func NewGzipDecompressReaderAsReader(reader io.Reader) (io.Reader, error) {
	zipRead, err := gzip.NewReader(reader)
	if err != nil {
		return nil, err
	}
	return zipRead, nil
}
