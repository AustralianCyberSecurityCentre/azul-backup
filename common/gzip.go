package common

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"io"
)

// Need to read a stream and compress
// https://gist.github.com/tomcatzh/cf8040820962e0f8c04700eb3b2f26be
func NewGzipCompressReader(source io.Reader) io.Reader {
	var intermediateBuffer bytes.Buffer
	zip, err := gzip.NewWriterLevel(&intermediateBuffer, gzip.BestSpeed)
	if err != nil {
		panic("PANIC1!")
	}
	defer zip.Close()
	_, err = io.Copy(zip, source)
	if err != nil {
		panic("PANIC2!")
	}
	zip.Flush()
	return bufio.NewReader(&intermediateBuffer)

	// Old piped writer doesn't work if the entire file isn't read via io.readAll()
	// The issue is noticed when AES is enabled.
	// r, w := io.Pipe()
	// go func() {
	// 	defer w.Close()

	// 	zip, err := gzip.NewWriterLevel(w, gzip.BestSpeed)
	// 	if err != nil {
	// 		w.CloseWithError(err)
	// 	}
	// 	defer zip.Close()
	// 	_, err = io.Copy(zip, source)
	// 	if err != nil {
	// 		w.CloseWithError(err)
	// 	}
	// 	zip.Flush()
	// 	err = zip.Close()
	// 	if err != nil {
	// 		w.CloseWithError(err)
	// 	}
	// }()
	// return r
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
