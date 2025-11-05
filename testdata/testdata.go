package testdata

import (
	"embed"
	"encoding/json"
	"log"
	"path"
	"runtime"
	"testing"

	"github.com/stretchr/testify/require"
)

// dir of this go module, so tests can load files beneath it
var Dir string

//go:embed data
var TestData embed.FS

func GetDataBytes(path string) []byte {
	ret, err := TestData.ReadFile(path)
	if err != nil {
		log.Fatalf("could not load test file %v: %v", path, err)
	}
	return ret
}

func init() {
	_, filename, _, _ := runtime.Caller(0)
	Dir = path.Dir(filename) + "/../../test-files/"
}

// compare two structures by first dumping to json
// this removes problems with time, and other non-normalised data
func MarshalEqual(t *testing.T, in1, in2 interface{}) {
	raw1, err := json.Marshal(in1)
	require.Nil(t, err)
	raw2, err := json.Marshal(in2)
	require.Nil(t, err)
	require.JSONEq(t, string(raw1), string(raw2))
}
