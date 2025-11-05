package common

import (
	"os"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestReadAllConfig(t *testing.T) {
	envs := os.Environ()
	os.Clearenv()

	os.Setenv("BK.DISPATCHER_EVENTS", "https://blah.dispatcher.local")
	yamlContent := `
sources:
  assemblyline:
    description: Samples forwarded to Azul by Assemblyline
    elastic:
      number_of_replicas: 2
      number_of_shards: 3
    kafka:
      numpartitions: 3
      replicationfactor: 1
      config:
        retention.ms: -1
        segment.bytes: 1073741824
        cleanup.policy: compact
    icon_class: fa-gears
    partition_unit: "year"
    references:
      - description: Priority of the submission
        name: priority
        required: true
      - description: Short description of what/why samples collected
        name: description
        required: true
      - description: User who submitted the sample
        name: user
        required: false
  virustotal:
    exclude_from_backup: true
`
	os.Setenv("BK.SOURCES", yamlContent)
	cfg := parseSettings()
	require.Equal(t, cfg.DispatcherEvents, "https://blah.dispatcher.local")
	require.Equal(t, cfg.Sources.Sources["assemblyline"].PartitionUnit, "year")
	require.Equal(t, cfg.Sources.Sources["assemblyline"].ExcludeFromBackup, false)
	require.Equal(t, cfg.Sources.Sources["virustotal"].ExcludeFromBackup, true)

	// restore variables
	os.Clearenv()
	for _, e := range envs {
		pair := strings.SplitN(e, "=", 2)
		os.Setenv(pair[0], pair[1])
	}
}
