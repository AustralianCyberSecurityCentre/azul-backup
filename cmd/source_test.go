package cmd

import (
	"maps"
	"slices"
	"testing"

	bkupcom "github.com/AustralianCyberSecurityCentre/azul-backup.git/common"
	bedclient "github.com/AustralianCyberSecurityCentre/azul-bedrock/v9/gosrc/client"
	"github.com/AustralianCyberSecurityCentre/azul-bedrock/v9/gosrc/models"
	"github.com/stretchr/testify/require"
)

func TestPrepareSources(t *testing.T) {
	st := bkupcom.Settings
	st.Sources.Sources = map[string]models.SourceItem{}
	st.Sources.Sources["assemblyline"] = models.SourceItem{}
	st.Sources.Sources["testing"] = models.SourceItem{ExcludeFromBackup: true}
	st.Sources.Sources["tasking"] = models.SourceItem{}

	// restore authors
	clients := prepareSources("restore", nil)
	keys := slices.Collect(maps.Keys(clients))
	keysAssemblyline := slices.Collect(maps.Keys(clients["assemblyline"]))
	keysTasking := slices.Collect(maps.Keys(clients["tasking"]))
	keysSystem := slices.Collect(maps.Keys(clients["system"]))
	require.ElementsMatch(t, keys, []string{"assemblyline", "tasking", "system"})
	require.ElementsMatch(t, keysAssemblyline, []string{"sourced", "extracted", "augmented", "mapped", "enriched"})
	require.ElementsMatch(t, keysTasking, []string{"sourced", "extracted", "augmented", "mapped", "enriched"})
	require.ElementsMatch(t, keysSystem, []string{"insert", "delete"})
	require.IsType(t, &bedclient.Client{}, clients["tasking"]["sourced"])

	require.Equal(t, clients["tasking"]["sourced"].(*bedclient.Client).Author.Name, "restore-events-tasking-sourced-1")
	require.Equal(t, clients["tasking"]["sourced"].(*bedclient.Client).Author.Version, bkupcom.RECOVERY_VERSION)
	require.Equal(t, clients["tasking"]["extracted"].(*bedclient.Client).Author.Name, "restore-events-tasking-extracted-1")
	require.Equal(t, clients["assemblyline"]["sourced"].(*bedclient.Client).Author.Name, "restore-events-assemblyline-sourced-1")
	require.Equal(t, clients["system"]["insert"].(*bedclient.Client).Author.Name, "restore-events-system-insert-1")

	// backup authors
	clients = prepareSources("backup", nil)
	keys = slices.Collect(maps.Keys(clients))
	keysAssemblyline = slices.Collect(maps.Keys(clients["assemblyline"]))
	keysTasking = slices.Collect(maps.Keys(clients["tasking"]))
	keysSystem = slices.Collect(maps.Keys(clients["system"]))
	require.ElementsMatch(t, keys, []string{"assemblyline", "tasking", "system"})
	require.ElementsMatch(t, keysAssemblyline, []string{"sourced", "extracted", "augmented", "mapped", "enriched"})
	require.ElementsMatch(t, keysTasking, []string{"sourced", "extracted", "augmented", "mapped", "enriched"})
	require.ElementsMatch(t, keysSystem, []string{"insert", "delete"})
	require.IsType(t, &bedclient.Client{}, clients["tasking"]["sourced"])

	require.Equal(t, clients["tasking"]["sourced"].(*bedclient.Client).Author.Name, "backup-events-tasking-sourced-1")
	require.Equal(t, clients["tasking"]["sourced"].(*bedclient.Client).Author.Version, bkupcom.RECOVERY_VERSION)
	require.Equal(t, clients["tasking"]["extracted"].(*bedclient.Client).Author.Name, "backup-events-tasking-extracted-1")
	require.Equal(t, clients["assemblyline"]["sourced"].(*bedclient.Client).Author.Name, "backup-events-assemblyline-sourced-1")
	require.Equal(t, clients["system"]["insert"].(*bedclient.Client).Author.Name, "backup-events-system-insert-1")

}
