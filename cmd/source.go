package cmd

import (
	"log"
	"strings"
	"testing"

	"github.com/AustralianCyberSecurityCentre/azul-bedrock/v9/gosrc/events"

	bkupcom "github.com/AustralianCyberSecurityCentre/azul-backup.git/common"
	bedclient "github.com/AustralianCyberSecurityCentre/azul-bedrock/v9/gosrc/client"
)

// NewAuthor generates a backup/restore author
func NewAuthor(operation, source, event, suffix string) *events.PluginEntity {
	name := strings.Join([]string{operation + "-events", source, event}, "-")
	if suffix != "" {
		name = name + "-" + suffix
	}
	return &events.PluginEntity{
		Name:        name,
		Version:     bkupcom.RECOVERY_VERSION,
		Contact:     "azul@asd.gov.au",
		Category:    "plugin",
		Description: "Backup critical Azul data.",
		Features:    []events.PluginEntityFeature{},
	}
}

// prepareSources will process configured sources and events to generate dispatcher clients
func prepareSources(operation string, forTest *testing.T) map[string]map[string]bedclient.ClientInterface {
	st := bkupcom.Settings
	// generate event clients
	dpEventClients := map[string]map[string]bedclient.ClientInterface{}
	for source, cfg := range st.Sources.Sources {
		if cfg.ExcludeFromBackup {
			continue
		}
		dpEventClients[source] = map[string]bedclient.ClientInterface{}
		for _, action := range events.ActionsBinary {
			author := NewAuthor(operation, source, action.Str(), st.BackupID)
			if forTest == nil {
				dpEventClients[source][action.Str()] = bedclient.NewClient(st.DispatcherEvents, st.DispatcherStreams, *author, st.DeploymentKey)
			} else {
				dpEventClients[source][action.Str()] = bedclient.NewMockClientInterface(forTest)
			}
		}
		log.Printf("'%s' source will process events: %v", source, events.ActionsBinary)
	}

	// clients for system models (for system, we use model name instead of action)
	dpEventClients["system"] = map[string]bedclient.ClientInterface{}
	systemModels := []string{
		string(events.ModelInsert),
		string(events.ModelDelete),
	}
	for _, event := range systemModels {
		author := NewAuthor(operation, "system", event, st.BackupID)
		if forTest == nil {
			dpEventClients["system"][event] = bedclient.NewClient(st.DispatcherEvents, st.DispatcherStreams, *author, st.DeploymentKey)
		} else {
			dpEventClients["system"][event] = bedclient.NewMockClientInterface(forTest)
		}
	}
	return dpEventClients
}
