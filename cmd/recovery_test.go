package cmd

import (
	"bufio"
	"bytes"
	"io"
	"sync"
	"testing"
	"time"

	"github.com/AustralianCyberSecurityCentre/azul-bedrock/v10/gosrc/events"
	"github.com/AustralianCyberSecurityCentre/azul-bedrock/v10/gosrc/models"

	"github.com/AustralianCyberSecurityCentre/azul-backup.git/backup"
	bkupcom "github.com/AustralianCyberSecurityCentre/azul-backup.git/common"
	restore "github.com/AustralianCyberSecurityCentre/azul-backup.git/restore"
	"github.com/AustralianCyberSecurityCentre/azul-backup.git/testdata"
	bedclient "github.com/AustralianCyberSecurityCentre/azul-bedrock/v10/gosrc/client"
	"github.com/AustralianCyberSecurityCentre/azul-bedrock/v10/gosrc/msginflight"
	"github.com/AustralianCyberSecurityCentre/azul-bedrock/v10/gosrc/store"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
)

type RecoveryTestSuite struct {
	suite.Suite
	streamStore      *store.StoreMem
	eventStore       *store.StoreMem
	dpStreamsClient  *bedclient.MockClientInterface
	dpEventsClients  map[string]map[string]bedclient.ClientInterface
	dpEventsClientsT map[string]map[string]*bedclient.MockClientInterface

	bk *Backup
	rt *Restore
}

func (s *RecoveryTestSuite) SetupTest() {
	bkupcom.ResetSettings()

	st := bkupcom.Settings
	st.Sources.Sources = map[string]models.SourceItem{}
	st.Sources.Sources["assemblyline"] = models.SourceItem{ExcludeFromBackup: true}
	st.Sources.Sources["testing"] = models.SourceItem{ExcludeFromBackup: false}
	st.Sources.Sources["tasking"] = models.SourceItem{ExcludeFromBackup: false}
	st.Sources.Sources["samples"] = models.SourceItem{ExcludeFromBackup: false}
	st.EventBatchSize = 20

	s.streamStore = store.NewStoreMem()
	s.eventStore = store.NewStoreMem()
	s.dpStreamsClient = bedclient.NewMockClientInterface(s.T())

	s.dpEventsClients = prepareSources("backup", s.T())
	s.dpEventsClientsT = map[string]map[string]*bedclient.MockClientInterface{}
	for source, vals := range s.dpEventsClients {
		_, ok := s.dpEventsClientsT[source]
		if !ok {
			s.dpEventsClientsT[source] = map[string]*bedclient.MockClientInterface{}
		}
		for event, client := range vals {
			s.dpEventsClientsT[source][event] = client.(*bedclient.MockClientInterface)
			// set default response for get events
			s.dpEventsClientsT[source][event].EXPECT().GetEventsBytes(mock.Anything).Return(
				[]byte{}, &models.EventResponseInfo{Ready: true, Fetched: 1}, nil,
			).Maybe()
		}
	}

	s.bk = NewBackup()
	s.rt = NewRestore()
	err := s.bk.LocalData.Reset()
	require.Nil(s.T(), err)
}

// provides all events and signals when they are consumed
func (s *RecoveryTestSuite) provideEvents(client *bedclient.MockClientInterface, filename string, model events.Model, wgBackup *sync.WaitGroup) {
	wgBackup.Add(1)
	signalFinish := false
	count := 0
	msgs, err := msginflight.AvroBulkToMsgInFlights(testdata.GetDataBytes(filename), model)
	s.Nil(err)
	client.EXPECT().GetEventsBytes(mock.Anything).Unset()
	client.EXPECT().GetEventsBytes(mock.Anything).RunAndReturn(
		func(fes *bedclient.FetchEventsStruct) ([]byte, *models.EventResponseInfo, error) {
			var subset []*msginflight.MsgInFlight
			if count > len(msgs) {
				if !signalFinish {
					wgBackup.Done()
					signalFinish = true
				} else {
					// prevent busy wait
					time.Sleep(time.Millisecond * 10)
				}
			} else if count+10 >= len(msgs) {
				subset = msgs[count:]
			} else {
				subset = msgs[count : count+10]
			}
			count += 10
			bulk, err := msginflight.MsgInFlightsToAvroBulk(subset, model)
			if err != nil {
				panic(err)
			}
			return bulk, &models.EventResponseInfo{Ready: true, Fetched: len(subset)}, nil
		})
	// restore events
	client.EXPECT().PostEventsBytes(mock.Anything, mock.Anything).RunAndReturn(
		func(b1 []byte, b2 *bedclient.PublishBytesOptions) (*models.ResponsePostEvent, error) {
			msgs, err := msginflight.AvroBulkToMsgInFlights(b1, model)
			if err != nil {
				panic(err)
			}
			totalok := len(msgs)
			require.True(s.T(), b2.PausePlugins)
			return &models.ResponsePostEvent{TotalOk: totalok, TotalFailures: 0}, nil
		}).Maybe()
}

func (s *RecoveryTestSuite) TestBackupRestore() {
	r := s.Require()
	dpe := s.dpEventsClientsT
	wgBackup := sync.WaitGroup{}
	s.provideEvents(dpe["samples"]["sourced"], "data/samples/samples-binary-sourced-100.avro", events.ModelBinary, &wgBackup)
	s.provideEvents(dpe["samples"]["extracted"], "data/samples/samples-binary-extracted-100.avro", events.ModelBinary, &wgBackup)
	s.provideEvents(dpe["testing"]["sourced"], "data/samples/testing-binary-sourced-100.avro", events.ModelBinary, &wgBackup)
	s.provideEvents(dpe["system"]["delete"], "data/samples/system-delete-default-2.avro", events.ModelDelete, &wgBackup)
	s.provideEvents(dpe["system"]["insert"], "data/samples/system-insert-default-100.avro", events.ModelInsert, &wgBackup)

	// return same binary every time
	s.dpStreamsClient.EXPECT().DownloadBinary(mock.Anything, mock.Anything, mock.Anything).RunAndReturn(func(s1 string, s2 events.DatastreamLabel, s3 string) (*bufio.Reader, error) {
		return bufio.NewReader(bytes.NewReader([]byte("hello"))), nil
	})

	// restore streams
	s.dpStreamsClient.EXPECT().PostStream(mock.Anything, mock.Anything, mock.Anything, mock.Anything).RunAndReturn(
		func(s1 string, s2 events.DatastreamLabel, s3 io.Reader, s4 *bedclient.PostStreamStruct) (*events.BinaryEntityDatastream, error) {
			return &events.BinaryEntityDatastream{Sha256: "fake"}, nil
		})

	// shut down backup when all events are consumed
	go func() {
		wgBackup.Wait()
		s.bk.CtxEventsCancel()
	}()

	// perform backup and check stats
	resBackup := s.bk.DoBackup(s.streamStore, s.eventStore, s.dpStreamsClient, s.dpEventsClients)
	r.Equal(resBackup, map[string]*backup.BackupStats{
		"samples": {EventsOk: 200, EventsInvalid: 0, EventsFail: 0, StreamsOk: 182, StreamsMissing: 0, StreamsFail: 0},
		"tasking": {EventsOk: 0, EventsInvalid: 0, EventsFail: 0, StreamsOk: 0, StreamsMissing: 0, StreamsFail: 0},
		"testing": {EventsOk: 100, EventsInvalid: 0, EventsFail: 0, StreamsOk: 89, StreamsMissing: 0, StreamsFail: 0},
		"system":  {EventsOk: 102, EventsInvalid: 0, EventsFail: 0, StreamsOk: 0, StreamsMissing: 0, StreamsFail: 0},
	})

	// check events were backed up in the expected way
	r.Equal(getKeys(s.eventStore.Data), []string{
		"samples/binary/extracted/2025-07-30T06:02:59.867075Z-20-dede7be1557d3d4aad1ac5bea8668cbf",
		"samples/binary/extracted/2025-07-30T06:05:10.845088Z-20-e6ea125ce254c5ac49cd718705a4e6df",
		"samples/binary/extracted/2025-07-30T06:05:47.625191Z-20-a984e3e91aae8334595ed9d8c2d23fbc",
		"samples/binary/extracted/2025-07-30T06:06:28.080151Z-20-ab58ac55f987e5c1064a4f48cb9ba983",
		"samples/binary/extracted/2025-07-30T06:08:02.749458Z-20-7170168ea9675be84e5f70ab48339012",
		"samples/binary/sourced/2025-07-30T11:00:40Z-20-852596e25bf83fe8717f789c401d8842",
		"samples/binary/sourced/2025-07-30T11:02:26Z-20-20350706b323f1bd793542cc4727f772",
		"samples/binary/sourced/2025-07-30T11:04:00Z-20-11efbc25be09f35ee75f691a081a6a67",
		"samples/binary/sourced/2025-07-30T11:06:59Z-20-9fe8b2d48a8617781b111fbe923eee9c",
		"samples/binary/sourced/2025-07-30T11:08:50Z-20-b7be3a0f03ed938e7c0b59e79e1be360",
		"system/delete/default/2025-08-13T18:13:40.005548Z-2-260485ec8b073977d63de201419239c9",
		"system/insert/default/2025-05-09T18:12:58.794902Z-20-ef0d098b235319a1c8245a5cc49e1fa0",
		"system/insert/default/2025-05-25T18:06:13.621205Z-20-45826ea5ccdd515bf8de1efae0115c61",
		"system/insert/default/2025-06-09T18:09:13.465197Z-20-0e030b0188d476ac0fe02964f0f870bb",
		"system/insert/default/2025-07-07T05:13:35.1932Z-20-d85c25868c500516b0cbcd00ef623f48",
		"system/insert/default/2025-08-06T02:07:00Z-20-5f20c337c6a4a48ac03b74b8959553fa",
		"testing/binary/sourced/2025-07-31T21:55:00Z-20-545bbdc4e5b6d782cd3f5bba20f3f170",
		"testing/binary/sourced/2025-08-01T18:19:00Z-20-59f4aa05d3c4a921e7bf9d16fb453575",
		"testing/binary/sourced/2025-08-03T18:10:00Z-20-f1ce0f06ebb1e413ae0933733bfbe2a0",
		"testing/binary/sourced/2025-08-04T05:28:57.211667Z-20-bf3e1437b52e7934dcb6c39af1088992",
		"testing/binary/sourced/2025-08-05T18:10:00Z-20-43ac3f293c7824223fcc26c555c56205",
	})
	r.Equal(len(getKeys(s.streamStore.Data)), 271)

	// perform restore and check that our final counts are the same as what was backed up
	resRestore := s.rt.DoRestore(s.streamStore, s.eventStore, s.dpStreamsClient, s.dpEventsClients)
	r.Equal(resRestore, map[string]*restore.RestoreStats{
		"samples": {EventsOk: 200, EventsFail: 0, EventTypesComplete: 5, StreamsOk: 182, StreamsFail: 0, StreamsComplete: true},
		"tasking": {EventsOk: 0, EventsFail: 0, EventTypesComplete: 5, StreamsOk: 0, StreamsFail: 0, StreamsComplete: true},
		"testing": {EventsOk: 100, EventsFail: 0, EventTypesComplete: 5, StreamsOk: 89, StreamsFail: 0, StreamsComplete: true},
		"system":  {EventsOk: 102, EventsFail: 0, EventTypesComplete: 2, StreamsOk: 0, StreamsFail: 0, StreamsComplete: true},
	})

	// FUTURE verify that several restored events and streams were as expected
}

func TestRecovery(t *testing.T) {
	suite.Run(t, new(RecoveryTestSuite))
}
