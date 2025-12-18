package cmd

import (
	"bytes"
	"context"
	"errors"
	"io"
	"testing"

	"github.com/AustralianCyberSecurityCentre/azul-bedrock/v10/gosrc/events"
	"github.com/AustralianCyberSecurityCentre/azul-bedrock/v10/gosrc/models"
	"github.com/AustralianCyberSecurityCentre/azul-bedrock/v10/gosrc/msginflight"

	bkupcom "github.com/AustralianCyberSecurityCentre/azul-backup.git/common"
	restore "github.com/AustralianCyberSecurityCentre/azul-backup.git/restore"
	"github.com/AustralianCyberSecurityCentre/azul-backup.git/testdata"
	bedclient "github.com/AustralianCyberSecurityCentre/azul-bedrock/v10/gosrc/client"
	"github.com/AustralianCyberSecurityCentre/azul-bedrock/v10/gosrc/store"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
)

// rawToGzip turns raw bytes into gzipped data
func rawToGzip(t *testing.T, raw []byte) io.ReadCloser {
	r := bytes.NewReader(raw)
	compressor := bkupcom.NewGzipCompressReader(r)
	return io.NopCloser(compressor)
}

type RestoreTestSuite struct {
	suite.Suite
	streamStore      *store.StoreMem
	eventStore       *store.StoreMem
	dpStreamsClient  *bedclient.MockClientInterface
	dpEventsClients  map[string]map[string]bedclient.ClientInterface
	dpEventsClientsT map[string]map[string]*bedclient.MockClientInterface

	dpEvTestingClient *bedclient.MockClientInterface

	rt *Restore
}

func (s *RestoreTestSuite) SetupTest() {
	bkupcom.ResetSettings()

	st := bkupcom.Settings
	st.Sources.Sources = map[string]models.SourceItem{}
	st.Sources.Sources["assemblyline"] = models.SourceItem{ExcludeFromBackup: true}
	st.Sources.Sources["testing"] = models.SourceItem{ExcludeFromBackup: false}
	st.Sources.Sources["tasking"] = models.SourceItem{ExcludeFromBackup: true}

	s.streamStore = store.NewStoreMem()
	s.eventStore = store.NewStoreMem()
	s.dpStreamsClient = bedclient.NewMockClientInterface(s.T())

	s.dpEventsClients = prepareSources("restore", s.T())
	s.dpEventsClientsT = map[string]map[string]*bedclient.MockClientInterface{}
	for source, vals := range s.dpEventsClients {
		_, ok := s.dpEventsClientsT[source]
		if !ok {
			s.dpEventsClientsT[source] = map[string]*bedclient.MockClientInterface{}
		}
		for event, client := range vals {
			s.dpEventsClientsT[source][event] = client.(*bedclient.MockClientInterface)
		}
	}

	s.dpEvTestingClient = s.dpEventsClientsT["testing"]["sourced"]

	s.rt = NewRestore()
	err := s.rt.LocalData.Reset()
	require.Nil(s.T(), err)
}

func (s *RestoreTestSuite) TestEventsStoreFetchError() {
	r := s.Require()

	mockStore := store.NewMockFileStorage(s.T())
	mockStore.EXPECT().List(mock.Anything, mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, prefix, after string) <-chan store.FileStorageObjectListInfo {
		ch := make(chan store.FileStorageObjectListInfo, 10)
		ch <- store.FileStorageObjectListInfo{Key: "testing/binary/sourced/2000-01-01T00:00:00Z-1", Source: "binary", Label: "sourced", Id: "2000-01-01T00:00:00Z-1"}
		close(ch)
		return ch
	})
	mockStore.EXPECT().Fetch(mock.Anything, mock.Anything, mock.Anything).Return(store.NewDataSlice(), errors.New("test forced a store failure"))

	// FUTURE as it never made it to dispatcher, is not recorded as an actual failure
	res := s.rt.DoRestore(s.streamStore, mockStore, s.dpStreamsClient, s.dpEventsClients)
	r.Equal(res, map[string]*restore.RestoreStats{
		"testing": {EventsOk: 0, EventsFail: 0, StreamsOk: 0, StreamsFail: 0, StreamsComplete: true},
		"system":  {EventsOk: 0, EventsFail: 0, EventTypesComplete: 0, StreamsOk: 0, StreamsFail: 0, StreamsComplete: true},
	})
}

func (s *RestoreTestSuite) TestEventsDPError() {
	var err error
	r := s.Require()

	// add events to event store
	td1 := testdata.GetDataBytes("data/samples/testing-binary-sourced-100.avro")
	compressed := rawToGzip(s.T(), td1)
	err = s.eventStore.Put("testing", "binary/sourced", "2000-01-01T00:00:00Z-1", compressed, -1)
	r.Nil(err)

	s.dpEvTestingClient.EXPECT().PostEventsBytes(mock.Anything, mock.Anything).Return(
		nil, errors.New("test forced a dp failure"),
	)

	// FUTURE as it never made it to dispatcher, is not recorded as an actual failure
	res := s.rt.DoRestore(s.streamStore, s.eventStore, s.dpStreamsClient, s.dpEventsClients)
	r.Equal(res, map[string]*restore.RestoreStats{
		"testing": {EventsOk: 0, EventsFail: 0, EventTypesComplete: 4, StreamsOk: 0, StreamsFail: 0, StreamsComplete: true},
		"system":  {EventsOk: 0, EventsFail: 0, EventTypesComplete: 2, StreamsOk: 0, StreamsFail: 0, StreamsComplete: true},
	})
}

func (s *RestoreTestSuite) TestEventsInvalid() {
	var err error
	r := s.Require()

	// add bad events to event store
	td2 := []byte("{}")
	compressed := rawToGzip(s.T(), td2)
	err = s.eventStore.Put("testing", "binary/sourced", "2000-01-01T01:00:00Z-1", compressed, int64(len(td2)))
	r.Nil(err)

	td3 := []byte("{}df")
	compressed = rawToGzip(s.T(), td3)
	err = s.eventStore.Put("testing", "binary/sourced", "2000-01-01T02:00:00Z-1", compressed, int64(len(td3)))
	r.Nil(err)

	// check events to dispatcher
	s.dpEvTestingClient.EXPECT().PostEventsBytes(mock.Anything, mock.Anything).RunAndReturn(
		func(b1 []byte, b2 *bedclient.PublishBytesOptions) (*models.ResponsePostEvent, error) {
			// Verify plugins will get paused
			require.True(s.T(), b2.PausePlugins)
			// verify not valid avro
			_, err := msginflight.AvroBulkToMsgInFlights(b1, events.ModelBinary)
			r.NotNil(err)
			return &models.ResponsePostEvent{TotalOk: 0, TotalFailures: 1, Failures: []models.ResponsePostEventFailure{
				{Event: string(td2), Error: "errored"},
			}}, nil
		}).Once()

	// check events to dispatcher
	s.dpEvTestingClient.EXPECT().PostEventsBytes(mock.Anything, mock.Anything).RunAndReturn(
		func(b1 []byte, b2 *bedclient.PublishBytesOptions) (*models.ResponsePostEvent, error) {
			// Verify plugins will get paused
			require.True(s.T(), b2.PausePlugins)
			// verify not valid avro
			_, err := msginflight.AvroBulkToMsgInFlights(b1, events.ModelBinary)
			r.NotNil(err)
			return &models.ResponsePostEvent{TotalOk: 0, TotalFailures: 1, Failures: []models.ResponsePostEventFailure{
				{Event: string(td3), Error: "errored"},
			}}, nil
		}).Once()

	// run the restore
	res := s.rt.DoRestore(s.streamStore, s.eventStore, s.dpStreamsClient, s.dpEventsClients)
	r.Equal(res, map[string]*restore.RestoreStats{
		"testing": {EventsOk: 0, EventsFail: 2, EventTypesComplete: 5, StreamsOk: 0, StreamsFail: 0, StreamsComplete: true},
		"system":  {EventsOk: 0, EventsFail: 0, EventTypesComplete: 2, StreamsOk: 0, StreamsFail: 0, StreamsComplete: true},
	})
}

func (s *RestoreTestSuite) TestEvents() {
	var err error
	r := s.Require()

	// add events to event store
	td1 := testdata.GetDataBytes("data/samples/testing-binary-sourced-100.avro")
	compressed := rawToGzip(s.T(), td1)
	err = s.eventStore.Put("testing", "binary/sourced", "2000-01-01T00:00:00Z-1", compressed, int64(len(td1)))
	r.Nil(err)

	// check events to dispatcher
	s.dpEvTestingClient.EXPECT().PostEventsBytes(mock.Anything, mock.Anything).RunAndReturn(
		func(b1 []byte, b2 *bedclient.PublishBytesOptions) (*models.ResponsePostEvent, error) {
			// Verify plugins will get paused
			require.True(s.T(), b2.PausePlugins)
			_, err := msginflight.AvroBulkToMsgInFlights(b1, events.ModelBinary)
			r.Nil(err)
			return &models.ResponsePostEvent{TotalOk: 1, TotalFailures: 0}, nil
		})

	// run the restore
	res := s.rt.DoRestore(s.streamStore, s.eventStore, s.dpStreamsClient, s.dpEventsClients)
	r.Equal(res, map[string]*restore.RestoreStats{
		"testing": {EventsOk: 1, EventsFail: 0, EventTypesComplete: 5, StreamsOk: 0, StreamsFail: 0, StreamsComplete: true},
		"system":  {EventsOk: 0, EventsFail: 0, EventTypesComplete: 2, StreamsOk: 0, StreamsFail: 0, StreamsComplete: true},
	})
}

func (s *RestoreTestSuite) TestStreamsInvalid() {
	r := s.Require()

	// add binary to s3 - not gzipped so will fail when restoring
	// This is implicitly tested by not adding any EXPECT() handlers
	rawData := []byte("hello")
	reader := bytes.NewReader(rawData)
	readCloser := io.NopCloser(reader)
	err := s.streamStore.Put("testing", "content", "ee303d3c6d7cfa24d42e6348bdd1103a26de77a887e9dbee3dd1fe6304414f69", readCloser, int64(len(rawData)))
	r.Nil(err)

	// run the restore
	res := s.rt.DoRestore(s.streamStore, s.eventStore, s.dpStreamsClient, s.dpEventsClients)
	r.Equal(res, map[string]*restore.RestoreStats{
		"testing": {EventsOk: 0, EventsFail: 0, EventTypesComplete: 5, StreamsOk: 0, StreamsFail: 1, StreamsComplete: true},
		"system":  {EventsOk: 0, EventsFail: 0, EventTypesComplete: 2, StreamsOk: 0, StreamsFail: 0, StreamsComplete: true},
	})
}

func (s *RestoreTestSuite) TestStreamsStoreError() {
	r := s.Require()

	mockStore := store.NewMockFileStorage(s.T())
	mockStore.EXPECT().List(mock.Anything, mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, prefix, after string) <-chan store.FileStorageObjectListInfo {
		s.T().Logf("%v", prefix)
		ch := make(chan store.FileStorageObjectListInfo, 10)
		if prefix == "testing/" {
			ch <- store.FileStorageObjectListInfo{Key: "testing/content/ee303d3c6d7cfa24d42e6348bdd1103a26de77a887e9dbee3dd1fe6304414f69", Source: "testing", Label: "content", Id: "ee303d3c6d7cfa24d42e6348bdd1103a26de77a887e9dbee3dd1fe6304414f69"}
		}
		close(ch)
		return ch
	})
	mockStore.EXPECT().Fetch(mock.Anything, mock.Anything, mock.Anything).Return(store.NewDataSlice(), errors.New("test forced a store failure"))

	res := s.rt.DoRestore(mockStore, s.eventStore, s.dpStreamsClient, s.dpEventsClients)
	r.Equal(res, map[string]*restore.RestoreStats{
		"testing": {EventsOk: 0, EventsFail: 0, EventTypesComplete: 5, StreamsOk: 0, StreamsFail: 1},
		"system":  {EventsOk: 0, EventsFail: 0, EventTypesComplete: 2, StreamsOk: 0, StreamsFail: 0, StreamsComplete: true},
	})
}

func (s *RestoreTestSuite) TestStreamsDPError() {
	var err error
	r := s.Require()

	// add events to event store
	compressed := rawToGzip(s.T(), []byte("hello"))
	err = s.streamStore.Put("testing", "content", "ee303d3c6d7cfa24d42e6348bdd1103a26de77a887e9dbee3dd1fe6304414f69", compressed, -1)
	r.Nil(err)

	s.dpStreamsClient.EXPECT().PostStream(mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, errors.New("test forced a dp failure"))

	res := s.rt.DoRestore(s.streamStore, s.eventStore, s.dpStreamsClient, s.dpEventsClients)
	r.Equal(res, map[string]*restore.RestoreStats{
		"testing": {EventsOk: 0, EventsFail: 0, EventTypesComplete: 5, StreamsOk: 0, StreamsFail: 1},
		"system":  {EventsOk: 0, EventsFail: 0, EventTypesComplete: 2, StreamsOk: 0, StreamsFail: 0, StreamsComplete: true},
	})
}

func (s *RestoreTestSuite) TestStreams() {
	var err error
	r := s.Require()

	// add binary to s3 with gzip compression
	compressed := rawToGzip(s.T(), []byte("hello"))
	err = s.streamStore.Put("testing", "content", "ee303d3c6d7cfa24d42e6348bdd1103a26de77a887e9dbee3dd1fe6304414f69", compressed, -1)
	r.Nil(err)

	// check we got correct streams to dispatcher
	s.dpStreamsClient.EXPECT().PostStream("testing", events.DatastreamLabel("content"), mock.Anything, mock.Anything).RunAndReturn(
		func(s1 string, s2 events.DatastreamLabel, s3 io.Reader, s4 *bedclient.PostStreamStruct) (*events.BinaryEntityDatastream, error) {
			resp := events.BinaryEntityDatastream{}
			raw, err := io.ReadAll(s3)
			r.Nil(err)
			r.Equal(string(raw), string("hello"))
			return &resp, nil
		}).Once()

	// run the restore
	res := s.rt.DoRestore(s.streamStore, s.eventStore, s.dpStreamsClient, s.dpEventsClients)
	r.Equal(res, map[string]*restore.RestoreStats{
		"testing": {EventsOk: 0, EventsFail: 0, EventTypesComplete: 5, StreamsOk: 1, StreamsFail: 0, StreamsComplete: true},
		"system":  {EventsOk: 0, EventsFail: 0, EventTypesComplete: 2, StreamsOk: 0, StreamsFail: 0, StreamsComplete: true},
	})
}

func TestRestore(t *testing.T) {
	suite.Run(t, new(RestoreTestSuite))
}
