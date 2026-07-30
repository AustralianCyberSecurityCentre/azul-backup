package cmd

// import (
// 	"bufio"
// 	"bytes"
// 	"cmp"
// 	"errors"
// 	"slices"
// 	"sync"
// 	"testing"
// 	"time"

// 	"github.com/AustralianCyberSecurityCentre/azul-bedrock/v12/gosrc/events"
// 	"github.com/AustralianCyberSecurityCentre/azul-bedrock/v12/gosrc/models"

// 	"github.com/AustralianCyberSecurityCentre/azul-backup.git/backup"
// 	bkupcom "github.com/AustralianCyberSecurityCentre/azul-backup.git/common"
// 	"github.com/AustralianCyberSecurityCentre/azul-backup.git/testdata"
// 	bedclient "github.com/AustralianCyberSecurityCentre/azul-bedrock/v12/gosrc/client"
// 	"github.com/AustralianCyberSecurityCentre/azul-bedrock/v12/gosrc/msginflight"
// 	"github.com/AustralianCyberSecurityCentre/azul-bedrock/v12/gosrc/store"
// 	"github.com/stretchr/testify/mock"
// 	"github.com/stretchr/testify/require"
// 	"github.com/stretchr/testify/suite"
// )

// // getKeys returns a list of keys in the map
// func getKeys[T any, V cmp.Ordered](m map[V]T) []V {
// 	ret := []V{}
// 	for k := range m {
// 		ret = append(ret, k)
// 	}
// 	slices.Sort(ret)
// 	return ret
// }

// type BackupTestSuite struct {
// 	suite.Suite
// 	streamStore      *store.StoreMem
// 	eventStore       *store.StoreMem
// 	dpStreamsClient  *bedclient.MockClientInterface
// 	dpEventsClients  map[string]map[string]bedclient.ClientInterface
// 	dpEventsClientsT map[string]map[string]*bedclient.MockClientInterface

// 	dpEvTestingClient *bedclient.MockClientInterface
// 	dpEvDeleteClient  *bedclient.MockClientInterface
// 	dpEvInsertClient  *bedclient.MockClientInterface

// 	bk *Backup
// }

// func (s *BackupTestSuite) SetupTest() {
// 	bkupcom.ResetSettings()

// 	st := bkupcom.Settings
// 	st.Sources.Sources = map[string]models.SourceItem{}
// 	st.Sources.Sources["assemblyline"] = models.SourceItem{ExcludeFromBackup: true}
// 	st.Sources.Sources["testing"] = models.SourceItem{ExcludeFromBackup: false}
// 	st.Sources.Sources["tasking"] = models.SourceItem{ExcludeFromBackup: true}

// 	s.streamStore = store.NewStoreMem()
// 	s.eventStore = store.NewStoreMem()
// 	s.dpStreamsClient = bedclient.NewMockClientInterface(s.T())

// 	s.dpEventsClients = prepareSources("backup", s.T())
// 	s.dpEventsClientsT = map[string]map[string]*bedclient.MockClientInterface{}
// 	for source, vals := range s.dpEventsClients {
// 		_, ok := s.dpEventsClientsT[source]
// 		if !ok {
// 			s.dpEventsClientsT[source] = map[string]*bedclient.MockClientInterface{}
// 		}
// 		for event, client := range vals {
// 			s.dpEventsClientsT[source][event] = client.(*bedclient.MockClientInterface)
// 			// set default response for get events
// 			s.dpEventsClientsT[source][event].EXPECT().GetEventsBytes(mock.Anything).Return(
// 				[]byte{}, &models.EventResponseInfo{Ready: true, Fetched: 1}, nil,
// 			).Maybe()
// 		}
// 	}
// 	s.dpEvTestingClient = s.dpEventsClientsT["testing"]["extracted"]
// 	s.dpEvDeleteClient = s.dpEventsClientsT["system"]["delete"]
// 	s.dpEvInsertClient = s.dpEventsClientsT["system"]["insert"]

// 	s.bk = NewBackup()
// 	err := s.bk.LocalData.Reset()
// 	require.Nil(s.T(), err)
// }

// // provides all events and signals when they are consumed
// func (s *RecoveryTestSuite) provideEventsV2(client *bedclient.MockClientInterface, filename string, model events.Model, wgBackup *sync.WaitGroup) {
// 	wgBackup.Add(1)
// 	signalFinish := false
// 	count := 0
// 	msgs, err := msginflight.AvroBulkToMsgInFlights(testdata.GetDataBytes(filename), model)
// 	s.Nil(err)
// 	client.EXPECT().GetEventsBytes(mock.Anything).Unset()
// 	client.EXPECT().GetEventsBytes(mock.Anything).RunAndReturn(
// 		func(fes *bedclient.FetchEventsStruct) ([]byte, *models.EventResponseInfo, error) {
// 			var subset []*msginflight.MsgInFlight
// 			if count > len(msgs) {
// 				if !signalFinish {
// 					wgBackup.Done()
// 					signalFinish = true
// 				} else {
// 					// prevent busy wait
// 					time.Sleep(time.Millisecond * 10)
// 				}
// 			} else if count+10 >= len(msgs) {
// 				subset = msgs[count:]
// 			} else {
// 				subset = msgs[count : count+10]
// 			}
// 			count += 10
// 			bulk, err := msginflight.MsgInFlightsToAvroBulk(subset, model)
// 			if err != nil {
// 				panic(err)
// 			}
// 			return bulk, &models.EventResponseInfo{Ready: true, Fetched: len(subset)}, nil
// 		})
// }

// func (s *RecoveryTestSuite) TestRapidBackupShutdown() {
// 	// Verify when a rapidshutdown is requested file metadata is saved to disk.
// 	r := s.Require()
// 	dpe := s.dpEventsClientsT
// 	wgBackup := sync.WaitGroup{}
// 	s.provideEventsV2(dpe["samples"]["sourced"], "data/samples/samples-binary-sourced-100.avro", events.ModelBinary, &wgBackup)
// 	s.provideEventsV2(dpe["samples"]["extracted"], "data/samples/samples-binary-extracted-100.avro", events.ModelBinary, &wgBackup)

// 	// return same binary every time
// 	s.dpStreamsClient.EXPECT().DownloadBinary(mock.Anything, mock.Anything, mock.Anything).RunAndReturn(func(s1 string, s2 events.DatastreamLabel, s3 string) (*bufio.Reader, error) {
// 		time.Sleep(time.Millisecond * 10)
// 		return bufio.NewReader(bytes.NewReader([]byte("hellohellohellohellohellohellohellohellohello"))), nil
// 	})

// 	// shut down backup when all events are consumed
// 	go func() {
// 		wgBackup.Wait()
// 		s.bk.CtxEventsCancel()
// 	}()

// 	// Rapid shutdown like a sigterm got issued.
// 	s.bk.RapidShutdown = true

// 	// perform backup and check stats
// 	s.bk.DoBackup(s.streamStore, s.eventStore, s.dpStreamsClient, s.dpEventsClients)

// 	// Verify rapid shutdown means not all streams get saved.
// 	r.Greater(len(getKeys(s.streamStore.Data)), 10, "Expected at least 10 binaries to be inserted prior to rushed shutdown.")
// 	r.Less(len(getKeys(s.streamStore.Data)), 190, "Not all files should have been saved, the rushed shutdown should have stashed some.")
// 	streamBkupRequests, err := s.bk.LocalData.BackupStreamLoad()
// 	r.Nil(err)

// 	r.Greater(len(streamBkupRequests), 1)
// 	r.Equal(len(streamBkupRequests)+len(getKeys(s.streamStore.Data)), 182, "All streams should have been saved to disk or S3, not equal indicates duplicates or lost streams.")
// }

// func (s *BackupTestSuite) TestBadDPGetEvents() {
// 	s.dpEvTestingClient.EXPECT().GetEventsBytes(mock.Anything).Unset()

// 	r := s.Require()
// 	s.dpEvTestingClient.EXPECT().GetEventsBytes(mock.Anything).Return(nil, nil, errors.New("test forced a dp failure")).Times(10)
// 	s.dpEvTestingClient.EXPECT().GetEventsBytes(mock.Anything).Run(func(query *bedclient.FetchEventsStruct) {
// 		s.bk.CtxEventsCancel()
// 	}).Return(nil, nil, errors.New("test forced a dp failure"))

// 	// run the backup
// 	res := s.bk.DoBackup(s.streamStore, s.eventStore, s.dpStreamsClient, s.dpEventsClients)
// 	r.Equal(res, map[string]*backup.BackupStats{
// 		"testing": {EventsOk: 0, EventsInvalid: 0, EventsFail: 0, StreamsOk: 0, StreamsMissing: 0, StreamsFail: 0},
// 		"system":  {EventsOk: 0, EventsInvalid: 0, EventsFail: 0, StreamsOk: 0, StreamsMissing: 0, StreamsFail: 0},
// 	})
// }

// func (s *BackupTestSuite) TestBadDPGetBinary() {
// 	s.dpEvTestingClient.EXPECT().GetEventsBytes(mock.Anything).Unset()

// 	r := s.Require()
// 	// send a valid event once
// 	td := testdata.GetDataBytes("data/samples/testing-binary-sourced-100.avro")
// 	td = td[:len(td)-1] // remove newline (also seems to be removed in code somewhere)
// 	s.dpEvTestingClient.EXPECT().GetEventsBytes(mock.Anything).Return(td, &models.EventResponseInfo{Ready: true, Fetched: 1}, nil).Once()
// 	// signal to stop test multiple times
// 	s.dpEvTestingClient.EXPECT().GetEventsBytes(mock.Anything).Run(func(query *bedclient.FetchEventsStruct) {
// 		s.bk.CtxEventsCancel()
// 	}).Return([]byte{}, &models.EventResponseInfo{Ready: true}, nil)

// 	s.dpStreamsClient.EXPECT().DownloadBinary(mock.Anything, mock.Anything, mock.Anything).RunAndReturn(func(s1 string, s2 events.DatastreamLabel, s3 string) (*bufio.Reader, error) {
// 		return nil, errors.New("test forced a dp failure")
// 	})
// 	// run the backup
// 	res := s.bk.DoBackup(s.streamStore, s.eventStore, s.dpStreamsClient, s.dpEventsClients)
// 	r.Equal(res, map[string]*backup.BackupStats{
// 		"testing": {EventsOk: 100, EventsInvalid: 0, EventsFail: 0, StreamsOk: 0, StreamsMissing: 0, StreamsFail: 89},
// 		"system":  {EventsOk: 0, EventsInvalid: 0, EventsFail: 0, StreamsOk: 0, StreamsMissing: 0, StreamsFail: 0},
// 	})
// }

// func (s *BackupTestSuite) TestGetMissingBinary() {
// 	s.dpEvTestingClient.EXPECT().GetEventsBytes(mock.Anything).Unset()

// 	r := s.Require()
// 	// send a valid event once
// 	td := testdata.GetDataBytes("data/samples/testing-binary-sourced-100.avro")
// 	td = td[:len(td)-1] // remove newline (also seems to be removed in code somewhere)
// 	s.dpEvTestingClient.EXPECT().GetEventsBytes(mock.Anything).Return(td, &models.EventResponseInfo{Ready: true, Fetched: 1}, nil).Once()
// 	// signal to stop test multiple times
// 	s.dpEvTestingClient.EXPECT().GetEventsBytes(mock.Anything).Run(func(query *bedclient.FetchEventsStruct) {
// 		s.bk.CtxEventsCancel()
// 	}).Return([]byte{}, &models.EventResponseInfo{Ready: true}, nil)

// 	s.dpStreamsClient.EXPECT().DownloadBinary(mock.Anything, mock.Anything, mock.Anything).RunAndReturn(func(s1 string, s2 events.DatastreamLabel, s3 string) (*bufio.Reader, error) {
// 		return nil, &bedclient.HttpError{Body: "", StatusCode: 404}
// 	})

// 	// run the backup
// 	res := s.bk.DoBackup(s.streamStore, s.eventStore, s.dpStreamsClient, s.dpEventsClients)
// 	r.Equal(res, map[string]*backup.BackupStats{
// 		"testing": {EventsOk: 100, EventsInvalid: 0, EventsFail: 0, StreamsOk: 0, StreamsMissing: 89, StreamsFail: 0},
// 		"system":  {EventsOk: 0, EventsInvalid: 0, EventsFail: 0, StreamsOk: 0, StreamsMissing: 0, StreamsFail: 0},
// 	})
// }

// func (s *BackupTestSuite) TestCombined() {
// 	s.dpEvTestingClient.EXPECT().GetEventsBytes(mock.Anything).Unset()

// 	r := s.Require()

// 	// send a valid event once
// 	td := testdata.GetDataBytes("data/samples/testing-binary-sourced-100.avro")
// 	td = td[:len(td)-1] // remove newline (also seems to be removed in code somewhere)
// 	s.dpEvTestingClient.EXPECT().GetEventsBytes(mock.Anything).Return(td, &models.EventResponseInfo{Ready: true, Fetched: 1}, nil).Once()

// 	// send an invalid event once - this will get filtered
// 	s.dpEvTestingClient.EXPECT().GetEventsBytes(mock.Anything).Return([]byte("{}"), &models.EventResponseInfo{Ready: true, Fetched: 1}, nil).Once()

// 	// signal to stop test multiple times
// 	s.dpEvTestingClient.EXPECT().GetEventsBytes(mock.Anything).Run(func(query *bedclient.FetchEventsStruct) {
// 		s.bk.CtxEventsCancel()
// 	}).Return([]byte{}, &models.EventResponseInfo{Ready: true}, nil)

// 	// return same binary every time
// 	s.dpStreamsClient.EXPECT().DownloadBinary(mock.Anything, mock.Anything, mock.Anything).RunAndReturn(func(s1 string, s2 events.DatastreamLabel, s3 string) (*bufio.Reader, error) {
// 		return bufio.NewReader(bytes.NewReader([]byte("hello"))), nil
// 	})

// 	// run the backup
// 	res := s.bk.DoBackup(s.streamStore, s.eventStore, s.dpStreamsClient, s.dpEventsClients)
// 	r.Equal(res, map[string]*backup.BackupStats{
// 		"testing": {EventsOk: 100, EventsInvalid: 0, EventsFail: 0, StreamsOk: 89, StreamsMissing: 0, StreamsFail: 0},
// 		"system":  {EventsOk: 0, EventsInvalid: 0, EventsFail: 0, StreamsOk: 0, StreamsMissing: 0, StreamsFail: 0},
// 	})

// 	// inspect state of storage
// 	r.Equal(getKeys(s.streamStore.Data), []string{"testing/content/001f4f38b6a486ca8446a8f5d6f373bd58029735e1c1cfd198e6dd6754004001", "testing/content/00a2bd105a1fe412f302f74953063f1406080de0abf3e28688ad486cca8d2de9", "testing/content/01943bceab0794cdc7df92a10cbacee6848bee39f1e2d9c2a80eff1e2035ac06", "testing/content/0a06e81a781997eb705a1a6c7f0ee27310a5a045a768e568eaaf2a29595f77be", "testing/content/0a20ab3ef2076a08e7e96ecb0e54523361a8300853b46cfb9edd515323e133a3", "testing/content/0adfa37a33260bde9368838870efac74b8a44b5db31b04bfcaba8d222d574a3b", "testing/content/0ae321d27350e4dcf51f117986155a4cc9bb69da05d761ec6278436064f661f1", "testing/content/0e5a901ffbde747785f2d29ce06a695c9db58d9c3aaad20d309730f252fbdd5f", "testing/content/126d2c25861c3005924e69b1304182bd341fa2c39567cc0254898e5fa1041617", "testing/content/1692a045152f578d3c9725fc2ed5cbf4b7571e3f0d02c6a7ca124b8374be5cb2", "testing/content/17fde64347ad50791476371c7b8e685964fdcbb93f7ddbc7b259a292ebae57e7", "testing/content/1a23cc4e2350d6bb38b864d45a08397bfe5e3898f35d97200c7c9b1f2e439071", "testing/content/1bd91e69b8a5d12d4fc5c69351861c910de6b530c47dbc1e85b32de3f1d5c2f9", "testing/content/1e1c3084334556fa3bdaa216f66226ea47550cc51922442f651318891f5893d2", "testing/content/1f25c878fd0a4f71e8101cbc72f853f7d64a9442fb813bffdf1d64205ba77975", "testing/content/1f797061898ad6f531afd2ebcb8ba82b1e3dd172e07a7558e9c7a4ca1a24d553", "testing/content/267410bf62532bb21a44ee1f3002e94ffcc0323abb02f4bb543b0209a8a12f2a", "testing/content/27187f1ce402f18b2f71f3788f53435356d133ae596083495fe026571399635f", "testing/content/279d37393b7a7f33689179d5a45684fae57e89cd839ab47b4b8ddb8883fa4f4f", "testing/content/29116246b6149233c3a7b2a5f6d0975009955bf9926545f66d29439b2b45b679", "testing/content/2e6eaa206748e344438f871ecff34bd67ca941b8cb8d622f90b026a18393bf16", "testing/content/309b8678d3bf7d9bb5fab2627643bf3da5e86f45d8bf8896c75f74a1cd6f4ba5", "testing/content/30ae3be748bc7161d4b073543c56b8f3acc63e9a248e9d7fdca717d64a48a4d6", "testing/content/33470bd58e256f2b226dc021a3664eb7f624125bcbe2d5656ee7cd9447aad487", "testing/content/3694d23f972f9ba9a4a766d0fe7be713595cd222b780edc3bb91e84c63e2e402", "testing/content/3ab33ba88e2250461620487fbfcbfb6b84102818fd11e6ee3719e9a65ff06a0d", "testing/content/3fea1894fb17233d2214fe71a6943274c4aa536f5991368e2cab4801056d3aa6", "testing/content/448321f716133f3f175c6c6d2e39db816c6cca39898126aed726a417632a38ef", "testing/content/44dc6a7ccdb71c6d9fb55dfebb22b71ff12cba43723e74de7e4cda4b73844274", "testing/content/489aede697fed5617da1d87f5ab169698e5b3ab47e8daba9eb66dff8477fce31", "testing/content/4e55faf5415eef3dcec742cc24cd30680559e8da1e522800b3343ec4eb58d1fa", "testing/content/538f2dda4d51e9af6f62f6de4233725325c0b72a0e2a31d250384c9e5772b1a4", "testing/content/5406ba03ee2c5ea0efc595ee8d71e0d6e3eaac0e9bf59ddc1a3279fc0f2c575e", "testing/content/599fb155c7a5276bc94fe3522468cf28684871eae932640dfee6820d49f62f2c", "testing/content/5cfa890f84f4d1eb9d9115d199536968b153fc8dc67da138150d9ad877388598", "testing/content/5de8ec3765f1756d81f950939fc4aa73de28dcd44ac2b9abc119556dc5fdc5c3", "testing/content/6db72b25cfdf05ec5a5ab1587c587b7fb4219fb3836ff9963132f5474435da15", "testing/content/6ea0f9ee557ca104e03640bea5ef63e14235df5397807b617c62340f49191cc7", "testing/content/70983b03b142313b98761f5b410baf3612514cc1c3a874ed33ac1bc410e87b3b", "testing/content/70ec8faba7aec7cf9fa018f3a4a65a37b1844a90279cb78a0af38c64b6773b95", "testing/content/77d851196a81be18732d699579442cb582e47da420bd95bdeeb29dcac0ecf1f0", "testing/content/78891064d5e219d98e85ee55595409590562b80bfd335beda23bb01df71ef24e", "testing/content/7db938540726e5eda416fabfe2dd4fb58f34863d0b3aba8ea33962e0c9279bf2", "testing/content/80b6442ff2a325db243af2c43c0d9b93ce9b7781ffe2d73580a36729a094e249", "testing/content/81ec07b39e320cccc0a00da7a0f7ffeedfb1933bf6d1b5502d5f42c892a13297", "testing/content/859ac6e29dea3d3fcd39dc38a498fba3424bd4ed12d78fe8bf58c862a0e0560d", "testing/content/85b1ea4ab1a0a3061d742a23a9ff3d655257404bffaab572a3467389b750bdca", "testing/content/89d545615569b75f8aa50b737e6945dc52669e768d9784882ae8eae10a76d506", "testing/content/8b7df228db759c8b5df2f652f1ba24bb139263f4d597c532d91eb0f92e0a4632", "testing/content/8c31be1ed3dc89e01d8b051ee943dbf3cdfbad0799d7ddf599019409730c1002", "testing/content/8c954d2769187acf6fede03f8f4205eb9106f6cd667397f31704d42a63bddfad", "testing/content/94c02f384c1e88d89a87b766bc54ad96c68ca4c7f0daddb86a6de215ab00fb2b", "testing/content/97a7af6c5e208d08187cff0d8f2c18b42a3fd903cc01e86618d299bce4e7f9bb", "testing/content/9f5a5ff918f021b89c52832df29c64b66f00ed21783ebfd8786aae6f96dba98e", "testing/content/9f93ee6f4a57edcc54bc9aaf5f639c45228e0ce1ed15a80769b128c327f8a6d2", "testing/content/9fe9bcb3057b1219d017b34e5e7c52e321ff2ff0cf9419d69c4fa5934a85555b", "testing/content/a135d0079326ad2bea3b21e8a3c99f79095bffdf0d291b90238f23132c5f66c6", "testing/content/a3463aa6568a7dfe57f301cd2d6bba5600eee88391161f3ca3b12ceb82466d3b", "testing/content/a395761bc0fc854cfc739a16aa21204174bd5bd0ee3dcd762532ac91d9c896a5", "testing/content/ab4e2e72c935c39eab9a87a16fbebdaa0d4e6f5d48a2596318177fe63c6b7d89", "testing/content/ad3ae3dc2693be80dbf2385b0e1613f82920d70ef7d87fdf4ac01e96c757cfbd", "testing/content/bf163e5fd6e395bc041650d44666824d95bc72d69b5ce71d0a533a2470d28767", "testing/content/c26bb3b8aca2b4edf4a7e32a2c95dda4c89d2a193e512fc931fd8509ed9e93aa", "testing/content/c3775bd097dacc26a862aab99d41499722a65ec3e68adfe12ade7c65670cf928", "testing/content/c396bb679fc42424424e0c8c9376cc39bcadf75369d31c0beeac99f5717230a4", "testing/content/c399c27fce3b447a72c1a3c658c549fa1e9fe83185abc7da921a605db0501c8d", "testing/content/c470cb30e9302c8a5448ff53ccd32ec332c74ab592238c46a6772dcf64595f92", "testing/content/c865448865d77c1b6f5164751d000dfa967988324366368a74d8312fa893dfda", "testing/content/ca6e2934491abe4c976608e602be4a05c1ee4344fe644a0a69016563f5faae8d", "testing/content/ccdaa2c5974af364cc1ee834cf42f449c470948d6b5128455cbdb312e37c4a0d", "testing/content/ce2c13159c817a7d16b30f66ad4ac7ed5688743a732602256514e93fb7fb850d", "testing/content/d3989f2514d9998cd63cd7e51919758b18cbbbbaad6f85a31e95820a249f95ac", "testing/content/d3ced3c33d91bc8f09f9dbd315867b09158fa907fdd7454eaea15e933a32cada", "testing/content/d71628e2cc453e12ab41fab509f26209ac4a77fc95a98e30038eb6fbc8f33219", "testing/content/d743fb6d667c184b9fbd32b2113f234b3a6278ee65104980af23c7841feefa4f", "testing/content/d7dafbbaf4acc73e299fe663d1ff2012fd5a022aa11404db8775b402bbee1346", "testing/content/d9fc8a619e67a0fc2c13bd3caa7917ab36e71881611934b8d7d3bfd1dd6a8f10", "testing/content/da7c16012166b16f2648e6bd6fe9ae0e003a8db01f3aeb4128ee9a36c3598dac", "testing/content/dc2c446b80547f35931f794eafbdcd3e6d872e1ef46f6163a91901b5eaa45f42", "testing/content/e5a9c361779b0ba4a65aa64edf03fbdcff6c5f28305b5e312b2a7d215e64d570", "testing/content/e5fa2b37b37cc2a2111750e4036e28e094aa5feb08bb81b5b615f12332614d13", "testing/content/ed121958017503d5d392f99f6219003cf5542d5555d17b20ee28c336e6adae13", "testing/content/f0529ef426eb624d3cc7df5974b5c366f63656033bba3ed8d3ba09d7963d66a1", "testing/content/f38286d8f7537ff591192e9f94853d48bd7c836cafc16aa287d21fb73a1418db", "testing/content/f71ffd1d453a3347822646c7ae793296abb9a7ad2e39bd8ff284eff508eb8fa7", "testing/content/f781a38348d6b05f4e23f37731c10bf092b8cbb8e342007c32f917c94336b711", "testing/content/fa6f06e650fdd4156250660319c2e31d304834c241f35a667e3181d6e90d3193", "testing/content/fb64f5a790bb1afe6399beb4bb66c1676141e6f50b16b6b627f1905ce41e2f44", "testing/content/fbee275d7d07c7b73020091a2d24cd1266b0ed65dc4c9d98d923ebbee7cdf8d1"})
// 	r.Equal(getKeys(s.eventStore.Data), []string{"testing/binary/extracted/2025-08-05T18:10:00Z-100-43ac3f293c7824223fcc26c555c56205"})

// 	// inspect stream
// 	compressedStream := s.streamStore.Data["testing/content/001f4f38b6a486ca8446a8f5d6f373bd58029735e1c1cfd198e6dd6754004001"]
// 	reader := bytes.NewReader([]byte(compressedStream))
// 	streamBytes, err := bkupcom.NewGzipDecompressReader(reader)
// 	r.Nil(err)
// 	r.Equal(streamBytes, []byte("hello"))

// 	// inspect events
// 	compressedEvents, ok := s.eventStore.Data["testing/binary/extracted/2025-08-05T18:10:00Z-100-43ac3f293c7824223fcc26c555c56205"]
// 	r.True(ok)

// 	rawData, err := bkupcom.NewGzipDecompressBytes(compressedEvents)
// 	r.Nil(err)

// 	bulk, err := msginflight.AvroBulkToMsgInFlights(rawData, events.ModelBinary)
// 	r.Nil(err)
// 	r.Equal(100, len(bulk))

// 	binary, _ := bulk[0].GetBinary()
// 	enc, err := binary.ToAvro()
// 	r.Nil(err)
// 	r.Equal(1497, len(enc), "!!! Schema length has changed, if the schema has been recently updated change the expected value for this assertion.\n")
// 	binary, _ = bulk[1].GetBinary()
// 	enc, err = binary.ToAvro()
// 	r.Nil(err)
// 	r.Equal(1560, len(enc), "!!! Schema length has changed, if the schema has been recently updated change the expected value for this assertion.\n")
// }

// func TestBackup(t *testing.T) {
// 	suite.Run(t, new(BackupTestSuite))
// }
