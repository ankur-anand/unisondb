package streamer_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"sync"
	"testing"
	"time"

	"github.com/ankur-anand/unijord/partitionlog"
	segmentsink "github.com/ankur-anand/unijord/partitionlog/blob/sink"
	"github.com/ankur-anand/unijord/partitionlog/blob/sink/multipart"
	"github.com/ankur-anand/unijord/partitionlog/catalog"
	plwriter "github.com/ankur-anand/unijord/partitionlog/writer"
	"github.com/ankur-anand/unisondb/dbkernel"
	"github.com/ankur-anand/unisondb/internal/services/streamer"
	"github.com/ankur-anand/unisondb/schemas/logrecord"
	v1 "github.com/ankur-anand/unisondb/schemas/proto/gen/go/unisondb/streamer/v1"
	"github.com/brianvoe/gofakeit/v7"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"
)

func TestDefaultBlobStoreStreamerConfig(t *testing.T) {
	cfg := streamer.DefaultBlobStoreStreamerConfig()
	assert.Equal(t, time.Second, cfg.FlushInterval)
	assert.Equal(t, uint32(1_048_576), cfg.Batch.MaxRecords)
}

func TestBlobStoreStreamer_StreamAndConsume(t *testing.T) {
	engine := createEngine(t)
	namespace := engine.Namespace()
	engines := map[string]*dbkernel.Engine{namespace: engine}

	for i := 0; i < 20; i++ {
		require.NoError(t, engine.PutKV([]byte(gofakeit.Noun()), randomValue()))
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	logs := map[string]*partitionlog.Log{namespace: newMemoryPartitionLog(t)}
	errGrp, gCtx := errgroup.WithContext(ctx)

	cfg := streamer.DefaultBlobStoreStreamerConfig()
	cfg.FlushInterval = 50 * time.Millisecond

	srv, err := streamer.NewBlobStoreStreamer(gCtx, errGrp, engines, logs, cfg)
	require.NoError(t, err)
	defer srv.Close()

	var streamErr error
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		streamErr = srv.StreamNamespace(gCtx, namespace)
	}()

	client := streamer.NewBlobStoreStreamerClient(logs[namespace], namespace, &noopWalIO{}, 0, 25*time.Millisecond)
	require.Eventually(t, func() bool {
		lsn, err := client.GetLatestLSN(context.Background())
		return err == nil && lsn == engine.OpsReceivedCount()
	}, 3*time.Second, 25*time.Millisecond)

	cancel()
	wg.Wait()
	if streamErr != nil {
		assert.ErrorIs(t, streamErr, context.Canceled)
	}
}

func TestBlobStoreStreamer_NamespaceNotFound(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	srv, err := streamer.NewBlobStoreStreamer(ctx, nil, map[string]*dbkernel.Engine{}, map[string]*partitionlog.Log{}, streamer.DefaultBlobStoreStreamerConfig())
	require.NoError(t, err)
	defer srv.Close()

	err = srv.StreamNamespace(ctx, "does-not-exist")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "not found")
}

func TestBlobStoreStreamerClient_EmptyStore_GetLatestLSN(t *testing.T) {
	client := streamer.NewBlobStoreStreamerClient(newMemoryPartitionLog(t), "ns", &noopWalIO{}, 0, 0)

	lsn, err := client.GetLatestLSN(context.Background())
	assert.NoError(t, err)
	assert.Equal(t, uint64(0), lsn)
}

func TestBlobStoreStreamerClient_StreamWAL_MaxRetry(t *testing.T) {
	nw := &noopWalIO{}
	client := streamer.NewBlobStoreStreamerClient(nil, "ns", nw, 0, 1*time.Millisecond)

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	err := client.StreamWAL(ctx)
	require.Error(t, err)
	assert.ErrorContains(t, err, "max retries")
}

func TestBlobStoreStreamer_E2E_Replication(t *testing.T) {
	namespace := "e2e-repl"
	sourceEngine := createNamedEngine(t, namespace)

	const numRecords = 25
	for i := 0; i < numRecords; i++ {
		require.NoError(t, sourceEngine.PutKV([]byte(gofakeit.UUID()), []byte(gofakeit.Sentence(3))))
	}
	sourceLSN := sourceEngine.OpsReceivedCount()
	require.Equal(t, uint64(numRecords), sourceLSN)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	logs := map[string]*partitionlog.Log{namespace: newMemoryPartitionLog(t)}
	errGrp, gCtx := errgroup.WithContext(ctx)

	cfg := streamer.DefaultBlobStoreStreamerConfig()
	cfg.FlushInterval = 50 * time.Millisecond

	srv, err := streamer.NewBlobStoreStreamer(gCtx, errGrp, map[string]*dbkernel.Engine{namespace: sourceEngine}, logs, cfg)
	require.NoError(t, err)
	defer srv.Close()

	var streamWg sync.WaitGroup
	streamWg.Add(1)
	go func() {
		defer streamWg.Done()
		_ = srv.StreamNamespace(gCtx, namespace)
	}()

	dummyClient := streamer.NewBlobStoreStreamerClient(logs[namespace], namespace, &noopWalIO{}, 0, 0)
	require.Eventually(t, func() bool {
		lsn, err := dummyClient.GetLatestLSN(context.Background())
		return err == nil && lsn == sourceLSN
	}, 3*time.Second, 50*time.Millisecond)

	replicaCfg := dbkernel.NewDefaultEngineConfig()
	replicaCfg.ReadOnly = true
	replicaEngine, err := dbkernel.NewStorageEngine(t.TempDir(), namespace, replicaCfg)
	require.NoError(t, err)
	defer replicaEngine.Close(context.Background())

	replicaHandler := dbkernel.NewReplicaWALHandler(replicaEngine)
	replicaWalIO := &replicaWalIOAdapter{replica: replicaHandler}

	replicaClient := streamer.NewBlobStoreStreamerClient(logs[namespace], namespace, replicaWalIO, 0, 25*time.Millisecond)

	streamCtx, streamCancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer streamCancel()
	if err := replicaClient.StreamWAL(streamCtx); err != nil {
		assert.True(t, errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled), "unexpected error: %v", err)
	}

	assert.Equal(t, sourceLSN, replicaEngine.OpsReceivedCount())
	cancel()
	streamWg.Wait()
}

func TestBlobStoreStreamer_NamespaceIsolation(t *testing.T) {
	namespaces := []string{"alpha", "beta"}
	recordCounts := map[string]int{"alpha": 7, "beta": 11}

	engines := make(map[string]*dbkernel.Engine, len(namespaces))
	logs := make(map[string]*partitionlog.Log, len(namespaces))
	for _, namespace := range namespaces {
		engine := createNamedEngine(t, namespace)
		engines[namespace] = engine
		logs[namespace] = newMemoryPartitionLog(t)
		for i := 0; i < recordCounts[namespace]; i++ {
			require.NoError(t, engine.PutKV([]byte(gofakeit.UUID()), []byte(gofakeit.Sentence(3))))
		}
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	errGrp, gCtx := errgroup.WithContext(ctx)
	cfg := streamer.DefaultBlobStoreStreamerConfig()
	cfg.FlushInterval = 50 * time.Millisecond

	srv, err := streamer.NewBlobStoreStreamer(gCtx, errGrp, engines, logs, cfg)
	require.NoError(t, err)
	defer srv.Close()

	var wg sync.WaitGroup
	for _, namespace := range namespaces {
		wg.Add(1)
		go func(ns string) {
			defer wg.Done()
			_ = srv.StreamNamespace(gCtx, ns)
		}(namespace)
	}

	for _, namespace := range namespaces {
		client := streamer.NewBlobStoreStreamerClient(logs[namespace], namespace, &noopWalIO{}, 0, 0)
		want := uint64(recordCounts[namespace])
		require.Eventually(t, func() bool {
			lsn, err := client.GetLatestLSN(context.Background())
			return err == nil && lsn == want
		}, 3*time.Second, 50*time.Millisecond)
	}

	cancel()
	wg.Wait()
}

func TestBlobStoreStreamer_CheckpointBootstrap(t *testing.T) {
	namespace := "checkpoint"
	engine := createNamedEngine(t, namespace)
	for i := 0; i < 10; i++ {
		require.NoError(t, engine.PutKV([]byte(fmt.Sprintf("k-%02d", i)), []byte("v")))
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	log := newMemoryPartitionLog(t)
	logs := map[string]*partitionlog.Log{namespace: log}
	errGrp, gCtx := errgroup.WithContext(ctx)

	cfg := streamer.DefaultBlobStoreStreamerConfig()
	cfg.FlushInterval = 50 * time.Millisecond
	cfg.BootstrapAfterLSN = map[string]uint64{namespace: 5}

	srv, err := streamer.NewBlobStoreStreamer(gCtx, errGrp, map[string]*dbkernel.Engine{namespace: engine}, logs, cfg)
	require.NoError(t, err)
	defer srv.Close()

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		_ = srv.StreamNamespace(gCtx, namespace)
	}()

	client := streamer.NewBlobStoreStreamerClient(log, namespace, &noopWalIO{}, 5, 25*time.Millisecond)
	require.Eventually(t, func() bool {
		lsn, err := client.GetLatestLSN(context.Background())
		return err == nil && lsn == 10
	}, 3*time.Second, 50*time.Millisecond)

	rw := &recordingWalIO{}
	client = streamer.NewBlobStoreStreamerClient(log, namespace, rw, 5, 25*time.Millisecond)
	streamCtx, streamCancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer streamCancel()
	_ = client.StreamWAL(streamCtx)
	require.Equal(t, 5, rw.count())

	_, err = log.Reader().Partition(0).Read(context.Background(), partitionlog.ReadRequest{
		StartLSN:  0,
		Limit:     1,
		Freshness: partitionlog.FreshnessLatest,
	})
	var expired partitionlog.LSNExpiredError
	assert.ErrorAs(t, err, &expired)

	cancel()
	wg.Wait()
}

func TestBlobStoreStreamer_LSNOrdering(t *testing.T) {
	engine := createEngine(t)
	namespace := engine.Namespace()
	for i := 0; i < 30; i++ {
		require.NoError(t, engine.PutKV([]byte(gofakeit.Noun()), []byte(gofakeit.LetterN(10))))
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	log := newMemoryPartitionLog(t)
	errGrp, gCtx := errgroup.WithContext(ctx)
	cfg := streamer.DefaultBlobStoreStreamerConfig()
	cfg.FlushInterval = 50 * time.Millisecond

	srv, err := streamer.NewBlobStoreStreamer(gCtx, errGrp, map[string]*dbkernel.Engine{namespace: engine}, map[string]*partitionlog.Log{namespace: log}, cfg)
	require.NoError(t, err)
	defer srv.Close()

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		_ = srv.StreamNamespace(gCtx, namespace)
	}()

	rw := &recordingWalIO{}
	client := streamer.NewBlobStoreStreamerClient(log, namespace, rw, 0, 25*time.Millisecond)
	streamCtx, streamCancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer streamCancel()
	_ = client.StreamWAL(streamCtx)

	cancel()
	wg.Wait()

	records := rw.records
	assert.Greater(t, len(records), 0)
	var prevLSN uint64
	for i, rec := range records {
		decoded := logrecord.GetRootAsLogRecord(rec.Record, 0)
		lsn := decoded.Lsn()
		assert.Greater(t, lsn, prevLSN, "LSN at index %d (%d) should be > previous (%d)", i, lsn, prevLSN)
		prevLSN = lsn
	}
}

func randomValue() []byte {
	valueSize := smallValue
	if rand.Float64() < largeValueChance {
		valueSize = largeValue
	}
	return bytes.Repeat([]byte(gofakeit.LetterN(5)), valueSize)
}

type recordingWalIO struct {
	mu      sync.Mutex
	records []*v1.WALRecord
}

func (r *recordingWalIO) Write(data *v1.WALRecord) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.records = append(r.records, data)
	return nil
}

func (r *recordingWalIO) WriteBatch(records []*v1.WALRecord) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.records = append(r.records, records...)
	return nil
}

func (r *recordingWalIO) count() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.records)
}

type replicaWalIOAdapter struct {
	replica *dbkernel.ReplicaWALHandler
}

func (r *replicaWalIOAdapter) Write(data *v1.WALRecord) error {
	return r.replica.ApplyRecordsLSNOnly([][]byte{data.Record})
}

func (r *replicaWalIOAdapter) WriteBatch(records []*v1.WALRecord) error {
	if len(records) == 0 {
		return nil
	}
	encoded := make([][]byte, len(records))
	for i, rec := range records {
		encoded[i] = rec.Record
	}
	return r.replica.ApplyRecordsLSNOnly(encoded)
}

func createNamedEngine(t *testing.T, namespace string) *dbkernel.Engine {
	t.Helper()
	engine, err := dbkernel.NewStorageEngine(t.TempDir(), namespace, dbkernel.NewDefaultEngineConfig())
	require.NoError(t, err)
	t.Cleanup(func() { _ = engine.Close(context.Background()) })
	return engine
}

func newMemoryPartitionLog(t *testing.T) *partitionlog.Log {
	t.Helper()
	objects := multipart.NewMemoryStore()
	sinkFactory, err := segmentsink.New(objects, segmentsink.Options{})
	require.NoError(t, err)
	store := &testPartitionLogStore{
		catalog: catalog.NewMemory(),
		sink:    sinkFactory,
		source:  &testSegmentStore{objects: objects},
	}
	log, err := partitionlog.Open(partitionlog.Options{Store: store})
	require.NoError(t, err)
	return log
}

type testPartitionLogStore struct {
	catalog *catalog.MemoryCatalog
	sink    *segmentsink.Factory
	source  *testSegmentStore
}

func (s *testPartitionLogStore) WriterManager() catalog.WriterManager { return s.catalog }
func (s *testPartitionLogStore) ReaderCatalog() catalog.Reader        { return s.catalog }
func (s *testPartitionLogStore) SinkFactory() plwriter.SinkFactory    { return s.sink }
func (s *testPartitionLogStore) SegmentStore() partitionlog.SegmentStore {
	return s.source
}

type testSegmentStore struct {
	objects *multipart.MemoryStore
}

func (s *testSegmentStore) ReadAt(ctx context.Context, uri string, off uint64, n uint64) ([]byte, error) {
	body, _, err := s.objects.Read(ctx, uri)
	if err != nil {
		return nil, err
	}
	if off > uint64(len(body)) {
		return nil, fmt.Errorf("offset=%d beyond object size=%d", off, len(body))
	}
	if n > uint64(len(body))-off {
		return nil, fmt.Errorf("range offset=%d length=%d beyond object size=%d", off, n, len(body))
	}
	start := int(off)
	end := start + int(n)
	return append([]byte(nil), body[start:end]...), nil
}
