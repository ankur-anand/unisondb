package streamer_test

import (
	"bytes"
	"context"
	"errors"
	"math/rand/v2"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ankur-anand/isledb"
	"github.com/ankur-anand/isledb/blobstore"
	"github.com/ankur-anand/unisondb/dbkernel"
	"github.com/ankur-anand/unisondb/internal/services/streamer"
	"github.com/ankur-anand/unisondb/schemas/logrecord"
	v1 "github.com/ankur-anand/unisondb/schemas/proto/gen/go/unisondb/streamer/v1"
	"github.com/brianvoe/gofakeit/v7"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"
)

func TestEncodeLSNKey_RoundTrip(t *testing.T) {
	tests := []uint64{0, 1, 255, 256, 1<<32 - 1, 1<<64 - 1}
	for _, lsn := range tests {
		key := streamer.EncodeLSNKey(lsn)
		assert.Len(t, key, 8)
		decoded := streamer.DecodeLSNKey(key)
		assert.Equal(t, lsn, decoded, "round-trip failed for LSN %d", lsn)
	}
}

func TestEncodeLSNKey_Ordering(t *testing.T) {
	prev := streamer.EncodeLSNKey(0)
	for i := uint64(1); i <= 1000; i++ {
		cur := streamer.EncodeLSNKey(i)
		assert.True(t, bytes.Compare(prev, cur) < 0,
			"key for LSN %d should sort before LSN %d", i-1, i)
		prev = cur
	}
}

func TestDefaultBlobStoreStreamerConfig(t *testing.T) {
	cfg := streamer.DefaultBlobStoreStreamerConfig()
	require.NotNil(t, cfg.WriterOptions)
	assert.Equal(t, cfg.FlushInterval, cfg.WriterOptions.FlushInterval)
	assert.Equal(t, 4, cfg.WriterOptions.MaxImmutableMemtables)
}

func newMemoryBlobStore(t *testing.T, prefix string) *blobstore.Store {
	t.Helper()
	return blobstore.NewMemory(prefix)
}

func newMemoryNamespaceStore(t *testing.T, base *blobstore.Store, namespace string) *blobstore.Store {
	t.Helper()
	return blobstore.New(base.Bucket(), "", streamer.NamespaceBlobStorePrefix(base.Prefix(), namespace))
}

func newMemoryNamespaceStores(t *testing.T, basePrefix string, namespaces ...string) map[string]*blobstore.Store {
	t.Helper()

	base := newMemoryBlobStore(t, basePrefix)
	stores := make(map[string]*blobstore.Store, len(namespaces))
	for _, namespace := range namespaces {
		stores[namespace] = newMemoryNamespaceStore(t, base, namespace)
	}
	return stores
}

func TestBlobStoreStreamer_StreamAndConsume(t *testing.T) {
	engine := createEngine(t)
	namespace := engine.Namespace()
	engines := map[string]*dbkernel.Engine{namespace: engine}

	totalRecords := 20
	for i := 0; i < totalRecords; i++ {
		key := []byte(gofakeit.Noun())
		valueSize := smallValue
		if rand.Float64() < largeValueChance {
			valueSize = largeValue
		}
		value := []byte(gofakeit.LetterN(5))
		data := bytes.Repeat(value, valueSize)
		err := engine.PutKV(key, data)
		require.NoError(t, err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	stores := newMemoryNamespaceStores(t, "test-stream", namespace)
	errGrp, gCtx := errgroup.WithContext(ctx)

	cfg := streamer.DefaultBlobStoreStreamerConfig()
	cfg.FlushInterval = 200 * time.Millisecond

	srv, err := streamer.NewBlobStoreStreamer(gCtx, errGrp, engines, stores, cfg)
	require.NoError(t, err)

	var streamErr error
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		streamErr = srv.StreamNamespace(gCtx, namespace, 0)
	}()

	time.Sleep(1 * time.Second)
	cancel()
	wg.Wait()
	_ = srv.Close()

	assert.ErrorIs(t, streamErr, context.Canceled)

	nw := &noopWalIO{}
	client := streamer.NewBlobStoreStreamerClient(stores[namespace], namespace, nw, 0, 100*time.Millisecond)
	client.CacheDir = t.TempDir()

	latestLSN, err := client.GetLatestLSN(context.Background())
	assert.NoError(t, err)
	assert.GreaterOrEqual(t, latestLSN, uint64(1),
		"should have at least 1 record in blob store")
	t.Logf("latest LSN in blob store: %d", latestLSN)
}

func TestBlobStoreStreamer_NamespaceNotFound(t *testing.T) {
	engines := map[string]*dbkernel.Engine{}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	stores := map[string]*blobstore.Store{}
	errGrp, gCtx := errgroup.WithContext(ctx)

	cfg := streamer.DefaultBlobStoreStreamerConfig()
	srv, err := streamer.NewBlobStoreStreamer(gCtx, errGrp, engines, stores, cfg)
	require.NoError(t, err)
	defer srv.Close()

	err = srv.StreamNamespace(ctx, "does-not-exist", 0)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "not found")
}

func TestBlobStoreStreamerClient_EmptyStore_GetLatestLSN(t *testing.T) {
	store := newMemoryBlobStore(t, "test-empty")
	client := streamer.NewBlobStoreStreamerClient(store, "ns", &noopWalIO{}, 0, 0)
	client.CacheDir = t.TempDir()

	lsn, err := client.GetLatestLSN(context.Background())
	assert.NoError(t, err)
	assert.Equal(t, uint64(0), lsn)
}

func TestBlobStoreStreamerClient_StreamWAL_MaxRetry(t *testing.T) {
	store := newMemoryBlobStore(t, "test-maxretry")
	require.NoError(t, store.Close())
	nw := &noopWalIO{}
	client := streamer.NewBlobStoreStreamerClient(store, "ns", nw, 0, 50*time.Millisecond)
	client.CacheDir = t.TempDir()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	err := client.StreamWAL(ctx)
	require.Error(t, err)
	assert.ErrorContains(t, err, "max retries")
}

func TestBlobStoreStreamer_EndToEnd(t *testing.T) {
	var engines = make(map[string]*dbkernel.Engine)
	var nameSpaces = make([]string, 0)

	for i := 0; i < 1; i++ {
		nameSpaces = append(nameSpaces, strings.ToLower(gofakeit.Noun()))
	}

	dir := t.TempDir()
	temp, err := os.MkdirTemp(dir, "unisondb")
	require.NoError(t, err)

	for _, nameSpace := range nameSpaces {
		se, err := dbkernel.NewStorageEngine(temp, nameSpace, dbkernel.NewDefaultEngineConfig())
		require.NoError(t, err)
		require.NotNil(t, se)

		for i := 0; i < 15; i++ {
			key := []byte(gofakeit.Noun())
			valueSize := smallValue
			if rand.Float64() < largeValueChance {
				valueSize = largeValue
			}
			value := []byte(gofakeit.LetterN(5))
			data := bytes.Repeat(value, valueSize)
			err = se.PutKV(key, data)
			require.NoError(t, err)
		}
		engines[nameSpace] = se
	}

	defer func() {
		for _, engine := range engines {
			_ = engine.Close(context.Background())
		}
	}()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	stores := newMemoryNamespaceStores(t, "test-e2e", nameSpaces...)
	errGrp, gCtx := errgroup.WithContext(ctx)

	cfg := streamer.DefaultBlobStoreStreamerConfig()
	cfg.FlushInterval = 200 * time.Millisecond

	srv, err := streamer.NewBlobStoreStreamer(gCtx, errGrp, engines, stores, cfg)
	require.NoError(t, err)

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		_ = srv.StreamNamespace(gCtx, nameSpaces[0], 0)
	}()

	time.Sleep(1 * time.Second)

	nw := &noopWalIO{}
	client := streamer.NewBlobStoreStreamerClient(stores[nameSpaces[0]], nameSpaces[0], nw, 0, 100*time.Millisecond)
	client.CacheDir = t.TempDir()

	var latestLSN uint64
	require.Eventually(t, func() bool {
		lsn, err := client.GetLatestLSN(context.Background())
		if err != nil {
			return false
		}
		latestLSN = lsn
		return latestLSN >= 1
	}, 3*time.Second, 100*time.Millisecond)

	streamCtx, streamCancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer streamCancel()

	streamErr := client.StreamWAL(streamCtx)
	if streamErr != nil {
		assert.True(t,
			strings.Contains(streamErr.Error(), context.DeadlineExceeded.Error()) ||
				strings.Contains(streamErr.Error(), context.Canceled.Error()),
			"unexpected error: %v", streamErr)
	}

	assert.Greater(t, nw.recordWalCount, 0,
		"client should have received WAL records from blob store")

	t.Logf("client received %d WAL records, latest blob store LSN: %d",
		nw.recordWalCount, latestLSN)

	cancel()
	wg.Wait()
	_ = srv.Close()
}

func TestBlobStoreStreamerClient_GetLatestLSN_UsesActiveReader(t *testing.T) {
	engine := createEngine(t)
	namespace := engine.Namespace()
	engines := map[string]*dbkernel.Engine{namespace: engine}

	for i := 0; i < 10; i++ {
		err := engine.PutKV([]byte(gofakeit.Noun()), []byte(gofakeit.LetterN(8)))
		require.NoError(t, err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	stores := newMemoryNamespaceStores(t, "test-active-tail-reader", namespace)
	errGrp, gCtx := errgroup.WithContext(ctx)

	cfg := streamer.DefaultBlobStoreStreamerConfig()
	cfg.FlushInterval = 200 * time.Millisecond

	srv, err := streamer.NewBlobStoreStreamer(gCtx, errGrp, engines, stores, cfg)
	require.NoError(t, err)
	defer srv.Close()

	var streamWG sync.WaitGroup
	streamWG.Add(1)
	go func() {
		defer streamWG.Done()
		_ = srv.StreamNamespace(gCtx, namespace, 0)
	}()

	bootstrapClient := streamer.NewBlobStoreStreamerClient(stores[namespace], namespace, &noopWalIO{}, 0, 0)
	bootstrapClient.CacheDir = t.TempDir()
	require.Eventually(t, func() bool {
		lsn, err := bootstrapClient.GetLatestLSN(context.Background())
		return err == nil && lsn > 0
	}, 3*time.Second, 100*time.Millisecond)

	client := streamer.NewBlobStoreStreamerClient(stores[namespace], namespace, &noopWalIO{}, 0, 100*time.Millisecond)
	client.CacheDir = t.TempDir()

	streamCtx, streamCancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer streamCancel()

	var clientErr error
	var clientWG sync.WaitGroup
	clientWG.Add(1)
	go func() {
		defer clientWG.Done()
		clientErr = client.StreamWAL(streamCtx)
	}()

	var latestLSN uint64
	require.Eventually(t, func() bool {
		lsn, err := client.GetLatestLSN(context.Background())
		if err != nil {
			return false
		}
		latestLSN = lsn
		return latestLSN == engine.OpsReceivedCount()
	}, 2*time.Second, 50*time.Millisecond)

	streamCancel()
	clientWG.Wait()
	if clientErr != nil {
		assert.True(t,
			errors.Is(clientErr, context.DeadlineExceeded) ||
				errors.Is(clientErr, context.Canceled),
			"unexpected error: %v", clientErr)
	}

	cancel()
	streamWG.Wait()
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

func TestBlobStoreStreamer_E2E_Replication(t *testing.T) {
	sourceDir := t.TempDir()
	namespace := "e2e-repl"
	sourceEngine, err := dbkernel.NewStorageEngine(sourceDir, namespace, dbkernel.NewDefaultEngineConfig())
	require.NoError(t, err)
	defer sourceEngine.Close(context.Background())

	insertedKV := make(map[string]string)
	numRecords := 25
	for i := 0; i < numRecords; i++ {
		key := gofakeit.UUID()
		val := gofakeit.Sentence(3)
		insertedKV[key] = val
		err := sourceEngine.PutKV([]byte(key), []byte(val))
		require.NoError(t, err)
	}

	sourceLSN := sourceEngine.OpsReceivedCount()
	require.Equal(t, uint64(numRecords), sourceLSN,
		"source engine should have %d ops", numRecords)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	stores := newMemoryNamespaceStores(t, "e2e-repl-store", namespace)
	errGrp, gCtx := errgroup.WithContext(ctx)

	cfg := streamer.DefaultBlobStoreStreamerConfig()
	cfg.FlushInterval = 200 * time.Millisecond

	engines := map[string]*dbkernel.Engine{namespace: sourceEngine}
	srv, err := streamer.NewBlobStoreStreamer(gCtx, errGrp, engines, stores, cfg)
	require.NoError(t, err)

	var streamWg sync.WaitGroup
	streamWg.Add(1)
	go func() {
		defer streamWg.Done()
		_ = srv.StreamNamespace(gCtx, namespace, 0)
	}()

	time.Sleep(1 * time.Second)

	dummyClient := streamer.NewBlobStoreStreamerClient(stores[namespace], namespace, &noopWalIO{}, 0, 0)
	dummyClient.CacheDir = t.TempDir()
	var blobLSN uint64
	require.Eventually(t, func() bool {
		lsn, err := dummyClient.GetLatestLSN(context.Background())
		if err != nil {
			return false
		}
		blobLSN = lsn
		return blobLSN == sourceLSN
	}, 3*time.Second, 100*time.Millisecond,
		"blob store should contain all %d records", numRecords)

	replicaDir := t.TempDir()
	replicaCfg := dbkernel.NewDefaultEngineConfig()
	replicaCfg.ReadOnly = true
	replicaEngine, err := dbkernel.NewStorageEngine(replicaDir, namespace, replicaCfg)
	require.NoError(t, err)
	defer replicaEngine.Close(context.Background())

	replicaHandler := dbkernel.NewReplicaWALHandler(replicaEngine)
	replicaWalIO := &replicaWalIOAdapter{replica: replicaHandler}

	replicaClient := streamer.NewBlobStoreStreamerClient(stores[namespace], namespace, replicaWalIO, 0, 100*time.Millisecond)
	replicaClient.CacheDir = t.TempDir()

	streamCtx, streamCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer streamCancel()

	replicaErr := replicaClient.StreamWAL(streamCtx)
	if replicaErr != nil {
		assert.True(t,
			errors.Is(replicaErr, context.DeadlineExceeded) ||
				errors.Is(replicaErr, context.Canceled),
			"unexpected error: %v", replicaErr)
	}

	replicaLSN := replicaEngine.OpsReceivedCount()
	assert.Equal(t, sourceLSN, replicaLSN,
		"replica should have same number of ops as source (%d vs %d)", replicaLSN, sourceLSN)

	t.Logf("e2e replication: source=%d, blob=%d, replica=%d records",
		sourceLSN, blobLSN, replicaLSN)

	cancel()
	streamWg.Wait()
	_ = srv.Close()
}

func TestBlobStoreStreamer_NamespaceIsolation(t *testing.T) {
	dir := t.TempDir()
	namespaces := []string{"alpha", "beta"}
	recordCounts := map[string]int{
		"alpha": 7,
		"beta":  11,
	}

	engines := make(map[string]*dbkernel.Engine, len(namespaces))
	for _, namespace := range namespaces {
		engine, err := dbkernel.NewStorageEngine(dir, namespace, dbkernel.NewDefaultEngineConfig())
		require.NoError(t, err)
		engines[namespace] = engine
		defer engine.Close(context.Background())

		for i := 0; i < recordCounts[namespace]; i++ {
			err = engine.PutKV([]byte(gofakeit.UUID()), []byte(gofakeit.Sentence(3)))
			require.NoError(t, err)
		}
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	stores := newMemoryNamespaceStores(t, "test-namespace-isolation", namespaces...)
	errGrp, gCtx := errgroup.WithContext(ctx)

	cfg := streamer.DefaultBlobStoreStreamerConfig()
	cfg.FlushInterval = 200 * time.Millisecond

	srv, err := streamer.NewBlobStoreStreamer(gCtx, errGrp, engines, stores, cfg)
	require.NoError(t, err)
	defer srv.Close()

	var wg sync.WaitGroup
	for _, namespace := range namespaces {
		wg.Add(1)
		go func(ns string) {
			defer wg.Done()
			_ = srv.StreamNamespace(gCtx, ns, 0)
		}(namespace)
	}

	clientAlpha := streamer.NewBlobStoreStreamerClient(stores["alpha"], "alpha", &noopWalIO{}, 0, 0)
	clientAlpha.CacheDir = t.TempDir()
	clientBeta := streamer.NewBlobStoreStreamerClient(stores["beta"], "beta", &noopWalIO{}, 0, 0)
	clientBeta.CacheDir = t.TempDir()

	var alphaLSN uint64
	require.Eventually(t, func() bool {
		lsn, err := clientAlpha.GetLatestLSN(context.Background())
		if err != nil {
			return false
		}
		alphaLSN = lsn
		return alphaLSN == uint64(recordCounts["alpha"])
	}, 3*time.Second, 100*time.Millisecond)

	var betaLSN uint64
	require.Eventually(t, func() bool {
		lsn, err := clientBeta.GetLatestLSN(context.Background())
		if err != nil {
			return false
		}
		betaLSN = lsn
		return betaLSN == uint64(recordCounts["beta"])
	}, 3*time.Second, 100*time.Millisecond)

	assert.Equal(t, uint64(recordCounts["alpha"]), alphaLSN)
	assert.Equal(t, uint64(recordCounts["beta"]), betaLSN)

	cancel()
	wg.Wait()
}

func TestBlobStoreStreamer_BackpressureRetries(t *testing.T) {
	engine := createEngine(t)
	namespace := engine.Namespace()
	engines := map[string]*dbkernel.Engine{namespace: engine}

	const numRecords = 64
	for i := 0; i < numRecords; i++ {
		err := engine.PutKV([]byte(gofakeit.UUID()), bytes.Repeat([]byte("v"), 128))
		require.NoError(t, err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	stores := newMemoryNamespaceStores(t, "test-backpressure", namespace)
	errGrp, gCtx := errgroup.WithContext(ctx)

	cfg := streamer.DefaultBlobStoreStreamerConfig()
	cfg.FlushInterval = 0

	writerOpts := isledb.DefaultWriterOptions()
	writerOpts.MemtableSize = 256
	writerOpts.MaxImmutableMemtables = 1
	writerOpts.FlushInterval = 25 * time.Millisecond
	writerOpts.ValueStorage.BlobThreshold = 1 << 20
	cfg.WriterOptions = &writerOpts

	srv, err := streamer.NewBlobStoreStreamer(gCtx, errGrp, engines, stores, cfg)
	require.NoError(t, err)
	defer srv.Close()

	var streamErr error
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		streamErr = srv.StreamNamespace(gCtx, namespace, 0)
	}()

	client := streamer.NewBlobStoreStreamerClient(stores[namespace], namespace, &noopWalIO{}, 0, 0)
	client.CacheDir = t.TempDir()

	require.Eventually(t, func() bool {
		lsn, err := client.GetLatestLSN(context.Background())
		return err == nil && lsn == uint64(numRecords)
	}, 5*time.Second, 50*time.Millisecond)

	cancel()
	wg.Wait()
	assert.ErrorIs(t, streamErr, context.Canceled)
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

func TestBlobStoreStreamer_LSNOrdering(t *testing.T) {
	engine := createEngine(t)
	namespace := engine.Namespace()
	engines := map[string]*dbkernel.Engine{namespace: engine}

	numRecords := 30
	for i := 0; i < numRecords; i++ {
		key := []byte(gofakeit.Noun())
		value := []byte(gofakeit.LetterN(10))
		err := engine.PutKV(key, value)
		require.NoError(t, err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	stores := newMemoryNamespaceStores(t, "test-lsn-order", namespace)
	errGrp, gCtx := errgroup.WithContext(ctx)

	cfg := streamer.DefaultBlobStoreStreamerConfig()
	cfg.FlushInterval = 200 * time.Millisecond

	srv, err := streamer.NewBlobStoreStreamer(gCtx, errGrp, engines, stores, cfg)
	require.NoError(t, err)

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		_ = srv.StreamNamespace(gCtx, namespace, 0)
	}()

	time.Sleep(1 * time.Second)

	rw := &recordingWalIO{}
	client := streamer.NewBlobStoreStreamerClient(stores[namespace], namespace, rw, 0, 100*time.Millisecond)
	client.CacheDir = t.TempDir()

	streamCtx, streamCancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer streamCancel()
	_ = client.StreamWAL(streamCtx)

	cancel()
	wg.Wait()
	_ = srv.Close()

	records := rw.records
	assert.Greater(t, len(records), 0, "should have received records")

	var prevLSN uint64
	for i, rec := range records {
		decoded := logrecord.GetRootAsLogRecord(rec.Record, 0)
		lsn := decoded.Lsn()
		assert.Greater(t, lsn, prevLSN,
			"LSN at index %d (%d) should be > previous (%d)", i, lsn, prevLSN)
		prevLSN = lsn
	}

	t.Logf("verified %d records with monotonically increasing LSNs", len(records))
}
