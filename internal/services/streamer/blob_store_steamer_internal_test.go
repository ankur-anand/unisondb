package streamer

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ankur-anand/unijord/partitionlog"
	segmentsink "github.com/ankur-anand/unijord/partitionlog/blob/sink"
	"github.com/ankur-anand/unijord/partitionlog/blob/sink/multipart"
	"github.com/ankur-anand/unijord/partitionlog/catalog"
	"github.com/ankur-anand/unijord/partitionlog/pmeta"
	plwriter "github.com/ankur-anand/unijord/partitionlog/writer"
	"github.com/ankur-anand/unisondb/dbkernel"
	v1 "github.com/ankur-anand/unisondb/schemas/proto/gen/go/unisondb/streamer/v1"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBlobStoreStreamer_NamespaceStateInitializesCheckpoint(t *testing.T) {
	ctx := context.Background()
	namespace := "checkpoint-internal"
	engine, err := dbkernel.NewStorageEngine(t.TempDir(), namespace, dbkernel.NewDefaultEngineConfig())
	require.NoError(t, err)
	t.Cleanup(func() { _ = engine.Close(context.Background()) })

	log := newInternalMemoryPartitionLog(t)
	streamer, err := NewBlobStoreStreamer(ctx, nil, map[string]*dbkernel.Engine{namespace: engine}, map[string]*partitionlog.Log{namespace: log}, BlobStoreStreamerConfig{
		BootstrapAfterLSN: map[string]uint64{namespace: 99},
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = streamer.Close() })

	nsState, err := streamer.namespaceState(ctx, namespace)
	require.NoError(t, err)
	assert.Equal(t, uint64(99), nsState.startLSN)

	head, err := log.Reader().Partition(0).Head(ctx)
	require.NoError(t, err)
	assert.Equal(t, uint64(100), head.NextLSN)
	assert.Equal(t, uint64(100), head.OldestLSN)
	assert.False(t, head.HasLastSegment)
}

func TestBlobStoreStreamer_NamespaceStateRejectsBootstrapAheadOfExistingHead(t *testing.T) {
	ctx := context.Background()
	namespace := "bootstrap-conflict"
	engine, err := dbkernel.NewStorageEngine(t.TempDir(), namespace, dbkernel.NewDefaultEngineConfig())
	require.NoError(t, err)
	t.Cleanup(func() { _ = engine.Close(context.Background()) })

	log := newInternalMemoryPartitionLog(t)
	_, err = log.InitializePartition(ctx, partitionlog.InitializePartition{Partition: 0, NextLSN: 10})
	require.NoError(t, err)

	streamer, err := NewBlobStoreStreamer(ctx, nil, map[string]*dbkernel.Engine{namespace: engine}, map[string]*partitionlog.Log{namespace: log}, BlobStoreStreamerConfig{
		BootstrapAfterLSN: map[string]uint64{namespace: 99},
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = streamer.Close() })

	_, err = streamer.namespaceState(ctx, namespace)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "before configured bootstrap_after_lsn")
}

func TestNormalizeHLCTimestampMS(t *testing.T) {
	fallback := time.UnixMilli(123).UnixMilli()
	assert.Equal(t, fallback, normalizeHLCTimestampMS(0, fallback))
	assert.Equal(t, int64(456), normalizeHLCTimestampMS(456, fallback))
	assert.Equal(t, int64(1_700_000_000_000), normalizeHLCTimestampMS(uint64(1_700_000_000_000_000_000), fallback))
}

func TestWriterIDForNamespaceStable(t *testing.T) {
	assert.Equal(t, writerIDForNamespace("orders"), writerIDForNamespace("orders"))
	assert.NotEqual(t, writerIDForNamespace("orders"), writerIDForNamespace("inventory"))
}

func TestBlobStoreStreamerClientApplyCommittedRangeUsesCachedHead(t *testing.T) {
	ctx := context.Background()
	log, countingCatalog := newCountingInternalMemoryPartitionLog(t)
	writer, err := log.OpenWriter(ctx, partitionlog.WriterOptions{
		Partition: 0,
		WriterID:  [16]byte{1},
		Batch: partitionlog.BatchPolicy{
			MaxRecords: 64,
		},
	})
	require.NoError(t, err)

	recordCount := batchSize*2 + 7
	for i := 0; i < recordCount; i++ {
		_, err = writer.Append(ctx, partitionlog.Record{
			TimestampMS: int64(i + 1),
			Value:       []byte("wal-record"),
		})
		require.NoError(t, err)
	}
	_, err = writer.Close(ctx)
	require.NoError(t, err)

	client := NewBlobStoreStreamerClient(log, "orders", &internalNoopWalIO{}, 0, time.Second)
	latest, err := client.GetLatestLSN(ctx)
	require.NoError(t, err)
	require.Equal(t, uint64(recordCount-1), latest)

	countingCatalog.loadPartitionCalls.Store(0)
	require.NoError(t, client.applyCommittedRange(ctx, latest))
	assert.Zero(t, countingCatalog.loadPartitionCalls.Load(), "batch reads should use cached head after latest head refresh")
}

func TestStreamNamespaceDerivesStartFromCatalog(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	namespace := "catalog-start"
	engine, err := dbkernel.NewStorageEngine(t.TempDir(), namespace, dbkernel.NewDefaultEngineConfig())
	require.NoError(t, err)
	t.Cleanup(func() { _ = engine.Close(context.Background()) })

	for i := 0; i < 6; i++ {
		require.NoError(t, engine.PutKV([]byte(fmt.Sprintf("k-%02d", i)), []byte("v")))
	}

	log := newInternalMemoryPartitionLog(t)
	srv, err := NewBlobStoreStreamer(ctx, nil, map[string]*dbkernel.Engine{namespace: engine}, map[string]*partitionlog.Log{namespace: log}, BlobStoreStreamerConfig{
		FlushInterval:     25 * time.Millisecond,
		BootstrapAfterLSN: map[string]uint64{namespace: 3},
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = srv.Close() })

	done := make(chan error, 1)
	go func() { done <- srv.StreamNamespace(ctx, namespace) }()

	client := NewBlobStoreStreamerClient(log, namespace, &internalNoopWalIO{}, 3, 10*time.Millisecond)
	require.Eventually(t, func() bool {
		latest, err := client.GetLatestLSN(context.Background())
		return err == nil && latest == 6
	}, 3*time.Second, 25*time.Millisecond)

	cancel()
	err = <-done
	assert.True(t, err == nil || errors.Is(err, context.Canceled), "unexpected stream error: %v", err)
}

func TestPartitionLogStoreFactoryURLValidation(t *testing.T) {
	tests := []struct {
		name      string
		bucketURL string
		wantErr   string
	}{
		{name: "unsupported", bucketURL: "file:///tmp/log", wantErr: "unsupported partitionlog bucket scheme"},
		{name: "missing s3 bucket", bucketURL: "s3://", wantErr: "s3 bucket missing"},
		{name: "missing gcs bucket", bucketURL: "gcs://", wantErr: "gcs bucket missing"},
		{name: "missing azure container", bucketURL: "azblob://", wantErr: "azblob container missing"},
		{name: "nested azure container", bucketURL: "azblob://container/nested", wantErr: "identify exactly one container"},
		{name: "bad azure auth", bucketURL: "azblob://container?url=http://127.0.0.1:10000/container&auth=bogus", wantErr: "unsupported azblob auth mode"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := newPartitionLogStoreFactory(context.Background(), tt.bucketURL, "prefix")
			require.Error(t, err)
			assert.Contains(t, err.Error(), tt.wantErr)
		})
	}
}

func TestPartitionLogStoreFactorySupportsLocalProviderURLs(t *testing.T) {
	tests := []struct {
		name      string
		bucketURL string
	}{
		{name: "gcs emulator", bucketURL: "gcs://bucket?endpoint=http://127.0.0.1:4443&no_auth=true"},
		{name: "azure no credential endpoint", bucketURL: "azblob://container?endpoint=http://127.0.0.1:10000/devstoreaccount1"},
		{name: "azure default credential account", bucketURL: "azblob://container?account=acct&auth=default"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			factory, err := newPartitionLogStoreFactory(context.Background(), tt.bucketURL, "prefix")
			require.NoError(t, err)
			store, err := factory.openStore("orders")
			require.NoError(t, err)
			assert.NotNil(t, store)
		})
	}
}

func newInternalMemoryPartitionLog(t *testing.T) *partitionlog.Log {
	t.Helper()
	objects := multipart.NewMemoryStore()
	sinkFactory, err := segmentsink.New(objects, segmentsink.Options{})
	require.NoError(t, err)
	store := &internalTestPartitionLogStore{
		catalog: catalog.NewMemory(),
		sink:    sinkFactory,
		source:  &internalTestSegmentStore{objects: objects},
	}
	log, err := partitionlog.Open(partitionlog.Options{Store: store})
	require.NoError(t, err)
	return log
}

func newCountingInternalMemoryPartitionLog(t *testing.T) (*partitionlog.Log, *countingCatalog) {
	t.Helper()
	objects := multipart.NewMemoryStore()
	sinkFactory, err := segmentsink.New(objects, segmentsink.Options{})
	require.NoError(t, err)
	cat := &countingCatalog{inner: catalog.NewMemory()}
	store := &countingTestPartitionLogStore{
		catalog: cat,
		sink:    sinkFactory,
		source:  &internalTestSegmentStore{objects: objects},
	}
	log, err := partitionlog.Open(partitionlog.Options{Store: store})
	require.NoError(t, err)
	return log, cat
}

type countingCatalog struct {
	inner              *catalog.MemoryCatalog
	loadPartitionCalls atomic.Int64
}

func (c *countingCatalog) LoadPartition(ctx context.Context, partition uint32) (pmeta.PartitionHead, error) {
	c.loadPartitionCalls.Add(1)
	return c.inner.LoadPartition(ctx, partition)
}

func (c *countingCatalog) FindSegment(ctx context.Context, partition uint32, lsn uint64) (pmeta.SegmentRef, bool, error) {
	return c.inner.FindSegment(ctx, partition, lsn)
}

func (c *countingCatalog) ListSegments(ctx context.Context, req catalog.ListSegmentsRequest) (pmeta.SegmentPage, error) {
	return c.inner.ListSegments(ctx, req)
}

func (c *countingCatalog) OpenWriter(ctx context.Context, partition uint32, writerID [16]byte) (catalog.WriterSession, error) {
	return c.inner.OpenWriter(ctx, partition, writerID)
}

func (c *countingCatalog) InitializePartition(ctx context.Context, partition uint32, nextLSN uint64) (pmeta.PartitionHead, bool, error) {
	return c.inner.InitializePartition(ctx, partition, nextLSN)
}

type countingTestPartitionLogStore struct {
	catalog *countingCatalog
	sink    *segmentsink.Factory
	source  *internalTestSegmentStore
}

func (s *countingTestPartitionLogStore) WriterManager() catalog.WriterManager { return s.catalog }
func (s *countingTestPartitionLogStore) ReaderCatalog() catalog.Reader        { return s.catalog }
func (s *countingTestPartitionLogStore) SinkFactory() plwriter.SinkFactory    { return s.sink }
func (s *countingTestPartitionLogStore) SegmentStore() partitionlog.SegmentStore {
	return s.source
}

type internalNoopWalIO struct{}

func (n *internalNoopWalIO) Write(*v1.WALRecord) error { return nil }

func (n *internalNoopWalIO) WriteBatch([]*v1.WALRecord) error { return nil }

type internalTestPartitionLogStore struct {
	catalog *catalog.MemoryCatalog
	sink    *segmentsink.Factory
	source  *internalTestSegmentStore
}

func (s *internalTestPartitionLogStore) WriterManager() catalog.WriterManager { return s.catalog }
func (s *internalTestPartitionLogStore) ReaderCatalog() catalog.Reader        { return s.catalog }
func (s *internalTestPartitionLogStore) SinkFactory() plwriter.SinkFactory    { return s.sink }
func (s *internalTestPartitionLogStore) SegmentStore() partitionlog.SegmentStore {
	return s.source
}

type internalTestSegmentStore struct {
	objects *multipart.MemoryStore
}

func (s *internalTestSegmentStore) ReadAt(ctx context.Context, uri string, off uint64, n uint64) ([]byte, error) {
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
