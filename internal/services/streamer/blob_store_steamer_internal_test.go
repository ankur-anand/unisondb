package streamer

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ankur-anand/objlog"
	"github.com/ankur-anand/unisondb/dbkernel"
	"github.com/ankur-anand/unisondb/internal/testutil/objlogtest"
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

	log := objlogtest.NewLog(t, nil)
	streamer, err := NewBlobStoreStreamer(ctx, nil, map[string]*dbkernel.Engine{namespace: engine}, map[string]*objlog.Log{namespace: log}, BlobStoreStreamerConfig{
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

	log := objlogtest.NewLog(t, nil)
	_, err = log.InitializePartition(ctx, objlog.InitializePartition{Partition: 0, NextLSN: 10})
	require.NoError(t, err)

	streamer, err := NewBlobStoreStreamer(ctx, nil, map[string]*dbkernel.Engine{namespace: engine}, map[string]*objlog.Log{namespace: log}, BlobStoreStreamerConfig{
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

func TestBlobStoreStreamer_ReopenUsesNewWriterIdentity(t *testing.T) {
	ctx := context.Background()
	const namespace = "orders"
	log := objlogtest.NewLog(t, nil)
	openStreamer := func() *BlobStoreStreamer {
		s, err := NewBlobStoreStreamer(ctx, nil, nil, map[string]*objlog.Log{namespace: log}, DefaultBlobStoreStreamerConfig())
		require.NoError(t, err)
		t.Cleanup(func() { require.NoError(t, s.Close()) })
		return s
	}

	first := openStreamer()
	firstState, err := first.namespaceState(ctx, namespace)
	require.NoError(t, err)
	firstIdentity := firstState.writer.State().Snapshot.Identity
	_, err = firstState.writer.Append(ctx, objlog.Record{TimestampMS: 1, Value: []byte("record")})
	require.NoError(t, err)
	require.NoError(t, first.Close())

	secondState, err := openStreamer().namespaceState(ctx, namespace)
	require.NoError(t, err)
	secondIdentity := secondState.writer.State().Snapshot.Identity
	assert.NotEqual(t, firstIdentity.Tag, secondIdentity.Tag)
	assert.Greater(t, secondIdentity.Epoch, firstIdentity.Epoch)
	assert.Equal(t, uint64(1), secondState.startLSN, "reopening should resume after the committed record")
}

func TestNewBlobStoreStreamerDefaultsMaxRecordsForKVWorkloads(t *testing.T) {
	ctx := context.Background()
	namespace := "defaults"
	engine, err := dbkernel.NewStorageEngine(t.TempDir(), namespace, dbkernel.NewDefaultEngineConfig())
	require.NoError(t, err)
	t.Cleanup(func() { _ = engine.Close(context.Background()) })

	s, err := NewBlobStoreStreamer(ctx, nil, map[string]*dbkernel.Engine{namespace: engine}, map[string]*objlog.Log{namespace: objlogtest.NewLog(t, nil)}, BlobStoreStreamerConfig{})
	require.NoError(t, err)
	t.Cleanup(func() { _ = s.Close() })

	assert.Equal(t, uint32(1_048_576), s.cfg.Batch.MaxRecords)
}

func TestNewBlobStoreStreamerKeepsConfiguredMaxRecords(t *testing.T) {
	ctx := context.Background()
	namespace := "configured"
	engine, err := dbkernel.NewStorageEngine(t.TempDir(), namespace, dbkernel.NewDefaultEngineConfig())
	require.NoError(t, err)
	t.Cleanup(func() { _ = engine.Close(context.Background()) })

	s, err := NewBlobStoreStreamer(ctx, nil, map[string]*dbkernel.Engine{namespace: engine}, map[string]*objlog.Log{namespace: objlogtest.NewLog(t, nil)}, BlobStoreStreamerConfig{
		Batch: objlog.BatchPolicy{
			MaxRecords: 64,
		},
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = s.Close() })

	assert.Equal(t, uint32(64), s.cfg.Batch.MaxRecords)
}

func TestBlobStoreStreamerClientApplyCommittedRangeUsesCachedHead(t *testing.T) {
	ctx := context.Background()
	metrics := &catalogRefreshMetrics{}
	log := objlogtest.NewLog(t, metrics)
	writer, err := log.OpenWriter(ctx, objlog.WriterOptions{
		Partition: 0,
		WriterID:  [16]byte{1},
		Batch: objlog.BatchPolicy{
			MaxRecords: 64,
		},
	})
	require.NoError(t, err)

	recordCount := batchSize*2 + 7
	for i := 0; i < recordCount; i++ {
		_, err = writer.Append(ctx, objlog.Record{
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

	require.Positive(t, metrics.refreshes.Load(), "latest head read should refresh the catalog")
	metrics.refreshes.Store(0)
	require.NoError(t, client.applyCommittedRange(ctx, latest))
	assert.Zero(t, metrics.refreshes.Load(), "batch reads should use cached head after latest head refresh")
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

	log := objlogtest.NewLog(t, nil)
	srv, err := NewBlobStoreStreamer(ctx, nil, map[string]*dbkernel.Engine{namespace: engine}, map[string]*objlog.Log{namespace: log}, BlobStoreStreamerConfig{
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

func TestObjLogStoreFactoryURLValidation(t *testing.T) {
	tests := []struct {
		name      string
		bucketURL string
		wantErr   string
	}{
		{name: "unsupported", bucketURL: "file:///tmp/log", wantErr: "unsupported objlog bucket scheme"},
		{name: "missing s3 bucket", bucketURL: "s3://", wantErr: "s3 bucket missing"},
		{name: "missing gcs bucket", bucketURL: "gcs://", wantErr: "gcs bucket missing"},
		{name: "missing azure container", bucketURL: "azblob://", wantErr: "azblob container missing"},
		{name: "nested azure container", bucketURL: "azblob://container/nested", wantErr: "identify exactly one container"},
		{name: "bad azure auth", bucketURL: "azblob://container?url=http://127.0.0.1:10000/container&auth=bogus", wantErr: "unsupported azblob auth mode"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := newObjLogStoreFactory(context.Background(), tt.bucketURL, "prefix")
			require.Error(t, err)
			assert.Contains(t, err.Error(), tt.wantErr)
		})
	}
}

func TestObjLogStoreFactorySupportsLocalProviderURLs(t *testing.T) {
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
			factory, err := newObjLogStoreFactory(context.Background(), tt.bucketURL, "prefix")
			require.NoError(t, err)
			store, err := factory.openStore("orders")
			require.NoError(t, err)
			assert.NotNil(t, store)
		})
	}
}

type catalogRefreshMetrics struct {
	refreshes atomic.Int64
}

func (m *catalogRefreshMetrics) Observe(metric objlog.Metric) {
	if metric.Name == objlog.MetricReaderCatalogRefresh {
		m.refreshes.Add(1)
	}
}

type internalNoopWalIO struct{}

func (n *internalNoopWalIO) Write(*v1.WALRecord) error { return nil }

func (n *internalNoopWalIO) WriteBatch([]*v1.WALRecord) error { return nil }
