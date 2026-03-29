package streamer

import (
	"context"
	"testing"

	"github.com/ankur-anand/isledb"
	"github.com/ankur-anand/isledb/blobstore"
	"github.com/ankur-anand/isledb/manifest"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBlobStoreStreamer_PutRecordWithRetry_WriterClosedFailsNamespace(t *testing.T) {
	ctx := context.Background()
	namespace := "writer-closed"
	store := blobstore.NewMemory("writer-closed")
	t.Cleanup(func() { _ = store.Close() })

	writerOpts := isledb.DefaultWriterOptions()
	writerOpts.FlushInterval = -1

	streamer, err := NewBlobStoreStreamer(ctx, nil, nil, map[string]*blobstore.Store{
		namespace: store,
	}, BlobStoreStreamerConfig{
		WriterOptions: &writerOpts,
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = streamer.Close() })

	nsState, err := streamer.namespaceState(ctx, namespace)
	require.NoError(t, err)
	require.NoError(t, nsState.close())

	err = streamer.putRecordWithRetry(ctx, nsState, 1, []byte("closed"))
	require.Error(t, err)
	assert.EqualError(t, err, blobStoreWriterClosedErr)

	streamer.mu.Lock()
	_, ok := streamer.namespaces[namespace]
	streamer.mu.Unlock()
	assert.False(t, ok, "closed namespace state should be removed")

	reopened, err := streamer.namespaceState(ctx, namespace)
	require.NoError(t, err)
	assert.NotSame(t, nsState, reopened)
}

func TestBlobStoreStreamer_PutRecordWithRetry_FencedFailsNamespace(t *testing.T) {
	ctx := context.Background()
	namespace := "writer-fenced"
	store := blobstore.NewMemory("writer-fenced")
	t.Cleanup(func() { _ = store.Close() })

	writerOpts := isledb.DefaultWriterOptions()
	writerOpts.FlushInterval = -1

	streamer, err := NewBlobStoreStreamer(ctx, nil, nil, map[string]*blobstore.Store{
		namespace: store,
	}, BlobStoreStreamerConfig{
		WriterOptions: &writerOpts,
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = streamer.Close() })

	nsState, err := streamer.namespaceState(ctx, namespace)
	require.NoError(t, err)

	require.NoError(t, nsState.writer.Put(EncodeLSNKey(1), []byte("seed")))

	competingDB, err := isledb.OpenDB(ctx, store, isledb.DBOptions{
		CommittedLSNExtractor: isledb.BigEndianUint64LSNExtractor,
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = competingDB.Close() })

	competingWriter, err := competingDB.OpenWriter(ctx, writerOpts)
	require.NoError(t, err)
	t.Cleanup(func() { _ = competingWriter.Close() })

	require.ErrorIs(t, nsState.writer.Flush(ctx), manifest.ErrFenced)

	err = streamer.putRecordWithRetry(ctx, nsState, 2, []byte("fenced"))
	require.ErrorIs(t, err, manifest.ErrFenced)

	streamer.mu.Lock()
	_, ok := streamer.namespaces[namespace]
	streamer.mu.Unlock()
	assert.False(t, ok, "fenced namespace state should be removed")
}
