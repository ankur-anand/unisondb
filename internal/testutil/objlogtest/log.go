// Package objlogtest provides objlog fixtures backed by in-process object storage.
package objlogtest

import (
	"testing"

	"github.com/ankur-anand/objlog"
	"github.com/ankur-anand/objlog/gcs"
	"github.com/fsouza/fake-gcs-server/fakestorage"
	"github.com/stretchr/testify/require"
)

// NewLog opens an isolated log using the public GCS provider without a listener.
// The log, client, and fake server are closed when the test finishes.
func NewLog(t testing.TB, metrics objlog.Metrics) *objlog.Log {
	t.Helper()
	server, err := fakestorage.NewServerWithOptions(fakestorage.Options{NoListener: true})
	require.NoError(t, err)
	t.Cleanup(server.Stop)
	const bucket = "unisondb-objlog-test"
	server.CreateBucketWithOpts(fakestorage.CreateBucketOpts{Name: bucket})

	client := server.Client()
	t.Cleanup(func() { require.NoError(t, client.Close()) })
	store, err := gcs.New(gcs.Options{
		Client:   client,
		Bucket:   bucket,
		StreamID: "test",
	})
	require.NoError(t, err)
	log, err := objlog.Open(objlog.Options{Store: store, Metrics: metrics})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, log.Close()) })
	return log
}
