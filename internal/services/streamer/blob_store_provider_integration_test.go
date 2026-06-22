package streamer_test

import (
	"context"
	"errors"
	"fmt"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/ankur-anand/unijord/partitionlog"
	plgcs "github.com/ankur-anand/unijord/partitionlog/gcs"
	pls3 "github.com/ankur-anand/unijord/partitionlog/s3"
	"github.com/ankur-anand/unisondb/dbkernel"
	"github.com/ankur-anand/unisondb/internal/services/streamer"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/fsouza/fake-gcs-server/fakestorage"
	"github.com/johannesboyne/gofakes3"
	"github.com/johannesboyne/gofakes3/backend/s3mem"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBlobStoreStreamer_ProviderBackedIntegration(t *testing.T) {
	tests := []struct {
		name    string
		openLog func(t *testing.T) func(namespace string) *partitionlog.Log
	}{
		{
			name:    "s3",
			openLog: newFakeS3LogOpener,
		},
		{
			name:    "gcs",
			openLog: newFakeGCSLogOpener,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			runProviderBackedStreamerIntegration(t, tt.openLog(t))
		})
	}
}

func runProviderBackedStreamerIntegration(t *testing.T, openLog func(namespace string) *partitionlog.Log) {
	t.Helper()

	namespace := "provider-" + safeTestPath(t.Name())
	engine := createNamedEngine(t, namespace)

	putProviderRecords(t, engine, 0, 12)
	require.Equal(t, uint64(12), engine.OpsReceivedCount())

	log := openLog(namespace)
	streamProviderUntilLatest(t, namespace, engine, log, 12)

	reopened := openLog(namespace)
	requireLatestLSN(t, reopened, 12)

	putProviderRecords(t, engine, 12, 8)
	require.Equal(t, uint64(20), engine.OpsReceivedCount())

	reopened = openLog(namespace)
	streamProviderUntilLatest(t, namespace, engine, reopened, 20)

	reopened = openLog(namespace)
	requireLatestLSN(t, reopened, 20)

	records := &recordingWalIO{}
	client := streamer.NewBlobStoreStreamerClient(reopened, namespace, records, 0, 10*time.Millisecond)
	streamCtx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	err := client.StreamWAL(streamCtx)
	assert.True(t, err == nil || errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled), "unexpected stream error: %v", err)
	require.Equal(t, 20, records.count())
}

func streamProviderUntilLatest(t *testing.T, namespace string, engine *dbkernel.Engine, log *partitionlog.Log, want uint64) {
	t.Helper()

	ctx, cancel := context.WithCancel(context.Background())
	cfg := streamer.DefaultBlobStoreStreamerConfig()
	cfg.FlushInterval = 25 * time.Millisecond

	srv, err := streamer.NewBlobStoreStreamer(ctx, nil, map[string]*dbkernel.Engine{namespace: engine}, map[string]*partitionlog.Log{namespace: log}, cfg)
	require.NoError(t, err)
	defer func() {
		cancel()
		require.NoError(t, srv.Close())
	}()

	done := make(chan error, 1)
	go func() {
		done <- srv.StreamNamespace(ctx, namespace)
	}()

	require.Eventually(t, func() bool {
		select {
		case err := <-done:
			require.FailNowf(t, "stream ended before reaching latest LSN", "error=%v", err)
		default:
		}
		latest, err := latestLSN(log)
		if err != nil {
			return false
		}
		return latest == want
	}, 5*time.Second, 25*time.Millisecond)

	cancel()
	err = <-done
	assert.True(t, err == nil || errors.Is(err, context.Canceled), "unexpected stream error: %v", err)
}

func requireLatestLSN(t *testing.T, log *partitionlog.Log, want uint64) {
	t.Helper()

	require.Eventually(t, func() bool {
		latest, err := latestLSN(log)
		return err == nil && latest == want
	}, 5*time.Second, 25*time.Millisecond)
}

func latestLSN(log *partitionlog.Log) (uint64, error) {
	client := streamer.NewBlobStoreStreamerClient(log, "provider-reader", &noopWalIO{}, 0, 10*time.Millisecond)
	return client.GetLatestLSN(context.Background())
}

func putProviderRecords(t *testing.T, engine *dbkernel.Engine, start, count int) {
	t.Helper()

	for i := 0; i < count; i++ {
		n := start + i
		require.NoError(t, engine.PutKV(
			[]byte(fmt.Sprintf("provider-key-%03d", n)),
			[]byte(fmt.Sprintf("provider-value-%03d", n)),
		))
	}
}

func newFakeS3LogOpener(t *testing.T) func(namespace string) *partitionlog.Log {
	t.Helper()

	bucket := "unisondb-provider-s3"
	backend := s3mem.New()
	require.NoError(t, backend.CreateBucket(bucket))

	faker := gofakes3.New(backend)
	server := httptest.NewServer(faker.Server())
	t.Cleanup(server.Close)

	cfg, err := config.LoadDefaultConfig(context.Background(),
		config.WithRegion("us-east-1"),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("access-key", "secret-key", "")),
		config.WithRequestChecksumCalculation(aws.RequestChecksumCalculationWhenRequired),
		config.WithResponseChecksumValidation(aws.ResponseChecksumValidationWhenRequired),
	)
	require.NoError(t, err)

	client := awss3.NewFromConfig(cfg, func(o *awss3.Options) {
		o.BaseEndpoint = aws.String(server.URL)
		o.UsePathStyle = true
	})
	prefix := "provider-tests/" + safeTestPath(t.Name())

	return func(namespace string) *partitionlog.Log {
		store, err := pls3.New(pls3.Options{
			Client:   client,
			Bucket:   bucket,
			Prefix:   prefix,
			StreamID: namespace,
		})
		require.NoError(t, err)
		log, err := partitionlog.Open(partitionlog.Options{Store: store})
		require.NoError(t, err)
		return log
	}
}

func newFakeGCSLogOpener(t *testing.T) func(namespace string) *partitionlog.Log {
	t.Helper()

	bucket := "unisondb-provider-gcs"
	server, err := fakestorage.NewServerWithOptions(fakestorage.Options{NoListener: true})
	require.NoError(t, err)
	t.Cleanup(server.Stop)
	server.CreateBucket(bucket)

	client := server.Client()
	t.Cleanup(func() { _ = client.Close() })
	prefix := "provider-tests/" + safeTestPath(t.Name())

	return func(namespace string) *partitionlog.Log {
		store, err := plgcs.New(plgcs.Options{
			Client:   client,
			Bucket:   bucket,
			Prefix:   prefix,
			StreamID: namespace,
		})
		require.NoError(t, err)
		log, err := partitionlog.Open(partitionlog.Options{Store: store})
		require.NoError(t, err)
		return log
	}
}

func safeTestPath(s string) string {
	s = strings.ToLower(s)
	replacer := strings.NewReplacer(
		"/", "-",
		" ", "-",
		"_", "-",
	)
	return replacer.Replace(s)
}
