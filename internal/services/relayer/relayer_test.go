package relayer

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"path/filepath"
	"testing"
	"time"

	"github.com/ankur-anand/unisondb/dbkernel"
	v1 "github.com/ankur-anand/unisondb/schemas/proto/gen/go/unisondb/streamer/v1"
	"github.com/brianvoe/gofakeit/v7"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
)

func TestRelayer_LogLag(t *testing.T) {
	baseDir := t.TempDir()
	namespace := "relayer"

	engine, err := dbkernel.NewStorageEngine(baseDir, namespace, dbkernel.NewDefaultEngineConfig())
	assert.NoError(t, err)

	t.Cleanup(func() {
		assert.NoError(t, engine.Close(context.Background()))
	})

	var logBuf bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&logBuf, &slog.HandlerOptions{Level: slog.LevelInfo}))
	relayer := NewRelayer(engine, namespace, nil, 1, logger)
	// should not panic
	relayer.logLag(nil, nil)

	remoteOffset := &dbkernel.Offset{
		SegmentID: 10,
		Offset:    10,
	}

	localOffset := &dbkernel.Offset{
		SegmentID: 8,
		Offset:    8,
	}

	relayer.logLag(remoteOffset, localOffset)
	logStr := logBuf.String()
	assert.Contains(t, logStr, "segment.lag.threshold.exceeded")
	assert.Contains(t, logStr, "namespace")
	assert.Contains(t, logStr, "offset.remote.segment_id")
	assert.Contains(t, logStr, "offset.remote.segment_id")

	relayer.logLag(remoteOffset, nil)
	relayer.logLag(nil, localOffset)
}

type mockStreamer struct {
	injectErr    error
	mockedOffset *dbkernel.Offset
	engine       *dbkernel.Engine
	walIOHandler walIOHandler
}

func (m *mockStreamer) GetLatestOffset(ctx context.Context) (*dbkernel.Offset, error) {
	if m.injectErr != nil {
		return nil, m.injectErr
	}
	if m.mockedOffset != nil {
		return m.mockedOffset, nil
	}
	return m.engine.CurrentOffset(), nil
}

func (m *mockStreamer) StreamWAL(ctx context.Context) error {
	if m.injectErr != nil {
		return m.injectErr
	}
	reader, err := m.engine.NewReader()
	if err != nil {
		return err
	}

	for {
		value, offset, err := reader.Next()
		if err != nil {
			if errors.Is(err, io.EOF) {
				return nil
			}
			return err
		}

		err = m.walIOHandler.Write(&v1.WALRecord{Record: value, Offset: offset.Encode()})
		if err != nil {
			return err
		}
	}
}

func TestRelayer_Monitor(t *testing.T) {
	baseDir := t.TempDir()
	namespace := "relayer"

	engine, err := dbkernel.NewStorageEngine(baseDir, namespace, dbkernel.NewDefaultEngineConfig())
	assert.NoError(t, err)

	t.Cleanup(func() {
		assert.NoError(t, engine.Close(context.Background()))
	})

	var logBuf bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&logBuf, &slog.HandlerOptions{Level: slog.LevelInfo}))
	relayer := NewRelayer(engine, namespace, nil, 1, logger)

	mockStreamer := &mockStreamer{
		engine:       engine,
		walIOHandler: relayer.walIOHandler,
		injectErr:    fmt.Errorf("io/error"),
	}

	relayer.client = mockStreamer

	relayer.monitor(t.Context())
	mockStreamer.injectErr = nil
	relayer.monitor(t.Context())
	mockStreamer.mockedOffset = &dbkernel.Offset{
		SegmentID: 10,
		Offset:    10,
	}
	relayer.monitor(t.Context())
	err = relayer.StartRelay(t.Context())
	assert.ErrorIs(t, err, ErrSegmentLagThresholdExceeded)
	logStr := logBuf.String()
	assert.Contains(t, logStr, "segment.lag.threshold.exceeded")
	assert.Contains(t, logStr, "event_type=error")

	assert.Equal(t, 1, testutil.CollectAndCount(segmentLagGauge))
	assert.Equal(t, 1, testutil.CollectAndCount(segmentLagThresholdGauge))
}

func TestRelayer_StartRelay(t *testing.T) {
	baseDir := t.TempDir()
	namespace := "relayer"

	engine, err := dbkernel.NewStorageEngine(baseDir, namespace, dbkernel.NewDefaultEngineConfig())
	assert.NoError(t, err)

	t.Cleanup(func() {
		assert.NoError(t, engine.Close(context.Background()))
	})

	mockedDir := filepath.Join(baseDir, "mockedDir")
	mockedNamespace := "mocked_upstream"
	mockedEngine, err := dbkernel.NewStorageEngine(mockedDir, mockedNamespace, dbkernel.NewDefaultEngineConfig())
	assert.NoError(t, err)
	t.Cleanup(func() {
		assert.NoError(t, mockedEngine.Close(context.Background()))
	})

	insertedKV := make(map[string]string)
	for i := 0; i < 100; i++ {
		key := gofakeit.UUID()
		val := gofakeit.Sentence(i + 1)
		insertedKV[key] = val
		err := mockedEngine.Put([]byte(key), []byte(val))
		assert.NoError(t, err, "error putting value to engine")
	}

	var logBuf bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&logBuf, &slog.HandlerOptions{Level: slog.LevelInfo}))
	relayer := NewRelayer(engine, namespace, nil, 1, logger)
	relayer.offsetMonitorInterval = 1 * time.Millisecond

	mockStreamer := &mockStreamer{
		engine:       mockedEngine,
		walIOHandler: relayer.walIOHandler,
	}

	relayer.client = mockStreamer

	err = relayer.StartRelay(t.Context())
	assert.NoError(t, err)
	logStr := logBuf.String()
	assert.Contains(t, logStr, "relayer.relay.started")
}
