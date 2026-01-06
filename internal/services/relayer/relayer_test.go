package relayer

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ankur-anand/unisondb/dbkernel"
	v1 "github.com/ankur-anand/unisondb/schemas/proto/gen/go/unisondb/streamer/v1"
	"github.com/brianvoe/gofakeit/v7"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"golang.org/x/time/rate"
)

type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (s *syncBuffer) Write(p []byte) (n int, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.buf.Write(p)
}

func (s *syncBuffer) String() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.buf.String()
}

type mockStreamer struct {
	injectErr    error
	engine       *dbkernel.Engine
	walIOHandler WalIO
}

func (m *mockStreamer) GetLatestLSN(ctx context.Context) (uint64, error) {
	if m.injectErr != nil {
		return 0, m.injectErr
	}
	return m.engine.OpsReceivedCount(), nil
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
		value, _, err := reader.Next()
		if err != nil {
			if errors.Is(err, io.EOF) {
				return nil
			}
			return err
		}

		err = m.walIOHandler.Write(&v1.WALRecord{
			Record: value,
		})
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

	var logBuf syncBuffer
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
	logStr := logBuf.String()
	assert.Contains(t, logStr, "event_type=error")

	assert.Equal(t, 1, testutil.CollectAndCount(lsnLagGauge))
	assert.Equal(t, 1, testutil.CollectAndCount(lsnLagThresholdGauge))
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
		err := mockedEngine.PutKV([]byte(key), []byte(val))
		assert.NoError(t, err, "error putting value to engine")
	}

	var logBuf syncBuffer
	logger := slog.New(slog.NewTextHandler(&logBuf, &slog.HandlerOptions{Level: slog.LevelInfo}))
	relayer := NewRelayer(engine, namespace, nil, 1, logger)

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

type mockWalIO struct {
	writeCount atomic.Int64
}

func (m *mockWalIO) Write(data *v1.WALRecord) error {
	m.writeCount.Add(1)
	return nil
}

func (m *mockWalIO) WriteBatch(records []*v1.WALRecord) error {
	m.writeCount.Add(int64(len(records)))
	return nil
}

func TestRateLimitedWalIO_ContextCancellation(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	mock := &mockWalIO{}
	limiter := rate.NewLimiter(rate.Limit(1), 1)
	rlWalIO := NewRateLimitedWalIO(ctx, mock, limiter)

	record := &v1.WALRecord{
		Record: []byte("test-record"),
	}

	err := rlWalIO.Write(record)
	assert.NoError(t, err)

	var wg sync.WaitGroup
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = rlWalIO.Write(record)
		}()
		time.Sleep(100 * time.Millisecond)
	}

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		cancel()
	}

	assert.LessOrEqual(t, mock.writeCount.Load(), int64(3), "should have blocked excessive writes")
}

func TestRateLimitedWalIO_WriteBatch(t *testing.T) {
	t.Run("basic_batch_write", func(t *testing.T) {
		ctx := context.Background()
		mock := &mockWalIO{}
		limiter := rate.NewLimiter(rate.Limit(100), 100)
		rlWalIO := NewRateLimitedWalIO(ctx, mock, limiter)

		records := []*v1.WALRecord{
			{Record: []byte("record1")},
			{Record: []byte("record2")},
			{Record: []byte("record3")},
		}

		err := rlWalIO.WriteBatch(records)
		assert.NoError(t, err)
		assert.Equal(t, int64(3), mock.writeCount.Load(), "should have written 3 records")
	})

	t.Run("empty_batch", func(t *testing.T) {
		ctx := context.Background()
		mock := &mockWalIO{}
		limiter := rate.NewLimiter(rate.Limit(100), 100)
		rlWalIO := NewRateLimitedWalIO(ctx, mock, limiter)

		err := rlWalIO.WriteBatch([]*v1.WALRecord{})
		assert.NoError(t, err)
		assert.Equal(t, int64(0), mock.writeCount.Load(), "should not have written any records")
	})

	t.Run("rate_limiting_batch", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		mock := &mockWalIO{}

		limiter := rate.NewLimiter(rate.Limit(10), 10)
		rlWalIO := NewRateLimitedWalIO(ctx, mock, limiter)

		records := make([]*v1.WALRecord, 10)
		for i := 0; i < 10; i++ {
			records[i] = &v1.WALRecord{
				Record: []byte(fmt.Sprintf("record%d", i)),
			}
		}

		start := time.Now()
		err := rlWalIO.WriteBatch(records)
		duration := time.Since(start)
		assert.NoError(t, err)
		assert.Equal(t, int64(10), mock.writeCount.Load())
		assert.Less(t, duration, 100*time.Millisecond, "first batch should be fast (burst)")

		records2 := make([]*v1.WALRecord, 5)
		for i := 0; i < 5; i++ {
			records2[i] = &v1.WALRecord{
				Record: []byte(fmt.Sprintf("record%d", 10+i)),
			}
		}

		start = time.Now()
		err = rlWalIO.WriteBatch(records2)
		duration = time.Since(start)
		assert.NoError(t, err)
		assert.Equal(t, int64(15), mock.writeCount.Load())
		// Should wait ~400-500ms for 5 tokens at 10/sec rate
		assert.Greater(t, duration, 400*time.Millisecond, "second batch should be rate limited")
	})

	t.Run("context_cancellation_batch", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		mock := &mockWalIO{}

		limiter := rate.NewLimiter(rate.Limit(1), 1)
		rlWalIO := NewRateLimitedWalIO(ctx, mock, limiter)

		firstRecord := []*v1.WALRecord{
			{Record: []byte("record0")},
		}
		err := rlWalIO.WriteBatch(firstRecord)
		assert.NoError(t, err)

		largeRecords := make([]*v1.WALRecord, 10)
		for i := 0; i < 10; i++ {
			largeRecords[i] = &v1.WALRecord{
				Record: []byte(fmt.Sprintf("record%d", i+1)),
			}
		}

		go func() {
			time.Sleep(100 * time.Millisecond)
			cancel()
		}()

		err = rlWalIO.WriteBatch(largeRecords)
		assert.Error(t, err, "should fail due to context cancellation")
		assert.Contains(t, err.Error(), "rate limit exceeded")
		assert.Less(t, mock.writeCount.Load(), int64(11))
	})

	t.Run("concurrent_batch_writes", func(t *testing.T) {
		ctx := context.Background()
		mock := &mockWalIO{}
		limiter := rate.NewLimiter(rate.Limit(100), 100)
		rlWalIO := NewRateLimitedWalIO(ctx, mock, limiter)

		var wg sync.WaitGroup
		numWorkers := 5
		recordsPerBatch := 10

		for worker := 0; worker < numWorkers; worker++ {
			wg.Add(1)
			go func(workerID int) {
				defer wg.Done()
				records := make([]*v1.WALRecord, recordsPerBatch)
				for i := 0; i < recordsPerBatch; i++ {
					records[i] = &v1.WALRecord{
						Record: []byte(fmt.Sprintf("worker%d-record%d", workerID, i)),
					}
				}
				err := rlWalIO.WriteBatch(records)
				assert.NoError(t, err)
			}(worker)
		}

		wg.Wait()
		expectedTotal := int64(numWorkers * recordsPerBatch)
		assert.Equal(t, expectedTotal, mock.writeCount.Load(), "should have written all records from all workers")
	})
}

func TestWalIOHandler_WriteBatch(t *testing.T) {
	baseDir := t.TempDir()
	namespace := "test_wal_handler_batch"

	engine, err := dbkernel.NewStorageEngine(baseDir, namespace, dbkernel.NewDefaultEngineConfig())
	assert.NoError(t, err)
	defer engine.Close(context.Background())

	handler := dbkernel.NewReplicaWALHandler(engine)
	walHandler := walIOHandler{replica: handler}

	t.Run("invalid_records_panic", func(t *testing.T) {
		records := make([]*v1.WALRecord, 5)
		for i := 0; i < 5; i++ {
			kv := fmt.Sprintf("key%d", i)
			value := fmt.Sprintf("value%d", i)

			encodedKV := []byte(kv + ":" + value)

			record := &v1.WALRecord{
				Record: encodedKV,
			}
			records[i] = record
		}

		assert.Panics(t, func() {
			_ = walHandler.WriteBatch(records)
		}, "should panic with improperly encoded records")
	})

	t.Run("empty_batch", func(t *testing.T) {
		err := walHandler.WriteBatch([]*v1.WALRecord{})
		assert.NoError(t, err, "empty batch should succeed")
	})
}

func TestRelayer_WithRateLimiterWalWriter(t *testing.T) {
	baseDir := t.TempDir()
	namespace := "relayer"

	engine, err := dbkernel.NewStorageEngine(baseDir, namespace, dbkernel.NewDefaultEngineConfig())
	assert.NoError(t, err)

	t.Cleanup(func() {
		assert.NoError(t, engine.Close(context.Background()))
	})

	logger := slog.New(slog.NewTextHandler(io.Discard, &slog.HandlerOptions{}))

	rel := NewRelayer(engine, namespace, nil, 10, logger)
	_, isRateLimited := rel.CurrentWalIO().(*RateLimitedWalIO)

	assert.False(t, isRateLimited, "should be simple")

	rel.EnableRateLimitedWalIO(nil)
	_, isRateLimited = rel.CurrentWalIO().(*RateLimitedWalIO)

	assert.False(t, isRateLimited, "should be simple")

	rateLimit := 5
	burstSize := 2
	limiter := rate.NewLimiter(rate.Limit(rateLimit), burstSize)

	rateLimiterIO := NewRateLimitedWalIO(t.Context(), rel.walIOHandler, limiter)

	rel.EnableRateLimitedWalIO(rateLimiterIO)

	_, isRateLimited = rel.CurrentWalIO().(*RateLimitedWalIO)
	assert.True(t, isRateLimited, "should be of type RateLimitedWalIO after EnableRateLimitedWalIO")
	// before the start it should be allowed to change
	rel.startOffset = &dbkernel.Offset{SegmentID: 10, Offset: 100}
	rel.EnableRateLimitedWalIO(rateLimiterIO)
	_, isRateLimited = rel.CurrentWalIO().(*RateLimitedWalIO)
	assert.True(t, isRateLimited, "should be of type RateLimitedWalIO after EnableRateLimitedWalIO")

	rel.started.Store(true)
	defer func() {
		if r := recover(); r == nil {
			t.Fatal("Expected panic when EnableRateLimitedWalIO is called after StartRelay, but it did not panic")
		}
	}()

	rel.EnableRateLimitedWalIO(rateLimiterIO)

}
