package replicator

import (
	"context"
	"errors"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/ankur-anand/unisondb/dbkernel"
	v1 "github.com/ankur-anand/unisondb/schemas/proto/gen/go/unisondb/streamer/v1"
	"github.com/brianvoe/gofakeit/v7"
	"github.com/stretchr/testify/assert"
)

func TestReplicator_Replicate(t *testing.T) {
	baseDir := t.TempDir()
	namespace := "test_put_get"

	engine, err := dbkernel.NewStorageEngine(baseDir, namespace, dbkernel.NewDefaultEngineConfig())
	assert.NoError(t, err)
	t.Cleanup(func() {
		err := engine.Close(context.Background())
		if err != nil {
			t.Errorf("Failed to close engine: %v", err)
		}
	})

	for i := 0; i < 100; i++ {
		key := gofakeit.UUID()
		value := gofakeit.Sentence(100)
		assert.NoError(t, engine.PutKV([]byte(key), []byte(value)), "put should not fail")
	}

	batchSize := 10
	replicatorInstance := NewReplicator(engine,
		batchSize, 5*time.Second, nil, "testing")

	wg := &sync.WaitGroup{}
	wg.Add(1)
	ticker := time.NewTicker(100 * time.Millisecond)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// parallely put
	go func() {
		defer wg.Done()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				key := gofakeit.UUID()
				value := gofakeit.Sentence(100)
				assert.NoError(t, engine.PutKV([]byte(key), []byte(value)), "put should not fail")
			}
		}
	}()

	recvChan := make(chan []*v1.WALRecord, 10)
	wg.Add(1)
	go func() {
		defer wg.Done()
		err := replicatorInstance.Replicate(ctx, recvChan)
		assert.True(t, errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled),
			"expected context deadline or cancel, got: %v", err)
	}()

	var recvRecords []*v1.WALRecord

outer:
	for {
		select {
		case recs := <-recvChan:
			recvRecords = append(recvRecords, recs...)
			assert.LessOrEqual(t, len(recs), batchSize, "should be less then batch size")
		case <-ctx.Done():
			break outer
		}
	}

	wg.Wait()
	lastRecord := recvRecords[len(recvRecords)-1]
	lastOffset := dbkernel.DecodeOffset(lastRecord.Offset)
	r, err := engine.NewReaderWithStart(lastOffset)
	assert.NoError(t, err)
	pendingReadCount := 0
	for {
		_, _, err := r.Next()
		if err != nil {
			break
		}
		pendingReadCount++
	}

	expected := len(recvRecords) + pendingReadCount - 1
	assert.Equal(t, uint64(expected), engine.OpsReceivedCount(),
		"expected %d records, got %d",
		expected, engine.OpsReceivedCount())
}

func TestReplicator_StartFromNonZeroOffset(t *testing.T) {
	baseDir := t.TempDir()
	namespace := "test_start_offset"

	engine, err := dbkernel.NewStorageEngine(baseDir, namespace, dbkernel.NewDefaultEngineConfig())
	assert.NoError(t, err)
	t.Cleanup(func() {
		_ = engine.Close(context.Background())
	})

	var offsets []*dbkernel.Offset
	for i := 0; i < 10; i++ {
		key := []byte("key" + strconv.Itoa(i))
		value := []byte("value" + strconv.Itoa(i))
		assert.NoError(t, engine.PutKV(key, value))
		time.Sleep(10 * time.Millisecond)
		last := engine.CurrentOffset()
		offsets = append(offsets, last)
	}

	startOffset := offsets[4]

	rep := NewReplicator(engine, 3, 2*time.Second, startOffset, "start-from-offset")

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	recvChan := make(chan []*v1.WALRecord, 5)

	go func() {
		_ = rep.Replicate(ctx, recvChan)
	}()

	var all []*v1.WALRecord
collect:
	for {
		select {
		case batch := <-recvChan:
			all = append(all, batch...)
		case <-ctx.Done():
			break collect
		}
	}

	assert.Len(t, all, 5, "should receive only records after start offset")

	for i, record := range all {
		offset := dbkernel.DecodeOffset(record.Offset)
		assert.True(t, isOffsetGreater(offset, startOffset),
			"record %d offset %v should be > %v", i, offset, startOffset)
	}
}

func isOffsetGreater(a, b *dbkernel.Offset) bool {
	if a.SegmentID > b.SegmentID {
		return true
	}
	if a.SegmentID == b.SegmentID && a.Offset > b.Offset {
		return true
	}
	return false
}

func TestReplicator_ContextCancelledBeforeLoop(t *testing.T) {
	baseDir := t.TempDir()
	namespace := "test_ctx_cancel"

	engine, err := dbkernel.NewStorageEngine(baseDir, namespace, dbkernel.NewDefaultEngineConfig())
	assert.NoError(t, err)
	t.Cleanup(func() { _ = engine.Close(context.Background()) })

	assert.NoError(t, engine.PutKV([]byte("key"), []byte("value")))

	rep := NewReplicator(engine, 5, 1*time.Second, nil, "testing")
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	recvChan := make(chan []*v1.WALRecord)

	err = rep.Replicate(ctx, recvChan)
	assert.Error(t, err)
	assert.True(t, errors.Is(err, context.Canceled), "expected context.Canceled, got %v", err)
}

func TestReplicator_SendFuncBlockedCtxDone(t *testing.T) {
	baseDir := t.TempDir()
	namespace := "test_sendfunc_ctxdone"

	engine, err := dbkernel.NewStorageEngine(baseDir, namespace, dbkernel.NewDefaultEngineConfig())
	assert.NoError(t, err)
	t.Cleanup(func() { _ = engine.Close(context.Background()) })

	for i := 0; i < 10; i++ {
		key := gofakeit.UUID()
		value := gofakeit.Sentence(10)
		assert.NoError(t, engine.PutKV([]byte(key), []byte(value)))
	}

	recvChan := make(chan []*v1.WALRecord)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	rep := NewReplicator(engine, 3, 1*time.Second, nil, "test-engine")
	close(rep.ctxDone)

	err = rep.replicateFromReader(ctx, recvChan)
	assert.NoError(t, err)
	assert.Nil(t, rep.reader, "reader should be nil after close")
}
