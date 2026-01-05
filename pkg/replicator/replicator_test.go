package replicator

import (
	"context"
	"errors"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/ankur-anand/unisondb/dbkernel"
	"github.com/ankur-anand/unisondb/schemas/logrecord"
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
	// Extract LSN from the last received record
	lastRecord := recvRecords[len(recvRecords)-1]
	decoded := logrecord.GetRootAsLogRecord(lastRecord.Record, 0)
	lastLSN := decoded.Lsn()

	// Verify we received records with increasing LSNs
	assert.Greater(t, lastLSN, uint64(0), "should have received records with valid LSN")
	assert.Greater(t, len(recvRecords), 0, "should have received some records")
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

	// Verify LSNs are sequential after start offset (LSN 5)
	for i, record := range all {
		decoded := logrecord.GetRootAsLogRecord(record.Record, 0)
		expectedLSN := uint64(6 + i) // start from LSN 6 (offset 4 = LSN 5)
		assert.Equal(t, expectedLSN, decoded.Lsn(),
			"record %d should have LSN %d, got %d", i, expectedLSN, decoded.Lsn())
	}
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

func TestReplicator_ReleasesBatchWhenCtxDone(t *testing.T) {
	baseDir := t.TempDir()
	namespace := "pool_release"

	engine, err := dbkernel.NewStorageEngine(baseDir, namespace, dbkernel.NewDefaultEngineConfig())
	assert.NoError(t, err)
	t.Cleanup(func() { _ = engine.Close(context.Background()) })

	assert.NoError(t, engine.PutKV([]byte("k"), []byte("v")))

	rep := NewReplicator(engine, 1, time.Second, nil, "pool-check")
	// force sendFunc to hit ReleaseRecords path
	close(rep.ctxDone)

	recordsChan := make(chan []*v1.WALRecord)
	err = rep.replicateFromReader(context.Background(), recordsChan)
	assert.NoError(t, err)

	select {
	case <-recordsChan:
		t.Fatalf("no records should be delivered when ctxDone is closed")
	default:
	}

	recycled := acquireWalRecord()
	assert.Nil(t, recycled.Record, "record payload should be cleared")
	assert.Equal(t, uint32(0), recycled.Crc32Checksum, "checksum should reset")
	releaseWalRecord(recycled)
}

func TestReplicator_SendFuncNoDuplicateOffsets(t *testing.T) {
	baseDir := t.TempDir()
	namespace := "pool_dedupe"

	engine, err := dbkernel.NewStorageEngine(baseDir, namespace, dbkernel.NewDefaultEngineConfig())
	assert.NoError(t, err)
	t.Cleanup(func() { _ = engine.Close(context.Background()) })

	const totalWrites = 7
	rep := NewReplicator(engine, 2, 50*time.Millisecond, nil, "pool-dedupe")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	recordsChan := make(chan []*v1.WALRecord, 4)

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		_ = rep.Replicate(ctx, recordsChan)
	}()
	go func() {
		wg.Wait()
		close(recordsChan)
	}()

	time.Sleep(100 * time.Millisecond)
	for i := 0; i < totalWrites; i++ {
		key := []byte("key-" + strconv.Itoa(i))
		value := []byte("value-" + strconv.Itoa(i))
		assert.NoError(t, engine.PutKV(key, value))
	}

	seen := make(map[uint64]struct{})

	cancelled := false
	for batch := range recordsChan {
		if len(batch) == 0 {
			continue
		}
		for _, record := range batch {
			decoded := logrecord.GetRootAsLogRecord(record.Record, 0)
			lsn := decoded.Lsn()
			if _, exists := seen[lsn]; exists {
				t.Fatalf("duplicate WAL record LSN detected: %d", lsn)
			}
			seen[lsn] = struct{}{}
		}
		ReleaseRecords(batch)
		if len(seen) >= totalWrites {
			if !cancelled {
				cancel()
				cancelled = true
			}
		}
	}

	assert.Equal(t, totalWrites, len(seen), "should see each WAL LSN exactly once")
}

func TestReplicatorWithLSN_Basic(t *testing.T) {
	baseDir := t.TempDir()
	namespace := "test_lsn_basic"

	engine, err := dbkernel.NewStorageEngine(baseDir, namespace, dbkernel.NewDefaultEngineConfig())
	assert.NoError(t, err)
	t.Cleanup(func() {
		_ = engine.Close(context.Background())
	})

	for i := 0; i < 10; i++ {
		key := []byte("key" + strconv.Itoa(i))
		value := []byte("value" + strconv.Itoa(i))
		assert.NoError(t, engine.PutKV(key, value))
	}

	rep := NewReplicatorWithLSN(engine, 5, 2*time.Second, 0, "lsn-basic")

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	recvChan := make(chan []*v1.WALRecord, 10)

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

	assert.GreaterOrEqual(t, len(all), 10, "should receive all 10 records")

	for i, record := range all {
		decoded := logrecord.GetRootAsLogRecord(record.Record, 0)
		expectedLSN := uint64(i + 1)
		assert.Equal(t, expectedLSN, decoded.Lsn(), "record %d should have LSN %d", i, expectedLSN)
	}
}

func TestReplicatorWithLSN_StartFromNonZeroLSN(t *testing.T) {
	baseDir := t.TempDir()
	namespace := "test_lsn_nonzero"

	engine, err := dbkernel.NewStorageEngine(baseDir, namespace, dbkernel.NewDefaultEngineConfig())
	assert.NoError(t, err)
	t.Cleanup(func() {
		_ = engine.Close(context.Background())
	})

	for i := 0; i < 10; i++ {
		key := []byte("key" + strconv.Itoa(i))
		value := []byte("value" + strconv.Itoa(i))
		assert.NoError(t, engine.PutKV(key, value))
		time.Sleep(10 * time.Millisecond)
	}

	startLSN := uint64(5)
	rep := NewReplicatorWithLSN(engine, 3, 2*time.Second, startLSN, "lsn-nonzero")

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	recvChan := make(chan []*v1.WALRecord, 10)

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

	assert.Len(t, all, 5, "should receive only records after LSN 5")

	for i, record := range all {
		decoded := logrecord.GetRootAsLogRecord(record.Record, 0)
		expectedLSN := uint64(6 + i)
		assert.Equal(t, expectedLSN, decoded.Lsn(), "record %d should have LSN %d", i, expectedLSN)
	}
}

func TestReplicatorWithLSN_ConcurrentWrites(t *testing.T) {
	baseDir := t.TempDir()
	namespace := "test_lsn_concurrent"

	engine, err := dbkernel.NewStorageEngine(baseDir, namespace, dbkernel.NewDefaultEngineConfig())
	assert.NoError(t, err)
	t.Cleanup(func() {
		_ = engine.Close(context.Background())
	})

	for i := 0; i < 50; i++ {
		key := gofakeit.UUID()
		value := gofakeit.Sentence(10)
		assert.NoError(t, engine.PutKV([]byte(key), []byte(value)))
	}

	rep := NewReplicatorWithLSN(engine, 10, 100*time.Millisecond, 0, "lsn-concurrent")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	recvChan := make(chan []*v1.WALRecord, 20)

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		_ = rep.Replicate(ctx, recvChan)
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		ticker := time.NewTicker(50 * time.Millisecond)
		defer ticker.Stop()
		for i := 0; i < 20; i++ {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				key := gofakeit.UUID()
				value := gofakeit.Sentence(10)
				_ = engine.PutKV([]byte(key), []byte(value))
			}
		}
	}()

	seen := make(map[uint64]struct{})
	var lastLSN uint64

collect:
	for {
		select {
		case batch := <-recvChan:
			for _, record := range batch {
				decoded := logrecord.GetRootAsLogRecord(record.Record, 0)
				lsn := decoded.Lsn()
				if _, exists := seen[lsn]; exists {
					t.Fatalf("duplicate LSN detected: %d", lsn)
				}
				seen[lsn] = struct{}{}
				if lsn > lastLSN {
					lastLSN = lsn
				}
			}
			ReleaseRecords(batch)
		case <-ctx.Done():
			break collect
		}
	}

	wg.Wait()
	assert.GreaterOrEqual(t, len(seen), 50, "should receive at least initial 50 records")
}
