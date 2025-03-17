package replicator

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/ankur-anand/unisondb/dbkernel"
	"github.com/ankur-anand/unisondb/schemas/proto/gen/go/unisondb/replicator/v1"
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
		assert.NoError(t, engine.Put([]byte(key), []byte(value)), "put should not fail")
	}

	replicatorInstance := NewReplicator(engine,
		10, 1*time.Second, nil, "testing")

	wg := &sync.WaitGroup{}
	wg.Add(1)
	ticker := time.NewTicker(5 * time.Millisecond)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
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
				assert.NoError(t, engine.Put([]byte(key), []byte(value)), "put should not fail")
			}
		}
	}()

	recvChan := make(chan []*v1.WALRecord)
	wg.Add(1)
	go func() {
		defer wg.Done()
		err := replicatorInstance.Replicate(ctx, recvChan)
		assert.ErrorIs(t, err, context.DeadlineExceeded, "ctx deadline should be the only error")
	}()

	var recvRecords []*v1.WALRecord

outer:
	for {
		select {
		case recs := <-recvChan:
			recvRecords = append(recvRecords, recs...)
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

func TestReplicator_ReplicateReaderTimer(t *testing.T) {
	baseDir := t.TempDir()
	namespace := "test_timer"

	engine, err := dbkernel.NewStorageEngine(baseDir, namespace, dbkernel.NewDefaultEngineConfig())
	assert.NoError(t, err)

	t.Cleanup(func() {
		err := engine.Close(context.Background())
		assert.NoError(t, err, "Failed to close engine")
	})

	for i := 0; i < 10000; i++ {
		key := gofakeit.UUID()
		value := gofakeit.Sentence(100)
		assert.NoError(t, engine.Put([]byte(key), []byte(value)), "put should not fail")
	}

	batchDuration := 2 * time.Millisecond
	replicatorInstance := NewReplicator(engine, 20000,
		batchDuration, nil, "testing")

	recvChan := make(chan []*v1.WALRecord, 1)

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()

	wg := &sync.WaitGroup{}
	wg.Add(1)

	go func() {
		defer wg.Done()
		_ = replicatorInstance.replicateFromReader(ctx, recvChan)
	}()

	for i := 0; i < 2; i++ {
		select {
		case records := <-recvChan:
			assert.Less(t, len(records), 10000, "replicator batch size should be less")
		case <-time.After(20 * time.Millisecond):
			t.Errorf("failed waiting for replicator batch")
		}
	}
	cancel()
	wg.Wait()
}
