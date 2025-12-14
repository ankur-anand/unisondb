package dbkernel

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/ankur-anand/unisondb/internal/logcodec"
	"github.com/ankur-anand/unisondb/pkg/raftwalfs"
	"github.com/ankur-anand/unisondb/pkg/walfs"
	"github.com/ankur-anand/unisondb/schemas/logrecord"
	"github.com/hashicorp/raft"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type raftApplierWrapper struct {
	raft    *raft.Raft
	timeout time.Duration
}

func (r *raftApplierWrapper) Apply(data []byte) (uint64, error) {
	future := r.raft.Apply(data, r.timeout)
	if err := future.Error(); err != nil {
		return 0, err
	}
	if resp := future.Response(); resp != nil {
		if err, ok := resp.(error); ok {
			return 0, err
		}
	}
	return future.Index(), nil
}

func setupSingleNodeRaft(t *testing.T, dir string, fsm raft.FSM) (*raft.Raft, *raftwalfs.LogStore, func()) {
	t.Helper()

	raftWALDir := filepath.Join(dir, "raft-wal")

	wal, err := walfs.NewWALog(raftWALDir, ".wal",
		walfs.WithMaxSegmentSize(16*1024*1024),
		walfs.WithClearIndexOnFlush(),
	)
	require.NoError(t, err)

	logStore, err := raftwalfs.NewLogStore(wal, 0)
	require.NoError(t, err)

	stableStore := raft.NewInmemStore()
	snapshotStore := raft.NewInmemSnapshotStore()

	addr := raft.ServerAddress("127.0.0.1:0")
	_, transport := raft.NewInmemTransport(addr)

	config := raft.DefaultConfig()
	config.LocalID = "node1"
	config.HeartbeatTimeout = 50 * time.Millisecond
	config.ElectionTimeout = 50 * time.Millisecond
	config.LeaderLeaseTimeout = 50 * time.Millisecond
	config.CommitTimeout = 5 * time.Millisecond
	config.LogOutput = io.Discard

	r, err := raft.NewRaft(config, fsm, logStore, stableStore, snapshotStore, transport)
	require.NoError(t, err)

	configuration := raft.Configuration{
		Servers: []raft.Server{
			{
				ID:      config.LocalID,
				Address: transport.LocalAddr(),
			},
		},
	}
	err = r.BootstrapCluster(configuration).Error()
	require.NoError(t, err)

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if r.State() == raft.Leader {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	require.Equal(t, raft.Leader, r.State(), "node should be leader")

	cleanup := func() {
		future := r.Shutdown()
		_ = future.Error()
		_ = transport.Close()
		_ = logStore.Close()
		_ = wal.Close()
	}

	return r, logStore, cleanup
}

func TestEngine_RaftMode_KVOperations(t *testing.T) {
	dir := t.TempDir()
	namespace := "raft_kv_test"

	config := NewDefaultEngineConfig()
	config.ArenaSize = 1 << 20
	engine, err := NewStorageEngine(dir, namespace, config)
	require.NoError(t, err)
	t.Cleanup(func() {
		err := engine.close(context.Background())
		assert.NoError(t, err)
	})

	r, logStore, cleanupRaft := setupSingleNodeRaft(t, dir, engine)
	t.Cleanup(cleanupRaft)

	applier := &raftApplierWrapper{raft: r, timeout: 5 * time.Second}
	engine.SetRaftMode(true)
	engine.SetRaftApplier(applier)
	engine.SetPositionLookup(logStore.GetPosition)

	testKey := []byte("test-key-1")
	testValue := []byte("test-value-1")

	err = engine.PutKV(testKey, testValue)
	require.NoError(t, err)

	gotValue, err := engine.GetKV(testKey)
	require.NoError(t, err)
	assert.Equal(t, testValue, gotValue)

	assert.Greater(t, engine.AppliedIndex(), uint64(0), "applied index should be updated")

	for i := 0; i < 10; i++ {
		key := []byte("batch-key-" + string(rune('a'+i)))
		value := []byte("batch-value-" + string(rune('a'+i)))
		err = engine.PutKV(key, value)
		require.NoError(t, err)
	}

	for i := 0; i < 10; i++ {
		key := []byte("batch-key-" + string(rune('a'+i)))
		expectedValue := []byte("batch-value-" + string(rune('a'+i)))
		gotValue, err := engine.GetKV(key)
		require.NoError(t, err)
		assert.Equal(t, expectedValue, gotValue)
	}

	t.Logf("Applied index: %d, Applied term: %d", engine.AppliedIndex(), engine.AppliedTerm())
}

func TestEngine_RaftMode_RowOperations(t *testing.T) {
	dir := t.TempDir()
	namespace := "raft_row_test"

	config := NewDefaultEngineConfig()
	config.ArenaSize = 1 << 20
	engine, err := NewStorageEngine(dir, namespace, config)
	require.NoError(t, err)
	t.Cleanup(func() {
		err := engine.close(context.Background())
		assert.NoError(t, err)
	})

	r, logStore, cleanupRaft := setupSingleNodeRaft(t, dir, engine)
	t.Cleanup(cleanupRaft)

	applier := &raftApplierWrapper{raft: r, timeout: 5 * time.Second}
	engine.SetRaftMode(true)
	engine.SetRaftApplier(applier)
	engine.SetPositionLookup(logStore.GetPosition)

	rowKey := []byte("user:1001")
	columns := map[string][]byte{
		"name":  []byte("Alice"),
		"email": []byte("alice@example.com"),
		"age":   []byte("30"),
	}

	err = engine.PutColumnsForRow(rowKey, columns)
	require.NoError(t, err)

	gotColumns, err := engine.GetRowColumns(string(rowKey), func(columnKey string) bool { return true })
	require.NoError(t, err)
	assert.Equal(t, columns["name"], gotColumns["name"])
	assert.Equal(t, columns["email"], gotColumns["email"])
	assert.Equal(t, columns["age"], gotColumns["age"])

	updatedColumns := map[string][]byte{
		"age":  []byte("31"),
		"city": []byte("NYC"),
	}
	err = engine.PutColumnsForRow(rowKey, updatedColumns)
	require.NoError(t, err)

	gotColumns, err = engine.GetRowColumns(string(rowKey), func(columnKey string) bool { return true })
	require.NoError(t, err)
	assert.Equal(t, []byte("Alice"), gotColumns["name"])
	assert.Equal(t, []byte("31"), gotColumns["age"])
	assert.Equal(t, []byte("NYC"), gotColumns["city"])

	t.Logf("Applied index: %d, Applied term: %d", engine.AppliedIndex(), engine.AppliedTerm())
}

func TestEngine_RaftMode_EventOperations_NotSupported(t *testing.T) {
	dir := t.TempDir()
	namespace := "raft_event_test"

	config := NewDefaultEngineConfig()
	config.ArenaSize = 1 << 20
	config.EventLogMode = true
	engine, err := NewStorageEngine(dir, namespace, config)
	require.NoError(t, err)
	t.Cleanup(func() {
		err := engine.close(context.Background())
		assert.NoError(t, err)
	})

	r, logStore, cleanupRaft := setupSingleNodeRaft(t, dir, engine)
	t.Cleanup(cleanupRaft)

	applier := &raftApplierWrapper{raft: r, timeout: 5 * time.Second}
	engine.SetRaftMode(true)
	engine.SetRaftApplier(applier)
	engine.SetPositionLookup(logStore.GetPosition)

	event := &logcodec.EventEntry{
		EventID:    "event-1",
		EventType:  "user_created",
		OccurredAt: uint64(time.Now().UnixNano()),
		Payload:    []byte(`{"user_id":"123"}`),
	}

	err = engine.AddEvent(event)
	require.ErrorIs(t, err, ErrNotSupportedInRaftMode, "events should not be supported in raft mode")
}

func TestEngine_RaftMode_DeleteOperations(t *testing.T) {
	dir := t.TempDir()
	namespace := "raft_delete_test"

	config := NewDefaultEngineConfig()
	config.ArenaSize = 1 << 20
	engine, err := NewStorageEngine(dir, namespace, config)
	require.NoError(t, err)
	t.Cleanup(func() {
		err := engine.close(context.Background())
		assert.NoError(t, err)
	})

	r, logStore, cleanupRaft := setupSingleNodeRaft(t, dir, engine)
	t.Cleanup(cleanupRaft)

	applier := &raftApplierWrapper{raft: r, timeout: 5 * time.Second}
	engine.SetRaftMode(true)
	engine.SetRaftApplier(applier)
	engine.SetPositionLookup(logStore.GetPosition)

	testKey := []byte("delete-me")
	testValue := []byte("some-value")

	err = engine.PutKV(testKey, testValue)
	require.NoError(t, err)

	gotValue, err := engine.GetKV(testKey)
	require.NoError(t, err)
	assert.Equal(t, testValue, gotValue)

	err = engine.DeleteKV(testKey)
	require.NoError(t, err)

	_, err = engine.GetKV(testKey)
	assert.Error(t, err, "key should be deleted")

	t.Logf("Applied index: %d, Applied term: %d", engine.AppliedIndex(), engine.AppliedTerm())
}

func TestEngine_RaftMode_SnapshotRestore(t *testing.T) {
	dir := t.TempDir()
	namespace := "raft_snapshot_test"

	config := NewDefaultEngineConfig()
	config.ArenaSize = 1 << 20
	engine, err := NewStorageEngine(dir, namespace, config)
	require.NoError(t, err)
	t.Cleanup(func() {
		err := engine.close(context.Background())
		assert.NoError(t, err)
	})

	r, logStore, cleanupRaft := setupSingleNodeRaft(t, dir, engine)
	t.Cleanup(cleanupRaft)

	applier := &raftApplierWrapper{raft: r, timeout: 5 * time.Second}
	engine.SetRaftMode(true)
	engine.SetRaftApplier(applier)
	engine.SetPositionLookup(logStore.GetPosition)

	for i := 0; i < 50; i++ {
		key := fmt.Sprintf("snapshot-key-%d", i)
		value := fmt.Sprintf("snapshot-value-%d", i)
		err = engine.PutKV([]byte(key), []byte(value))
		require.NoError(t, err)
	}

	t.Logf("Before snapshot - Applied: %d/%d, Flushed: %d/%d",
		engine.AppliedIndex(), engine.AppliedTerm(),
		engine.FlushedIndex(), engine.FlushedTerm())

	require.Greater(t, engine.AppliedIndex(), uint64(0), "appliedIndex should be > 0 after writes")

	snapshot, err := engine.Snapshot()
	require.NoError(t, err)

	var snapshotBuf bytes.Buffer
	mockSink := &mockSnapshotSink{buf: &snapshotBuf}
	err = snapshot.Persist(mockSink)
	require.NoError(t, err)

	flushedIndexBefore := engine.FlushedIndex()
	flushedTermBefore := engine.FlushedTerm()

	dir2 := t.TempDir()
	engine2, err := NewStorageEngine(dir2, namespace, config)
	require.NoError(t, err)
	t.Cleanup(func() {
		err := engine2.close(context.Background())
		assert.NoError(t, err)
	})
	engine2.SetRaftMode(true)

	err = engine2.Restore(io.NopCloser(bytes.NewReader(snapshotBuf.Bytes())))
	require.NoError(t, err)

	assert.Equal(t, flushedIndexBefore, engine2.AppliedIndex())
	assert.Equal(t, flushedTermBefore, engine2.AppliedTerm())
	assert.Equal(t, flushedIndexBefore, engine2.FlushedIndex())
	assert.Equal(t, flushedTermBefore, engine2.FlushedTerm())

	t.Logf("After restore - Applied: %d/%d, Flushed: %d/%d",
		engine2.AppliedIndex(), engine2.AppliedTerm(),
		engine2.FlushedIndex(), engine2.FlushedTerm())
}

type mockSnapshotSink struct {
	buf      *bytes.Buffer
	canceled bool
}

func (m *mockSnapshotSink) Write(p []byte) (n int, err error) {
	return m.buf.Write(p)
}

func (m *mockSnapshotSink) Close() error {
	return nil
}

func (m *mockSnapshotSink) ID() string {
	return "mock-snapshot-id"
}

func (m *mockSnapshotSink) Cancel() error {
	m.canceled = true
	return nil
}

func TestEngine_RaftMode_ConcurrentOperations(t *testing.T) {
	dir := t.TempDir()
	namespace := "raft_concurrent_test"

	config := NewDefaultEngineConfig()
	config.ArenaSize = 1 << 20
	engine, err := NewStorageEngine(dir, namespace, config)
	require.NoError(t, err)
	t.Cleanup(func() {
		err := engine.close(context.Background())
		assert.NoError(t, err)
	})

	r, logStore, cleanupRaft := setupSingleNodeRaft(t, dir, engine)
	t.Cleanup(cleanupRaft)

	applier := &raftApplierWrapper{raft: r, timeout: 10 * time.Second}
	engine.SetRaftMode(true)
	engine.SetRaftApplier(applier)
	engine.SetPositionLookup(logStore.GetPosition)

	numWorkers := 5
	numOpsPerWorker := 20
	var wg sync.WaitGroup

	errChan := make(chan error, numWorkers*numOpsPerWorker)

	for w := 0; w < numWorkers; w++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			for i := 0; i < numOpsPerWorker; i++ {
				key := fmt.Sprintf("concurrent-key-%d-%d", workerID, i)
				value := fmt.Sprintf("concurrent-value-%d-%d", workerID, i)
				if err := engine.PutKV([]byte(key), []byte(value)); err != nil {
					errChan <- fmt.Errorf("worker %d op %d: %w", workerID, i, err)
					return
				}
			}
		}(w)
	}

	wg.Wait()
	close(errChan)

	var errors []error
	for err := range errChan {
		errors = append(errors, err)
	}
	require.Empty(t, errors, "should have no errors: %v", errors)

	for w := 0; w < numWorkers; w++ {
		for i := 0; i < numOpsPerWorker; i++ {
			key := fmt.Sprintf("concurrent-key-%d-%d", w, i)
			expectedValue := fmt.Sprintf("concurrent-value-%d-%d", w, i)
			gotValue, err := engine.GetKV([]byte(key))
			require.NoError(t, err, "key %s should exist", key)
			assert.Equal(t, []byte(expectedValue), gotValue)
		}
	}

	t.Logf("Applied index: %d, Applied term: %d", engine.AppliedIndex(), engine.AppliedTerm())
	t.Logf("Total operations: %d", numWorkers*numOpsPerWorker)
}

func TestEngine_RaftMode_LargeValues(t *testing.T) {
	dir := t.TempDir()
	namespace := "raft_large_values_test"

	config := NewDefaultEngineConfig()
	config.ArenaSize = 4 << 20
	engine, err := NewStorageEngine(dir, namespace, config)
	require.NoError(t, err)
	t.Cleanup(func() {
		err := engine.close(context.Background())
		assert.NoError(t, err)
	})

	r, logStore, cleanupRaft := setupSingleNodeRaft(t, dir, engine)
	t.Cleanup(cleanupRaft)

	applier := &raftApplierWrapper{raft: r, timeout: 10 * time.Second}
	engine.SetRaftMode(true)
	engine.SetRaftApplier(applier)
	engine.SetPositionLookup(logStore.GetPosition)

	sizes := []int{1024, 10 * 1024, 100 * 1024}

	for _, size := range sizes {
		key := fmt.Sprintf("large-key-%d", size)
		value := bytes.Repeat([]byte("x"), size)

		err = engine.PutKV([]byte(key), value)
		require.NoError(t, err, "should write %d byte value", size)

		gotValue, err := engine.GetKV([]byte(key))
		require.NoError(t, err)
		assert.Equal(t, value, gotValue, "value size %d should match", size)

		t.Logf("Successfully wrote and read %d byte value", size)
	}

	t.Logf("Applied index: %d, Applied term: %d", engine.AppliedIndex(), engine.AppliedTerm())
}

func TestEngine_RaftMode_MultipleRows(t *testing.T) {
	dir := t.TempDir()
	namespace := "raft_multi_row_test"

	config := NewDefaultEngineConfig()
	config.ArenaSize = 1 << 20
	engine, err := NewStorageEngine(dir, namespace, config)
	require.NoError(t, err)
	t.Cleanup(func() {
		err := engine.close(context.Background())
		assert.NoError(t, err)
	})

	r, logStore, cleanupRaft := setupSingleNodeRaft(t, dir, engine)
	t.Cleanup(cleanupRaft)

	applier := &raftApplierWrapper{raft: r, timeout: 5 * time.Second}
	engine.SetRaftMode(true)
	engine.SetRaftApplier(applier)
	engine.SetPositionLookup(logStore.GetPosition)

	numRows := 20
	for i := 0; i < numRows; i++ {
		rowKey := []byte(fmt.Sprintf("user:%d", i))
		columns := map[string][]byte{
			"name":  []byte(fmt.Sprintf("User%d", i)),
			"email": []byte(fmt.Sprintf("user%d@example.com", i)),
			"score": []byte(fmt.Sprintf("%d", i*100)),
		}
		err = engine.PutColumnsForRow(rowKey, columns)
		require.NoError(t, err)
	}

	for i := 0; i < numRows; i++ {
		rowKey := fmt.Sprintf("user:%d", i)
		gotColumns, err := engine.GetRowColumns(rowKey, func(columnKey string) bool { return true })
		require.NoError(t, err)
		assert.Equal(t, []byte(fmt.Sprintf("User%d", i)), gotColumns["name"])
		assert.Equal(t, []byte(fmt.Sprintf("user%d@example.com", i)), gotColumns["email"])
		assert.Equal(t, []byte(fmt.Sprintf("%d", i*100)), gotColumns["score"])
	}

	t.Logf("Applied index: %d, Applied term: %d", engine.AppliedIndex(), engine.AppliedTerm())
	t.Logf("Total rows: %d", numRows)
}

func TestEngine_RaftMode_MixedOperations(t *testing.T) {
	dir := t.TempDir()
	namespace := "raft_mixed_ops_test"

	config := NewDefaultEngineConfig()
	config.ArenaSize = 1 << 20
	engine, err := NewStorageEngine(dir, namespace, config)
	require.NoError(t, err)
	t.Cleanup(func() {
		err := engine.close(context.Background())
		assert.NoError(t, err)
	})

	r, logStore, cleanupRaft := setupSingleNodeRaft(t, dir, engine)
	t.Cleanup(cleanupRaft)

	applier := &raftApplierWrapper{raft: r, timeout: 5 * time.Second}
	engine.SetRaftMode(true)
	engine.SetRaftApplier(applier)
	engine.SetPositionLookup(logStore.GetPosition)

	for i := 0; i < 10; i++ {
		kvKey := fmt.Sprintf("kv-key-%d", i)
		kvValue := fmt.Sprintf("kv-value-%d", i)
		err = engine.PutKV([]byte(kvKey), []byte(kvValue))
		require.NoError(t, err)

		rowKey := []byte(fmt.Sprintf("row:%d", i))
		columns := map[string][]byte{
			"col1": []byte(fmt.Sprintf("val1-%d", i)),
			"col2": []byte(fmt.Sprintf("val2-%d", i)),
		}
		err = engine.PutColumnsForRow(rowKey, columns)
		require.NoError(t, err)
	}

	for i := 0; i < 10; i++ {
		kvKey := fmt.Sprintf("kv-key-%d", i)
		expectedValue := fmt.Sprintf("kv-value-%d", i)
		gotValue, err := engine.GetKV([]byte(kvKey))
		require.NoError(t, err)
		assert.Equal(t, []byte(expectedValue), gotValue)
	}

	for i := 0; i < 10; i++ {
		rowKey := fmt.Sprintf("row:%d", i)
		gotColumns, err := engine.GetRowColumns(rowKey, func(columnKey string) bool { return true })
		require.NoError(t, err)
		assert.Equal(t, []byte(fmt.Sprintf("val1-%d", i)), gotColumns["col1"])
		assert.Equal(t, []byte(fmt.Sprintf("val2-%d", i)), gotColumns["col2"])
	}

	t.Logf("Applied index: %d, Applied term: %d", engine.AppliedIndex(), engine.AppliedTerm())
}

func TestEngine_RaftMode_AppliedIndexMonotonicity(t *testing.T) {
	dir := t.TempDir()
	namespace := "raft_monotonic_test"

	config := NewDefaultEngineConfig()
	config.ArenaSize = 1 << 20
	engine, err := NewStorageEngine(dir, namespace, config)
	require.NoError(t, err)
	t.Cleanup(func() {
		err := engine.close(context.Background())
		assert.NoError(t, err)
	})

	r, logStore, cleanupRaft := setupSingleNodeRaft(t, dir, engine)
	t.Cleanup(cleanupRaft)

	applier := &raftApplierWrapper{raft: r, timeout: 5 * time.Second}
	engine.SetRaftMode(true)
	engine.SetRaftApplier(applier)
	engine.SetPositionLookup(logStore.GetPosition)

	var previousIndex uint64

	for i := 0; i < 50; i++ {
		err = engine.PutKV([]byte(fmt.Sprintf("key-%d", i)), []byte(fmt.Sprintf("value-%d", i)))
		require.NoError(t, err)

		currentIndex := engine.AppliedIndex()
		assert.Greater(t, currentIndex, previousIndex, "applied index should be monotonically increasing")
		previousIndex = currentIndex
	}

	t.Logf("Final applied index: %d", engine.AppliedIndex())
}

func TestEngine_RaftMode_IdempotentApply(t *testing.T) {
	dir := t.TempDir()
	namespace := "raft_idempotent_test"

	config := NewDefaultEngineConfig()
	config.ArenaSize = 1 << 20
	engine, err := NewStorageEngine(dir, namespace, config)
	require.NoError(t, err)
	t.Cleanup(func() {
		err := engine.close(context.Background())
		assert.NoError(t, err)
	})

	r, logStore, cleanupRaft := setupSingleNodeRaft(t, dir, engine)
	t.Cleanup(cleanupRaft)

	applier := &raftApplierWrapper{raft: r, timeout: 5 * time.Second}
	engine.SetRaftMode(true)
	engine.SetRaftApplier(applier)
	engine.SetPositionLookup(logStore.GetPosition)

	for i := 0; i < 20; i++ {
		key := fmt.Sprintf("idempotent-key-%d", i)
		value := fmt.Sprintf("idempotent-value-%d", i)
		err = engine.PutKV([]byte(key), []byte(value))
		require.NoError(t, err)
	}

	appliedBefore := engine.AppliedIndex()
	t.Logf("Applied index before flush: %d, Flushed index: %d", appliedBefore, engine.FlushedIndex())

	fsyncDone := make(chan struct{}, 1)
	engine.setFsyncCallback(func() {
		select {
		case fsyncDone <- struct{}{}:
		default:
		}
	})

	engine.mu.Lock()
	engine.rotateMemTable()
	engine.mu.Unlock()

	select {
	case <-fsyncDone:
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for fsync memtableRotateCallback")
	}

	flushedIndex := engine.FlushedIndex()
	require.Greater(t, flushedIndex, uint64(0), "flushedIndex should be > 0 after flush")
	t.Logf("Flushed index after flush: %d", flushedIndex)

	kvEntry := logcodec.SerializeKVEntry([]byte("replay-key"), []byte("replay-value"))
	record := logcodec.LogRecord{
		OperationType: logrecord.LogOperationTypeInsert,
		TxnState:      logrecord.TransactionStateNone,
		EntryType:     logrecord.LogEntryTypeKV,
		Entries:       [][]byte{kvEntry},
	}
	logData := record.FBEncode(256)

	replayLog := &raft.Log{
		Index: flushedIndex,
		Term:  2,
		Type:  raft.LogCommand,
		Data:  logData,
	}

	result := engine.Apply(replayLog)
	assert.Nil(t, result, "Apply() should return nil for already-flushed logs")

	assert.Equal(t, flushedIndex, engine.AppliedIndex(),
		"appliedIndex should be updated even for skipped logs")

	for i := 0; i < 20; i++ {
		key := fmt.Sprintf("idempotent-key-%d", i)
		expectedValue := fmt.Sprintf("idempotent-value-%d", i)
		gotValue, err := engine.GetKV([]byte(key))
		require.NoError(t, err, "key %s should still exist", key)
		assert.Equal(t, []byte(expectedValue), gotValue,
			"value for %s should not be corrupted by replay", key)
	}

	_, err = engine.GetKV([]byte("replay-key"))
	assert.Error(t, err, "replay-key should not exist since Apply was skipped")

	err = engine.PutKV([]byte("new-key-after-flush"), []byte("new-value-after-flush"))
	require.NoError(t, err)

	newAppliedIndex := engine.AppliedIndex()
	assert.Greater(t, newAppliedIndex, flushedIndex,
		"appliedIndex should advance for new logs")

	gotValue, err := engine.GetKV([]byte("new-key-after-flush"))
	require.NoError(t, err)
	assert.Equal(t, []byte("new-value-after-flush"), gotValue)

	t.Logf("Idempotent Apply test passed. Final applied: %d, flushed: %d",
		engine.AppliedIndex(), engine.FlushedIndex())
}
