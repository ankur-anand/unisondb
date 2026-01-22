package dbkernel

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/ankur-anand/unisondb/internal/keycodec"
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

func setupSingleNodeRaftWithEngineWAL(t *testing.T, engine *Engine) (*raft.Raft, *raftwalfs.LogStore, func()) {
	t.Helper()

	engineWAL := engine.WAL()
	require.NotNil(t, engineWAL, "engine WAL should not be nil")

	logStore, err := raftwalfs.NewLogStore(engineWAL, 0)
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

	r, err := raft.NewRaft(config, engine, logStore, stableStore, snapshotStore, transport)
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
	}

	return r, logStore, cleanup
}

func setupSingleNodeRaftWithEngineWALCodec(t *testing.T, engine *Engine, codec raftwalfs.Codec) (*raft.Raft, *raftwalfs.LogStore, func()) {
	t.Helper()

	engineWAL := engine.WAL()
	require.NotNil(t, engineWAL, "engine WAL should not be nil")

	var (
		logStore *raftwalfs.LogStore
		err      error
	)
	if codec == nil {
		logStore, err = raftwalfs.NewLogStore(engineWAL, 0)
	} else {
		logStore, err = raftwalfs.NewLogStore(engineWAL, 0, raftwalfs.WithCodec(codec))
	}
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

	r, err := raft.NewRaft(config, engine, logStore, stableStore, snapshotStore, transport)
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
	}

	return r, logStore, cleanup
}

func setupRaftApplyTestEngine(t *testing.T) *Engine {
	t.Helper()

	dir := t.TempDir()
	namespace := "raft_apply_test"

	config := NewDefaultEngineConfig()
	config.WalConfig.RaftMode = true

	engine, err := NewStorageEngine(dir, namespace, config)
	require.NoError(t, err)
	t.Cleanup(func() {
		err := engine.close(context.Background())
		assert.NoError(t, err)
	})

	engine.SetRaftMode(true)
	return engine
}

func encodeLogRecord(record logcodec.LogRecord) []byte {
	hintSize := 128
	for _, entry := range record.Entries {
		hintSize += len(entry)
	}
	return record.FBEncode(hintSize)
}

func makeRaftLog(index uint64, record logcodec.LogRecord) *raft.Log {
	return &raft.Log{
		Index: index,
		Term:  1,
		Type:  raft.LogCommand,
		Data:  encodeLogRecord(record),
	}
}

func appendRaftLogRecord(t *testing.T, engine *Engine, index uint64, record logcodec.LogRecord) walfs.RecordPosition {
	t.Helper()

	log := makeRaftLog(index, record)
	encoded, err := raftwalfs.BinaryCodecV1{}.Encode(log)
	require.NoError(t, err)

	offset, err := engine.walIO.Append(encoded, index)
	require.NoError(t, err)

	return *offset
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
	}
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
}

func TestEngine_RaftMode_WALCommitCallback(t *testing.T) {
	dir := t.TempDir()
	namespace := "raft_commit_callback_test"

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

	var mu sync.Mutex
	var callbackPositions []walfs.RecordPosition
	engine.SetWALCommitCallback(func(pos walfs.RecordPosition) {
		mu.Lock()
		callbackPositions = append(callbackPositions, pos)
		mu.Unlock()
	})

	numEntries := 5
	for i := 0; i < numEntries; i++ {
		key := []byte(fmt.Sprintf("key-%d", i))
		value := []byte(fmt.Sprintf("value-%d", i))
		err = engine.PutKV(key, value)
		require.NoError(t, err)
	}

	mu.Lock()
	callbackCount := len(callbackPositions)
	mu.Unlock()

	assert.Equal(t, numEntries, callbackCount,
		"callback should be invoked for each applied entry")

	mu.Lock()
	for i, pos := range callbackPositions {
		assert.Greater(t, pos.SegmentID, walfs.SegmentID(0),
			"position %d should have valid segment ID", i)
		assert.Greater(t, pos.Offset, int64(0),
			"position %d should have valid offset", i)
	}
	mu.Unlock()
}

func TestEngine_RaftMode_WALCommitCallback_NotCalledInNonRaftMode(t *testing.T) {
	dir := t.TempDir()
	namespace := "non_raft_callback_test"

	config := NewDefaultEngineConfig()
	config.ArenaSize = 1 << 20
	engine, err := NewStorageEngine(dir, namespace, config)
	require.NoError(t, err)
	t.Cleanup(func() {
		err := engine.close(context.Background())
		assert.NoError(t, err)
	})

	callbackCount := 0
	engine.SetWALCommitCallback(func(pos walfs.RecordPosition) {
		callbackCount++
	})

	assert.False(t, engine.IsRaftMode(), "engine should not be in Raft mode by default")

	for i := 0; i < 5; i++ {
		key := []byte(fmt.Sprintf("key-%d", i))
		value := []byte(fmt.Sprintf("value-%d", i))
		err = engine.PutKV(key, value)
		require.NoError(t, err)
	}

	assert.Equal(t, 0, callbackCount,
		"callback should NOT be invoked in non-Raft mode")
}

func TestEngine_RaftMode_WALCommitCallback_WithLogStoreCommitPosition(t *testing.T) {
	dir := t.TempDir()
	namespace := "raft_logstore_commit_test"

	config := NewDefaultEngineConfig()
	config.ArenaSize = 1 << 20
	engine, err := NewStorageEngine(dir, namespace, config)
	require.NoError(t, err)
	t.Cleanup(func() {
		err := engine.close(context.Background())
		assert.NoError(t, err)
	})

	raftWALDir := filepath.Join(dir, "raft-wal-commit")
	wal, err := walfs.NewWALog(raftWALDir, ".wal",
		walfs.WithMaxSegmentSize(16*1024*1024),
		walfs.WithClearIndexOnFlush(),
		walfs.WithReaderCommitCheck(),
	)
	require.NoError(t, err)

	logStore, err := raftwalfs.NewLogStore(wal, 0)
	require.NoError(t, err)

	stableStore := raft.NewInmemStore()
	snapshotStore := raft.NewInmemSnapshotStore()

	addr := raft.ServerAddress("127.0.0.1:0")
	_, transport := raft.NewInmemTransport(addr)

	raftConfig := raft.DefaultConfig()
	raftConfig.LocalID = "node1"
	raftConfig.HeartbeatTimeout = 50 * time.Millisecond
	raftConfig.ElectionTimeout = 50 * time.Millisecond
	raftConfig.LeaderLeaseTimeout = 50 * time.Millisecond
	raftConfig.CommitTimeout = 5 * time.Millisecond
	raftConfig.LogOutput = io.Discard

	r, err := raft.NewRaft(raftConfig, engine, logStore, stableStore, snapshotStore, transport)
	require.NoError(t, err)

	configuration := raft.Configuration{
		Servers: []raft.Server{
			{
				ID:      raftConfig.LocalID,
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

	t.Cleanup(func() {
		future := r.Shutdown()
		_ = future.Error()
		_ = transport.Close()
		_ = logStore.Close()
		_ = wal.Close()
	})

	applier := &raftApplierWrapper{raft: r, timeout: 5 * time.Second}
	engine.SetRaftMode(true)
	engine.SetRaftApplier(applier)
	engine.SetPositionLookup(logStore.GetPosition)
	engine.SetWALCommitCallback(logStore.CommitPosition)

	initialCommitted := wal.CommittedPosition()
	assert.Equal(t, walfs.NilRecordPosition, initialCommitted,
		"initially no committed position")

	numEntries := 5
	for i := 0; i < numEntries; i++ {
		key := []byte(fmt.Sprintf("key-%d", i))
		value := []byte(fmt.Sprintf("value-%d", i))
		err = engine.PutKV(key, value)
		require.NoError(t, err)
	}

	finalCommitted := wal.CommittedPosition()
	assert.NotEqual(t, walfs.NilRecordPosition, finalCommitted,
		"committed position should be set after writes")
	assert.Greater(t, finalCommitted.SegmentID, walfs.SegmentID(0),
		"committed segment ID should be valid")
	assert.Greater(t, finalCommitted.Offset, int64(0),
		"committed offset should be valid")
}

func TestEngine_RaftMode_ISRReaderRespectsCommitBoundary(t *testing.T) {
	dir := t.TempDir()
	namespace := "raft_isr_reader_test"

	config := NewDefaultEngineConfig()
	config.ArenaSize = 1 << 20
	engine, err := NewStorageEngine(dir, namespace, config)
	require.NoError(t, err)
	t.Cleanup(func() {
		err := engine.close(context.Background())
		assert.NoError(t, err)
	})

	raftWALDir := filepath.Join(dir, "raft-wal-isr")
	wal, err := walfs.NewWALog(raftWALDir, ".wal",
		walfs.WithMaxSegmentSize(16*1024*1024),
		walfs.WithClearIndexOnFlush(),
		walfs.WithReaderCommitCheck(),
	)
	require.NoError(t, err)

	logStore, err := raftwalfs.NewLogStore(wal, 0)
	require.NoError(t, err)

	stableStore := raft.NewInmemStore()
	snapshotStore := raft.NewInmemSnapshotStore()

	addr := raft.ServerAddress("127.0.0.1:0")
	_, transport := raft.NewInmemTransport(addr)

	raftConfig := raft.DefaultConfig()
	raftConfig.LocalID = "node1"
	raftConfig.HeartbeatTimeout = 50 * time.Millisecond
	raftConfig.ElectionTimeout = 50 * time.Millisecond
	raftConfig.LeaderLeaseTimeout = 50 * time.Millisecond
	raftConfig.CommitTimeout = 5 * time.Millisecond
	raftConfig.LogOutput = io.Discard

	r, err := raft.NewRaft(raftConfig, engine, logStore, stableStore, snapshotStore, transport)
	require.NoError(t, err)

	configuration := raft.Configuration{
		Servers: []raft.Server{
			{
				ID:      raftConfig.LocalID,
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

	t.Cleanup(func() {
		future := r.Shutdown()
		_ = future.Error()
		_ = transport.Close()
		_ = logStore.Close()
		_ = wal.Close()
	})

	applier := &raftApplierWrapper{raft: r, timeout: 5 * time.Second}
	engine.SetRaftMode(true)
	engine.SetRaftApplier(applier)
	engine.SetPositionLookup(logStore.GetPosition)
	engine.SetWALCommitCallback(logStore.CommitPosition)

	numEntries := 10
	for i := 0; i < numEntries; i++ {
		key := []byte(fmt.Sprintf("key-%d", i))
		value := []byte(fmt.Sprintf("value-%d", i))
		err = engine.PutKV(key, value)
		require.NoError(t, err)
	}

	reader := wal.NewReader()
	defer reader.Close()

	codec := raftwalfs.BinaryCodecV1{}
	readCount := 0
	commandCount := 0

	for {
		data, pos, err := reader.Next()
		if errors.Is(err, walfs.ErrNoNewData) {
			break
		}
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		assert.NotNil(t, data, "data should not be nil")
		assert.Greater(t, pos.SegmentID, walfs.SegmentID(0), "valid segment ID")

		raftLog, err := codec.Decode(data)
		require.NoError(t, err, "should decode raft log")

		if raftLog.Type == raft.LogCommand {
			record := logrecord.GetRootAsLogRecord(raftLog.Data, 0)
			assert.NotNil(t, record, "should be valid log record")
			_ = record.Lsn()
			commandCount++
		}

		readCount++
	}

	assert.GreaterOrEqual(t, readCount, numEntries,
		"should read at least %d entries (got %d)", numEntries, readCount)
	assert.Equal(t, numEntries, commandCount,
		"should have exactly %d command entries", numEntries)
}

func TestEngine_RaftMode_WALCommitCallback_MonotonicPositions(t *testing.T) {
	dir := t.TempDir()
	namespace := "raft_monotonic_pos_test"

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

	var mu sync.Mutex
	var positions []walfs.RecordPosition
	engine.SetWALCommitCallback(func(pos walfs.RecordPosition) {
		mu.Lock()
		positions = append(positions, pos)
		mu.Unlock()
	})

	numEntries := 20
	for i := 0; i < numEntries; i++ {
		key := []byte(fmt.Sprintf("key-%d", i))
		value := []byte(fmt.Sprintf("value-%d", i))
		err = engine.PutKV(key, value)
		require.NoError(t, err)
	}

	mu.Lock()
	defer mu.Unlock()

	for i := 1; i < len(positions); i++ {
		prev := positions[i-1]
		curr := positions[i]

		if curr.SegmentID == prev.SegmentID {
			assert.Greater(t, curr.Offset, prev.Offset,
				"offset should increase within same segment (pos %d)", i)
		} else {
			assert.Greater(t, curr.SegmentID, prev.SegmentID,
				"segment ID should increase (pos %d)", i)
		}
	}

}

func TestEngine_RaftMode_ReaderUsesDecoder(t *testing.T) {
	dir := t.TempDir()
	namespace := "raft_reader_decoder_test"

	config := NewDefaultEngineConfig()
	config.ArenaSize = 1 << 20
	config.WalConfig.RaftMode = true
	engine, err := NewStorageEngine(dir, namespace, config)
	require.NoError(t, err)
	t.Cleanup(func() {
		err := engine.close(context.Background())
		assert.NoError(t, err)
	})

	r, logStore, cleanupRaft := setupSingleNodeRaftWithEngineWAL(t, engine)
	t.Cleanup(cleanupRaft)

	applier := &raftApplierWrapper{raft: r, timeout: 5 * time.Second}
	engine.SetRaftMode(true)
	engine.SetRaftApplier(applier)
	engine.SetPositionLookup(logStore.GetPosition)
	engine.SetWALCommitCallback(logStore.CommitPosition)

	numEntries := 5
	for i := 0; i < numEntries; i++ {
		key := fmt.Appendf(nil, "key-%d", i)
		value := fmt.Appendf(nil, "value-%d", i)
		err = engine.PutKV(key, value)
		require.NoError(t, err)
	}

	reader, err := engine.NewReader()
	require.NoError(t, err)
	defer reader.Close()

	readCount := 0
	commandCount := 0
	raftInternalCount := 0

	for {
		data, pos, err := reader.Next()
		if errors.Is(err, io.EOF) || errors.Is(err, walfs.ErrNoNewData) {
			break
		}
		require.NoError(t, err)
		assert.NotNil(t, data)
		assert.Greater(t, pos.SegmentID, walfs.SegmentID(0))

		record := logrecord.GetRootAsLogRecord(data, 0)
		require.NotNil(t, record)

		opType := record.OperationType()
		switch opType {
		case logrecord.LogOperationTypeRaftInternal:
			raftInternalCount++
		case logrecord.LogOperationTypeInsert:
			commandCount++
		}

		readCount++
	}

	assert.GreaterOrEqual(t, readCount, numEntries)
	assert.Equal(t, numEntries, commandCount)
	assert.Greater(t, raftInternalCount, 0, "should have RaftInternal entries for Raft noop/config")
}

func TestEngine_ApplyBatch(t *testing.T) {
	t.Run("empty_batch_returns_empty_results", func(t *testing.T) {
		dir := t.TempDir()
		namespace := "apply_batch_empty"

		config := NewDefaultEngineConfig()
		engine, err := NewStorageEngine(dir, namespace, config)
		require.NoError(t, err)
		t.Cleanup(func() {
			_ = engine.close(context.Background())
		})

		engine.SetRaftMode(true)

		results := engine.ApplyBatch([]*raft.Log{})
		assert.Empty(t, results, "empty batch should return empty results")

		results = engine.ApplyBatch(nil)
		assert.Empty(t, results, "nil batch should return empty results")
	})

	t.Run("non_command_logs_return_nil", func(t *testing.T) {
		dir := t.TempDir()
		namespace := "apply_batch_non_command"

		config := NewDefaultEngineConfig()
		engine, err := NewStorageEngine(dir, namespace, config)
		require.NoError(t, err)
		t.Cleanup(func() {
			_ = engine.close(context.Background())
		})

		engine.SetRaftMode(true)

		logs := []*raft.Log{
			{
				Index: 1,
				Term:  1,
				Type:  raft.LogNoop,
				Data:  nil,
			},
			{
				Index: 2,
				Term:  1,
				Type:  raft.LogConfiguration,
				Data:  []byte("some config"),
			},
		}

		results := engine.ApplyBatch(logs)
		require.Len(t, results, 2)

		assert.Nil(t, results[0], "noop log should return nil")
		assert.Nil(t, results[1], "configuration log should return nil")
	})

	t.Run("empty_data_logs_return_nil", func(t *testing.T) {
		dir := t.TempDir()
		namespace := "apply_batch_empty_data"

		config := NewDefaultEngineConfig()
		engine, err := NewStorageEngine(dir, namespace, config)
		require.NoError(t, err)
		t.Cleanup(func() {
			_ = engine.close(context.Background())
		})

		engine.SetRaftMode(true)

		logs := []*raft.Log{
			{
				Index: 1,
				Term:  1,
				Type:  raft.LogCommand,
				Data:  nil,
			},
			{
				Index: 2,
				Term:  1,
				Type:  raft.LogCommand,
				Data:  []byte{},
			},
		}

		results := engine.ApplyBatch(logs)
		require.Len(t, results, 2)

		assert.Nil(t, results[0], "command with nil data should return nil")
		assert.Nil(t, results[1], "command with empty data should return nil")
	})

	t.Run("results_length_matches_input", func(t *testing.T) {
		dir := t.TempDir()
		namespace := "apply_batch_length"

		config := NewDefaultEngineConfig()
		engine, err := NewStorageEngine(dir, namespace, config)
		require.NoError(t, err)
		t.Cleanup(func() {
			_ = engine.close(context.Background())
		})

		engine.SetRaftMode(true)

		testCases := []int{1, 5, 10, 50, 100}

		for _, count := range testCases {
			logs := make([]*raft.Log, count)
			for i := 0; i < count; i++ {
				logs[i] = &raft.Log{
					Index: uint64(i + 1),
					Term:  1,
					Type:  raft.LogNoop,
				}
			}

			results := engine.ApplyBatch(logs)
			assert.Len(t, results, count, "results length should match input length for count=%d", count)
		}
	})

	t.Run("with_full_raft_integration", func(t *testing.T) {
		dir := t.TempDir()
		namespace := "apply_batch_full_raft"

		config := NewDefaultEngineConfig()
		config.ArenaSize = 1 << 20
		engine, err := NewStorageEngine(dir, namespace, config)
		require.NoError(t, err)
		t.Cleanup(func() {
			_ = engine.close(context.Background())
		})

		r, logStore, cleanupRaft := setupSingleNodeRaft(t, dir, engine)
		t.Cleanup(cleanupRaft)

		applier := &raftApplierWrapper{raft: r, timeout: 5 * time.Second}
		engine.SetRaftMode(true)
		engine.SetRaftApplier(applier)
		engine.SetPositionLookup(logStore.GetPosition)

		numEntries := 10
		for i := 0; i < numEntries; i++ {
			key := fmt.Sprintf("batch-key-%d", i)
			value := fmt.Sprintf("batch-value-%d", i)
			err = engine.PutKV([]byte(key), []byte(value))
			require.NoError(t, err)
		}

		for i := 0; i < numEntries; i++ {
			key := fmt.Sprintf("batch-key-%d", i)
			expectedValue := fmt.Sprintf("batch-value-%d", i)
			gotValue, err := engine.GetKV([]byte(key))
			require.NoError(t, err, "key %s should exist", key)
			assert.Equal(t, []byte(expectedValue), gotValue)
		}

		assert.Greater(t, engine.AppliedIndex(), uint64(0), "applied index should advance")
	})

	t.Run("batch_applies_logs_in_order", func(t *testing.T) {
		dir := t.TempDir()
		namespace := "apply_batch_order"

		config := NewDefaultEngineConfig()
		config.ArenaSize = 1 << 20
		engine, err := NewStorageEngine(dir, namespace, config)
		require.NoError(t, err)
		t.Cleanup(func() {
			_ = engine.close(context.Background())
		})

		r, logStore, cleanupRaft := setupSingleNodeRaft(t, dir, engine)
		t.Cleanup(cleanupRaft)

		applier := &raftApplierWrapper{raft: r, timeout: 5 * time.Second}
		engine.SetRaftMode(true)
		engine.SetRaftApplier(applier)
		engine.SetPositionLookup(logStore.GetPosition)

		err = engine.PutKV([]byte("order-test"), []byte("value1"))
		require.NoError(t, err)

		err = engine.PutKV([]byte("order-test"), []byte("value2"))
		require.NoError(t, err)

		err = engine.PutKV([]byte("order-test"), []byte("final-value"))
		require.NoError(t, err)

		gotValue, err := engine.GetKV([]byte("order-test"))
		require.NoError(t, err)
		assert.Equal(t, []byte("final-value"), gotValue, "should have final value after ordered applies")
	})

	t.Run("batch_with_delete_operations", func(t *testing.T) {
		dir := t.TempDir()
		namespace := "apply_batch_with_delete"

		config := NewDefaultEngineConfig()
		config.ArenaSize = 1 << 20
		engine, err := NewStorageEngine(dir, namespace, config)
		require.NoError(t, err)
		t.Cleanup(func() {
			_ = engine.close(context.Background())
		})

		r, logStore, cleanupRaft := setupSingleNodeRaft(t, dir, engine)
		t.Cleanup(cleanupRaft)

		applier := &raftApplierWrapper{raft: r, timeout: 5 * time.Second}
		engine.SetRaftMode(true)
		engine.SetRaftApplier(applier)
		engine.SetPositionLookup(logStore.GetPosition)

		err = engine.PutKV([]byte("key1"), []byte("value1"))
		require.NoError(t, err)
		err = engine.PutKV([]byte("key2"), []byte("value2"))
		require.NoError(t, err)

		err = engine.DeleteKV([]byte("key1"))
		require.NoError(t, err)

		_, err = engine.GetKV([]byte("key1"))
		assert.Error(t, err, "deleted key should not exist")

		gotValue, err := engine.GetKV([]byte("key2"))
		require.NoError(t, err)
		assert.Equal(t, []byte("value2"), gotValue)
	})

	t.Run("ops_count_increments_correctly", func(t *testing.T) {
		dir := t.TempDir()
		namespace := "apply_batch_ops"

		config := NewDefaultEngineConfig()
		config.ArenaSize = 1 << 20
		engine, err := NewStorageEngine(dir, namespace, config)
		require.NoError(t, err)
		t.Cleanup(func() {
			_ = engine.close(context.Background())
		})

		r, logStore, cleanupRaft := setupSingleNodeRaft(t, dir, engine)
		t.Cleanup(cleanupRaft)

		applier := &raftApplierWrapper{raft: r, timeout: 5 * time.Second}
		engine.SetRaftMode(true)
		engine.SetRaftApplier(applier)
		engine.SetPositionLookup(logStore.GetPosition)

		initialOps := engine.OpsReceivedCount()

		numOps := 5
		for i := 0; i < numOps; i++ {
			err = engine.PutKV([]byte(fmt.Sprintf("key-%d", i)), []byte(fmt.Sprintf("value-%d", i)))
			require.NoError(t, err)
		}

		finalOps := engine.OpsReceivedCount()
		assert.Equal(t, initialOps+uint64(numOps), finalOps,
			"ops count should increase by number of operations")
	})

	t.Run("applied_index_and_term_update", func(t *testing.T) {
		dir := t.TempDir()
		namespace := "apply_batch_index_term"

		config := NewDefaultEngineConfig()
		config.ArenaSize = 1 << 20
		engine, err := NewStorageEngine(dir, namespace, config)
		require.NoError(t, err)
		t.Cleanup(func() {
			_ = engine.close(context.Background())
		})

		r, logStore, cleanupRaft := setupSingleNodeRaft(t, dir, engine)
		t.Cleanup(cleanupRaft)

		applier := &raftApplierWrapper{raft: r, timeout: 5 * time.Second}
		engine.SetRaftMode(true)
		engine.SetRaftApplier(applier)
		engine.SetPositionLookup(logStore.GetPosition)

		for i := 0; i < 5; i++ {
			err = engine.PutKV([]byte(fmt.Sprintf("key-%d", i)), []byte(fmt.Sprintf("value-%d", i)))
			require.NoError(t, err)
		}

		assert.Greater(t, engine.AppliedIndex(), uint64(0), "applied index should be > 0")
		assert.Greater(t, engine.AppliedTerm(), uint64(0), "applied term should be > 0")
	})
}

func TestEngine_RaftMode_SkipsWALRecovery(t *testing.T) {
	dir := t.TempDir()
	namespace := "raft_skip_recovery_test"

	config := NewDefaultEngineConfig()
	config.ArenaSize = 1 << 20
	config.WalConfig.RaftMode = true

	engine, err := NewStorageEngine(dir, namespace, config)
	require.NoError(t, err)
	t.Cleanup(func() {
		err := engine.close(context.Background())
		assert.NoError(t, err)
	})

	assert.Equal(t, 0, engine.RecoveredWALCount(),
		"no entries should be recovered in Raft mode")

	engine.SetRaftMode(true)

	r, logStore, cleanupRaft := setupSingleNodeRaft(t, dir, engine)
	t.Cleanup(cleanupRaft)

	applier := &raftApplierWrapper{raft: r, timeout: 5 * time.Second}
	engine.SetRaftApplier(applier)
	engine.SetPositionLookup(logStore.GetPosition)

	err = engine.PutKV([]byte("test-key"), []byte("test-value"))
	require.NoError(t, err)

	gotValue, err := engine.GetKV([]byte("test-key"))
	require.NoError(t, err)
	assert.Equal(t, []byte("test-value"), gotValue)
}

func TestEngine_ApplyRaftTxnStateBeginPrepare_NoMemtableApply(t *testing.T) {
	engine := setupRaftApplyTestEngine(t)

	txnID := []byte("txn-begin-prepare")
	begin := logcodec.LogRecord{
		HLC:       HLCNow(),
		TxnID:     txnID,
		TxnState:  logrecord.TransactionStateBegin,
		EntryType: logrecord.LogEntryTypeKV,
	}
	result := engine.Apply(makeRaftLog(1, begin))
	require.Nil(t, result)
	assert.Equal(t, uint64(1), engine.AppliedIndex())

	prepareEntry := logcodec.SerializeKVEntry(keycodec.KeyKV([]byte("key1")), []byte("value1"))
	prepare := logcodec.LogRecord{
		HLC:           HLCNow(),
		TxnID:         txnID,
		OperationType: logrecord.LogOperationTypeInsert,
		TxnState:      logrecord.TransactionStatePrepare,
		EntryType:     logrecord.LogEntryTypeKV,
		Entries:       [][]byte{prepareEntry},
	}
	result = engine.Apply(makeRaftLog(2, prepare))
	require.Nil(t, result)
	assert.Equal(t, uint64(2), engine.AppliedIndex())

	_, err := engine.GetKV([]byte("key1"))
	assert.ErrorIs(t, err, ErrKeyNotFound)
}

func TestEngine_ApplyRaftTxnStateCommitKV_ReplaysChain(t *testing.T) {
	engine := setupRaftApplyTestEngine(t)

	txnID := []byte("txn-commit-kv")
	begin := logcodec.LogRecord{
		HLC:       HLCNow(),
		TxnID:     txnID,
		TxnState:  logrecord.TransactionStateBegin,
		EntryType: logrecord.LogEntryTypeKV,
	}
	beginOffset := appendRaftLogRecord(t, engine, 1, begin)

	prepareEntry := logcodec.SerializeKVEntry(keycodec.KeyKV([]byte("key1")), []byte("value1"))
	prepare := logcodec.LogRecord{
		HLC:             HLCNow(),
		TxnID:           txnID,
		OperationType:   logrecord.LogOperationTypeInsert,
		TxnState:        logrecord.TransactionStatePrepare,
		EntryType:       logrecord.LogEntryTypeKV,
		PrevTxnWalIndex: beginOffset.Encode(),
		Entries:         [][]byte{prepareEntry},
	}
	prepareOffset := appendRaftLogRecord(t, engine, 2, prepare)

	commit := logcodec.LogRecord{
		HLC:             HLCNow(),
		TxnID:           txnID,
		OperationType:   logrecord.LogOperationTypeInsert,
		TxnState:        logrecord.TransactionStateCommit,
		EntryType:       logrecord.LogEntryTypeKV,
		PrevTxnWalIndex: prepareOffset.Encode(),
	}

	result := engine.Apply(makeRaftLog(3, commit))
	require.Nil(t, result)

	val, err := engine.GetKV([]byte("key1"))
	require.NoError(t, err)
	assert.Equal(t, []byte("value1"), val)
}

func TestEngine_ApplyRaftTxnStateCommitRow_ReplaysChain(t *testing.T) {
	engine := setupRaftApplyTestEngine(t)

	txnID := []byte("txn-commit-row")
	begin := logcodec.LogRecord{
		HLC:       HLCNow(),
		TxnID:     txnID,
		TxnState:  logrecord.TransactionStateBegin,
		EntryType: logrecord.LogEntryTypeRow,
	}
	beginOffset := appendRaftLogRecord(t, engine, 1, begin)

	rowKey := keycodec.RowKey([]byte("row1"))
	rowEntry := logcodec.SerializeRowUpdateEntry(rowKey, map[string][]byte{
		"col1": []byte("val1"),
	})
	prepare := logcodec.LogRecord{
		HLC:             HLCNow(),
		TxnID:           txnID,
		OperationType:   logrecord.LogOperationTypeInsert,
		TxnState:        logrecord.TransactionStatePrepare,
		EntryType:       logrecord.LogEntryTypeRow,
		PrevTxnWalIndex: beginOffset.Encode(),
		Entries:         [][]byte{rowEntry},
	}
	prepareOffset := appendRaftLogRecord(t, engine, 2, prepare)

	commit := logcodec.LogRecord{
		HLC:             HLCNow(),
		TxnID:           txnID,
		OperationType:   logrecord.LogOperationTypeInsert,
		TxnState:        logrecord.TransactionStateCommit,
		EntryType:       logrecord.LogEntryTypeRow,
		PrevTxnWalIndex: prepareOffset.Encode(),
	}

	result := engine.Apply(makeRaftLog(3, commit))
	require.Nil(t, result)

	rowData, err := engine.GetRowColumns("row1", nil)
	require.NoError(t, err)
	assert.Equal(t, []byte("val1"), rowData["col1"])
}

func TestEngine_ApplyRaftTxnStateCommitChunked_UsesCommitOffset(t *testing.T) {
	engine := setupRaftApplyTestEngine(t)

	chunkKey := keycodec.KeyBlobChunk([]byte("blob"), 0)
	commitEntry := logcodec.SerializeKVEntry(chunkKey, nil)
	commit := logcodec.LogRecord{
		HLC:           HLCNow(),
		OperationType: logrecord.LogOperationTypeInsert,
		TxnState:      logrecord.TransactionStateCommit,
		EntryType:     logrecord.LogEntryTypeChunked,
		Entries:       [][]byte{commitEntry},
	}

	commitLog := makeRaftLog(1, commit)
	encoded, err := raftwalfs.BinaryCodecV1{}.Encode(commitLog)
	require.NoError(t, err)

	commitOffset, err := engine.walIO.Append(encoded, commitLog.Index)
	require.NoError(t, err)

	engine.SetPositionLookup(func(index uint64) (walfs.RecordPosition, bool) {
		if index == commitLog.Index {
			return *commitOffset, true
		}
		return walfs.RecordPosition{}, false
	})

	result := engine.Apply(commitLog)
	require.Nil(t, result)

	memValue := engine.activeMemTable.Get(chunkKey)
	assert.Equal(t, byte(logrecord.LogOperationTypeInsert), memValue.Meta)
	assert.Equal(t, byte(logrecord.LogEntryTypeChunked), memValue.UserMeta)
	assert.True(t, bytes.Equal(commitOffset.Encode(), memValue.Value))
}

func TestEngine_ApplyRaftTxnStateNone_AppliesRecord(t *testing.T) {
	engine := setupRaftApplyTestEngine(t)

	entry := logcodec.SerializeKVEntry(keycodec.KeyKV([]byte("key1")), []byte("value1"))
	record := logcodec.LogRecord{
		HLC:           HLCNow(),
		OperationType: logrecord.LogOperationTypeInsert,
		TxnState:      logrecord.TransactionStateNone,
		EntryType:     logrecord.LogEntryTypeKV,
		Entries:       [][]byte{entry},
	}

	result := engine.Apply(makeRaftLog(1, record))
	require.Nil(t, result)

	val, err := engine.GetKV([]byte("key1"))
	require.NoError(t, err)
	assert.Equal(t, []byte("value1"), val)
}

func TestEngine_ApplyRaftTxnStateUnknown_DoesNotApplyRecord(t *testing.T) {
	engine := setupRaftApplyTestEngine(t)

	entry := logcodec.SerializeKVEntry(keycodec.KeyKV([]byte("key1")), []byte("value1"))
	record := logcodec.LogRecord{
		HLC:           HLCNow(),
		OperationType: logrecord.LogOperationTypeInsert,
		TxnState:      logrecord.TransactionState(99),
		EntryType:     logrecord.LogEntryTypeKV,
		Entries:       [][]byte{entry},
	}

	result := engine.Apply(makeRaftLog(1, record))
	require.Nil(t, result)
	assert.Equal(t, uint64(1), engine.AppliedIndex())

	_, err := engine.GetKV([]byte("key1"))
	assert.ErrorIs(t, err, ErrKeyNotFound)
}

func setupRaftTxnReaderEngine(t *testing.T) (*Engine, func()) {
	t.Helper()

	dir := t.TempDir()
	namespace := "txn_reader_raft"

	conf := NewDefaultEngineConfig()
	conf.DBEngine = LMDBEngine
	conf.BtreeConfig.Namespace = namespace
	conf.WalConfig.RaftMode = true
	conf.WalConfig.AutoCleanup = false

	engine, err := NewStorageEngine(dir, namespace, conf)
	require.NoError(t, err)

	r, logStore, raftCleanup := setupSingleNodeRaftWithEngineWAL(t, engine)
	engine.SetPositionLookup(logStore.GetPosition)
	engine.SetRaftApplier(&raftApplierWrapper{
		raft:    r,
		timeout: 5 * time.Second,
	})
	engine.SetWALCommitCallback(logStore.CommitPosition)
	engine.SetRaftMode(true)

	cleanup := func() {
		raftCleanup()
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = engine.Close(ctx)
	}

	return engine, cleanup
}

func TestRaftTxnReaderReturnsDecodedChunkedCommit(t *testing.T) {
	engine, cleanup := setupRaftTxnReaderEngine(t)
	defer cleanup()

	txn, err := engine.NewTransaction(logrecord.LogOperationTypeInsert, logrecord.LogEntryTypeChunked)
	require.NoError(t, err)

	err = txn.AppendKVTxn([]byte("lob-key"), []byte("chunk1-"))
	require.NoError(t, err)
	err = txn.AppendKVTxn([]byte("lob-key"), []byte("chunk2"))
	require.NoError(t, err)

	err = txn.Commit()
	require.NoError(t, err)

	reader, err := engine.NewReader()
	require.NoError(t, err)
	defer reader.Close()

	expectedKey := keycodec.KeyBlobChunk([]byte("lob-key"), 0)
	found := false

	for {
		data, _, err := reader.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		require.NoError(t, err)

		record := logrecord.GetRootAsLogRecord(data, 0)
		if record.OperationType() == logrecord.LogOperationTypeRaftInternal {
			continue
		}
		if !bytes.Equal(record.TxnIdBytes(), txn.TxnID()) {
			continue
		}

		if record.TxnState() == logrecord.TransactionStateCommit && record.EntryType() == logrecord.LogEntryTypeChunked {
			logEntry := logcodec.DeserializeFBRootLogRecord(record)
			require.Len(t, logEntry.Entries, 1)
			kv := logcodec.DeserializeKVEntry(logEntry.Entries[0])
			require.Equal(t, expectedKey, kv.Key)
			found = true
			break
		}
	}

	require.True(t, found, "expected decoded chunked commit record from reader")
}

type nopCloser struct {
	io.Reader
}

func (nopCloser) Close() error { return nil }

func waitForCallbacks(t *testing.T, ch <-chan struct{}, count int) {
	t.Helper()
	for i := 0; i < count; i++ {
		select {
		case <-ch:
		case <-time.After(5 * time.Second):
			t.Fatalf("timed out waiting for callback %d", i+1)
		}
	}
}

func TestRestoreModeMismatch_RaftSnapshotToStandalone(t *testing.T) {
	dir := t.TempDir()
	namespace := "restore_mode_test"
	configRaft := NewDefaultEngineConfig()
	configRaft.ArenaSize = 1 << 20
	configRaft.WalConfig.RaftMode = true
	configRaft.DBEngine = BoltDBEngine

	engine, err := NewStorageEngine(dir, namespace, configRaft)
	require.NoError(t, err)

	var snapshotBuf bytes.Buffer
	_, err = engine.BtreeSnapshot(&snapshotBuf)
	require.NoError(t, err)

	err = engine.close(context.Background())
	require.NoError(t, err)

	standaloneDir := t.TempDir()
	configStandalone := NewDefaultEngineConfig()
	configStandalone.ArenaSize = 1 << 20
	configStandalone.WalConfig.RaftMode = false
	configStandalone.DBEngine = BoltDBEngine

	standaloneEngine, err := NewStorageEngine(standaloneDir, namespace, configStandalone)
	require.NoError(t, err)
	defer standaloneEngine.close(context.Background())

	err = standaloneEngine.Restore(nopCloser{&snapshotBuf})
	require.Error(t, err, "restoring Raft snapshot into standalone engine should fail")
	require.ErrorIs(t, err, ErrEngineModeMismatch, "error should be ErrEngineModeMismatch")
}

func TestRestoreModeMismatch_StandaloneSnapshotToRaft(t *testing.T) {
	dir := t.TempDir()
	namespace := "restore_mode_test"

	configStandalone := NewDefaultEngineConfig()
	configStandalone.ArenaSize = 1 << 20
	configStandalone.WalConfig.RaftMode = false
	configStandalone.DBEngine = BoltDBEngine

	engine, err := NewStorageEngine(dir, namespace, configStandalone)
	require.NoError(t, err)
	var snapshotBuf bytes.Buffer
	_, err = engine.BtreeSnapshot(&snapshotBuf)
	require.NoError(t, err)

	err = engine.close(context.Background())
	require.NoError(t, err)
	raftDir := t.TempDir()
	configRaft := NewDefaultEngineConfig()
	configRaft.ArenaSize = 1 << 20
	configRaft.WalConfig.RaftMode = true
	configRaft.DBEngine = BoltDBEngine

	raftEngine, err := NewStorageEngine(raftDir, namespace, configRaft)
	require.NoError(t, err)
	defer raftEngine.close(context.Background())

	err = raftEngine.Restore(nopCloser{&snapshotBuf})
	require.Error(t, err, "restoring standalone snapshot into Raft engine should fail")
	require.ErrorIs(t, err, ErrEngineModeMismatch, "error should be ErrEngineModeMismatch")
}

func TestRestoreSameMode_Success(t *testing.T) {
	t.Run("raft_to_raft", func(t *testing.T) {
		dir := t.TempDir()
		namespace := "restore_same_mode"

		config := NewDefaultEngineConfig()
		config.ArenaSize = 1 << 20
		config.WalConfig.RaftMode = true
		config.DBEngine = BoltDBEngine

		engine, err := NewStorageEngine(dir, namespace, config)
		require.NoError(t, err)
		callbackSignal := make(chan struct{}, 2)
		callback := func() {
			select {
			case callbackSignal <- struct{}{}:
			default:
			}
		}
		engine.memtableRotateCallback = callback
		engine.setFsyncCallback(callback)

		err = engine.PutKV([]byte("key1"), []byte("value1"))
		require.NoError(t, err)
		engine.rotateMemTable()
		waitForCallbacks(t, callbackSignal, 2)

		var snapshotBuf bytes.Buffer
		_, err = engine.BtreeSnapshot(&snapshotBuf)
		require.NoError(t, err)

		err = engine.close(context.Background())
		require.NoError(t, err)

		raftDir2 := t.TempDir()
		engine2, err := NewStorageEngine(raftDir2, namespace, config)
		require.NoError(t, err)
		defer engine2.close(context.Background())

		err = engine2.Restore(nopCloser{&snapshotBuf})
		require.NoError(t, err, "restoring Raft snapshot into Raft engine should succeed")
	})

	t.Run("standalone_to_standalone", func(t *testing.T) {
		dir := t.TempDir()
		namespace := "restore_same_mode"

		config := NewDefaultEngineConfig()
		config.ArenaSize = 1 << 20
		config.WalConfig.RaftMode = false
		config.DBEngine = BoltDBEngine

		engine, err := NewStorageEngine(dir, namespace, config)
		require.NoError(t, err)
		callbackSignal := make(chan struct{}, 2)
		callback := func() {
			select {
			case callbackSignal <- struct{}{}:
			default:
			}
		}
		engine.memtableRotateCallback = callback
		engine.setFsyncCallback(callback)

		err = engine.PutKV([]byte("key1"), []byte("value1"))
		require.NoError(t, err)
		engine.rotateMemTable()
		waitForCallbacks(t, callbackSignal, 2)

		var snapshotBuf bytes.Buffer
		_, err = engine.BtreeSnapshot(&snapshotBuf)
		require.NoError(t, err)

		err = engine.close(context.Background())
		require.NoError(t, err)

		standaloneDir2 := t.TempDir()
		engine2, err := NewStorageEngine(standaloneDir2, namespace, config)
		require.NoError(t, err)
		defer engine2.close(context.Background())

		err = engine2.Restore(nopCloser{&snapshotBuf})
		require.NoError(t, err, "restoring standalone snapshot into standalone engine should succeed")
	})
}
