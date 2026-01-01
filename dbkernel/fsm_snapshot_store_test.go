package dbkernel

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/ankur-anand/unisondb/dbkernel/internal"
	"github.com/hashicorp/raft"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestFSMSnapshotStore_WithRealRaft(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "fsm-snapshot-test-*")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	engineDir := filepath.Join(tmpDir, "engine")
	require.NoError(t, os.MkdirAll(engineDir, 0755))

	config := NewDefaultEngineConfig()
	config.ArenaSize = 1 << 20
	engine, err := NewStorageEngine(engineDir, "test-ns", config)
	require.NoError(t, err)
	defer engine.close(context.Background())
	engine.SetRaftMode(true)

	holder := engine.SnapshotIndexHolder()

	logStore := raft.NewInmemStore()
	stableStore := raft.NewInmemStore()

	snapshotDir := filepath.Join(tmpDir, "snapshots")
	require.NoError(t, os.MkdirAll(snapshotDir, 0755))
	fileSnapshotStore, err := raft.NewFileSnapshotStore(snapshotDir, 3, os.Stderr)
	require.NoError(t, err)

	snapshotStore := NewFSMSnapshotStore(fileSnapshotStore, holder)

	_, transport := raft.NewInmemTransport("")

	raftConfig := raft.DefaultConfig()
	raftConfig.LocalID = "node1"
	raftConfig.HeartbeatTimeout = 50 * time.Millisecond
	raftConfig.ElectionTimeout = 50 * time.Millisecond
	raftConfig.LeaderLeaseTimeout = 50 * time.Millisecond
	raftConfig.CommitTimeout = 5 * time.Millisecond
	raftConfig.SnapshotThreshold = 10
	raftConfig.SnapshotInterval = 100 * time.Millisecond
	raftConfig.LogOutput = io.Discard

	r, err := raft.NewRaft(raftConfig, engine, logStore, stableStore, snapshotStore, transport)
	require.NoError(t, err)
	defer r.Shutdown()

	configuration := raft.Configuration{
		Servers: []raft.Server{
			{ID: "node1", Address: transport.LocalAddr()},
		},
	}
	f := r.BootstrapCluster(configuration)
	require.NoError(t, f.Error())

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if r.State() == raft.Leader {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	require.Equal(t, raft.Leader, r.State(), "should become leader")

	applier := &raftApplierWrapper{raft: r, timeout: 5 * time.Second}
	engine.SetRaftApplier(applier)

	for i := 0; i < 20; i++ {
		err := engine.PutKV([]byte(fmt.Sprintf("key%d", i)), []byte(fmt.Sprintf("value%d", i)))
		require.NoError(t, err)
	}

	t.Logf("Applied 20 logs. Applied index: %d, Flushed index: %d",
		engine.AppliedIndex(), engine.FlushedIndex())

	appliedBefore := engine.AppliedIndex()
	flushedBefore := engine.FlushedIndex()
	fmt.Println(flushedBefore)
	assert.Greater(t, appliedBefore, flushedBefore,
		"applied should be > flushed before flush")

	snapshotFuture := r.Snapshot()
	require.NoError(t, snapshotFuture.Error())

	snapshots, err := snapshotStore.List()
	require.NoError(t, err)
	require.NotEmpty(t, snapshots, "should have at least one snapshot")

	latestSnapshot := snapshots[0]
	fmt.Println(latestSnapshot)
	t.Logf("Snapshot created with index: %d, term: %d",
		latestSnapshot.Index, latestSnapshot.Term)

	assert.LessOrEqual(t, latestSnapshot.Index, appliedBefore,
		"snapshot index should not exceed applied index")

	t.Logf("Test passed! Snapshot index %d <= applied index %d",
		latestSnapshot.Index, appliedBefore)
}

func TestFSMSnapshotStore_RestoreFromCheckpoint(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "fsm-restore-test-*")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	engineDir := filepath.Join(tmpDir, "engine")
	require.NoError(t, os.MkdirAll(engineDir, 0755))

	config := NewDefaultEngineConfig()
	config.ArenaSize = 1 << 20
	engine, err := NewStorageEngine(engineDir, "test-ns", config)
	require.NoError(t, err)

	engine.SetRaftMode(true)

	r, logStore, cleanupRaft := setupSingleNodeRaft(t, tmpDir, engine)
	defer cleanupRaft()

	engine.SetPositionLookup(logStore.GetPosition)

	applier := &raftApplierWrapper{raft: r, timeout: 5 * time.Second}
	engine.SetRaftApplier(applier)

	for i := 0; i < 10; i++ {
		err := engine.PutKV([]byte(fmt.Sprintf("key%d", i)), []byte(fmt.Sprintf("value%d", i)))
		require.NoError(t, err)
	}

	t.Logf("Applied 10 logs. Applied: %d, Flushed: %d",
		engine.AppliedIndex(), engine.FlushedIndex())

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

	t.Logf("After flush. Applied: %d, Flushed: %d",
		engine.AppliedIndex(), engine.FlushedIndex())

	for i := 10; i < 15; i++ {
		err := engine.PutKV([]byte(fmt.Sprintf("key%d", i)), []byte(fmt.Sprintf("value%d", i)))
		require.NoError(t, err)
	}

	appliedIndex := engine.AppliedIndex()
	flushedIndex := engine.FlushedIndex()
	t.Logf("Final state. Applied: %d, Flushed: %d", appliedIndex, flushedIndex)

	assert.Less(t, flushedIndex, appliedIndex)

	checkpointBefore, err := engine.dataStore.RetrieveMetadata(internal.SysKeyWalCheckPoint)
	if err != nil {
		t.Logf("Debug: No checkpoint found before snapshot: %v", err)
	} else if len(checkpointBefore) > 0 {
		metaBefore := internal.UnmarshalMetadata(checkpointBefore)
		t.Logf("Debug: B-tree metadata BEFORE snapshot: RecordProcessed=%d, RaftIndex=%d, RaftTerm=%d",
			metaBefore.RecordProcessed, metaBefore.RaftIndex, metaBefore.RaftTerm)
	} else {
		t.Log("Debug: Empty checkpoint data before snapshot")
	}

	holder := engine.SnapshotIndexHolder()
	fsmSnapshot, err := engine.Snapshot()
	require.NoError(t, err)

	holderIndex, holderTerm := holder.Get()
	assert.Equal(t, flushedIndex, holderIndex)
	t.Logf("Holder set to index: %d, term: %d", holderIndex, holderTerm)

	snapshotDir := filepath.Join(tmpDir, "snapshots")
	require.NoError(t, os.MkdirAll(snapshotDir, 0755))
	snapshotFile := filepath.Join(snapshotDir, "snapshot.dat")

	f, err := os.Create(snapshotFile)
	require.NoError(t, err)

	sink := &testFileSink{file: f}
	err = fsmSnapshot.Persist(sink)
	require.NoError(t, err)
	fsmSnapshot.Release()

	engine.close(context.Background())

	engine2Dir := filepath.Join(tmpDir, "engine2")
	require.NoError(t, os.MkdirAll(engine2Dir, 0755))

	engine2, err := NewStorageEngine(engine2Dir, "test-ns", config)
	require.NoError(t, err)
	defer engine2.close(context.Background())

	engine2.SetRaftMode(true)

	snapshotReader, err := os.Open(snapshotFile)
	require.NoError(t, err)

	err = engine2.Restore(snapshotReader)
	require.NoError(t, err)

	t.Logf("Restored engine. Applied: %d, Flushed: %d",
		engine2.AppliedIndex(), engine2.FlushedIndex())

	assert.Equal(t, flushedIndex, engine2.AppliedIndex(),
		"restored applied index should match flushed index from checkpoint")
	assert.Equal(t, flushedIndex, engine2.FlushedIndex(),
		"restored flushed index should match checkpoint")

	for i := 0; i < 10; i++ {
		key := fmt.Sprintf("key%d", i)
		value, err := engine2.GetKV([]byte(key))
		require.NoError(t, err, "key %s should be restored", key)
		assert.Equal(t, fmt.Sprintf("value%d", i), string(value),
			"key %s should be restored", key)
	}

	t.Log("Restore test passed!")
}

func TestFSMSnapshotStore_SnapshotAndRestoreWithRaft(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "fsm-full-test-*")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	engineDir := filepath.Join(tmpDir, "engine")
	require.NoError(t, os.MkdirAll(engineDir, 0755))

	config := NewDefaultEngineConfig()
	config.ArenaSize = 1 << 20
	engine, err := NewStorageEngine(engineDir, "test-ns", config)
	require.NoError(t, err)

	engine.SetRaftMode(true)

	holder := engine.SnapshotIndexHolder()

	logStore := raft.NewInmemStore()
	stableStore := raft.NewInmemStore()
	inMemSnapshotStore := raft.NewInmemSnapshotStore()
	snapshotStore := NewFSMSnapshotStore(inMemSnapshotStore, holder)

	_, transport := raft.NewInmemTransport("")

	raftConfig := raft.DefaultConfig()
	raftConfig.LocalID = "node1"
	raftConfig.HeartbeatTimeout = 50 * time.Millisecond
	raftConfig.ElectionTimeout = 50 * time.Millisecond
	raftConfig.LeaderLeaseTimeout = 50 * time.Millisecond
	raftConfig.CommitTimeout = 5 * time.Millisecond
	raftConfig.LogOutput = io.Discard

	r, err := raft.NewRaft(raftConfig, engine, logStore, stableStore, snapshotStore, transport)
	require.NoError(t, err)
	defer r.Shutdown()

	configuration := raft.Configuration{
		Servers: []raft.Server{
			{ID: "node1", Address: transport.LocalAddr()},
		},
	}
	require.NoError(t, r.BootstrapCluster(configuration).Error())

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if r.State() == raft.Leader {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	require.Equal(t, raft.Leader, r.State())

	applier := &raftApplierWrapper{raft: r, timeout: 5 * time.Second}
	engine.SetRaftApplier(applier)

	for i := 0; i < 50; i++ {
		err := engine.PutKV([]byte(fmt.Sprintf("key%d", i)), []byte(fmt.Sprintf("value%d", i)))
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

	flushedAfterFirst := engine.FlushedIndex()
	t.Logf("After first flush: Applied=%d, Flushed=%d", engine.AppliedIndex(), flushedAfterFirst)

	for i := 50; i < 75; i++ {
		err := engine.PutKV([]byte(fmt.Sprintf("key%d", i)), []byte(fmt.Sprintf("value%d", i)))
		require.NoError(t, err)
	}

	appliedBeforeSnapshot := engine.AppliedIndex()
	flushedBeforeSnapshot := engine.FlushedIndex()
	t.Logf("Before snapshot: Applied=%d, Flushed=%d", appliedBeforeSnapshot, flushedBeforeSnapshot)

	require.Less(t, flushedBeforeSnapshot, appliedBeforeSnapshot,
		"flushed should be less than applied before snapshot")

	snapshotFuture := r.Snapshot()
	require.NoError(t, snapshotFuture.Error())

	snapshots, err := snapshotStore.List()
	require.NoError(t, err)
	require.NotEmpty(t, snapshots)

	latestSnap := snapshots[0]
	t.Logf("Snapshot metadata: Index=%d, Term=%d", latestSnap.Index, latestSnap.Term)

	assert.LessOrEqual(t, latestSnap.Index, appliedBeforeSnapshot,
		"snapshot index should not exceed applied index")

	meta, reader, err := snapshotStore.Open(latestSnap.ID)
	require.NoError(t, err)
	require.NotNil(t, meta)
	require.NotNil(t, reader)
	defer reader.Close()

	var buf bytes.Buffer
	_, err = io.Copy(&buf, reader)
	require.NoError(t, err)

	engine2Dir := filepath.Join(tmpDir, "engine2")
	require.NoError(t, os.MkdirAll(engine2Dir, 0755))

	engine2, err := NewStorageEngine(engine2Dir, "test-ns", config)
	require.NoError(t, err)
	defer engine2.close(context.Background())

	engine2.SetRaftMode(true)

	err = engine2.Restore(io.NopCloser(bytes.NewReader(buf.Bytes())))
	require.NoError(t, err)

	t.Logf("After restore: Applied=%d, Flushed=%d", engine2.AppliedIndex(), engine2.FlushedIndex())

	assert.Equal(t, engine2.AppliedIndex(), engine2.FlushedIndex(),
		"restored engine should have applied == flushed")

	t.Log("Full snapshot/restore test passed!")
}

func TestFSMSnapshotStore_RestoreAndLogReplay(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "fsm-replay-test-*")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	engineDir := filepath.Join(tmpDir, "engine")
	require.NoError(t, os.MkdirAll(engineDir, 0755))

	config := NewDefaultEngineConfig()
	config.ArenaSize = 1 << 20
	engine, err := NewStorageEngine(engineDir, "test-ns", config)
	require.NoError(t, err)

	engine.SetRaftMode(true)

	holder := engine.SnapshotIndexHolder()

	logStore := raft.NewInmemStore()
	stableStore := raft.NewInmemStore()
	inMemSnapshotStore := raft.NewInmemSnapshotStore()
	snapshotStore := NewFSMSnapshotStore(inMemSnapshotStore, holder)

	_, transport := raft.NewInmemTransport("")

	raftConfig := raft.DefaultConfig()
	raftConfig.LocalID = "node1"
	raftConfig.HeartbeatTimeout = 50 * time.Millisecond
	raftConfig.ElectionTimeout = 50 * time.Millisecond
	raftConfig.LeaderLeaseTimeout = 50 * time.Millisecond
	raftConfig.CommitTimeout = 5 * time.Millisecond
	raftConfig.TrailingLogs = 1000
	raftConfig.SnapshotThreshold = 10000
	raftConfig.LogOutput = io.Discard

	r, err := raft.NewRaft(raftConfig, engine, logStore, stableStore, snapshotStore, transport)
	require.NoError(t, err)
	defer r.Shutdown()

	configuration := raft.Configuration{
		Servers: []raft.Server{
			{ID: "node1", Address: transport.LocalAddr()},
		},
	}
	require.NoError(t, r.BootstrapCluster(configuration).Error())

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if r.State() == raft.Leader {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	require.Equal(t, raft.Leader, r.State())

	applier := &raftApplierWrapper{raft: r, timeout: 5 * time.Second}
	engine.SetRaftApplier(applier)

	for i := 0; i < 50; i++ {
		err := engine.PutKV([]byte(fmt.Sprintf("key%d", i)), []byte(fmt.Sprintf("value%d", i)))
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

	flushedAfterFirstBatch := engine.FlushedIndex()
	t.Logf("After first batch: Applied=%d, Flushed=%d", engine.AppliedIndex(), flushedAfterFirstBatch)

	for i := 50; i < 75; i++ {
		err := engine.PutKV([]byte(fmt.Sprintf("key%d", i)), []byte(fmt.Sprintf("value%d", i)))
		require.NoError(t, err)
	}

	appliedBeforeSnapshot := engine.AppliedIndex()
	flushedBeforeSnapshot := engine.FlushedIndex()
	t.Logf("Before snapshot: Applied=%d, Flushed=%d", appliedBeforeSnapshot, flushedBeforeSnapshot)

	require.Greater(t, appliedBeforeSnapshot, flushedBeforeSnapshot,
		"should have unflushed data in memtable")

	snapshotFuture := r.Snapshot()
	require.NoError(t, snapshotFuture.Error())

	snapshots, err := snapshotStore.List()
	require.NoError(t, err)
	require.NotEmpty(t, snapshots)

	snapshotMeta := snapshots[0]
	t.Logf("Snapshot taken at index=%d (flushed=%d, applied=%d)",
		snapshotMeta.Index, flushedBeforeSnapshot, appliedBeforeSnapshot)

	assert.Equal(t, flushedBeforeSnapshot, snapshotMeta.Index,
		"snapshot index should match flushedIndex")

	firstIdx, _ := logStore.FirstIndex()
	lastIdx, _ := logStore.LastIndex()
	t.Logf("LogStore range: first=%d, last=%d", firstIdx, lastIdx)
	assert.GreaterOrEqual(t, lastIdx, appliedBeforeSnapshot,
		"LogStore should have logs up to appliedIndex")

	engine2Dir := filepath.Join(tmpDir, "engine2")
	require.NoError(t, os.MkdirAll(engine2Dir, 0755))

	engine2, err := NewStorageEngine(engine2Dir, "test-ns", config)
	require.NoError(t, err)
	defer engine2.close(context.Background())

	engine2.SetRaftMode(true)

	meta, reader, err := snapshotStore.Open(snapshotMeta.ID)
	require.NoError(t, err)
	require.NotNil(t, meta)

	var snapshotBuf bytes.Buffer
	_, err = io.Copy(&snapshotBuf, reader)
	require.NoError(t, err)
	reader.Close()

	err = engine2.Restore(io.NopCloser(bytes.NewReader(snapshotBuf.Bytes())))
	require.NoError(t, err)

	t.Logf("After restore: Applied=%d, Flushed=%d", engine2.AppliedIndex(), engine2.FlushedIndex())

	assert.Equal(t, flushedBeforeSnapshot, engine2.AppliedIndex(),
		"restored appliedIndex should match snapshot index (flushedIndex)")

	for i := 0; i < 50; i++ {
		key := fmt.Sprintf("key%d", i)
		value, err := engine2.GetKV([]byte(key))
		require.NoError(t, err, "key %s should exist in B-tree after restore", key)
		assert.Equal(t, fmt.Sprintf("value%d", i), string(value))
	}
	t.Log("B-tree data (keys 0-49) verified after restore")

	for i := 50; i < 75; i++ {
		key := fmt.Sprintf("key%d", i)
		_, err := engine2.GetKV([]byte(key))
		assert.Error(t, err, "key %s should NOT exist before log replay", key)
	}
	t.Log("Confirmed keys 50-74 do NOT exist before log replay")

	t.Logf("Replaying logs from %d to %d", flushedBeforeSnapshot+1, appliedBeforeSnapshot)

	for idx := flushedBeforeSnapshot + 1; idx <= appliedBeforeSnapshot; idx++ {
		log := new(raft.Log)
		err := logStore.GetLog(idx, log)
		require.NoError(t, err, "should be able to read log at index %d", idx)

		if log.Type == raft.LogCommand {
			result := engine2.Apply(log)
			assert.Nil(t, result, "Apply should succeed for log %d", idx)
		}
	}

	t.Logf("After replay: Applied=%d, Flushed=%d", engine2.AppliedIndex(), engine2.FlushedIndex())

	for i := 0; i < 75; i++ {
		key := fmt.Sprintf("key%d", i)
		value, err := engine2.GetKV([]byte(key))
		require.NoError(t, err, "key %s should exist after restore + replay", key)
		assert.Equal(t, fmt.Sprintf("value%d", i), string(value),
			"key %s should have correct value", key)
	}

	assert.Equal(t, appliedBeforeSnapshot, engine2.AppliedIndex(),
		"appliedIndex should match original after replay")
	assert.Equal(t, flushedBeforeSnapshot, engine2.FlushedIndex(),
		"flushedIndex should still be snapshot index (no new flush)")

	t.Logf("SUCCESS: Restore + Log Replay recovered all %d keys", 75)
	t.Logf("  - B-tree (from snapshot): keys 0-49")
	t.Logf("  - Memtable (from replay): keys 50-74")
}

type testFileSink struct {
	file *os.File
}

func (s *testFileSink) Write(p []byte) (n int, err error) {
	return s.file.Write(p)
}

func (s *testFileSink) Close() error {
	return s.file.Close()
}

func (s *testFileSink) ID() string {
	return "test-snapshot"
}

func (s *testFileSink) Cancel() error {
	s.file.Close()
	return os.Remove(s.file.Name())
}
