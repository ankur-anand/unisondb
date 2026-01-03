package dbkernel

import (
	"fmt"
	"io"
	"log/slog"
	"sync/atomic"

	"github.com/ankur-anand/unisondb/dbkernel/internal"
	"github.com/ankur-anand/unisondb/dbkernel/internal/wal"
	"github.com/ankur-anand/unisondb/pkg/walfs"
	"github.com/ankur-anand/unisondb/schemas/logrecord"
	"github.com/hashicorp/raft"
)

// PositionLookup is a function that returns the WAL position for a Raft log index.
type PositionLookup func(index uint64) (walfs.RecordPosition, bool)

// RaftApplier is an interface for proposing commands to Raft.
type RaftApplier interface {
	// Apply proposes data to the Raft cluster and waits for it to be committed.
	// Returns the committed log index and error if the proposal fails or times out.
	Apply(data []byte) (index uint64, err error)
}

// WALCommitCallback is called when a committed entry's WAL position is known.
// In Raft mode, this is used to update the WAL's commit boundary so that
// ISR followers streaming from the WAL only see committed entries.
type WALCommitCallback func(pos walfs.RecordPosition)

type raftState struct {
	raftMode bool
	// applied Raft log index (in memtable)
	appliedIndex atomic.Uint64
	appliedTerm  atomic.Uint64
	//  raft index flushed to B-tree (for snapshots)
	flushedIndex   atomic.Uint64
	flushedTerm    atomic.Uint64
	positionLookup PositionLookup
	applier        RaftApplier

	// snapshotIndexHolder communicates flushedIndex to FSMSnapshotStore.
	// It's Set by Snapshot(), and read by FSMSnapshotStore.Create().
	snapshotIndexHolder *FlushedIndexHolder

	// walCommitCallback is called when a committed entry is applied to update
	// the WAL's commit boundary. This enables ISR-style replication where
	// followers stream from the WAL and only see committed entries.
	walCommitCallback WALCommitCallback

	// raftWalIO wraps the Raft log store's WAL for reading.
	// In raft mode, readers use this WalIO with RaftWALDecoder.
	raftWalIO *wal.WalIO
}

// SetPositionLookup sets the function used to look up WAL positions from Raft log indices.
func (e *Engine) SetPositionLookup(lookup PositionLookup) {
	e.raftState.positionLookup = lookup
}

// SetRaftApplier sets the RaftApplier used to propose commands to Raft.
func (e *Engine) SetRaftApplier(applier RaftApplier) {
	e.raftState.applier = applier
}

// SetWALCommitCallback sets the callback that is invoked when a committed
// Raft entry is applied. The callback receives the WAL position of the
// committed entry, which can be used to update the WAL's commit boundary.
func (e *Engine) SetWALCommitCallback(callback WALCommitCallback) {
	e.raftState.walCommitCallback = callback
}

// SetRaftWAL sets the Raft log store's underlying WAL.
// In raft mode, engine readers will use this WAL with RaftWALDecoder.
func (e *Engine) SetRaftWAL(w *walfs.WALog) {
	e.raftState.raftWalIO = wal.WrapWAL(w, e.namespace)
}

// Apply implements raft.FSM interface.
// It applies a committed Raft log entry to the Engine's state.
// In Raft mode, this is the only way data enters the Engine.
//
// Idempotency: This function is idempotent with respect to entries already flushed
// to the B-tree. If log.Index <= flushedIndex, the entry is already persisted in
// the B-tree and we skip re-applying it. This handles the timing race where:
//  1. Snapshot() captures flushedIndex=80
//  2. Create() records metadata with index=80
//  3. Concurrent flush advances B-tree to index=100
//  4. Persist() writes B-tree snapshot (contains data up to 100)
//  5. On restore, Raft replays from 81, but B-tree already has 81-100
func (e *Engine) Apply(log *raft.Log) interface{} {
	if log.Type != raft.LogCommand {
		return nil
	}

	// Idempotency check: skip if already flushed to B-Tree.
	flushedIndex := e.raftState.flushedIndex.Load()
	if flushedIndex > 0 && log.Index <= flushedIndex {
		e.raftState.appliedIndex.Store(log.Index)
		e.raftState.appliedTerm.Store(log.Term)
		return nil
	}

	record := logrecord.GetRootAsLogRecord(log.Data, 0)

	e.mu.Lock()
	defer e.mu.Unlock()

	var offset *wal.Offset
	if e.raftState.positionLookup != nil {
		if pos, ok := e.raftState.positionLookup(log.Index); ok {
			offset = &wal.Offset{
				SegmentID: pos.SegmentID,
				Offset:    pos.Offset,
			}
		}
	}

	if err := e.applyLogRecord(record, offset); err != nil {
		slog.Error("[dbkernel]",
			slog.String("message", "Failed to apply Raft log"),
			slog.Group("raft",
				slog.Uint64("index", log.Index),
				slog.Uint64("term", log.Term),
			),
			slog.String("error", err.Error()),
		)
		return err
	}

	e.raftState.appliedIndex.Store(log.Index)
	e.raftState.appliedTerm.Store(log.Term)

	// raft position in memtable
	e.activeMemTable.SetRaftPosition(log.Index, log.Term)

	e.writeSeenCounter.Add(1)
	return nil
}

// Snapshot implements raft.FSM interface.
// It returns an FSMSnapshot that can be used to save the current state.
// Uses flushedIndex (not appliedIndex) since dataStore only has flushed data.
// After restore, Raft will replay logs from flushedIndex+1 to current.
//
// IMPORTANT: This sets the flushed index in snapshotIndexHolder so that
// FSMSnapshotStore can use it when creating the snapshot. Users MUST use
// FSMSnapshotStore (via SnapshotIndexHolder()) to ensure correct snapshot
// index in Raft's metadata.
func (e *Engine) Snapshot() (raft.FSMSnapshot, error) {
	e.mu.RLock()
	defer e.mu.RUnlock()

	flushedIndex := e.raftState.flushedIndex.Load()
	flushedTerm := e.raftState.flushedTerm.Load()

	if e.raftState.snapshotIndexHolder != nil {
		e.raftState.snapshotIndexHolder.Set(flushedIndex, flushedTerm)
	}

	return &engineSnapshot{
		engine:       e,
		appliedIndex: flushedIndex,
		appliedTerm:  flushedTerm,
	}, nil
}

// Restore implements raft.FSM interface.
func (e *Engine) Restore(rc io.ReadCloser) error {
	defer rc.Close()

	e.mu.Lock()
	defer e.mu.Unlock()

	// Restore the B-Tree from snapshot
	if err := e.dataStore.Restore(rc); err != nil {
		return fmt.Errorf("restore btree: %w", err)
	}

	// Read raft index/term from the checkpoint metadata stored in B-tree
	// Note: If no checkpoint exists (key not found), we use zeros which is correct
	// for a fresh snapshot with no flushed data.
	var raftIndex, raftTerm uint64
	checkpoint, err := e.dataStore.RetrieveMetadata(internal.SysKeyWalCheckPoint)
	if err == nil && len(checkpoint) > 0 {
		metadata := internal.UnmarshalMetadata(checkpoint)
		raftIndex = metadata.RaftIndex
		raftTerm = metadata.RaftTerm
	}

	// applied index/term
	e.raftState.appliedIndex.Store(raftIndex)
	e.raftState.appliedTerm.Store(raftTerm)

	// so flushed = applied
	e.raftState.flushedIndex.Store(raftIndex)
	e.raftState.flushedTerm.Store(raftTerm)

	e.writeSeenCounter.Store(raftIndex)
	e.opsFlushedCounter.Store(raftIndex)

	slog.Info("[dbkernel]",
		slog.String("message", "Restored from Raft snapshot"),
		slog.Group("engine",
			slog.String("namespace", e.namespace),
		),
		slog.Group("snapshot",
			slog.Uint64("raft_index", raftIndex),
			slog.Uint64("raft_term", raftTerm),
		),
	)

	return nil
}

// AppliedIndex returns the last applied Raft log index.
func (e *Engine) AppliedIndex() uint64 {
	return e.raftState.appliedIndex.Load()
}

// AppliedTerm returns the last applied Raft log term.
func (e *Engine) AppliedTerm() uint64 {
	return e.raftState.appliedTerm.Load()
}

// FlushedIndex returns the last Raft log index flushed to B-tree.
func (e *Engine) FlushedIndex() uint64 {
	return e.raftState.flushedIndex.Load()
}

// FlushedTerm returns the last Raft log term flushed to B-tree.
func (e *Engine) FlushedTerm() uint64 {
	return e.raftState.flushedTerm.Load()
}

// IsRaftMode returns true if the Engine is operating in Raft mode.
func (e *Engine) IsRaftMode() bool {
	return e.raftState.raftMode
}

// SetRaftMode enables or disables Raft mode on the Engine.
// When in Raft mode, the Raft log IS the WAL.
func (e *Engine) SetRaftMode(enabled bool) {
	e.raftState.raftMode = enabled
}

// SnapshotIndexHolder returns the FlushedIndexHolder used to communicate
// the flushed index to FSMSnapshotStore. This creates the holder if it doesn't exist.
//
// Usage: When setting up Raft, wrap your SnapshotStore with FSMSnapshotStore:
//
//	holder := engine.SnapshotIndexHolder()
//	snapshotStore := dbkernel.NewFSMSnapshotStore(fileSnapshotStore, holder)
//	raftConfig.SnapshotStore = snapshotStore
//
// This ensures that Raft's snapshot metadata uses the flushed index (actual B-tree state)
// instead of the applied index (which may include unflushed memtable data).
func (e *Engine) SnapshotIndexHolder() *FlushedIndexHolder {
	if e.raftState.snapshotIndexHolder == nil {
		e.raftState.snapshotIndexHolder = &FlushedIndexHolder{}
	}
	return e.raftState.snapshotIndexHolder
}

// engineSnapshot implements raft.FSMSnapshot.
type engineSnapshot struct {
	engine       *Engine
	appliedIndex uint64
	appliedTerm  uint64
}

// Persist writes the snapshot to the given sink.
func (s *engineSnapshot) Persist(sink raft.SnapshotSink) error {
	s.engine.mu.RLock()
	defer s.engine.mu.RUnlock()

	if err := s.engine.dataStore.Snapshot(sink); err != nil {
		_ = sink.Cancel()
		return fmt.Errorf("snapshot btree: %w", err)
	}

	return sink.Close()
}

func (s *engineSnapshot) Release() {
}
