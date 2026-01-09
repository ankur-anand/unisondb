package dbkernel

import (
	"io"
	"sync/atomic"

	"github.com/hashicorp/raft"
)

// Raft assumes that when you take a snapshot, everything up to that index is persisted.
// But we have a gap between what's applied to memtable vs what's flushed to disk.
// appliedIndex = 100 (in memtable)
// flushedIndex = 80 (on disk in B-tree)
// When Raft snapshots, it uses appliedIndex for the metadata, then deletes logs up to 100.
// But our B-tree only has data through 80. On recovery, Raft replays from 101. Entries 81-100? Gone.
// We intercept the snapshot store and swap out the index.
// Raft calls Create(index=100), we call the inner store with Create(index=80).
// Now Raft only compacts logs up to 80 and keeps 81-100 around for replay.
//
// The FlushedIndexHolder is just a way to pass the real flushed index from FSM.Snapshot() to SnapshotStore.Create()
// since they're called separately by Raft.
// Race condition?
// What if a flush happens between Snapshot() and Persist()?
// The metadata says 80 but the B-tree now has data through 100.
// Doesn't matter. On recovery we'll replay 81-100 over data that's already there.
// As long as Apply is idempotent (it is), we're fine.

// FlushedIndexHolder holds the flushed index/term that should be used for snapshots.
// This is set by FSM.Snapshot() and read by FSMSnapshotStore.Create().
// The holder ensures that the snapshot metadata reflects the actual B-tree state
// (flushedIndex) rather than the memtable state (appliedIndex).
type FlushedIndexHolder struct {
	index atomic.Uint64
	term  atomic.Uint64
}

// Set stores the flushed index and term.
func (h *FlushedIndexHolder) Set(index, term uint64) {
	h.index.Store(index)
	h.term.Store(term)
}

// Get retrieves the flushed index and term.
func (h *FlushedIndexHolder) Get() (index, term uint64) {
	return h.index.Load(), h.term.Load()
}

// SafeGCIndex implements SafeGCIndexProvider.
// The flushed index is the safe-to-GC index because logs can only be deleted
// after their data has been flushed to the B-tree.
func (h *FlushedIndexHolder) SafeGCIndex() (index uint64, ok bool) {
	v := h.index.Load()
	return v, v > 0
}

// FSMSnapshotStore wraps a standard raft.SnapshotStore and overrides the index/term
// used in snapshot metadata to match the actual flushed state of the FSM.
//
// Background: hashicorp/raft calls FSM.Snapshot() then SnapshotStore.Create(appliedIndex, ...).
// The appliedIndex is what Raft has applied to the FSM, but the Engine may have data
// in memtables that hasn't been flushed to B-tree yet. If we use appliedIndex in the
// snapshot metadata, Raft will compact logs up to that index. On restore, Raft will
// replay from appliedIndex+1, missing the unflushed data between flushedIndex and appliedIndex.
//
// FSM.Snapshot() sets the flushedIndex in FlushedIndexHolder. This wrapper
// reads it in Create() and uses flushedIndex instead of the appliedIndex that Raft passed.
// This ensures Raft's log compaction respects the actual B-tree state.
type FSMSnapshotStore struct {
	inner  raft.SnapshotStore
	holder *FlushedIndexHolder
}

// NewFSMSnapshotStore creates a new FSMSnapshotStore wrapping the given SnapshotStore.
func NewFSMSnapshotStore(inner raft.SnapshotStore, holder *FlushedIndexHolder) *FSMSnapshotStore {
	return &FSMSnapshotStore{
		inner:  inner,
		holder: holder,
	}
}

// Create creates a new snapshot using the flushed index/term from the holder
// instead of the applied index/term passed by Raft.
func (s *FSMSnapshotStore) Create(
	version raft.SnapshotVersion,
	index, term uint64,
	configuration raft.Configuration,
	configurationIndex uint64,
	trans raft.Transport,
) (raft.SnapshotSink, error) {
	flushedIndex, flushedTerm := s.holder.Get()

	// (flushedIndex should always be <= appliedIndex)
	if flushedIndex > 0 && flushedIndex <= index {
		index = flushedIndex
		term = flushedTerm
	}

	return s.inner.Create(version, index, term, configuration, configurationIndex, trans)
}

// List returns the available snapshots.
func (s *FSMSnapshotStore) List() ([]*raft.SnapshotMeta, error) {
	return s.inner.List()
}

// Open opens a snapshot for reading.
func (s *FSMSnapshotStore) Open(id string) (*raft.SnapshotMeta, io.ReadCloser, error) {
	return s.inner.Open(id)
}
