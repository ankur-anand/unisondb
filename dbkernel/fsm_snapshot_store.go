package dbkernel

import (
	"io"
	"sync/atomic"

	"github.com/hashicorp/raft"
)

// Raft Snapshot Design for UnisonDB Engine
//
//  The Problem: appliedIndex vs flushedIndex
//
//  UnisonDB Engine uses a two-tier storage architecture:
//
//	 ┌─────────────────────────────────────────────────────────────┐
//	 │                     Raft Log (WAL)                          │
//	 │  [1] [2] [3] ... [80] [81] ... [100]                        │
//	 │                        ↑              ↑                     │
//	 │                   flushedIndex   appliedIndex               │
//	 └─────────────────────────────────────────────────────────────┘
//							  │              │
//							  ▼              ▼
//	 ┌─────────────────────┐  ┌─────────────────────┐
//	 │      B-Tree         │  │     MemTable        │
//	 │  (persistent)       │  │  (in-memory)        │
//	 │                     │  │                     │
//	 │  Data: 1-80         │  │  Data: 81-100       │
//	 │                     │  │                     │
//	 └─────────────────────┘  └─────────────────────┘
//
//  - appliedIndex (100): Last log entry applied to MemTable
//  - flushedIndex (80): Last log entry flushed to B-Tree
//
//  The B-Tree snapshot only contains data up to flushedIndex, not appliedIndex!
//
//  Why Standard Raft Snapshots Break
//
//  hashicorp/raft's default behavior:
//
//  1. Raft calls FSM.Snapshot()
//  2. Raft calls SnapshotStore.Create(appliedIndex=100, ...)  ← Uses appliedIndex!
//  3. Raft calls FSMSnapshot.Persist() → Writes B-Tree (contains data up to 80)
//  4. Raft compacts logs up to index 100
//  5. Logs 81-100 are DELETED
//
//  On Restore:
//  1. Raft restores snapshot (thinks it's at index 100)
//  2. B-Tree only has data up to 80
//  3. Raft replays from 101 (logs 81-100 are gone!)
//  4. DATA LOSS for entries 81-100!
//
//
// FSMSnapshotStore
//
//  FSMSnapshotStore intercepts Create() and uses flushedIndex instead of appliedIndex:
//
//	┌────────────────────────────────────────────────────────────────────────────┐
//	│                             SNAPSHOT FLOW                                  │
//	├────────────────────────────────────────────────────────────────────────────┤
//	│                                                                            │
//	│  (1) FSM.Snapshot()                                                        │
//	│      └── sets FlushedIndexHolder = { flushedIndex = 80, term = 5 }         │
//	│                                                                            │
//	│  (2) FSMSnapshotStore.Create(appliedIndex = 100, term = 7, ...)            │
//	│      └── reads holder → { flushedIndex = 80, term = 5 }                    │
//	│      └── calls inner.Create(index = 80, term = 5, ...)   ← overridden      │
//	│                                                                            │
//	│  (3) FSMSnapshot.Persist()                                                 │
//	│      └── writes B-Tree snapshot (contains data only up to index = 80)      │
//	│                                                                            │
//	│  (4) Raft log compaction                                                   │
//	│      └── compacts logs up to index = 80                                    │
//	│      └── logs [81..100] are preserved (NOT compacted)                      │
//	│                                                                            │
//	└────────────────────────────────────────────────────────────────────────────┘

//  On Restore:
//  1. Raft restores snapshot at index 80
//  2. B-Tree has data up to 80
//  3. Raft replays logs 81-100
//  4. All data recovered!
//
// Timing Considerations
//
//  Potential Race Condition
//
//  Timeline:
//  1. Snapshot() called: flushedIndex=80, holder set to 80
//  2. Create() called: reads holder=80, metadata says 80
//  3. Apply() runs + triggers flush: flushedIndex becomes 100
//  4. Persist() called: B-Tree now has data up to 100!
//
//  Result: Snapshot metadata says 80, but B-Tree contains data up to 100
//
//  Why This Is Safe
//
//  On restore:
//  - Raft thinks snapshot is at index 80
//  - B-Tree actually has data up to 100 (from checkpoint metadata)
//  - Raft replays logs 81-100
//  - If Apply() is idempotent, replaying already-flushed entries is harmless

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
