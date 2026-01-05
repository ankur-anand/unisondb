package dbkernel

import (
	"sync/atomic"

	"github.com/hashicorp/raft"
)

// SafeGCIndexProvider provides the safe-to-GC index for log deletion.
type SafeGCIndexProvider interface {
	SafeGCIndex() (index uint64, ok bool)
}

// SafeGCLogStore wraps a LogStore to prevent premature log deletion.
//
// Raft by default wants to delete logs after snapshotting, but we might have not processed.
// Raft calls DeleteRange(1, 400) after applying index 400. But if only
// segments up to index 200 are processed, other raft node lagging might be keep on asking from snapshot as this logs are gone.
//
// This wrapper intercepts DeleteRange and caps the max to our safe-to-GC
// index. Raft asks to delete up to 400, we delete up to 200 instead.
type SafeGCLogStore struct {
	inner    raft.LogStore
	provider SafeGCIndexProvider
}

// SafeGCIndexHolder holds the safe-to-GC index that represents the maximum
// log index that can be safely deleted without losing data.
type SafeGCIndexHolder struct {
	index atomic.Uint64
}

// Set stores the safe-to-GC index.
func (h *SafeGCIndexHolder) Set(index uint64) {
	h.index.Store(index)
}

// SafeGCIndex implements SafeGCIndexProvider.
func (h *SafeGCIndexHolder) SafeGCIndex() (index uint64, ok bool) {
	v := h.index.Load()
	return v, v > 0
}

// Get retrieves the safe-to-GC index (alias for SafeGCIndex).
func (h *SafeGCIndexHolder) Get() (index uint64, ok bool) {
	return h.SafeGCIndex()
}

// NewSafeGCLogStore creates a new SafeGCLogStore wrapping the given LogStore.
// The provider supplies the safe-to-GC index (typically FlushedIndexHolder or SafeGCIndexHolder).
func NewSafeGCLogStore(inner raft.LogStore, provider SafeGCIndexProvider) *SafeGCLogStore {
	return &SafeGCLogStore{
		inner:    inner,
		provider: provider,
	}
}

// FirstIndex returns the first index written. 0 for no entries.
func (s *SafeGCLogStore) FirstIndex() (uint64, error) {
	return s.inner.FirstIndex()
}

// LastIndex returns the last index written. 0 for no entries.
func (s *SafeGCLogStore) LastIndex() (uint64, error) {
	return s.inner.LastIndex()
}

// GetLog gets a log entry at a given index.
func (s *SafeGCLogStore) GetLog(index uint64, log *raft.Log) error {
	return s.inner.GetLog(index, log)
}

// StoreLog stores a log entry.
func (s *SafeGCLogStore) StoreLog(log *raft.Log) error {
	return s.inner.StoreLog(log)
}

// StoreLogs stores multiple log entries.
func (s *SafeGCLogStore) StoreLogs(logs []*raft.Log) error {
	return s.inner.StoreLogs(logs)
}

// DeleteRange deletes a range of log entries, but limits the max to the
// safe-to-GC index to preserve logs needed for pending segment processing.
func (s *SafeGCLogStore) DeleteRange(min, max uint64) error {
	safeIndex, ok := s.provider.SafeGCIndex()

	if ok && safeIndex < max {
		max = safeIndex
	}

	if min > max {
		return nil
	}

	return s.inner.DeleteRange(min, max)
}

var _ raft.LogStore = (*SafeGCLogStore)(nil)
