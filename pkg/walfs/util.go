package walfs

import "sync"

// Why not use sync.Map?
//
// - sync.Map is optimized for scenarios with infrequent writes and frequent reads.
//   In our use case, reader add/remove operations are relatively frequent and concurrent.
//
// Our fuzzer with high reader showed
// Ops/sec (Add + Remove)
// ~5.2 million with SYNC.Map
// ~13.6 million with our reader Tracker
// improvement.

const readerShardCount = 32
const readerShardMask = readerShardCount - 1

// readerTracker efficiently tracks active readers using sharded locking.
// This minimizes contention in concurrent environments like WAL segment tracking.
type readerTracker struct {
	shards [readerShardCount]readerShard
}

// readerShard holds a portion of the overall ID space to reduce lock contention.
type readerShard struct {
	mu    sync.Mutex
	items map[uint64]struct{}
}

// newReaderTracker creates a new readerTracker with initialized shards.
func newReaderTracker() *readerTracker {
	rt := &readerTracker{}
	for i := range rt.shards {
		rt.shards[i].items = make(map[uint64]struct{})
	}
	return rt
}

// shard returns the appropriate shard for the given ID.
func (rt *readerTracker) shard(id uint64) *readerShard {
	return &rt.shards[id&readerShardMask]
}

// Add marks a reader ID as active.
func (rt *readerTracker) Add(id uint64) {
	sh := rt.shard(id)
	sh.mu.Lock()
	sh.items[id] = struct{}{}
	sh.mu.Unlock()
}

// Remove deletes a reader ID from the tracker.
// It returns true if the ID was present and removed.
func (rt *readerTracker) Remove(id uint64) bool {
	sh := rt.shard(id)
	sh.mu.Lock()
	_, exists := sh.items[id]
	if exists {
		delete(sh.items, id)
	}
	sh.mu.Unlock()
	return exists
}

// HasAny returns true if there is at least one active reader across all shards.
func (rt *readerTracker) HasAny() bool {
	for i := range rt.shards {
		sh := &rt.shards[i]
		sh.mu.Lock()
		if len(sh.items) > 0 {
			sh.mu.Unlock()
			return true
		}
		sh.mu.Unlock()
	}
	return false
}

// Contains returns true if the given reader ID is currently tracked.
func (rt *readerTracker) Contains(id uint64) bool {
	sh := rt.shard(id)
	sh.mu.Lock()
	_, exists := sh.items[id]
	sh.mu.Unlock()
	return exists
}
