package raftwalfs

import (
	"sync"
	"sync/atomic"

	"github.com/ankur-anand/unisondb/pkg/walfs"
)

const (
	defaultShardCount = 64
)

// ShardedIndex is a concurrent-safe sharded map for Raft index → WAL position.
type ShardedIndex struct {
	shards    []*indexShard
	shardMask uint64
	count     int
	// Total entries across all shards
	size atomic.Int64
}

type indexShard struct {
	mu    sync.RWMutex
	items map[uint64]walfs.RecordPosition
}

// NewShardedIndex creates a sharded index with the specified number of shards.
func NewShardedIndex() *ShardedIndex {
	shardCount := defaultShardCount

	shards := make([]*indexShard, shardCount)
	for i := 0; i < shardCount; i++ {
		shards[i] = &indexShard{
			items: make(map[uint64]walfs.RecordPosition),
		}
	}

	return &ShardedIndex{
		shards:    shards,
		shardMask: uint64(shardCount - 1),
		count:     shardCount,
	}
}

// getShard returns the shard for a given index using fast bitwise modulo.
func (s *ShardedIndex) getShard(index uint64) *indexShard {
	return s.shards[index&s.shardMask]
}

// Set stores the position for an index.
func (s *ShardedIndex) Set(index uint64, pos walfs.RecordPosition) {
	shard := s.getShard(index)
	shard.mu.Lock()
	_, existed := shard.items[index]
	shard.items[index] = pos
	shard.mu.Unlock()

	if !existed {
		s.size.Add(1)
	}
}

// Get retrieves the position for an index.
// Returns the position and true if found, zero value and false otherwise.
func (s *ShardedIndex) Get(index uint64) (walfs.RecordPosition, bool) {
	shard := s.getShard(index)
	shard.mu.RLock()
	pos, ok := shard.items[index]
	shard.mu.RUnlock()
	return pos, ok
}

// Delete removes an index from the map.
func (s *ShardedIndex) Delete(index uint64) {
	shard := s.getShard(index)
	shard.mu.Lock()
	_, existed := shard.items[index]
	delete(shard.items, index)
	shard.mu.Unlock()

	if existed {
		s.size.Add(-1)
	}
}

// DeleteRange removes all indices in [min, max] range.
func (s *ShardedIndex) DeleteRange(min, max uint64) int64 {
	if min > max {
		return 0
	}

	type shardOp struct {
		indices []uint64
	}

	ops := make([]shardOp, s.count)

	rangeSize := max - min + 1
	expectedPerShard := int(rangeSize/uint64(s.count)) + 1
	for i := range ops {
		ops[i].indices = make([]uint64, 0, expectedPerShard)
	}

	for idx := min; idx <= max; idx++ {
		shardIdx := idx & s.shardMask
		ops[shardIdx].indices = append(ops[shardIdx].indices, idx)
	}

	var wg sync.WaitGroup
	deleted := make([]int64, s.count)

	for i, op := range ops {
		if len(op.indices) == 0 {
			continue
		}
		wg.Add(1)
		go func(shardIdx int, shard *indexShard, indices []uint64) {
			defer wg.Done()
			shard.mu.Lock()
			for _, idx := range indices {
				if _, existed := shard.items[idx]; existed {
					delete(shard.items, idx)
					deleted[shardIdx]++
				}
			}
			shard.mu.Unlock()
		}(i, s.shards[i], op.indices)
	}
	wg.Wait()

	var totalDeleted int64
	for _, d := range deleted {
		totalDeleted += d
	}
	s.size.Add(-totalDeleted)

	return totalDeleted
}

// IndexEntry represents an index to position mapping for batch operations.
type IndexEntry struct {
	Index uint64
	Pos   walfs.RecordPosition
}

// SetBatch stores multiple index→position mappings.
func (s *ShardedIndex) SetBatch(entries []IndexEntry) {
	if len(entries) == 0 {
		return
	}

	type shardOp struct {
		entries []IndexEntry
	}

	ops := make([]shardOp, s.count)
	expectedPerShard := len(entries)/s.count + 1
	for i := range ops {
		ops[i].entries = make([]IndexEntry, 0, expectedPerShard)
	}

	for _, e := range entries {
		shardIdx := e.Index & s.shardMask
		ops[shardIdx].entries = append(ops[shardIdx].entries, e)
	}

	var wg sync.WaitGroup
	added := make([]int64, s.count)

	for i, op := range ops {
		if len(op.entries) == 0 {
			continue
		}
		wg.Add(1)
		go func(shardIdx int, shard *indexShard, entries []IndexEntry) {
			defer wg.Done()
			shard.mu.Lock()
			for _, e := range entries {
				if _, existed := shard.items[e.Index]; !existed {
					added[shardIdx]++
				}
				shard.items[e.Index] = e.Pos
			}
			shard.mu.Unlock()
		}(i, s.shards[i], op.entries)
	}
	wg.Wait()

	var totalAdded int64
	for _, a := range added {
		totalAdded += a
	}
	s.size.Add(totalAdded)
}

// IsCurrentEntry checks if the given position is the current one for an index.
// Returns true if the index exists and the position matches exactly.
func (s *ShardedIndex) IsCurrentEntry(index uint64, segmentID walfs.SegmentID, offset int64) bool {
	pos, ok := s.Get(index)
	if !ok {
		return false
	}
	return pos.SegmentID == segmentID && pos.Offset == offset
}

// Len returns the total number of entries across all shards.
func (s *ShardedIndex) Len() int64 {
	return s.size.Load()
}

// LenSlow returns the total number of entries by summing all shards.
func (s *ShardedIndex) LenSlow() int64 {
	var total int64
	for _, shard := range s.shards {
		shard.mu.RLock()
		total += int64(len(shard.items))
		shard.mu.RUnlock()
	}
	return total
}

// Clear removes all entries from all shards.
func (s *ShardedIndex) Clear() {
	var wg sync.WaitGroup
	for _, shard := range s.shards {
		wg.Add(1)
		go func(sh *indexShard) {
			defer wg.Done()
			sh.mu.Lock()
			sh.items = make(map[uint64]walfs.RecordPosition)
			sh.mu.Unlock()
		}(shard)
	}
	wg.Wait()
	s.size.Store(0)
}

// Range iterates over all entries and calls fn for each.
// If fn returns false, iteration stops.
func (s *ShardedIndex) Range(fn func(index uint64, pos walfs.RecordPosition) bool) {
	for _, shard := range s.shards {
		shard.mu.RLock()
		for idx, pos := range shard.items {
			if !fn(idx, pos) {
				shard.mu.RUnlock()
				return
			}
		}
		shard.mu.RUnlock()
	}
}

// GetFirstLast returns the first and last indices in the index.
// Returns (0, 0, false) if the index is empty.
func (s *ShardedIndex) GetFirstLast() (first, last uint64, ok bool) {
	first = ^uint64(0)
	last = 0
	found := false

	for _, shard := range s.shards {
		shard.mu.RLock()
		for idx := range shard.items {
			found = true
			if idx < first {
				first = idx
			}
			if idx > last {
				last = idx
			}
		}
		shard.mu.RUnlock()
	}

	if !found {
		return 0, 0, false
	}
	return first, last, true
}
