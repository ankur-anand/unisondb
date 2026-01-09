package raftwalfs

import (
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ankur-anand/unisondb/pkg/walfs"
	"github.com/hashicorp/raft"
)

var (
	// ErrClosed is returned when operations are attempted on a closed store.
	ErrClosed = errors.New("log store is closed")
)

// LogStoreOption configures a LogStore.
type LogStoreOption func(*LogStore)

// WithCodec sets a custom codec for encoding/decoding log entries.
func WithCodec(codec Codec) LogStoreOption {
	return func(l *LogStore) {
		if codec != nil {
			l.codec = codec
		}
	}
}

// LogStore Implements raft.LogStore.
type LogStore struct {
	mu  sync.RWMutex
	wal *walfs.WALog

	codec Codec

	firstIndex atomic.Uint64
	lastIndex  atomic.Uint64

	committedIndex atomic.Uint64

	index *walfs.ShardedIndex

	closed   atomic.Bool
	closedCh chan struct{}
}

// NewLogStore creates a new LogStore backed by the provided WAL.
func NewLogStore(wal *walfs.WALog, committedIndex uint64, opts ...LogStoreOption) (*LogStore, error) {
	store := &LogStore{
		wal:      wal,
		codec:    BinaryCodecV1{},
		index:    walfs.NewShardedIndex(),
		closedCh: make(chan struct{}),
	}

	for _, opt := range opts {
		opt(store)
	}

	store.committedIndex.Store(committedIndex)

	startTime := time.Now()
	if err := store.recoverIndex(); err != nil {
		return nil, fmt.Errorf("recover index: %w", err)
	}

	first := store.firstIndex.Load()
	last := store.lastIndex.Load()
	slog.Info("[raftwalfs]", "message", "log store recovered",
		"first_index", first,
		"last_index", last,
		"entry_count", store.index.Len(),
		"duration", time.Since(startTime),
	)

	return store, nil
}

// recoverIndex rebuilds the in-memory index from all WAL segments.
func (l *LogStore) recoverIndex() error {
	segments := l.wal.Segments()
	if len(segments) == 0 {
		return nil
	}

	keys := slices.Collect(maps.Keys(segments))
	slices.Sort(keys)

	var first, last uint64
	var entries []walfs.IndexEntry

	for _, key := range keys {
		segment := segments[key]
		segFirstIdx := segment.FirstLogIndex()
		if segFirstIdx == 0 {
			continue
		}

		idxEntries := segment.IndexEntries()
		if len(idxEntries) == 0 {
			continue
		}

		// first/last
		if first == 0 || segFirstIdx < first {
			first = segFirstIdx
		}

		for i, entry := range idxEntries {
			logIndex := segFirstIdx + uint64(i)
			entries = append(entries, walfs.IndexEntry{
				Index: logIndex,
				Pos: walfs.RecordPosition{
					Offset:    entry.Offset,
					SegmentID: entry.SegmentID,
				},
			})
			if logIndex > last {
				last = logIndex
			}
		}

		// free memory
		// make sure to initialize the walfs with clear to segment rotation for index.
		segment.ClearIndexFromMemory()
	}

	if len(entries) > 0 {
		l.index.SetBatch(entries)
	}

	l.firstIndex.Store(first)
	l.lastIndex.Store(last)

	return nil
}

// FirstIndex returns the first index written.
func (l *LogStore) FirstIndex() (uint64, error) {
	if l.closed.Load() {
		return 0, ErrClosed
	}
	return l.firstIndex.Load(), nil
}

// LastIndex returns the last index written.
func (l *LogStore) LastIndex() (uint64, error) {
	if l.closed.Load() {
		return 0, ErrClosed
	}
	return l.lastIndex.Load(), nil
}

// GetLog returns the *raft.Log at the given Index.
func (l *LogStore) GetLog(index uint64, out *raft.Log) error {
	if l.closed.Load() {
		return ErrClosed
	}

	first := l.firstIndex.Load()
	last := l.lastIndex.Load()

	if first == 0 || index < first || index > last {
		return raft.ErrLogNotFound
	}

	pos, ok := l.index.Get(index)
	if !ok {
		return raft.ErrLogNotFound
	}

	data, err := l.wal.Read(pos)
	if err != nil {
		return fmt.Errorf("read wal at %v: %w", pos, err)
	}

	decoded, err := l.codec.Decode(data)
	if err != nil {
		return fmt.Errorf("decode log at index %d: %w", index, err)
	}

	*out = decoded
	return nil
}

// StoreLog stores a single log entry.
func (l *LogStore) StoreLog(log *raft.Log) error {
	return l.StoreLogs([]*raft.Log{log})
}

// StoreLogs stores multiple log entries.
// The logs must be contiguous with each other. They may overwrite existing entries
// if there's a gap or conflict (leader change scenario).
func (l *LogStore) StoreLogs(logs []*raft.Log) error {
	if len(logs) == 0 {
		return nil
	}

	if l.closed.Load() {
		return ErrClosed
	}

	l.mu.Lock()
	defer l.mu.Unlock()

	if err := l.validateBatch(logs); err != nil {
		return err
	}

	first := l.firstIndex.Load()
	last := l.lastIndex.Load()
	start := logs[0].Index

	if first > 0 && start < first {
		return raft.ErrLogNotFound
	}
	if last > 0 && start > last+1 {
		return fmt.Errorf("gap in log append: start %d expected %d", start, last+1)
	}

	// start <= last: The incoming batch starts at or before our last index (overlap/conflict)
	// Mostly leader change.
	if last > 0 && start <= last {
		if start == 0 {
			return errors.New("invalid start index 0")
		}
		// Truncate conflicting suffix in WAL
		if err := l.wal.Truncate(start - 1); err != nil {
			return fmt.Errorf("truncate before overwrite: %w", err)
		}
		// Remove old entries from index
		l.index.DeleteRange(start, last)
	}

	records := make([][]byte, len(logs))
	logIndexes := make([]uint64, len(logs))
	for i, log := range logs {
		enc, err := l.codec.Encode(log)
		if err != nil {
			return fmt.Errorf("encode log at index %d: %w", log.Index, err)
		}
		records[i] = enc
		logIndexes[i] = log.Index
	}

	positions, err := l.wal.WriteBatch(records, logIndexes)
	if err != nil {
		return fmt.Errorf("wal write batch: %w", err)
	}

	entries := make([]walfs.IndexEntry, len(logs))
	for i, log := range logs {
		entries[i] = walfs.IndexEntry{
			Index: log.Index,
			Pos:   positions[i],
		}
	}
	l.index.SetBatch(entries)

	if first == 0 {
		l.firstIndex.Store(logs[0].Index)
	}
	l.lastIndex.Store(logs[len(logs)-1].Index)

	return nil
}

func (l *LogStore) validateBatch(logs []*raft.Log) error {
	for i := 1; i < len(logs); i++ {
		if logs[i].Index != logs[i-1].Index+1 {
			return fmt.Errorf("non-contiguous batch: index %d after %d", logs[i].Index, logs[i-1].Index)
		}
	}
	return nil
}

// DeleteRange deletes logs in [min, max] inclusive.
//
// Supports two scenarios required by Raft:
//  1. PREFIX deletion: Log compaction after snapshot (min=firstIndex)
//     - Recalculates firstIndex from remaining entries
//  2. SUFFIX deletion: Log truncation on leader change (max>=lastIndex)
//     - Updates lastIndex to min-1 and truncates WAL
//
// Note: Middle deletion (min>firstIndex && max<lastIndex) is NOT fully supported.
// The entries will be removed from the index, but firstIndex/lastIndex bounds
// will not be recalculated. This is acceptable because Raft only performs
// prefix deletions (compaction) and suffix deletions (truncation), never middle.
func (l *LogStore) DeleteRange(min, max uint64) error {
	if max < min {
		return nil
	}

	if l.closed.Load() {
		return ErrClosed
	}

	l.mu.Lock()
	defer l.mu.Unlock()

	first := l.firstIndex.Load()
	last := l.lastIndex.Load()

	if first == 0 || max < first {
		return nil
	}

	l.index.DeleteRange(min, max)

	// SUFFIX deletion (log truncation on leader change)
	if max >= last && min > first {
		l.lastIndex.Store(min - 1)
		// Truncate WAL
		if err := l.wal.Truncate(min - 1); err != nil {
			return fmt.Errorf("truncate wal: %w", err)
		}
		return nil
	}

	// PREFIX deletion (log compaction after snapshot)
	if min == first {
		// Update first index to next available
		newFirst, _, ok := l.index.GetFirstLast()
		if ok {
			l.firstIndex.Store(newFirst)
		} else {
			l.firstIndex.Store(0)
			l.lastIndex.Store(0)
		}
	}

	return nil
}

// CommittedIndex returns the current committed index.
func (l *LogStore) CommittedIndex() uint64 {
	return l.committedIndex.Load()
}

// SetCommittedIndex updates the committed index.
func (l *LogStore) SetCommittedIndex(index uint64) {
	l.committedIndex.Store(index)
}

// CommitPosition updates the WAL's commit boundary to the given position.
// This enables ISR-style replication where followers streaming from the WAL
// only see committed entries. Call this when a Raft entry is committed.
func (l *LogStore) CommitPosition(pos walfs.RecordPosition) {
	l.wal.Commit(pos)
}

// Close closes the log store.
func (l *LogStore) Close() error {
	if l.closed.Swap(true) {
		return nil
	}
	close(l.closedCh)
	return nil
}

// Sync syncs the underlying WAL to disk.
func (l *LogStore) Sync() error {
	if l.closed.Load() {
		return ErrClosed
	}
	return l.wal.Sync()
}

// WAL returns the underlying WAL for advanced operations.
func (l *LogStore) WAL() *walfs.WALog {
	return l.wal
}

// DeletionPredicate returns a function that determines if a WAL segment is safe to delete.
// A segment is safe to delete if all its log entries have been compacted (index <= compactedIndex).
//
// The getCompactedIndex function is called each time the predicate is evaluated,
// allowing the compacted index to be fetched dynamically (e.g., from Raft's last snapshot index).
//
// Usage with WAL cleanup:
//
//	predicate := store.DeletionPredicate(func() uint64 {
//	    return raft.LastSnapshotIndex()
//	})
//	for segID, seg := range store.WAL().Segments() {
//	    if predicate(segID) {
//	        seg.MarkForDeletion()
//	    }
//	}
//
// Segments containing only entries at or below the compacted index can be safely removed.
//   - A segment is safe to delete if:
//     a. It exists in the WAL
//     b. It's not the active segment
//     c. It has valid log entries (firstLogIndex > 0 and entryCount > 0)
//     d. All its log entries are at or below the compactedIndex
func (l *LogStore) DeletionPredicate(getCompactedIndex func() uint64) walfs.DeletionPredicate {
	return func(segID walfs.SegmentID) bool {
		l.mu.RLock()
		defer l.mu.RUnlock()

		seg, ok := l.wal.Segments()[segID]
		if !ok {
			return false
		}

		if l.wal.Current() != nil && l.wal.Current().ID() == segID {
			return false
		}

		firstIdx := seg.FirstLogIndex()
		if firstIdx == 0 {
			return false
		}

		entryCount := seg.GetEntryCount()
		if entryCount == 0 {
			return false
		}

		lastIdx := firstIdx + uint64(entryCount) - 1

		compactedIndex := getCompactedIndex()

		// Only Safe to delete if all entries are at or below compacted index
		return lastIdx <= compactedIndex
	}
}

// IsMonotonic implements raft.MonotonicLogStore.
func (l *LogStore) IsMonotonic() bool {
	return true
}

// GetPosition returns the WAL position for a given log index.
func (l *LogStore) GetPosition(index uint64) (walfs.RecordPosition, bool) {
	if l.closed.Load() {
		return walfs.RecordPosition{}, false
	}
	return l.index.Get(index)
}

var _ raft.MonotonicLogStore = (*LogStore)(nil)
var _ raft.LogStore = (*LogStore)(nil)
