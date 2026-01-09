package raftwalfs

import (
	"sync"
	"testing"
	"time"

	"github.com/ankur-anand/unisondb/pkg/walfs"
	"github.com/hashicorp/raft"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func makeLog(idx, term uint64, data string) *raft.Log {
	return &raft.Log{
		Index:      idx,
		Term:       term,
		Type:       raft.LogCommand,
		Data:       []byte(data),
		AppendedAt: time.Unix(0, int64(idx)),
	}
}

func newTestStore(t *testing.T, opts ...walfs.WALogOptions) *LogStore {
	t.Helper()
	dir := t.TempDir()
	wal, err := walfs.NewWALog(dir, ".wal", opts...)
	require.NoError(t, err)
	store, err := NewLogStore(wal, 0)
	require.NoError(t, err)
	t.Cleanup(func() {
		store.Close()
		wal.Close()
	})
	return store
}

func TestLogStore_EmptyStore(t *testing.T) {
	store := newTestStore(t)

	first, err := store.FirstIndex()
	require.NoError(t, err)
	assert.Equal(t, uint64(0), first)

	last, err := store.LastIndex()
	require.NoError(t, err)
	assert.Equal(t, uint64(0), last)

	var got raft.Log
	err = store.GetLog(1, &got)
	assert.ErrorIs(t, err, raft.ErrLogNotFound)
}

func TestLogStore_AppendAndGet(t *testing.T) {
	store := newTestStore(t, walfs.WithMaxSegmentSize(1024))

	logs := []*raft.Log{
		makeLog(1, 1, "a"),
		makeLog(2, 1, "b"),
		makeLog(3, 1, "c"),
	}
	require.NoError(t, store.StoreLogs(logs))

	first, err := store.FirstIndex()
	require.NoError(t, err)
	assert.Equal(t, uint64(1), first)

	last, err := store.LastIndex()
	require.NoError(t, err)
	assert.Equal(t, uint64(3), last)
	assert.Equal(t, int64(3), store.index.Len())

	var got raft.Log
	require.NoError(t, store.GetLog(2, &got))
	assert.Equal(t, logs[1].Index, got.Index)
	assert.Equal(t, logs[1].Term, got.Term)
	assert.Equal(t, logs[1].Type, got.Type)
	assert.Equal(t, logs[1].Data, got.Data)
	assert.Equal(t, logs[1].AppendedAt, got.AppendedAt)
}

func TestLogStore_StoreLogSingle(t *testing.T) {
	store := newTestStore(t)

	require.NoError(t, store.StoreLog(makeLog(1, 1, "first")))
	require.NoError(t, store.StoreLog(makeLog(2, 1, "second")))

	var got raft.Log
	require.NoError(t, store.GetLog(1, &got))
	assert.Equal(t, []byte("first"), got.Data)

	require.NoError(t, store.GetLog(2, &got))
	assert.Equal(t, []byte("second"), got.Data)
}

func TestLogStore_OverwriteSuffix(t *testing.T) {
	store := newTestStore(t, walfs.WithMaxSegmentSize(2048))

	orig := []*raft.Log{
		makeLog(1, 1, "a"),
		makeLog(2, 1, "b"),
		makeLog(3, 1, "c"),
		makeLog(4, 1, "d"),
		makeLog(5, 1, "e"),
	}
	require.NoError(t, store.StoreLogs(orig))
	overwrite := []*raft.Log{
		makeLog(3, 2, "c2"),
		makeLog(4, 2, "d2"),
	}
	require.NoError(t, store.StoreLogs(overwrite))

	last, err := store.LastIndex()
	require.NoError(t, err)
	assert.Equal(t, uint64(4), last)
	assert.Equal(t, int64(4), store.index.Len())

	var got raft.Log
	require.NoError(t, store.GetLog(3, &got))
	assert.Equal(t, uint64(2), got.Term)
	assert.Equal(t, []byte("c2"), got.Data)

	err = store.GetLog(5, &got)
	assert.ErrorIs(t, err, raft.ErrLogNotFound)
}

func TestLogStore_DeleteRangeSuffix(t *testing.T) {
	store := newTestStore(t)

	logs := []*raft.Log{
		makeLog(1, 1, "a"),
		makeLog(2, 1, "b"),
		makeLog(3, 1, "c"),
		makeLog(4, 1, "d"),
		makeLog(5, 1, "e"),
	}
	require.NoError(t, store.StoreLogs(logs))

	require.NoError(t, store.DeleteRange(4, 5))

	first, _ := store.FirstIndex()
	last, _ := store.LastIndex()
	assert.Equal(t, uint64(1), first)
	assert.Equal(t, uint64(3), last)

	var got raft.Log
	err := store.GetLog(4, &got)
	assert.ErrorIs(t, err, raft.ErrLogNotFound)

	require.NoError(t, store.GetLog(3, &got))
	assert.Equal(t, []byte("c"), got.Data)
}

func TestLogStore_DeleteRangePrefix(t *testing.T) {
	store := newTestStore(t)

	logs := []*raft.Log{
		makeLog(1, 1, "a"),
		makeLog(2, 1, "b"),
		makeLog(3, 1, "c"),
		makeLog(4, 1, "d"),
		makeLog(5, 1, "e"),
	}
	require.NoError(t, store.StoreLogs(logs))
	require.NoError(t, store.DeleteRange(1, 2))

	first, _ := store.FirstIndex()
	last, _ := store.LastIndex()
	assert.Equal(t, uint64(3), first)
	assert.Equal(t, uint64(5), last)

	var got raft.Log
	err := store.GetLog(1, &got)
	assert.ErrorIs(t, err, raft.ErrLogNotFound)

	require.NoError(t, store.GetLog(3, &got))
	assert.Equal(t, []byte("c"), got.Data)
}

func TestLogStore_DeleteRangeAll(t *testing.T) {
	store := newTestStore(t)

	logs := []*raft.Log{
		makeLog(1, 1, "a"),
		makeLog(2, 1, "b"),
	}
	require.NoError(t, store.StoreLogs(logs))

	require.NoError(t, store.DeleteRange(1, 2))

	first, _ := store.FirstIndex()
	last, _ := store.LastIndex()
	assert.Equal(t, uint64(0), first)
	assert.Equal(t, uint64(0), last)
	assert.Equal(t, int64(0), store.index.Len())
}

func TestLogStore_DeleteRangeEmptyNoop(t *testing.T) {
	store := newTestStore(t)
	assert.NoError(t, store.DeleteRange(1, 10))
}

func TestLogStore_DeleteRangeInvalidNoop(t *testing.T) {
	store := newTestStore(t)
	assert.NoError(t, store.DeleteRange(10, 5))
}

func TestLogStore_NonContiguousBatchFails(t *testing.T) {
	store := newTestStore(t)

	err := store.StoreLogs([]*raft.Log{
		makeLog(1, 1, "a"),
		makeLog(3, 1, "b"),
	})
	assert.Error(t, err)
}

func TestLogStore_GapInAppendFails(t *testing.T) {
	store := newTestStore(t)

	require.NoError(t, store.StoreLogs([]*raft.Log{
		makeLog(1, 1, "a"),
		makeLog(2, 1, "b"),
	}))

	err := store.StoreLog(makeLog(5, 1, "gap"))
	assert.Error(t, err)
}

func TestLogStore_StoreBeforeFirstFails(t *testing.T) {
	store := newTestStore(t)

	require.NoError(t, store.StoreLogs([]*raft.Log{
		makeLog(5, 1, "a"),
		makeLog(6, 1, "b"),
	}))

	err := store.StoreLog(makeLog(4, 1, "older"))
	assert.ErrorIs(t, err, raft.ErrLogNotFound)
}

func TestLogStore_GetLogNotFound(t *testing.T) {
	store := newTestStore(t)

	require.NoError(t, store.StoreLog(makeLog(5, 1, "a")))

	var got raft.Log
	err := store.GetLog(4, &got)
	assert.ErrorIs(t, err, raft.ErrLogNotFound)

	err = store.GetLog(6, &got)
	assert.ErrorIs(t, err, raft.ErrLogNotFound)
}

func TestLogStore_CommittedIndex(t *testing.T) {
	store := newTestStore(t)

	assert.Equal(t, uint64(0), store.CommittedIndex())

	store.SetCommittedIndex(42)
	assert.Equal(t, uint64(42), store.CommittedIndex())
}

func TestLogStore_Sync(t *testing.T) {
	store := newTestStore(t)

	require.NoError(t, store.StoreLog(makeLog(1, 1, "data")))
	require.NoError(t, store.Sync())
}

func TestLogStore_Close(t *testing.T) {
	store := newTestStore(t)

	require.NoError(t, store.StoreLog(makeLog(1, 1, "data")))
	require.NoError(t, store.Close())

	_, err := store.FirstIndex()
	assert.ErrorIs(t, err, ErrClosed)

	_, err = store.LastIndex()
	assert.ErrorIs(t, err, ErrClosed)

	var got raft.Log
	err = store.GetLog(1, &got)
	assert.ErrorIs(t, err, ErrClosed)

	err = store.StoreLog(makeLog(2, 1, "data"))
	assert.ErrorIs(t, err, ErrClosed)

	err = store.Sync()
	assert.ErrorIs(t, err, ErrClosed)

	require.NoError(t, store.Close())
}

func TestLogStore_Recovery(t *testing.T) {
	dir := t.TempDir()

	wal1, err := walfs.NewWALog(dir, ".wal", walfs.WithMaxSegmentSize(2048))
	require.NoError(t, err)

	store1, err := NewLogStore(wal1, 0)
	require.NoError(t, err)

	require.NoError(t, store1.StoreLogs([]*raft.Log{
		makeLog(1, 1, "a"),
		makeLog(2, 1, "b"),
		makeLog(3, 1, "c"),
	}))

	store1.Close()
	wal1.Close()

	wal2, err := walfs.NewWALog(dir, ".wal", walfs.WithMaxSegmentSize(2048))
	require.NoError(t, err)
	defer wal2.Close()

	store2, err := NewLogStore(wal2, 0)
	require.NoError(t, err)
	defer store2.Close()

	first, err := store2.FirstIndex()
	require.NoError(t, err)
	assert.Equal(t, uint64(1), first)

	last, err := store2.LastIndex()
	require.NoError(t, err)
	assert.Equal(t, uint64(3), last)

	assert.Equal(t, int64(3), store2.index.Len())

	var got raft.Log
	require.NoError(t, store2.GetLog(2, &got))
	assert.Equal(t, []byte("b"), got.Data)
}

func TestLogStore_RecoveryWithCommittedIndex(t *testing.T) {
	dir := t.TempDir()

	wal, err := walfs.NewWALog(dir, ".wal")
	require.NoError(t, err)
	defer wal.Close()

	store, err := NewLogStore(wal, 100)
	require.NoError(t, err)
	defer store.Close()

	assert.Equal(t, uint64(100), store.CommittedIndex())
}

func TestLogStore_ConcurrentReads(t *testing.T) {
	store := newTestStore(t)

	var batch []*raft.Log
	for i := 1; i <= 1000; i++ {
		batch = append(batch, makeLog(uint64(i), 1, "data"))
	}
	require.NoError(t, store.StoreLogs(batch))

	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			var got raft.Log
			for j := 0; j < 100; j++ {
				idx := uint64((j % 1000) + 1)
				if err := store.GetLog(idx, &got); err != nil {
					t.Errorf("GetLog(%d) failed: %v", idx, err)
					return
				}
			}
		}()
	}
	wg.Wait()
}

func TestLogStore_ConcurrentWriteRead(t *testing.T) {
	store := newTestStore(t)

	require.NoError(t, store.StoreLogs([]*raft.Log{
		makeLog(1, 1, "initial"),
	}))

	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 2; i <= 100; i++ {
			if err := store.StoreLog(makeLog(uint64(i), 1, "data")); err != nil {
				t.Errorf("StoreLog(%d) failed: %v", i, err)
				return
			}
		}
	}()

	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			var got raft.Log
			for j := 0; j < 50; j++ {
				last, _ := store.LastIndex()
				if last > 0 {
					idx := uint64(1)
					_ = store.GetLog(idx, &got)
				}
				time.Sleep(time.Microsecond)
			}
		}()
	}

	wg.Wait()
}

func TestLogStore_LargePayload(t *testing.T) {
	store := newTestStore(t, walfs.WithMaxSegmentSize(1<<20))

	payload := make([]byte, 100*1024)
	for i := range payload {
		payload[i] = byte(i % 256)
	}

	log := &raft.Log{
		Index: 1,
		Term:  1,
		Type:  raft.LogCommand,
		Data:  payload,
	}
	require.NoError(t, store.StoreLog(log))

	var got raft.Log
	require.NoError(t, store.GetLog(1, &got))
	assert.Equal(t, payload, got.Data)
}

func TestLogStore_AllLogTypes(t *testing.T) {
	store := newTestStore(t)

	logs := []*raft.Log{
		{Index: 1, Term: 1, Type: raft.LogCommand, Data: []byte("cmd")},
		{Index: 2, Term: 1, Type: raft.LogNoop},
		{Index: 3, Term: 1, Type: raft.LogBarrier},
		{Index: 4, Term: 1, Type: raft.LogConfiguration, Data: []byte("config")},
	}
	require.NoError(t, store.StoreLogs(logs))

	for _, expected := range logs {
		var got raft.Log
		require.NoError(t, store.GetLog(expected.Index, &got))
		assert.Equal(t, expected.Type, got.Type)
		assert.Equal(t, expected.Data, got.Data)
	}
}

func TestLogStore_WithExtensions(t *testing.T) {
	store := newTestStore(t)

	log := &raft.Log{
		Index:      1,
		Term:       1,
		Type:       raft.LogCommand,
		Data:       []byte("data"),
		Extensions: []byte("extensions-data"),
	}
	require.NoError(t, store.StoreLog(log))

	var got raft.Log
	require.NoError(t, store.GetLog(1, &got))
	assert.Equal(t, log.Data, got.Data)
	assert.Equal(t, log.Extensions, got.Extensions)
}

func TestLogStore_AppendAfterOverwrite(t *testing.T) {
	store := newTestStore(t)

	require.NoError(t, store.StoreLogs([]*raft.Log{
		makeLog(1, 1, "a"),
		makeLog(2, 1, "b"),
		makeLog(3, 1, "c"),
		makeLog(4, 1, "d"),
	}))

	require.NoError(t, store.StoreLogs([]*raft.Log{
		makeLog(3, 2, "c'"),
		makeLog(4, 2, "d'"),
		makeLog(5, 2, "e"),
	}))

	first, _ := store.FirstIndex()
	last, _ := store.LastIndex()
	assert.Equal(t, uint64(1), first)
	assert.Equal(t, uint64(5), last)

	var got raft.Log
	require.NoError(t, store.GetLog(3, &got))
	assert.Equal(t, []byte("c'"), got.Data)

	assert.ErrorIs(t, store.GetLog(6, &got), raft.ErrLogNotFound)
}

func TestLogStore_MultipleSegments(t *testing.T) {
	store := newTestStore(t, walfs.WithMaxSegmentSize(512))

	for i := 1; i <= 100; i++ {
		require.NoError(t, store.StoreLog(makeLog(uint64(i), 1, "data-data-data")))
	}

	first, _ := store.FirstIndex()
	last, _ := store.LastIndex()
	assert.Equal(t, uint64(1), first)
	assert.Equal(t, uint64(100), last)

	var got raft.Log
	require.NoError(t, store.GetLog(50, &got))
	assert.Equal(t, uint64(50), got.Index)

	require.NoError(t, store.GetLog(1, &got))
	assert.Equal(t, uint64(1), got.Index)

	require.NoError(t, store.GetLog(100, &got))
	assert.Equal(t, uint64(100), got.Index)
}

func TestLogStore_InterfaceCompliance(t *testing.T) {
	store := newTestStore(t)

	var _ raft.LogStore = store
}

func TestShardedIndex_Basic(t *testing.T) {
	idx := walfs.NewShardedIndex()

	idx.Set(1, walfs.RecordPosition{SegmentID: 1, Offset: 100})
	idx.Set(2, walfs.RecordPosition{SegmentID: 1, Offset: 200})

	pos, ok := idx.Get(1)
	assert.True(t, ok)
	assert.Equal(t, walfs.SegmentID(1), pos.SegmentID)
	assert.Equal(t, int64(100), pos.Offset)

	pos, ok = idx.Get(2)
	assert.True(t, ok)
	assert.Equal(t, int64(200), pos.Offset)

	_, ok = idx.Get(3)
	assert.False(t, ok)

	assert.Equal(t, int64(2), idx.Len())
}

func TestShardedIndex_DeleteRange(t *testing.T) {
	idx := walfs.NewShardedIndex()

	for i := uint64(1); i <= 10; i++ {
		idx.Set(i, walfs.RecordPosition{SegmentID: 1, Offset: int64(i * 100)})
	}

	deleted := idx.DeleteRange(3, 7)
	assert.Equal(t, int64(5), deleted)
	assert.Equal(t, int64(5), idx.Len())

	_, ok := idx.Get(5)
	assert.False(t, ok)

	_, ok = idx.Get(2)
	assert.True(t, ok)

	_, ok = idx.Get(8)
	assert.True(t, ok)
}

func TestShardedIndex_GetFirstLast(t *testing.T) {
	idx := walfs.NewShardedIndex()

	_, _, ok := idx.GetFirstLast()
	assert.False(t, ok)

	idx.Set(5, walfs.RecordPosition{})
	idx.Set(10, walfs.RecordPosition{})
	idx.Set(3, walfs.RecordPosition{})

	first, last, ok := idx.GetFirstLast()
	assert.True(t, ok)
	assert.Equal(t, uint64(3), first)
	assert.Equal(t, uint64(10), last)
}

func TestShardedIndex_SetBatch(t *testing.T) {
	idx := walfs.NewShardedIndex()

	entries := []walfs.IndexEntry{
		{Index: 1, Pos: walfs.RecordPosition{SegmentID: 1, Offset: 100}},
		{Index: 2, Pos: walfs.RecordPosition{SegmentID: 1, Offset: 200}},
		{Index: 3, Pos: walfs.RecordPosition{SegmentID: 1, Offset: 300}},
	}

	idx.SetBatch(entries)
	assert.Equal(t, int64(3), idx.Len())

	for _, e := range entries {
		pos, ok := idx.Get(e.Index)
		assert.True(t, ok)
		assert.Equal(t, e.Pos.Offset, pos.Offset)
	}
}

func TestLogStore_EmptyBatch(t *testing.T) {
	store := newTestStore(t)

	require.NoError(t, store.StoreLogs(nil))
	require.NoError(t, store.StoreLogs([]*raft.Log{}))

	first, _ := store.FirstIndex()
	last, _ := store.LastIndex()
	assert.Equal(t, uint64(0), first)
	assert.Equal(t, uint64(0), last)
}

func TestLogStore_StartFromNonOne(t *testing.T) {
	store := newTestStore(t)

	logs := []*raft.Log{
		makeLog(100, 5, "after-snapshot"),
		makeLog(101, 5, "second"),
		makeLog(102, 5, "third"),
	}
	require.NoError(t, store.StoreLogs(logs))

	first, _ := store.FirstIndex()
	last, _ := store.LastIndex()
	assert.Equal(t, uint64(100), first)
	assert.Equal(t, uint64(102), last)

	var got raft.Log
	require.NoError(t, store.GetLog(100, &got))
	assert.Equal(t, []byte("after-snapshot"), got.Data)
}

func TestLogStore_HighTermNumbers(t *testing.T) {
	store := newTestStore(t)

	log := &raft.Log{
		Index: 1,
		Term:  ^uint64(0) - 1,
		Type:  raft.LogCommand,
		Data:  []byte("data"),
	}
	require.NoError(t, store.StoreLog(log))

	var got raft.Log
	require.NoError(t, store.GetLog(1, &got))
	assert.Equal(t, log.Term, got.Term)
}

func TestLogStore_HighIndexNumbers(t *testing.T) {
	store := newTestStore(t)

	log := makeLog(1<<40, 1, "high-index")
	require.NoError(t, store.StoreLog(log))

	first, _ := store.FirstIndex()
	last, _ := store.LastIndex()
	assert.Equal(t, uint64(1<<40), first)
	assert.Equal(t, uint64(1<<40), last)

	var got raft.Log
	require.NoError(t, store.GetLog(1<<40, &got))
	assert.Equal(t, []byte("high-index"), got.Data)
}

func TestLogStore_EmptyData(t *testing.T) {
	store := newTestStore(t)

	log := &raft.Log{
		Index: 1,
		Term:  1,
		Type:  raft.LogNoop,
		Data:  nil,
	}
	require.NoError(t, store.StoreLog(log))

	var got raft.Log
	require.NoError(t, store.GetLog(1, &got))
	assert.Nil(t, got.Data)
	assert.Equal(t, raft.LogNoop, got.Type)
}

func TestLogStore_ZeroTimestamp(t *testing.T) {
	store := newTestStore(t)

	log := &raft.Log{
		Index:      1,
		Term:       1,
		Type:       raft.LogCommand,
		Data:       []byte("data"),
		AppendedAt: time.Time{},
	}
	require.NoError(t, store.StoreLog(log))

	var got raft.Log
	require.NoError(t, store.GetLog(1, &got))
	assert.True(t, got.AppendedAt.IsZero())
}

func TestLogStore_FutureTimestamp(t *testing.T) {
	store := newTestStore(t)

	futureTime := time.Date(2100, 1, 1, 0, 0, 0, 0, time.UTC)
	log := &raft.Log{
		Index:      1,
		Term:       1,
		Type:       raft.LogCommand,
		Data:       []byte("future"),
		AppendedAt: futureTime,
	}
	require.NoError(t, store.StoreLog(log))

	var got raft.Log
	require.NoError(t, store.GetLog(1, &got))
	assert.Equal(t, futureTime.UnixNano(), got.AppendedAt.UnixNano())
}

func TestLogStore_MultipleOverwrites(t *testing.T) {
	store := newTestStore(t)

	require.NoError(t, store.StoreLogs([]*raft.Log{
		makeLog(1, 1, "a"),
		makeLog(2, 1, "b"),
		makeLog(3, 1, "c"),
	}))

	require.NoError(t, store.StoreLogs([]*raft.Log{
		makeLog(2, 2, "b2"),
		makeLog(3, 2, "c2"),
	}))

	require.NoError(t, store.StoreLogs([]*raft.Log{
		makeLog(2, 3, "b3"),
	}))

	last, _ := store.LastIndex()
	assert.Equal(t, uint64(2), last)

	var got raft.Log
	require.NoError(t, store.GetLog(2, &got))
	assert.Equal(t, uint64(3), got.Term)
	assert.Equal(t, []byte("b3"), got.Data)

	err := store.GetLog(3, &got)
	assert.ErrorIs(t, err, raft.ErrLogNotFound)
}

func TestLogStore_OverwriteAtFirst(t *testing.T) {
	store := newTestStore(t)

	require.NoError(t, store.StoreLogs([]*raft.Log{
		makeLog(1, 1, "a"),
		makeLog(2, 1, "b"),
	}))

	require.NoError(t, store.StoreLogs([]*raft.Log{
		makeLog(1, 2, "a2"),
	}))

	first, _ := store.FirstIndex()
	last, _ := store.LastIndex()
	assert.Equal(t, uint64(1), first)
	assert.Equal(t, uint64(1), last)

	var got raft.Log
	require.NoError(t, store.GetLog(1, &got))
	assert.Equal(t, uint64(2), got.Term)

	err := store.GetLog(2, &got)
	assert.ErrorIs(t, err, raft.ErrLogNotFound)
}

func TestLogStore_DeleteRangeMiddle(t *testing.T) {
	store := newTestStore(t)

	require.NoError(t, store.StoreLogs([]*raft.Log{
		makeLog(1, 1, "a"),
		makeLog(2, 1, "b"),
		makeLog(3, 1, "c"),
		makeLog(4, 1, "d"),
		makeLog(5, 1, "e"),
	}))

	require.NoError(t, store.DeleteRange(3, 4))

	_, ok := store.index.Get(3)
	assert.False(t, ok)
	_, ok = store.index.Get(4)
	assert.False(t, ok)
}

func TestLogStore_DeleteRangeOneEntry(t *testing.T) {
	store := newTestStore(t)

	require.NoError(t, store.StoreLogs([]*raft.Log{
		makeLog(1, 1, "a"),
		makeLog(2, 1, "b"),
		makeLog(3, 1, "c"),
	}))

	require.NoError(t, store.DeleteRange(3, 3))

	last, _ := store.LastIndex()
	assert.Equal(t, uint64(2), last)
}

func TestLogStore_GetLogBoundaries(t *testing.T) {
	store := newTestStore(t)

	require.NoError(t, store.StoreLogs([]*raft.Log{
		makeLog(10, 1, "first"),
		makeLog(11, 1, "middle"),
		makeLog(12, 1, "last"),
	}))

	var got raft.Log

	require.NoError(t, store.GetLog(10, &got))
	assert.Equal(t, []byte("first"), got.Data)

	require.NoError(t, store.GetLog(12, &got))
	assert.Equal(t, []byte("last"), got.Data)

	err := store.GetLog(9, &got)
	assert.ErrorIs(t, err, raft.ErrLogNotFound)

	err = store.GetLog(13, &got)
	assert.ErrorIs(t, err, raft.ErrLogNotFound)

	err = store.GetLog(1, &got)
	assert.ErrorIs(t, err, raft.ErrLogNotFound)

	err = store.GetLog(1000, &got)
	assert.ErrorIs(t, err, raft.ErrLogNotFound)

	err = store.GetLog(0, &got)
	assert.ErrorIs(t, err, raft.ErrLogNotFound)
}

func TestLogStore_StoreLogsEmptyAfterData(t *testing.T) {
	store := newTestStore(t)

	require.NoError(t, store.StoreLog(makeLog(1, 1, "data")))

	require.NoError(t, store.StoreLogs([]*raft.Log{}))

	first, _ := store.FirstIndex()
	last, _ := store.LastIndex()
	assert.Equal(t, uint64(1), first)
	assert.Equal(t, uint64(1), last)
}

func TestLogStore_IndexConsistencyAfterOperations(t *testing.T) {
	store := newTestStore(t)

	require.NoError(t, store.StoreLogs([]*raft.Log{
		makeLog(1, 1, "a"),
		makeLog(2, 1, "b"),
		makeLog(3, 1, "c"),
		makeLog(4, 1, "d"),
		makeLog(5, 1, "e"),
	}))

	assert.Equal(t, int64(5), store.index.Len())

	require.NoError(t, store.StoreLogs([]*raft.Log{
		makeLog(3, 2, "c2"),
		makeLog(4, 2, "d2"),
	}))

	assert.Equal(t, int64(4), store.index.Len())

	require.NoError(t, store.DeleteRange(1, 1))
	assert.Equal(t, int64(3), store.index.Len())

	for _, idx := range []uint64{2, 3, 4} {
		_, ok := store.index.Get(idx)
		assert.True(t, ok, "index %d should exist", idx)
	}

	_, ok := store.index.Get(1)
	assert.False(t, ok, "index 1 should be deleted")

	_, ok = store.index.Get(5)
	assert.False(t, ok, "index 5 should be deleted")
}

func TestLogStore_WALAccessor(t *testing.T) {
	store := newTestStore(t)

	wal := store.WAL()
	assert.NotNil(t, wal)
}

func TestLogStore_RecoveryAfterOverwrite(t *testing.T) {
	dir := t.TempDir()

	wal1, err := walfs.NewWALog(dir, ".wal", walfs.WithMaxSegmentSize(2048))
	require.NoError(t, err)
	store1, err := NewLogStore(wal1, 0)
	require.NoError(t, err)

	require.NoError(t, store1.StoreLogs([]*raft.Log{
		makeLog(1, 1, "a"),
		makeLog(2, 1, "b"),
		makeLog(3, 1, "c"),
	}))

	require.NoError(t, store1.StoreLogs([]*raft.Log{
		makeLog(2, 2, "b2"),
		makeLog(3, 2, "c2"),
		makeLog(4, 2, "d"),
	}))

	store1.Close()
	wal1.Close()

	wal2, err := walfs.NewWALog(dir, ".wal", walfs.WithMaxSegmentSize(2048))
	require.NoError(t, err)
	defer wal2.Close()

	store2, err := NewLogStore(wal2, 0)
	require.NoError(t, err)
	defer store2.Close()

	first, _ := store2.FirstIndex()
	last, _ := store2.LastIndex()
	assert.Equal(t, uint64(1), first)
	assert.Equal(t, uint64(4), last)

	var got raft.Log
	require.NoError(t, store2.GetLog(2, &got))
	assert.Equal(t, uint64(2), got.Term)
	assert.Equal(t, []byte("b2"), got.Data)
}

func TestLogStore_ManySmallLogs(t *testing.T) {
	store := newTestStore(t, walfs.WithMaxSegmentSize(4096))

	for i := 1; i <= 500; i++ {
		require.NoError(t, store.StoreLog(makeLog(uint64(i), 1, "x")))
	}

	first, _ := store.FirstIndex()
	last, _ := store.LastIndex()
	assert.Equal(t, uint64(1), first)
	assert.Equal(t, uint64(500), last)

	var got raft.Log
	for _, idx := range []uint64{1, 50, 100, 250, 500} {
		require.NoError(t, store.GetLog(idx, &got))
		assert.Equal(t, idx, got.Index)
	}
}

func TestShardedIndex_ClearAndReuse(t *testing.T) {
	idx := walfs.NewShardedIndex()

	for i := uint64(1); i <= 100; i++ {
		idx.Set(i, walfs.RecordPosition{SegmentID: 1, Offset: int64(i)})
	}
	assert.Equal(t, int64(100), idx.Len())

	idx.Clear()
	assert.Equal(t, int64(0), idx.Len())

	for i := uint64(1); i <= 50; i++ {
		idx.Set(i, walfs.RecordPosition{SegmentID: 2, Offset: int64(i)})
	}
	assert.Equal(t, int64(50), idx.Len())

	pos, ok := idx.Get(25)
	assert.True(t, ok)
	assert.Equal(t, walfs.SegmentID(2), pos.SegmentID)
}

func TestShardedIndex_IsCurrentEntry(t *testing.T) {
	idx := walfs.NewShardedIndex()

	idx.Set(1, walfs.RecordPosition{SegmentID: 1, Offset: 100})

	assert.True(t, idx.IsCurrentEntry(1, 1, 100))
	assert.False(t, idx.IsCurrentEntry(1, 1, 200))
	assert.False(t, idx.IsCurrentEntry(1, 2, 100))
	assert.False(t, idx.IsCurrentEntry(2, 1, 100))
}

func TestShardedIndex_UpdateExisting(t *testing.T) {
	idx := walfs.NewShardedIndex()

	idx.Set(1, walfs.RecordPosition{SegmentID: 1, Offset: 100})
	assert.Equal(t, int64(1), idx.Len())

	idx.Set(1, walfs.RecordPosition{SegmentID: 2, Offset: 200})
	assert.Equal(t, int64(1), idx.Len())

	pos, _ := idx.Get(1)
	assert.Equal(t, walfs.SegmentID(2), pos.SegmentID)
	assert.Equal(t, int64(200), pos.Offset)
}

func TestShardedIndex_Range(t *testing.T) {
	idx := walfs.NewShardedIndex()

	for i := uint64(1); i <= 10; i++ {
		idx.Set(i, walfs.RecordPosition{SegmentID: 1, Offset: int64(i * 100)})
	}

	var visited []uint64
	idx.Range(func(index uint64, pos walfs.RecordPosition) bool {
		visited = append(visited, index)
		return true
	})

	assert.Len(t, visited, 10)
}

func TestShardedIndex_RangeEarlyStop(t *testing.T) {
	idx := walfs.NewShardedIndex()

	for i := uint64(1); i <= 100; i++ {
		idx.Set(i, walfs.RecordPosition{SegmentID: 1, Offset: int64(i)})
	}

	count := 0
	idx.Range(func(index uint64, pos walfs.RecordPosition) bool {
		count++
		return count < 5
	})

	assert.Equal(t, 5, count)
}

func TestShardedIndex_EmptyBatchOperations(t *testing.T) {
	idx := walfs.NewShardedIndex()

	idx.SetBatch(nil)
	assert.Equal(t, int64(0), idx.Len())

	idx.SetBatch([]walfs.IndexEntry{})
	assert.Equal(t, int64(0), idx.Len())

	deleted := idx.DeleteRange(1, 100)
	assert.Equal(t, int64(0), deleted)
}

func TestShardedIndex_DeleteNonExistent(t *testing.T) {
	idx := walfs.NewShardedIndex()

	idx.Set(1, walfs.RecordPosition{SegmentID: 1, Offset: 100})

	idx.Delete(999)
	assert.Equal(t, int64(1), idx.Len())

	idx.Delete(1)
	assert.Equal(t, int64(0), idx.Len())
}

func TestShardedIndex_LenVsLenSlow(t *testing.T) {
	idx := walfs.NewShardedIndex()

	for i := uint64(0); i < 1000; i++ {
		idx.Set(i, walfs.RecordPosition{SegmentID: 1, Offset: int64(i)})
	}

	assert.Equal(t, idx.Len(), idx.LenSlow())

	idx.DeleteRange(100, 199)
	assert.Equal(t, idx.Len(), idx.LenSlow())
	assert.Equal(t, int64(900), idx.Len())
}

func TestDecodeRaftLog(t *testing.T) {
	t.Run("basic decode", func(t *testing.T) {
		original := &raft.Log{
			Index:      12345,
			Term:       99,
			Type:       raft.LogCommand,
			Data:       []byte("hello world"),
			Extensions: []byte("ext-data"),
			AppendedAt: time.Unix(1234567890, 123456789),
		}

		encoded, err := BinaryCodecV1{}.Encode(original)
		require.NoError(t, err)

		decoded, err := BinaryCodecV1{}.Decode(encoded)
		require.NoError(t, err)

		assert.Equal(t, original.Index, decoded.Index)
		assert.Equal(t, original.Term, decoded.Term)
		assert.Equal(t, original.Type, decoded.Type)
		assert.Equal(t, original.Data, decoded.Data)
		assert.Equal(t, original.Extensions, decoded.Extensions)
		assert.Equal(t, original.AppendedAt.UnixNano(), decoded.AppendedAt.UnixNano())
	})

	t.Run("empty data and extensions", func(t *testing.T) {
		original := &raft.Log{
			Index: 1,
			Term:  1,
			Type:  raft.LogNoop,
		}

		encoded, err := BinaryCodecV1{}.Encode(original)
		require.NoError(t, err)

		decoded, err := BinaryCodecV1{}.Decode(encoded)
		require.NoError(t, err)

		assert.Equal(t, original.Index, decoded.Index)
		assert.Equal(t, original.Term, decoded.Term)
		assert.Nil(t, decoded.Data)
		assert.Nil(t, decoded.Extensions)
	})

	t.Run("large payload", func(t *testing.T) {
		data := make([]byte, 100*1024)
		for i := range data {
			data[i] = byte(i % 256)
		}

		original := &raft.Log{
			Index: 999,
			Term:  50,
			Type:  raft.LogCommand,
			Data:  data,
		}

		encoded, err := BinaryCodecV1{}.Encode(original)
		require.NoError(t, err)

		decoded, err := BinaryCodecV1{}.Decode(encoded)
		require.NoError(t, err)

		assert.Equal(t, original.Data, decoded.Data)
	})

	t.Run("zero timestamp", func(t *testing.T) {
		original := &raft.Log{
			Index:      1,
			Term:       1,
			Type:       raft.LogCommand,
			Data:       []byte("data"),
			AppendedAt: time.Time{},
		}

		encoded, err := BinaryCodecV1{}.Encode(original)
		require.NoError(t, err)

		decoded, err := BinaryCodecV1{}.Decode(encoded)
		require.NoError(t, err)

		assert.True(t, decoded.AppendedAt.IsZero())
	})

	t.Run("all log types", func(t *testing.T) {
		types := []raft.LogType{
			raft.LogCommand,
			raft.LogNoop,
			raft.LogBarrier,
			raft.LogConfiguration,
		}

		for _, lt := range types {
			original := &raft.Log{Index: 1, Term: 1, Type: lt, Data: []byte("x")}
			encoded, _ := BinaryCodecV1{}.Encode(original)
			decoded, err := BinaryCodecV1{}.Decode(encoded)
			require.NoError(t, err)
			assert.Equal(t, lt, decoded.Type)
		}
	})

	t.Run("roundtrip various inputs", func(t *testing.T) {
		testCases := []*raft.Log{
			{Index: 1, Term: 1, Type: raft.LogCommand, Data: []byte("small")},
			{Index: 999999, Term: 888, Type: raft.LogNoop},
			{Index: 1, Term: 1, Type: raft.LogCommand, Data: make([]byte, 4096), Extensions: []byte("ext")},
			{Index: ^uint64(0) - 1, Term: ^uint64(0) - 1, Type: raft.LogBarrier},
		}

		for i, original := range testCases {
			encoded, err := BinaryCodecV1{}.Encode(original)
			require.NoError(t, err, "case %d encode", i)

			decoded, err := BinaryCodecV1{}.Decode(encoded)
			require.NoError(t, err, "case %d decode", i)

			assert.Equal(t, original.Index, decoded.Index, "case %d Index", i)
			assert.Equal(t, original.Term, decoded.Term, "case %d Term", i)
			assert.Equal(t, original.Type, decoded.Type, "case %d Type", i)
			assert.Equal(t, original.Data, decoded.Data, "case %d Data", i)
			assert.Equal(t, original.Extensions, decoded.Extensions, "case %d Extensions", i)
		}
	})

	t.Run("error on short data", func(t *testing.T) {
		_, err := BinaryCodecV1{}.Decode([]byte{1, 2, 3})
		assert.Error(t, err)
	})
}
