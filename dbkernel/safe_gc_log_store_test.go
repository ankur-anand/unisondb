package dbkernel

import (
	"testing"

	"github.com/hashicorp/raft"
	"github.com/stretchr/testify/require"
)

type mockLogStore struct {
	logs         map[uint64]*raft.Log
	firstIndex   uint64
	lastIndex    uint64
	deleteRanges []deleteRangeCall
}

type deleteRangeCall struct {
	min, max uint64
}

func newMockLogStore() *mockLogStore {
	return &mockLogStore{
		logs: make(map[uint64]*raft.Log),
	}
}

func (m *mockLogStore) FirstIndex() (uint64, error) {
	return m.firstIndex, nil
}

func (m *mockLogStore) LastIndex() (uint64, error) {
	return m.lastIndex, nil
}

func (m *mockLogStore) GetLog(index uint64, log *raft.Log) error {
	if l, ok := m.logs[index]; ok {
		*log = *l
		return nil
	}
	return raft.ErrLogNotFound
}

func (m *mockLogStore) StoreLog(log *raft.Log) error {
	m.logs[log.Index] = log
	if log.Index > m.lastIndex {
		m.lastIndex = log.Index
	}
	if m.firstIndex == 0 || log.Index < m.firstIndex {
		m.firstIndex = log.Index
	}
	return nil
}

func (m *mockLogStore) StoreLogs(logs []*raft.Log) error {
	for _, log := range logs {
		if err := m.StoreLog(log); err != nil {
			return err
		}
	}
	return nil
}

func (m *mockLogStore) DeleteRange(min, max uint64) error {
	m.deleteRanges = append(m.deleteRanges, deleteRangeCall{min: min, max: max})
	for i := min; i <= max; i++ {
		delete(m.logs, i)
	}
	if min <= m.firstIndex && max >= m.firstIndex {
		m.firstIndex = max + 1
	}
	return nil
}

func TestSafeGCLogStore_DeleteRange_CapsToSafeIndex(t *testing.T) {
	inner := newMockLogStore()
	holder := &SafeGCIndexHolder{}
	store := NewSafeGCLogStore(inner, holder)

	for i := uint64(1); i <= 100; i++ {
		require.NoError(t, inner.StoreLog(&raft.Log{Index: i, Data: []byte("data")}))
	}

	holder.Set(50)

	err := store.DeleteRange(1, 100)
	require.NoError(t, err)

	require.Len(t, inner.deleteRanges, 1)
	require.Equal(t, uint64(1), inner.deleteRanges[0].min)
	require.Equal(t, uint64(50), inner.deleteRanges[0].max)
}

func TestSafeGCLogStore_DeleteRange_NoCapWhenSafeIndexHigher(t *testing.T) {
	inner := newMockLogStore()
	holder := &SafeGCIndexHolder{}
	store := NewSafeGCLogStore(inner, holder)

	for i := uint64(1); i <= 100; i++ {
		require.NoError(t, inner.StoreLog(&raft.Log{Index: i, Data: []byte("data")}))
	}

	holder.Set(150)

	err := store.DeleteRange(1, 100)
	require.NoError(t, err)

	require.Len(t, inner.deleteRanges, 1)
	require.Equal(t, uint64(1), inner.deleteRanges[0].min)
	require.Equal(t, uint64(100), inner.deleteRanges[0].max)
}

func TestSafeGCLogStore_DeleteRange_NoCapWhenNotSet(t *testing.T) {
	inner := newMockLogStore()
	holder := &SafeGCIndexHolder{}
	store := NewSafeGCLogStore(inner, holder)

	for i := uint64(1); i <= 100; i++ {
		require.NoError(t, inner.StoreLog(&raft.Log{Index: i, Data: []byte("data")}))
	}

	err := store.DeleteRange(1, 100)
	require.NoError(t, err)

	require.Len(t, inner.deleteRanges, 1)
	require.Equal(t, uint64(1), inner.deleteRanges[0].min)
	require.Equal(t, uint64(100), inner.deleteRanges[0].max)
}

func TestSafeGCLogStore_DeleteRange_SkipsWhenMinGreaterThanMax(t *testing.T) {
	inner := newMockLogStore()
	holder := &SafeGCIndexHolder{}
	store := NewSafeGCLogStore(inner, holder)

	for i := uint64(1); i <= 100; i++ {
		require.NoError(t, inner.StoreLog(&raft.Log{Index: i, Data: []byte("data")}))
	}

	holder.Set(10)

	err := store.DeleteRange(50, 100)
	require.NoError(t, err)

	require.Len(t, inner.deleteRanges, 0)
}

func TestSafeGCLogStore_DeleteRange_ExactMatch(t *testing.T) {
	inner := newMockLogStore()
	holder := &SafeGCIndexHolder{}
	store := NewSafeGCLogStore(inner, holder)

	for i := uint64(1); i <= 100; i++ {
		require.NoError(t, inner.StoreLog(&raft.Log{Index: i, Data: []byte("data")}))
	}

	holder.Set(100)

	err := store.DeleteRange(1, 100)
	require.NoError(t, err)

	require.Len(t, inner.deleteRanges, 1)
	require.Equal(t, uint64(1), inner.deleteRanges[0].min)
	require.Equal(t, uint64(100), inner.deleteRanges[0].max)
}

func TestSafeGCLogStore_PassthroughMethods(t *testing.T) {
	inner := newMockLogStore()
	holder := &SafeGCIndexHolder{}
	store := NewSafeGCLogStore(inner, holder)

	require.NoError(t, store.StoreLog(&raft.Log{Index: 1, Data: []byte("one")}))
	require.NoError(t, store.StoreLog(&raft.Log{Index: 2, Data: []byte("two")}))
	require.NoError(t, store.StoreLogs([]*raft.Log{
		{Index: 3, Data: []byte("three")},
		{Index: 4, Data: []byte("four")},
	}))

	first, err := store.FirstIndex()
	require.NoError(t, err)
	require.Equal(t, uint64(1), first)

	last, err := store.LastIndex()
	require.NoError(t, err)
	require.Equal(t, uint64(4), last)

	var log raft.Log
	require.NoError(t, store.GetLog(2, &log))
	require.Equal(t, uint64(2), log.Index)
	require.Equal(t, []byte("two"), log.Data)

	require.Error(t, store.GetLog(999, &log))
}

func TestSafeGCIndexHolder_SetAndGet(t *testing.T) {
	holder := &SafeGCIndexHolder{}

	idx, ok := holder.Get()
	require.False(t, ok)
	require.Equal(t, uint64(0), idx)

	idx, ok = holder.SafeGCIndex()
	require.False(t, ok)
	require.Equal(t, uint64(0), idx)

	holder.Set(42)

	idx, ok = holder.Get()
	require.True(t, ok)
	require.Equal(t, uint64(42), idx)

	idx, ok = holder.SafeGCIndex()
	require.True(t, ok)
	require.Equal(t, uint64(42), idx)

	holder.Set(100)

	idx, ok = holder.Get()
	require.True(t, ok)
	require.Equal(t, uint64(100), idx)
}

func TestSafeGCLogStore_MultipleDeleteRanges(t *testing.T) {
	inner := newMockLogStore()
	holder := &SafeGCIndexHolder{}
	store := NewSafeGCLogStore(inner, holder)

	for i := uint64(1); i <= 200; i++ {
		require.NoError(t, inner.StoreLog(&raft.Log{Index: i, Data: []byte("data")}))
	}

	holder.Set(50)
	require.NoError(t, store.DeleteRange(1, 100))

	holder.Set(80)
	require.NoError(t, store.DeleteRange(51, 100))

	holder.Set(150)
	require.NoError(t, store.DeleteRange(81, 200))

	require.Len(t, inner.deleteRanges, 3)

	require.Equal(t, uint64(1), inner.deleteRanges[0].min)
	require.Equal(t, uint64(50), inner.deleteRanges[0].max)

	require.Equal(t, uint64(51), inner.deleteRanges[1].min)
	require.Equal(t, uint64(80), inner.deleteRanges[1].max)

	require.Equal(t, uint64(81), inner.deleteRanges[2].min)
	require.Equal(t, uint64(150), inner.deleteRanges[2].max)
}

func TestSafeGCLogStore_ImplementsLogStore(t *testing.T) {
	var _ raft.LogStore = (*SafeGCLogStore)(nil)
}

func TestFlushedIndexHolder_SafeGCIndex(t *testing.T) {
	holder := &FlushedIndexHolder{}

	idx, ok := holder.SafeGCIndex()
	require.False(t, ok)
	require.Equal(t, uint64(0), idx)

	holder.Set(100, 5)

	idx, ok = holder.SafeGCIndex()
	require.True(t, ok)
	require.Equal(t, uint64(100), idx)

	index, term := holder.Get()
	require.Equal(t, uint64(100), index)
	require.Equal(t, uint64(5), term)
}

func TestSafeGCLogStore_WithFlushedIndexHolder(t *testing.T) {
	inner := newMockLogStore()
	holder := &FlushedIndexHolder{}
	store := NewSafeGCLogStore(inner, holder)

	for i := uint64(1); i <= 100; i++ {
		require.NoError(t, inner.StoreLog(&raft.Log{Index: i, Data: []byte("data")}))
	}

	holder.Set(60, 2)

	err := store.DeleteRange(1, 100)
	require.NoError(t, err)

	require.Len(t, inner.deleteRanges, 1)
	require.Equal(t, uint64(1), inner.deleteRanges[0].min)
	require.Equal(t, uint64(60), inner.deleteRanges[0].max)
}
