package walfs

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestShardedIndex_NewShardedIndex(t *testing.T) {
	idx := NewShardedIndex()

	assert.NotNil(t, idx)
	assert.Equal(t, int64(0), idx.Len())
	assert.Equal(t, defaultShardCount, idx.count)
	assert.Equal(t, uint64(defaultShardCount-1), idx.shardMask)
}

func TestShardedIndex_SetAndGet(t *testing.T) {
	idx := NewShardedIndex()

	t.Run("set and get single entry", func(t *testing.T) {
		pos := RecordPosition{SegmentID: 1, Offset: 100}
		idx.Set(1, pos)

		got, ok := idx.Get(1)
		assert.True(t, ok)
		assert.Equal(t, pos.SegmentID, got.SegmentID)
		assert.Equal(t, pos.Offset, got.Offset)
	})

	t.Run("get non-existent entry", func(t *testing.T) {
		_, ok := idx.Get(999)
		assert.False(t, ok)
	})

	t.Run("overwrite existing entry", func(t *testing.T) {
		idx.Set(1, RecordPosition{SegmentID: 1, Offset: 100})
		initialLen := idx.Len()

		idx.Set(1, RecordPosition{SegmentID: 2, Offset: 200})

		assert.Equal(t, initialLen, idx.Len())

		got, ok := idx.Get(1)
		assert.True(t, ok)
		assert.Equal(t, SegmentID(2), got.SegmentID)
		assert.Equal(t, int64(200), got.Offset)
	})
}

func TestShardedIndex_SetDistributesAcrossShards(t *testing.T) {
	idx := NewShardedIndex()

	for i := uint64(0); i < 1000; i++ {
		idx.Set(i, RecordPosition{SegmentID: SegmentID(i), Offset: int64(i * 100)})
	}

	assert.Equal(t, int64(1000), idx.Len())

	for i := uint64(0); i < 1000; i++ {
		pos, ok := idx.Get(i)
		assert.True(t, ok, "entry %d should exist", i)
		assert.Equal(t, SegmentID(i), pos.SegmentID)
		assert.Equal(t, int64(i*100), pos.Offset)
	}
}

func TestShardedIndex_Delete(t *testing.T) {
	idx := NewShardedIndex()

	t.Run("delete existing entry", func(t *testing.T) {
		idx.Set(1, RecordPosition{SegmentID: 1, Offset: 100})
		assert.Equal(t, int64(1), idx.Len())

		idx.Delete(1)
		assert.Equal(t, int64(0), idx.Len())

		_, ok := idx.Get(1)
		assert.False(t, ok)
	})

	t.Run("delete non-existent entry", func(t *testing.T) {
		idx.Set(1, RecordPosition{SegmentID: 1, Offset: 100})
		initialLen := idx.Len()

		idx.Delete(999)
		assert.Equal(t, initialLen, idx.Len())
	})

	t.Run("double delete", func(t *testing.T) {
		idx.Set(5, RecordPosition{SegmentID: 1, Offset: 100})

		idx.Delete(5)
		assert.Equal(t, int64(1), idx.Len())

		idx.Delete(5)
		assert.Equal(t, int64(1), idx.Len())
	})
}

func TestShardedIndex_DeleteRange_Comprehensive(t *testing.T) {
	t.Run("delete range in middle", func(t *testing.T) {
		idx := NewShardedIndex()
		for i := uint64(1); i <= 10; i++ {
			idx.Set(i, RecordPosition{SegmentID: 1, Offset: int64(i * 100)})
		}

		deleted := idx.DeleteRange(3, 7)
		assert.Equal(t, int64(5), deleted)
		assert.Equal(t, int64(5), idx.Len())

		for i := uint64(3); i <= 7; i++ {
			_, ok := idx.Get(i)
			assert.False(t, ok, "entry %d should be deleted", i)
		}

		for _, i := range []uint64{1, 2, 8, 9, 10} {
			_, ok := idx.Get(i)
			assert.True(t, ok, "entry %d should exist", i)
		}
	})

	t.Run("delete range at start", func(t *testing.T) {
		idx := NewShardedIndex()
		for i := uint64(1); i <= 10; i++ {
			idx.Set(i, RecordPosition{SegmentID: 1, Offset: int64(i)})
		}

		deleted := idx.DeleteRange(1, 5)
		assert.Equal(t, int64(5), deleted)
		assert.Equal(t, int64(5), idx.Len())
	})

	t.Run("delete range at end", func(t *testing.T) {
		idx := NewShardedIndex()
		for i := uint64(1); i <= 10; i++ {
			idx.Set(i, RecordPosition{SegmentID: 1, Offset: int64(i)})
		}

		deleted := idx.DeleteRange(6, 10)
		assert.Equal(t, int64(5), deleted)
		assert.Equal(t, int64(5), idx.Len())
	})

	t.Run("delete all entries", func(t *testing.T) {
		idx := NewShardedIndex()
		for i := uint64(1); i <= 10; i++ {
			idx.Set(i, RecordPosition{SegmentID: 1, Offset: int64(i)})
		}

		deleted := idx.DeleteRange(1, 10)
		assert.Equal(t, int64(10), deleted)
		assert.Equal(t, int64(0), idx.Len())
	})

	t.Run("delete range with min > max", func(t *testing.T) {
		idx := NewShardedIndex()
		for i := uint64(1); i <= 10; i++ {
			idx.Set(i, RecordPosition{SegmentID: 1, Offset: int64(i)})
		}

		deleted := idx.DeleteRange(10, 1)
		assert.Equal(t, int64(0), deleted)
		assert.Equal(t, int64(10), idx.Len())
	})

	t.Run("delete range on empty index", func(t *testing.T) {
		idx := NewShardedIndex()

		deleted := idx.DeleteRange(1, 100)
		assert.Equal(t, int64(0), deleted)
	})

	t.Run("delete range with gaps", func(t *testing.T) {
		idx := NewShardedIndex()
		for i := uint64(2); i <= 20; i += 2 {
			idx.Set(i, RecordPosition{SegmentID: 1, Offset: int64(i)})
		}
		assert.Equal(t, int64(10), idx.Len())

		deleted := idx.DeleteRange(5, 15)
		assert.Equal(t, int64(5), deleted)
		assert.Equal(t, int64(5), idx.Len())
	})

	t.Run("delete large range", func(t *testing.T) {
		idx := NewShardedIndex()
		for i := uint64(0); i < 10000; i++ {
			idx.Set(i, RecordPosition{SegmentID: 1, Offset: int64(i)})
		}

		deleted := idx.DeleteRange(1000, 8999)
		assert.Equal(t, int64(8000), deleted)
		assert.Equal(t, int64(2000), idx.Len())
	})
}

func TestShardedIndex_SetBatch_Comprehensive(t *testing.T) {
	t.Run("set batch of entries", func(t *testing.T) {
		idx := NewShardedIndex()

		entries := make([]IndexEntry, 100)
		for i := 0; i < 100; i++ {
			entries[i] = IndexEntry{
				Index: uint64(i + 1),
				Pos:   RecordPosition{SegmentID: SegmentID(i), Offset: int64(i * 100)},
			}
		}

		idx.SetBatch(entries)
		assert.Equal(t, int64(100), idx.Len())

		for i := 0; i < 100; i++ {
			pos, ok := idx.Get(uint64(i + 1))
			assert.True(t, ok)
			assert.Equal(t, SegmentID(i), pos.SegmentID)
			assert.Equal(t, int64(i*100), pos.Offset)
		}
	})

	t.Run("set empty batch", func(t *testing.T) {
		idx := NewShardedIndex()

		idx.SetBatch(nil)
		assert.Equal(t, int64(0), idx.Len())

		idx.SetBatch([]IndexEntry{})
		assert.Equal(t, int64(0), idx.Len())
	})

	t.Run("set batch with duplicates overwrites", func(t *testing.T) {
		idx := NewShardedIndex()

		entries := []IndexEntry{
			{Index: 1, Pos: RecordPosition{SegmentID: 1, Offset: 100}},
			{Index: 2, Pos: RecordPosition{SegmentID: 1, Offset: 200}},
			{Index: 1, Pos: RecordPosition{SegmentID: 2, Offset: 300}},
		}

		idx.SetBatch(entries)
		assert.Equal(t, int64(2), idx.Len())

		pos, _ := idx.Get(1)
		assert.Equal(t, SegmentID(2), pos.SegmentID)
		assert.Equal(t, int64(300), pos.Offset)
	})

	t.Run("set batch updates existing entries", func(t *testing.T) {
		idx := NewShardedIndex()

		idx.Set(1, RecordPosition{SegmentID: 1, Offset: 100})
		idx.Set(2, RecordPosition{SegmentID: 1, Offset: 200})

		entries := []IndexEntry{
			{Index: 2, Pos: RecordPosition{SegmentID: 2, Offset: 999}},
			{Index: 3, Pos: RecordPosition{SegmentID: 2, Offset: 300}},
		}

		idx.SetBatch(entries)
		assert.Equal(t, int64(3), idx.Len())

		pos, _ := idx.Get(2)
		assert.Equal(t, int64(999), pos.Offset)
	})
}

func TestShardedIndex_IsCurrentEntry_Comprehensive(t *testing.T) {
	idx := NewShardedIndex()

	idx.Set(1, RecordPosition{SegmentID: 1, Offset: 100})

	t.Run("exact match", func(t *testing.T) {
		assert.True(t, idx.IsCurrentEntry(1, 1, 100))
	})

	t.Run("wrong offset", func(t *testing.T) {
		assert.False(t, idx.IsCurrentEntry(1, 1, 200))
	})

	t.Run("wrong segment", func(t *testing.T) {
		assert.False(t, idx.IsCurrentEntry(1, 2, 100))
	})

	t.Run("wrong index", func(t *testing.T) {
		assert.False(t, idx.IsCurrentEntry(2, 1, 100))
	})

	t.Run("non-existent index", func(t *testing.T) {
		assert.False(t, idx.IsCurrentEntry(999, 1, 100))
	})
}

func TestShardedIndex_Len(t *testing.T) {
	idx := NewShardedIndex()

	assert.Equal(t, int64(0), idx.Len())

	idx.Set(1, RecordPosition{SegmentID: 1, Offset: 100})
	assert.Equal(t, int64(1), idx.Len())

	idx.Set(2, RecordPosition{SegmentID: 1, Offset: 200})
	assert.Equal(t, int64(2), idx.Len())

	idx.Set(1, RecordPosition{SegmentID: 2, Offset: 300})
	assert.Equal(t, int64(2), idx.Len())

	idx.Delete(1)
	assert.Equal(t, int64(1), idx.Len())

	idx.Delete(2)
	assert.Equal(t, int64(0), idx.Len())
}

func TestShardedIndex_LenSlow(t *testing.T) {
	idx := NewShardedIndex()

	for i := uint64(0); i < 1000; i++ {
		idx.Set(i, RecordPosition{SegmentID: 1, Offset: int64(i)})
	}

	assert.Equal(t, idx.Len(), idx.LenSlow())
	assert.Equal(t, int64(1000), idx.LenSlow())

	idx.DeleteRange(100, 199)
	assert.Equal(t, idx.Len(), idx.LenSlow())
	assert.Equal(t, int64(900), idx.LenSlow())
}

func TestShardedIndex_Clear(t *testing.T) {
	idx := NewShardedIndex()

	for i := uint64(0); i < 1000; i++ {
		idx.Set(i, RecordPosition{SegmentID: 1, Offset: int64(i)})
	}
	assert.Equal(t, int64(1000), idx.Len())

	idx.Clear()
	assert.Equal(t, int64(0), idx.Len())
	assert.Equal(t, int64(0), idx.LenSlow())

	_, ok := idx.Get(500)
	assert.False(t, ok)

	idx.Set(1, RecordPosition{SegmentID: 2, Offset: 100})
	assert.Equal(t, int64(1), idx.Len())

	pos, ok := idx.Get(1)
	assert.True(t, ok)
	assert.Equal(t, SegmentID(2), pos.SegmentID)
}

func TestShardedIndex_Range_Comprehensive(t *testing.T) {
	t.Run("iterate all entries", func(t *testing.T) {
		idx := NewShardedIndex()
		for i := uint64(1); i <= 100; i++ {
			idx.Set(i, RecordPosition{SegmentID: 1, Offset: int64(i * 10)})
		}

		visited := make(map[uint64]bool)
		idx.Range(func(index uint64, pos RecordPosition) bool {
			visited[index] = true
			assert.Equal(t, int64(index*10), pos.Offset)
			return true
		})

		assert.Len(t, visited, 100)
		for i := uint64(1); i <= 100; i++ {
			assert.True(t, visited[i], "index %d should be visited", i)
		}
	})

	t.Run("early termination", func(t *testing.T) {
		idx := NewShardedIndex()
		for i := uint64(1); i <= 100; i++ {
			idx.Set(i, RecordPosition{SegmentID: 1, Offset: int64(i)})
		}

		count := 0
		idx.Range(func(index uint64, pos RecordPosition) bool {
			count++
			return count < 10
		})

		assert.Equal(t, 10, count)
	})

	t.Run("range on empty index", func(t *testing.T) {
		idx := NewShardedIndex()

		count := 0
		idx.Range(func(index uint64, pos RecordPosition) bool {
			count++
			return true
		})

		assert.Equal(t, 0, count)
	})
}

func TestShardedIndex_GetFirstLast_Comprehensive(t *testing.T) {
	t.Run("empty index", func(t *testing.T) {
		idx := NewShardedIndex()

		first, last, ok := idx.GetFirstLast()
		assert.False(t, ok)
		assert.Equal(t, uint64(0), first)
		assert.Equal(t, uint64(0), last)
	})

	t.Run("single entry", func(t *testing.T) {
		idx := NewShardedIndex()
		idx.Set(42, RecordPosition{SegmentID: 1, Offset: 100})

		first, last, ok := idx.GetFirstLast()
		assert.True(t, ok)
		assert.Equal(t, uint64(42), first)
		assert.Equal(t, uint64(42), last)
	})

	t.Run("multiple entries", func(t *testing.T) {
		idx := NewShardedIndex()
		idx.Set(10, RecordPosition{})
		idx.Set(5, RecordPosition{})
		idx.Set(100, RecordPosition{})
		idx.Set(50, RecordPosition{})

		first, last, ok := idx.GetFirstLast()
		assert.True(t, ok)
		assert.Equal(t, uint64(5), first)
		assert.Equal(t, uint64(100), last)
	})

	t.Run("non-contiguous entries", func(t *testing.T) {
		idx := NewShardedIndex()
		for _, i := range []uint64{1000, 5, 500, 999, 1} {
			idx.Set(i, RecordPosition{SegmentID: 1, Offset: int64(i)})
		}

		first, last, ok := idx.GetFirstLast()
		assert.True(t, ok)
		assert.Equal(t, uint64(1), first)
		assert.Equal(t, uint64(1000), last)
	})

	t.Run("after deletions", func(t *testing.T) {
		idx := NewShardedIndex()
		for i := uint64(1); i <= 10; i++ {
			idx.Set(i, RecordPosition{})
		}

		idx.Delete(1)
		idx.Delete(10)

		first, last, ok := idx.GetFirstLast()
		assert.True(t, ok)
		assert.Equal(t, uint64(2), first)
		assert.Equal(t, uint64(9), last)
	})

	t.Run("high index values", func(t *testing.T) {
		idx := NewShardedIndex()
		idx.Set(1<<40, RecordPosition{})
		idx.Set(1<<50, RecordPosition{})
		idx.Set(1<<45, RecordPosition{})

		first, last, ok := idx.GetFirstLast()
		assert.True(t, ok)
		assert.Equal(t, uint64(1<<40), first)
		assert.Equal(t, uint64(1<<50), last)
	})
}

func TestShardedIndex_ConcurrentReads(t *testing.T) {
	idx := NewShardedIndex()

	for i := uint64(0); i < 10000; i++ {
		idx.Set(i, RecordPosition{SegmentID: SegmentID(i % 100), Offset: int64(i)})
	}

	var wg sync.WaitGroup
	for g := 0; g < 10; g++ {
		wg.Add(1)
		go func(goroutine int) {
			defer wg.Done()
			for i := 0; i < 1000; i++ {
				index := uint64((goroutine*1000 + i) % 10000)
				pos, ok := idx.Get(index)
				if !ok {
					t.Errorf("entry %d not found", index)
					return
				}
				if pos.Offset != int64(index) {
					t.Errorf("wrong offset for %d: got %d", index, pos.Offset)
					return
				}
			}
		}(g)
	}
	wg.Wait()
}

func TestShardedIndex_ConcurrentWrites(t *testing.T) {
	idx := NewShardedIndex()

	var wg sync.WaitGroup
	for g := 0; g < 10; g++ {
		wg.Add(1)
		go func(goroutine int) {
			defer wg.Done()
			base := uint64(goroutine * 1000)
			for i := uint64(0); i < 1000; i++ {
				idx.Set(base+i, RecordPosition{
					SegmentID: SegmentID(goroutine),
					Offset:    int64(i),
				})
			}
		}(g)
	}
	wg.Wait()

	assert.Equal(t, int64(10000), idx.Len())
}

func TestShardedIndex_ConcurrentMixed(t *testing.T) {
	idx := NewShardedIndex()

	for i := uint64(0); i < 5000; i++ {
		idx.Set(i, RecordPosition{SegmentID: 1, Offset: int64(i)})
	}

	var wg sync.WaitGroup

	for g := 0; g < 5; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 1000; i++ {
				idx.Get(uint64(i % 5000))
			}
		}()
	}

	for g := 0; g < 5; g++ {
		wg.Add(1)
		go func(goroutine int) {
			defer wg.Done()
			base := uint64(5000 + goroutine*1000)
			for i := uint64(0); i < 1000; i++ {
				idx.Set(base+i, RecordPosition{SegmentID: 1, Offset: int64(base + i)})
			}
		}(g)
	}

	wg.Wait()

	assert.Equal(t, int64(10000), idx.Len())
}

func TestShardedIndex_ConcurrentDeleteRange(t *testing.T) {
	idx := NewShardedIndex()

	for i := uint64(0); i < 10000; i++ {
		idx.Set(i, RecordPosition{SegmentID: 1, Offset: int64(i)})
	}

	var wg sync.WaitGroup

	ranges := []struct{ min, max uint64 }{
		{0, 999},
		{2000, 2999},
		{4000, 4999},
		{6000, 6999},
		{8000, 8999},
	}

	for _, r := range ranges {
		wg.Add(1)
		go func(min, max uint64) {
			defer wg.Done()
			idx.DeleteRange(min, max)
		}(r.min, r.max)
	}

	wg.Wait()

	assert.Equal(t, int64(5000), idx.Len())
}

func TestShardedIndex_ConcurrentSetBatch(t *testing.T) {
	idx := NewShardedIndex()

	var wg sync.WaitGroup

	for g := 0; g < 10; g++ {
		wg.Add(1)
		go func(goroutine int) {
			defer wg.Done()
			base := uint64(goroutine * 100)
			entries := make([]IndexEntry, 100)
			for i := 0; i < 100; i++ {
				entries[i] = IndexEntry{
					Index: base + uint64(i),
					Pos:   RecordPosition{SegmentID: SegmentID(goroutine), Offset: int64(i)},
				}
			}
			idx.SetBatch(entries)
		}(g)
	}

	wg.Wait()

	assert.Equal(t, int64(1000), idx.Len())
}

func TestShardedIndex_ShardDistribution(t *testing.T) {
	idx := NewShardedIndex()

	for i := uint64(0); i < uint64(defaultShardCount*10); i++ {
		idx.Set(i, RecordPosition{SegmentID: 1, Offset: int64(i)})
	}

	shardCounts := make([]int, defaultShardCount)
	for _, shard := range idx.shards {
		shard.mu.RLock()
		shardCounts[len(shard.items)%defaultShardCount] += len(shard.items)
		shard.mu.RUnlock()
	}

	totalPerShard := defaultShardCount * 10 / defaultShardCount
	for i, shard := range idx.shards {
		shard.mu.RLock()
		count := len(shard.items)
		shard.mu.RUnlock()
		assert.InDelta(t, totalPerShard, count, float64(totalPerShard), "shard %d has unexpected count %d", i, count)
	}
}

func TestShardedIndex_ZeroIndex(t *testing.T) {
	idx := NewShardedIndex()

	idx.Set(0, RecordPosition{SegmentID: 1, Offset: 100})

	pos, ok := idx.Get(0)
	assert.True(t, ok)
	assert.Equal(t, int64(100), pos.Offset)

	idx.Delete(0)
	_, ok = idx.Get(0)
	assert.False(t, ok)
}

func TestShardedIndex_MaxUint64Index(t *testing.T) {
	idx := NewShardedIndex()

	maxIdx := ^uint64(0)
	idx.Set(maxIdx, RecordPosition{SegmentID: 999, Offset: 12345})

	pos, ok := idx.Get(maxIdx)
	assert.True(t, ok)
	assert.Equal(t, SegmentID(999), pos.SegmentID)
	assert.Equal(t, int64(12345), pos.Offset)
}

func TestShardedIndex_GetShardConsistency(t *testing.T) {
	idx := NewShardedIndex()

	for i := 0; i < 100; i++ {
		testIndex := uint64(12345)
		shard1 := idx.getShard(testIndex)
		shard2 := idx.getShard(testIndex)
		require.Same(t, shard1, shard2, "same index should always return same shard")
	}
}

func TestShardedIndex_BatchOperationsLargeScale(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping large scale test in short mode")
	}

	idx := NewShardedIndex()

	entries := make([]IndexEntry, 100000)
	for i := 0; i < 100000; i++ {
		entries[i] = IndexEntry{
			Index: uint64(i),
			Pos:   RecordPosition{SegmentID: SegmentID(i % 1000), Offset: int64(i)},
		}
	}

	idx.SetBatch(entries)
	assert.Equal(t, int64(100000), idx.Len())

	deleted := idx.DeleteRange(25000, 74999)
	assert.Equal(t, int64(50000), deleted)
	assert.Equal(t, int64(50000), idx.Len())

	_, ok := idx.Get(24999)
	assert.True(t, ok)

	_, ok = idx.Get(25000)
	assert.False(t, ok)

	_, ok = idx.Get(74999)
	assert.False(t, ok)

	_, ok = idx.Get(75000)
	assert.True(t, ok)
}
