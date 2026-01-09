package raftwalfs

import (
	"fmt"
	"io"
	"log/slog"
	"math/rand"
	"testing"

	"github.com/ankur-anand/unisondb/pkg/walfs"
	"github.com/hashicorp/raft"
)

func init() {
	slog.SetDefault(slog.New(slog.NewTextHandler(io.Discard, nil)))
}

func benchLog(idx, term uint64, payload []byte) *raft.Log {
	return &raft.Log{
		Index: idx,
		Term:  term,
		Type:  raft.LogCommand,
		Data:  payload,
	}
}

func newBenchStore(b *testing.B, opts ...walfs.WALogOptions) *LogStore {
	b.Helper()
	dir := b.TempDir()
	wal, err := walfs.NewWALog(dir, ".wal", opts...)
	if err != nil {
		b.Fatal(err)
	}
	store, err := NewLogStore(wal, 0)
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() {
		store.Close()
		wal.Close()
	})
	return store
}

func BenchmarkLogStore_StoreLog(b *testing.B) {
	payloadSizes := []int{64, 256, 1024, 4096}

	for _, size := range payloadSizes {
		b.Run(fmt.Sprintf("payload=%d", size), func(b *testing.B) {
			store := newBenchStore(b)
			payload := make([]byte, size)

			b.ReportAllocs()
			b.SetBytes(int64(size))
			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				log := benchLog(uint64(i+1), 1, payload)
				if err := store.StoreLog(log); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func BenchmarkLogStore_StoreLogs_Batch(b *testing.B) {
	batchSizes := []int{1, 10, 100, 1000}

	for _, batchSize := range batchSizes {
		b.Run(fmt.Sprintf("batch=%d", batchSize), func(b *testing.B) {
			store := newBenchStore(b, walfs.WithMaxSegmentSize(64<<20))
			payload := make([]byte, 256)

			b.ReportAllocs()
			b.SetBytes(int64(256 * batchSize))
			b.ResetTimer()

			idx := uint64(1)
			for i := 0; i < b.N; i++ {
				batch := make([]*raft.Log, batchSize)
				for j := 0; j < batchSize; j++ {
					batch[j] = benchLog(idx, 1, payload)
					idx++
				}
				if err := store.StoreLogs(batch); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func BenchmarkLogStore_GetLog_Sequential(b *testing.B) {
	store := newBenchStore(b)

	const total = 10000
	payload := make([]byte, 256)
	for i := 0; i < total; i++ {
		if err := store.StoreLog(benchLog(uint64(i+1), 1, payload)); err != nil {
			b.Fatal(err)
		}
	}

	b.ReportAllocs()
	b.SetBytes(int64(len(payload)))
	b.ResetTimer()

	var out raft.Log
	for i := 0; i < b.N; i++ {
		idx := uint64((i % total) + 1)
		if err := store.GetLog(idx, &out); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkLogStore_GetLog_Random(b *testing.B) {
	store := newBenchStore(b)

	const total = 10000
	payload := make([]byte, 256)
	for i := 0; i < total; i++ {
		if err := store.StoreLog(benchLog(uint64(i+1), 1, payload)); err != nil {
			b.Fatal(err)
		}
	}

	rng := rand.New(rand.NewSource(42))
	indices := make([]uint64, b.N)
	for i := range indices {
		indices[i] = uint64(rng.Intn(total)) + 1
	}

	b.ReportAllocs()
	b.SetBytes(int64(len(payload)))
	b.ResetTimer()

	var out raft.Log
	for i := 0; i < b.N; i++ {
		if err := store.GetLog(indices[i], &out); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkLogStore_GetLog_Concurrent(b *testing.B) {
	store := newBenchStore(b)

	const total = 10000
	payload := make([]byte, 256)
	for i := 0; i < total; i++ {
		if err := store.StoreLog(benchLog(uint64(i+1), 1, payload)); err != nil {
			b.Fatal(err)
		}
	}

	b.ReportAllocs()
	b.SetBytes(int64(len(payload)))
	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		var out raft.Log
		i := uint64(1)
		for pb.Next() {
			idx := (i % total) + 1
			if err := store.GetLog(idx, &out); err != nil {
				b.Errorf("GetLog(%d) failed: %v", idx, err)
				return
			}
			i++
		}
	})
}

func BenchmarkLogStore_FirstLastIndex(b *testing.B) {
	store := newBenchStore(b)

	for i := 1; i <= 1000; i++ {
		if err := store.StoreLog(benchLog(uint64(i), 1, []byte("data"))); err != nil {
			b.Fatal(err)
		}
	}

	b.Run("FirstIndex", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_, _ = store.FirstIndex()
		}
	})

	b.Run("LastIndex", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_, _ = store.LastIndex()
		}
	})
}

func BenchmarkLogStore_DeleteRange(b *testing.B) {
	for _, deleteSize := range []int{10, 100, 1000} {
		b.Run(fmt.Sprintf("size=%d", deleteSize), func(b *testing.B) {
			b.ReportAllocs()

			for i := 0; i < b.N; i++ {
				b.StopTimer()
				store := newBenchStore(b)

				total := deleteSize * 2
				for j := 1; j <= total; j++ {
					if err := store.StoreLog(benchLog(uint64(j), 1, []byte("data"))); err != nil {
						b.Fatal(err)
					}
				}

				b.StartTimer()
				if err := store.DeleteRange(1, uint64(deleteSize)); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func BenchmarkShardedIndex_Set(b *testing.B) {
	idx := walfs.NewShardedIndex()
	pos := walfs.RecordPosition{SegmentID: 1, Offset: 100}

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		idx.Set(uint64(i), pos)
	}
}

func BenchmarkShardedIndex_Get(b *testing.B) {
	idx := walfs.NewShardedIndex()
	pos := walfs.RecordPosition{SegmentID: 1, Offset: 100}

	const total = 100000
	for i := uint64(0); i < total; i++ {
		idx.Set(i, pos)
	}

	b.Run("Sequential", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			idx.Get(uint64(i % total))
		}
	})

	b.Run("Random", func(b *testing.B) {
		rng := rand.New(rand.NewSource(42))
		indices := make([]uint64, b.N)
		for i := range indices {
			indices[i] = uint64(rng.Int63n(total))
		}

		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			idx.Get(indices[i])
		}
	})
}

func BenchmarkShardedIndex_SetBatch(b *testing.B) {
	batchSizes := []int{10, 100, 1000}

	for _, batchSize := range batchSizes {
		b.Run(fmt.Sprintf("batch=%d", batchSize), func(b *testing.B) {
			idx := walfs.NewShardedIndex()

			b.ReportAllocs()
			b.ResetTimer()

			baseIdx := uint64(0)
			for i := 0; i < b.N; i++ {
				entries := make([]walfs.IndexEntry, batchSize)
				for j := 0; j < batchSize; j++ {
					entries[j] = walfs.IndexEntry{
						Index: baseIdx + uint64(j),
						Pos:   walfs.RecordPosition{SegmentID: 1, Offset: int64(j * 100)},
					}
				}
				idx.SetBatch(entries)
				baseIdx += uint64(batchSize)
			}
		})
	}
}

func BenchmarkShardedIndex_DeleteRange(b *testing.B) {
	for _, rangeSize := range []int{100, 1000, 10000} {
		b.Run(fmt.Sprintf("range=%d", rangeSize), func(b *testing.B) {
			b.ReportAllocs()

			for i := 0; i < b.N; i++ {
				b.StopTimer()
				idx := walfs.NewShardedIndex()
				pos := walfs.RecordPosition{SegmentID: 1, Offset: 100}

				for j := uint64(0); j < uint64(rangeSize*2); j++ {
					idx.Set(j, pos)
				}

				b.StartTimer()
				idx.DeleteRange(0, uint64(rangeSize-1))
			}
		})
	}
}

func BenchmarkShardedIndex_GetFirstLast(b *testing.B) {
	idx := walfs.NewShardedIndex()
	pos := walfs.RecordPosition{SegmentID: 1, Offset: 100}

	for i := uint64(1); i <= 10000; i++ {
		idx.Set(i*7, pos)
	}

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		idx.GetFirstLast()
	}
}

func BenchmarkShardedIndex_Concurrent(b *testing.B) {
	idx := walfs.NewShardedIndex()
	pos := walfs.RecordPosition{SegmentID: 1, Offset: 100}

	// Pre-populate
	const total = 10000
	for i := uint64(0); i < total; i++ {
		idx.Set(i, pos)
	}

	b.Run("Read", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		b.RunParallel(func(pb *testing.PB) {
			i := uint64(0)
			for pb.Next() {
				idx.Get(i % total)
				i++
			}
		})
	})

	b.Run("Write", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		b.RunParallel(func(pb *testing.PB) {
			i := uint64(total)
			for pb.Next() {
				idx.Set(i, pos)
				i++
			}
		})
	})

	b.Run("Mixed", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		b.RunParallel(func(pb *testing.PB) {
			i := uint64(0)
			for pb.Next() {
				if i%2 == 0 {
					idx.Get(i % total)
				} else {
					idx.Set(i+total, pos)
				}
				i++
			}
		})
	})
}

// BenchmarkLogStore_Recovery measures index rebuild time
func BenchmarkLogStore_Recovery(b *testing.B) {
	for _, count := range []int{1000, 10000, 100000} {
		b.Run(fmt.Sprintf("entries=%d", count), func(b *testing.B) {
			dir := b.TempDir()

			wal, err := walfs.NewWALog(dir, ".wal")
			if err != nil {
				b.Fatal(err)
			}

			store, err := NewLogStore(wal, 0)
			if err != nil {
				b.Fatal(err)
			}

			payload := make([]byte, 128)
			batch := make([]*raft.Log, 1000)
			for i := 0; i < count; i += 1000 {
				for j := 0; j < 1000 && i+j < count; j++ {
					batch[j] = benchLog(uint64(i+j+1), 1, payload)
				}
				batchLen := 1000
				if i+1000 > count {
					batchLen = count - i
				}
				if err := store.StoreLogs(batch[:batchLen]); err != nil {
					b.Fatal(err)
				}
			}

			store.Close()
			wal.Close()

			b.ReportAllocs()
			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				wal2, err := walfs.NewWALog(dir, ".wal")
				if err != nil {
					b.Fatal(err)
				}

				store2, err := NewLogStore(wal2, 0)
				if err != nil {
					b.Fatal(err)
				}

				store2.Close()
				wal2.Close()
			}
		})
	}
}

// BenchmarkEncodeRaftLog measures encoding performance
func BenchmarkEncodeRaftLog(b *testing.B) {
	payloadSizes := []int{64, 256, 1024, 4096}

	for _, size := range payloadSizes {
		b.Run(fmt.Sprintf("payload=%d", size), func(b *testing.B) {
			log := &raft.Log{
				Index: 12345,
				Term:  10,
				Type:  raft.LogCommand,
				Data:  make([]byte, size),
			}

			b.ReportAllocs()
			b.SetBytes(int64(size))
			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				_, err := BinaryCodecV1{}.Encode(log)
				if err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// BenchmarkDecodeRaftLog measures decoding allocation overhead
func BenchmarkDecodeRaftLog(b *testing.B) {
	payloadSizes := []int{64, 256, 1024, 4096}

	for _, size := range payloadSizes {
		b.Run(fmt.Sprintf("payload=%d", size), func(b *testing.B) {
			log := &raft.Log{
				Index: 12345,
				Term:  10,
				Type:  raft.LogCommand,
				Data:  make([]byte, size),
			}
			encoded, _ := BinaryCodecV1{}.Encode(log)

			b.ReportAllocs()
			b.SetBytes(int64(size))
			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				_, err := BinaryCodecV1{}.Decode(encoded)
				if err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
