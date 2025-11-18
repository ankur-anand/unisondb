package walfs

import (
	"errors"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type threadSafeRand struct {
	r  *rand.Rand
	mu sync.Mutex
}

func newThreadSafeRand() *threadSafeRand {
	return &threadSafeRand{
		r: rand.New(rand.NewSource(time.Now().UnixNano())),
	}
}

func (r *threadSafeRand) Intn(n int) int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.r.Intn(n)
}

var globalRand = newThreadSafeRand()

func calculateMaxEntries(dataSize int) int {
	entrySize := int64(recordHeaderSize + segmentHeaderSize + dataSize + recordTrailerMarkerSize)
	maxEntries := int(segmentSize / entrySize)
	if maxEntries == 0 {
		return 1
	}
	return maxEntries
}

func BenchmarkSegment(b *testing.B) {
	syncOptions := []struct {
		name string
		opt  MsyncOption
	}{
		{"NoSync", MsyncNone},
		{"SyncAfterWrite", MsyncOnWrite},
	}

	benchCases := []struct {
		name     string
		dataSize int
	}{
		{"Tiny_16B", 16},
		{"Small_1KB", 1 * 1024},
		{"Medium_32KB", 32 * 1024},
		{"Large_64KB", 64 * 1024},
		{"Large_512KB", 512 * 1024},
	}

	patterns := []struct {
		name    string
		pattern func(size int) []byte
	}{
		{
			name: "Sequential",
			pattern: func(size int) []byte {
				data := make([]byte, size)
				for i := range data {
					data[i] = byte(i % 256)
				}
				return data
			},
		},
		{
			name: "Random",
			pattern: func(size int) []byte {
				data := make([]byte, size)
				for i := range data {
					data[i] = byte(globalRand.Intn(256))
				}
				return data
			},
		},
	}

	for _, syncOpt := range syncOptions {
		for _, pattern := range patterns {
			for _, bc := range benchCases {
				testData := pattern.pattern(bc.dataSize)
				maxWrites := calculateMaxEntries(bc.dataSize)

				b.Run(fmt.Sprintf("%s/%s/Write/%s", syncOpt.name, pattern.name, bc.name), func(b *testing.B) {
					dir := b.TempDir()
					seg, err := OpenSegmentFile(dir, ".wal", 1, WithSyncOption(syncOpt.opt))
					if err != nil {
						b.Fatal(err)
					}
					defer seg.Close()

					b.ResetTimer()
					b.SetBytes(int64(len(testData)))

					for i := 0; i < b.N; i++ {
						if (i%maxWrites) == 0 && i > 0 {
							b.StopTimer()
							seg.Close()
							seg, err = OpenSegmentFile(dir, ".wal", uint32(i/maxWrites+1), WithSyncOption(syncOpt.opt))
							if err != nil {
								b.Fatal(err)
							}
							b.StartTimer()
						}
						if _, err := seg.Write(testData, 0); err != nil {
							b.Fatal(err)
						}
					}
				})

				b.Run(fmt.Sprintf("%s/%s/Read/%s", syncOpt.name, pattern.name, bc.name), func(b *testing.B) {
					dir := b.TempDir()
					seg, err := OpenSegmentFile(dir, ".wal", 1, WithSyncOption(syncOpt.opt))
					if err != nil {
						b.Fatal(err)
					}
					defer seg.Close()

					pos, err := seg.Write(testData, 0)
					if err != nil {
						b.Fatal(err)
					}

					if err := seg.Sync(); err != nil {
						b.Fatal(err)
					}

					b.ResetTimer()
					b.SetBytes(int64(len(testData)))

					for i := 0; i < b.N; i++ {
						if _, _, err := seg.Read(pos.Offset); err != nil {
							b.Fatal(err)
						}
					}
				})

				b.Run(fmt.Sprintf("%s/%s/SequentialRead/%s", syncOpt.name, pattern.name, bc.name), func(b *testing.B) {
					dir := b.TempDir()
					seg, err := OpenSegmentFile(dir, ".wal", 1, WithSyncOption(syncOpt.opt))
					if err != nil {
						b.Fatal(err)
					}
					defer seg.Close()

					numEntries := min(100, maxWrites)
					for i := 0; i < numEntries; i++ {
						if _, err := seg.Write(testData, 0); err != nil {
							b.Fatal(err)
						}
					}

					if err := seg.Sync(); err != nil {
						b.Fatal(err)
					}

					b.ResetTimer()
					b.SetBytes(int64(len(testData)) * int64(numEntries))

					for i := 0; i < b.N; i++ {
						reader := seg.NewReader()
						count := 0
						for {
							_, _, err := reader.Next()
							if err != nil {
								break
							}
							count++
						}
						reader.Close()
						if count != numEntries {
							b.Fatalf("expected %d entries, got %d", numEntries, count)
						}
					}
				})
			}
		}
	}
}

func BenchmarkConcurrent(b *testing.B) {
	dataSize := 1024
	concurrencyLevels := []int{2, 4, 8, 16}
	syncOptions := []struct {
		name string
		opt  MsyncOption
	}{
		{"NoSync", MsyncNone},
		{"SyncAfterWrite", MsyncOnWrite},
	}

	for _, syncOpt := range syncOptions {
		for _, numGoroutines := range concurrencyLevels {
			b.Run(fmt.Sprintf("%s/ConcurrentWrite_%dGoroutines", syncOpt.name, numGoroutines), func(b *testing.B) {
				dir := b.TempDir()

				var currentSeg atomic.Pointer[Segment]
				seg, err := OpenSegmentFile(dir, ".wal", 1, WithSyncOption(syncOpt.opt))
				if err != nil {
					b.Fatal(err)
				}
				currentSeg.Store(seg)
				defer func() {
					if s := currentSeg.Load(); s != nil {
						s.Close()
					}
				}()

				var segmentID atomic.Uint32
				segmentID.Store(1)
				var mu sync.Mutex

				data := make([]byte, dataSize)
				for i := range data {
					data[i] = byte(i % 256)
				}

				b.ResetTimer()
				b.SetBytes(int64(dataSize))

				b.RunParallel(func(pb *testing.PB) {
					for pb.Next() {
						for {
							seg := currentSeg.Load()
							if seg == nil {
								b.Fatal("Segment is nil")
							}

							_, err := seg.Write(data, 0)
							if err == nil {
								break
							}

							mu.Lock()
							if currentSeg.Load() == seg {
								nextID := segmentID.Add(1)
								newSeg, err := OpenSegmentFile(dir, ".wal", nextID, WithSyncOption(syncOpt.opt))
								if err != nil {
									mu.Unlock()
									b.Fatal(err)
								}
								oldSeg := currentSeg.Swap(newSeg)
								if oldSeg != nil {
									oldSeg.Close()
								}
							}
							mu.Unlock()
						}
					}
				})
			})

			b.Run(fmt.Sprintf("%s/ConcurrentReadWrite_%dGoroutines", syncOpt.name, numGoroutines), func(b *testing.B) {
				dir := b.TempDir()
				seg, err := OpenSegmentFile(dir, ".wal", 1, WithSyncOption(syncOpt.opt))
				if err != nil {
					b.Fatal(err)
				}
				defer seg.Close()

				data := make([]byte, dataSize)
				for i := range data {
					data[i] = byte(i % 256)
				}

				var positions sync.Map
				numbRewrites := min(100, calculateMaxEntries(dataSize)/2)

				for i := 0; i < numbRewrites; i++ {
					pos, err := seg.Write(data, 0)
					if err != nil {
						b.Fatal(err)
					}
					positions.Store(i, pos)
				}

				if err := seg.Sync(); err != nil {
					b.Fatal(err)
				}

				b.ResetTimer()
				b.SetBytes(int64(dataSize))

				b.RunParallel(func(pb *testing.PB) {
					for pb.Next() {
						if globalRand.Intn(2) == 0 {
							if v, ok := positions.Load(globalRand.Intn(numbRewrites)); ok {
								pos := v.(RecordPosition)
								if _, _, err := seg.Read(pos.Offset); err != nil {
									b.Fatal(err)
								}
							}
						} else {
							pos, err := seg.Write(data, 0)
							if err == nil {
								positions.Store(globalRand.Intn(numbRewrites), pos)
							}
						}
					}
				})
			})
		}
	}
}

func BenchmarkSyncLatency(b *testing.B) {
	sizes := []int{
		1024,
		32 * 1024,
		64 * 1024,
	}

	syncOptions := []struct {
		name string
		opt  MsyncOption
	}{
		{"NoSync", MsyncNone},
		{"SyncAfterWrite", MsyncOnWrite},
	}

	for _, syncOpt := range syncOptions {
		for _, size := range sizes {
			b.Run(fmt.Sprintf("%s/Write_%dB", syncOpt.name, size), func(b *testing.B) {
				dir := b.TempDir()
				seg, err := OpenSegmentFile(dir, ".wal", 1, WithSyncOption(syncOpt.opt))
				if err != nil {
					b.Fatal(err)
				}
				defer seg.Close()

				data := make([]byte, size)
				for i := range data {
					data[i] = byte(i % 256)
				}

				maxWrites := calculateMaxEntries(size)
				b.ResetTimer()
				b.SetBytes(int64(size))

				for i := 0; i < b.N; i++ {
					if (i%maxWrites) == 0 && i > 0 {
						b.StopTimer()
						seg.Close()
						seg, err = OpenSegmentFile(dir, ".wal", uint32(i/maxWrites+1), WithSyncOption(syncOpt.opt))
						if err != nil {
							b.Fatal(err)
						}
						b.StartTimer()
					}

					if _, err := seg.Write(data, 0); err != nil {
						b.Fatal(err)
					}
				}
			})
		}
	}
}

var sink []byte

func BenchmarkEncodeRecordPositionTo(b *testing.B) {
	pos := RecordPosition{SegmentID: 123456789, Offset: 9876543210}
	buf := make([]byte, 12)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		sink = EncodeRecordPositionTo(pos, buf)
	}
}

var sink1 []byte

func BenchmarkEncodeRecordPosition(b *testing.B) {
	pos := RecordPosition{SegmentID: 123456789, Offset: 9876543210}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		sink1 = pos.Encode()
	}
}

func BenchmarkWriteBatch(b *testing.B) {
	batchSizes := []int{10, 50, 100, 500}
	recordSize := 1024

	syncOptions := []struct {
		name string
		opt  MsyncOption
	}{
		{"NoSync", MsyncNone},
		{"SyncAfterWrite", MsyncOnWrite},
	}

	for _, syncOpt := range syncOptions {
		for _, batchSize := range batchSizes {
			b.Run(fmt.Sprintf("%s/BatchSize_%d", syncOpt.name, batchSize), func(b *testing.B) {
				dir := b.TempDir()
				seg, err := OpenSegmentFile(dir, ".wal", 1, WithSyncOption(syncOpt.opt))
				if err != nil {
					b.Fatal(err)
				}
				defer seg.Close()

				// Prepare batch
				records := make([][]byte, batchSize)
				for i := range records {
					records[i] = make([]byte, recordSize)
					for j := range records[i] {
						records[i][j] = byte(j % 256)
					}
				}

				totalBytes := int64(recordSize * batchSize)
				maxBatches := calculateMaxEntries(recordSize*batchSize) / batchSize

				b.ResetTimer()
				b.SetBytes(totalBytes)

				for i := 0; i < b.N; i++ {
					if (i%maxBatches) == 0 && i > 0 {
						b.StopTimer()
						seg.Close()
						seg, err = OpenSegmentFile(dir, ".wal", uint32(i/maxBatches+1), WithSyncOption(syncOpt.opt))
						if err != nil {
							b.Fatal(err)
						}
						b.StartTimer()
					}

					if _, _, err := seg.WriteBatch(records, nil); err != nil && !errors.Is(err, ErrSegmentFull) {
						b.Fatal(err)
					}
				}
			})
		}
	}
}

// BenchmarkWriteVsWriteBatch compares individual writes vs batch writes
func BenchmarkWriteVsWriteBatch(b *testing.B) {
	testCases := []struct {
		recordSize int
		batchSizes []int
	}{
		{1024, []int{5, 10, 20, 50}},
		{16 * 1024, []int{5, 10, 20}},
		{100 * 1024, []int{5, 10, 20}},
		{512 * 1024, []int{2, 5, 10}},
	}

	for _, tc := range testCases {
		recordSize := tc.recordSize

		b.Run(fmt.Sprintf("IndividualWrites_%dKB", recordSize/1024), func(b *testing.B) {
			dir := b.TempDir()
			seg, err := OpenSegmentFile(dir, ".wal", 1)
			if err != nil {
				b.Fatal(err)
			}
			defer seg.Close()

			data := make([]byte, recordSize)
			maxWrites := calculateMaxEntries(recordSize)

			b.ResetTimer()
			b.SetBytes(int64(recordSize))

			for i := 0; i < b.N; i++ {
				if (i%maxWrites) == 0 && i > 0 {
					b.StopTimer()
					seg.Close()
					seg, err = OpenSegmentFile(dir, ".wal", uint32(i/maxWrites+1))
					if err != nil {
						b.Fatal(err)
					}
					b.StartTimer()
				}

				if _, err := seg.Write(data, 0); err != nil {
					b.Fatal(err)
				}
			}
		})

		for _, batchSize := range tc.batchSizes {
			b.Run(fmt.Sprintf("BatchWrites_%dKB_Batch%d", recordSize/1024, batchSize), func(b *testing.B) {
				dir := b.TempDir()
				seg, err := OpenSegmentFile(dir, ".wal", 1)
				if err != nil {
					b.Fatal(err)
				}
				defer seg.Close()

				records := make([][]byte, batchSize)
				for i := range records {
					records[i] = make([]byte, recordSize)
				}

				totalBytes := int64(recordSize * batchSize)
				maxWrites := calculateMaxEntries(recordSize)
				if maxWrites < batchSize {
					maxWrites = batchSize
				}

				b.ResetTimer()
				b.SetBytes(totalBytes)

				writtenCount := 0
				for i := 0; i < b.N; i++ {
					if writtenCount >= maxWrites-batchSize && writtenCount > 0 {
						b.StopTimer()
						seg.Close()
						seg, err = OpenSegmentFile(dir, ".wal", uint32(writtenCount/maxWrites+2))
						if err != nil {
							b.Fatal(err)
						}
						writtenCount = 0
						b.StartTimer()
					}

					positions, written, err := seg.WriteBatch(records, nil)
					if err != nil && !errors.Is(err, ErrSegmentFull) {
						b.Fatal(err)
					}
					writtenCount += written
					_ = positions
				}
			})
		}
	}
}
