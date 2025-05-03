package main

import (
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/edsrzf/mmap-go"
)

var (
	chunkSizes = []int64{
		1 << 10,   // 1KB
		4 << 10,   // 4KB
		16 << 10,  // 16KB
		64 << 10,  // 64KB
		256 << 10, // 256KB
		1 << 20,   // 1MB
	}
)

func runBenchmarkMMAP(segmentSize, chunkSize int64, flushPerWrite bool) time.Duration {
	tmpFile, err := os.CreateTemp("", fmt.Sprintf("segment-mmap-%d-", segmentSize))
	if err != nil {
		panic(err)
	}
	defer os.Remove(tmpFile.Name())
	defer tmpFile.Close()

	if err := tmpFile.Truncate(segmentSize); err != nil {
		panic(err)
	}

	mmapData, err := mmap.Map(tmpFile, mmap.RDWR, 0)
	if err != nil {
		panic(err)
	}
	defer mmapData.Unmap()

	start := time.Now()
	data := make([]byte, chunkSize)

	for offset := int64(0); offset+chunkSize <= segmentSize; offset += chunkSize {
		copy(mmapData[offset:], data)

		if flushPerWrite {
			if err := mmapData.Flush(); err != nil {
				panic(err)
			}
		}
	}

	if !flushPerWrite {
		if err := mmapData.Flush(); err != nil {
			panic(err)
		}
	}
	return time.Since(start)
}

func runBenchmarkFile(segmentSize, chunkSize int64, syncPerWrite bool) time.Duration {
	tmpFile, err := os.CreateTemp("", fmt.Sprintf("segment-fd-%d-", segmentSize))
	if err != nil {
		panic(err)
	}
	defer os.Remove(tmpFile.Name())
	defer tmpFile.Close()

	data := make([]byte, chunkSize)
	start := time.Now()

	for offset := int64(0); offset+chunkSize <= segmentSize; offset += chunkSize {
		if _, err := tmpFile.WriteAt(data, offset); err != nil {
			panic(err)
		}

		if syncPerWrite {
			if err := tmpFile.Sync(); err != nil {
				panic(err)
			}
		}
	}

	if !syncPerWrite {
		if err := tmpFile.Sync(); err != nil {
			panic(err)
		}
	}
	return time.Since(start)
}

func BenchmarkSegments(b *testing.B) {
	segmentSizes := []int64{
		16 * 1024 * 1024, // 16MB
		32 * 1024 * 1024, // 32MB
	}

	for _, size := range segmentSizes {
		for _, chunk := range chunkSizes {
			b.Run(fmt.Sprintf("MMAP_%dMB_%dKB_BatchFlush", size/(1<<20), chunk/(1<<10)), func(b *testing.B) {
				for i := 0; i < b.N; i++ {
					runBenchmarkMMAP(size, chunk, false)
				}
			})
			b.Run(fmt.Sprintf("MMAP_%dMB_%dKB_PerWriteFlush", size/(1<<20), chunk/(1<<10)), func(b *testing.B) {
				for i := 0; i < b.N; i++ {
					runBenchmarkMMAP(size, chunk, true)
				}
			})

			b.Run(fmt.Sprintf("FD_%dMB_%dKB_BatchSync", size/(1<<20), chunk/(1<<10)), func(b *testing.B) {
				for i := 0; i < b.N; i++ {
					runBenchmarkFile(size, chunk, false)
				}
			})
			b.Run(fmt.Sprintf("FD_%dMB_%dKB_PerWriteSync", size/(1<<20), chunk/(1<<10)), func(b *testing.B) {
				for i := 0; i < b.N; i++ {
					runBenchmarkFile(size, chunk, true)
				}
			})
		}
	}
}
