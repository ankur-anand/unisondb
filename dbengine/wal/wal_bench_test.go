package wal

import (
	"sync"
	"testing"

	"github.com/brianvoe/gofakeit/v7"
	"github.com/hashicorp/go-metrics"
	"github.com/stretchr/testify/assert"
)

func setupWalTest(t *testing.T) *WalIO {
	dir := t.TempDir()

	walInstance, err := NewWalIO(dir, "test_namespace", NewDefaultConfig(), metrics.Default())
	assert.NoError(t, err)

	t.Cleanup(func() {
		err := walInstance.Close()
		assert.NoError(t, err, "closing wal instance failed")
	})

	return walInstance
}

func BenchmarkWalIOReadThroughput(b *testing.B) {
	walInstance := setupWalTest(&testing.T{})

	data := []byte(gofakeit.LetterN(1024))
	offset, err := walInstance.Append(data)
	if err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()
	var totalBytes int64

	for i := 0; i < b.N; i++ {
		readData, err := walInstance.Read(offset)
		if err != nil {
			b.Fatal(err)
		}
		totalBytes += int64(len(readData))
	}

	b.ReportMetric(float64(totalBytes)/b.Elapsed().Seconds(), "bytes/sec")
}

func BenchmarkWalIOAppendThroughput(b *testing.B) {
	walInstance := setupWalTest(&testing.T{})

	data := []byte(gofakeit.LetterN(1024))

	b.ResetTimer()
	var totalBytes int64

	for i := 0; i < b.N; i++ {
		_, err := walInstance.Append(data)
		if err != nil {
			b.Fatal(err)
		}
		totalBytes += int64(len(data))
	}

	b.ReportMetric(float64(totalBytes)/b.Elapsed().Seconds(), "bytes/sec")
}

func BenchmarkWalIOReadThroughputConcurrent(b *testing.B) {
	walInstance := setupWalTest(&testing.T{})

	data := []byte(gofakeit.LetterN(1024))
	offset, err := walInstance.Append(data)
	if err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()
	var totalBytes int64
	var mu sync.Mutex // To safely update totalBytes

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() { // Each goroutine runs this loop
			readData, err := walInstance.Read(offset)
			if err != nil {
				b.Fatal(err)
			}
			mu.Lock()
			totalBytes += int64(len(readData))
			mu.Unlock()
		}
	})

	b.ReportMetric(float64(totalBytes)/b.Elapsed().Seconds(), "bytes/sec")
}

func BenchmarkWalIOAppendThroughputConcurrent(b *testing.B) {
	walInstance := setupWalTest(&testing.T{})

	data := []byte(gofakeit.LetterN(1024)) // 1KB data

	b.ResetTimer()
	var totalBytes int64
	var mu sync.Mutex // To safely update totalBytes

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_, err := walInstance.Append(data)
			if err != nil {
				b.Fatal(err)
			}
			mu.Lock()
			totalBytes += int64(len(data))
			mu.Unlock()
		}
	})

	b.ReportMetric(float64(totalBytes)/b.Elapsed().Seconds(), "bytes/sec")
}

func BenchmarkWalIOReadWriteThroughputConcurrent(b *testing.B) {
	walInstance := setupWalTest(&testing.T{})

	data := []byte(gofakeit.LetterN(1024)) // 1KB data
	var totalBytesWritten int64
	var totalBytesRead int64
	var mu sync.Mutex
	var wg sync.WaitGroup

	var offsets []Offset
	var offsetMu sync.Mutex

	numWriters := 4
	numReaders := 4

	b.ResetTimer()

	for i := 0; i < numWriters; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < b.N; j++ {
				pos, err := walInstance.Append(data)
				if err != nil {
					b.Fatal(err)
				}
				mu.Lock()
				totalBytesWritten += int64(len(data))
				mu.Unlock()

				// Store the latest offset for readers
				offsetMu.Lock()
				offsets = append(offsets, pos)
				offsetMu.Unlock()
			}
		}()
	}

	for i := 0; i < numReaders; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < b.N; j++ {
				offsetMu.Lock()
				if len(offsets) == 0 {
					offsetMu.Unlock()
					continue
				}
				pos := offsets[j%len(offsets)]
				offsetMu.Unlock()

				readData, err := walInstance.Read(pos)
				if err != nil {
					b.Fatal(err)
				}

				mu.Lock()
				totalBytesRead += int64(len(readData))
				mu.Unlock()
			}
		}()
	}

	wg.Wait()

	b.ReportMetric(float64(totalBytesWritten)/b.Elapsed().Seconds(), "write_bytes/sec")
	b.ReportMetric(float64(totalBytesRead)/b.Elapsed().Seconds(), "read_bytes/sec")
}
