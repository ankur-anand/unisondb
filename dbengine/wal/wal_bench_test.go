package wal

import (
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
