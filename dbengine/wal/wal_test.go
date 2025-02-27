package wal_test

import (
	"bytes"
	"io"
	"sync"
	"testing"
	"time"

	"github.com/ankur-anand/kvalchemy/dbengine/wal"
	"github.com/ankur-anand/kvalchemy/internal/etc"
	"github.com/brianvoe/gofakeit/v7"
	"github.com/hashicorp/go-metrics"
	"github.com/stretchr/testify/assert"
)

func setupWalTest(t *testing.T) *wal.WalIO {
	dir := t.TempDir()

	inm := metrics.NewInmemSink(1*time.Millisecond, time.Minute)
	cfg := metrics.DefaultConfig("wal")
	cfg.TimerGranularity = time.Second
	cfg.EnableHostname = false
	cfg.EnableRuntimeMetrics = true
	m, err := metrics.New(cfg, inm)
	if err != nil {
		panic(err)
	}

	walInstance, err := wal.NewWalIO(dir, "test_namespace", wal.NewDefaultConfig(), m)
	assert.NoError(t, err)

	t.Cleanup(func() {
		err := walInstance.Close()
		assert.NoError(t, err, "closing wal instance failed")
		buf := new(bytes.Buffer)
		err = etc.DumpStats(inm, buf)
		assert.NoError(t, err, "failed to dump stats")
		output := buf.String()
		assert.Contains(t, output, "wal.fsync.total")
		assert.Contains(t, output, "wal.append.total")
		assert.Contains(t, output, "wal.read.total")
	})

	return walInstance
}

func TestWalIO_Suite(t *testing.T) {

	walInstance := setupWalTest(t)

	appendData := gofakeit.LetterN(10)
	t.Run("wal_append_read", func(t *testing.T) {
		pos, err := walInstance.Append([]byte(appendData))
		assert.NoError(t, err)
		assert.NotNil(t, pos)

		data, err := walInstance.Read(pos)
		assert.NoError(t, err)
		assert.Equal(t, appendData, string(data))
	})

	t.Run("wal_append_reader", func(t *testing.T) {
		reader, err := walInstance.NewReader()
		assert.NoError(t, err)
		assert.NotNil(t, reader)
		value, pos, err := reader.Next()
		assert.NoError(t, err)
		assert.Equal(t, appendData, string(value))
		assert.NotNil(t, pos)

		_, pos, err = reader.Next()
		assert.ErrorIs(t, err, io.EOF)
		assert.Nil(t, pos)
	})

	t.Run("wal_reader_with_start", func(t *testing.T) {
		appendData2 := gofakeit.LetterN(10)
		pos, err := walInstance.Append([]byte(appendData2))
		assert.NoError(t, err)
		assert.NotNil(t, pos)

		reader, err := walInstance.NewReaderWithStart(pos)
		assert.NoError(t, err)
		assert.NotNil(t, reader)
		value, pos, err := reader.Next()
		assert.NoError(t, err)
		assert.Equal(t, appendData2, string(value))

		_, pos, err = reader.Next()
		assert.ErrorIs(t, err, io.EOF)
		assert.Nil(t, pos)
	})

	t.Run("wal_fsync", func(t *testing.T) {
		err := walInstance.Sync()
		assert.NoError(t, err)
	})

	t.Run("concurrent_read_write", func(t *testing.T) {
		var offsets []wal.Offset
		var offsetMu sync.Mutex

		numWriters := 4
		numReaders := 4
		numOps := 100

		var wg sync.WaitGroup

		for i := 0; i < numWriters; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for j := 0; j < numOps; j++ {
					data := gofakeit.LetterN(128)
					pos, err := walInstance.Append([]byte(data))
					assert.NoError(t, err, "append should not fail")
					assert.NotNil(t, pos, "append should return a valid position")

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
				for j := 0; j < numOps; j++ {
					offsetMu.Lock()
					if len(offsets) == 0 {
						offsetMu.Unlock()
						continue
					}
					pos := offsets[j%len(offsets)] // Read a recent offset
					offsetMu.Unlock()

					data, err := walInstance.Read(pos)
					assert.NoError(t, err, "read should not fail")
					assert.NotEmpty(t, data, "read should return valid data")
				}
			}()
		}

		wg.Wait()
	})
}
