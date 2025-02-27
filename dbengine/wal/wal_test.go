package wal_test

import (
	"bytes"
	"io"
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
}
