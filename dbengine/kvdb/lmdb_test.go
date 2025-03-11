package kvdb_test

import (
	"bytes"
	"path/filepath"
	"testing"
	"time"

	kv2 "github.com/ankur-anand/unisondb/dbengine/kvdb"
	"github.com/ankur-anand/unisondb/internal/etc"
	"github.com/hashicorp/go-metrics"
	"github.com/stretchr/testify/assert"
)

func TestLMDB_Suite(t *testing.T) {
	tempDir := t.TempDir()
	path := filepath.Join(tempDir, "lmdb_test.lmdb")
	store, err := kv2.NewLmdb(path, kv2.Config{
		Namespace: "test",
		NoSync:    false,
		MmapSize:  1 << 30,
	})

	assert.NoError(t, err)
	inm := metrics.NewInmemSink(1*time.Millisecond, time.Minute)
	cfg := metrics.DefaultConfig("lmdb_test")
	cfg.TimerGranularity = time.Second
	cfg.EnableHostname = false
	cfg.EnableRuntimeMetrics = true
	_, err = metrics.NewGlobal(cfg, inm)
	if err != nil {
		panic(err)
	}

	assert.NoError(t, err, "failed to create lmdb")
	assert.NotNil(t, store, "store should not be nil")

	lmdbConstructor := func(path string, config kv2.Config) (bTreeStore, error) {
		return kv2.NewLmdb(path, config)
	}

	ts := &testSuite{
		store:         store,
		dbConstructor: lmdbConstructor,
	}

	suites := getTestSuites(ts)
	t.Run("lmdb", func(t *testing.T) {
		for _, tc := range suites {
			t.Run(tc.name, func(t *testing.T) {
				tc.runFunc(t)
			})
		}
	})

	buf := new(bytes.Buffer)
	err = etc.DumpStats(inm, buf)
	assert.NoError(t, err, "failed to dump stats")
	assert.NoError(t, store.Close(), "failed to close store")
	output := buf.String()
	assert.Contains(t, output, "set.total")
	assert.Contains(t, output, "get.total")
	assert.Contains(t, output, "delete.total")
}
