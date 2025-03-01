package kvdb_test

import (
	"bytes"
	"path/filepath"
	"testing"
	"time"

	kv2 "github.com/ankur-anand/kvalchemy/dbengine/kvdb"
	"github.com/ankur-anand/kvalchemy/internal/etc"
	"github.com/hashicorp/go-metrics"
	"github.com/stretchr/testify/assert"
)

func TestBolt_Suite(t *testing.T) {
	tempDir := t.TempDir()
	path := filepath.Join(tempDir, "bolt_test.bolt.db")
	store, err := kv2.NewBoltdb(path, kv2.Config{
		Namespace: "test",
		NoSync:    true,
	})

	assert.NoError(t, err, "failed to create boltdb")
	assert.NotNil(t, store, "store should not be nil")
	inm := metrics.NewInmemSink(1*time.Millisecond, time.Minute)
	cfg := metrics.DefaultConfig("bolt_test")
	cfg.TimerGranularity = time.Second
	cfg.EnableHostname = false
	cfg.EnableRuntimeMetrics = true
	_, err = metrics.NewGlobal(cfg, inm)
	if err != nil {
		panic(err)
	}

	boltConstructor := func(path string, config kv2.Config) (bTreeStore, error) {
		return kv2.NewBoltdb(path, config)
	}

	ts := &testSuite{
		store:         store,
		dbConstructor: boltConstructor,
	}

	suites := getTestSuites(ts)
	t.Run("boltdb", func(t *testing.T) {
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
	assert.Contains(t, output, "bolt.set.total")
	assert.Contains(t, output, "bolt.get.total")
	assert.Contains(t, output, "bolt.delete.total")
}
