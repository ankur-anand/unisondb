package kvdrivers_test

import (
	"path/filepath"
	"testing"

	"github.com/ankur-anand/unisondb/dbkernel/internal/kvdrivers"
	"github.com/stretchr/testify/assert"
)

func TestLMDB_Suite(t *testing.T) {
	tempDir := t.TempDir()
	path := filepath.Join(tempDir, "lmdb_test.lmdb")
	store, err := kvdrivers.NewLmdb(path, kvdrivers.Config{
		Namespace: "test",
		NoSync:    false,
		MmapSize:  1 << 30,
	})

	assert.NoError(t, err)

	assert.NoError(t, err, "failed to create lmdb")
	assert.NotNil(t, store, "store should not be nil")

	lmdbConstructor := func(path string, config kvdrivers.Config) (bTreeStore, error) {
		return kvdrivers.NewLmdb(path, config)
	}

	ts := &testSuite{
		store:         store,
		dbConstructor: lmdbConstructor,
		txnBatcherConstructor: func(maxBatchSize int) TxnBatcher {
			return store.NewTxnQueue(maxBatchSize)
		},
	}

	suites := getTestSuites(ts)
	t.Run("lmdb", func(t *testing.T) {
		for _, tc := range suites {
			t.Run(tc.name, func(t *testing.T) {
				tc.runFunc(t)
			})
		}
	})

	assert.NoError(t, store.Close(), "failed to close store")
}
