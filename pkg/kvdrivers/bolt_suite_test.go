package kvdrivers_test

import (
	"path/filepath"
	"testing"

	kvdrivers2 "github.com/ankur-anand/unisondb/pkg/kvdrivers"
	"github.com/stretchr/testify/assert"
)

func TestBolt_Suite(t *testing.T) {
	tempDir := t.TempDir()
	path := filepath.Join(tempDir, "bolt_test.bolt.db")
	store, err := kvdrivers2.NewBoltdb(path, kvdrivers2.Config{
		Namespace: "test",
		NoSync:    true,
	})

	assert.NoError(t, err, "failed to create boltdb")
	assert.NotNil(t, store, "store should not be nil")

	boltConstructor := func(path string, config kvdrivers2.Config) (bTreeStore, error) {
		return kvdrivers2.NewBoltdb(path, config)
	}

	ts := &testSuite{
		store:         store,
		dbConstructor: boltConstructor,
		txnBatcherConstructor: func(maxBatchSize int) TxnBatcher {
			return store.NewTxnQueue(maxBatchSize)
		},
	}

	suites := getTestSuites(ts)
	t.Run("boltdb", func(t *testing.T) {
		for _, tc := range suites {
			t.Run(tc.name, func(t *testing.T) {
				tc.runFunc(t)
			})
		}
	})

	assert.NoError(t, store.Close(), "failed to close store")
}
