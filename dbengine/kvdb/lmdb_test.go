package kvdb_test

import (
	"path/filepath"
	"testing"

	kv2 "github.com/ankur-anand/kvalchemy/dbengine/kvdb"
	"github.com/stretchr/testify/assert"
)

func TestLMDB_Suite(t *testing.T) {
	tempDir := t.TempDir()
	path := filepath.Join(tempDir, "lmdb_test.lmdb")
	store, err := kv2.NewLmdb(kv2.Config{
		Path:      path,
		Namespace: "test",
		NoSync:    false,
		MmapSize:  1 << 20,
	})

	assert.NoError(t, err, "failed to create lmdb")
	assert.NotNil(t, store, "store should not be nil")

	lmdbConstructor := func(config kv2.Config) (bTreeStore, error) {
		return kv2.NewLmdb(config)
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
}
