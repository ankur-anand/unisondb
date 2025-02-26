package kv_test

import (
	"path/filepath"
	"testing"

	"github.com/ankur-anand/kvalchemy/storage/kv"
	"github.com/stretchr/testify/assert"
)

func TestLMDB_Suite(t *testing.T) {
	tempDir := t.TempDir()
	path := filepath.Join(tempDir, "lmdb_test.lmdb")
	store, err := kv.NewLmdb(kv.Config{
		Path:      path,
		Namespace: "test",
		NoSync:    false,
		MmapSize:  1 << 20,
	})

	assert.NoError(t, err, "failed to create lmdb")
	assert.NotNil(t, store, "store should not be nil")

	lmdbConstructor := func(config kv.Config) (bTreeStore, error) {
		return kv.NewLmdb(config)
	}

	ts := &TestSuite{
		t:             t,
		store:         store,
		dbConstructor: lmdbConstructor,
	}

	RunTestSuite(t, "lmdb", ts)
}
