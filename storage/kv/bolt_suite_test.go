package kv_test

import (
	"path/filepath"
	"testing"

	"github.com/ankur-anand/kvalchemy/storage/kv"
	"github.com/stretchr/testify/assert"
)

func TestBolt_Suite(t *testing.T) {
	tempDir := t.TempDir()
	path := filepath.Join(tempDir, "bolt_test.bolt.db")
	store, err := kv.NewBoltdb(kv.Config{
		Path:      path,
		Namespace: "test",
		NoSync:    true,
	})

	assert.NoError(t, err, "failed to create boltdb")
	assert.NotNil(t, store, "store should not be nil")

	boltConstructor := func(config kv.Config) (bTreeStore, error) {
		return kv.NewBoltdb(config)
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
}
