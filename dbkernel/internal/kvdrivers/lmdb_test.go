package kvdrivers_test

import (
	"bytes"
	"os"
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

func openLMDBTemp(t *testing.T) (*kvdrivers.LmdbEmbed, string) {
	t.Helper()
	dir := t.TempDir()
	conf := kvdrivers.Config{
		Namespace: "test",
		NoSync:    true,
		MmapSize:  1 << 30,
	}
	db, err := kvdrivers.NewLmdb(dir, conf)
	assert.NoError(t, err)
	return db, dir
}

func TestLMDB_Snapshot_BufferAndFile(t *testing.T) {
	db, _ := openLMDBTemp(t)
	defer db.Close()

	assert.NoError(t, db.SetKV([]byte("k1"), []byte("v1")))
	assert.NoError(t, db.SetKV([]byte("k2"), []byte("v2")))

	var buf bytes.Buffer
	assert.NoError(t, db.Snapshot(&buf))
	assert.Greater(t, buf.Len(), 0, "buffer snapshot should produce bytes")

	tmpFile := filepath.Join(t.TempDir(), "snapshot.mdb")
	f, err := os.Create(tmpFile)
	assert.NoError(t, err)
	assert.NoError(t, db.Snapshot(f))
	assert.NoError(t, f.Close())

	info, err := os.Stat(tmpFile)
	assert.NoError(t, err)
	assert.Greater(t, info.Size(), int64(0), "file snapshot should produce data")
}

func TestLMDB_Restore_FromBufferAndFileSnapshots(t *testing.T) {

	donor, _ := openLMDBTemp(t)
	defer donor.Close()

	assert.NoError(t, donor.SetKV([]byte("new_k"), []byte("new_v")))

	var buf bytes.Buffer
	assert.NoError(t, donor.Snapshot(&buf))

	tmpFile := filepath.Join(t.TempDir(), "snapshot.mdb")
	f, err := os.Create(tmpFile)
	assert.NoError(t, err)
	assert.NoError(t, donor.Snapshot(f))
	assert.NoError(t, f.Close())

	target, _ := openLMDBTemp(t)
	defer target.Close()
	assert.NoError(t, target.SetKV([]byte("old_k"), []byte("old_v")))

	assert.NoError(t, target.Restore(bytes.NewReader(buf.Bytes())))

	_, err = target.GetKV([]byte("old_k"))
	assert.ErrorIs(t, err, kvdrivers.ErrKeyNotFound)
	got, err := target.GetKV([]byte("new_k"))
	assert.NoError(t, err)
	assert.Equal(t, []byte("new_v"), got)

	snapBytes, err := os.ReadFile(tmpFile)
	assert.NoError(t, err)
	assert.NoError(t, target.Restore(bytes.NewReader(snapBytes)))

	got, err = target.GetKV([]byte("new_k"))
	assert.NoError(t, err)
	assert.Equal(t, []byte("new_v"), got)
}
