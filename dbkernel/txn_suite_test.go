package dbkernel

import (
	"bytes"
	"context"
	"testing"
	"time"

	"github.com/ankur-anand/unisondb/schemas/logrecord"
	"github.com/stretchr/testify/require"
)

func requireTxnKVValue(t *testing.T, engine *Engine, key, expected []byte) {
	t.Helper()

	require.Eventually(t, func() bool {
		val, err := engine.GetKV(key)
		return err == nil && bytes.Equal(val, expected)
	}, time.Second, 10*time.Millisecond)
}

func requireTxnRowValue(t *testing.T, engine *Engine, rowKey, column string, expected []byte) {
	t.Helper()

	require.Eventually(t, func() bool {
		rowData, err := engine.GetRowColumns(rowKey, nil)
		if err != nil {
			return false
		}
		return bytes.Equal(rowData[column], expected)
	}, time.Second, 10*time.Millisecond)
}

func requireTxnLOBValue(t *testing.T, engine *Engine, key []byte, expected []byte) {
	t.Helper()

	require.Eventually(t, func() bool {
		val, err := engine.GetLOB(key)
		return err == nil && bytes.Equal(val, expected)
	}, time.Second, 10*time.Millisecond)
}

func setupTxnSuiteEngine(t *testing.T, raftMode bool) (*Engine, func()) {
	t.Helper()

	if raftMode {
		engine, _, cleanup := setupRaftTxnTestEngine(t)
		return engine, cleanup
	}

	dir := t.TempDir()
	namespace := "txn_suite_local"

	conf := NewDefaultEngineConfig()
	conf.DBEngine = LMDBEngine
	conf.BtreeConfig.Namespace = namespace

	engine, err := NewStorageEngine(dir, namespace, conf)
	require.NoError(t, err)

	cleanup := func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = engine.Close(ctx)
	}

	return engine, cleanup
}

func TestNewTransactionSuite(t *testing.T) {
	tests := []struct {
		name     string
		raftMode bool
	}{
		{name: "local", raftMode: false},
		{name: "raft", raftMode: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			engine, cleanup := setupTxnSuiteEngine(t, tt.raftMode)
			defer cleanup()

			t.Run("KV", func(t *testing.T) {
				txn, err := engine.NewTransaction(logrecord.LogOperationTypeInsert, logrecord.LogEntryTypeKV)
				require.NoError(t, err)
				if tt.raftMode {
					_, ok := txn.(*RaftTxn)
					require.True(t, ok)
				} else {
					_, ok := txn.(*Txn)
					require.True(t, ok)
				}

				err = txn.AppendKVTxn([]byte("key1"), []byte("value1"))
				require.NoError(t, err)

				_, err = engine.GetKV([]byte("key1"))
				require.ErrorIs(t, err, ErrKeyNotFound)

				err = txn.Commit()
				require.NoError(t, err)

				requireTxnKVValue(t, engine, []byte("key1"), []byte("value1"))
			})

			t.Run("Row", func(t *testing.T) {
				txn, err := engine.NewTransaction(logrecord.LogOperationTypeInsert, logrecord.LogEntryTypeRow)
				require.NoError(t, err)

				err = txn.AppendColumnTxn([]byte("row1"), map[string][]byte{"col1": []byte("val1")})
				require.NoError(t, err)

				_, err = engine.GetRowColumns("row1", nil)
				require.ErrorIs(t, err, ErrKeyNotFound)

				err = txn.Commit()
				require.NoError(t, err)

				requireTxnRowValue(t, engine, "row1", "col1", []byte("val1"))
			})

			t.Run("Abort", func(t *testing.T) {
				txn, err := engine.NewTransaction(logrecord.LogOperationTypeInsert, logrecord.LogEntryTypeKV)
				require.NoError(t, err)

				err = txn.AppendKVTxn([]byte("abort-key"), []byte("abort-value"))
				require.NoError(t, err)

				txn.Abort()
				err = txn.Commit()
				require.ErrorIs(t, err, ErrTxnAborted)
			})

			t.Run("Chunked", func(t *testing.T) {
				txn, err := engine.NewTransaction(logrecord.LogOperationTypeInsert, logrecord.LogEntryTypeChunked)
				require.NoError(t, err)

				err = txn.AppendKVTxn([]byte("lob-key"), []byte("chunk1-"))
				require.NoError(t, err)
				err = txn.AppendKVTxn([]byte("lob-key"), []byte("chunk2"))
				require.NoError(t, err)

				_, err = engine.GetLOB([]byte("lob-key"))
				require.ErrorIs(t, err, ErrKeyNotFound)

				err = txn.Commit()
				require.NoError(t, err)

				requireTxnLOBValue(t, engine, []byte("lob-key"), []byte("chunk1-chunk2"))
			})
		})
	}
}
