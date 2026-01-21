package dbkernel

import (
	"bytes"
	"context"
	"sync"
	"testing"
	"time"

	"github.com/ankur-anand/unisondb/pkg/raftwalfs"
	"github.com/ankur-anand/unisondb/schemas/logrecord"
	"github.com/hashicorp/raft"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func setupRaftTxnTestEngine(t *testing.T) (*Engine, *raft.Raft, func()) {
	t.Helper()

	dir := t.TempDir()
	namespace := "test_raft_txn"

	conf := NewDefaultEngineConfig()
	conf.DBEngine = LMDBEngine
	conf.BtreeConfig.Namespace = namespace
	conf.WalConfig.RaftMode = true
	conf.WalConfig.AutoCleanup = false
	engine, err := NewStorageEngine(dir, namespace, conf)
	require.NoError(t, err)

	r, logStore, raftCleanup := setupSingleNodeRaftWithEngineWAL(t, engine)
	engine.SetPositionLookup(logStore.GetPosition)

	engine.SetRaftApplier(&raftApplierWrapper{
		raft:    r,
		timeout: 5 * time.Second,
	})

	engine.SetRaftMode(true)

	cleanup := func() {
		raftCleanup()
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = engine.Close(ctx)
	}

	return engine, r, cleanup
}

func setupRaftTxnTestEngineWithCodec(t *testing.T, codec raftwalfs.Codec) (*Engine, *raft.Raft, *raftwalfs.LogStore, func()) {
	t.Helper()

	dir := t.TempDir()
	namespace := "test_raft_txn_codec"

	conf := NewDefaultEngineConfig()
	conf.DBEngine = LMDBEngine
	conf.BtreeConfig.Namespace = namespace
	conf.WalConfig.RaftMode = true
	conf.WalConfig.AutoCleanup = false

	engine, err := NewStorageEngine(dir, namespace, conf)
	require.NoError(t, err)

	r, logStore, raftCleanup := setupSingleNodeRaftWithEngineWALCodec(t, engine, codec)

	engine.SetPositionLookup(logStore.GetPosition)
	engine.SetRaftApplier(&raftApplierWrapper{
		raft:    r,
		timeout: 5 * time.Second,
	})
	engine.SetRaftMode(true)

	cleanup := func() {
		raftCleanup()
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = engine.Close(ctx)
	}

	return engine, r, logStore, cleanup
}

func requireKVValue(t *testing.T, engine *Engine, key, expected []byte) {
	t.Helper()

	require.Eventually(t, func() bool {
		val, err := engine.GetKV(key)
		return err == nil && bytes.Equal(val, expected)
	}, time.Second, 10*time.Millisecond)
}

func requireRowValue(t *testing.T, engine *Engine, rowKey string, column string, expected []byte) {
	t.Helper()

	require.Eventually(t, func() bool {
		rowData, err := engine.GetRowColumns(rowKey, func(columnKey string) bool { return true })
		if err != nil {
			return false
		}
		return bytes.Equal(rowData[column], expected)
	}, time.Second, 10*time.Millisecond)
}

func TestRaftTxn_BasicKVTransaction(t *testing.T) {
	engine, _, cleanup := setupRaftTxnTestEngine(t)
	defer cleanup()

	txn, err := engine.NewRaftTxn(logrecord.LogOperationTypeInsert, logrecord.LogEntryTypeKV)
	require.NoError(t, err)
	require.NotNil(t, txn)

	err = txn.AppendKV([]byte("key1"), []byte("value1"))
	require.NoError(t, err)

	err = txn.AppendKV([]byte("key2"), []byte("value2"))
	require.NoError(t, err)
	assert.Equal(t, 2, txn.ValuesCount())

	_, err = engine.GetKV([]byte("key1"))
	require.ErrorIs(t, err, ErrKeyNotFound)
	err = txn.Commit()
	require.NoError(t, err)

	requireKVValue(t, engine, []byte("key1"), []byte("value1"))
	requireKVValue(t, engine, []byte("key2"), []byte("value2"))
}

func TestRaftTxn_NonTxnWriteOrdering(t *testing.T) {
	engine, _, cleanup := setupRaftTxnTestEngine(t)
	defer cleanup()

	txn, err := engine.NewRaftTxn(logrecord.LogOperationTypeInsert, logrecord.LogEntryTypeKV)
	require.NoError(t, err)

	err = txn.AppendKV([]byte("conflictKey"), []byte("txnValue"))
	require.NoError(t, err)
	err = engine.PutKV([]byte("conflictKey"), []byte("nonTxnValue"))
	require.NoError(t, err)

	requireKVValue(t, engine, []byte("conflictKey"), []byte("nonTxnValue"))
	err = txn.Commit()
	require.NoError(t, err)
	requireKVValue(t, engine, []byte("conflictKey"), []byte("txnValue"))
}

func TestRaftTxn_OverlappingTransactionsOrdering(t *testing.T) {
	engine, _, cleanup := setupRaftTxnTestEngine(t)
	defer cleanup()

	txn1, err := engine.NewRaftTxn(logrecord.LogOperationTypeInsert, logrecord.LogEntryTypeKV)
	require.NoError(t, err)

	txn2, err := engine.NewRaftTxn(logrecord.LogOperationTypeInsert, logrecord.LogEntryTypeKV)
	require.NoError(t, err)

	err = txn1.AppendKV([]byte("sharedKey"), []byte("value1"))
	require.NoError(t, err)
	err = txn2.AppendKV([]byte("sharedKey"), []byte("value2"))
	require.NoError(t, err)

	err = txn2.Commit()
	require.NoError(t, err)
	requireKVValue(t, engine, []byte("sharedKey"), []byte("value2"))

	err = txn1.Commit()
	require.NoError(t, err)
	requireKVValue(t, engine, []byte("sharedKey"), []byte("value1"))
}

func TestRaftTxn_Abort(t *testing.T) {
	engine, _, cleanup := setupRaftTxnTestEngine(t)
	defer cleanup()

	txn, err := engine.NewRaftTxn(logrecord.LogOperationTypeInsert, logrecord.LogEntryTypeKV)
	require.NoError(t, err)

	err = txn.AppendKV([]byte("abortedKey"), []byte("abortedValue"))
	require.NoError(t, err)
	txn.Abort()

	err = txn.Commit()
	require.Error(t, err)
	assert.ErrorIs(t, err, ErrTxnAborted)
	_, err = engine.GetKV([]byte("abortedKey"))
	assert.ErrorIs(t, err, ErrKeyNotFound)
}

func TestRaftTxn_RowTransaction(t *testing.T) {
	engine, _, cleanup := setupRaftTxnTestEngine(t)
	defer cleanup()

	txn, err := engine.NewRaftTxn(logrecord.LogOperationTypeInsert, logrecord.LogEntryTypeRow)
	require.NoError(t, err)
	columns := map[string][]byte{
		"col1": []byte("val1"),
		"col2": []byte("val2"),
	}
	err = txn.AppendRowColumn([]byte("row1"), columns)
	require.NoError(t, err)

	err = txn.Commit()
	require.NoError(t, err)
	requireRowValue(t, engine, "row1", "col1", []byte("val1"))
	requireRowValue(t, engine, "row1", "col2", []byte("val2"))
}

func TestRaftTxn_MultipleKeys(t *testing.T) {
	engine, _, cleanup := setupRaftTxnTestEngine(t)
	defer cleanup()

	txn, err := engine.NewRaftTxn(logrecord.LogOperationTypeInsert, logrecord.LogEntryTypeKV)
	require.NoError(t, err)
	for i := 0; i < 10; i++ {
		key := []byte(string(rune('a' + i)))
		value := []byte(string(rune('A' + i)))
		err = txn.AppendKV(key, value)
		require.NoError(t, err)
	}

	assert.Equal(t, 10, txn.ValuesCount())
	err = txn.Commit()
	require.NoError(t, err)

	for i := 0; i < 10; i++ {
		key := []byte(string(rune('a' + i)))
		expectedValue := []byte(string(rune('A' + i)))
		requireKVValue(t, engine, key, expectedValue)
	}
}

func TestRaftTxn_NotInRaftMode(t *testing.T) {
	dir := t.TempDir()
	namespace := "test_not_raft"

	conf := NewDefaultEngineConfig()
	conf.DBEngine = LMDBEngine
	conf.BtreeConfig.Namespace = namespace

	engine, err := NewStorageEngine(dir, namespace, conf)
	require.NoError(t, err)

	defer func() {
		ctx := context.Background()
		_ = engine.Close(ctx)
	}()

	_, err = engine.NewRaftTxn(logrecord.LogOperationTypeInsert, logrecord.LogEntryTypeKV)
	require.Error(t, err)
	assert.ErrorIs(t, err, ErrNotSupportedInRaftMode)
}

func TestRaftTxn_InvalidOperationType(t *testing.T) {
	engine, _, cleanup := setupRaftTxnTestEngine(t)
	defer cleanup()

	_, err := engine.NewRaftTxn(logrecord.LogOperationTypeNoOperation, logrecord.LogEntryTypeKV)
	require.Error(t, err)
	assert.ErrorIs(t, err, ErrUnsupportedTxnType)

	_, err = engine.NewRaftTxn(logrecord.LogOperationTypeDelete, logrecord.LogEntryTypeChunked)
	require.Error(t, err)
	assert.ErrorIs(t, err, ErrUnsupportedTxnType)
}

func TestRaftTxn_WrongMethodForEntryType(t *testing.T) {
	engine, _, cleanup := setupRaftTxnTestEngine(t)
	defer cleanup()

	kvTxn, err := engine.NewRaftTxn(logrecord.LogOperationTypeInsert, logrecord.LogEntryTypeKV)
	require.NoError(t, err)

	err = kvTxn.AppendRowColumn([]byte("row"), map[string][]byte{"col": []byte("val")})
	require.Error(t, err)
	assert.ErrorIs(t, err, ErrUnsupportedTxnType)
	kvTxn.Abort()
	rowTxn, err := engine.NewRaftTxn(logrecord.LogOperationTypeInsert, logrecord.LogEntryTypeRow)
	require.NoError(t, err)

	err = rowTxn.AppendKV([]byte("key"), []byte("val"))
	require.Error(t, err)
	assert.ErrorIs(t, err, ErrUnsupportedTxnType)
	rowTxn.Abort()
}

func TestRaftTxn_AppendChunk(t *testing.T) {
	engine, _, cleanup := setupRaftTxnTestEngine(t)
	defer cleanup()

	kvTxn, err := engine.NewRaftTxn(logrecord.LogOperationTypeInsert, logrecord.LogEntryTypeKV)
	require.NoError(t, err)
	err = kvTxn.AppendChunk([]byte("blob"), []byte("chunk"))
	require.ErrorIs(t, err, ErrUnsupportedTxnType)
	kvTxn.Abort()

	chunkedTxn, err := engine.NewRaftTxn(logrecord.LogOperationTypeInsert, logrecord.LogEntryTypeChunked)
	require.NoError(t, err)
	err = chunkedTxn.AppendChunk([]byte("blob"), []byte("chunk1"))
	require.NoError(t, err)
	err = chunkedTxn.AppendChunk([]byte("blob"), []byte("chunk2"))
	require.NoError(t, err)
	assert.Equal(t, 2, chunkedTxn.ValuesCount())
	err = chunkedTxn.Commit()
	require.NoError(t, err)

	conflictTxn, err := engine.NewRaftTxn(logrecord.LogOperationTypeInsert, logrecord.LogEntryTypeChunked)
	require.NoError(t, err)
	err = conflictTxn.AppendChunk([]byte("blob1"), []byte("chunk1"))
	require.NoError(t, err)
	err = conflictTxn.AppendChunk([]byte("blob2"), []byte("chunk2"))
	require.ErrorIs(t, err, ErrKeyChangedForChunkedType)
	conflictTxn.Abort()
}

func TestRaftTxn_EmptyColumns(t *testing.T) {
	engine, _, cleanup := setupRaftTxnTestEngine(t)
	defer cleanup()

	txn, err := engine.NewRaftTxn(logrecord.LogOperationTypeInsert, logrecord.LogEntryTypeRow)
	require.NoError(t, err)

	err = txn.AppendRowColumn([]byte("row"), map[string][]byte{})
	require.Error(t, err)
	assert.ErrorIs(t, err, ErrEmptyColumns)
	txn.Abort()
}

func TestRaftTxn_DoubleCommit(t *testing.T) {
	engine, _, cleanup := setupRaftTxnTestEngine(t)
	defer cleanup()

	txn, err := engine.NewRaftTxn(logrecord.LogOperationTypeInsert, logrecord.LogEntryTypeKV)
	require.NoError(t, err)

	err = txn.AppendKV([]byte("key"), []byte("value"))
	require.NoError(t, err)
	err = txn.Commit()
	require.NoError(t, err)

	err = txn.Commit()
	require.Error(t, err)
	assert.ErrorIs(t, err, ErrTxnAlreadyCommitted)
}

func TestRaftTxn_OperationAfterAbort(t *testing.T) {
	engine, _, cleanup := setupRaftTxnTestEngine(t)
	defer cleanup()

	txn, err := engine.NewRaftTxn(logrecord.LogOperationTypeInsert, logrecord.LogEntryTypeKV)
	require.NoError(t, err)

	txn.Abort()
	err = txn.AppendKV([]byte("key"), []byte("value"))
	require.Error(t, err)
	assert.ErrorIs(t, err, ErrTxnAborted)
}

func TestRaftTxn_ConcurrentTransactions(t *testing.T) {
	engine, _, cleanup := setupRaftTxnTestEngine(t)
	defer cleanup()

	const numTxns = 5
	const keysPerTxn = 10

	var wg sync.WaitGroup
	errors := make(chan error, numTxns)

	for i := 0; i < numTxns; i++ {
		wg.Add(1)
		go func(txnNum int) {
			defer wg.Done()

			txn, err := engine.NewRaftTxn(logrecord.LogOperationTypeInsert, logrecord.LogEntryTypeKV)
			if err != nil {
				errors <- err
				return
			}

			for j := 0; j < keysPerTxn; j++ {
				key := []byte(string(rune('a'+txnNum)) + string(rune('0'+j)))
				value := []byte(string(rune('A'+txnNum)) + string(rune('0'+j)))
				if err := txn.AppendKV(key, value); err != nil {
					txn.Abort()
					errors <- err
					return
				}
			}

			if err := txn.Commit(); err != nil {
				errors <- err
				return
			}
		}(i)
	}

	wg.Wait()
	close(errors)

	for err := range errors {
		require.NoError(t, err)
	}

	for i := 0; i < numTxns; i++ {
		for j := 0; j < keysPerTxn; j++ {
			key := []byte(string(rune('a'+i)) + string(rune('0'+j)))
			expectedValue := []byte(string(rune('A'+i)) + string(rune('0'+j)))
			requireKVValue(t, engine, key, expectedValue)
		}
	}
}

func TestRaftTxn_LSNMutatedByRaft(t *testing.T) {
	codec := raftwalfs.BinaryCodecV1{DataMutator: raftwalfs.LogRecordMutator{}}
	engine, _, logStore, cleanup := setupRaftTxnTestEngineWithCodec(t, codec)
	defer cleanup()

	txn, err := engine.NewRaftTxn(logrecord.LogOperationTypeInsert, logrecord.LogEntryTypeKV)
	require.NoError(t, err)

	err = txn.AppendKV([]byte("key1"), []byte("value1"))
	require.NoError(t, err)

	err = txn.Commit()
	require.NoError(t, err)

	lastIndex, err := logStore.LastIndex()
	require.NoError(t, err)

	foundTxn := false
	foundCommit := false
	for index := txn.BeginRaftIndex(); index <= lastIndex; index++ {
		var raftLog raft.Log
		require.NoError(t, logStore.GetLog(index, &raftLog))
		if raftLog.Type != raft.LogCommand || len(raftLog.Data) == 0 {
			continue
		}

		record := logrecord.GetRootAsLogRecord(raftLog.Data, 0)
		if !bytes.Equal(record.TxnIdBytes(), txn.TxnID()) {
			continue
		}
		foundTxn = true
		assert.Equal(t, index, record.Lsn())
		if record.TxnState() == logrecord.TransactionStateCommit {
			foundCommit = true
		}
	}

	assert.True(t, foundTxn, "expected to find txn records in raft log")
	assert.True(t, foundCommit, "expected to find commit record in raft log")
}
