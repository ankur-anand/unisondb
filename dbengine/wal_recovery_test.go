package dbengine

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/ankur-anand/kvalchemy/dbengine/kvdb"
	"github.com/ankur-anand/kvalchemy/dbengine/wal"
	"github.com/ankur-anand/kvalchemy/dbengine/wal/walrecord"
	"github.com/bits-and-blooms/bloom/v3"
	"github.com/brianvoe/gofakeit/v7"
	"github.com/hashicorp/go-metrics"
	"github.com/stretchr/testify/assert"
)

func TestWalRecovery(t *testing.T) {
	tdir := t.TempDir()
	walDir := filepath.Join(tdir, "wal_test")
	err := os.MkdirAll(walDir, 0777)
	assert.NoError(t, err)

	walConfig := wal.NewDefaultConfig()
	//walConfig.FSync = true
	//walConfig.SyncInterval = 0
	//walConfig.BytesPerSync = 0
	walInstance, err := wal.NewWalIO(walDir, testNamespace, walConfig, metrics.Default())
	assert.NoError(t, err)
	t.Cleanup(func() {
		err := walInstance.Close()
		assert.NoError(t, err, "failed to close wal")
	})

	dbFile := filepath.Join(tdir, "test_flush.db")

	db, err := kvdb.NewLmdb(dbFile, kvdb.Config{
		Namespace: testNamespace,
		NoSync:    true,
		MmapSize:  1 << 30,
	})

	assert.NoError(t, err)
	t.Cleanup(func() {
		err := db.Close()
		assert.NoError(t, err, "failed to close db")
	})

	allCommitedKeys := make(map[string]struct{})
	unCommitedKeys := make(map[string]struct{})
	allCommitedDeleteKeys := make(map[string]struct{})
	bloomFilter := bloom.NewWithEstimates(1_000_000, 0.0001)
	// 50 full insert value.
	recordCount := 50
	kv := generateNFBRecord(t, uint64(recordCount))

	for k, v := range kv {
		_, err := walInstance.Append(v)
		assert.NoError(t, err)
		allCommitedKeys[k] = struct{}{}
	}

	// 9 chunked Value. One is start record
	recordCount = 10
	key, values, checksum := generateNChunkFBRecord(t, uint64(recordCount))
	var lastOffset *wal.Offset
	for _, value := range values {
		value.PrevTxnOffset = lastOffset
		encoded, err := value.FBEncode()
		assert.NoError(t, err)
		offset, err := walInstance.Append(encoded)
		assert.NoError(t, err)
		lastOffset = offset
	}

	lastRecord := walrecord.Record{
		Index:         0,
		Hlc:           0,
		Key:           []byte(key),
		Value:         marshalChecksum(checksum),
		LogOperation:  walrecord.LogOperationInsert,
		EntryType:     walrecord.EntryTypeChunked,
		TxnStatus:     walrecord.TxnStatusCommit,
		TxnID:         []byte(key),
		PrevTxnOffset: lastOffset,
	}

	encoded, err := lastRecord.FBEncode()
	assert.NoError(t, err)
	lastOffset, err = walInstance.Append(encoded)
	assert.NoError(t, err)
	allCommitedKeys[key] = struct{}{}

	recoveryInstance := &walRecovery{
		store: db,
		walIO: walInstance,
		bloom: bloomFilter,
	}

	t.Run("full_value_insert_chunk_recovery", func(t *testing.T) {
		err := recoveryInstance.recoverWAL()
		assert.NoError(t, err, "failed to recover wal")
		assert.Equal(t, *recoveryInstance.lastRecoveredPos, *lastOffset)

		// save this to metadata table
		err = SaveMetadata(db, lastOffset, 61)
		assert.NoError(t, err, "failed to save metadata")

		err = recoveryInstance.recoverWAL()
		assert.NoError(t, err, "failed to recover wal")

		assert.Equal(t, recoveryInstance.recoveredCount, 61, "60 record + 1 txn commit record")
		assert.Equal(t, *recoveryInstance.lastRecoveredPos, *lastOffset)
		for k := range allCommitedKeys {
			value, err := db.Get([]byte(k))
			assert.NoError(t, err)
			assert.NotNil(t, value)
		}
	})

	t.Run("chunked_uncommited_not_recovered", func(t *testing.T) {
		recordCount = 10
		key, newVal, _ := generateNChunkFBRecord(t, uint64(recordCount))
		var lastOff *wal.Offset
		for _, value := range newVal {
			value.PrevTxnOffset = lastOff
			encoded, err := value.FBEncode()
			assert.NoError(t, err)
			offset, err := walInstance.Append(encoded)
			assert.NoError(t, err)
			lastOff = offset
		}
		lastOffset = lastOff
		err = recoveryInstance.recoverWAL()
		unCommitedKeys[key] = struct{}{}
		assert.NoError(t, err, "failed to recover wal")
		assert.Equal(t, recoveryInstance.recoveredCount, 61, "60 record + 1 txn commit record")
		assert.Equal(t, *recoveryInstance.lastRecoveredPos, *lastOffset)
		for k := range allCommitedKeys {
			value, err := db.Get([]byte(k))
			assert.NoError(t, err)
			assert.NotNil(t, value)
		}

		for k := range unCommitedKeys {
			value, err := db.Get([]byte(k))
			assert.ErrorIs(t, err, kvdb.ErrKeyNotFound)
			assert.Nil(t, value)
		}

	})

	t.Run("full_commited_insert_recovered", func(t *testing.T) {
		recordCount = 10
		_, nkv, checksum := generateNChunkFBRecord(t, uint64(recordCount))

		txID := gofakeit.UUID()
		var lastOffset *wal.Offset
		for _, value := range nkv {
			value.PrevTxnOffset = lastOffset
			value.Key = []byte(gofakeit.UUID())
			value.TxnID = []byte(txID)
			value.EntryType = walrecord.EntryTypeKV
			encoded, err := value.FBEncode()
			assert.NoError(t, err)
			offset, err := walInstance.Append(encoded)
			assert.NoError(t, err)
			lastOffset = offset
			if value.TxnStatus == walrecord.TxnStatusPrepare {
				allCommitedKeys[string(value.Key)] = struct{}{}
			}
		}

		lastRecord := walrecord.Record{
			Index:         0,
			Hlc:           0,
			Key:           nil,
			Value:         marshalChecksum(checksum),
			LogOperation:  walrecord.LogOperationInsert,
			EntryType:     walrecord.EntryTypeKV,
			TxnStatus:     walrecord.TxnStatusCommit,
			TxnID:         []byte(txID),
			PrevTxnOffset: lastOffset,
		}

		encoded, err := lastRecord.FBEncode()
		assert.NoError(t, err)
		lastOffset, err = walInstance.Append(encoded)
		assert.NoError(t, err)

		err = recoveryInstance.recoverWAL()
		assert.NoError(t, err, "failed to recover wal")
		assert.Equal(t, *recoveryInstance.lastRecoveredPos, *lastOffset)
		assert.Equal(t, 72, recoveryInstance.recoveredCount, "60+10 record + 1 txn commit record")
		for k := range allCommitedKeys {
			value, err := db.Get([]byte(k))
			assert.NoError(t, err, "key %s should be present", k)
			assert.NotNil(t, value, "key %s should not be nil", k)
		}
	})

	t.Run("full_uncommited_not_recovered_without_savepoint", func(t *testing.T) {
		recordCount = 10
		_, nkv, _ := generateNChunkFBRecord(t, uint64(recordCount))

		txID := gofakeit.UUID()
		var lOffset *wal.Offset
		for _, value := range nkv {
			value.PrevTxnOffset = lOffset
			value.Key = []byte(gofakeit.UUID())
			value.TxnID = []byte(txID)
			value.EntryType = walrecord.EntryTypeKV
			encoded, err := value.FBEncode()
			assert.NoError(t, err)
			offset, err := walInstance.Append(encoded)
			assert.NoError(t, err)
			lOffset = offset
			if value.TxnStatus == walrecord.TxnStatusPrepare {
				unCommitedKeys[string(value.Key)] = struct{}{}
			}
		}

		lastOffset = lOffset

		err = recoveryInstance.recoverWAL()
		assert.NoError(t, err, "failed to recover wal")
		assert.Equal(t, *recoveryInstance.lastRecoveredPos, *lastOffset)
		// as there was no savepoint, last 11 will be recovered again.
		assert.Equal(t, 83, recoveryInstance.recoveredCount, "total record recovered failed")
		err = SaveMetadata(db, lastOffset, 83)
		assert.NoError(t, err, "failed to save metadata")
	})

	t.Run("validate btree dataStore", func(t *testing.T) {
		assert.NoError(t, db.FSync())
		for k := range allCommitedKeys {
			value, err := db.Get([]byte(k))
			assert.NoError(t, err)
			assert.NotNil(t, value)
		}

		for k := range unCommitedKeys {
			value, err := db.Get([]byte(k))
			assert.ErrorIs(t, err, kvdb.ErrKeyNotFound)
			assert.Nil(t, value)
		}
	})

	t.Run("delete_key_recovery", func(t *testing.T) {
		key := gofakeit.RandomMapKey(allCommitedKeys)
		record := walrecord.Record{
			Index:         0,
			Hlc:           0,
			Key:           []byte(key.(string)),
			Value:         nil,
			LogOperation:  walrecord.LogOperationDelete,
			TxnStatus:     walrecord.TxnStatusTxnNone,
			PrevTxnOffset: nil,
		}

		encoded, err := record.FBEncode()
		assert.NoError(t, err)
		offset, err := walInstance.Append(encoded)
		assert.NoError(t, err)
		lastOffset = offset

		recovery := &walRecovery{
			store: db,
			walIO: walInstance,
			bloom: bloomFilter,
		}
		err = recovery.recoverWAL()
		assert.NoError(t, err, "failed to recover wal")
		assert.Equal(t, *recovery.lastRecoveredPos, *lastOffset)
		assert.Equal(t, recovery.recoveredCount, 1)
		value, err := db.Get([]byte(key.(string)))
		assert.ErrorIs(t, err, kvdb.ErrKeyNotFound)
		assert.Nil(t, value, "deleted key value should be nil")
		delete(allCommitedKeys, key.(string))
		allCommitedDeleteKeys[key.(string)] = struct{}{}
	})

	t.Run("delete_many_keys_recovery", func(t *testing.T) {
		assert.NoError(t, walInstance.Sync())
		assert.NoError(t, db.FSync())
		txID := gofakeit.UUID()
		deleteKeys := make([]string, 0)
		for i := 0; i < 10; i++ {
			key := gofakeit.RandomMapKey(allCommitedKeys)
			deleteKeys = append(deleteKeys, key.(string))
		}

		_, nkv, _ := generateNChunkFBRecord(t, uint64(10))

		var lastOffset *wal.Offset
		startRecord := walrecord.Record{
			Index:         0,
			Hlc:           0,
			Key:           nil,
			Value:         nil,
			LogOperation:  walrecord.LogOperationTxnMarker,
			EntryType:     walrecord.EntryTypeKV,
			TxnStatus:     walrecord.TxnStatusBegin,
			TxnID:         []byte(txID),
			PrevTxnOffset: nil,
		}

		encoded, err := startRecord.FBEncode()
		assert.NoError(t, err)
		offset, err := walInstance.Append(encoded)
		assert.NoError(t, err)
		lastOffset = offset

		for i, value := range nkv {
			value.PrevTxnOffset = lastOffset
			value.Key = []byte(deleteKeys[i])
			value.TxnID = []byte(txID)
			value.EntryType = walrecord.EntryTypeKV
			value.LogOperation = walrecord.LogOperationDelete

			encoded, err := value.FBEncode()
			assert.NoError(t, err)
			offset, err := walInstance.Append(encoded)
			assert.NoError(t, err)
			lastOffset = offset
		}

		record := walrecord.Record{
			Index:         0,
			Hlc:           0,
			Key:           nil,
			Value:         nil,
			LogOperation:  walrecord.LogOperationDelete,
			TxnStatus:     walrecord.TxnStatusCommit,
			PrevTxnOffset: lastOffset,
		}

		encoded, err = record.FBEncode()
		assert.NoError(t, err)
		offset, err = walInstance.Append(encoded)
		assert.NoError(t, err)
		lastOffset = offset
		recovery := &walRecovery{
			store: db,
			walIO: walInstance,
			bloom: bloomFilter,
		}
		err = recovery.recoverWAL()
		assert.NoError(t, err, "failed to recover wal")
		assert.Equal(t, *recovery.lastRecoveredPos, *lastOffset)
		// last test op of single delete is also not saved, so 12 + 1
		assert.Equal(t, recovery.recoveredCount, 13)
		assert.NoError(t, db.FSync())

		for _, key := range deleteKeys {
			value, err := db.Get([]byte(key))
			assert.ErrorIs(t, err, kvdb.ErrKeyNotFound, "%s", key)
			assert.Nil(t, value, "deleted key value should be nil %s", key)
			allCommitedDeleteKeys[key] = struct{}{}
			delete(allCommitedKeys, key)
		}
	})

	t.Run("btree_store_validator", func(t *testing.T) {
		for k := range allCommitedKeys {
			value, err := db.Get([]byte(k))
			assert.NoError(t, err)
			assert.NotNil(t, value)
		}

		for k := range unCommitedKeys {
			value, err := db.Get([]byte(k))
			assert.ErrorIs(t, err, kvdb.ErrKeyNotFound)
			assert.Nil(t, value)
		}

		for key := range allCommitedDeleteKeys {
			value, err := db.Get([]byte(key))
			assert.ErrorIs(t, err, kvdb.ErrKeyNotFound)
			assert.Nil(t, value, "deleted key value should be nil %s", key)
		}
	})

}
