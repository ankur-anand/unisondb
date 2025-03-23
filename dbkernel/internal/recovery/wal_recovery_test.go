package recovery

import (
	"hash/crc32"
	"os"
	"path/filepath"
	"testing"

	"github.com/ankur-anand/unisondb/dbkernel/internal"
	"github.com/ankur-anand/unisondb/dbkernel/internal/kvdrivers"
	"github.com/ankur-anand/unisondb/dbkernel/internal/wal"
	"github.com/ankur-anand/unisondb/internal/logcodec"
	"github.com/ankur-anand/unisondb/schemas/logrecord"
	"github.com/bits-and-blooms/bloom/v3"
	"github.com/brianvoe/gofakeit/v7"
	"github.com/hashicorp/go-metrics"
	"github.com/stretchr/testify/assert"
)

var (
	testNamespace = "test_namespace"
)

func TestWalRecoveryForKV(t *testing.T) {
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

	db, err := kvdrivers.NewLmdb(dbFile, kvdrivers.Config{
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
	totalRecordCount := 0
	// 50 full insert value.
	recordCount := 50
	records, kv := generateNKeyValueFBRecord(uint64(recordCount))

	for k := range kv {
		allCommitedKeys[k] = struct{}{}

	}
	for _, record := range records {
		_, err := walInstance.Append(record)
		assert.NoError(t, err)
	}

	totalRecordCount = recordCount
	t.Run("key_value_txn_none_recovery", func(t *testing.T) {
		recoveryInstance := &walRecovery{
			store: db,
			walIO: walInstance,
			bloom: bloomFilter,
		}
		err := recoveryInstance.recoverWAL(nil)
		assert.NoError(t, err)
		assert.Equal(t, 50, recoveryInstance.recoveredCount, "recovered count should be 10")
	})

	// 9 chunked Value. One is start record
	recordCount = 10
	key, values, checksum := generateNChunkFBRecord(uint64(recordCount))
	var lastOffset *wal.Offset
	for _, value := range values {
		if lastOffset != nil {
			value.PrevTxnWalIndex = lastOffset.Encode()
		}

		encoded := value.FBEncode(1024)

		offset, err := walInstance.Append(encoded)
		assert.NoError(t, err)
		lastOffset = offset
	}

	totalRecordCount += recordCount
	encoded := logcodec.SerializeKVEntry([]byte(key), nil)
	lastRecord := logcodec.LogRecord{
		LSN:             0,
		HLC:             0,
		OperationType:   logrecord.LogOperationTypeInsert,
		EntryType:       logrecord.LogEntryTypeChunked,
		TxnState:        logrecord.TransactionStateCommit,
		TxnID:           []byte(key),
		PrevTxnWalIndex: lastOffset.Encode(),
		CRC32Checksum:   checksum,
		Entries:         [][]byte{encoded},
	}

	encoded = lastRecord.FBEncode(1024)
	lastOffset, err = walInstance.Append(encoded)
	assert.NoError(t, err)
	allCommitedKeys[key] = struct{}{}

	var checkpoint []byte
	totalRecordCount++

	t.Run("chunk_recovery", func(t *testing.T) {
		recoveryInstance := &walRecovery{
			store: db,
			walIO: walInstance,
			bloom: bloomFilter,
		}
		err := recoveryInstance.recoverWAL(nil)
		assert.NoError(t, err, "failed to recover wal")
		assert.Equal(t, *recoveryInstance.lastRecoveredPos, *lastOffset)

		//save this to metadata table
		err = internal.SaveMetadata(db, lastOffset, uint64(totalRecordCount))
		assert.NoError(t, err, "failed to save metadata")

		metaData := internal.Metadata{
			RecordProcessed: uint64(totalRecordCount),
			Pos:             lastOffset,
		}
		checkpoint = metaData.MarshalBinary()

		err = recoveryInstance.recoverWAL(checkpoint)
		assert.NoError(t, err, "failed to recover wal")

		assert.Equal(t, totalRecordCount, recoveryInstance.recoveredCount, "60 record + 1 txn commit record")
		assert.Equal(t, *recoveryInstance.lastRecoveredPos, *lastOffset)
		for k := range allCommitedKeys {
			value, err := db.Get([]byte(k))
			assert.NoError(t, err)
			assert.NotNil(t, value)
		}
	})

	t.Run("chunked_uncommited_not_recovered", func(t *testing.T) {
		recoveryInstance := &walRecovery{
			store: db,
			walIO: walInstance,
			bloom: bloomFilter,
		}

		recordCount = 10
		key, newVal, _ := generateNChunkFBRecord(uint64(recordCount))
		var lastOff *wal.Offset
		for _, value := range newVal {
			if lastOff != nil {
				value.PrevTxnWalIndex = lastOff.Encode()
			}

			encoded := value.FBEncode(1024)
			assert.NoError(t, err)
			offset, err := walInstance.Append(encoded)
			assert.NoError(t, err)
			lastOff = offset
		}
		lastOffset = lastOff
		err = recoveryInstance.recoverWAL(checkpoint)
		unCommitedKeys[key] = struct{}{}
		assert.NoError(t, err, "failed to recover wal")
		assert.Equal(t, 0, recoveryInstance.recoveredCount, "0")
		assert.Equal(t, *recoveryInstance.lastRecoveredPos, *lastOffset)
		for k := range allCommitedKeys {
			value, err := db.Get([]byte(k))
			assert.NoError(t, err)
			assert.NotNil(t, value)
		}

		for k := range unCommitedKeys {
			value, err := db.Get([]byte(k))
			assert.ErrorIs(t, err, kvdrivers.ErrKeyNotFound)
			assert.Nil(t, value)
		}

	})

	t.Run("kv_commited_insert_recovered", func(t *testing.T) {
		recordCount = 10
		logRecords, kvDB := generateNTxnKeyValueFBRecord(uint64(recordCount))

		txID := gofakeit.UUID()
		var lastOffset *wal.Offset
		for _, value := range logRecords {
			if lastOffset != nil {
				value.PrevTxnWalIndex = lastOffset.Encode()
			}
			encoded := value.FBEncode(1024)
			offset, err := walInstance.Append(encoded)
			assert.NoError(t, err)
			lastOffset = offset
		}

		for key := range kvDB {
			allCommitedKeys[key] = struct{}{}
		}

		lastRecord := logcodec.LogRecord{
			LSN:             0,
			HLC:             0,
			CRC32Checksum:   0,
			OperationType:   logrecord.LogOperationTypeInsert,
			TxnState:        logrecord.TransactionStateCommit,
			EntryType:       logrecord.LogEntryTypeKV,
			TxnID:           []byte(txID),
			PrevTxnWalIndex: lastOffset.Encode(),
			Entries:         nil,
		}

		encoded := lastRecord.FBEncode(1024)
		lastOffset, err = walInstance.Append(encoded)
		assert.NoError(t, err)

		recoveryInstance := &walRecovery{
			store: db,
			walIO: walInstance,
			bloom: bloomFilter,
		}

		err = recoveryInstance.recoverWAL(checkpoint)
		assert.NoError(t, err, "failed to recover wal")
		assert.Equal(t, *recoveryInstance.lastRecoveredPos, *lastOffset)
		assert.Equal(t, 11, recoveryInstance.recoveredCount, "61+10 record + 1 txn commit record")
		for k := range allCommitedKeys {
			value, err := db.Get([]byte(k))
			assert.NoError(t, err, "key %s should be present", k)
			assert.NotNil(t, value, "key %s should not be nil", k)
		}

		totalRecordCount += 11
		metaData := internal.Metadata{
			RecordProcessed: uint64(totalRecordCount),
			Pos:             lastOffset,
		}
		checkpoint = metaData.MarshalBinary()
	})

	t.Run("kv_uncommited_not_recovered", func(t *testing.T) {
		recordCount = 10
		logRecords, _ := generateNTxnKeyValueFBRecord(uint64(recordCount))

		var lastOffset *wal.Offset
		for _, value := range logRecords {
			if lastOffset != nil {
				value.PrevTxnWalIndex = lastOffset.Encode()
			}
			encoded := value.FBEncode(1024)
			offset, err := walInstance.Append(encoded)
			assert.NoError(t, err)
			lastOffset = offset
		}

		recoveryInstance := &walRecovery{
			store: db,
			walIO: walInstance,
			bloom: bloomFilter,
		}

		err = recoveryInstance.recoverWAL(checkpoint)
		assert.NoError(t, err, "failed to recover wal")
		assert.Equal(t, *recoveryInstance.lastRecoveredPos, *lastOffset)
		assert.Equal(t, 0, recoveryInstance.recoveredCount, "total record recovered failed")
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
			assert.ErrorIs(t, err, kvdrivers.ErrKeyNotFound)
			assert.Nil(t, value)
		}
	})

	t.Run("delete_key_recovery", func(t *testing.T) {
		var deleteKeys []string
		for i := 0; i < 5; i++ {
			key := gofakeit.RandomMapKey(allCommitedKeys)
			deleteKeys = append(deleteKeys, key.(string))
		}

		var encodedEntries [][]byte
		for _, key := range deleteKeys {
			ee := logcodec.SerializeKVEntry([]byte(key), nil)
			encodedEntries = append(encodedEntries, ee)
		}

		record := logcodec.LogRecord{
			LSN:           0,
			HLC:           0,
			OperationType: logrecord.LogOperationTypeDelete,
			TxnState:      logrecord.TransactionStateNone,
			EntryType:     logrecord.LogEntryTypeKV,
			Entries:       encodedEntries,
		}

		encoded := record.FBEncode(1024)
		offset, err := walInstance.Append(encoded)
		assert.NoError(t, err)
		lastOffset = offset

		recovery := &walRecovery{
			store: db,
			walIO: walInstance,
			bloom: bloomFilter,
		}
		err = recovery.recoverWAL(checkpoint)
		assert.NoError(t, err, "failed to recover wal")
		assert.Equal(t, *recovery.lastRecoveredPos, *lastOffset)
		assert.Equal(t, 1, recovery.recoveredCount)
		for _, key := range deleteKeys {
			value, err := db.Get([]byte(key))
			assert.ErrorIs(t, err, kvdrivers.ErrKeyNotFound)
			assert.Nil(t, value, "deleted key value should be nil")
			delete(allCommitedKeys, key)
			allCommitedDeleteKeys[key] = struct{}{}
		}
	})

	t.Run("delete_txn_recovery", func(t *testing.T) {
		assert.NoError(t, walInstance.Sync())
		assert.NoError(t, db.FSync())
		txID := gofakeit.UUID()

		var lastOffset *wal.Offset
		startRecord := logcodec.LogRecord{
			LSN:             0,
			HLC:             0,
			CRC32Checksum:   0,
			OperationType:   logrecord.LogOperationTypeTxnMarker,
			TxnState:        logrecord.TransactionStateBegin,
			EntryType:       logrecord.LogEntryTypeKV,
			TxnID:           []byte(txID),
			PrevTxnWalIndex: nil,
			Entries:         nil,
		}

		lastOffset, err = walInstance.Append(startRecord.FBEncode(1024))
		assert.NoError(t, err, "failed to append start record")

		deleteKeys := make([]string, 0)
		for i := 0; i < 10; i++ {
			key := gofakeit.RandomMapKey(allCommitedKeys).(string)
			deleteKeys = append(deleteKeys, key)
			kvEncoded := logcodec.SerializeKVEntry([]byte(key), nil)
			record := logcodec.LogRecord{
				LSN:             uint64(i),
				HLC:             uint64(i),
				OperationType:   logrecord.LogOperationTypeDelete,
				TxnState:        logrecord.TransactionStatePrepare,
				EntryType:       logrecord.LogEntryTypeKV,
				Entries:         [][]byte{kvEncoded},
				PrevTxnWalIndex: lastOffset.Encode(),
			}
			encoded := record.FBEncode(1024)
			lastOffset, err = walInstance.Append(encoded)
			assert.NoError(t, err)
		}

		record := logcodec.LogRecord{
			LSN:             uint64(11),
			HLC:             uint64(11),
			OperationType:   logrecord.LogOperationTypeDelete,
			TxnState:        logrecord.TransactionStateCommit,
			EntryType:       logrecord.LogEntryTypeKV,
			PrevTxnWalIndex: lastOffset.Encode(),
		}

		encoded = record.FBEncode(1024)
		lastOffset, err = walInstance.Append(encoded)
		assert.NoError(t, err)

		recovery := &walRecovery{
			store: db,
			walIO: walInstance,
			bloom: bloomFilter,
		}
		err = recovery.recoverWAL(checkpoint)
		assert.NoError(t, err, "failed to recover wal")
		assert.Equal(t, *recovery.lastRecoveredPos, *lastOffset)
		// last test op of single delete is also not saved, so 12 + 1
		assert.Equal(t, recovery.recoveredCount, 13)
		assert.NoError(t, db.FSync())

		for _, key := range deleteKeys {
			value, err := db.Get([]byte(key))
			assert.ErrorIs(t, err, kvdrivers.ErrKeyNotFound, "%s", key)
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
			assert.ErrorIs(t, err, kvdrivers.ErrKeyNotFound)
			assert.Nil(t, value)
		}

		for key := range allCommitedDeleteKeys {
			value, err := db.Get([]byte(key))
			assert.ErrorIs(t, err, kvdrivers.ErrKeyNotFound)
			assert.Nil(t, value, "deleted key value should be nil %s", key)
		}
	})

}

func generateNChunkFBRecord(n uint64) (string, []logcodec.LogRecord, uint32) {
	txnID := gofakeit.UUID()
	key := gofakeit.UUID()
	values := make([]logcodec.LogRecord, 0, n)

	encoded := logcodec.SerializeKVEntry([]byte(key), nil)
	startRecord := logcodec.LogRecord{
		LSN:           0,
		HLC:           0,
		CRC32Checksum: 0,
		OperationType: logrecord.LogOperationTypeTxnMarker,
		TxnState:      logrecord.TransactionStateBegin,
		EntryType:     logrecord.LogEntryTypeChunked,
		TxnID:         []byte(txnID),
		Entries:       [][]byte{encoded},
	}

	values = append(values, startRecord)

	var checksum uint32
	for i := uint64(1); i < n; i++ {
		val := gofakeit.LetterN(110)
		checksum = crc32.Update(checksum, crc32.IEEETable, []byte(val))
		encoded := logcodec.SerializeKVEntry(nil, []byte(val))
		record := logcodec.LogRecord{
			LSN:           i,
			HLC:           i,
			CRC32Checksum: 0,
			OperationType: logrecord.LogOperationTypeInsert,
			TxnState:      logrecord.TransactionStatePrepare,
			EntryType:     logrecord.LogEntryTypeChunked,
			TxnID:         []byte(txnID),
			Entries:       [][]byte{encoded},
		}

		values = append(values, record)
	}

	return key, values, checksum
}

func generateNTxnKeyValueFBRecord(n uint64) ([]logcodec.LogRecord, map[string][]byte) {
	txnID := gofakeit.UUID()
	values := make([]logcodec.LogRecord, 0, n)

	keyValuesDB := make(map[string][]byte)
	startRecord := logcodec.LogRecord{
		LSN:           0,
		HLC:           0,
		CRC32Checksum: 0,
		OperationType: logrecord.LogOperationTypeTxnMarker,
		TxnState:      logrecord.TransactionStateBegin,
		EntryType:     logrecord.LogEntryTypeKV,
		TxnID:         []byte(txnID),
		Entries:       nil,
	}

	values = append(values, startRecord)

	for i := uint64(1); i < n; i++ {
		var encodedVals [][]byte

		for j := uint64(0); j < i; j++ {
			key := gofakeit.UUID()
			val := gofakeit.LetterN(110)
			keyValuesDB[key] = []byte(val)
			encoded := logcodec.SerializeKVEntry([]byte(key), []byte(val))
			encodedVals = append(encodedVals, encoded)
		}

		record := logcodec.LogRecord{
			LSN:           i,
			HLC:           i,
			OperationType: logrecord.LogOperationTypeInsert,
			TxnState:      logrecord.TransactionStatePrepare,
			EntryType:     logrecord.LogEntryTypeKV,
			TxnID:         []byte(txnID),
			Entries:       encodedVals,
		}

		values = append(values, record)
	}

	return values, keyValuesDB
}

func generateNKeyValueFBRecord(n uint64) ([][]byte, map[string][]byte) {
	kv := make(map[string][]byte)
	var records [][]byte
	for i := uint64(1); i <= n; i++ {
		var encodedVals [][]byte

		for j := uint64(0); j < i; j++ {
			key := gofakeit.UUID()
			val := gofakeit.LetterN(110)
			kv[key] = []byte(val)
			encoded := logcodec.SerializeKVEntry([]byte(key), []byte(val))
			encodedVals = append(encodedVals, encoded)
		}

		record := logcodec.LogRecord{
			LSN:             i,
			HLC:             i,
			CRC32Checksum:   0,
			OperationType:   logrecord.LogOperationTypeInsert,
			TxnState:        logrecord.TransactionStateNone,
			EntryType:       logrecord.LogEntryTypeKV,
			TxnID:           []byte(gofakeit.UUID()),
			Entries:         encodedVals,
			PrevTxnWalIndex: nil,
		}

		encoded := record.FBEncode(1024)
		records = append(records, encoded)
	}

	return records, kv
}
