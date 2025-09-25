package recovery

import (
	"hash/crc32"
	"os"
	"path/filepath"
	"testing"

	"github.com/ankur-anand/unisondb/dbkernel/internal"
	"github.com/ankur-anand/unisondb/dbkernel/internal/wal"
	"github.com/ankur-anand/unisondb/internal/logcodec"
	kvdrivers2 "github.com/ankur-anand/unisondb/pkg/kvdrivers"
	"github.com/ankur-anand/unisondb/schemas/logrecord"
	"github.com/bits-and-blooms/bloom/v3"
	"github.com/brianvoe/gofakeit/v7"
	"github.com/stretchr/testify/assert"
)

var (
	testNamespace = "test_namespace"
)

func TestWalRecoveryForKV_Row(t *testing.T) {
	tdir := t.TempDir()
	walDir := filepath.Join(tdir, "wal_test")
	err := os.MkdirAll(walDir, 0777)
	assert.NoError(t, err)

	walConfig := wal.NewDefaultConfig()
	//walConfig.FSync = true
	//walConfig.SyncInterval = 0
	//walConfig.BytesPerSync = 0
	walInstance, err := wal.NewWalIO(walDir, testNamespace, walConfig)
	assert.NoError(t, err)
	t.Cleanup(func() {
		err := walInstance.Close()
		assert.NoError(t, err, "failed to close wal")
	})

	dbFile := filepath.Join(tdir, "test_flush.db")

	db, err := kvdrivers2.NewLmdb(dbFile, kvdrivers2.Config{
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
			chunks, chErr := db.GetLOBChunks([]byte(k))
			if chErr == nil {
				assert.Greater(t, len(chunks), 0)
				continue
			}
			// not chunked, must be KV
			value, kvErr := db.GetKV([]byte(k))
			assert.NoError(t, kvErr)
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
			chunks, chErr := db.GetLOBChunks([]byte(k))
			if chErr == nil {
				assert.Greater(t, len(chunks), 0)
				continue
			}
			// not chunked, must be KV
			value, kvErr := db.GetKV([]byte(k))
			assert.NoError(t, kvErr)
			assert.NotNil(t, value)
		}

		for k := range unCommitedKeys {
			value, err := db.GetKV([]byte(k))
			assert.ErrorIs(t, err, kvdrivers2.ErrKeyNotFound)
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
			chunks, chErr := db.GetLOBChunks([]byte(k))
			if chErr == nil {
				assert.Greater(t, len(chunks), 0)
				continue
			}
			// not chunked, must be KV
			value, kvErr := db.GetKV([]byte(k))
			assert.NoError(t, kvErr)
			assert.NotNil(t, value)
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
			chunks, chErr := db.GetLOBChunks([]byte(k))
			if chErr == nil {
				assert.Greater(t, len(chunks), 0)
				continue
			}
			// not chunked, must be KV
			value, kvErr := db.GetKV([]byte(k))
			assert.NoError(t, kvErr)
			assert.NotNil(t, value)
		}

		for k := range unCommitedKeys {
			value, err := db.GetKV([]byte(k))
			assert.ErrorIs(t, err, kvdrivers2.ErrKeyNotFound)
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
			value, err := db.GetKV([]byte(key))
			assert.ErrorIs(t, err, kvdrivers2.ErrKeyNotFound)
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
			value, err := db.GetKV([]byte(key))
			assert.ErrorIs(t, err, kvdrivers2.ErrKeyNotFound, "%s", key)
			assert.Nil(t, value, "deleted key value should be nil %s", key)
			allCommitedDeleteKeys[key] = struct{}{}
			delete(allCommitedKeys, key)
		}
	})

	var insertedRows map[string]map[string][]byte

	t.Run("row_insert", func(t *testing.T) {
		recordCount = 50
		rowRecords, rowKV := generateNRowColumnFBRecord(uint64(recordCount))
		insertedRows = rowKV

		var lastOffset *wal.Offset
		for _, record := range rowRecords {
			offset, err := walInstance.Append(record)
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
		assert.Equal(t, 63, recoveryInstance.recoveredCount, "total record recovered failed")

		for rKey, rValue := range insertedRows {
			columns, err := db.ScanRowCells([]byte(rKey), nil)
			assert.NoError(t, err, "failed to read row columns")
			assert.Equal(t, len(rValue), len(columns))
			assert.Equal(t, rValue, columns)
		}
	})

	t.Run("delete_row_by_key", func(t *testing.T) {
		randomRow := gofakeit.RandomMapKey(insertedRows).(string)
		delete(insertedRows, randomRow)

		encoded := logcodec.SerializeKVEntry([]byte(randomRow), nil)
		record := logcodec.LogRecord{
			LSN:           0,
			HLC:           0,
			OperationType: logrecord.LogOperationTypeDeleteRowByKey,
			TxnState:      logrecord.TransactionStateNone,
			EntryType:     logrecord.LogEntryTypeRow,
			Entries:       [][]byte{encoded},
		}

		lastOffset, err := walInstance.Append(record.FBEncode(1024))
		assert.NoError(t, err)
		recoveryInstance := &walRecovery{
			store: db,
			walIO: walInstance,
			bloom: bloomFilter,
		}

		err = recoveryInstance.recoverWAL(checkpoint)
		assert.NoError(t, err, "failed to recover wal")
		assert.Equal(t, *recoveryInstance.lastRecoveredPos, *lastOffset)
		assert.Equal(t, 64, recoveryInstance.recoveredCount, "total record recovered failed")

		columns, err := db.ScanRowCells([]byte(randomRow), nil)
		assert.ErrorIs(t, err, kvdrivers2.ErrKeyNotFound)
		assert.Empty(t, columns)
	})

	t.Run("delete_columns_in_row", func(t *testing.T) {
		randomRow := gofakeit.RandomMapKey(insertedRows).(string)
		deletedColumns := make(map[string]struct{})
		var encoded [][]byte
		for i := 0; i < 5; i++ {
			randomColumnName := gofakeit.RandomMapKey(insertedRows[randomRow]).(string)
			deletedColumns[randomColumnName] = struct{}{}
			columnToDelete := map[string][]byte{
				randomColumnName: nil,
			}
			encoded = append(encoded, logcodec.SerializeRowUpdateEntry([]byte(randomRow), columnToDelete))
		}

		record := logcodec.LogRecord{
			LSN:           0,
			HLC:           0,
			OperationType: logrecord.LogOperationTypeDelete,
			TxnState:      logrecord.TransactionStateNone,
			EntryType:     logrecord.LogEntryTypeRow,
			Entries:       encoded,
		}

		lastOffset, err := walInstance.Append(record.FBEncode(1024))
		assert.NoError(t, err)
		recoveryInstance := &walRecovery{
			store: db,
			walIO: walInstance,
			bloom: bloomFilter,
		}

		err = recoveryInstance.recoverWAL(checkpoint)
		assert.NoError(t, err, "failed to recover wal")
		assert.Equal(t, *recoveryInstance.lastRecoveredPos, *lastOffset)
		assert.Equal(t, 65, recoveryInstance.recoveredCount, "total record recovered failed")

		columns, err := db.ScanRowCells([]byte(randomRow), nil)
		assert.NoError(t, err, "failed to read row columns")
		assert.NotContains(t, columns, deletedColumns)
		metaData := internal.Metadata{
			RecordProcessed: uint64(totalRecordCount),
			Pos:             lastOffset,
		}
		checkpoint = metaData.MarshalBinary()
	})

	var cleanupLogRecords []logcodec.LogRecord
	var cleanupKVEntries map[string]map[string][]byte
	t.Run("row_txn_insert", func(t *testing.T) {
		recordCount = 10
		logRecords, kvDB := generateNRowColumnFBRecordTxn(10)
		cleanupKVEntries = kvDB
		cleanupLogRecords = logRecords

		var lastOffset *wal.Offset
		for _, record := range logRecords {
			if lastOffset != nil {
				record.PrevTxnWalIndex = lastOffset.Encode()
			}

			offset, err := walInstance.Append(record.FBEncode(1024))
			assert.NoError(t, err)
			lastOffset = offset
		}

		lastRecord := logcodec.LogRecord{
			LSN:             0,
			HLC:             0,
			OperationType:   logrecord.LogOperationTypeInsert,
			TxnState:        logrecord.TransactionStateCommit,
			EntryType:       logrecord.LogEntryTypeRow,
			PrevTxnWalIndex: lastOffset.Encode(),
		}

		lastOffset, err = walInstance.Append(lastRecord.FBEncode(1024))

		recoveryInstance := &walRecovery{
			store: db,
			walIO: walInstance,
			bloom: bloomFilter,
		}

		err = recoveryInstance.recoverWAL(checkpoint)
		assert.NoError(t, err, "failed to recover wal")
		assert.Equal(t, *recoveryInstance.lastRecoveredPos, *lastOffset)
		assert.Equal(t, 11, recoveryInstance.recoveredCount, "total record recovered failed")
		for rKey, rValue := range kvDB {
			columns, err := db.ScanRowCells([]byte(rKey), nil)
			assert.NoError(t, err, "failed to read row columns")
			assert.Equal(t, len(rValue), len(columns))
			assert.Equal(t, rValue, columns)
		}
	})

	t.Run("row_txn_delete", func(t *testing.T) {

		var lastOffset *wal.Offset
		for _, record := range cleanupLogRecords {
			if lastOffset != nil {
				record.PrevTxnWalIndex = lastOffset.Encode()
			}

			record.OperationType = logrecord.LogOperationTypeDelete

			offset, err := walInstance.Append(record.FBEncode(1024))
			assert.NoError(t, err)
			lastOffset = offset
		}

		lastRecord := logcodec.LogRecord{
			LSN:             0,
			HLC:             0,
			OperationType:   logrecord.LogOperationTypeDelete,
			TxnState:        logrecord.TransactionStateCommit,
			EntryType:       logrecord.LogEntryTypeRow,
			PrevTxnWalIndex: lastOffset.Encode(),
		}

		lastOffset, err = walInstance.Append(lastRecord.FBEncode(1024))

		recoveryInstance := NewWalRecovery(db, walInstance, bloomFilter)

		err = recoveryInstance.Recover(checkpoint)
		assert.NoError(t, err, "failed to recover wal")
		assert.Equal(t, *recoveryInstance.LastRecoveredOffset(), *lastOffset)
		assert.Equal(t, 22, recoveryInstance.RecoveredCount(), "total record recovered failed")
		for rKey := range cleanupKVEntries {
			columns, err := db.ScanRowCells([]byte(rKey), nil)
			assert.ErrorIs(t, err, kvdrivers2.ErrKeyNotFound, "row %s should be fully deleted", rKey)
			assert.Len(t, columns, 0)
		}
	})

	t.Run("btree_store_validator", func(t *testing.T) {
		for k := range allCommitedKeys {
			chunks, chErr := db.GetLOBChunks([]byte(k))
			if chErr == nil {
				assert.Greater(t, len(chunks), 0)
				continue
			}
			// not chunked, must be KV
			value, kvErr := db.GetKV([]byte(k))
			assert.NoError(t, kvErr)
			assert.NotNil(t, value)
		}

		for k := range unCommitedKeys {
			value, err := db.GetKV([]byte(k))
			assert.ErrorIs(t, err, kvdrivers2.ErrKeyNotFound)
			assert.Nil(t, value)
		}

		for key := range allCommitedDeleteKeys {
			value, err := db.GetKV([]byte(key))
			assert.ErrorIs(t, err, kvdrivers2.ErrKeyNotFound)
			assert.Nil(t, value, "deleted key value should be nil %s", key)
		}
	})

	t.Run("bloom_filter_validator", func(t *testing.T) {
		for k := range allCommitedKeys {
			ok := bloomFilter.Test([]byte(k))
			assert.True(t, ok)
		}

		for k := range insertedRows {
			ok := bloomFilter.Test([]byte(k))
			assert.True(t, ok)
		}

		for k := range cleanupKVEntries {
			ok := bloomFilter.Test([]byte(k))
			assert.True(t, ok)
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

func generateNRowColumnFBRecord(n uint64) ([][]byte, map[string]map[string][]byte) {
	rowsEntries := make(map[string]map[string][]byte)
	var rowsEncoded [][]byte

	for i := uint64(0); i < n; i++ {
		rowKey := gofakeit.UUID()

		if rowsEntries[rowKey] == nil {
			rowsEntries[rowKey] = make(map[string][]byte)
		}

		var encodedVals [][]byte
		// for each row Key generate 5 ops
		for j := 0; j < 5; j++ {
			entries := make(map[string][]byte)
			for k := 0; k < 10; k++ {
				key := gofakeit.UUID()
				val := gofakeit.LetterN(uint(i + 1))
				rowsEntries[rowKey][key] = []byte(val)
				entries[key] = []byte(val)
			}
			enc := logcodec.SerializeRowUpdateEntry([]byte(rowKey), entries)
			encodedVals = append(encodedVals, enc)
		}
		record := logcodec.LogRecord{
			LSN:             i,
			HLC:             i,
			OperationType:   logrecord.LogOperationTypeInsert,
			TxnState:        logrecord.TransactionStateNone,
			EntryType:       logrecord.LogEntryTypeRow,
			Entries:         encodedVals,
			PrevTxnWalIndex: nil,
			TxnID:           []byte(gofakeit.UUID()),
		}

		enc := record.FBEncode(1024)
		rowsEncoded = append(rowsEncoded, enc)
	}

	return rowsEncoded, rowsEntries
}

func generateNRowColumnFBRecordTxn(n uint64) ([]logcodec.LogRecord, map[string]map[string][]byte) {
	rowsEntries := make(map[string]map[string][]byte)
	var rowsEncoded []logcodec.LogRecord

	record := logcodec.LogRecord{
		LSN:             0,
		HLC:             0,
		CRC32Checksum:   0,
		OperationType:   logrecord.LogOperationTypeTxnMarker,
		TxnState:        logrecord.TransactionStateBegin,
		EntryType:       logrecord.LogEntryTypeRow,
		Entries:         nil,
		PrevTxnWalIndex: nil,
		TxnID:           []byte(gofakeit.UUID()),
	}

	rowsEncoded = append(rowsEncoded, record)
	for i := uint64(1); i < n; i++ {
		rowKey := gofakeit.UUID()

		if rowsEntries[rowKey] == nil {
			rowsEntries[rowKey] = make(map[string][]byte)
		}

		var encodedVals [][]byte
		// for each row Key generate 5 ops
		for j := 0; j < 5; j++ {
			entries := make(map[string][]byte)
			for k := 0; k < 10; k++ {
				key := gofakeit.UUID()
				val := gofakeit.LetterN(uint(i + 1))
				rowsEntries[rowKey][key] = []byte(val)
				entries[key] = []byte(val)
			}
			enc := logcodec.SerializeRowUpdateEntry([]byte(rowKey), entries)
			encodedVals = append(encodedVals, enc)
		}
		record := logcodec.LogRecord{
			LSN:             i,
			HLC:             i,
			OperationType:   logrecord.LogOperationTypeInsert,
			TxnState:        logrecord.TransactionStatePrepare,
			EntryType:       logrecord.LogEntryTypeRow,
			Entries:         encodedVals,
			PrevTxnWalIndex: nil,
			TxnID:           []byte(gofakeit.UUID()),
		}

		rowsEncoded = append(rowsEncoded, record)
	}

	return rowsEncoded, rowsEntries
}
