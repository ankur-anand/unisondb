package memtable

import (
	"bytes"
	"context"
	"fmt"
	"hash/crc32"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/ankur-anand/unisondb/dbkernel/internal"
	"github.com/ankur-anand/unisondb/dbkernel/internal/wal"
	"github.com/ankur-anand/unisondb/internal/keycodec"
	"github.com/ankur-anand/unisondb/internal/logcodec"
	kvdrivers2 "github.com/ankur-anand/unisondb/pkg/kvdrivers"
	"github.com/ankur-anand/unisondb/schemas/logrecord"
	"github.com/brianvoe/gofakeit/v7"
	"github.com/dgraph-io/badger/v4/y"
	"github.com/stretchr/testify/assert"
)

const (
	testNamespace = "test_namespace"
)

func setupMemTableWithLMDB(t *testing.T, capacity int64) (*MemTable, internal.BTreeStore) {
	dir := t.TempDir()

	dbFile := filepath.Join(dir, "test_flush.db")

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

	walDir := filepath.Join(t.TempDir(), fmt.Sprintf("wal_test_%d", time.Now().UnixNano()))
	err = os.MkdirAll(walDir, 0777)
	assert.NoError(t, err)

	walInstance, err := wal.NewWalIO(walDir, testNamespace, wal.NewDefaultConfig())
	assert.NoError(t, err)
	t.Cleanup(func() {
		err := walInstance.Close()
		assert.NoError(t, err, "failed to close wal")
	})

	return NewMemTable(capacity, walInstance, testNamespace, func(maxBatchSize int) internal.TxnBatcher {
		return db.NewTxnQueue(maxBatchSize)
	}), db
}

func setupMemTableWithBoltDB(t *testing.T, capacity int64) (*MemTable, internal.BTreeStore) {
	dir := t.TempDir()

	dbFile := filepath.Join(dir, "test_flush.db")

	db, err := kvdrivers2.NewBoltdb(dbFile, kvdrivers2.Config{
		Namespace: testNamespace,
		NoSync:    true,
		MmapSize:  1 << 30,
	})

	assert.NoError(t, err)
	t.Cleanup(func() {
		err := db.Close()
		assert.NoError(t, err, "failed to close db")
	})

	walDir := filepath.Join(t.TempDir(), fmt.Sprintf("wal_test_%d", time.Now().UnixNano()))
	err = os.MkdirAll(walDir, 0777)
	assert.NoError(t, err)

	walInstance, err := wal.NewWalIO(walDir, testNamespace, wal.NewDefaultConfig())
	assert.NoError(t, err)
	t.Cleanup(func() {
		err := walInstance.Close()
		assert.NoError(t, err, "failed to close wal")
	})

	return NewMemTable(capacity, walInstance, testNamespace, func(maxBatchSize int) internal.TxnBatcher {
		return db.NewTxnQueue(maxBatchSize)
	}), db
}

func TestMemTable_PutAndGet(t *testing.T) {
	const capacity = 1 << 20
	table := NewMemTable(capacity, nil, "", nil)

	// Create a test key, value, and WAL position.
	key := keycodec.KeyKV([]byte("test-key"))
	val := y.ValueStruct{Value: []byte("test-value")}
	pos := new(wal.Offset)
	pos.SegmentID = 1

	assert.True(t, table.canPut(key, val), "expected canPut to return true for key %q", key)
	err := table.Put(key, val)
	assert.NoError(t, err, "unexpected error on Put")

	gotVal := table.Get(key)

	assert.Equal(t, val.Value, gotVal.Value, "unexpected value on GetKV")
	assert.Nil(t, table.lastOffset, "unexpected offset on GetKV")
	assert.Nil(t, table.firstOffset, "unexpected offset on GetKV")
}

func TestMemTable_CannotPut(t *testing.T) {
	const capacity = 1 << 10
	table := NewMemTable(capacity, nil, "", nil)

	key := keycodec.KeyKV([]byte("key"))
	// more than 1 KB
	value := gofakeit.LetterN(1100)
	val := y.ValueStruct{Value: []byte(value)}
	pos := new(wal.Offset)
	pos.SegmentID = 1

	// should not panic
	err := table.Put(key, val)
	assert.ErrorIs(t, err, ErrArenaSizeWillExceed, "expected error on Put")
}

func TestMemTable_Flush_LMDBSuite(t *testing.T) {

	t.Run("flush_direct_value", func(t *testing.T) {
		// Put direct values on mem table.
		memTable, db := setupMemTableWithLMDB(t, 1<<20)
		recordCount := 10
		kv := make(map[string][]byte)
		for i := 0; i < recordCount; i++ {
			key := keycodec.KeyKV([]byte(fmt.Sprintf("key-%d", i)))
			value := gofakeit.LetterN(100)
			kv[string(key)] = []byte(value)
			vs := y.ValueStruct{Value: []byte(value), Meta: internal.LogOperationInsert,
				UserMeta: internal.EntryTypeKV,
				Version:  uint64(i)}
			err := memTable.Put(key, vs)
			memTable.SetOffset(nil)
			assert.NoError(t, err)
		}

		count, err := memTable.Flush(context.Background())
		assert.NoError(t, err, "failed to processBatch")
		assert.Equal(t, recordCount, count, "expected records to be flushed")
		for k, v := range kv {
			retrievedValue, err := db.GetKV(keycodec.KeyKV([]byte(k)))
			assert.NoError(t, err, "failed to GetKV")
			assert.Equal(t, v, retrievedValue, "unexpected value on GetKV")
		}
	})

	t.Run("flush_chunk_value", func(t *testing.T) {
		memTable, db := setupMemTableWithLMDB(t, 1<<20)
		recordCount := 10
		key, values, checksum := generateNChunkFBRecord(t, uint64(recordCount))
		var lastOffset *wal.Offset
		for _, value := range values {
			if lastOffset != nil {
				value.PrevTxnWalIndex = lastOffset.Encode()
			}
			encoded := value.FBEncode(len(value.Entries[0]))
			offset, err := memTable.wIO.Append(encoded)
			assert.NoError(t, err)
			lastOffset = offset
		}

		kvEncoded := logcodec.SerializeKVEntry(keycodec.KeyBlobChunk([]byte(key), 0), nil)
		lastRecord := logcodec.LogRecord{
			LSN:             0,
			HLC:             0,
			CRC32Checksum:   checksum,
			OperationType:   logrecord.LogOperationTypeTxnMarker,
			EntryType:       logrecord.LogEntryTypeChunked,
			TxnState:        logrecord.TransactionStateCommit,
			TxnID:           []byte(key),
			PrevTxnWalIndex: lastOffset.Encode(),
			Entries:         [][]byte{kvEncoded},
		}

		encoded := lastRecord.FBEncode(2*len(key) + 30)
		offset, err := memTable.wIO.Append(encoded)
		assert.NoError(t, err)
		lastOffset = offset

		vs := y.ValueStruct{
			Meta:     internal.LogOperationInsert,
			UserMeta: internal.EntryTypeChunked,
			Value:    lastOffset.Encode(),
		}

		blobMetaKey := keycodec.KeyBlobChunk([]byte(key), 0)
		err = memTable.Put(blobMetaKey, vs)
		assert.NoError(t, err)
		memTable.SetOffset(lastOffset)

		count, err := memTable.Flush(t.Context())
		assert.NoError(t, err, "failed to processBatch")
		assert.Equal(t, recordCount+2, count, "expected records to be flushed")

		chunks, err := db.GetLOBChunks(blobMetaKey)
		assert.NoError(t, err, "failed to GetKV")
		joined := bytes.Join(chunks, nil)
		assert.Equal(t, checksum, crc32.ChecksumIEEE(joined), "unexpected value on GetKV")
	})
}

func TestMemTable_Flush_BoltDBSuite(t *testing.T) {

	t.Run("flush_direct_value", func(t *testing.T) {
		// Put direct values on mem table.
		memTable, db := setupMemTableWithBoltDB(t, 1<<20)
		recordCount := 10
		kv := make(map[string][]byte)
		for i := 0; i < recordCount; i++ {
			key := keycodec.KeyKV([]byte(fmt.Sprintf("key-%d", i)))
			value := gofakeit.LetterN(100)
			kv[string(key)] = []byte(value)
			vs := y.ValueStruct{Value: []byte(value), Meta: internal.LogOperationInsert,
				UserMeta: internal.EntryTypeKV,
				Version:  uint64(i)}
			err := memTable.Put(key, vs)
			memTable.SetOffset(nil)
			assert.NoError(t, err)
		}

		count, err := memTable.Flush(context.Background())
		assert.NoError(t, err, "failed to processBatch")
		assert.Equal(t, recordCount, count, "expected records to be flushed")
		for k, v := range kv {
			retrievedValue, err := db.GetKV(keycodec.KeyKV([]byte(k)))
			assert.NoError(t, err, "failed to GetKV")
			assert.Equal(t, v, retrievedValue, "unexpected value on GetKV")
		}
	})

	t.Run("flush_chunk_value", func(t *testing.T) {
		memTable, db := setupMemTableWithBoltDB(t, 1<<20)
		recordCount := 10
		key, values, checksum := generateNChunkFBRecord(t, uint64(recordCount))
		var lastOffset *wal.Offset
		for _, value := range values {
			if lastOffset != nil {
				value.PrevTxnWalIndex = lastOffset.Encode()
			}
			encoded := value.FBEncode(len(value.Entries[0]))
			offset, err := memTable.wIO.Append(encoded)
			assert.NoError(t, err)
			lastOffset = offset
		}

		kvEncoded := logcodec.SerializeKVEntry(keycodec.KeyBlobChunk([]byte(key), 0), nil)
		lastRecord := logcodec.LogRecord{
			LSN:             0,
			HLC:             0,
			CRC32Checksum:   checksum,
			OperationType:   logrecord.LogOperationTypeTxnMarker,
			EntryType:       logrecord.LogEntryTypeChunked,
			TxnState:        logrecord.TransactionStateCommit,
			TxnID:           []byte(key),
			PrevTxnWalIndex: lastOffset.Encode(),
			Entries:         [][]byte{kvEncoded},
		}

		encoded := lastRecord.FBEncode(2*len(key) + 30)
		offset, err := memTable.wIO.Append(encoded)
		assert.NoError(t, err)
		lastOffset = offset

		vs := y.ValueStruct{
			Meta:     internal.LogOperationInsert,
			UserMeta: internal.EntryTypeChunked,
			Value:    lastOffset.Encode(),
		}

		blobMetaKey := keycodec.KeyBlobChunk([]byte(key), 0)
		err = memTable.Put(blobMetaKey, vs)
		assert.NoError(t, err)
		memTable.SetOffset(lastOffset)

		count, err := memTable.Flush(t.Context())
		assert.NoError(t, err, "failed to processBatch")
		assert.Equal(t, recordCount+2, count, "expected records to be flushed")

		chunks, err := db.GetLOBChunks(blobMetaKey)
		assert.NoError(t, err, "failed to GetKV")
		joined := bytes.Join(chunks, nil)
		assert.Equal(t, checksum, crc32.ChecksumIEEE(joined), "unexpected value on GetKV")
	})
}

func TestMemTable_Flush_SetDelete(t *testing.T) {
	memTable, db := setupMemTableWithBoltDB(t, 1<<20)
	recordCount := 10

	kv := make(map[string][]byte)
	for i := 0; i < recordCount; i++ {
		key := keycodec.KeyKV([]byte(fmt.Sprintf("key-%d", i)))
		value := gofakeit.LetterN(100)
		kv[string(key)] = []byte(value)
	}

	t.Run("flush_direct_value", func(t *testing.T) {

		for k, v := range kv {
			vs := y.ValueStruct{Value: []byte(v),
				Meta:     internal.LogOperationInsert,
				UserMeta: internal.EntryTypeKV}
			err := memTable.Put(keycodec.KeyKV([]byte(k)), vs)
			assert.NoError(t, err)
			memTable.SetOffset(nil)
			val := memTable.Get(keycodec.KeyKV([]byte(k)))
			assert.Equal(t, val, vs, "unexpected value on GetKV")
		}

		count, err := memTable.Flush(t.Context())
		assert.NoError(t, err, "failed to processBatch")
		assert.Equal(t, recordCount, count, "expected records to be flushed")
		for k, v := range kv {
			retrievedValue, err := db.GetKV(keycodec.KeyKV([]byte(k)))
			assert.NoError(t, err, "failed to GetKV")
			assert.Equal(t, v, retrievedValue, "unexpected value on GetKV")
		}
	})

	t.Run("flush_delete_value", func(t *testing.T) {
		for k, v := range kv {
			retrievedValue, err := db.GetKV(keycodec.KeyKV([]byte(k)))
			assert.NoError(t, err, "failed to GetKV")
			assert.Equal(t, v, retrievedValue, "unexpected value on GetKV")

			vs := y.ValueStruct{Value: []byte(v), Meta: internal.LogOperationDelete, UserMeta: internal.EntryTypeKV}
			err = memTable.Put(keycodec.KeyKV([]byte(k)), vs)
			assert.NoError(t, err)
			memTable.SetOffset(nil)
		}

		count, err := memTable.Flush(context.Background())
		assert.NoError(t, err, "failed to processBatch")
		assert.Equal(t, 2*recordCount, count, "expected records to be flushed")
		for k := range kv {
			retrievedValue, err := db.GetKV(keycodec.KeyKV([]byte(k)))
			assert.Nil(t, retrievedValue)
			assert.ErrorIs(t, err, kvdrivers2.ErrKeyNotFound, "failed to GetKV")
		}
	})

}

func TestFlush_EmptyMemTable(t *testing.T) {
	mmTable, _ := setupMemTableWithLMDB(t, 1<<10)

	_, err := mmTable.Flush(t.Context())
	assert.NoError(t, err)
}

func TestRow_KeysPut_Delete_GetRows_Flush(t *testing.T) {
	mmTable, db := setupMemTableWithLMDB(t, 1<<20)
	rowsEntries := make(map[string]map[string][]byte)

	for i := uint64(0); i < 10; i++ {
		rowKey := gofakeit.UUID()

		if rowsEntries[rowKey] == nil {
			rowsEntries[rowKey] = make(map[string][]byte)
		}

		// for each row Key generate 5 ops
		for j := 0; j < 5; j++ {

			entries := make(map[string][]byte)
			for k := 0; k < 10; k++ {
				key := gofakeit.UUID()
				val := gofakeit.LetterN(uint(i + 1))
				rowsEntries[rowKey][key] = []byte(val)
				entries[key] = []byte(val)
			}

			encoded := logcodec.SerializeRowUpdateEntry([]byte(rowKey), entries)
			vs := y.ValueStruct{Value: []byte(encoded), Meta: internal.LogOperationInsert, UserMeta: internal.EntryTypeRow}

			err := mmTable.Put([]byte(rowKey), vs)
			assert.NoError(t, err)
			mmTable.SetOffset(nil)
		}
	}

	for k, v := range rowsEntries {
		rowEntries := mmTable.GetRowYValue([]byte(k))
		buildColumns := make(map[string][]byte)

		BuildColumnMap(buildColumns, rowEntries)

		assert.Equal(t, len(v), len(buildColumns), "unexpected number of column values")
		assert.Equal(t, v, buildColumns, "unexpected column values")
	}

	count, err := mmTable.Flush(t.Context())
	assert.NoError(t, err, "failed to processBatch")
	assert.Equal(t, count, 50, "expected records to be flushed")

	randomRow := gofakeit.RandomMapKey(rowsEntries).(string)
	columnMap := rowsEntries[randomRow]
	deleteEntries := make(map[string][]byte, 0)
	for i := 0; i < 2; i++ {
		key := gofakeit.RandomMapKey(columnMap).(string)
		deleteEntries[key] = nil
	}

	encoded := logcodec.SerializeRowUpdateEntry([]byte(randomRow), deleteEntries)

	vs := y.ValueStruct{Value: encoded, Meta: internal.LogOperationDelete, UserMeta: internal.EntryTypeRow}
	err = mmTable.Put([]byte(randomRow), vs)
	assert.NoError(t, err)
	mmTable.SetOffset(nil)

	count, err = mmTable.Flush(t.Context())
	assert.NoError(t, err, "failed to processBatch")
	assert.Equal(t, count, 51, "expected records to be flushed")

	rowEntries := mmTable.GetRowYValue([]byte(randomRow))
	buildColumns := make(map[string][]byte)
	BuildColumnMap(buildColumns, rowEntries)
	assert.NoError(t, err, "failed to build column map")
	assert.Equal(t, len(buildColumns), len(columnMap)-len(deleteEntries), "unexpected number of column values")
	assert.NotContains(t, buildColumns, deleteEntries, "unexpected column values")

	value, err := db.ScanRowCells([]byte(randomRow), nil)
	assert.NoError(t, err, len(value))
	assert.Equal(t, len(value), len(columnMap)-len(deleteEntries), "unexpected number of column values")

	vs = y.ValueStruct{Meta: internal.LogOperationDeleteRowByKey, UserMeta: internal.EntryTypeRow}
	err = mmTable.Put([]byte(randomRow), vs)
	assert.NoError(t, err)
	mmTable.SetOffset(nil)
	count, err = mmTable.Flush(t.Context())
	assert.NoError(t, err, "failed to processBatch")
	assert.Equal(t, count, 52, "expected records to be flushed")
	_, err = db.ScanRowCells([]byte(randomRow), nil)
	assert.ErrorIs(t, err, kvdrivers2.ErrKeyNotFound)

	values := mmTable.GetRowYValue([]byte(randomRow))
	buildColumns = make(map[string][]byte)
	BuildColumnMap(buildColumns, values)
	assert.Empty(t, buildColumns, "deleted row should be empty")

	addEntries := make(map[string][]byte, 0)
	for i := 0; i < 2; i++ {
		key := gofakeit.RandomMapKey(columnMap).(string)
		addEntries[key] = []byte(gofakeit.LetterN(10))
	}

	// just a new entry
	addEntries[gofakeit.Name()] = []byte(gofakeit.LetterN(10))
	encoded = logcodec.SerializeRowUpdateEntry([]byte(randomRow), addEntries)

	vs = y.ValueStruct{Value: encoded, Meta: internal.LogOperationInsert, UserMeta: internal.EntryTypeRow}
	err = mmTable.Put([]byte(randomRow), vs)
	assert.NoError(t, err)
	mmTable.SetOffset(nil)

	count, err = mmTable.Flush(t.Context())
	assert.NoError(t, err, "failed to processBatch")
	assert.Equal(t, count, 53, "expected records to be flushed")

	values = mmTable.GetRowYValue([]byte(randomRow))
	buildColumns = make(map[string][]byte)
	BuildColumnMap(buildColumns, values)
	assert.Equal(t, buildColumns, addEntries, "new added entry should be added")

	value, err = db.ScanRowCells([]byte(randomRow), nil)
	assert.NoError(t, err, "get should not fail")
	assert.Equal(t, len(value), len(addEntries), "unexpected number of column values")
	assert.Equal(t, value, addEntries, "new added entry should be added")
}

func generateNChunkFBRecord(t *testing.T, n uint64) (string, []logcodec.LogRecord, uint32) {
	key := gofakeit.UUID()
	values := make([]logcodec.LogRecord, 0, n)

	encoded := logcodec.SerializeKVEntry([]byte(key), nil)
	startRecord := logcodec.LogRecord{
		LSN:           0,
		HLC:           0,
		CRC32Checksum: 0,
		OperationType: 0,
		TxnState:      0,
		EntryType:     logrecord.LogEntryTypeChunked,
		TxnID:         []byte(key),
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
			TxnID:         []byte(key),
			Entries:       [][]byte{encoded},
		}

		values = append(values, record)
	}

	return key, values, checksum
}

func TestPublic_Functions(t *testing.T) {
	mmTable, _ := setupMemTableWithLMDB(t, 1<<20)
	assert.NotNil(t, mmTable)
	assert.True(t, mmTable.IsEmpty())
	mmTable.IncrOffset()
	n, err := mmTable.Flush(t.Context())
	assert.NoError(t, err)
	assert.Equal(t, n, 1)

	key := keycodec.KeyKV([]byte("key"))
	// more than 1 KB
	value := gofakeit.LetterN(1100)
	val := y.ValueStruct{Value: []byte(value)}
	pos := new(wal.Offset)
	pos.SegmentID = 1

	err = mmTable.Put(key, val)
	assert.NoError(t, err)
	mmTable.SetOffset(pos)
	assert.False(t, mmTable.IsEmpty())
	assert.Equal(t, mmTable.GetLastOffset(), pos)
	assert.Equal(t, mmTable.GetFirstOffset(), pos)
	n, err = mmTable.Flush(t.Context())
	assert.NoError(t, err)
	assert.Equal(t, n, 2)

	size := len(key) + len(value)
	assert.Equal(t, mmTable.GetBytesStored(), size)
}

func TestGetRowYValue_ShortKeysShouldNotPanic(t *testing.T) {
	mmTable, _ := setupMemTableWithLMDB(t, 1<<20)

	shortKeys := [][]byte{
		[]byte("a"),
		[]byte("ab"),
		[]byte("abc"),
		[]byte("abcd"),
		[]byte("abcde"),
		[]byte("abcdef"),
		[]byte("kp7y7d"),
	}

	for _, key := range shortKeys {
		val := y.ValueStruct{Value: key,
			UserMeta: internal.EntryTypeRow, Meta: internal.LogOperationInsert}
		err := mmTable.Put(key, val)
		assert.NoError(t, err, "Put should not fail for key: %s", key)
	}

	for _, key := range shortKeys {
		func() {
			defer func() {
				if r := recover(); r != nil {
					t.Errorf("GetRowYValue panicked for key %s: %v", key, r)
				}
			}()

			vals := mmTable.GetRowYValue(key)
			assert.Equal(t, 1, len(vals))
			assert.NotNil(t, vals, "Returned value slice should not be nil for key: %s", key)
		}()
	}
}

func TestSamePrefix_RowKey(t *testing.T) {
	mmTable, _ := setupMemTableWithLMDB(t, 1<<20)

	val := y.ValueStruct{
		Value:    []byte("value"),
		UserMeta: internal.EntryTypeRow,
		Meta:     internal.LogOperationInsert,
	}

	_ = mmTable.Put([]byte("xyzmncabc"), val)
	_ = mmTable.Put([]byte("xyzmncabcdef"), val)
	values := mmTable.GetRowYValue([]byte("xyzmncabc"))
	assert.NotNil(t, values, "Returned value slice should not be nil for key: %s", val)
	assert.Equal(t, 1, len(values), "only one row ")
}

func TestBloomFilter_KVOperations(t *testing.T) {
	const capacity = 1 << 20
	table := NewMemTable(capacity, nil, testNamespace, nil)

	assert.NotNil(t, table.bloomFilter, "bloom filter should be initialized")

	testKeys := [][]byte{
		keycodec.KeyKV([]byte("key1")),
		keycodec.KeyKV([]byte("key2")),
		keycodec.KeyKV([]byte("key3")),
		keycodec.KeyKV([]byte("key4")),
		keycodec.KeyKV([]byte("key5")),
	}

	for _, key := range testKeys {
		val := y.ValueStruct{
			Value:    []byte("value-" + string(key)),
			UserMeta: internal.EntryTypeKV,
			Meta:     internal.LogOperationInsert,
		}
		err := table.Put(key, val)
		assert.NoError(t, err, "Put should succeed")
		assert.True(t, table.bloomFilter.Test(key), "bloom filter should contain key after Put")
	}

	for _, key := range testKeys {
		result := table.Get(key)
		assert.NotNil(t, result.Value, "Get should return value for existing key")
		assert.Equal(t, "value-"+string(key), string(result.Value), "Get should return correct value")
	}

	nonExistentKeys := [][]byte{
		keycodec.KeyKV([]byte("nonexistent1")),
		keycodec.KeyKV([]byte("nonexistent2")),
		keycodec.KeyKV([]byte("nonexistent3")),
	}

	for _, key := range nonExistentKeys {
		assert.False(t, table.bloomFilter.Test(key), "bloom filter should not contain non-existent key")

		result := table.Get(key)
		assert.Nil(t, result.Value, "Get should return nil for non-existent key")
	}
}

func TestBloomFilter_RowOperations(t *testing.T) {
	mmTable, _ := setupMemTableWithLMDB(t, 1<<20)

	rowKeys := [][]byte{
		[]byte("row1"),
		[]byte("row2"),
		[]byte("row3"),
	}

	for _, rowKey := range rowKeys {
		val := y.ValueStruct{
			Value:    logcodec.SerializeRowUpdateEntry(rowKey, map[string][]byte{"col1": []byte("value1")}),
			UserMeta: internal.EntryTypeRow,
			Meta:     internal.LogOperationInsert,
		}
		err := mmTable.Put(rowKey, val)
		assert.NoError(t, err, "Put should succeed for row key")

		assert.True(t, mmTable.bloomFilter.Test(rowKey), "bloom filter should contain row key after Put")
	}

	for _, rowKey := range rowKeys {
		result := mmTable.GetRowYValue(rowKey)
		assert.NotNil(t, result, "GetRowYValue should return results for existing key")
		assert.Greater(t, len(result), 0, "GetRowYValue should return at least one entry")
	}

	nonExistentRowKeys := [][]byte{
		[]byte("nonexistent_row1"),
		[]byte("nonexistent_row2"),
		[]byte("nonexistent_row3"),
	}

	for _, rowKey := range nonExistentRowKeys {
		assert.False(t, mmTable.bloomFilter.Test(rowKey), "bloom filter should not contain non-existent row key")
		result := mmTable.GetRowYValue(rowKey)
		assert.Nil(t, result, "GetRowYValue should return nil for non-existent key")
	}
}

func TestBloomFilter_MVCCRows(t *testing.T) {
	mmTable, _ := setupMemTableWithLMDB(t, 1<<20)

	rowKey := []byte("mvcc_row")

	for i := 0; i < 5; i++ {
		val := y.ValueStruct{
			Value: logcodec.SerializeRowUpdateEntry(rowKey, map[string][]byte{
				fmt.Sprintf("col%d", i): []byte(fmt.Sprintf("value%d", i)),
			}),
			UserMeta: internal.EntryTypeRow,
			Meta:     internal.LogOperationInsert,
		}
		err := mmTable.Put(rowKey, val)
		assert.NoError(t, err, "Put should succeed for MVCC row")
	}

	assert.True(t, mmTable.bloomFilter.Test(rowKey), "bloom filter should contain MVCC row key")

	results := mmTable.GetRowYValue(rowKey)
	assert.NotNil(t, results, "GetRowYValue should return results")
	assert.Equal(t, 5, len(results), "GetRowYValue should return all 5 MVCC versions")
}

func TestBloomFilter_ManyKeys(t *testing.T) {
	const capacity = 10 << 20 // 10MB
	table := NewMemTable(capacity, nil, testNamespace, nil)

	numKeys := 1000
	keys := make([][]byte, numKeys)

	for i := 0; i < numKeys; i++ {
		key := keycodec.KeyKV([]byte(fmt.Sprintf("key-%d", i)))
		keys[i] = key
		val := y.ValueStruct{
			Value:    []byte(fmt.Sprintf("value-%d", i)),
			UserMeta: internal.EntryTypeKV,
			Meta:     internal.LogOperationInsert,
		}
		err := table.Put(key, val)
		assert.NoError(t, err, "Put should succeed")
	}

	for i, key := range keys {
		assert.True(t, table.bloomFilter.Test(key), "bloom filter should contain key %d", i)
	}

	falsePositives := 0
	nonExistentTests := 1000

	for i := 0; i < nonExistentTests; i++ {
		key := keycodec.KeyKV([]byte(fmt.Sprintf("nonexistent-key-%d", i)))
		if table.bloomFilter.Test(key) {
			falsePositives++
		}
	}

	// test very low false positive rate
	assert.Less(t, falsePositives, 5, "bloom filter should have very low false positive rate")
	t.Logf("False positives: %d out of %d tests (%.2f%%)", falsePositives, nonExistentTests, float64(falsePositives)/float64(nonExistentTests)*100)
}
