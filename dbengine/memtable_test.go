package dbengine

import (
	"context"
	"hash/crc32"
	"os"
	"path/filepath"
	"testing"

	"github.com/ankur-anand/kvalchemy/dbengine/kvdb"
	"github.com/ankur-anand/kvalchemy/dbengine/wal"
	"github.com/ankur-anand/kvalchemy/dbengine/wal/walrecord"
	"github.com/brianvoe/gofakeit/v7"
	"github.com/dgraph-io/badger/v4/y"
	"github.com/hashicorp/go-metrics"
	"github.com/stretchr/testify/assert"
)

const (
	testNamespace = "test_namespace"
)

func setupMemTableWithLMDB(t *testing.T, capacity int64) *memTable {
	dir := t.TempDir()

	dbFile := filepath.Join(dir, "test_flush.db")

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

	tdir := os.TempDir()
	walDir := filepath.Join(tdir, "wal_test")
	err = os.MkdirAll(walDir, 0777)
	assert.NoError(t, err)

	walInstance, err := wal.NewWalIO(walDir, testNamespace, wal.NewDefaultConfig(), metrics.Default())
	assert.NoError(t, err)
	t.Cleanup(func() {
		err := walInstance.Close()
		assert.NoError(t, err, "failed to close wal")
	})

	return newMemTable(capacity, db, walInstance, testNamespace)
}

func setupMemTableWithBoltDB(t *testing.T, capacity int64) *memTable {
	dir := t.TempDir()

	dbFile := filepath.Join(dir, "test_flush.db")

	db, err := kvdb.NewBoltdb(dbFile, kvdb.Config{
		Namespace: testNamespace,
		NoSync:    true,
		MmapSize:  1 << 30,
	})

	assert.NoError(t, err)
	t.Cleanup(func() {
		err := db.Close()
		assert.NoError(t, err, "failed to close db")
	})

	tdir := os.TempDir()
	walDir := filepath.Join(tdir, "wal_test")
	err = os.MkdirAll(walDir, 0777)
	assert.NoError(t, err)

	walInstance, err := wal.NewWalIO(walDir, testNamespace, wal.NewDefaultConfig(), metrics.Default())
	assert.NoError(t, err)
	t.Cleanup(func() {
		err := walInstance.Close()
		assert.NoError(t, err, "failed to close wal")
	})

	return newMemTable(capacity, db, walInstance, testNamespace)
}

func TestMemTable_PutAndGet(t *testing.T) {
	const capacity = 1 << 20
	table := newMemTable(capacity, nil, nil, "")

	// Create a test key, value, and WAL position.
	key := []byte("test-key")
	val := y.ValueStruct{Value: []byte("test-value")}
	pos := new(wal.Offset)
	pos.SegmentId = 1

	assert.True(t, table.canPut(key, val), "expected canPut to return true for key %q", key)
	err := table.put(key, val, pos)
	assert.NoError(t, err, "unexpected error on put")

	gotVal := table.get(key)

	assert.Equal(t, val.Value, gotVal.Value, "unexpected value on get")
	assert.Equal(t, table.lastOffset, pos, "unexpected offset on get")
	assert.Equal(t, table.firstOffset, pos, "unexpected offset on get")
}

func TestMemTable_CannotPut(t *testing.T) {
	const capacity = 1 << 10
	table := newMemTable(capacity, nil, nil, "")

	key := []byte("key")
	// more than 1 KB
	value := gofakeit.LetterN(1100)
	val := y.ValueStruct{Value: []byte(value)}
	pos := new(wal.Offset)
	pos.SegmentId = 1

	// should not panic
	err := table.put(key, val, pos)
	assert.ErrorIs(t, err, errArenaSizeWillExceed, "expected error on put")
}

func TestMemTable_Flush_LMDBSuite(t *testing.T) {

	t.Run("flush_direct_value", func(t *testing.T) {
		// put direct values on mem table.
		memTable := setupMemTableWithLMDB(t, 1<<20)
		recordCount := 10
		kv := generateNFBRecord(t, uint64(recordCount))
		for k, v := range kv {
			offset, err := memTable.wIO.Append(v)
			assert.NoError(t, err)
			vs := getValueStruct(metaValueInsert, true, v)
			err = memTable.put([]byte(k), vs, offset)
			assert.NoError(t, err)
		}

		count, err := memTable.flush(context.Background())
		assert.NoError(t, err, "failed to processBatch")
		assert.Equal(t, recordCount, count, "expected records to be flushed")
		for k, v := range kv {
			retrievedValue, err := memTable.db.Get([]byte(k))
			assert.NoError(t, err, "failed to get")
			recordValue := walrecord.GetRootAsWalRecord(v, 0)
			assert.Equal(t, recordValue.ValueBytes(), retrievedValue, "unexpected value on get")
		}
	})

	t.Run("flush_chunk_value", func(t *testing.T) {
		// put direct values on mem table.
		memTable := setupMemTableWithLMDB(t, 1<<20)
		recordCount := 10
		key, values, checksum := generateNChunkFBRecord(t, uint64(recordCount))
		var lastOffset *wal.Offset
		for _, value := range values {
			value.PrevTxnOffset = lastOffset
			encoded, err := value.FBEncode()
			assert.NoError(t, err)
			offset, err := memTable.wIO.Append(encoded)
			assert.NoError(t, err)
			lastOffset = offset
		}

		lastRecord := walrecord.Record{
			Index:         0,
			Hlc:           0,
			Key:           []byte(key),
			Value:         marshalChecksum(checksum),
			LogOperation:  walrecord.LogOperationTxnMarker,
			ValueType:     walrecord.ValueTypeChunked,
			TxnStatus:     walrecord.TxnStatusCommit,
			TxnID:         []byte(key),
			PrevTxnOffset: lastOffset,
		}

		encoded, err := lastRecord.FBEncode()
		assert.NoError(t, err)
		offset, err := memTable.wIO.Append(encoded)
		assert.NoError(t, err)
		lastOffset = offset

		vs := getValueStruct(metaValueInsert, true, encoded)
		err = memTable.put([]byte(key), vs, offset)
		assert.NoError(t, err)

		count, err := memTable.flush(context.Background())
		assert.NoError(t, err, "failed to processBatch")
		assert.Equal(t, recordCount+2, count, "expected records to be flushed")

		retrievedValue, err := memTable.db.Get([]byte(key))
		assert.NoError(t, err, "failed to get")
		assert.Equal(t, checksum, crc32.ChecksumIEEE(retrievedValue), "unexpected value on get")
	})

	t.Run("flush_indirect_value", func(t *testing.T) {
		// put direct values on mem table.
		memTable := setupMemTableWithLMDB(t, 1<<20)
		recordCount := 10
		kv := generateNFBRecord(t, uint64(recordCount))
		for k, v := range kv {
			offset, err := memTable.wIO.Append(v)
			assert.NoError(t, err)
			vs := getValueStruct(metaValueInsert, false, offset.Encode())
			err = memTable.put([]byte(k), vs, offset)
			assert.NoError(t, err)
		}

		count, err := memTable.flush(context.Background())
		assert.NoError(t, err, "failed to processBatch")
		assert.Equal(t, recordCount, count, "expected records to be flushed")
		for k, v := range kv {
			retrievedValue, err := memTable.db.Get([]byte(k))
			assert.NoError(t, err, "failed to get")
			recordValue := walrecord.GetRootAsWalRecord(v, 0)
			assert.Equal(t, recordValue.ValueBytes(), retrievedValue, "unexpected value on get")
		}
	})
}

func TestMemTable_Flush_BoltDBSuite(t *testing.T) {

	t.Run("flush_direct_value", func(t *testing.T) {
		// put direct values on mem table.
		memTable := setupMemTableWithBoltDB(t, 1<<20)
		recordCount := 10
		kv := generateNFBRecord(t, uint64(recordCount))
		for k, v := range kv {
			offset, err := memTable.wIO.Append(v)
			assert.NoError(t, err)
			vs := getValueStruct(metaValueInsert, true, v)
			err = memTable.put([]byte(k), vs, offset)
			assert.NoError(t, err)
		}

		count, err := memTable.flush(context.Background())
		assert.NoError(t, err, "failed to processBatch")
		assert.Equal(t, recordCount, count, "expected records to be flushed")
		for k, v := range kv {
			retrievedValue, err := memTable.db.Get([]byte(k))
			assert.NoError(t, err, "failed to get")
			recordValue := walrecord.GetRootAsWalRecord(v, 0)
			assert.Equal(t, recordValue.ValueBytes(), retrievedValue, "unexpected value on get")
		}
	})

	t.Run("flush_chunk_value", func(t *testing.T) {
		// put direct values on mem table.
		memTable := setupMemTableWithBoltDB(t, 1<<20)
		recordCount := 10
		key, values, checksum := generateNChunkFBRecord(t, uint64(recordCount))
		var lastOffset *wal.Offset
		for _, value := range values {
			value.PrevTxnOffset = lastOffset
			encoded, err := value.FBEncode()
			assert.NoError(t, err)
			offset, err := memTable.wIO.Append(encoded)
			assert.NoError(t, err)
			lastOffset = offset
		}

		lastRecord := walrecord.Record{
			Index:         0,
			Hlc:           0,
			Key:           []byte(key),
			Value:         marshalChecksum(checksum),
			LogOperation:  walrecord.LogOperationTxnMarker,
			ValueType:     walrecord.ValueTypeChunked,
			TxnStatus:     walrecord.TxnStatusCommit,
			TxnID:         []byte(key),
			PrevTxnOffset: lastOffset,
		}

		encoded, err := lastRecord.FBEncode()
		assert.NoError(t, err)
		offset, err := memTable.wIO.Append(encoded)
		assert.NoError(t, err)
		lastOffset = offset

		vs := getValueStruct(metaValueInsert, true, encoded)
		err = memTable.put([]byte(key), vs, offset)
		assert.NoError(t, err)

		count, err := memTable.flush(context.Background())
		assert.NoError(t, err, "failed to processBatch")
		assert.Equal(t, recordCount+2, count, "expected records to be flushed")

		retrievedValue, err := memTable.db.Get([]byte(key))
		assert.NoError(t, err, "failed to get")
		assert.Equal(t, checksum, crc32.ChecksumIEEE(retrievedValue), "unexpected value on get")
	})

	t.Run("flush_indirect_value", func(t *testing.T) {
		// put direct values on mem table.
		memTable := setupMemTableWithBoltDB(t, 1<<20)
		recordCount := 10
		kv := generateNFBRecord(t, uint64(recordCount))
		for k, v := range kv {
			offset, err := memTable.wIO.Append(v)
			assert.NoError(t, err)
			vs := getValueStruct(metaValueInsert, false, offset.Encode())
			err = memTable.put([]byte(k), vs, offset)
			assert.NoError(t, err)
		}

		count, err := memTable.flush(context.Background())
		assert.NoError(t, err, "failed to processBatch")
		assert.Equal(t, recordCount, count, "expected records to be flushed")
		for k, v := range kv {
			retrievedValue, err := memTable.db.Get([]byte(k))
			assert.NoError(t, err, "failed to get")
			recordValue := walrecord.GetRootAsWalRecord(v, 0)
			assert.Equal(t, recordValue.ValueBytes(), retrievedValue, "unexpected value on get")
		}
	})
}

func TestMemTable_Flush_SetDelete(t *testing.T) {

	// put direct values on mem table.
	memTable := setupMemTableWithBoltDB(t, 1<<20)
	recordCount := 10
	kv := generateNFBRecord(t, uint64(recordCount))

	t.Run("flush_direct_value", func(t *testing.T) {

		for k, v := range kv {
			offset, err := memTable.wIO.Append(v)
			assert.NoError(t, err)
			vs := getValueStruct(metaValueInsert, true, v)
			err = memTable.put([]byte(k), vs, offset)
			assert.NoError(t, err)
		}

		count, err := memTable.flush(context.Background())
		assert.NoError(t, err, "failed to processBatch")
		assert.Equal(t, recordCount, count, "expected records to be flushed")
		for k, v := range kv {
			retrievedValue, err := memTable.db.Get([]byte(k))
			assert.NoError(t, err, "failed to get")
			recordValue := walrecord.GetRootAsWalRecord(v, 0)
			assert.Equal(t, recordValue.ValueBytes(), retrievedValue, "unexpected value on get")
		}
	})

	t.Run("flush_delete_value", func(t *testing.T) {
		for k, v := range kv {
			retrievedValue, err := memTable.db.Get([]byte(k))
			assert.NoError(t, err, "failed to get")
			recordValue := walrecord.GetRootAsWalRecord(v, 0)
			assert.Equal(t, recordValue.ValueBytes(), retrievedValue, "unexpected value on get")
			record := walrecord.Record{
				Index:         1,
				Hlc:           1,
				Key:           []byte(k),
				Value:         nil,
				LogOperation:  walrecord.LogOperationDelete,
				ValueType:     walrecord.ValueTypeFull,
				TxnStatus:     walrecord.TxnStatusTxnNone,
				TxnID:         nil,
				PrevTxnOffset: nil,
			}

			encoded, err := record.FBEncode()
			assert.NoError(t, err)
			offset, err := memTable.wIO.Append(encoded)
			assert.NoError(t, err)
			vs := getValueStruct(metaValueDelete, true, encoded)
			err = memTable.put([]byte(k), vs, offset)
			assert.NoError(t, err)
		}

		count, err := memTable.flush(context.Background())
		assert.NoError(t, err, "failed to processBatch")
		assert.Equal(t, recordCount, count, "expected records to be flushed")
		for k := range kv {
			retrievedValue, err := memTable.db.Get([]byte(k))
			assert.Nil(t, retrievedValue)
			assert.ErrorIs(t, err, kvdb.ErrKeyNotFound, "failed to get")
		}
	})
}

func generateNFBRecord(t *testing.T, n uint64) map[string][]byte {

	kv := make(map[string][]byte)
	for i := uint64(0); i < n; i++ {
		key := gofakeit.UUID()
		val := gofakeit.LetterN(110)
		record := walrecord.Record{
			Index:         i,
			Hlc:           i,
			Key:           []byte(key),
			Value:         []byte(val),
			LogOperation:  walrecord.LogOperationInsert,
			ValueType:     walrecord.ValueTypeFull,
			TxnStatus:     walrecord.TxnStatusTxnNone,
			TxnID:         nil,
			PrevTxnOffset: nil,
		}

		encoded, err := record.FBEncode()
		assert.NoError(t, err)
		kv[key] = encoded
	}

	return kv
}

func generateNChunkFBRecord(t *testing.T, n uint64) (string, []walrecord.Record, uint32) {
	key := gofakeit.UUID()
	values := make([]walrecord.Record, 0, n)

	startRecord := walrecord.Record{
		Index:         0,
		Hlc:           0,
		Key:           []byte(key),
		Value:         nil,
		LogOperation:  walrecord.LogOperationTxnMarker,
		ValueType:     walrecord.ValueTypeChunked,
		TxnStatus:     walrecord.TxnStatusBegin,
		TxnID:         []byte(key),
		PrevTxnOffset: nil,
	}

	values = append(values, startRecord)

	var checksum uint32
	for i := uint64(1); i < n; i++ {
		val := gofakeit.LetterN(110)
		checksum = crc32.Update(checksum, crc32.IEEETable, []byte(val))
		record := walrecord.Record{
			Index:         i,
			Hlc:           i,
			Key:           []byte(key),
			Value:         []byte(val),
			LogOperation:  walrecord.LogOperationInsert,
			ValueType:     walrecord.ValueTypeChunked,
			TxnStatus:     walrecord.TxnStatusPrepare,
			TxnID:         nil,
			PrevTxnOffset: nil,
		}

		values = append(values, record)
	}

	return key, values, checksum
}

func TestFlush_EmptyMemTable(t *testing.T) {
	mmTable := setupMemTableWithBoltDB(t, 1<<10)

	_, err := mmTable.flush(context.Background())
	assert.NoError(t, err)
}

func TestRow_KeysPut(t *testing.T) {
	mmTable := setupMemTableWithBoltDB(t, 1<<20)

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

			record := &walrecord.Record{
				Index:         uint64(j),
				Hlc:           HLCNow(uint64(j)),
				Key:           []byte(rowKey),
				Value:         nil,
				LogOperation:  walrecord.LogOperationInsert,
				TxnID:         nil,
				TxnStatus:     walrecord.TxnStatusPrepare,
				ValueType:     walrecord.ValueTypeColumn,
				PrevTxnOffset: nil,
				ColumnEntries: entries,
			}

			encoded, err := record.FBEncode()
			assert.NoError(t, err)

			v := getValueStruct(metaValueInsert, true, encoded)
			v.UserMeta = valueTypeColumn
			err = mmTable.put([]byte(rowKey), v, nil)
			assert.NoError(t, err)
		}
	}

	for k, v := range rowsEntries {
		rowEntries := mmTable.getRowYValue([]byte(k))
		buildColumns := make(map[string][]byte)
		err := buildColumnMap(buildColumns, rowEntries, mmTable.wIO)
		assert.NoError(t, err, "failed to build column map")
		assert.Equal(t, len(v), len(buildColumns), "unexpected number of column values")
		assert.Equal(t, v, buildColumns, "unexpected column values")
	}

	randomRow := gofakeit.RandomMapKey(rowsEntries).(string)
	columnMap := rowsEntries[randomRow]
	deleteEntries := make(map[string][]byte, 0)
	for i := 0; i < 2; i++ {
		key := gofakeit.RandomMapKey(columnMap).(string)
		deleteEntries[key] = nil
	}

	record := &walrecord.Record{
		Index:         uint64(50),
		Hlc:           HLCNow(uint64(50)),
		Key:           []byte(randomRow),
		Value:         nil,
		LogOperation:  walrecord.LogOperationDelete,
		TxnID:         nil,
		TxnStatus:     walrecord.TxnStatusPrepare,
		ValueType:     walrecord.ValueTypeColumn,
		PrevTxnOffset: nil,
		ColumnEntries: deleteEntries,
	}

	encoded, err := record.FBEncode()
	assert.NoError(t, err)

	v := getValueStruct(metaValueDelete, true, encoded)
	v.UserMeta = valueTypeColumn
	err = mmTable.put([]byte(randomRow), v, nil)
	assert.NoError(t, err)

	rowEntries := mmTable.getRowYValue([]byte(randomRow))
	buildColumns := make(map[string][]byte)
	err = buildColumnMap(buildColumns, rowEntries, mmTable.wIO)
	assert.NoError(t, err, "failed to build column map")
	assert.Equal(t, len(buildColumns), len(columnMap)-len(deleteEntries), "unexpected number of column values")
	assert.NotContains(t, buildColumns, deleteEntries, "unexpected column values")
}
