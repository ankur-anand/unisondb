package memtable

import (
	"context"
	"hash/crc32"
	"os"
	"path/filepath"
	"testing"

	"github.com/ankur-anand/unisondb/dbkernel"
	kvdrivers2 "github.com/ankur-anand/unisondb/dbkernel/internal/kvdrivers"
	wal2 "github.com/ankur-anand/unisondb/dbkernel/internal/wal"
	"github.com/ankur-anand/unisondb/dbkernel/wal/walrecord"
	"github.com/brianvoe/gofakeit/v7"
	"github.com/dgraph-io/badger/v4/y"
	"github.com/hashicorp/go-metrics"
	"github.com/stretchr/testify/assert"
)

const (
	testNamespace = "test_namespace"
)

func setupMemTableWithLMDB(t *testing.T, capacity int64) *MemTable {
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

	tdir := os.TempDir()
	walDir := filepath.Join(tdir, "wal_test")
	err = os.MkdirAll(walDir, 0777)
	assert.NoError(t, err)

	walInstance, err := wal2.NewWalIO(walDir, testNamespace, wal2.NewDefaultConfig(), metrics.Default())
	assert.NoError(t, err)
	t.Cleanup(func() {
		err := walInstance.Close()
		assert.NoError(t, err, "failed to close wal")
	})

	return NewMemTable(capacity, db, walInstance, testNamespace)
}

func setupMemTableWithBoltDB(t *testing.T, capacity int64) *MemTable {
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

	tdir := os.TempDir()
	walDir := filepath.Join(tdir, "wal_test")
	err = os.MkdirAll(walDir, 0777)
	assert.NoError(t, err)

	walInstance, err := wal2.NewWalIO(walDir, testNamespace, wal2.NewDefaultConfig(), metrics.Default())
	assert.NoError(t, err)
	t.Cleanup(func() {
		err := walInstance.Close()
		assert.NoError(t, err, "failed to close wal")
	})

	return NewMemTable(capacity, db, walInstance, testNamespace)
}

func TestMemTable_PutAndGet(t *testing.T) {
	const capacity = 1 << 20
	table := NewMemTable(capacity, nil, nil, "")

	// Create a test key, value, and WAL position.
	key := []byte("test-key")
	val := y.ValueStruct{Value: []byte("test-value")}
	pos := new(wal2.Offset)
	pos.SegmentId = 1

	assert.True(t, table.canPut(key, val), "expected canPut to return true for key %q", key)
	err := table.Put(key, val, pos)
	assert.NoError(t, err, "unexpected error on Put")

	gotVal := table.Get(key)

	assert.Equal(t, val.Value, gotVal.Value, "unexpected value on Get")
	assert.Equal(t, table.lastOffset, pos, "unexpected offset on Get")
	assert.Equal(t, table.firstOffset, pos, "unexpected offset on Get")
}

func TestMemTable_CannotPut(t *testing.T) {
	const capacity = 1 << 10
	table := NewMemTable(capacity, nil, nil, "")

	key := []byte("key")
	// more than 1 KB
	value := gofakeit.LetterN(1100)
	val := y.ValueStruct{Value: []byte(value)}
	pos := new(wal2.Offset)
	pos.SegmentId = 1

	// should not panic
	err := table.Put(key, val, pos)
	assert.ErrorIs(t, err, ErrArenaSizeWillExceed, "expected error on Put")
}

func TestMemTable_Flush_LMDBSuite(t *testing.T) {

	t.Run("flush_direct_value", func(t *testing.T) {
		// Put direct values on mem table.
		memTable := setupMemTableWithLMDB(t, 1<<20)
		recordCount := 10
		kv := generateNFBRecord(t, uint64(recordCount))
		for k, v := range kv {
			offset, err := memTable.wIO.Append(v)
			assert.NoError(t, err)
			vs := dbkernel.getValueStruct(dbkernel.logOperationInsert, true, v)
			err = memTable.Put([]byte(k), vs, offset)
			assert.NoError(t, err)
		}

		count, err := memTable.flush(context.Background())
		assert.NoError(t, err, "failed to processBatch")
		assert.Equal(t, recordCount, count, "expected records to be flushed")
		for k, v := range kv {
			retrievedValue, err := memTable.db.Get([]byte(k))
			assert.NoError(t, err, "failed to Get")
			recordValue := walrecord.GetRootAsWalRecord(v, 0)
			assert.Equal(t, recordValue.ValueBytes(), retrievedValue, "unexpected value on Get")
		}
	})

	t.Run("flush_chunk_value", func(t *testing.T) {
		// Put direct values on mem table.
		memTable := setupMemTableWithLMDB(t, 1<<20)
		recordCount := 10
		key, values, checksum := generateNChunkFBRecord(t, uint64(recordCount))
		var lastOffset *wal2.Offset
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
			Value:         dbkernel.marshalChecksum(checksum),
			LogOperation:  walrecord.LogOperationTxnMarker,
			EntryType:     walrecord.EntryTypeChunked,
			TxnStatus:     walrecord.TxnStatusCommit,
			TxnID:         []byte(key),
			PrevTxnOffset: lastOffset,
		}

		encoded, err := lastRecord.FBEncode()
		assert.NoError(t, err)
		offset, err := memTable.wIO.Append(encoded)
		assert.NoError(t, err)
		lastOffset = offset

		vs := dbkernel.getValueStruct(dbkernel.logOperationInsert, true, encoded)
		err = memTable.Put([]byte(key), vs, offset)
		assert.NoError(t, err)

		count, err := memTable.flush(context.Background())
		assert.NoError(t, err, "failed to processBatch")
		assert.Equal(t, recordCount+2, count, "expected records to be flushed")

		retrievedValue, err := memTable.db.Get([]byte(key))
		assert.NoError(t, err, "failed to Get")
		assert.Equal(t, checksum, crc32.ChecksumIEEE(retrievedValue), "unexpected value on Get")
	})

	t.Run("flush_indirect_value", func(t *testing.T) {
		// Put direct values on mem table.
		memTable := setupMemTableWithLMDB(t, 1<<20)
		recordCount := 10
		kv := generateNFBRecord(t, uint64(recordCount))
		for k, v := range kv {
			offset, err := memTable.wIO.Append(v)
			assert.NoError(t, err)
			vs := dbkernel.getValueStruct(dbkernel.logOperationInsert, false, offset.Encode())
			err = memTable.Put([]byte(k), vs, offset)
			assert.NoError(t, err)
		}

		count, err := memTable.flush(context.Background())
		assert.NoError(t, err, "failed to processBatch")
		assert.Equal(t, recordCount, count, "expected records to be flushed")
		for k, v := range kv {
			retrievedValue, err := memTable.db.Get([]byte(k))
			assert.NoError(t, err, "failed to Get")
			recordValue := walrecord.GetRootAsWalRecord(v, 0)
			assert.Equal(t, recordValue.ValueBytes(), retrievedValue, "unexpected value on Get")
		}
	})
}

func TestMemTable_Flush_BoltDBSuite(t *testing.T) {

	t.Run("flush_direct_value", func(t *testing.T) {
		// Put direct values on mem table.
		memTable := setupMemTableWithBoltDB(t, 1<<20)
		recordCount := 10
		kv := generateNFBRecord(t, uint64(recordCount))
		for k, v := range kv {
			offset, err := memTable.wIO.Append(v)
			assert.NoError(t, err)
			vs := dbkernel.getValueStruct(dbkernel.logOperationInsert, true, v)
			err = memTable.Put([]byte(k), vs, offset)
			assert.NoError(t, err)
		}

		count, err := memTable.flush(context.Background())
		assert.NoError(t, err, "failed to processBatch")
		assert.Equal(t, recordCount, count, "expected records to be flushed")
		for k, v := range kv {
			retrievedValue, err := memTable.db.Get([]byte(k))
			assert.NoError(t, err, "failed to Get")
			recordValue := walrecord.GetRootAsWalRecord(v, 0)
			assert.Equal(t, recordValue.ValueBytes(), retrievedValue, "unexpected value on Get")
		}
	})

	t.Run("flush_chunk_value", func(t *testing.T) {
		// Put direct values on mem table.
		memTable := setupMemTableWithBoltDB(t, 1<<20)
		recordCount := 10
		key, values, checksum := generateNChunkFBRecord(t, uint64(recordCount))
		var lastOffset *wal2.Offset
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
			Value:         dbkernel.marshalChecksum(checksum),
			LogOperation:  walrecord.LogOperationTxnMarker,
			EntryType:     walrecord.EntryTypeChunked,
			TxnStatus:     walrecord.TxnStatusCommit,
			TxnID:         []byte(key),
			PrevTxnOffset: lastOffset,
		}

		encoded, err := lastRecord.FBEncode()
		assert.NoError(t, err)
		offset, err := memTable.wIO.Append(encoded)
		assert.NoError(t, err)
		lastOffset = offset

		vs := dbkernel.getValueStruct(dbkernel.logOperationInsert, true, encoded)
		err = memTable.Put([]byte(key), vs, offset)
		assert.NoError(t, err)

		count, err := memTable.flush(context.Background())
		assert.NoError(t, err, "failed to processBatch")
		assert.Equal(t, recordCount+2, count, "expected records to be flushed")

		retrievedValue, err := memTable.db.Get([]byte(key))
		assert.NoError(t, err, "failed to Get")
		assert.Equal(t, checksum, crc32.ChecksumIEEE(retrievedValue), "unexpected value on Get")
	})

	t.Run("flush_indirect_value", func(t *testing.T) {
		// Put direct values on mem table.
		memTable := setupMemTableWithBoltDB(t, 1<<20)
		recordCount := 10
		kv := generateNFBRecord(t, uint64(recordCount))
		for k, v := range kv {
			offset, err := memTable.wIO.Append(v)
			assert.NoError(t, err)
			vs := dbkernel.getValueStruct(dbkernel.logOperationInsert, false, offset.Encode())
			err = memTable.Put([]byte(k), vs, offset)
			assert.NoError(t, err)
		}

		count, err := memTable.flush(context.Background())
		assert.NoError(t, err, "failed to processBatch")
		assert.Equal(t, recordCount, count, "expected records to be flushed")
		for k, v := range kv {
			retrievedValue, err := memTable.db.Get([]byte(k))
			assert.NoError(t, err, "failed to Get")
			recordValue := walrecord.GetRootAsWalRecord(v, 0)
			assert.Equal(t, recordValue.ValueBytes(), retrievedValue, "unexpected value on Get")
		}
	})
}

func TestMemTable_Flush_SetDelete(t *testing.T) {

	// Put direct values on mem table.
	memTable := setupMemTableWithBoltDB(t, 1<<20)
	recordCount := 10
	kv := generateNFBRecord(t, uint64(recordCount))

	t.Run("flush_direct_value", func(t *testing.T) {

		for k, v := range kv {
			offset, err := memTable.wIO.Append(v)
			assert.NoError(t, err)
			vs := dbkernel.getValueStruct(dbkernel.logOperationInsert, true, v)
			err = memTable.Put([]byte(k), vs, offset)
			assert.NoError(t, err)
		}

		count, err := memTable.flush(context.Background())
		assert.NoError(t, err, "failed to processBatch")
		assert.Equal(t, recordCount, count, "expected records to be flushed")
		for k, v := range kv {
			retrievedValue, err := memTable.db.Get([]byte(k))
			assert.NoError(t, err, "failed to Get")
			recordValue := walrecord.GetRootAsWalRecord(v, 0)
			assert.Equal(t, recordValue.ValueBytes(), retrievedValue, "unexpected value on Get")
		}
	})

	t.Run("flush_delete_value", func(t *testing.T) {
		for k, v := range kv {
			retrievedValue, err := memTable.db.Get([]byte(k))
			assert.NoError(t, err, "failed to Get")
			recordValue := walrecord.GetRootAsWalRecord(v, 0)
			assert.Equal(t, recordValue.ValueBytes(), retrievedValue, "unexpected value on Get")
			record := walrecord.Record{
				Index:         1,
				Hlc:           1,
				Key:           []byte(k),
				Value:         nil,
				LogOperation:  walrecord.LogOperationDelete,
				EntryType:     walrecord.EntryTypeKV,
				TxnStatus:     walrecord.TxnStatusTxnNone,
				TxnID:         nil,
				PrevTxnOffset: nil,
			}

			encoded, err := record.FBEncode()
			assert.NoError(t, err)
			offset, err := memTable.wIO.Append(encoded)
			assert.NoError(t, err)
			vs := dbkernel.getValueStruct(dbkernel.logOperationDelete, true, encoded)
			err = memTable.Put([]byte(k), vs, offset)
			assert.NoError(t, err)
		}

		count, err := memTable.flush(context.Background())
		assert.NoError(t, err, "failed to processBatch")
		assert.Equal(t, recordCount, count, "expected records to be flushed")
		for k := range kv {
			retrievedValue, err := memTable.db.Get([]byte(k))
			assert.Nil(t, retrievedValue)
			assert.ErrorIs(t, err, kvdrivers2.ErrKeyNotFound, "failed to Get")
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
			EntryType:     walrecord.EntryTypeKV,
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
		EntryType:     walrecord.EntryTypeChunked,
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
			EntryType:     walrecord.EntryTypeChunked,
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
				Hlc:           dbkernel.HLCNow(uint64(j)),
				Key:           []byte(rowKey),
				Value:         nil,
				LogOperation:  walrecord.LogOperationInsert,
				TxnID:         nil,
				TxnStatus:     walrecord.TxnStatusPrepare,
				EntryType:     walrecord.EntryTypeRow,
				PrevTxnOffset: nil,
				ColumnEntries: entries,
			}

			encoded, err := record.FBEncode()
			assert.NoError(t, err)

			v := dbkernel.getValueStruct(dbkernel.logOperationInsert, true, encoded)
			v.UserMeta = dbkernel.entryTypeRow
			err = mmTable.Put([]byte(rowKey), v, nil)
			assert.NoError(t, err)
		}
	}

	for k, v := range rowsEntries {
		rowEntries := mmTable.GetRowYValue([]byte(k))
		buildColumns := make(map[string][]byte)
		err := dbkernel.buildColumnMap(buildColumns, rowEntries, mmTable.wIO)
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
		Hlc:           dbkernel.HLCNow(uint64(50)),
		Key:           []byte(randomRow),
		Value:         nil,
		LogOperation:  walrecord.LogOperationDelete,
		TxnID:         nil,
		TxnStatus:     walrecord.TxnStatusPrepare,
		EntryType:     walrecord.EntryTypeRow,
		PrevTxnOffset: nil,
		ColumnEntries: deleteEntries,
	}

	encoded, err := record.FBEncode()
	assert.NoError(t, err)

	v := dbkernel.getValueStruct(dbkernel.logOperationDelete, true, encoded)
	v.UserMeta = dbkernel.entryTypeRow
	err = mmTable.Put([]byte(randomRow), v, nil)
	assert.NoError(t, err)

	rowEntries := mmTable.GetRowYValue([]byte(randomRow))
	buildColumns := make(map[string][]byte)
	err = dbkernel.buildColumnMap(buildColumns, rowEntries, mmTable.wIO)
	assert.NoError(t, err, "failed to build column map")
	assert.Equal(t, len(buildColumns), len(columnMap)-len(deleteEntries), "unexpected number of column values")
	assert.NotContains(t, buildColumns, deleteEntries, "unexpected column values")
}
