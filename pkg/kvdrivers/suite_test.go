package kvdrivers_test

import (
	"bytes"
	"fmt"
	"hash/crc32"
	"io"
	"path/filepath"
	"testing"

	"github.com/ankur-anand/unisondb/pkg/kvdrivers"
	"github.com/brianvoe/gofakeit/v7"
	"github.com/stretchr/testify/assert"
)

// kvStoreWriter defines the interface for interacting with a B-tree based storage
// for setting individual values, chunks and many value at once.
type kvStoreWriter interface {
	FSync() error
	SetKV(key []byte, value []byte) error
	BatchSetKV(keys [][]byte, value [][]byte) error
	SetLobChunks(key []byte, chunks [][]byte, checksum uint32) error
	BatchSetCells(rowKeys [][]byte, columnEntriesPerRow []map[string][]byte) error
	BatchDeleteCells(rowKeys [][]byte, columnEntriesPerRow []map[string][]byte) error
	BatchDeleteRows(rowKeys [][]byte) (int, error)
	DeleteKV(key []byte) error
	BatchDeleteKV(keys [][]byte) error
	BatchDeleteLobChunks(keys [][]byte) error
	StoreMetadata(key []byte, value []byte) error
	Restore(reader io.Reader) error
}

type TxnBatcher interface {
	BatchPutKV(keys, values [][]byte) error
	BatchDeleteKV(keys [][]byte) error
	SetLobChunks(key []byte, chunks [][]byte, checksum uint32) error
	BatchDeleteLobChunks(keys [][]byte) error
	BatchSetCells(rowKeys [][]byte, columnEntriesPerRow []map[string][]byte) error
	BatchDeleteCells(rowKeys [][]byte, columnEntriesPerRow []map[string][]byte) error
	BatchDeleteRows(rowKeys [][]byte) error
	Stats() kvdrivers.TxnStats
	Commit() error
}

// kvStoreReader defines the interface for interacting with a B-tree based storage
// for getting individual values, chunks and many value at once.
type kvStoreReader interface {
	GetKV(key []byte) ([]byte, error)
	GetLOBChunks(key []byte) ([][]byte, error)
	ScanRowCells(rowKey []byte, filter func(columnKey []byte) bool) (map[string][]byte, error)
	GetCell(rowKey []byte, columnName string) ([]byte, error)
	GetCells(rowKey []byte, columns []string) (map[string][]byte, error)
	RetrieveMetadata(key []byte) ([]byte, error)
	Snapshot(w io.Writer) error
}

// bTreeStore combines the btreeWriter and kvStoreReader interfaces.
type bTreeStore interface {
	kvStoreWriter
	kvStoreReader
	Close() error
}

// testSuite defines all the test cases that is common in both the lmdb and boltdb.
type testSuite struct {
	dbConstructor         func(path string, config kvdrivers.Config) (bTreeStore, error)
	txnBatcherConstructor func(maxBatchSize int) TxnBatcher
	store                 bTreeStore
}

type suite struct {
	name    string
	runFunc func(*testing.T)
}

func getTestSuites(factory *testSuite) []suite {
	var ts []suite

	ts = append(ts, basicKVSuite(factory)...)
	ts = append(ts, lobChunkKVSuite(factory)...)
	ts = append(ts, rowColumnSuite(factory)...)
	ts = append(ts, metadataSuite(factory)...)
	ts = append(ts, snapshotSuite(factory)...)
	ts = append(ts, txnSuite(factory)...)

	return ts
}

func basicKVSuite(factory *testSuite) []suite {
	return []suite{
		{"TestBasicSetGet", factory.TestBasicSetGet},
		{"TestBasicSetGetDelete", factory.TestBasicSetGetAndDelete},
		{"TestBathSetGetAndDelete", factory.TestBathSetGetAndDelete},
		{"TestEmptyValueSet", factory.TestEmptyValueSet},
		{"TestNonExistent", factory.TestNonExistent},
		{"TestBatchSetKV_LengthMismatch", factory.TestBatchSetKV_LengthMismatch},
		{"TestBatchDeleteKV_NoOpOnMissing", factory.TestBatchDeleteKV_NoOpOnMissing},
		{"TestKV_BinaryKeyAndValue", factory.TestKV_BinaryKeyAndValue},
		{"TestFSync_Smoke", factory.TestFSync_Smoke},
	}
}

func lobChunkKVSuite(factory *testSuite) []suite {
	return []suite{
		{"TestBlobChunkSetGetAndDelete", factory.TestBlobChunkSetGetAndDelete},
		{"TestSetGetAndDeleteMany_Combined", factory.TestSetGetAndDeleteMany_Combined},
		{"TestBatchDeleteLobChunks_NoOpOnMissing", factory.TestBatchDeleteLobChunks_NoOpOnMissing},
	}
}

func rowColumnSuite(factory *testSuite) []suite {
	return []suite{
		{"TestRowColumnSetGetDelete", factory.TestRowColumnSetGetDelete},
		{"TestRowColumnWithFilter", factory.TestRowColumnWithFilter},
		{"TestRowColumnMultipleRows", factory.TestRowColumnMultipleRows},
		{"TestBatchDeleteRows_MixedAndEmpty", factory.TestBatchDeleteRows_MixedAndEmpty},
		{"TestGetCells_PartialHit", factory.TestGetCells_PartialHit},
		{"TestScanRowCells_FilterExcludesAll", factory.TestScanRowCells_FilterExcludesAll},
		{"TestBatchCells_InvalidArgsLengthMismatch", factory.TestBatchCells_InvalidArgsLengthMismatch},
		{"TestGetCells_EmptyRequest", factory.TestGetCells_EmptyRequest},
		{"TestRowColumn_BinaryRowAndCol", factory.TestRowColumn_BinaryRowAndCol},
		{"TestGetCell_MissingRowOrColumn", factory.TestGetCell_MissingRowOrColumn},
	}
}

func metadataSuite(factory *testSuite) []suite {
	return []suite{
		{"TestMetadataSetGet", factory.TestStoreMetadataAndRetrieveMetadata},
		{"TestMetadataGet", factory.TestRetrieveMetadata},
	}
}

func snapshotSuite(factory *testSuite) []suite {
	return []suite{
		{"TestSnapshotAndRetrieve", factory.TestSnapshotAndRetrieve},
		{"TestRestore_ReplacesExistingData", factory.TestRestore_ReplacesExistingData},
	}
}

func txnSuite(factory *testSuite) []suite {
	return []suite{
		{"TestTxnQueue_BatchPutGetDelete", factory.TestTxnQueue_BatchPutGetDelete},
		{"TestTxnSetGetAndDeleteMany", factory.TestTxnSetGetAndDeleteMany},
		{"TestTxn_ChunkSetAndDelete", factory.TestTxn_ChunkSetAndDelete},
		{"TestTxn_SetGetDelete_RowColumns", factory.TestTxn_SetGetDelete_RowColumns},
		{"TestTxn_SetGetDelete_NMRowColumns", factory.TestTxn_SetGetDelete_NMRowColumns},
		{"TestTxn_SetGetAndDeleteMany_Combined", factory.TestTxn_SetGetAndDeleteMany_Combined},
	}
}

func (s *testSuite) TestBasicSetGet(t *testing.T) {
	key := []byte("test_key")
	value := []byte("hello world")
	err := s.store.SetKV(key, value)
	assert.NoError(t, err, "Failed to insert full value")
	retrievedValue, err := s.store.GetKV(key)
	assert.NoError(t, err, "Failed to retrieve value")
	assert.Equal(t, value, retrievedValue, "retrieved value should be the same")
}

func (s *testSuite) TestBasicSetGetAndDelete(t *testing.T) {
	key := []byte("test_key")
	value := []byte("hello world")
	err := s.store.SetKV(key, value)
	assert.NoError(t, err, "Failed to insert full value")
	retrievedValue, err := s.store.GetKV(key)
	assert.NoError(t, err, "Failed to retrieve value")
	assert.Equal(t, value, retrievedValue, "retrieved value should be the same")
	err = s.store.DeleteKV(key)
	assert.NoError(t, err, "Failed to delete key")
	retrievedValue, err = s.store.GetKV(key)
	assert.ErrorIs(t, err, kvdrivers.ErrKeyNotFound, "error should be ErrKeyNotFound")
	assert.Nil(t, retrievedValue, "retrieved value should be nil")
}

func (s *testSuite) TestBathSetGetAndDelete(t *testing.T) {
	var keys [][]byte
	var values [][]byte
	for i := 0; i < 10; i++ {
		keys = append(keys, []byte(fmt.Sprintf("key_%d", i)))
		values = append(values, []byte(fmt.Sprintf("value_%d", i)))
	}

	err := s.store.BatchSetKV(keys, values)
	assert.NoError(t, err, "Failed to insert values")

	for i, key := range keys {
		retrievedValue, err := s.store.GetKV(key)
		assert.NoError(t, err, "Failed to retrieve value")
		assert.Equal(t, values[i], retrievedValue, "retrieved value should be the same")
	}

	err = s.store.BatchDeleteKV(keys)
	assert.NoError(t, err, "Failed to delete keys")
	for _, key := range keys {
		retrievedValue, err := s.store.GetKV(key)
		assert.Empty(t, retrievedValue, "retrieved value should be empty")
		assert.ErrorIs(t, err, kvdrivers.ErrKeyNotFound, "error should be ErrKeyNotFound")
	}
}

func (s *testSuite) TestEmptyValueSet(t *testing.T) {
	var keys [][]byte
	for i := 0; i < 100; i++ {
		key := gofakeit.LetterN(uint(i + 1))
		keys = append(keys, []byte(key))
		err := s.store.SetKV([]byte(key), nil)
		assert.NoError(t, err, "Failed to insert value")
	}

	for _, key := range keys {
		retrievedValue, err := s.store.GetKV(key)
		assert.NoError(t, err, "Failed to retrieve value")
		assert.Equal(t, []byte(""), retrievedValue, "Retrieved value does not match")
	}
}

func (s *testSuite) TestNonExistent(t *testing.T) {
	var keys [][]byte
	for i := 0; i < 100; i++ {
		key := fmt.Sprintf("TestNonExistent_%s_%d", gofakeit.LetterN(uint(i+1)), i)
		keys = append(keys, []byte(key))
		_, err := s.store.GetKV([]byte(key))
		assert.ErrorIs(t, err, kvdrivers.ErrKeyNotFound, "error should be ErrKeyNotFound")
		err = s.store.DeleteKV([]byte(key))
		assert.NoError(t, err, "Failed to delete value")
	}

	err := s.store.BatchDeleteKV(keys)
	assert.NoError(t, err, "Failed to delete")
}

func (s *testSuite) TestBlobChunkSetGetAndDelete(t *testing.T) {

	key := []byte("chunked_key")
	chunks := [][]byte{
		[]byte("chunk_1_"),
		[]byte("chunk_2_"),
		[]byte("chunk_3"),
	}

	var checksum uint32
	for i := 0; i < len(chunks); i++ {
		checksum = crc32.Update(checksum, crc32.IEEETable, chunks[i])
	}

	err := s.store.SetLobChunks(key, chunks, checksum)
	assert.NoError(t, err, "Failed to insert chunked value")

	retrievedValue, err := s.store.GetLOBChunks(key)
	assert.NoError(t, err, "Failed to retrieve chunked value")

	assert.Equal(t, chunks, retrievedValue, "Retrieved chunked value does not match")

	chunks = [][]byte{
		[]byte("chunk_1_"),
		[]byte("chunk_2_"),
	}

	checksum = 0
	for i := 0; i < len(chunks); i++ {
		checksum = crc32.Update(checksum, crc32.IEEETable, chunks[i])
	}

	err = s.store.SetLobChunks(key, chunks, checksum)
	assert.NoError(t, err, "Failed to insert chunked value")

	retrievedValue, err = s.store.GetLOBChunks(key)
	assert.NoError(t, err, "Failed to retrieve chunked value")
	assert.Equal(t, chunks, retrievedValue, "Retrieved chunked value does not match")

	err = s.store.BatchDeleteLobChunks([][]byte{key})
	assert.NoError(t, err, "Failed to delete key")

	_, err = s.store.GetLOBChunks(key)
	assert.ErrorIs(t, err, kvdrivers.ErrKeyNotFound, "error should be ErrKeyNotFound")
}

func (s *testSuite) TestSnapshotAndRetrieve(t *testing.T) {
	testData := make(map[string][]byte)
	for i := 0; i < 100; i++ {
		key := gofakeit.LetterN(uint(i + 1))
		value := gofakeit.LetterN(uint(i + 100))
		testData[key] = []byte(value)
	}

	for key := range testData {
		err := s.store.SetKV([]byte(key), testData[key])
		assert.NoError(t, err, "Failed to insert value")
	}

	buf := new(bytes.Buffer)
	err := s.store.Snapshot(buf)
	assert.NoError(t, err, "Failed to snapshot value")

	restoreDir := t.TempDir()
	path := filepath.Join(restoreDir, "snapshot")
	conf := kvdrivers.Config{
		Namespace: "test",
		NoSync:    true,
		MmapSize:  1 << 30,
	}
	restoreDB, err := s.dbConstructor(path, conf)
	assert.NoError(t, err, "Failed to create db constructor")
	err = restoreDB.Restore(bytes.NewReader(buf.Bytes()))
	assert.NoError(t, err, "Failed to restore")
	for key := range testData {
		retrievedValue, err := s.store.GetKV([]byte(key))
		assert.NoError(t, err, "Failed to get value")
		assert.Equal(t, testData[key], retrievedValue, "Retrieved value does not match")
	}
}

func (s *testSuite) TestStoreMetadataAndRetrieveMetadata(t *testing.T) {
	metadata := gofakeit.UUID()
	key := []byte("metadata")
	err := s.store.StoreMetadata(key, []byte(metadata))
	assert.NoError(t, err, "Failed to store metadata")
	retrievedValue, err := s.store.RetrieveMetadata(key)
	assert.NoError(t, err, "Failed to retrieve metadata")
	assert.Equal(t, metadata, string(retrievedValue), "Retrieved metadata does not match")
}

func (s *testSuite) TestRetrieveMetadata(t *testing.T) {
	key := []byte("hello")
	retrievedValue, err := s.store.RetrieveMetadata(key)
	assert.ErrorIs(t, err, kvdrivers.ErrKeyNotFound)
	assert.Nil(t, retrievedValue, "Retrieved value should be nil")

}

func (s *testSuite) TestSetGetAndDeleteMany_Combined(t *testing.T) {
	t.Helper()

	lobKey := []byte("chunked_key_to_be_deleted")
	chunks := [][]byte{
		[]byte("chunk_1_"),
		[]byte("chunk_2_"),
		[]byte("chunk_3"),
	}

	var checksum uint32
	for i := 0; i < len(chunks); i++ {
		checksum = crc32.Update(checksum, crc32.IEEETable, chunks[i])
	}

	n := len(chunks)
	keys := make([][]byte, n)
	values := make([][]byte, n)
	for i := 0; i < n; i++ {
		keys[i] = []byte(gofakeit.UUID())
		values[i] = []byte(gofakeit.LetterN(uint(i + 1)))
		assert.NoError(t, s.store.SetKV(keys[i], values[i]), "SetKV failed")
	}

	assert.NoError(t, s.store.SetLobChunks(lobKey, chunks, checksum), "SetLobChunks failed")

	nonExistKV := []byte(gofakeit.UUID())
	nonExistLOB := []byte("missing-blob-id")

	kvToDelete := append(append([][]byte(nil), keys...), nonExistKV)
	assert.NoError(t, s.store.BatchDeleteKV(kvToDelete), "BatchDeleteKV failed")

	assert.NoError(t, s.store.BatchDeleteLobChunks([][]byte{lobKey, nonExistLOB}), "BatchDeleteLobChunks failed")

	for _, k := range keys {
		val, err := s.store.GetKV(k)
		assert.ErrorIs(t, err, kvdrivers.ErrKeyNotFound, "expected KV to be deleted")
		assert.Nil(t, val, "deleted KV should return nil value")
	}

	{
		val, err := s.store.GetKV(nonExistKV)
		assert.ErrorIs(t, err, kvdrivers.ErrKeyNotFound)
		assert.Nil(t, val)
	}

	{
		got, err := s.store.GetLOBChunks(lobKey)
		assert.ErrorIs(t, err, kvdrivers.ErrKeyNotFound, "expected LOB to be deleted")
		assert.Nil(t, got)
	}

	{
		got, err := s.store.GetLOBChunks(nonExistLOB)
		assert.ErrorIs(t, err, kvdrivers.ErrKeyNotFound)
		assert.Nil(t, got)
	}
}

func (s *testSuite) TestRowColumnSetGetDelete(t *testing.T) {

	singleRowKey := []byte("single_row")
	singleColumnKey := []byte("single_column")
	singleColumnValue := []byte("single_column_value")

	rowKeys := [][]byte{singleRowKey}
	columnEntries := []map[string][]byte{
		{
			string(singleColumnKey): singleColumnValue,
		},
	}

	err := s.store.BatchSetCells(rowKeys, columnEntries)
	assert.NoError(t, err, "Failed to set single row/column pair")
	scannedValue, err := s.store.ScanRowCells(singleRowKey, nil)
	assert.NoError(t, err, "Failed to scan single row/column pair")
	assert.Equal(t, columnEntries[0], scannedValue, "Scan result does not match")

	getValue, err := s.store.GetCell(singleRowKey, string(singleColumnKey))
	assert.NoError(t, err, "Failed to get value")
	assert.Equal(t, singleColumnValue, getValue, "Get result does not match")

	rowKey := "rows_key"
	entries := make(map[string][]byte)
	for i := 0; i < 1000; i++ {
		key := gofakeit.UUID()
		value := gofakeit.LetterN(uint(1000))
		entries[key] = []byte(value)
	}

	rowKeys = [][]byte{[]byte(rowKey)}
	columEntriesPerRow := make([]map[string][]byte, 0)
	columEntriesPerRow = append(columEntriesPerRow, entries)

	t.Run("invalid-arguments", func(t *testing.T) {
		err := s.store.BatchSetCells(rowKeys, nil)
		assert.ErrorIs(t, err, kvdrivers.ErrInvalidArguments)
		err = s.store.BatchDeleteCells(rowKeys, nil)
		assert.ErrorIs(t, err, kvdrivers.ErrInvalidArguments)
	})

	t.Run("set", func(t *testing.T) {
		err := s.store.BatchSetCells(rowKeys, columEntriesPerRow)
		assert.NoError(t, err, "Failed to set row columns")
		_, err = s.store.GetKV([]byte(rowKey))
		assert.ErrorIs(t, err, kvdrivers.ErrKeyNotFound)
		_, err = s.store.GetLOBChunks([]byte(rowKey))
		assert.ErrorIs(t, err, kvdrivers.ErrKeyNotFound)

	})

	t.Run("get", func(t *testing.T) {
		fetchedEntries, err := s.store.ScanRowCells([]byte(rowKey), nil)
		assert.NoError(t, err, "Failed to fetch row columns")
		assert.Equal(t, len(entries), len(fetchedEntries))
	})

	// delete 10 Columns
	deleteColumnPerRow := make([]map[string][]byte, 0)
	deleteColumn := make(map[string][]byte)
	for i := 0; i < 100; i++ {
		deleteColumn[gofakeit.RandomMapKey(entries).(string)] = nil
	}
	deleteColumnPerRow = append(deleteColumnPerRow, deleteColumn)

	t.Run("delete_column", func(t *testing.T) {
		err := s.store.BatchDeleteCells(rowKeys, deleteColumnPerRow)
		assert.NoError(t, err, "Failed to delete row columns")

		retrievedEntries, err := s.store.ScanRowCells([]byte(rowKey), nil)
		assert.NoError(t, err, "Failed to fetch row columns")
		assert.Equal(t, len(deleteColumn), len(entries)-len(retrievedEntries))
	})

	t.Run("get_delete", func(t *testing.T) {
		for key := range deleteColumn {
			value, err := s.store.GetCell([]byte(rowKey), key)
			assert.ErrorIs(t, err, kvdrivers.ErrKeyNotFound)
			assert.Nil(t, value, "Retrieved value should be nil")
		}
	})

	updateColumnPerRow := make([]map[string][]byte, 0)
	updateColumn := make(map[string][]byte)
	columnKeys := make([]string, 0)
	t.Run("put_more_columns", func(t *testing.T) {
		for i := 0; i < 10; i++ {
			key := gofakeit.UUID()
			columnKeys = append(columnKeys, key)
			value := gofakeit.LetterN(uint(1000))
			entries[key] = []byte(value)
			updateColumn[key] = []byte(value)
		}
		updateColumnPerRow = append(updateColumnPerRow, updateColumn)

		err := s.store.BatchSetCells(rowKeys, updateColumnPerRow)
		assert.NoError(t, err, "Failed to set row columns")
		v, err := s.store.GetCells([]byte(rowKey), columnKeys)
		assert.NoError(t, err, "Failed to fetch row columns")
		assert.Equal(t, len(columnKeys), len(v))
	})

	t.Run("delete_row", func(t *testing.T) {
		deleteRow, err := s.store.BatchDeleteRows(rowKeys)
		assert.NoError(t, err, "Failed to delete row")
		assert.Equal(t, deleteRow, len(entries)-len(deleteColumn), "total deleted Column should match")
	})
}

func (s *testSuite) TestRowColumnWithFilter(t *testing.T) {
	rowKey := "rows_key"
	filterKeys := make(map[string][]byte)
	entries := make(map[string][]byte)
	for i := 0; i < 10; i++ {
		key := gofakeit.UUID()
		value := gofakeit.LetterN(uint(1000))
		entries[key] = []byte(value)
		filterKeys[key] = []byte(value)
	}

	for i := 0; i < 100; i++ {
		key := gofakeit.UUID()
		value := gofakeit.LetterN(uint(1000))
		entries[key] = []byte(value)
	}

	rowKeys := [][]byte{[]byte(rowKey)}
	columEntriesPerRow := make([]map[string][]byte, 0)
	columEntriesPerRow = append(columEntriesPerRow, entries)
	filteredEntries := make([]map[string][]byte, 0)
	filteredEntries = append(filteredEntries, filterKeys)

	t.Run("set", func(t *testing.T) {
		err := s.store.BatchSetCells(rowKeys, filteredEntries)
		assert.NoError(t, err, "Failed to set row columns")
		err = s.store.BatchSetCells(rowKeys, columEntriesPerRow)
		assert.NoError(t, err, "Failed to set row columns")
	})

	t.Run("get", func(t *testing.T) {
		fetchedEntries, err := s.store.ScanRowCells([]byte(rowKey), func(i []byte) bool {
			return filterKeys[string(i)] != nil
		})
		assert.NoError(t, err, "Failed to fetch row columns")
		assert.Equal(t, filterKeys, fetchedEntries)
	})

	t.Run("get_validate", func(t *testing.T) {
		fetchedEntries, err := s.store.ScanRowCells([]byte(rowKey), nil)
		assert.NoError(t, err, "Failed to fetch row columns")
		assert.Equal(t, entries, fetchedEntries)
	})

	t.Run("un_matched_column_should_not_return_any_error", func(t *testing.T) {
		nonExistentColumns := map[string][]byte{}
		for i := 0; i < 100; i++ {
			key := gofakeit.UUID()
			value := gofakeit.LetterN(uint(1000))
			nonExistentColumns[key] = []byte(value)
		}

		err := s.store.BatchDeleteCells(rowKeys, []map[string][]byte{nonExistentColumns})
		assert.NoError(t, err)
	})
}

func (s *testSuite) TestRowColumnMultipleRows(t *testing.T) {
	rowKey1 := "rows_key_nm_1"
	columnsRow1 := make(map[string][]byte)
	for i := 0; i < 5; i++ {
		key := gofakeit.UUID()
		value := gofakeit.LetterN(uint(1000))
		columnsRow1[key] = []byte(value)
	}

	rowKey2 := "rows_key_nm_2"
	columnsRow2 := make(map[string][]byte)
	for i := 0; i < 4; i++ {
		key := gofakeit.UUID()
		value := gofakeit.LetterN(uint(1000))
		columnsRow2[key] = []byte(value)
	}

	rowKeys := [][]byte{[]byte(rowKey1), []byte(rowKey2)}

	columEntriesPerRow := make([]map[string][]byte, 0)
	columEntriesPerRow = append(columEntriesPerRow, columnsRow1, columnsRow2)

	t.Run("set", func(t *testing.T) {
		err := s.store.BatchSetCells(rowKeys, columEntriesPerRow)
		assert.NoError(t, err, "Failed to set row columns")
	})

	t.Run("get", func(t *testing.T) {
		fe2, err := s.store.ScanRowCells([]byte(rowKey2), nil)
		assert.NoError(t, err, "Failed to fetch row columns")
		assert.Equal(t, columnsRow2, fe2)

		fe1, err := s.store.ScanRowCells([]byte(rowKey1), nil)
		assert.NoError(t, err, "Failed to fetch row columns")
		assert.Equal(t, columnsRow1, fe1)
	})

	// delete 10 Columns
	deleteColumnPerRow := make([]map[string][]byte, 0)
	deleteColumn := make(map[string][]byte)
	for i := 0; i < 2; i++ {
		deleteColumn[gofakeit.RandomMapKey(columnsRow1).(string)] = nil
	}
	deleteColumnPerRow = append(deleteColumnPerRow, deleteColumn)

	t.Run("delete_column_1", func(t *testing.T) {
		err := s.store.BatchDeleteCells([][]byte{[]byte(rowKey1)}, deleteColumnPerRow)
		assert.NoError(t, err, "Failed to delete row columns")

		retrievedEntries, err := s.store.ScanRowCells([]byte(rowKey1), nil)
		assert.NoError(t, err, "Failed to fetch row columns")
		assert.Equal(t, len(deleteColumn), len(columnsRow1)-len(retrievedEntries))
	})

	t.Run("get_delete", func(t *testing.T) {
		for key := range deleteColumn {
			value, err := s.store.GetCell([]byte(rowKey1), key)
			assert.ErrorIs(t, err, kvdrivers.ErrKeyNotFound)
			assert.Nil(t, value, "Retrieved value should be nil")
		}
	})

	updateColumnPerRow := make([]map[string][]byte, 0)
	updateColumn := make(map[string][]byte)
	t.Run("put_more_columns_1", func(t *testing.T) {
		for i := 0; i < 1; i++ {
			key := gofakeit.UUID()
			value := gofakeit.LetterN(uint(1000))
			columnsRow1[key] = []byte(value)
			updateColumn[key] = []byte(value)
		}
		updateColumnPerRow = append(updateColumnPerRow, updateColumn)

		err := s.store.BatchSetCells([][]byte{[]byte(rowKey1)}, updateColumnPerRow)
		assert.NoError(t, err, "Failed to set row columns")

		for key, entry := range columnsRow1 {
			if _, ok := deleteColumn[key]; ok {
				continue
			}
			value, err := s.store.GetCell([]byte(rowKey1), key)
			assert.Nil(t, err, "Failed to fetch row columns")
			assert.Equal(t, value, entry)
		}
	})

	totalDeleted := len(columnsRow1) + len(columnsRow2) - len(deleteColumn)

	t.Run("delete_row", func(t *testing.T) {
		deleteRow, err := s.store.BatchDeleteRows(rowKeys)
		assert.NoError(t, err, "Failed to delete row")
		assert.Equal(t, deleteRow, totalDeleted, "total deleted Column should match")
	})

	t.Run("get_key_not_found", func(t *testing.T) {
		result, err := s.store.ScanRowCells([]byte(rowKey2), nil)
		assert.ErrorIs(t, err, kvdrivers.ErrKeyNotFound)
		assert.Nil(t, result)

		result, err = s.store.ScanRowCells([]byte(rowKey1), nil)
		assert.ErrorIs(t, err, kvdrivers.ErrKeyNotFound)
		assert.Nil(t, result)
	})
}

func (s *testSuite) TestBatchDeleteRows_MixedAndEmpty(t *testing.T) {
	row1 := []byte("row_exists_1")
	row2 := []byte("row_exists_2")
	missing := []byte("row_missing")

	cols1 := map[string][]byte{"a": []byte("1"), "b": []byte("2")}
	cols2 := map[string][]byte{"x": []byte("9")}

	assert.NoError(t, s.store.BatchSetCells([][]byte{row1, row2}, []map[string][]byte{cols1, cols2}))

	deleted, err := s.store.BatchDeleteRows([][]byte{row1, missing})
	assert.NoError(t, err)
	assert.Equal(t, 2, deleted)

	_, err = s.store.ScanRowCells(row1, nil)
	assert.ErrorIs(t, err, kvdrivers.ErrKeyNotFound)

	got2, err := s.store.ScanRowCells(row2, nil)
	assert.NoError(t, err)
	assert.Equal(t, cols2, got2)

	deleted, err = s.store.BatchDeleteRows(nil)
	assert.NoError(t, err)
	assert.Equal(t, 0, deleted)
}

func (s *testSuite) TestGetCells_PartialHit(t *testing.T) {
	row := []byte("row_partial")
	cols := map[string][]byte{"c1": []byte("v1"), "c2": []byte("v2")}
	assert.NoError(t, s.store.BatchSetCells([][]byte{row}, []map[string][]byte{cols}))

	got, err := s.store.GetCells(row, []string{"c1", "nope"})
	assert.NoError(t, err)
	assert.Equal(t, map[string][]byte{"c1": []byte("v1")}, got)
}

func (s *testSuite) TestScanRowCells_FilterExcludesAll(t *testing.T) {
	row := []byte("row_filter_none")
	cols := map[string][]byte{"k": []byte("v")}
	assert.NoError(t, s.store.BatchSetCells([][]byte{row}, []map[string][]byte{cols}))

	res, err := s.store.ScanRowCells(row, func(_ []byte) bool { return false })
	assert.ErrorIs(t, err, kvdrivers.ErrKeyNotFound)
	assert.Nil(t, res)
}

func (s *testSuite) TestBatchDeleteLobChunks_NoOpOnMissing(t *testing.T) {
	err := s.store.BatchDeleteLobChunks([][]byte{[]byte("no_such_lob")})
	assert.NoError(t, err)
}

func (s *testSuite) TestBatchCells_InvalidArgsLengthMismatch(t *testing.T) {
	row := []byte("row_bad_args")
	err := s.store.BatchSetCells([][]byte{row}, []map[string][]byte{})
	assert.ErrorIs(t, err, kvdrivers.ErrInvalidArguments)

	err = s.store.BatchDeleteCells([][]byte{row}, []map[string][]byte{})
	assert.ErrorIs(t, err, kvdrivers.ErrInvalidArguments)
}

func (s *testSuite) TestTxnQueue_BatchPutGetDelete(t *testing.T) {
	key := []byte("test_key_txn")
	value := []byte("hello world_txn")

	txn := s.txnBatcherConstructor(10)

	assert.NoError(t, txn.BatchPutKV([][]byte{key}, [][]byte{value}))
	_, err := s.store.GetKV(key)
	assert.ErrorIs(t, err, kvdrivers.ErrKeyNotFound)

	assert.NoError(t, txn.Commit())
	got, err := s.store.GetKV(key)
	assert.NoError(t, err)
	assert.Equal(t, value, got)

	assert.NoError(t, txn.BatchDeleteKV([][]byte{key}))
	got, err = s.store.GetKV(key)
	assert.NoError(t, err)
	assert.Equal(t, value, got)

	assert.NoError(t, txn.Commit())
	_, err = s.store.GetKV(key)
	assert.ErrorIs(t, err, kvdrivers.ErrKeyNotFound)
}

func (s *testSuite) TestTxnSetGetAndDeleteMany(t *testing.T) {
	var keys, values [][]byte
	for i := 0; i < 10; i++ {
		keys = append(keys, []byte(fmt.Sprintf("key_%d", i)))
		values = append(values, []byte(fmt.Sprintf("value_%d", i)))
	}

	txn := s.txnBatcherConstructor(10)

	assert.NoError(t, txn.BatchPutKV(keys, values))
	assert.NoError(t, txn.Commit())

	for i, k := range keys {
		got, err := s.store.GetKV(k)
		assert.NoError(t, err)
		assert.Equal(t, values[i], got)
	}

	assert.NoError(t, txn.BatchDeleteKV(keys))
	assert.NoError(t, txn.Commit())

	for _, k := range keys {
		got, err := s.store.GetKV(k)
		assert.Nil(t, got)
		assert.ErrorIs(t, err, kvdrivers.ErrKeyNotFound)
	}
}

func (s *testSuite) TestTxn_ChunkSetAndDelete(t *testing.T) {
	key := []byte("chunked_key")
	chunks := [][]byte{[]byte("chunk_1_"), []byte("chunk_2_"), []byte("chunk_3")}

	var checksum uint32
	for _, ch := range chunks {
		checksum = crc32.Update(checksum, crc32.IEEETable, ch)
	}

	txn := s.txnBatcherConstructor(10)

	assert.NoError(t, txn.SetLobChunks(key, chunks, checksum))
	assert.NoError(t, txn.Commit())

	gotChunks, err := s.store.GetLOBChunks(key)
	assert.NoError(t, err)
	assert.Equal(t, chunks, gotChunks)

	chunks2 := [][]byte{[]byte("chunk_1_"), []byte("chunk_2_")}
	checksum = 0
	for _, ch := range chunks2 {
		checksum = crc32.Update(checksum, crc32.IEEETable, ch)
	}

	assert.NoError(t, txn.SetLobChunks(key, chunks2, checksum))
	assert.NoError(t, txn.Commit())

	gotChunks, err = s.store.GetLOBChunks(key)
	assert.NoError(t, err)
	assert.Equal(t, chunks2, gotChunks)

	assert.NoError(t, txn.BatchDeleteLobChunks([][]byte{key}))
	assert.NoError(t, txn.Commit())

	gotChunks, err = s.store.GetLOBChunks(key)
	assert.Nil(t, gotChunks)
	assert.ErrorIs(t, err, kvdrivers.ErrKeyNotFound)
}

func (s *testSuite) TestTxn_SetGetDelete_RowColumns(t *testing.T) {
	rowKey := "rows_key_txn"
	entries := make(map[string][]byte)
	for i := 0; i < 100; i++ {
		k := gofakeit.UUID()
		v := gofakeit.LetterN(uint(1))
		entries[k] = []byte(v)
	}

	rowKeys := [][]byte{[]byte(rowKey)}
	columnEntriesPerRow := []map[string][]byte{entries}

	txn := s.txnBatcherConstructor(10)

	t.Run("invalid-arguments", func(t *testing.T) {
		err := txn.BatchSetCells(rowKeys, nil)
		assert.ErrorIs(t, err, kvdrivers.ErrInvalidArguments)
		err = txn.BatchDeleteCells(rowKeys, nil)
		assert.ErrorIs(t, err, kvdrivers.ErrInvalidArguments)
	})

	txn = s.txnBatcherConstructor(10)
	assert.NoError(t, txn.BatchSetCells(rowKeys, columnEntriesPerRow))
	assert.NoError(t, txn.Commit())

	got, err := s.store.ScanRowCells([]byte(rowKey), nil)
	assert.NoError(t, err)
	assert.Equal(t, entries, got)

	for c, v := range entries {
		cv, err := s.store.GetCell([]byte(rowKey), c)
		assert.NoError(t, err)
		assert.Equal(t, v, cv)
	}

	delCols := make(map[string][]byte)
	for i := 0; i < 10; i++ {
		delCols[gofakeit.RandomMapKey(entries).(string)] = nil
	}
	assert.NoError(t, txn.BatchDeleteCells(rowKeys, []map[string][]byte{delCols}))
	assert.NoError(t, txn.Commit())

	after, err := s.store.ScanRowCells([]byte(rowKey), nil)
	assert.NoError(t, err)
	assert.Equal(t, len(entries)-len(delCols), len(after))

	for c := range delCols {
		v, err := s.store.GetCell([]byte(rowKey), c)
		assert.ErrorIs(t, err, kvdrivers.ErrKeyNotFound)
		assert.Nil(t, v)
	}

	add := make(map[string][]byte)
	for i := 0; i < 20; i++ {
		k := gofakeit.UUID()
		v := gofakeit.LetterN(uint(1000))
		add[k] = []byte(v)
		entries[k] = []byte(v)
	}
	assert.NoError(t, txn.BatchSetCells(rowKeys, []map[string][]byte{add}))
	assert.NoError(t, txn.Commit())

	cur, err := s.store.ScanRowCells([]byte(rowKey), nil)
	assert.NoError(t, err)
	assert.Equal(t, entriesCountMinus(entries, delCols), len(cur))
	for k, v := range entries {
		if _, deleted := delCols[k]; deleted {
			continue
		}
		cv, err := s.store.GetCell([]byte(rowKey), k)
		assert.NoError(t, err)
		assert.Equal(t, v, cv)
	}

	txn = s.txnBatcherConstructor(10)
	assert.NoError(t, txn.BatchDeleteRows(rowKeys))
	assert.NoError(t, txn.Commit())

	cur, err = s.store.ScanRowCells([]byte(rowKey), nil)
	assert.ErrorIs(t, err, kvdrivers.ErrKeyNotFound)
	assert.Nil(t, cur)
}

func entriesCountMinus(m map[string][]byte, minus map[string][]byte) int {
	n := 0
	for k := range m {
		if _, del := minus[k]; !del {
			n++
		}
	}
	return n
}

func (s *testSuite) TestTxn_SetGetDelete_NMRowColumns(t *testing.T) {
	rowKey1 := "rows_key_nm_1_txn"
	rowKey2 := "rows_key_nm_2_txn"

	cols1 := map[string][]byte{}
	for i := 0; i < 5; i++ {
		cols1[gofakeit.UUID()] = []byte(gofakeit.LetterN(uint(1000)))
	}
	cols2 := map[string][]byte{}
	for i := 0; i < 4; i++ {
		cols2[gofakeit.UUID()] = []byte(gofakeit.LetterN(uint(1000)))
	}

	rowKeys := [][]byte{[]byte(rowKey1), []byte(rowKey2)}
	entries := []map[string][]byte{cols1, cols2}

	txn := s.txnBatcherConstructor(2)
	assert.NoError(t, txn.BatchSetCells(rowKeys, entries))
	assert.NoError(t, txn.Commit())

	got2, err := s.store.ScanRowCells([]byte(rowKey2), nil)
	assert.NoError(t, err)
	assert.Equal(t, cols2, got2)

	got1, err := s.store.ScanRowCells([]byte(rowKey1), nil)
	assert.NoError(t, err)
	assert.Equal(t, cols1, got1)

	delCols := map[string][]byte{}
	for i := 0; i < 2; i++ {
		delCols[gofakeit.RandomMapKey(cols1).(string)] = nil
	}
	txn = s.txnBatcherConstructor(2)
	assert.NoError(t, txn.BatchDeleteCells([][]byte{[]byte(rowKey1)}, []map[string][]byte{delCols}))
	assert.NoError(t, txn.Commit())

	after1, err := s.store.ScanRowCells([]byte(rowKey1), nil)
	assert.NoError(t, err)
	assert.Equal(t, len(cols1)-len(delCols), len(after1))

	for c := range delCols {
		cv, err := s.store.GetCell([]byte(rowKey1), c)
		assert.ErrorIs(t, err, kvdrivers.ErrKeyNotFound)
		assert.Nil(t, cv)
	}

	add := map[string][]byte{}
	k := gofakeit.UUID()
	v := []byte(gofakeit.LetterN(uint(1000)))
	add[k] = v
	cols1[k] = v

	txn = s.txnBatcherConstructor(1)
	assert.NoError(t, txn.BatchSetCells([][]byte{[]byte(rowKey1)}, []map[string][]byte{add}))
	assert.NoError(t, txn.Commit())

	cur1, err := s.store.ScanRowCells([]byte(rowKey1), nil)
	assert.NoError(t, err)
	assert.Equal(t, len(cols1)-len(delCols), len(cur1))

	txn = s.txnBatcherConstructor(1)
	assert.NoError(t, txn.BatchDeleteRows(rowKeys))
	assert.NoError(t, txn.Commit())

	_, err = s.store.ScanRowCells([]byte(rowKey2), nil)
	assert.ErrorIs(t, err, kvdrivers.ErrKeyNotFound)
	_, err = s.store.ScanRowCells([]byte(rowKey1), nil)
	assert.ErrorIs(t, err, kvdrivers.ErrKeyNotFound)
}

func (s *testSuite) TestTxn_SetGetAndDeleteMany_Combined(t *testing.T) {
	lobKey := []byte("chunked_key_to_be_deleted_txn")
	chunks := [][]byte{[]byte("chunk_1_"), []byte("chunk_2_"), []byte("chunk_3")}

	var checksum uint32
	for _, ch := range chunks {
		checksum = crc32.Update(checksum, crc32.IEEETable, ch)
	}

	txn := s.txnBatcherConstructor(10)

	n := len(chunks)
	keys := make([][]byte, n)
	values := make([][]byte, n)
	for i := 0; i < n; i++ {
		keys[i] = []byte(gofakeit.UUID())
		values[i] = []byte(gofakeit.LetterN(uint(i + 1)))
	}
	assert.NoError(t, txn.BatchPutKV(keys, values))
	assert.NoError(t, txn.Commit())

	assert.NoError(t, txn.SetLobChunks(lobKey, chunks, checksum))
	assert.NoError(t, txn.Commit())

	nonExistKV := []byte(gofakeit.UUID())
	assert.NoError(t, txn.BatchDeleteKV(append(append([][]byte(nil), keys...), nonExistKV)))
	assert.NoError(t, txn.Commit())

	assert.NoError(t, txn.BatchDeleteLobChunks([][]byte{lobKey, []byte("non-existent-blob")}))
	assert.NoError(t, txn.Commit())

	for _, k := range keys {
		_, err := s.store.GetKV(k)
		assert.ErrorIs(t, err, kvdrivers.ErrKeyNotFound)
	}
	_, err := s.store.GetLOBChunks(lobKey)
	assert.ErrorIs(t, err, kvdrivers.ErrKeyNotFound)
}

func (s *testSuite) TestBatchSetKV_LengthMismatch(t *testing.T) {
	keys := [][]byte{[]byte("k1"), []byte("k2")}
	vals := [][]byte{[]byte("v1")}
	err := s.store.BatchSetKV(keys, vals)
	assert.ErrorIs(t, err, kvdrivers.ErrInvalidArguments)
}

func (s *testSuite) TestBatchDeleteKV_NoOpOnMissing(t *testing.T) {
	keys := [][]byte{[]byte("nope1"), []byte("nope2")}
	err := s.store.BatchDeleteKV(keys)
	assert.NoError(t, err)
	for _, k := range keys {
		_, err := s.store.GetKV(k)
		assert.ErrorIs(t, err, kvdrivers.ErrKeyNotFound)
	}
}

func (s *testSuite) TestGetCells_EmptyRequest(t *testing.T) {
	row := []byte("row_empty_getcells")
	cols := map[string][]byte{"a": []byte("1")}
	assert.NoError(t, s.store.BatchSetCells([][]byte{row}, []map[string][]byte{cols}))

	got, err := s.store.GetCells(row, nil)
	assert.NoError(t, err)
	assert.Equal(t, 0, len(got))

	got, err = s.store.GetCells(row, []string{})
	assert.NoError(t, err)
	assert.Equal(t, 0, len(got))
}

func (s *testSuite) TestKV_BinaryKeyAndValue(t *testing.T) {
	key := []byte{0x00, 0xFF, 0x10, 0x7F}
	val := []byte{0x00, 0x01, 0x02, 0x03}
	assert.NoError(t, s.store.SetKV(key, val))

	got, err := s.store.GetKV(key)
	assert.NoError(t, err)
	assert.Equal(t, val, got)

	assert.NoError(t, s.store.DeleteKV(key))
	_, err = s.store.GetKV(key)
	assert.ErrorIs(t, err, kvdrivers.ErrKeyNotFound)
}

func (s *testSuite) TestRowColumn_BinaryRowAndCol(t *testing.T) {
	row := []byte{0x00, 0x01, 0x02}
	colName := string([]byte{0xFF, 0xFE, 0xFD})
	val := []byte{0xAA, 0xBB}

	assert.NoError(t, s.store.BatchSetCells([][]byte{row}, []map[string][]byte{{colName: val}}))

	gotRow, err := s.store.ScanRowCells(row, nil)
	assert.NoError(t, err)
	assert.Equal(t, map[string][]byte{colName: val}, gotRow)

	gotCell, err := s.store.GetCell(row, colName)
	assert.NoError(t, err)
	assert.Equal(t, val, gotCell)

	n, err := s.store.BatchDeleteRows([][]byte{row})
	assert.NoError(t, err)
	assert.Equal(t, 1, n)

	_, err = s.store.GetCell(row, colName)
	assert.ErrorIs(t, err, kvdrivers.ErrKeyNotFound)
}

func (s *testSuite) TestRestore_ReplacesExistingData(t *testing.T) {
	assert.NoError(t, s.store.SetKV([]byte("old_k"), []byte("old_v")))

	dir := t.TempDir()
	path := filepath.Join(dir, "donor.db")
	conf := kvdrivers.Config{Namespace: "test", NoSync: true, MmapSize: 1 << 30}
	donor, err := s.dbConstructor(path, conf)
	assert.NoError(t, err)

	assert.NoError(t, donor.SetKV([]byte("new_k"), []byte("new_v")))
	buf := new(bytes.Buffer)
	assert.NoError(t, donor.Snapshot(buf))
	assert.NoError(t, donor.Close())

	assert.NoError(t, s.store.Restore(bytes.NewReader(buf.Bytes())))

	_, err = s.store.GetKV([]byte("old_k"))
	assert.ErrorIs(t, err, kvdrivers.ErrKeyNotFound)

	v, err := s.store.GetKV([]byte("new_k"))
	assert.NoError(t, err)
	assert.Equal(t, []byte("new_v"), v)
}

func (s *testSuite) TestFSync_Smoke(t *testing.T) {
	for i := 0; i < 50; i++ {
		k := []byte(fmt.Sprintf("fsync_k_%d", i))
		v := []byte(fmt.Sprintf("fsync_v_%d", i))
		assert.NoError(t, s.store.SetKV(k, v))
	}
	assert.NoError(t, s.store.FSync())
}

func (s *testSuite) TestGetCell_MissingRowOrColumn(t *testing.T) {
	row := []byte("rc_missing_row")
	col := "c1"
	_, err := s.store.GetCell(row, col)
	assert.ErrorIs(t, err, kvdrivers.ErrKeyNotFound)

	assert.NoError(t, s.store.BatchSetCells([][]byte{row}, []map[string][]byte{{"c2": []byte("v2")}}))
	_, err = s.store.GetCell(row, "nope")
	assert.ErrorIs(t, err, kvdrivers.ErrKeyNotFound)
}
