package kvdrivers_test

import (
	"bytes"
	"fmt"
	"hash/crc32"
	"io"
	"path/filepath"
	"testing"

	"github.com/ankur-anand/unisondb/dbkernel/internal/kvdrivers"
	"github.com/brianvoe/gofakeit/v7"
	"github.com/stretchr/testify/assert"
)

// btreeWriter defines the interface for interacting with a B-tree based storage
// for setting individual values, chunks and many value at once.
type btreeWriter interface {
	// Put associates a value with a key.
	Set(key []byte, value []byte) error
	// SetMany associates multiple values with corresponding keys.
	SetMany(keys [][]byte, values [][]byte) error
	// SetChunks stores a value that has been split into chunks, associating them with a single key.
	SetChunks(key []byte, chunks [][]byte, checksum uint32) error
	// Delete deletes a value with a key.
	Delete(key []byte) error
	// DeleteMany delete multiple values with corresponding keys.
	DeleteMany(keys [][]byte) error
	SetManyRowColumns(rowKeys [][]byte, columnEntriesPerRow []map[string][]byte) error
	DeleteManyRowColumns(rowKeys [][]byte, columnEntriesPerRow []map[string][]byte) error
	DeleteEntireRows(rowKeys [][]byte) (int, error)
	StoreMetadata(key []byte, value []byte) error
	FSync() error
	Restore(reader io.Reader) error
}

type TxnBatcher interface {
	BatchPut(keys, values [][]byte) error
	BatchDelete(keys [][]byte) error
	SetChunks(key []byte, chunks [][]byte, checksum uint32) error
	BatchPutRowColumns(rowKeys [][]byte, columnEntriesPerRow []map[string][]byte) error
	BatchDeleteRowColumns(rowKeys [][]byte, columnEntriesPerRow []map[string][]byte) error
	BatchDeleteRows(rowKeys [][]byte) error
	Commit() error
	Stats() kvdrivers.TxnStats
}

// btreeReader defines the interface for interacting with a B-tree based storage
// for getting individual values, chunks and many value at once.
type btreeReader interface {
	// Get retrieves a value associated with a key.
	Get(key []byte) ([]byte, error)
	GetRowColumns(rowKey []byte, filter func([]byte) bool) (map[string][]byte, error)
	// SnapShot writes the complete database to the provided io writer.
	Snapshot(w io.Writer) error
	RetrieveMetadata(key []byte) ([]byte, error)
}

// bTreeStore combines the btreeWriter and btreeReader interfaces.
type bTreeStore interface {
	btreeWriter
	btreeReader
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
	return []suite{
		{
			name:    "get_set",
			runFunc: factory.TestSetAndGet,
		},
		{
			name:    "set_get_delete",
			runFunc: factory.TestSetAndGet,
		},
		{
			name:    "many_set_get_and_delete_many",
			runFunc: factory.TestManySetGetAndDeleteMany,
		},
		{
			name:    "chunk_set_get_and_delete",
			runFunc: factory.TestChunkSetAndDelete,
		},
		{
			name:    "snapshot_and_retrieve",
			runFunc: factory.TestSnapshotAndRetrieve,
		},
		{
			name:    "empty_value_set",
			runFunc: factory.TestEmptyValueSet,
		},
		{
			name:    "non_existent_value_delete",
			runFunc: factory.TestNonExistentDelete,
		},
		{
			name:    "retrieve_empty_metadata",
			runFunc: factory.TestRetrieveMetadata,
		},
		{
			name:    "store_and_retrieve_metadata",
			runFunc: factory.TestStoreMetadataAndRetrieveMetadata,
		},
		{
			name:    "store_and_delete_many_chunk_full_value",
			runFunc: factory.TestSetGetAndDeleteMany_Combined,
		},
		{
			name:    "store_set_get_delete_columns",
			runFunc: factory.TestSetGetDelete_RowColumns,
		},
		{
			name:    "store_delete_columns_with_filter",
			runFunc: factory.TestSetGetDelete_RowColumns_Filter,
		},
		{
			name:    "store_set_columns_nm_row_column",
			runFunc: factory.TestSetGetDelete_NMRowColumns,
		},
		{
			name:    "txn_batch_put_get",
			runFunc: factory.TestTxnQueue_BatchPutGetDelete,
		},
		{
			name:    "txn_many_batch_put_delete",
			runFunc: factory.TestTxnSetGetAndDeleteMany,
		},
		{
			name:    "txn_chunk_get_set_del",
			runFunc: factory.TestTxn_ChunkSetAndDelete,
		},
		{
			name:    "txn_row_column_set_get_delete",
			runFunc: factory.TestTxn_SetGetDelete_RowColumns,
		},
		{
			name:    "txn_combined_key_delete",
			runFunc: factory.TestTxn_SetGetAndDeleteMany_Combined,
		},
		{
			name:    "txn_set_delete_nm_rows_columns",
			runFunc: factory.TestTxn_SetGetDelete_NMRowColumns,
		},
	}
}

func (s *testSuite) TestSetAndGet(t *testing.T) {
	key := []byte("test_key")
	value := []byte("hello world")
	err := s.store.Set(key, value)
	assert.NoError(t, err, "Failed to insert full value")
	retrievedValue, err := s.store.Get(key)
	assert.NoError(t, err, "Failed to retrieve value")
	assert.Equal(t, value, retrievedValue, "retrieved value should be the same")
}

func (s *testSuite) TestSetGetAndDelete(t *testing.T) {
	key := []byte("test_key")
	value := []byte("hello world")
	err := s.store.Set(key, value)
	assert.NoError(t, err, "Failed to insert full value")
	retrievedValue, err := s.store.Get(key)
	assert.NoError(t, err, "Failed to retrieve value")
	assert.Equal(t, value, retrievedValue, "retrieved value should be the same")
	err = s.store.Delete(key)
	assert.NoError(t, err, "Failed to delete key")
	retrievedValue, err = s.store.Get(key)
	assert.ErrorIs(t, err, kvdrivers.ErrKeyNotFound, "error should be ErrKeyNotFound")
	assert.Nil(t, retrievedValue, "retrieved value should be nil")
}

func (s *testSuite) TestManySetGetAndDeleteMany(t *testing.T) {
	var keys [][]byte
	var values [][]byte
	for i := 0; i < 10; i++ {
		keys = append(keys, []byte(fmt.Sprintf("key_%d", i)))
		values = append(values, []byte(fmt.Sprintf("value_%d", i)))
	}

	err := s.store.SetMany(keys, values)
	assert.NoError(t, err, "Failed to insert values")

	for i, key := range keys {
		retrievedValue, err := s.store.Get(key)
		assert.NoError(t, err, "Failed to retrieve value")
		assert.Equal(t, values[i], retrievedValue, "retrieved value should be the same")
	}

	err = s.store.DeleteMany(keys)
	assert.NoError(t, err, "Failed to delete keys")
	for _, key := range keys {
		retrievedValue, err := s.store.Get(key)
		assert.Empty(t, retrievedValue, "retrieved value should be empty")
		assert.ErrorIs(t, err, kvdrivers.ErrKeyNotFound, "error should be ErrKeyNotFound")
	}
}

func (s *testSuite) TestChunkSetAndDelete(t *testing.T) {

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

	err := s.store.SetChunks(key, chunks, checksum)
	assert.NoError(t, err, "Failed to insert chunked value")

	retrievedValue, err := s.store.Get(key)
	assert.NoError(t, err, "Failed to retrieve chunked value")

	expectedValue := bytes.Join(chunks, nil)
	assert.Equal(t, expectedValue, retrievedValue, "Retrieved chunked value does not match")

	chunks = [][]byte{
		[]byte("chunk_1_"),
		[]byte("chunk_2_"),
	}

	checksum = 0
	for i := 0; i < len(chunks); i++ {
		checksum = crc32.Update(checksum, crc32.IEEETable, chunks[i])
	}

	err = s.store.SetChunks(key, chunks, checksum)
	assert.NoError(t, err, "Failed to insert chunked value")

	retrievedValue, err = s.store.Get(key)
	assert.NoError(t, err, "Failed to retrieve chunked value")

	expectedValue = bytes.Join(chunks, nil)
	assert.Equal(t, expectedValue, retrievedValue, "Retrieved chunked value does not match")

	keys := make(map[string]error)
	keys["chunked_key_chunk_0"] = kvdrivers.ErrInvalidOpsForValueType // as we are fetching the stored chunk value
	keys["chunked_key_chunk_1"] = kvdrivers.ErrInvalidOpsForValueType
	keys["chunked_key_chunk_2"] = kvdrivers.ErrKeyNotFound

	for key, e := range keys {
		_, err := s.store.Get([]byte(key))
		assert.ErrorIs(t, err, e, "error should be match")
	}

	err = s.store.Delete(key)
	assert.NoError(t, err, "Failed to delete key")
	retrievedValue, err = s.store.Get(key)
	assert.ErrorIs(t, err, kvdrivers.ErrKeyNotFound, "error should be ErrKeyNotFound")
	assert.Nil(t, retrievedValue, "retrieved value should be nil")
	for key := range keys {
		_, err := s.store.Get([]byte(key))
		assert.ErrorIs(t, err, kvdrivers.ErrKeyNotFound, "error should be match")
	}
}

func (s *testSuite) TestSnapshotAndRetrieve(t *testing.T) {
	testData := make(map[string][]byte)
	for i := 0; i < 100; i++ {
		key := gofakeit.LetterN(uint(i + 1))
		value := gofakeit.LetterN(uint(i + 100))
		testData[key] = []byte(value)
	}

	for key := range testData {
		err := s.store.Set([]byte(key), testData[key])
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
		retrievedValue, err := s.store.Get([]byte(key))
		assert.NoError(t, err, "Failed to get value")
		assert.Equal(t, testData[key], retrievedValue, "Retrieved value does not match")
	}
}

func (s *testSuite) TestEmptyValueSet(t *testing.T) {
	var keys [][]byte
	for i := 0; i < 100; i++ {
		key := gofakeit.LetterN(uint(i + 1))
		keys = append(keys, []byte(key))
		err := s.store.Set([]byte(key), nil)
		assert.NoError(t, err, "Failed to insert value")
	}

	for _, key := range keys {
		retrievedValue, err := s.store.Get(key)
		assert.NoError(t, err, "Failed to retrieve value")
		assert.Equal(t, []byte(""), retrievedValue, "Retrieved value does not match")
	}
}

func (s *testSuite) TestNonExistentDelete(t *testing.T) {
	var keys [][]byte
	for i := 0; i < 100; i++ {
		key := gofakeit.LetterN(uint(i + 1))
		keys = append(keys, []byte(key))
		err := s.store.Delete([]byte(key))
		assert.NoError(t, err, "Failed to delete value")
	}

	err := s.store.DeleteMany(keys)
	assert.NoError(t, err, "Failed to delete")
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
	key := []byte("chunked_key_to_be_deleted")
	chunks := [][]byte{
		[]byte("chunk_1_"),
		[]byte("chunk_2_"),
		[]byte("chunk_3"),
	}

	var checksum uint32
	for i := 0; i < len(chunks); i++ {
		checksum = crc32.Update(checksum, crc32.IEEETable, chunks[i])
	}

	keys := make([][]byte, len(chunks))
	values := make([][]byte, len(chunks))
	for i := 0; i < len(chunks); i++ {
		keys[i] = []byte(gofakeit.UUID())
		values[i] = []byte(gofakeit.LetterN(uint(i + 1)))
		assert.NoError(t, s.store.Set(keys[i], values[i]), "Failed to set value")
	}

	assert.NoError(t, s.store.SetChunks(key, chunks, checksum), "Failed to set chunks")

	nonExistentKey := gofakeit.UUID()
	keysToBeDeleted := make([][]byte, 0)

	keysToBeDeleted = append(keysToBeDeleted, keys...)
	keysToBeDeleted = append(keysToBeDeleted, key)
	keysToBeDeleted = append(keysToBeDeleted, []byte(nonExistentKey))

	err := s.store.DeleteMany(keysToBeDeleted)
	assert.NoError(t, err, "Failed to delete many keys")

	for _, key := range keysToBeDeleted {
		retrievedValue, err := s.store.Get(key)
		assert.ErrorIs(t, err, kvdrivers.ErrKeyNotFound, "Failed to retrieve value")
		assert.Nil(t, retrievedValue, "Retrieved value should be nil")
	}
}

func (s *testSuite) TestSetGetDelete_RowColumns(t *testing.T) {
	rowKey := "rows_key"
	entries := make(map[string][]byte)
	for i := 0; i < 1000; i++ {
		key := gofakeit.UUID()
		value := gofakeit.LetterN(uint(1000))
		entries[key] = []byte(value)
	}

	rowKeys := [][]byte{[]byte(rowKey)}
	columEntriesPerRow := make([]map[string][]byte, 0)
	columEntriesPerRow = append(columEntriesPerRow, entries)

	t.Run("invalid-arguments", func(t *testing.T) {
		err := s.store.SetManyRowColumns(rowKeys, nil)
		assert.ErrorIs(t, err, kvdrivers.ErrInvalidArguments)
		err = s.store.DeleteManyRowColumns(rowKeys, nil)
		assert.ErrorIs(t, err, kvdrivers.ErrInvalidArguments)
	})

	t.Run("set", func(t *testing.T) {
		err := s.store.SetManyRowColumns(rowKeys, columEntriesPerRow)
		assert.NoError(t, err, "Failed to set row columns")
		_, err = s.store.Get([]byte(rowKey))
		assert.ErrorIs(t, err, kvdrivers.ErrUseGetColumnAPI)

		for key, entry := range entries {
			value, err := s.store.Get([]byte(rowKey + "::" + key))
			assert.ErrorIs(t, err, kvdrivers.ErrInvalidOpsForValueType)
			assert.Equal(t, value, entry)
		}

	})

	t.Run("get", func(t *testing.T) {
		fetchedEntries, err := s.store.GetRowColumns([]byte(rowKey), nil)
		assert.NoError(t, err, "Failed to fetch row columns")
		assert.Equal(t, entries, fetchedEntries)
	})

	// delete 10 Columns
	deleteColumnPerRow := make([]map[string][]byte, 0)
	deleteColumn := make(map[string][]byte)
	for i := 0; i < 100; i++ {
		deleteColumn[gofakeit.RandomMapKey(entries).(string)] = nil
	}
	deleteColumnPerRow = append(deleteColumnPerRow, deleteColumn)

	t.Run("delete_column", func(t *testing.T) {
		err := s.store.DeleteManyRowColumns(rowKeys, deleteColumnPerRow)
		assert.NoError(t, err, "Failed to delete row columns")

		retrievedEntries, err := s.store.GetRowColumns([]byte(rowKey), nil)
		assert.NoError(t, err, "Failed to fetch row columns")
		assert.Equal(t, len(deleteColumn), len(entries)-len(retrievedEntries))
	})

	t.Run("get_delete", func(t *testing.T) {
		_, err := s.store.Get([]byte(rowKey))
		assert.ErrorIs(t, err, kvdrivers.ErrUseGetColumnAPI)
		for key := range deleteColumn {
			value, err := s.store.Get([]byte(rowKey + "::" + key))
			assert.ErrorIs(t, err, kvdrivers.ErrKeyNotFound)
			assert.Nil(t, value, "Retrieved value should be nil")
		}
	})

	updateColumnPerRow := make([]map[string][]byte, 0)
	updateColumn := make(map[string][]byte)
	t.Run("put_more_columns", func(t *testing.T) {
		for i := 0; i < 10; i++ {
			key := gofakeit.UUID()
			value := gofakeit.LetterN(uint(1000))
			entries[key] = []byte(value)
			updateColumn[key] = []byte(value)
		}
		updateColumnPerRow = append(updateColumnPerRow, updateColumn)

		err := s.store.SetManyRowColumns(rowKeys, updateColumnPerRow)
		assert.NoError(t, err, "Failed to set row columns")
		_, err = s.store.Get([]byte(rowKey))
		assert.ErrorIs(t, err, kvdrivers.ErrUseGetColumnAPI)

		for key, entry := range entries {
			if _, ok := deleteColumn[key]; ok {
				continue
			}
			value, err := s.store.Get([]byte(rowKey + "::" + key))
			assert.ErrorIs(t, err, kvdrivers.ErrInvalidOpsForValueType)
			assert.Equal(t, value, entry)
		}
	})

	t.Run("delete_row", func(t *testing.T) {
		deleteRow, err := s.store.DeleteEntireRows(rowKeys)
		assert.NoError(t, err, "Failed to delete row")
		assert.Equal(t, deleteRow, len(entries)-len(deleteColumn), "total deleted Column should match")
	})
}

func (s *testSuite) TestSetGetDelete_RowColumns_Filter(t *testing.T) {
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
		err := s.store.SetManyRowColumns(rowKeys, filteredEntries)
		assert.NoError(t, err, "Failed to set row columns")
		err = s.store.SetManyRowColumns(rowKeys, columEntriesPerRow)
		assert.NoError(t, err, "Failed to set row columns")
	})

	t.Run("get", func(t *testing.T) {
		fetchedEntries, err := s.store.GetRowColumns([]byte(rowKey), func(i []byte) bool {
			return filterKeys[string(i)] != nil
		})
		assert.NoError(t, err, "Failed to fetch row columns")
		assert.Equal(t, filterKeys, fetchedEntries)
	})

	t.Run("delete_should_not_be_performed", func(t *testing.T) {
		err := s.store.Delete([]byte(rowKey))
		assert.NoError(t, err, "delete call failed.")
		_, err = s.store.Get([]byte(rowKey))
		assert.ErrorIs(t, err, kvdrivers.ErrUseGetColumnAPI)

		err = s.store.Delete([]byte(rowKey + "::"))
		assert.ErrorIs(t, err, kvdrivers.ErrInvalidOpsForValueType)
		for key := range entries {
			err := s.store.Delete([]byte(rowKey + "::" + key))
			assert.ErrorIs(t, err, kvdrivers.ErrInvalidOpsForValueType)
		}
	})

	t.Run("get_validate", func(t *testing.T) {
		fetchedEntries, err := s.store.GetRowColumns([]byte(rowKey), nil)
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

		err := s.store.DeleteManyRowColumns(rowKeys, []map[string][]byte{nonExistentColumns})
		assert.NoError(t, err)
	})
}

func (s *testSuite) TestSetGetDelete_NMRowColumns(t *testing.T) {
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
		err := s.store.SetManyRowColumns(rowKeys, columEntriesPerRow)
		assert.NoError(t, err, "Failed to set row columns")
		_, err = s.store.Get([]byte(rowKey1))
		assert.ErrorIs(t, err, kvdrivers.ErrUseGetColumnAPI)

		for key, entry := range columnsRow1 {
			value, err := s.store.Get([]byte(rowKey1 + "::" + key))
			assert.ErrorIs(t, err, kvdrivers.ErrInvalidOpsForValueType)
			assert.Equal(t, value, entry)
		}

		_, err = s.store.Get([]byte(rowKey2))
		assert.ErrorIs(t, err, kvdrivers.ErrUseGetColumnAPI)

		for key, entry := range columnsRow2 {
			value, err := s.store.Get([]byte(rowKey2 + "::" + key))
			assert.ErrorIs(t, err, kvdrivers.ErrInvalidOpsForValueType)
			assert.Equal(t, value, entry)
		}

	})

	t.Run("get", func(t *testing.T) {
		fe2, err := s.store.GetRowColumns([]byte(rowKey2), nil)
		assert.NoError(t, err, "Failed to fetch row columns")
		assert.Equal(t, columnsRow2, fe2)

		fe1, err := s.store.GetRowColumns([]byte(rowKey1), nil)
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
		err := s.store.DeleteManyRowColumns([][]byte{[]byte(rowKey1)}, deleteColumnPerRow)
		assert.NoError(t, err, "Failed to delete row columns")

		retrievedEntries, err := s.store.GetRowColumns([]byte(rowKey1), nil)
		assert.NoError(t, err, "Failed to fetch row columns")
		assert.Equal(t, len(deleteColumn), len(columnsRow1)-len(retrievedEntries))
	})

	t.Run("get_delete", func(t *testing.T) {
		_, err := s.store.Get([]byte(rowKey1))
		assert.ErrorIs(t, err, kvdrivers.ErrUseGetColumnAPI)
		for key := range deleteColumn {
			value, err := s.store.Get([]byte(rowKey1 + "::" + key))
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

		err := s.store.SetManyRowColumns([][]byte{[]byte(rowKey1)}, updateColumnPerRow)
		assert.NoError(t, err, "Failed to set row columns")
		_, err = s.store.Get([]byte(rowKey1))
		assert.ErrorIs(t, err, kvdrivers.ErrUseGetColumnAPI)

		for key, entry := range columnsRow1 {
			if _, ok := deleteColumn[key]; ok {
				continue
			}
			value, err := s.store.Get([]byte(rowKey1 + "::" + key))
			assert.ErrorIs(t, err, kvdrivers.ErrInvalidOpsForValueType)
			assert.Equal(t, value, entry)
		}
	})

	totalDeleted := len(columnsRow1) + len(columnsRow2) - len(deleteColumn)

	t.Run("delete_row", func(t *testing.T) {
		deleteRow, err := s.store.DeleteEntireRows(rowKeys)
		assert.NoError(t, err, "Failed to delete row")
		assert.Equal(t, deleteRow, totalDeleted, "total deleted Column should match")
	})

	t.Run("get_key_not_found", func(t *testing.T) {
		_, err := s.store.GetRowColumns([]byte(rowKey2), nil)
		assert.ErrorIs(t, err, kvdrivers.ErrKeyNotFound, "Failed to fetch row columns")

		_, err = s.store.GetRowColumns([]byte(rowKey1), nil)
		assert.ErrorIs(t, err, kvdrivers.ErrKeyNotFound, "Failed to fetch row columns")
	})
}

func (s *testSuite) TestTxnQueue_BatchPutGetDelete(t *testing.T) {
	key := []byte("test_key_txn")
	value := []byte("hello world_txn")
	txn := s.txnBatcherConstructor(10)
	keys := [][]byte{key}
	values := [][]byte{value}

	err := txn.BatchPut(keys, values)
	assert.NoError(t, err, "Failed to batch put")
	_, err = s.store.Get(key)
	assert.ErrorIs(t, err, kvdrivers.ErrKeyNotFound, "Failed to retrieve value")
	err = txn.Commit()
	assert.NoError(t, err, "Failed to commit")
	retrievedValue, err := s.store.Get(key)
	assert.NoError(t, err, "Failed to retrieve value")
	assert.Equal(t, value, retrievedValue, "retrieved value should be the same")
	err = txn.BatchDelete(keys)
	assert.NoError(t, err, "Failed to batch delete")
	retrievedValue, err = s.store.Get(key)
	assert.NoError(t, err, "Failed to retrieve value")
	assert.Equal(t, value, retrievedValue, "retrieved value should be the same")
	err = txn.Commit()
	assert.NoError(t, err, "Failed to commit")
	_, err = s.store.Get(key)
	assert.ErrorIs(t, err, kvdrivers.ErrKeyNotFound, "Failed to retrieve value")
}

func (s *testSuite) TestTxnSetGetAndDeleteMany(t *testing.T) {
	var keys [][]byte
	var values [][]byte
	for i := 0; i < 10; i++ {
		keys = append(keys, []byte(fmt.Sprintf("key_%d", i)))
		values = append(values, []byte(fmt.Sprintf("value_%d", i)))
	}

	txn := s.txnBatcherConstructor(10)

	err := txn.BatchPut(keys, values)
	assert.NoError(t, err, "Failed to insert values")
	assert.NoError(t, txn.Commit(), "Failed to commit")

	for i, key := range keys {
		retrievedValue, err := s.store.Get(key)
		assert.NoError(t, err, "Failed to retrieve value")
		assert.Equal(t, values[i], retrievedValue, "retrieved value should be the same")
	}

	err = txn.BatchDelete(keys)
	assert.NoError(t, err, "Failed to insert values")
	assert.NoError(t, txn.Commit(), "Failed to commit")
	assert.NoError(t, err, "Failed to delete keys")
	for _, key := range keys {
		retrievedValue, err := s.store.Get(key)
		assert.Empty(t, retrievedValue, "retrieved value should be empty")
		assert.ErrorIs(t, err, kvdrivers.ErrKeyNotFound, "error should be ErrKeyNotFound")
	}
}

func (s *testSuite) TestTxn_ChunkSetAndDelete(t *testing.T) {

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

	txn := s.txnBatcherConstructor(10)

	err := txn.SetChunks(key, chunks, checksum)
	assert.NoError(t, err, "Failed to insert chunked value")

	assert.NoError(t, txn.Commit(), "Failed to commit")
	retrievedValue, err := s.store.Get(key)
	assert.NoError(t, err, "Failed to retrieve chunked value")

	expectedValue := bytes.Join(chunks, nil)
	assert.Equal(t, expectedValue, retrievedValue, "Retrieved chunked value does not match")

	chunks = [][]byte{
		[]byte("chunk_1_"),
		[]byte("chunk_2_"),
	}

	checksum = 0
	for i := 0; i < len(chunks); i++ {
		checksum = crc32.Update(checksum, crc32.IEEETable, chunks[i])
	}

	err = txn.SetChunks(key, chunks, checksum)
	assert.NoError(t, err, "Failed to insert chunked value")
	assert.NoError(t, txn.Commit(), "Failed to commit")

	retrievedValue, err = s.store.Get(key)
	assert.NoError(t, err, "Failed to retrieve chunked value")

	expectedValue = bytes.Join(chunks, nil)
	assert.Equal(t, expectedValue, retrievedValue, "Retrieved chunked value does not match")

	keys := make(map[string]error)
	keys["chunked_key_chunk_0"] = kvdrivers.ErrInvalidOpsForValueType // as we are fetching the stored chunk value
	keys["chunked_key_chunk_1"] = kvdrivers.ErrInvalidOpsForValueType
	keys["chunked_key_chunk_2"] = kvdrivers.ErrKeyNotFound

	for key, e := range keys {
		_, err := s.store.Get([]byte(key))
		assert.ErrorIs(t, err, e, "error should be match")
	}

	delKeys := [][]byte{key}
	err = txn.BatchDelete(delKeys)
	assert.NoError(t, err, "Failed to delete key")
	assert.NoError(t, txn.Commit(), "Failed to commit")

	retrievedValue, err = s.store.Get(key)
	assert.ErrorIs(t, err, kvdrivers.ErrKeyNotFound, "error should be ErrKeyNotFound")
	assert.Nil(t, retrievedValue, "retrieved value should be nil")
	for key := range keys {
		_, err := s.store.Get([]byte(key))
		assert.ErrorIs(t, err, kvdrivers.ErrKeyNotFound, "error should be match")
	}
}

func (s *testSuite) TestTxn_SetGetDelete_RowColumns(t *testing.T) {
	rowKey := "rows_key_txn"
	entries := make(map[string][]byte)
	for i := 0; i < 100; i++ {
		key := gofakeit.UUID()
		value := gofakeit.LetterN(uint(1))
		entries[key] = []byte(value)
	}

	rowKeys := [][]byte{[]byte(rowKey)}
	columEntriesPerRow := make([]map[string][]byte, 0)
	columEntriesPerRow = append(columEntriesPerRow, entries)

	txn := s.txnBatcherConstructor(10)

	t.Run("invalid-arguments", func(t *testing.T) {
		err := txn.BatchPutRowColumns(rowKeys, nil)
		assert.ErrorIs(t, err, kvdrivers.ErrInvalidArguments)
		err = txn.BatchDeleteRowColumns(rowKeys, nil)
		assert.ErrorIs(t, err, kvdrivers.ErrInvalidArguments)
	})

	txn = s.txnBatcherConstructor(10)

	t.Run("set", func(t *testing.T) {
		err := txn.BatchPutRowColumns(rowKeys, columEntriesPerRow)
		assert.NoError(t, err, "Failed to set row columns")
		assert.NoError(t, txn.Commit(), "Failed to commit")
		_, err = s.store.Get([]byte(rowKey))
		assert.ErrorIs(t, err, kvdrivers.ErrUseGetColumnAPI)

		for key, entry := range entries {
			value, err := s.store.Get([]byte(rowKey + "::" + key))
			assert.ErrorIs(t, err, kvdrivers.ErrInvalidOpsForValueType)
			assert.Equal(t, value, entry)
		}

	})

	t.Run("get", func(t *testing.T) {
		fetchedEntries, err := s.store.GetRowColumns([]byte(rowKey), nil)
		assert.NoError(t, err, "Failed to fetch row columns")
		assert.Equal(t, entries, fetchedEntries)
	})

	// delete 10 Columns
	deleteColumnPerRow := make([]map[string][]byte, 0)
	deleteColumn := make(map[string][]byte)
	for i := 0; i < 10; i++ {
		deleteColumn[gofakeit.RandomMapKey(entries).(string)] = nil
	}
	deleteColumnPerRow = append(deleteColumnPerRow, deleteColumn)

	t.Run("delete_column", func(t *testing.T) {
		err := txn.BatchDeleteRowColumns(rowKeys, deleteColumnPerRow)
		assert.NoError(t, err, "Failed to delete row columns")
		assert.NoError(t, txn.Commit(), "Failed to commit")
		retrievedEntries, err := s.store.GetRowColumns([]byte(rowKey), nil)
		assert.NoError(t, err, "Failed to fetch row columns")
		assert.Equal(t, len(deleteColumn), len(entries)-len(retrievedEntries))
	})

	t.Run("get_delete", func(t *testing.T) {
		_, err := s.store.Get([]byte(rowKey))
		assert.ErrorIs(t, err, kvdrivers.ErrUseGetColumnAPI)
		for key := range deleteColumn {
			value, err := s.store.Get([]byte(rowKey + "::" + key))
			assert.ErrorIs(t, err, kvdrivers.ErrKeyNotFound)
			assert.Nil(t, value, "Retrieved value should be nil")
		}
	})

	updateColumnPerRow := make([]map[string][]byte, 0)
	updateColumn := make(map[string][]byte)
	t.Run("put_more_columns", func(t *testing.T) {
		for i := 0; i < 20; i++ {
			key := gofakeit.UUID()
			value := gofakeit.LetterN(uint(1000))
			entries[key] = []byte(value)
			updateColumn[key] = []byte(value)
		}
		updateColumnPerRow = append(updateColumnPerRow, updateColumn)

		err := txn.BatchPutRowColumns(rowKeys, updateColumnPerRow)
		assert.NoError(t, err, "Failed to set row columns")
		assert.NoError(t, txn.Commit(), "Failed to commit")
		_, err = s.store.Get([]byte(rowKey))
		assert.ErrorIs(t, err, kvdrivers.ErrUseGetColumnAPI)

		for key, entry := range entries {
			if _, ok := deleteColumn[key]; ok {
				continue
			}
			value, err := s.store.Get([]byte(rowKey + "::" + key))
			assert.ErrorIs(t, err, kvdrivers.ErrInvalidOpsForValueType)
			assert.Equal(t, value, entry)
		}
	})

	txn = s.txnBatcherConstructor(10)
	t.Run("delete_row", func(t *testing.T) {
		err := txn.BatchDeleteRows(rowKeys)
		assert.NoError(t, err, "Failed to delete row")
		assert.NoError(t, txn.Commit(), "Failed to commit")
		assert.Equal(t, int(txn.Stats().EntriesModified), len(entries)-len(deleteColumn)+1, "total deleted Column should match")
	})
}

func (s *testSuite) TestTxn_SetGetAndDeleteMany_Combined(t *testing.T) {
	key := []byte("chunked_key_to_be_deleted_txn")
	chunks := [][]byte{
		[]byte("chunk_1_"),
		[]byte("chunk_2_"),
		[]byte("chunk_3"),
	}

	var checksum uint32
	for i := 0; i < len(chunks); i++ {
		checksum = crc32.Update(checksum, crc32.IEEETable, chunks[i])
	}

	txn := s.txnBatcherConstructor(10)
	keys := make([][]byte, len(chunks))
	values := make([][]byte, len(chunks))
	for i := 0; i < len(chunks); i++ {
		keys[i] = []byte(gofakeit.UUID())
		values[i] = []byte(gofakeit.LetterN(uint(i + 1)))
	}

	assert.NoError(t, txn.BatchPut(keys, values), "Failed to set value")
	assert.NoError(t, txn.Commit(), "Failed to commit")
	assert.NoError(t, txn.SetChunks(key, chunks, checksum), "Failed to set chunks")
	assert.NoError(t, txn.Commit(), "Failed to commit")

	nonExistentKey := gofakeit.UUID()
	keysToBeDeleted := make([][]byte, 0)

	keysToBeDeleted = append(keysToBeDeleted, keys...)
	keysToBeDeleted = append(keysToBeDeleted, key)
	keysToBeDeleted = append(keysToBeDeleted, []byte(nonExistentKey))

	err := txn.BatchDelete(keysToBeDeleted)
	assert.NoError(t, err, "Failed to delete many keys")
	assert.NoError(t, txn.Commit(), "Failed to commit")
	for _, key := range keysToBeDeleted {
		retrievedValue, err := s.store.Get(key)
		assert.ErrorIs(t, err, kvdrivers.ErrKeyNotFound, "Failed to retrieve value")
		assert.Nil(t, retrievedValue, "Retrieved value should be nil")
	}
}

func (s *testSuite) TestTxn_SetGetDelete_NMRowColumns(t *testing.T) {
	rowKey1 := "rows_key_nm_1_txn"
	columnsRow1 := make(map[string][]byte)
	for i := 0; i < 5; i++ {
		key := gofakeit.UUID()
		value := gofakeit.LetterN(uint(1000))
		columnsRow1[key] = []byte(value)
	}

	rowKey2 := "rows_key_nm_2_txn"
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
		txn := s.txnBatcherConstructor(2)
		err := txn.BatchPutRowColumns(rowKeys, columEntriesPerRow)
		assert.NoError(t, err, "Failed to set row columns")
		_, err = s.store.Get([]byte(rowKey1))
		assert.ErrorIs(t, err, kvdrivers.ErrUseGetColumnAPI)

		for key, entry := range columnsRow1 {
			value, err := s.store.Get([]byte(rowKey1 + "::" + key))
			assert.ErrorIs(t, err, kvdrivers.ErrInvalidOpsForValueType)
			assert.Equal(t, value, entry)
		}

		_, err = s.store.Get([]byte(rowKey2))
		assert.ErrorIs(t, err, kvdrivers.ErrUseGetColumnAPI)

		for key, entry := range columnsRow2 {
			value, err := s.store.Get([]byte(rowKey2 + "::" + key))
			assert.ErrorIs(t, err, kvdrivers.ErrInvalidOpsForValueType)
			assert.Equal(t, value, entry)
		}

	})

	t.Run("get", func(t *testing.T) {
		fe2, err := s.store.GetRowColumns([]byte(rowKey2), nil)
		assert.NoError(t, err, "Failed to fetch row columns")
		assert.Equal(t, columnsRow2, fe2)

		fe1, err := s.store.GetRowColumns([]byte(rowKey1), nil)
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
		txn := s.txnBatcherConstructor(2)
		err := txn.BatchDeleteRowColumns([][]byte{[]byte(rowKey1)}, deleteColumnPerRow)
		assert.NoError(t, err, "Failed to delete row columns")
		assert.NoError(t, txn.Commit(), "Failed to commit")

		retrievedEntries, err := s.store.GetRowColumns([]byte(rowKey1), nil)
		assert.NoError(t, err, "Failed to fetch row columns")
		assert.Equal(t, len(deleteColumn), len(columnsRow1)-len(retrievedEntries))
	})

	t.Run("get_delete", func(t *testing.T) {
		_, err := s.store.Get([]byte(rowKey1))
		assert.ErrorIs(t, err, kvdrivers.ErrUseGetColumnAPI)
		for key := range deleteColumn {
			value, err := s.store.Get([]byte(rowKey1 + "::" + key))
			assert.ErrorIs(t, err, kvdrivers.ErrKeyNotFound)
			assert.Nil(t, value, "Retrieved value should be nil")
		}
	})

	txn := s.txnBatcherConstructor(1)
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

		err := txn.BatchPutRowColumns([][]byte{[]byte(rowKey1)}, updateColumnPerRow)
		assert.NoError(t, err, "Failed to set row columns")
		_, err = s.store.Get([]byte(rowKey1))
		assert.ErrorIs(t, err, kvdrivers.ErrUseGetColumnAPI)

		for key, entry := range columnsRow1 {
			if _, ok := deleteColumn[key]; ok {
				continue
			}
			value, err := s.store.Get([]byte(rowKey1 + "::" + key))
			assert.ErrorIs(t, err, kvdrivers.ErrInvalidOpsForValueType)
			assert.Equal(t, value, entry)
		}
	})

	totalDeleted := len(columnsRow1) + len(columnsRow2) - len(deleteColumn) + 2 // row keys

	txn = s.txnBatcherConstructor(1)
	t.Run("delete_row", func(t *testing.T) {
		err := txn.BatchDeleteRows(rowKeys)
		assert.NoError(t, err, "Failed to delete row")
		assert.Equal(t, int(txn.Stats().EntriesModified), totalDeleted, "total deleted Column should match")
	})

	t.Run("get_key_not_found", func(t *testing.T) {
		_, err := s.store.GetRowColumns([]byte(rowKey2), nil)
		assert.ErrorIs(t, err, kvdrivers.ErrKeyNotFound, "Failed to fetch row columns")

		_, err = s.store.GetRowColumns([]byte(rowKey1), nil)
		assert.ErrorIs(t, err, kvdrivers.ErrKeyNotFound, "Failed to fetch row columns")
	})
}
