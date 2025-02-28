package kvdb_test

import (
	"bytes"
	"fmt"
	"hash/crc32"
	"io"
	"path/filepath"
	"testing"

	"github.com/ankur-anand/kvalchemy/dbengine/kvdb"
	"github.com/brianvoe/gofakeit/v7"
	"github.com/stretchr/testify/assert"
)

// btreeWriter defines the interface for interacting with a B-tree based storage
// for setting individual values, chunks and many value at once.
type btreeWriter interface {
	// Set associates a value with a key.
	Set(key []byte, value []byte) error
	// SetMany associates multiple values with corresponding keys.
	SetMany(keys [][]byte, values [][]byte) error
	// SetChunks stores a value that has been split into chunks, associating them with a single key.
	SetChunks(key []byte, chunks [][]byte, checksum uint32) error
	// Delete deletes a value with a key.
	Delete(key []byte) error
	// DeleteMany delete multiple values with corresponding keys.
	DeleteMany(keys [][]byte) error

	StoreMetadata(key []byte, value []byte) error
	FSync() error
	Restore(reader io.Reader) error
}

// btreeReader defines the interface for interacting with a B-tree based storage
// for getting individual values, chunks and many value at once.
type btreeReader interface {
	// Get retrieves a value associated with a key.
	Get(key []byte) ([]byte, error)
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
	dbConstructor func(config kvdb.Config) (bTreeStore, error)
	store         bTreeStore
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
	assert.ErrorIs(t, err, kvdb.ErrKeyNotFound, "error should be ErrKeyNotFound")
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
		assert.ErrorIs(t, err, kvdb.ErrKeyNotFound, "error should be ErrKeyNotFound")
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
	keys["chunked_key_chunk_0"] = kvdb.ErrInvalidDataFormat // as we are fetching the stored chunk value
	keys["chunked_key_chunk_1"] = kvdb.ErrInvalidDataFormat
	keys["chunked_key_chunk_2"] = kvdb.ErrKeyNotFound

	for key, e := range keys {
		_, err := s.store.Get([]byte(key))
		assert.ErrorIs(t, err, e, "error should be match")
	}

	err = s.store.Delete(key)
	assert.NoError(t, err, "Failed to delete key")
	retrievedValue, err = s.store.Get(key)
	assert.ErrorIs(t, err, kvdb.ErrKeyNotFound, "error should be ErrKeyNotFound")
	assert.Nil(t, retrievedValue, "retrieved value should be nil")
	for key := range keys {
		_, err := s.store.Get([]byte(key))
		assert.ErrorIs(t, err, kvdb.ErrKeyNotFound, "error should be match")
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
	conf := kvdb.Config{
		Path:      path,
		Namespace: "test",
		NoSync:    true,
		MmapSize:  1 << 30,
	}
	restoreDB, err := s.dbConstructor(conf)
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
	assert.ErrorIs(t, err, kvdb.ErrKeyNotFound)
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
		assert.ErrorIs(t, err, kvdb.ErrKeyNotFound, "Failed to retrieve value")
		assert.Nil(t, retrievedValue, "Retrieved value should be nil")
	}
}
