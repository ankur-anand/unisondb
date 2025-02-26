package kv_test

import (
	"bytes"
	"fmt"
	"hash/crc32"
	"io"
	"path/filepath"
	"testing"

	"github.com/ankur-anand/kvalchemy/storage/kv"
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

// TestSuite defines all the test cases that is common in both the lmdb and boltdb.
type TestSuite struct {
	t             *testing.T
	dbConstructor func(config kv.Config) (bTreeStore, error)
	store         bTreeStore
}

func RunTestSuite(t *testing.T, name string, factory *TestSuite) {
	t.Run(name, func(t *testing.T) {
		t.Run("get_set", func(t *testing.T) {
			factory.TestSetAndGet()
		})

		t.Run("set_get_delete", func(t *testing.T) {
			factory.TestSetGetAndDelete()
		})

		t.Run("many_set_get_and_delete_many", func(t *testing.T) {
			factory.TestManySetGetAndDeleteMany()
		})

		t.Run("chunk_set_get_and_delete", func(t *testing.T) {
			factory.TestChunkSetAndDelete()
		})

		t.Run("snapshot_and_retrieve", func(t *testing.T) {
			factory.TestSnapshotAndRetrieve()
		})

		t.Run("empty_value_set", func(t *testing.T) {
			factory.TestEmptyValueSet()
		})

		t.Run("non_existent_value_delete", func(t *testing.T) {
			factory.TestNonExistentDelete()
		})

		t.Run("store_and_retrieve_metadata", func(t *testing.T) {
			factory.TestStoreMetadataAndRetrieveMetadata()
		})
	})
}

func (s *TestSuite) TestSetAndGet() {
	key := []byte("test_key")
	value := []byte("hello world")
	err := s.store.Set(key, value)
	assert.NoError(s.t, err, "Failed to insert full value")
	retrievedValue, err := s.store.Get(key)
	assert.NoError(s.t, err, "Failed to retrieve value")
	assert.Equal(s.t, value, retrievedValue, "retrieved value should be the same")
}

func (s *TestSuite) TestSetGetAndDelete() {
	key := []byte("test_key")
	value := []byte("hello world")
	err := s.store.Set(key, value)
	assert.NoError(s.t, err, "Failed to insert full value")
	retrievedValue, err := s.store.Get(key)
	assert.NoError(s.t, err, "Failed to retrieve value")
	assert.Equal(s.t, value, retrievedValue, "retrieved value should be the same")
	err = s.store.Delete(key)
	assert.NoError(s.t, err, "Failed to delete key")
	retrievedValue, err = s.store.Get(key)
	assert.ErrorIs(s.t, err, kv.ErrKeyNotFound, "error should be ErrKeyNotFound")
}

func (s *TestSuite) TestManySetGetAndDeleteMany() {
	var keys [][]byte
	var values [][]byte
	for i := 0; i < 10; i++ {
		keys = append(keys, []byte(fmt.Sprintf("key_%d", i)))
		values = append(values, []byte(fmt.Sprintf("value_%d", i)))
	}

	err := s.store.SetMany(keys, values)
	assert.NoError(s.t, err, "Failed to insert values")

	for i, key := range keys {
		retrievedValue, err := s.store.Get(key)
		assert.NoError(s.t, err, "Failed to retrieve value")
		assert.Equal(s.t, values[i], retrievedValue, "retrieved value should be the same")
	}

	err = s.store.DeleteMany(keys)
	assert.NoError(s.t, err, "Failed to delete keys")
	for _, key := range keys {
		retrievedValue, err := s.store.Get(key)
		assert.Empty(s.t, retrievedValue, "retrieved value should be empty")
		assert.ErrorIs(s.t, err, kv.ErrKeyNotFound, "error should be ErrKeyNotFound")
	}
}

func (s *TestSuite) TestChunkSetAndDelete() {

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
	assert.NoError(s.t, err, "Failed to insert chunked value")

	retrievedValue, err := s.store.Get(key)
	assert.NoError(s.t, err, "Failed to retrieve chunked value")

	expectedValue := bytes.Join(chunks, nil)
	assert.Equal(s.t, expectedValue, retrievedValue, "Retrieved chunked value does not match")

	chunks = [][]byte{
		[]byte("chunk_1_"),
		[]byte("chunk_2_"),
	}

	checksum = 0
	for i := 0; i < len(chunks); i++ {
		checksum = crc32.Update(checksum, crc32.IEEETable, chunks[i])
	}

	err = s.store.SetChunks(key, chunks, checksum)
	assert.NoError(s.t, err, "Failed to insert chunked value")

	retrievedValue, err = s.store.Get(key)
	assert.NoError(s.t, err, "Failed to retrieve chunked value")

	expectedValue = bytes.Join(chunks, nil)
	assert.Equal(s.t, expectedValue, retrievedValue, "Retrieved chunked value does not match")

	keys := make(map[string]error)
	keys["chunked_key_chunk_0"] = kv.ErrInvalidDataFormat // as we are fetching the stored chunk value
	keys["chunked_key_chunk_1"] = kv.ErrInvalidDataFormat
	keys["chunked_key_chunk_2"] = kv.ErrKeyNotFound

	for key, e := range keys {
		_, err := s.store.Get([]byte(key))
		assert.ErrorIs(s.t, err, e, "error should be match")
	}

	err = s.store.Delete(key)
	assert.NoError(s.t, err, "Failed to delete key")
	retrievedValue, err = s.store.Get(key)
	assert.ErrorIs(s.t, err, kv.ErrKeyNotFound, "error should be ErrKeyNotFound")
	for key := range keys {
		_, err := s.store.Get([]byte(key))
		assert.ErrorIs(s.t, err, kv.ErrKeyNotFound, "error should be match")
	}
}

func (s *TestSuite) TestSnapshotAndRetrieve() {
	testData := make(map[string][]byte)
	for i := 0; i < 100; i++ {
		key := gofakeit.LetterN(uint(i + 1))
		value := gofakeit.LetterN(uint(i + 100))
		testData[key] = []byte(value)
	}

	for key := range testData {
		err := s.store.Set([]byte(key), testData[key])
		assert.NoError(s.t, err, "Failed to insert value")
	}

	buf := new(bytes.Buffer)
	err := s.store.Snapshot(buf)
	assert.NoError(s.t, err, "Failed to snapshot value")

	restoreDir := s.t.TempDir()
	path := filepath.Join(restoreDir, "snapshot")
	conf := kv.Config{
		Path:      path,
		Namespace: "test",
		NoSync:    true,
		MmapSize:  1 << 30,
	}
	restoreDB, err := s.dbConstructor(conf)
	assert.NoError(s.t, err, "Failed to create db constructor")
	err = restoreDB.Restore(bytes.NewReader(buf.Bytes()))
	assert.NoError(s.t, err, "Failed to restore")
	for key := range testData {
		retrievedValue, err := s.store.Get([]byte(key))
		assert.NoError(s.t, err, "Failed to get value")
		assert.Equal(s.t, testData[key], retrievedValue, "Retrieved value does not match")
	}
}

func (s *TestSuite) TestEmptyValueSet() {
	var keys [][]byte
	for i := 0; i < 100; i++ {
		key := gofakeit.LetterN(uint(i + 1))
		keys = append(keys, []byte(key))
		err := s.store.Set([]byte(key), nil)
		assert.NoError(s.t, err, "Failed to insert value")
	}

	for _, key := range keys {
		retrievedValue, err := s.store.Get(key)
		assert.NoError(s.t, err, "Failed to retrieve value")
		assert.Equal(s.t, []byte(""), retrievedValue, "Retrieved value does not match")
	}
}

func (s *TestSuite) TestNonExistentDelete() {
	var keys [][]byte
	for i := 0; i < 100; i++ {
		key := gofakeit.LetterN(uint(i + 1))
		keys = append(keys, []byte(key))
		err := s.store.Delete([]byte(key))
		assert.NoError(s.t, err, "Failed to delete value")
	}

	err := s.store.DeleteMany(keys)
	assert.NoError(s.t, err, "Failed to delete")
}

func (s *TestSuite) TestStoreMetadataAndRetrieveMetadata() {
	metadata := gofakeit.UUID()
	key := []byte("metadata")
	err := s.store.StoreMetadata(key, []byte(metadata))
	assert.NoError(s.t, err, "Failed to store metadata")
	retrievedValue, err := s.store.RetrieveMetadata(key)
	assert.NoError(s.t, err, "Failed to retrieve metadata")
	assert.Equal(s.t, metadata, string(retrievedValue), "Retrieved metadata does not match")
}
