package kvdrivers

import (
	"bytes"
	"hash/crc32"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.etcd.io/bbolt"
)

const testNamespace = "test_bucket"

func setupTestDB(t *testing.T) *BoltDBEmbed {
	dir := t.TempDir()
	tempFile := filepath.Join(dir, "test.db")

	db, err := NewBoltdb(tempFile, Config{
		Namespace: testNamespace,
		NoSync:    false,
	})

	require.NoError(t, err, "Failed to open BoltDB")

	err = db.db.Update(func(tx *bbolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists([]byte(testNamespace))
		return err
	})

	require.NoError(t, err, "Failed to create test bucket")

	t.Cleanup(func() {
		db.Close()
	})

	return db
}

func TestBoltDBInsertAndRetrieveFullValue(t *testing.T) {
	db := setupTestDB(t)

	key := []byte("test_key")
	value := []byte("hello world")
	err := db.SetKV(key, value)
	assert.NoError(t, err, "Failed to insert full value")

	retrievedValue, err := db.GetKV(key)
	assert.NoError(t, err, "Failed to retrieve full value")
	assert.Equal(t, value, retrievedValue, "Retrieved value does not match")
}

func TestBoltDBInsertAndRetrieveChunkedValue(t *testing.T) {
	db := setupTestDB(t)

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

	err := db.SetLobChunks(key, chunks, checksum)
	assert.NoError(t, err, "Failed to insert chunked value")

	_, err = db.GetKV(key)
	assert.Error(t, ErrKeyNotFound, "GetKV should not return ChunkedKV")

	expectedValue, err := db.GetLOBChunks(key)
	assert.NoError(t, err, "Failed to retrieve chunked value")
	assert.Equal(t, chunks, expectedValue, "Retrieved chunked value does not match")

	chunks = [][]byte{
		[]byte("chunk_1_"),
		[]byte("chunk_2_"),
	}

	checksum = 0
	for i := 0; i < len(chunks); i++ {
		checksum = crc32.Update(checksum, crc32.IEEETable, chunks[i])
	}

	err = db.SetLobChunks(key, chunks, checksum)
	assert.NoError(t, err, "Failed to insert chunked value")

	_, err = db.GetKV(key)
	assert.Error(t, ErrKeyNotFound, "GetKV should not return ChunkedKV")

	expectedValue, err = db.GetLOBChunks(key)
	assert.NoError(t, err, "Failed to retrieve chunked value")
	assert.Equal(t, chunks, expectedValue, "Retrieved chunked value does not match")

}

// IMP: no panic
func TestBoltDBCorruptChunkMetadata(t *testing.T) {
	db := setupTestDB(t)

	key := []byte("corrupt_metadata")

	typedKey := KeyBlobChunk(key, 0)
	err := db.db.Update(func(tx *bbolt.Tx) error {
		b := tx.Bucket([]byte(testNamespace))
		assert.NotNil(t, b, "bucket should exist")

		// an invalid metadata entry (less than 5 bytes)
		return b.Put(typedKey, []byte{1})
	})
	assert.NoError(t, err, "Failed to insert corrupt metadata")

	_, err = db.GetLOBChunks(key)
	assert.Error(t, err, "Expected error for corrupt metadata")
	assert.Equal(t, "invalid chunk metadata", err.Error(), "Unexpected error message")
}

func TestBoltDBDeleteChunks(t *testing.T) {
	db := setupTestDB(t)

	key := []byte("chunk_delete_key")
	chunks := [][]byte{
		[]byte("chunk1"),
		[]byte("chunk2"),
		[]byte("chunk3"),
	}

	checksum := crc32.ChecksumIEEE(bytes.Join(chunks, nil))

	err := db.SetLobChunks(key, chunks, checksum)
	assert.NoError(t, err)

	err = db.DeleteKV(key)
	assert.NoError(t, err)

	for i := range chunks {
		chunkKey := KeyBlobChunk(key, i+1)
		chunkValue, err := db.GetLOBChunks(chunkKey)
		assert.Error(t, err)
		assert.Nil(t, chunkValue)
	}
}

func TestBoltDBDeleteMany(t *testing.T) {
	db := setupTestDB(t)

	keys := [][]byte{
		[]byte("key1"),
		[]byte("key2"),
	}
	values := [][]byte{
		[]byte("value1"),
		[]byte("value2"),
	}

	err := db.BatchSetKV(keys, values)
	assert.NoError(t, err)

	err = db.BatchDeleteKV(keys)
	assert.NoError(t, err)

	for _, key := range keys {
		retrievedValue, err := db.GetKV(key)
		assert.Error(t, err)
		assert.Nil(t, retrievedValue)
	}
}

func TestBoltdbDelete(t *testing.T) {
	db := setupTestDB(t)

	key := []byte("deleteKey")
	value := []byte("deleteValue")

	err := db.SetKV(key, value)
	assert.NoError(t, err)

	err = db.DeleteKV(key)
	assert.NoError(t, err)

	retrievedValue, err := db.GetKV(key)
	assert.Error(t, err)
	assert.Nil(t, retrievedValue)
}
