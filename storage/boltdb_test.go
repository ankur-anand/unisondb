package storage

import (
	"bytes"
	"fmt"
	"hash/crc32"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"go.etcd.io/bbolt"
)

const testNamespace = "test_bucket"

func setupTestDB(t *testing.T) *boltdb {
	dir := t.TempDir()
	tempFile := filepath.Join(dir, "test.db")

	db, err := newBoltdb(tempFile, testNamespace)

	assert.NoError(t, err, "Failed to open BoltDB")

	err = db.db.Update(func(tx *bbolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists([]byte(testNamespace))
		return err
	})
	assert.NoError(t, err, "Failed to create test bucket")

	t.Cleanup(func() {
		db.Close()
	})

	return db
}

func TestInsertAndRetrieveFullValue(t *testing.T) {
	db := setupTestDB(t)

	key := []byte("test_key")
	value := []byte("hello world")
	compressed, err := CompressLZ4(value)
	assert.NoError(t, err)
	err = db.Set(key, compressed)
	assert.NoError(t, err, "Failed to insert full value")

	retrievedValue, err := db.Get(key)
	assert.NoError(t, err, "Failed to retrieve full value")
	assert.Equal(t, value, retrievedValue, "Retrieved value does not match")
}

func TestInsertAndRetrieveChunkedValue(t *testing.T) {
	db := setupTestDB(t)

	key := []byte("chunked_key")
	chunks := [][]byte{
		[]byte("chunk_1_"),
		[]byte("chunk_2_"),
		[]byte("chunk_3"),
	}

	compressed := make([][]byte, len(chunks))
	var checksum uint32
	for i := 0; i < len(chunks); i++ {
		var err error
		compressed[i], err = CompressLZ4(chunks[i])
		checksum = crc32.Update(checksum, crc32.IEEETable, chunks[i])
		assert.NoError(t, err, "Failed to compress chunk")
	}

	err := db.SetChunks(key, compressed, checksum)
	assert.NoError(t, err, "Failed to insert chunked value")

	retrievedValue, err := db.Get(key)
	assert.NoError(t, err, "Failed to retrieve chunked value")

	expectedValue := bytes.Join(chunks, nil)
	assert.Equal(t, expectedValue, retrievedValue, "Retrieved chunked value does not match")
}

// IMP: no panic
func TestCorruptChunkMetadata(t *testing.T) {
	db := setupTestDB(t)

	key := []byte("corrupt_metadata")

	err := db.db.Update(func(tx *bbolt.Tx) error {
		b := tx.Bucket([]byte(testNamespace))
		assert.NotNil(t, b, "bucket should exist")

		// an invalid metadata entry (less than 5 bytes)
		return b.Put(key, []byte{ChunkedValueFlag, 1})
	})
	assert.NoError(t, err, "Failed to insert corrupt metadata")

	_, err = db.Get(key)
	assert.Error(t, err, "Expected error for corrupt metadata")
	assert.Equal(t, "invalid chunk metadata", err.Error(), "Unexpected error message")
}

func TestDeleteChunks(t *testing.T) {
	db := setupTestDB(t)

	key := []byte("chunk_delete_key")
	chunks := [][]byte{
		[]byte("chunk1"),
		[]byte("chunk2"),
		[]byte("chunk3"),
	}

	checksum := crc32.ChecksumIEEE(bytes.Join(chunks, nil))

	err := db.SetChunks(key, chunks, checksum)
	assert.NoError(t, err)

	err = db.Delete(key)
	assert.NoError(t, err)

	for i := range chunks {
		chunkKey := []byte(fmt.Sprintf("%s_chunk_%d", key, i))
		chunkValue, err := db.Get(chunkKey)
		assert.Error(t, err)
		assert.Nil(t, chunkValue)
	}
}

func TestDeleteMany(t *testing.T) {
	db := setupTestDB(t)

	keys := [][]byte{
		[]byte("key1"),
		[]byte("key2"),
	}
	values := [][]byte{
		[]byte("value1"),
		[]byte("value2"),
	}

	err := db.SetMany(keys, values)
	assert.NoError(t, err)

	err = db.DeleteMany(keys)
	assert.NoError(t, err)

	for _, key := range keys {
		retrievedValue, err := db.Get(key)
		assert.Error(t, err)
		assert.Nil(t, retrievedValue)
	}
}

func TestDelete(t *testing.T) {
	db := setupTestDB(t)

	key := []byte("deleteKey")
	value := []byte("deleteValue")

	err := db.Set(key, value)
	assert.NoError(t, err)

	err = db.Delete(key)
	assert.NoError(t, err)

	retrievedValue, err := db.Get(key)
	assert.Error(t, err)
	assert.Nil(t, retrievedValue)
}
