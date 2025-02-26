package kv

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

func setupTestDB(t *testing.T) *BoltDBEmbed {
	dir := t.TempDir()
	tempFile := filepath.Join(dir, "test.db")

	db, err := NewBoltdb(Config{
		Path:      tempFile,
		Namespace: testNamespace,
		NoSync:    false,
	})

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

func TestBoltDBInsertAndRetrieveFullValue(t *testing.T) {
	db := setupTestDB(t)

	key := []byte("test_key")
	value := []byte("hello world")
	err := db.Set(key, value)
	assert.NoError(t, err, "Failed to insert full value")

	retrievedValue, err := db.Get(key)
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

	err := db.SetChunks(key, chunks, checksum)
	assert.NoError(t, err, "Failed to insert chunked value")

	retrievedValue, err := db.Get(key)
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

	err = db.SetChunks(key, chunks, checksum)
	assert.NoError(t, err, "Failed to insert chunked value")

	retrievedValue, err = db.Get(key)
	assert.NoError(t, err, "Failed to retrieve chunked value")

	expectedValue = bytes.Join(chunks, nil)
	assert.Equal(t, expectedValue, retrievedValue, "Retrieved chunked value does not match")

	keys := make(map[string]bool)
	keys["chunked_key_chunk_0"] = true
	keys["chunked_key_chunk_1"] = true
	keys["chunked_key_chunk_2"] = false

	for k, v := range keys {
		err := db.db.View(func(tx *bbolt.Tx) error {
			bucket := tx.Bucket([]byte(testNamespace))
			assert.NotNil(t, bucket, "bucket should not be nil")
			val := bucket.Get([]byte(k))
			if v {
				assert.NotNil(t, val, "val should not be nil")
			}

			if !v {
				assert.Nil(t, val, "val should not be nil")
			}
			return nil
		})

		assert.NoError(t, err, "Failed to retrieve chunked value")
	}
}

// IMP: no panic
func TestBoltDBCorruptChunkMetadata(t *testing.T) {
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

func TestBoltDBDeleteChunks(t *testing.T) {
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

func TestBoltdbDelete(t *testing.T) {
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
