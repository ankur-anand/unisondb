package storage

import (
	"bytes"
	"hash/crc32"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"go.etcd.io/bbolt"
)

const testNamespace = "test_bucket"

func setupTestDB(t *testing.T) (*bbolt.DB, func()) {
	dir := t.TempDir()
	tempFile := filepath.Join(dir, "test.db")
	db, err := bbolt.Open(tempFile, 0600, nil)
	assert.NoError(t, err, "Failed to open BoltDB")

	err = db.Update(func(tx *bbolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists([]byte(testNamespace))
		return err
	})
	assert.NoError(t, err, "Failed to create test bucket")

	return db, func() {
		db.Close()
	}
}

func TestInsertAndRetrieveFullValue(t *testing.T) {
	db, cleanup := setupTestDB(t)
	defer cleanup()

	key := []byte("test_key")
	value := []byte("hello world")
	compressed, err := CompressLZ4(value)
	assert.NoError(t, err)
	err = insertIntoBoltDB(testNamespace, db, key, compressed)
	assert.NoError(t, err, "Failed to insert full value")

	retrievedValue, err := retrieveFromBoltDB(testNamespace, db, key)
	assert.NoError(t, err, "Failed to retrieve full value")
	assert.Equal(t, value, retrievedValue, "Retrieved value does not match")
}

func TestInsertAndRetrieveChunkedValue(t *testing.T) {
	db, cleanup := setupTestDB(t)
	defer cleanup()

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

	err := insertChunkIntoBoltDB(testNamespace, db, key, compressed, checksum)
	assert.NoError(t, err, "Failed to insert chunked value")

	retrievedValue, err := retrieveFromBoltDB(testNamespace, db, key)
	assert.NoError(t, err, "Failed to retrieve chunked value")

	expectedValue := bytes.Join(chunks, nil)
	assert.Equal(t, expectedValue, retrievedValue, "Retrieved chunked value does not match")
}

// IMP: no panic
func TestCorruptChunkMetadata(t *testing.T) {
	db, cleanup := setupTestDB(t)
	defer cleanup()

	key := []byte("corrupt_metadata")

	err := db.Update(func(tx *bbolt.Tx) error {
		b := tx.Bucket([]byte(testNamespace))
		assert.NotNil(t, b, "bucket should exist")

		// an invalid metadata entry (less than 5 bytes)
		return b.Put(key, []byte{ChunkedValueFlag, 1})
	})
	assert.NoError(t, err, "Failed to insert corrupt metadata")

	_, err = retrieveFromBoltDB(testNamespace, db, key)
	assert.Error(t, err, "Expected error for corrupt metadata")
	assert.Equal(t, "invalid chunk metadata", err.Error(), "Unexpected error message")
}
