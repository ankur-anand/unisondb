package storage

import (
	"bytes"
	"encoding/binary"
	"fmt"

	"go.etcd.io/bbolt"
)

const (
	FullValueFlag    byte = 0
	ChunkedValueFlag byte = 1
)

// insertIntoBoltDB stores a complete key-value pair in BoltDB.
func insertIntoBoltDB(namespace string, db *bbolt.DB, key, value []byte) error {
	return db.Update(func(tx *bbolt.Tx) error {
		b := tx.Bucket([]byte(namespace))
		if b == nil {
			return ErrBucketNotFound
		}
		// Prepend FullValueFlag to indicate this is a full value, not chunked
		storedValue := append([]byte{FullValueFlag}, value...)

		// Store in BoltDB
		return b.Put(key, storedValue)
	})
}

// insertChunkIntoBoltDB stores a chunked key-value pair in BoltDB.
func insertChunkIntoBoltDB(namespace string, db *bbolt.DB, key []byte, chunks [][]byte) error {
	return db.Update(func(tx *bbolt.Tx) error {
		b := tx.Bucket([]byte(namespace))
		if b == nil {
			return ErrBucketNotFound
		}

		chunkCount := uint32(len(chunks))
		metaData := make([]byte, 5) // 1 byte for flag + 4 bytes for chunk count
		metaData[0] = ChunkedValueFlag
		binary.LittleEndian.PutUint32(metaData[1:], chunkCount)

		// chunk metadata
		if err := b.Put(key, metaData); err != nil {
			return err
		}

		// individual chunk
		for i, chunk := range chunks {
			chunkKey := fmt.Sprintf("%s_chunk_%d", key, i)
			if err := b.Put([]byte(chunkKey), chunk); err != nil {
				return err
			}
		}

		return nil
	})
}

// retrieveFromBoltDB retrieves either a full value or a chunked value from BoltDB.
func retrieveFromBoltDB(namespace string, db *bbolt.DB, key []byte) ([]byte, error) {
	var value []byte

	err := db.View(func(tx *bbolt.Tx) error {
		b := tx.Bucket([]byte(namespace))
		if b == nil {
			return ErrBucketNotFound
		}

		storedValue := b.Get(key)
		if storedValue == nil {
			return fmt.Errorf("key not found")
		}

		flag := storedValue[0]
		if flag == FullValueFlag {

			value = make([]byte, len(storedValue)-1)
			copy(value, storedValue[1:])
			return nil
		} else if flag == ChunkedValueFlag {

			if len(storedValue) < 5 {
				return fmt.Errorf("invalid chunk metadata")
			}

			chunkCount := binary.LittleEndian.Uint32(storedValue[1:])
			fullValue := new(bytes.Buffer)

			for i := 0; i < int(chunkCount); i++ {
				chunkKey := fmt.Sprintf("%s_chunk_%d", key, i)
				chunkData := b.Get([]byte(chunkKey))
				if chunkData == nil {
					return fmt.Errorf("chunk %d missing", i)
				}
				fullValue.Write(chunkData)
			}

			value = make([]byte, fullValue.Len())
			copy(value, fullValue.Bytes())
			return nil
		}

		return fmt.Errorf("unknown data format")
	})

	return value, err
}
