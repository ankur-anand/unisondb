package storage

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"

	"go.etcd.io/bbolt"
)

const (
	FullValueFlag    byte = 0
	ChunkedValueFlag byte = 1
)

var (
	ErrInvalidChunkMetadata = errors.New("invalid chunk metadata")
	ErrInvalidDataFormat    = errors.New("invalid data format")
)

// insertIntoBoltDB stores a complete key-value pair in BoltDB.
func insertIntoBoltDB(namespace string, db *bbolt.DB, key, value []byte) error {
	return db.Update(func(tx *bbolt.Tx) error {
		b := tx.Bucket([]byte(namespace))
		if b == nil {
			return ErrBucketNotFound
		}
		// indicate this is a full value, not chunked
		storedValue := append([]byte{FullValueFlag}, value...)

		return b.Put(key, storedValue)
	})
}

// insertChunkIntoBoltDB stores a chunked key-value pair in BoltDB.
func insertChunkIntoBoltDB(namespace string, db *bbolt.DB, key []byte, chunks [][]byte, checksum uint32) error {

	return db.Update(func(tx *bbolt.Tx) error {
		b := tx.Bucket([]byte(namespace))
		if b == nil {
			return ErrBucketNotFound
		}

		chunkCount := uint32(len(chunks))
		// Metadata: 1 byte flag + 4 bytes chunk count + 4 bytes checksum
		metaData := make([]byte, 9)
		metaData[0] = ChunkedValueFlag
		binary.LittleEndian.PutUint32(metaData[1:], chunkCount)
		binary.LittleEndian.PutUint32(metaData[5:], checksum)

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
			return ErrKeyNotFound
		}

		flag := storedValue[0]
		switch flag {
		case FullValueFlag:

			decompressed, err := DecompressLZ4(storedValue[1:])
			if err != nil {
				return err
			}
			value = make([]byte, len(decompressed))
			copy(value, decompressed)
			return nil

		case ChunkedValueFlag:
			if len(storedValue) < 9 {
				return ErrInvalidChunkMetadata
			}

			chunkCount := binary.LittleEndian.Uint32(storedValue[1:5])
			storedChecksum := binary.LittleEndian.Uint32(storedValue[5:9])
			var calculatedChecksum uint32

			fullValue := new(bytes.Buffer)

			for i := 0; i < int(chunkCount); i++ {
				chunkKey := fmt.Sprintf("%s_chunk_%d", key, i)

				chunkData := b.Get([]byte(chunkKey))
				if chunkData == nil {
					return fmt.Errorf("chunk %d missing", i)
				}
				decompressed, err := DecompressLZ4(chunkData)
				if err != nil {
					return err
				}
				calculatedChecksum = crc32.Update(calculatedChecksum, crc32.IEEETable, decompressed)
				fullValue.Write(decompressed)
			}
			
			if calculatedChecksum != storedChecksum {
				return ErrRecordCorrupted
			}

			value = make([]byte, fullValue.Len())
			copy(value, fullValue.Bytes())
			return nil
		}

		return ErrInvalidDataFormat
	})

	return value, err
}
