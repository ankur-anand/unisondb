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

// boltdb embed an initialized bolt db and implements PersistenceWriter and PersistenceReader.
type boltdb struct {
	db        *bbolt.DB
	namespace []byte
}

func newBoltdb(path string) (*boltdb, error) {
	db, err := bbolt.Open(path, 0600, nil)
	if err != nil {
		return nil, err
	}
	return &boltdb{db: db}, nil
}

func (b *boltdb) Close() error {
	return b.db.Close()
}

// Set associates a value with a key within a specific namespace.
func (b *boltdb) Set(namespace string, key []byte, value []byte) error {
	return b.db.Update(func(tx *bbolt.Tx) error {
		b := tx.Bucket([]byte(namespace))
		if b == nil {
			return ErrBucketNotFound
		}
		// indicate this is a full value, not chunked
		storedValue := append([]byte{FullValueFlag}, value...)

		return b.Put(key, storedValue)
	})
}

// SetMany associates multiple values with corresponding keys within a namespace.
func (b *boltdb) SetMany(namespace string, keys [][]byte, value [][]byte) error {
	return b.db.Update(func(tx *bbolt.Tx) error {
		b := tx.Bucket([]byte(namespace))
		if b == nil {
			return ErrBucketNotFound
		}
		for i, key := range keys {
			// indicate this is a full value, not chunked
			storedValue := append([]byte{FullValueFlag}, value[i]...)

			err := b.Put(key, storedValue)
			if err != nil {
				return err
			}
		}
		return nil
	})
}

// SetChunks stores a value that has been split into chunks, associating them with a single key.
func (b *boltdb) SetChunks(namespace string, key []byte, chunks [][]byte, checksum uint32) error {
	return b.db.Update(func(tx *bbolt.Tx) error {
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

// Delete deletes a value with a key within a specific namespace.
func (b *boltdb) Delete(namespace string, key []byte) error {
	return b.db.Update(func(tx *bbolt.Tx) error {
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

			return b.Delete(key)

		case ChunkedValueFlag:
			if len(storedValue) < 9 {
				return ErrInvalidChunkMetadata
			}

			chunkCount := binary.LittleEndian.Uint32(storedValue[1:5])

			for i := 0; i < int(chunkCount); i++ {
				chunkKey := fmt.Sprintf("%s_chunk_%d", key, i)
				if err := b.Delete([]byte(chunkKey)); err != nil {
					return err
				}
			}

			return b.Delete(key)
		}

		return ErrInvalidDataFormat
	})
}

// DeleteMany delete multiple values with corresponding keys within a namespace.
func (b *boltdb) DeleteMany(namespace string, keys [][]byte) error {
	return b.db.Update(func(tx *bbolt.Tx) error {
		b := tx.Bucket([]byte(namespace))
		if b == nil {
			return ErrBucketNotFound
		}
		for _, key := range keys {
			storedValue := b.Get(key)
			if storedValue == nil {
				return ErrKeyNotFound
			}

			flag := storedValue[0]
			switch flag {
			case FullValueFlag:

				return b.Delete(key)

			case ChunkedValueFlag:
				if len(storedValue) < 9 {
					return ErrInvalidChunkMetadata
				}

				chunkCount := binary.LittleEndian.Uint32(storedValue[1:5])

				for i := 0; i < int(chunkCount); i++ {
					chunkKey := fmt.Sprintf("%s_chunk_%d", key, i)
					if err := b.Delete([]byte(chunkKey)); err != nil {
						return err
					}
				}

				return b.Delete(key)
			}
		}
		return nil
	})
}

// Get retrieves a value associated with a key within a specific namespace.
func (b *boltdb) Get(namespace string, key []byte) ([]byte, error) {
	var value []byte

	err := b.db.View(func(tx *bbolt.Tx) error {
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

func (b *boltdb) StoreMetadata(key []byte, value []byte) error {
	return b.db.Update(func(tx *bbolt.Tx) error {
		b := tx.Bucket([]byte(sysBucketMetaData))
		if b == nil {
			return ErrBucketNotFound
		}
		return b.Put(key, value)
	})
}

func (b *boltdb) RetrieveMetadata(key []byte) ([]byte, error) {
	var value []byte
	err := b.db.View(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket([]byte(sysBucketMetaData))
		if bucket == nil {
			return ErrBucketNotFound
		}
		data := bucket.Get(key)
		if data == nil {
			return ErrKeyNotFound
		}
		value = make([]byte, len(data))
		copy(value, data)
		return nil
	})
	return value, err
}
