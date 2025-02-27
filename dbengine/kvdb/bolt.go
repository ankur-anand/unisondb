package kvdb

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"time"

	"github.com/hashicorp/go-metrics"
	"go.etcd.io/bbolt"
)

// BoltDBEmbed embed an initialized bolt db and implements PersistenceWriter and PersistenceReader.
type BoltDBEmbed struct {
	db        *bbolt.DB
	namespace []byte
	label     []metrics.Label
	conf      Config
}

func NewBoltdb(conf Config) (*BoltDBEmbed, error) {
	db, err := bbolt.Open(conf.Path, 0600, nil)
	if err != nil {
		return nil, err
	}
	l := []metrics.Label{{Name: "namespace", Value: conf.Namespace}}
	err = db.Update(func(tx *bbolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists([]byte(conf.Namespace))
		if err != nil {
			return err
		}
		_, err = tx.CreateBucketIfNotExists([]byte(sysBucketMetaData))
		return err
	})
	return &BoltDBEmbed{db: db, namespace: []byte(conf.Namespace), label: l, conf: conf}, err
}

func (b *BoltDBEmbed) FSync() error {
	return b.db.Sync()
}

func (b *BoltDBEmbed) Close() error {
	return b.db.Close()
}

// Set associates a value with a key within a specific namespace.
func (b *BoltDBEmbed) Set(key []byte, value []byte) error {
	metrics.IncrCounterWithLabels([]string{"kvalchemy", "storage", "boltdb", "set", "total"}, 1, b.label)
	startTime := time.Now()
	defer func() {
		metrics.MeasureSinceWithLabels([]string{"kvalchemy", "storage", "boltdb", "set", "latency", "msec"}, startTime, b.label)
	}()

	return b.db.Update(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket(b.namespace)
		if bucket == nil {
			return ErrBucketNotFound
		}
		// indicate this is a full value, not chunked
		storedValue := append([]byte{FullValueFlag}, value...)

		return bucket.Put(key, storedValue)
	})
}

// SetMany associates multiple values with corresponding keys within a namespace.
func (b *BoltDBEmbed) SetMany(keys [][]byte, value [][]byte) error {
	metrics.IncrCounterWithLabels([]string{"kvalchemy", "storage", "boltdb", "set", "many", "total"}, 1, b.label)
	startTime := time.Now()
	defer func() {
		metrics.MeasureSinceWithLabels([]string{"kvalchemy", "storage", "boltdb", "set", "many", "latency", "msec"}, startTime, b.label)
	}()

	return b.db.Update(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket(b.namespace)
		if bucket == nil {
			return ErrBucketNotFound
		}
		for i, key := range keys {
			// indicate this is a full value, not chunked
			storedValue := append([]byte{FullValueFlag}, value[i]...)

			err := bucket.Put(key, storedValue)
			if err != nil {
				return err
			}
		}
		return nil
	})
}

// SetChunks stores a value that has been split into chunks, associating them with a single key.
func (b *BoltDBEmbed) SetChunks(key []byte, chunks [][]byte, checksum uint32) error {
	metrics.IncrCounterWithLabels([]string{"kvalchemy", "storage", "boltdb", "set", "chunks", "total"}, 1, b.label)
	startTime := time.Now()
	defer func() {
		metrics.MeasureSinceWithLabels([]string{"kvalchemy", "storage", "boltdb", "set", "chunks", "latency", "msec"}, startTime, b.label)
	}()
	return b.db.Update(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket(b.namespace)
		if bucket == nil {
			return ErrBucketNotFound
		}

		// get last stored for keys, if present.
		// older chunk needs to deleted for not leaking the space.
		storedValue := bucket.Get(key)
		if storedValue != nil && storedValue[0] == ChunkedValueFlag {
			if len(storedValue) < 9 {
				return ErrInvalidChunkMetadata
			}
			chunkCount := binary.LittleEndian.Uint32(storedValue[1:5])

			for i := 0; i < int(chunkCount); i++ {
				chunkKey := fmt.Sprintf("%s_chunk_%d", key, i)
				if err := bucket.Delete([]byte(chunkKey)); err != nil {
					return err
				}
			}
		}

		chunkCount := uint32(len(chunks))
		// Metadata: 1 byte flag + 4 bytes chunk count + 4 bytes checksum
		metaData := make([]byte, 9)
		metaData[0] = ChunkedValueFlag
		binary.LittleEndian.PutUint32(metaData[1:], chunkCount)
		binary.LittleEndian.PutUint32(metaData[5:], checksum)

		// chunk metadata
		if err := bucket.Put(key, metaData); err != nil {
			return err
		}

		// individual chunk
		for i, chunk := range chunks {
			chunkKey := fmt.Sprintf("%s_chunk_%d", key, i)
			if err := bucket.Put([]byte(chunkKey), chunk); err != nil {
				return err
			}
		}

		return nil
	})
}

// Delete deletes a value with a key within a specific namespace.
func (b *BoltDBEmbed) Delete(key []byte) error {
	metrics.IncrCounterWithLabels([]string{"kvalchemy", "storage", "boltdb", "delete", "total"}, 1, b.label)
	startTime := time.Now()
	defer func() {
		metrics.MeasureSinceWithLabels([]string{"kvalchemy", "storage", "boltdb", "delete", "latency", "msec"}, startTime, b.label)
	}()
	return b.db.Update(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket(b.namespace)
		if bucket == nil {
			return ErrBucketNotFound
		}

		storedValue := bucket.Get(key)
		if storedValue == nil {
			return nil
		}

		flag := storedValue[0]
		switch flag {
		case FullValueFlag:

			return bucket.Delete(key)

		case ChunkedValueFlag:
			if len(storedValue) < 9 {
				return ErrInvalidChunkMetadata
			}

			chunkCount := binary.LittleEndian.Uint32(storedValue[1:5])

			for i := 0; i < int(chunkCount); i++ {
				chunkKey := fmt.Sprintf("%s_chunk_%d", key, i)
				if err := bucket.Delete([]byte(chunkKey)); err != nil {
					return err
				}
			}

			return bucket.Delete(key)
		}

		return ErrInvalidDataFormat
	})
}

// DeleteMany delete multiple values with corresponding keys within a namespace.
func (b *BoltDBEmbed) DeleteMany(keys [][]byte) error {
	metrics.IncrCounterWithLabels([]string{"kvalchemy", "storage", "boltdb", "delete", "many", "total"}, 1, b.label)
	startTime := time.Now()
	defer func() {
		metrics.MeasureSinceWithLabels([]string{"kvalchemy", "storage", "boltdb", "delete", "many", "latency", "msec"}, startTime, b.label)
	}()
	return b.db.Update(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket(b.namespace)
		if bucket == nil {
			return ErrBucketNotFound
		}
		for _, key := range keys {
			storedValue := bucket.Get(key)
			if storedValue == nil {
				return nil
			}

			flag := storedValue[0]
			switch flag {
			case FullValueFlag:
				if err := bucket.Delete(key); err != nil {
					return err
				}

			case ChunkedValueFlag:
				if len(storedValue) < 9 {
					return ErrInvalidChunkMetadata
				}

				return b.deleteChunk(key, storedValue, bucket)
			}
		}
		return nil
	})
}

func (b *BoltDBEmbed) deleteChunk(key []byte, storedValue []byte, bucket *bbolt.Bucket) error {
	chunkCount := binary.LittleEndian.Uint32(storedValue[1:5])

	for i := 0; i < int(chunkCount); i++ {
		chunkKey := fmt.Sprintf("%s_chunk_%d", key, i)
		if err := bucket.Delete([]byte(chunkKey)); err != nil {
			return err
		}
	}
	return bucket.Delete(key)
}

// Get retrieves a value associated with a key within a specific namespace.
func (b *BoltDBEmbed) Get(key []byte) ([]byte, error) {
	metrics.IncrCounterWithLabels([]string{"kvalchemy", "storage", "boltdb", "get", "total"}, 1, b.label)
	startTime := time.Now()
	defer func() {
		metrics.MeasureSinceWithLabels([]string{"kvalchemy", "storage", "boltdb", "get", "latency", "msec"}, startTime, b.label)
	}()
	var value []byte

	err := b.db.View(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket(b.namespace)
		if bucket == nil {
			return ErrBucketNotFound
		}

		storedValue := bucket.Get(key)
		if storedValue == nil {
			return ErrKeyNotFound
		}

		flag := storedValue[0]
		switch flag {
		case FullValueFlag:
			value = make([]byte, len(storedValue[1:]))
			copy(value, storedValue[1:])
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

				chunkData := bucket.Get([]byte(chunkKey))
				if chunkData == nil {
					return fmt.Errorf("chunk %d missing", i)
				}
				calculatedChecksum = crc32.Update(calculatedChecksum, crc32.IEEETable, chunkData)
				fullValue.Write(chunkData)
			}

			if calculatedChecksum != storedChecksum {
				return ErrRecordCorrupted
			}

			value = make([]byte, fullValue.Len())
			copy(value, fullValue.Bytes())
			return nil
		default:
			// we don't know how to deal with this return the data and error.
			value = make([]byte, len(storedValue))
			copy(value, storedValue)
			return fmt.Errorf("invalid data format for key %s: %w", string(key), ErrInvalidDataFormat)
		}
	})

	return value, err
}

func (b *BoltDBEmbed) StoreMetadata(key []byte, value []byte) error {
	return b.db.Update(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket([]byte(sysBucketMetaData))
		if bucket == nil {
			return ErrBucketNotFound
		}
		return bucket.Put(key, value)
	})
}

func (b *BoltDBEmbed) RetrieveMetadata(key []byte) ([]byte, error) {
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

func (b *BoltDBEmbed) Restore(reader io.Reader) error {
	// close the current db
	if err := b.db.Close(); err != nil {
		return err
	}

	if err := os.Remove(b.conf.Path); err != nil {
		return err
	}

	newDBFile, err := os.Create(b.conf.Path)
	if err != nil {
		return fmt.Errorf("failed to create new database file: %w", err)
	}

	_, err = io.Copy(newDBFile, reader)
	if err != nil {
		return fmt.Errorf("failed to restore database from snapshot: %w", err)
	}

	if err := newDBFile.Close(); err != nil {
		return fmt.Errorf("failed to close new database file: %w", err)
	}

	db, err := bbolt.Open(b.conf.Path, 0600, nil)
	if err != nil {
		return fmt.Errorf("failed to open new database file: %w", err)
	}
	db.NoSync = b.conf.NoSync

	b.db = db
	return nil
}
