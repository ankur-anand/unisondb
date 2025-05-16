package kvdrivers

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"time"

	"go.etcd.io/bbolt"
)

// BoltDBEmbed embed an initialized bolt db and implements PersistenceWriter and PersistenceReader.
type BoltDBEmbed struct {
	db        *bbolt.DB
	namespace []byte
	conf      Config
	path      string
	mt        *MetricsTracker
}

func NewBoltdb(path string, conf Config) (*BoltDBEmbed, error) {
	db, err := bbolt.Open(path, 0600, nil)
	if err != nil {
		return nil, err
	}
	err = db.Update(func(tx *bbolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists([]byte(conf.Namespace))
		if err != nil {
			return err
		}
		_, err = tx.CreateBucketIfNotExists([]byte(sysBucketMetaData))
		return err
	})
	return &BoltDBEmbed{db: db,
		namespace: []byte(conf.Namespace),
		conf:      conf,
		path:      path,
		mt:        NewMetricsTracker("bolt", conf.Namespace),
	}, err
}

func (b *BoltDBEmbed) FSync() error {
	return b.db.Sync()
}

func (b *BoltDBEmbed) Close() error {
	return b.db.Close()
}

// Set associates a value with a key within a specific namespace.
func (b *BoltDBEmbed) Set(key []byte, value []byte) error {
	err := b.db.Update(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket(b.namespace)
		if bucket == nil {
			return ErrBucketNotFound
		}
		// indicate this is a full value, not chunked
		storedValue := append([]byte{kvValue}, value...)

		return bucket.Put(key, storedValue)
	})

	return err
}

// SetMany associates multiple values with corresponding keys within a namespace.
func (b *BoltDBEmbed) SetMany(keys [][]byte, value [][]byte) error {
	err := b.db.Update(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket(b.namespace)
		if bucket == nil {
			return ErrBucketNotFound
		}
		for i, key := range keys {
			// indicate this is a full value, not chunked
			storedValue := append([]byte{kvValue}, value[i]...)

			err := bucket.Put(key, storedValue)
			if err != nil {
				return err
			}
		}
		return nil
	})

	return err
}

// SetChunks stores a value that has been split into chunks, associating them with a single key.
func (b *BoltDBEmbed) SetChunks(key []byte, chunks [][]byte, checksum uint32) error {
	err := b.db.Update(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket(b.namespace)
		if bucket == nil {
			return ErrBucketNotFound
		}

		// get last stored for keys, if present.
		// older chunk needs to deleted for not leaking the space.
		storedValue := bucket.Get(key)
		if storedValue != nil && storedValue[0] == chunkedValue {
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
		metaData[0] = chunkedValue
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

	return err
}

// SetManyRowColumns update/insert multiple rows and the provided columnEntries to the row.
func (b *BoltDBEmbed) SetManyRowColumns(rowKeys [][]byte, columnEntriesPerRow []map[string][]byte) error {
	if len(rowKeys) != len(columnEntriesPerRow) {
		return ErrInvalidArguments
	}

	totalColumns := 0
	err := b.db.Update(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket(b.namespace)
		if bucket == nil {
			return ErrBucketNotFound
		}

		for i, rowKey := range rowKeys {
			columnEntries := columnEntriesPerRow[i]
			if len(columnEntries) == 0 {
				continue
			}

			totalColumns += len(columnEntries)
			pKey := append(append([]byte(nil), rowKey...), rowKeySeperator...)
			entries := appendRowKeyToColumnKey(pKey, columnEntries)

			for entryKey, entry := range entries {
				if err := bucket.Put([]byte(entryKey), entry); err != nil {
					return err
				}
			}

			if err := bucket.Put(pKey, []byte{rowColumnValue}); err != nil {
				return err
			}
		}
		return nil
	})

	return err
}

// DeleteManyRowColumns delete the provided columnEntries from the associated row.
func (b *BoltDBEmbed) DeleteManyRowColumns(rowKeys [][]byte, columnEntriesPerRow []map[string][]byte) error {
	if len(rowKeys) != len(columnEntriesPerRow) {
		return ErrInvalidArguments
	}

	totalColumns := 0
	err := b.db.Update(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket(b.namespace)
		if bucket == nil {
			return ErrBucketNotFound
		}

		for i, rowKey := range rowKeys {
			columnEntries := columnEntriesPerRow[i]
			if len(columnEntries) == 0 {
				continue
			}

			totalColumns += len(columnEntries)
			pKey := append(append([]byte(nil), rowKey...), rowKeySeperator...)
			entries := appendRowKeyToColumnKey(pKey, columnEntries)

			for entryKey := range entries {
				if err := bucket.Delete([]byte(entryKey)); err != nil {
					return err
				}
			}

			if err := bucket.Put(pKey, []byte{rowColumnValue}); err != nil {
				return err
			}
		}
		return nil
	})

	return err
}

// DeleteEntireRows deletes all the columns associated with all the rowKeys.
func (b *BoltDBEmbed) DeleteEntireRows(rowKeys [][]byte) (int, error) {
	columnsDeleted := -len(rowKeys)
	err := b.db.Update(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket(b.namespace)
		if bucket == nil {
			return ErrBucketNotFound
		}

		for _, rowKey := range rowKeys {
			c := bucket.Cursor()
			pKey := append(append([]byte(nil), rowKey...), rowKeySeperator...)

			keys := make([][]byte, 0)
			// IMP: Never delete in cursor, directly followed by Next
			// k, _ := c.Seek(rowKey); k != nil; k, _ = c.Next() {
			// 		c.Delete()
			// }
			// The above pattern should be avoided at all for
			// https://github.com/boltdb/bolt/issues/620
			// https://github.com/etcd-io/bbolt/issues/146
			// Cursor.Delete followed by Next skips the next k/v pair
			for k, _ := c.Seek(rowKey); k != nil; k, _ = c.Next() {
				if !bytes.HasPrefix(k, pKey) {
					break // Stop if key is outside the prefix range
				}
				keys = append(keys, k)
			}

			for _, k := range keys {
				columnsDeleted++
				if err := bucket.Delete(k); err != nil {
					return err
				}
			}
		}

		return nil
	})

	return columnsDeleted, err
}

// Delete deletes a value with a key within a specific namespace.
func (b *BoltDBEmbed) Delete(key []byte) error {
	err := b.db.Update(func(tx *bbolt.Tx) error {
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
		case kvValue:
			return bucket.Delete(key)

		case chunkedValue:
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

		return ErrInvalidOpsForValueType
	})

	return err
}

// DeleteMany delete multiple values with corresponding keys within a namespace.
func (b *BoltDBEmbed) DeleteMany(keys [][]byte) error {
	err := b.db.Update(func(tx *bbolt.Tx) error {
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
			case kvValue:
				if err := bucket.Delete(key); err != nil {
					return err
				}

			case chunkedValue:
				if len(storedValue) < 9 {
					return ErrInvalidChunkMetadata
				}

				if err := b.deleteChunk(key, storedValue, bucket); err != nil {
					return err
				}
			}
		}
		return nil
	})

	return err
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

// GetValueType returns the kind of value associated with the key if any.
func (b *BoltDBEmbed) GetValueType(key []byte) (ValueEntryType, error) {
	rowKey := []byte(string(key) + rowKeySeperator)
	entryType := UnknownValueEntry

	err := b.db.View(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket(b.namespace)
		if bucket == nil {
			return ErrBucketNotFound
		}

		storedValue := bucket.Get(key)
		if storedValue == nil {
			// check if Column Value type
			storedValue = bucket.Get(rowKey)
			if storedValue == nil {
				return ErrKeyNotFound
			}
		}

		flag := storedValue[0]
		switch flag {
		case kvValue:
			entryType = KeyValueValueEntry
		case chunkedValue:
			entryType = ChunkedValueEntry
		case rowColumnValue:
			entryType = RowColumnValueEntry
		}

		return nil
	})

	return entryType, err
}

func (b *BoltDBEmbed) recordOpResult(op string, start time.Time, err error) {
	if err != nil {
		b.mt.RecordError(op)
	} else {
		b.mt.RecordOp(op, start)
	}
}

// Get retrieves a value associated with a key within a specific namespace.
func (b *BoltDBEmbed) Get(key []byte) ([]byte, error) {
	rowKey := []byte(string(key) + rowKeySeperator)

	startTime := time.Now()
	var value []byte

	err := b.db.View(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket(b.namespace)
		if bucket == nil {
			return ErrBucketNotFound
		}

		storedValue := bucket.Get(key)
		if storedValue == nil {
			// check if Column Value type
			storedValue = bucket.Get(rowKey)
			if storedValue == nil {
				return ErrKeyNotFound
			}
		}

		flag := storedValue[0]
		switch flag {
		case kvValue:
			value = make([]byte, len(storedValue[1:]))
			copy(value, storedValue[1:])
			return nil

		case chunkedValue:
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
		case rowColumnValue:
			return ErrUseGetColumnAPI
		default:
			// we don't know how to deal with this return the data and error.
			value = make([]byte, len(storedValue))
			copy(value, storedValue)
			return fmt.Errorf("invalid data format for key %s: %w", string(key), ErrInvalidOpsForValueType)
		}
	})

	b.recordOpResult(OpGet, startTime, err)
	return value, err
}

// GetRowColumns returns all the columns for the given row. If a ColumnPredicate predicate func is provided, it will only
// return those columns for which the ColumnFilterFunc func returns true.
func (b *BoltDBEmbed) GetRowColumns(rowKey []byte, filter func(columnKey []byte) bool) (map[string][]byte, error) {
	rowKey = []byte(string(rowKey) + rowKeySeperator)
	startTime := time.Now()

	if filter == nil {
		filter = func(columnKey []byte) bool {
			return true
		}
	}

	entries := make(map[string][]byte)
	err := b.db.View(func(txn *bbolt.Tx) error {
		bucket := txn.Bucket(b.namespace)
		if bucket == nil {
			return ErrBucketNotFound
		}

		storedValue := bucket.Get(rowKey)
		if storedValue == nil {
			return ErrKeyNotFound
		}

		flag := storedValue[0]
		switch flag {
		case rowColumnValue:

			return b.getColumns(bucket, rowKey, filter, entries)

		default:
			return fmt.Errorf("invalid data format for Row key %s: %w", string(rowKey), ErrInvalidOpsForValueType)
		}
	})

	b.recordOpResult(OpGet, startTime, err)
	return entries, err
}

func (b *BoltDBEmbed) getColumns(bucket *bbolt.Bucket,
	rowKey []byte,
	filter ColumnPredicate, entries map[string][]byte) error {
	c := bucket.Cursor()
	skip := true

	for k, value := c.Seek(rowKey); k != nil; k, value = c.Next() {
		if !bytes.HasPrefix(k, rowKey) {
			break // Stop if key is outside the prefix range
		}

		if skip {
			skip = false
			continue
		}

		trimmedKey := bytes.TrimPrefix(k, rowKey)
		if filter(trimmedKey) {
			entries[string(trimmedKey)] = make([]byte, len(value))
			copy(entries[string(trimmedKey)], value)
		}
	}
	return nil
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

	if err := os.Remove(b.path); err != nil {
		return err
	}

	newDBFile, err := os.Create(b.path)
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

	db, err := bbolt.Open(b.path, 0600, nil)
	if err != nil {
		return fmt.Errorf("failed to open new database file: %w", err)
	}
	db.NoSync = b.conf.NoSync

	b.db = db
	return nil
}
