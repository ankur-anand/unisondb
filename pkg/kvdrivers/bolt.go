package kvdrivers

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"time"

	"go.etcd.io/bbolt"
)

// compile time check.
var _ unifiedStorage = (*BoltDBEmbed)(nil)

// BoltDBEmbed wraps an initialized BoltDB (bbolt) database and related metadata.
type BoltDBEmbed struct {
	db        *bbolt.DB
	namespace []byte
	conf      Config
	path      string
	mt        *MetricsTracker
}

// NewBoltdb opens (or creates) a BoltDB database at the given file path,
// initializes the namespace bucket and a system metadata bucket,
// and returns a BoltDBEmbed wrapper.
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

// FSync ensures all database pages are flushed to disk.
func (b *BoltDBEmbed) FSync() error {
	return b.db.Sync()
}

// Close closes the underlying BoltDB database.
func (b *BoltDBEmbed) Close() error {
	return b.db.Close()
}

// SetKV associates a value with a key within a specific namespace.
func (b *BoltDBEmbed) SetKV(key []byte, value []byte) error {
	err := b.db.Update(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket(b.namespace)
		if bucket == nil {
			return ErrBucketNotFound
		}
		typedKey := KeyKV(key)
		return bucket.Put(typedKey, value)
	})

	return err
}

// BatchSetKV associates multiple values with corresponding keys within a namespace.
func (b *BoltDBEmbed) BatchSetKV(keys [][]byte, values [][]byte) error {
	if len(keys) == 0 || len(values) == 0 || len(keys) != len(values) {
		return ErrInvalidArguments
	}

	err := b.db.Update(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket(b.namespace)
		if bucket == nil {
			return ErrBucketNotFound
		}
		for i, key := range keys {
			err := bucket.Put(KeyKV(key), values[i])
			if err != nil {
				return err
			}
		}
		return nil
	})

	return err
}

// SetLobChunks stores a value that has been split into chunks, associating them with a single key.
func (b *BoltDBEmbed) SetLobChunks(key []byte, chunks [][]byte, checksum uint32) error {
	err := b.db.Update(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket(b.namespace)
		if bucket == nil {
			return ErrBucketNotFound
		}

		// get last stored for keys, if present.
		// older chunk needs to deleted for not leaking the space.
		// Chunk Count will Start with 1.
		// we keep the zero for the metadata key.
		typedKey := KeyBlobChunk(key, 0)
		storedValue := bucket.Get(typedKey)
		if storedValue != nil && storedValue[0] == chunkedValue {
			if len(storedValue) < 9 {
				return ErrInvalidChunkMetadata
			}
			chunkCount := binary.LittleEndian.Uint32(storedValue[1:5])

			for i := 1; i <= int(chunkCount); i++ {
				chunkKey := KeyBlobChunk(key, i)
				if err := bucket.Delete(chunkKey); err != nil {
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

		// individual chunk
		// make sure chunk start at 1.
		for i, chunk := range chunks {
			chunkKey := KeyBlobChunk(key, i+1)
			if err := bucket.Put(chunkKey, chunk); err != nil {
				return err
			}
		}

		// chunk metadata
		if err := bucket.Put(typedKey, metaData); err != nil {
			return err
		}

		return nil
	})

	return err
}

// BatchSetCells sets or updates multiple cells (row, column, value) in one call.
func (b *BoltDBEmbed) BatchSetCells(rowKeys [][]byte, columnEntriesPerRow []map[string][]byte) error {
	if len(rowKeys) != len(columnEntriesPerRow) {
		return ErrInvalidArguments
	}

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

			for entryKey, entryValue := range columnEntries {
				typedKey := KeyColumn(rowKey, unsafeStringToBytes(entryKey))
				if err := bucket.Put(typedKey, entryValue); err != nil {
					return err
				}
			}
		}
		return nil
	})

	return err
}

// BatchDeleteCells deletes specific cells (row, column pairs) from the store.
func (b *BoltDBEmbed) BatchDeleteCells(rowKeys [][]byte, columnEntriesPerRow []map[string][]byte) error {
	if len(rowKeys) != len(columnEntriesPerRow) {
		return ErrInvalidArguments
	}

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

			for entryKey := range columnEntries {
				typedKey := KeyColumn(rowKey, unsafeStringToBytes(entryKey))
				if err := bucket.Delete(typedKey); err != nil {
					return err
				}
			}
		}
		return nil
	})

	return err
}

// BatchDeleteRows deletes all columns (cells) associated with each row key.
// Returns the number of columns deleted and an error, if any.
func (b *BoltDBEmbed) BatchDeleteRows(rowKeys [][]byte) (int, error) {
	columnsDeleted := 0

	err := b.db.Update(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket(b.namespace)
		if bucket == nil {
			return ErrBucketNotFound
		}

		for _, rowKey := range rowKeys {
			c := bucket.Cursor()
			pKey := RowKey(rowKey)

			keys := make([][]byte, 0)
			// IMP: Never delete in cursor, directly followed by Next
			// k, _ := c.Seek(rowKey); k != nil; k, _ = c.Next() {
			// 		c.DeleteKV()
			// }
			// The above pattern should be avoided at all for
			// https://github.com/boltdb/bolt/issues/620
			// https://github.com/etcd-io/bbolt/issues/146
			// Cursor.DeleteKV followed by Next skips the next k/v pair
			for k, _ := c.Seek(pKey); k != nil; k, _ = c.Next() {
				if !bytes.HasPrefix(k, pKey) {
					break // Stop if key is outside the prefix range
				}
				kk := append([]byte(nil), k...)
				keys = append(keys, kk)
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

// DeleteKV deletes a value with a key within a specific namespace.
func (b *BoltDBEmbed) DeleteKV(key []byte) error {
	err := b.db.Update(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket(b.namespace)
		if bucket == nil {
			return ErrBucketNotFound
		}
		typedKey := KeyKV(key)

		storedValue := bucket.Get(typedKey)
		if storedValue == nil {
			return nil
		}
		return bucket.Delete(typedKey)
	})

	return err
}

// BatchDeleteKV delete multiple values with corresponding keys within a namespace.
func (b *BoltDBEmbed) BatchDeleteKV(keys [][]byte) error {
	err := b.db.Update(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket(b.namespace)
		if bucket == nil {
			return ErrBucketNotFound
		}
		for _, key := range keys {
			typedKey := KeyKV(key)
			storedValue := bucket.Get(typedKey)
			if storedValue == nil {
				continue
			}
			if err := bucket.Delete(typedKey); err != nil {
				return err
			}
		}
		return nil
	})

	return err
}

// BatchDeleteLobChunks Delete all chunked parts of a large value.
func (b *BoltDBEmbed) BatchDeleteLobChunks(keys [][]byte) error {
	err := b.db.Update(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket(b.namespace)
		if bucket == nil {
			return ErrBucketNotFound
		}

		for _, key := range keys {
			typedKey := KeyBlobChunk(key, 0)
			storedValue := bucket.Get(typedKey)
			if storedValue == nil {
				continue
			}

			if len(storedValue) < 9 {
				return ErrInvalidChunkMetadata
			}
			chunkCount := binary.LittleEndian.Uint32(storedValue[1:5])

			for i := 1; i <= int(chunkCount); i++ {
				chunkKey := KeyBlobChunk(key, i)
				if err := bucket.Delete(chunkKey); err != nil {
					return err
				}
			}

			if err := bucket.Delete(typedKey); err != nil {
				return err
			}
		}

		return nil
	})
	return err
}

func (b *BoltDBEmbed) recordOpResult(op string, start time.Time, err error) {
	if err != nil {
		b.mt.RecordError(op)
	} else {
		b.mt.RecordOp(op, start)
	}
}

// GetKV retrieves a value associated with a key within a specific namespace.
func (b *BoltDBEmbed) GetKV(key []byte) ([]byte, error) {
	typedKey := KeyKV(key)

	startTime := time.Now()
	var value []byte

	err := b.db.View(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket(b.namespace)
		if bucket == nil {
			return ErrBucketNotFound
		}

		storedValue := bucket.Get(typedKey)
		if storedValue == nil {
			return ErrKeyNotFound
		}
		value = make([]byte, len(storedValue))
		copy(value, storedValue)
		return nil
	})

	b.recordOpResult(OpGet, startTime, err)
	return value, err
}

// GetLOBChunks retrieves all chunks associated with a large object (LOB) key.
func (b *BoltDBEmbed) GetLOBChunks(key []byte) ([][]byte, error) {
	typedKey := KeyBlobChunk(key, 0)
	startTime := time.Now()
	var value [][]byte
	err := b.db.View(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket(b.namespace)
		if bucket == nil {
			return ErrBucketNotFound
		}
		storedValue := bucket.Get(typedKey)
		if storedValue == nil {
			return ErrKeyNotFound
		}
		if len(storedValue) < 9 {
			return ErrInvalidChunkMetadata
		}
		chunkCount := binary.LittleEndian.Uint32(storedValue[1:5])
		value = make([][]byte, chunkCount)

		for i := 0; i < int(chunkCount); i++ {
			chunkKey := KeyBlobChunk(key, i+1)
			chunkData := bucket.Get(chunkKey)
			if chunkData == nil {
				return fmt.Errorf("chunk %d missing", i)
			}
			chunkCopy := make([]byte, len(chunkData))
			copy(chunkCopy, chunkData)
			value[i] = chunkCopy
		}
		return nil
	})

	b.recordOpResult(OpGet, startTime, err)
	return value, err
}

// ScanRowCells returns all cells (column name/value pairs) for the given rowKey.
// If a ColumnPredicate predicate func is provided, it will only
// return those columns for which the ColumnFilterFunc func returns true.
func (b *BoltDBEmbed) ScanRowCells(rowKey []byte, filter func(columnKey []byte) bool) (map[string][]byte, error) {
	prefix := RowKey(rowKey)
	startTime := time.Now()

	if filter == nil {
		filter = func(columnKey []byte) bool { return true }
	}

	var entries map[string][]byte
	err := b.db.View(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket(b.namespace)
		if bucket == nil {
			return ErrBucketNotFound
		}
		tmp := make(map[string][]byte)
		if err := b.getColumns(bucket, prefix, filter, tmp); err != nil {
			return err
		}
		entries = tmp
		return nil
	})

	b.recordOpResult(OpGet, startTime, err)

	return entries, err
}

func (b *BoltDBEmbed) getColumns(
	bucket *bbolt.Bucket,
	prefix []byte,
	filter func([]byte) bool,
	entries map[string][]byte,
) error {
	cursor := bucket.Cursor()

	for k, v := cursor.Seek(prefix); k != nil; k, v = cursor.Next() {
		if !bytes.HasPrefix(k, prefix) {
			break
		}
		// skip bare row key
		if len(k) == len(prefix) {
			continue
		}
		columnKey := bytes.TrimPrefix(k, prefix)

		if filter(columnKey) {
			valCopy := make([]byte, len(v))
			copy(valCopy, v)
			entries[string(columnKey)] = valCopy
		}
	}

	if len(entries) == 0 {
		return ErrKeyNotFound
	}

	return nil
}

// GetCell retrieves the value for the given (rowKey, columnName) pair.
func (b *BoltDBEmbed) GetCell(rowKey []byte, columnName string) ([]byte, error) {
	typedKey := KeyColumn(rowKey, unsafeStringToBytes(columnName))

	startTime := time.Now()
	var value []byte

	err := b.db.View(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket(b.namespace)
		if bucket == nil {
			return ErrBucketNotFound
		}
		storedValue := bucket.Get(typedKey)
		if storedValue == nil {
			return ErrKeyNotFound
		}
		value = make([]byte, len(storedValue))
		copy(value, storedValue)
		return nil
	})

	b.recordOpResult(OpGet, startTime, err)
	return value, err
}

// GetCells fetches the values for the specified columns in the given row.
func (b *BoltDBEmbed) GetCells(rowKey []byte, columns []string) (map[string][]byte, error) {
	startTime := time.Now()
	result := make(map[string][]byte)

	err := b.db.View(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket(b.namespace)
		if bucket == nil {
			return ErrBucketNotFound
		}
		for _, column := range columns {
			typedKey := KeyColumn(rowKey, unsafeStringToBytes(column))
			gotValue := bucket.Get(typedKey)
			if gotValue != nil {
				valueCopy := make([]byte, len(gotValue))
				copy(valueCopy, gotValue)
				result[column] = valueCopy
			}
		}
		return nil
	})

	b.recordOpResult(OpGet, startTime, err)
	return result, err
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
		return fmt.Errorf("failed to close current db: %w", err)
	}

	tmpPath := b.path + ".restore.tmp"
	tmpFile, err := os.Create(tmpPath)
	if err != nil {
		return fmt.Errorf("failed to create temp restore file: %w", err)
	}

	if _, err := io.Copy(tmpFile, reader); err != nil {
		_ = tmpFile.Close()
		_ = os.Remove(tmpPath)
		return fmt.Errorf("failed to restore database from snapshot: %w", err)
	}

	if err := tmpFile.Sync(); err != nil {
		_ = tmpFile.Close()
		_ = os.Remove(tmpPath)
		return fmt.Errorf("failed to sync temp restore file: %w", err)
	}

	if err := tmpFile.Close(); err != nil {
		_ = os.Remove(tmpPath)
		return fmt.Errorf("failed to close temp restore file: %w", err)
	}

	if err := os.Rename(tmpPath, b.path); err != nil {
		_ = os.Remove(tmpPath)
		return fmt.Errorf("failed to replace old db with restored db: %w", err)
	}

	db, err := bbolt.Open(b.path, 0600, nil)
	if err != nil {
		return fmt.Errorf("failed to open new database file: %w", err)
	}
	db.NoSync = b.conf.NoSync

	b.db = db
	return nil
}

func (b *BoltDBEmbed) Snapshot(w io.Writer) error {
	// a real file, use it directly and fsync at the end.
	if f, ok := w.(*os.File); ok {
		return b.db.View(func(tx *bbolt.Tx) error {
			if _, err := tx.WriteTo(f); err != nil {
				return err
			}
			return f.Sync()
		})
	}

	// non-file writers, add buffering.
	buf := bufio.NewWriterSize(w, 1<<20)
	err := b.db.View(func(tx *bbolt.Tx) error {
		_, err := tx.WriteTo(buf)
		return err
	})
	if err != nil {
		return err
	}
	return buf.Flush()
}
