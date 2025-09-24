package kvdrivers

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"time"

	"github.com/PowerDNS/lmdb-go/lmdb"
)

var _ unifiedStorage = (*LmdbEmbed)(nil)

// LmdbEmbed stores an initialized lmdb environment.
// http://www.lmdb.tech/doc/group__mdb.html
type LmdbEmbed struct {
	env       *lmdb.Env
	namespace []byte
	dataDB    lmdb.DBI
	metaDB    lmdb.DBI
	mt        *MetricsTracker
}

// NewLmdb returns an initialized Lmdb Env with the provided configuration Parameter.
func NewLmdb(path string, conf Config) (*LmdbEmbed, error) {
	// Create directory if it doesn't exist
	if err := os.MkdirAll(path, 0755); err != nil {
		return nil, fmt.Errorf("failed to create directory %s: %w", path, err)
	}

	env, err := lmdb.NewEnv()
	if err != nil {
		return nil, err
	}

	// one extra for metadata storage.
	err = env.SetMaxDBs(2)
	if err != nil {
		return nil, fmt.Errorf("failed to set max DBs: %w", err)
	}

	err = env.SetMapSize(conf.MmapSize)
	if err != nil {
		return nil, fmt.Errorf("failed to set map size: %w", err)
	}

	err = env.Open(path, lmdb.Create|lmdb.NoReadahead, 0644)
	if err != nil {
		return nil, fmt.Errorf("failed to open environment: %w", err)
	}

	if conf.NoSync {
		err := env.SetFlags(lmdb.NoSync)
		if err != nil {
			return nil, fmt.Errorf("failed to open environment: %w", err)
		}
	}

	// stale readers
	staleReaders, err := env.ReaderCheck()
	if err != nil {
		return nil, fmt.Errorf("failed to check for stale readers: %w", err)
	}
	if staleReaders > 0 {
		slog.Warn("[kvdrivers]", slog.String("message", "Cleared reader slots from dead processes"),
			slog.Int("stale_readers", staleReaders))
	}

	var dataDB, metaDB lmdb.DBI
	err = env.Update(func(txn *lmdb.Txn) error {
		var err error
		dataDB, err = txn.OpenDBI(conf.Namespace, lmdb.Create)
		return err
	})
	if err != nil {
		return nil, err
	}

	// created for storing system metadata.
	err = env.Update(func(txn *lmdb.Txn) error {
		var err error
		metaDB, err = txn.OpenDBI(sysBucketMetaData, lmdb.Create)
		return err
	})
	if err != nil {
		return nil, err
	}

	mt := NewMetricsTracker("lmdb", conf.Namespace)
	return &LmdbEmbed{env: env, dataDB: dataDB, metaDB: metaDB, namespace: []byte(conf.Namespace), mt: mt}, nil
}

// FSync Call the underlying Fsync.
func (l *LmdbEmbed) FSync() error {
	return l.env.Sync(true)
}

// Close the underlying lmdb env.
func (l *LmdbEmbed) Close() error {
	return l.env.Close()
}

// SetKV associates a value with a key within a specific namespace.
func (l *LmdbEmbed) SetKV(key []byte, value []byte) error {
	return l.env.Update(func(txn *lmdb.Txn) error {
		typedKey := KeyKV(key)
		err := txn.Put(l.dataDB, typedKey, value, 0)
		if err != nil {
			return err
		}
		return nil
	})
}

// BatchSetKV associates multiple values with corresponding keys within a namespace.
func (l *LmdbEmbed) BatchSetKV(keys [][]byte, values [][]byte) error {
	if len(keys) != len(values) {
		return fmt.Errorf("keys and values length mismatch: keys=%d values=%d", len(keys), len(values))
	}

	return l.env.Update(func(txn *lmdb.Txn) error {
		for i, key := range keys {
			typedKey := KeyKV(key)
			if err := txn.Put(l.dataDB, typedKey, values[i], 0); err != nil {
				return err
			}
		}
		return nil
	})
}

// SetLobChunks stores a value that has been split into chunks, associating them with a single key.
func (l *LmdbEmbed) SetLobChunks(key []byte, chunks [][]byte, checksum uint32) error {
	if len(chunks) == 0 {
		return errors.New("empty chunks array")
	}

	typedKey := KeyBlobChunk(key, 0)

	metaData := make([]byte, 9)
	metaData[0] = chunkedValue
	binary.LittleEndian.PutUint32(metaData[1:], uint32(len(chunks)))
	binary.LittleEndian.PutUint32(metaData[5:], checksum)

	return l.env.Update(func(txn *lmdb.Txn) error {
		// existing chunks and delete them
		storedValue, err := txn.Get(l.dataDB, typedKey)
		if err == nil && len(storedValue) > 0 && storedValue[0] == chunkedValue {
			if len(storedValue) < 9 {
				return fmt.Errorf("invalid chunk metadata for key %s: %w", string(key), ErrInvalidChunkMetadata)
			}

			oldChunkCount := binary.LittleEndian.Uint32(storedValue[1:5])
			for i := 1; i <= int(oldChunkCount); i++ {
				chunkKey := KeyBlobChunk(key, i)
				if err := txn.Del(l.dataDB, chunkKey, nil); err != nil && !lmdb.IsNotFound(err) {
					return err
				}
			}
		}

		// Store chunks
		// We Use 0 For Metadata, so make sure the value starts at 1 index.
		for i, chunk := range chunks {
			chunkKey := KeyBlobChunk(key, i+1)
			err := txn.Put(l.dataDB, chunkKey, chunk, 0)
			if err != nil {
				return err
			}
		}

		// Store metadata
		if err := txn.Put(l.dataDB, typedKey, metaData, 0); err != nil {
			return err
		}

		return nil
	})
}

// BatchSetCells update/insert multiple rows and the provided columnEntries to the row.
func (l *LmdbEmbed) BatchSetCells(rowKeys [][]byte, columnEntriesPerRow []map[string][]byte) error {
	if len(rowKeys) != len(columnEntriesPerRow) {
		return ErrInvalidArguments
	}

	err := l.env.Update(func(tx *lmdb.Txn) error {
		for i, rowKey := range rowKeys {
			columnEntries := columnEntriesPerRow[i]
			if len(columnEntries) == 0 {
				continue
			}
			for columnKey, columnValue := range columnEntries {
				typedKey := KeyColumn(rowKey, unsafeStringToBytes(columnKey))
				if err := tx.Put(l.dataDB, typedKey, columnValue, 0); err != nil {
					return err
				}
			}
		}
		return nil
	})

	return err
}

// BatchDeleteCells delete the provided columnEntries from the associated row.
func (l *LmdbEmbed) BatchDeleteCells(rowKeys [][]byte, columnEntriesPerRow []map[string][]byte) error {
	if len(rowKeys) != len(columnEntriesPerRow) {
		return ErrInvalidArguments
	}

	err := l.env.Update(func(tx *lmdb.Txn) error {
		for i, rowKey := range rowKeys {
			columnEntries := columnEntriesPerRow[i]
			if len(columnEntries) == 0 {
				continue
			}

			for columnKey := range columnEntries {
				typedKey := KeyColumn(rowKey, unsafeStringToBytes(columnKey))
				if err := tx.Del(l.dataDB, typedKey, nil); err != nil {
					if !lmdb.IsNotFound(err) {
						return err
					}
				}
			}
		}
		return nil
	})

	return err
}

// BatchDeleteRows deletes all the columns associated with all the rowKeys.
func (l *LmdbEmbed) BatchDeleteRows(rowKeys [][]byte) (int, error) {
	if len(rowKeys) == 0 {
		return 0, nil
	}

	columnsDeleted := 0

	err := l.env.Update(func(tx *lmdb.Txn) error {
		c, err := tx.OpenCursor(l.dataDB)
		if err != nil {
			return err
		}
		defer c.Close()

		for _, rowKey := range rowKeys {
			pKey := RowKey(rowKey)
			// http://www.lmdb.tech/doc/group__mdb.html#ga1206b2af8b95e7f6b0ef6b28708c9127
			// MDB_SET_RANGE
			// Position at first key greater than or equal to specified key.
			k, _, err := c.Get(pKey, nil, lmdb.SetRange)

			for err == nil {
				if !bytes.HasPrefix(k, pKey) {
					break // Stop if key is outside the prefix range
				}

				if err := c.Del(0); err != nil && !lmdb.IsNotFound(err) {
					return err
				}
				// Move to next key
				columnsDeleted++
				k, _, err = c.Get(nil, nil, lmdb.Next)
			}

			if err != nil {
				if lmdb.IsNotFound(err) {
					return nil
				}
				return err
			}
		}
		return nil
	})

	return columnsDeleted, err
}

// DeleteKV deletes a value with a key within a specific namespace.
func (l *LmdbEmbed) DeleteKV(key []byte) error {
	typedKey := KeyKV(key)

	return l.env.Update(func(txn *lmdb.Txn) error {
		err := txn.Del(l.dataDB, typedKey, nil)
		if err != nil {
			if lmdb.IsNotFound(err) {
				return nil
			}
			return err
		}
		return nil
	})
}

// BatchDeleteKV deletes multiple values with corresponding keys within a namespace.
func (l *LmdbEmbed) BatchDeleteKV(keys [][]byte) error {
	if len(keys) == 0 {
		return nil
	}

	return l.env.Update(func(txn *lmdb.Txn) error {
		for _, key := range keys {
			typedKey := KeyKV(key)
			err := txn.Del(l.dataDB, typedKey, nil)

			if err != nil {
				if lmdb.IsNotFound(err) {
					continue // Skip non-existent keys
				}
				return err
			}
		}
		return nil
	})
}

// nolint: gocognit
func (l *LmdbEmbed) BatchDeleteLobChunks(keys [][]byte) error {
	if len(keys) == 0 {
		return nil
	}

	return l.env.Update(func(txn *lmdb.Txn) error {
		for _, key := range keys {
			typedKey := KeyBlobChunk(key, 0)
			storedValue, err := txn.Get(l.dataDB, typedKey)
			if err != nil {
				if lmdb.IsNotFound(err) {
					continue
				}
				return err
			}

			if len(storedValue) < 9 {
				return ErrInvalidChunkMetadata
			}
			chunkCount := binary.LittleEndian.Uint32(storedValue[1:5])

			for i := 1; i <= int(chunkCount); i++ {
				chunkKey := KeyBlobChunk(key, i)
				if err := txn.Del(l.dataDB, chunkKey, nil); err != nil {
					if !lmdb.IsNotFound(err) {
						return err
					}
				}
			}

			if err := txn.Del(l.dataDB, typedKey, nil); err != nil {
				if !lmdb.IsNotFound(err) {
					return err
				}
			}
		}
		return nil
	})
}

func (l *LmdbEmbed) recordOpResult(op string, start time.Time, err error) {
	if err != nil {
		l.mt.RecordError(op)
	} else {
		l.mt.RecordOp(op, start)
	}
}

// GetKV retrieves a value associated with a key within a specific namespace.
func (l *LmdbEmbed) GetKV(key []byte) ([]byte, error) {
	typedKey := KeyKV(key)
	startTime := time.Now()
	var value []byte
	err := l.env.View(func(txn *lmdb.Txn) error {
		storedValue, err := txn.Get(l.dataDB, typedKey)
		if err != nil {
			if lmdb.IsNotFound(err) {
				return ErrKeyNotFound
			}
			return err
		}

		value = make([]byte, len(storedValue))
		copy(value, storedValue)
		return nil
	})

	l.recordOpResult(OpGet, startTime, err)
	return value, err
}

// GetLOBChunks retrieves all chunks associated with a large object (LOB) key.
func (l *LmdbEmbed) GetLOBChunks(key []byte) ([][]byte, error) {
	typedKey := KeyBlobChunk(key, 0)
	startTime := time.Now()
	var value [][]byte

	err := l.env.View(func(txn *lmdb.Txn) error {
		storedValue, err := txn.Get(l.dataDB, typedKey)
		if err != nil {
			if lmdb.IsNotFound(err) {
				return ErrKeyNotFound
			}
			return err
		}

		if len(storedValue) < 9 {
			return ErrInvalidChunkMetadata
		}

		chunkCount := binary.LittleEndian.Uint32(storedValue[1:5])
		value = make([][]byte, chunkCount)
		for i := 1; i <= int(chunkCount); i++ {
			chunkKey := KeyBlobChunk(key, i)
			chunkValue, err := txn.Get(l.dataDB, chunkKey)
			if err != nil {
				if lmdb.IsNotFound(err) {
					return fmt.Errorf("chunk %d missing", i)
				}
				return err
			}

			valueCopy := make([]byte, len(chunkValue))
			copy(valueCopy, chunkValue)
			value[i-1] = valueCopy
		}
		return nil
	})

	l.recordOpResult(OpGet, startTime, err)
	return value, err
}

// GetCell retrieves the value of a single cell (row, column) from the wide-column store.
func (l *LmdbEmbed) GetCell(rowKey []byte, columnName string) ([]byte, error) {
	var value []byte
	err := l.env.View(func(txn *lmdb.Txn) error {
		key := KeyColumn(rowKey, unsafeStringToBytes(columnName))
		gotValue, err := txn.Get(l.dataDB, key)
		if err != nil {
			if lmdb.IsNotFound(err) {
				return ErrKeyNotFound
			}
			return err
		}
		value = make([]byte, len(gotValue))
		copy(value, gotValue)
		return nil
	})
	return value, err
}

// GetCells fetches the values of multiple columns (cells) from a given row.
func (l *LmdbEmbed) GetCells(rowKey []byte, columns []string) (map[string][]byte, error) {
	result := make(map[string][]byte, len(columns))
	err := l.env.View(func(txn *lmdb.Txn) error {
		for _, column := range columns {
			key := KeyColumn(rowKey, unsafeStringToBytes(column))
			gotValue, err := txn.Get(l.dataDB, key)
			if err != nil {
				if lmdb.IsNotFound(err) {
					continue // skip missing columns
				}
				return err
			}
			valueCopy := make([]byte, len(gotValue))
			copy(valueCopy, gotValue)
			result[column] = valueCopy
		}
		return nil
	})
	return result, err
}

// ScanRowCells returns all the columns for the given row. If a filter func is provided,
// it returns only those columns whose keys match the predicate.
func (l *LmdbEmbed) ScanRowCells(rowKey []byte, filter func(columnKey []byte) bool) (map[string][]byte, error) {
	prefix := RowKey(rowKey)

	startTime := time.Now()

	if filter == nil {
		filter = func(columnKey []byte) bool { return true }
	}

	var entries map[string][]byte

	err := l.env.View(func(txn *lmdb.Txn) error {
		tmp := make(map[string][]byte)
		if err := l.getColumns(txn, prefix, filter, tmp); err != nil {
			return err
		}
		entries = tmp
		return nil
	})

	l.recordOpResult(OpGet, startTime, err)
	return entries, err
}

func (l *LmdbEmbed) getColumns(
	txn *lmdb.Txn,
	prefix []byte,
	filter func([]byte) bool,
	entries map[string][]byte,
) error {
	cursor, err := txn.OpenCursor(l.dataDB)
	if err != nil {
		return err
	}
	defer cursor.Close()

	// http://www.lmdb.tech/doc/group__mdb.html#ga1206b2af8b95e7f6b0ef6b28708c9127
	// MDB_SET_RANGE
	// Position at first key greater than or equal to specified key.
	k, v, err := cursor.Get(prefix, nil, lmdb.SetRange)
	if err != nil {
		if lmdb.IsNotFound(err) {
			return ErrKeyNotFound
		}
		return err
	}

	for {
		if !bytes.HasPrefix(k, prefix) {
			break
		}

		columnKey := bytes.TrimPrefix(k, prefix)
		if filter(columnKey) {
			valCopy := make([]byte, len(v))
			copy(valCopy, v)
			entries[string(columnKey)] = valCopy
		}

		k, v, err = cursor.Get(nil, nil, lmdb.Next)
		// at the boundary condition, this will give
		// MDB_NOTFOUND
		// which will get returned if not returned from here.
		if lmdb.IsNotFound(err) {
			break
		} else if err != nil {
			return err
		}
	}

	if len(entries) == 0 {
		return ErrKeyNotFound
	}

	return nil
}

func (l *LmdbEmbed) Snapshot(w io.Writer) error {
	startTime := time.Now()
	defer func() {
		l.mt.RecordSnapshot(startTime)
	}()
	bw := bufio.NewWriter(w)
	defer bw.Flush()

	return l.env.View(func(txn *lmdb.Txn) error {
		cursor, err := txn.OpenCursor(l.dataDB)
		if err != nil {
			return fmt.Errorf("failed to open cursor: %w", err)
		}
		defer cursor.Close()

		var bytesWritten int

		// Iterate through all pages
		for {
			key, val, err := cursor.Get(nil, nil, lmdb.Next)
			if lmdb.IsNotFound(err) {
				break
			}
			if err != nil {
				return fmt.Errorf("cursor iteration failed: %w", err)
			}
			keyLen := len(key)
			valLen := len(val)
			entrySize := 4 + keyLen + 4 + valLen

			buffer := make([]byte, entrySize)

			offset := 0
			binary.LittleEndian.PutUint32(buffer[offset:], uint32(keyLen))
			offset += 4
			bytesWritten = 4
			copy(buffer[offset:], key)
			offset += keyLen
			bytesWritten += len(key)

			binary.LittleEndian.PutUint32(buffer[offset:], uint32(valLen))
			offset += 4
			bytesWritten += 4
			copy(buffer[offset:], val)
			bytesWritten += len(val)

			if _, err := bw.Write(buffer[:bytesWritten]); err != nil {
				return fmt.Errorf("failed to write to snapshot: %w", err)
			}
		}

		return nil
	})
}

// Restore restores an LMDB database from a reader.
func (l *LmdbEmbed) Restore(r io.Reader) error {
	return l.env.Update(func(txn *lmdb.Txn) error {
		br := bufio.NewReader(r)

		for {
			var keyLen uint32
			if err := binary.Read(br, binary.LittleEndian, &keyLen); err != nil {
				if err == io.EOF {
					break // End of file
				}
				return fmt.Errorf("failed to read key length: %w", err)
			}

			key := make([]byte, keyLen)
			if _, err := io.ReadFull(br, key); err != nil {
				return fmt.Errorf("failed to read key: %w", err)
			}

			var valLen uint32
			if err := binary.Read(br, binary.LittleEndian, &valLen); err != nil {
				return fmt.Errorf("failed to read value length: %w", err)
			}

			value := make([]byte, valLen)
			if _, err := io.ReadFull(br, value); err != nil {
				return fmt.Errorf("failed to read value: %w", err)
			}

			if err := txn.Put(l.dataDB, key, value, 0); err != nil {
				return fmt.Errorf("failed to insert key-value pair: %w", err)
			}
		}

		return nil
	})
}

func (l *LmdbEmbed) StoreMetadata(key []byte, value []byte) error {
	return l.env.Update(func(txn *lmdb.Txn) error {
		return txn.Put(l.metaDB, key, value, 0)
	})
}

func (l *LmdbEmbed) RetrieveMetadata(key []byte) ([]byte, error) {
	var value []byte
	err := l.env.View(func(txn *lmdb.Txn) error {
		data, err := txn.Get(l.metaDB, key)
		if lmdb.IsNotFound(err) {
			return ErrKeyNotFound
		}
		if err != nil {
			return err
		}
		value = append([]byte(nil), data...)
		return nil
	})
	return value, err
}
