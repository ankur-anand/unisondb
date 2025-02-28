package kvdb

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"log/slog"
	"os"
	"time"

	"github.com/PowerDNS/lmdb-go/lmdb"
	"github.com/hashicorp/go-metrics"
)

var (
	lmdbSetMetricKeyTotal          = append(packageKey, []string{"lmdb", "set", "total"}...)
	lmdbGetMetricKeyTotal          = append(packageKey, []string{"lmdb", "get", "total"}...)
	lmdbDeleteMetricKeyTotal       = append(packageKey, []string{"lmdb", "delete", "total"}...)
	lmdbSetMetricKeyLatency        = append(packageKey, []string{"lmdb", "set", "durations", "seconds"}...)
	lmdbGetMetricKeyLatency        = append(packageKey, []string{"lmdb", "get", "durations", "seconds"}...)
	lmdbDeleteMetricKeyLatency     = append(packageKey, []string{"lmdb", "delete", "durations", "seconds"}...)
	lmdbSetChunksMetricKeyTotal    = append(packageKey, []string{"lmdb", "set", "chunks", "total"}...)
	lmdbSetChunksMetricsKeyLatency = append(packageKey, []string{"lmdb", "set", "chunks", "durations", "seconds"}...)
	lmdbSetManyMetricKeyTotal      = append(packageKey, []string{"lmdb", "set", "many", "total"}...)
	lmdbSetManyMetricsLatency      = append(packageKey, []string{"lmdb", "set", "many", "durations", "seconds"}...)
	lmdbDeleteManyMetricKeyTotal   = append(packageKey, []string{"lmdb", "delete", "many", "total"}...)
	lmdbDeleteManyMetricKeyLatency = append(packageKey, []string{"lmdb", "delete", "many", "durations", "seconds"}...)
	lmdbSnapshotMetricKeyTotal     = append(packageKey, []string{"lmdb", "snapshot", "total"}...)
	lmdbSnapshotMetricsKeyLatency  = append(packageKey, []string{"lmdb", "snapshot", "durations", "seconds"}...)
)

// LmdbEmbed stores an initialized lmdb environment.
// http://www.lmdb.tech/doc/group__mdb.html
type LmdbEmbed struct {
	env       *lmdb.Env
	namespace []byte
	label     []metrics.Label
	db        lmdb.DBI
}

// FSync Call the underlying Fsync.
func (l *LmdbEmbed) FSync() error {
	return l.env.Sync(true)
}

// NewLmdb returns an initialized Lmdb Env with the provided configuration Parameter.
func NewLmdb(conf Config) (*LmdbEmbed, error) {
	// Create directory if it doesn't exist
	if err := os.MkdirAll(conf.Path, 0755); err != nil {
		return nil, fmt.Errorf("failed to create directory %s: %w", conf.Path, err)
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

	err = env.Open(conf.Path, lmdb.Create|lmdb.NoReadahead, 0644)
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
		slog.Warn("Cleared reader slots from dead processes", "stale_readers", staleReaders)
	}

	var db lmdb.DBI
	err = env.Update(func(txn *lmdb.Txn) error {
		var err error
		db, err = txn.OpenDBI(conf.Namespace, lmdb.Create)
		return err
	})
	if err != nil {
		return nil, err
	}

	// created for storing system metadata.
	err = env.Update(func(txn *lmdb.Txn) error {
		var err error
		db, err = txn.OpenDBI(sysBucketMetaData, lmdb.Create)
		return err
	})
	if err != nil {
		return nil, err
	}

	l := []metrics.Label{{Name: "namespace", Value: conf.Namespace}}
	return &LmdbEmbed{env: env, db: db, namespace: []byte(conf.Namespace), label: l}, nil
}

func (l *LmdbEmbed) Close() error {
	return l.env.Close()
}

// Set associates a value with a key within a specific namespace.
func (l *LmdbEmbed) Set(key []byte, value []byte) error {
	metrics.IncrCounterWithLabels(lmdbSetMetricKeyTotal, 1, l.label)
	startTime := time.Now()
	defer func() {
		metrics.MeasureSinceWithLabels(lmdbSetMetricKeyLatency, startTime, l.label)
	}()

	return l.env.Update(func(txn *lmdb.Txn) error {
		storedValue := append([]byte{FullValueFlag}, value...)
		err := txn.Put(l.db, key, storedValue, 0)
		if err != nil {
			return err
		}
		return nil
	})
}

func (l *LmdbEmbed) SetMany(keys [][]byte, values [][]byte) error {
	if len(keys) != len(values) {
		return fmt.Errorf("keys and values length mismatch: keys=%d values=%d", len(keys), len(values))
	}

	metrics.IncrCounterWithLabels(lmdbSetManyMetricKeyTotal, 1, l.label)
	startTime := time.Now()
	defer func() {
		metrics.MeasureSinceWithLabels(lmdbSetManyMetricsLatency, startTime, l.label)
	}()

	maxValueSize := 0
	for _, v := range values {
		if len(v) > maxValueSize {
			maxValueSize = len(v)
		}
	}
	buffer := make([]byte, 0, maxValueSize+1) // +1 for FullValueFlag

	return l.env.Update(func(txn *lmdb.Txn) error {
		for i, key := range keys {
			buffer = buffer[:0]
			buffer = append(buffer, FullValueFlag)
			buffer = append(buffer, values[i]...)

			if err := txn.Put(l.db, key, buffer, 0); err != nil {
				return err
			}
		}
		return nil
	})
}

// SetChunks stores a value that has been split into chunks, associating them with a single key.
func (l *LmdbEmbed) SetChunks(key []byte, chunks [][]byte, checksum uint32) error {
	if len(chunks) == 0 {
		return errors.New("empty chunks array")
	}

	metrics.IncrCounterWithLabels(lmdbSetChunksMetricKeyTotal, 1, l.label)
	startTime := time.Now()
	defer func() {
		metrics.MeasureSinceWithLabels(lmdbSetChunksMetricsKeyLatency, startTime, l.label)
	}()

	metaData := make([]byte, 9)
	metaData[0] = ChunkedValueFlag
	binary.LittleEndian.PutUint32(metaData[1:], uint32(len(chunks)))
	binary.LittleEndian.PutUint32(metaData[5:], checksum)

	return l.env.Update(func(txn *lmdb.Txn) error {
		// existing chunks and delete them
		storedValue, err := txn.Get(l.db, key)
		if err == nil && len(storedValue) > 0 && storedValue[0] == ChunkedValueFlag {
			if len(storedValue) < 9 {
				return fmt.Errorf("invalid chunk metadata for key %s: %w", string(key), ErrInvalidChunkMetadata)
			}

			oldChunkCount := binary.LittleEndian.Uint32(storedValue[1:5])
			for i := uint32(0); i < oldChunkCount; i++ {
				chunkKey := fmt.Sprintf("%s_chunk_%d", key, i)
				if err := txn.Del(l.db, []byte(chunkKey), nil); err != nil && !lmdb.IsNotFound(err) {
					return err
				}
			}
		}

		// Store chunks
		for i, chunk := range chunks {
			chunkKey := fmt.Sprintf("%s_chunk_%d", key, i)
			err := txn.Put(l.db, []byte(chunkKey), chunk, 0)
			if err != nil {
				return err
			}
		}

		// Store metadata
		if err := txn.Put(l.db, key, metaData, 0); err != nil {
			return err
		}

		return nil
	})
}

// Delete deletes a value with a key within a specific namespace.
func (l *LmdbEmbed) Delete(key []byte) error {
	metrics.IncrCounterWithLabels(lmdbDeleteMetricKeyTotal, 1, l.label)
	startTime := time.Now()
	defer func() {
		metrics.MeasureSinceWithLabels(lmdbDeleteMetricKeyLatency, startTime, l.label)
	}()

	return l.env.Update(func(txn *lmdb.Txn) error {
		storedValue, err := txn.Get(l.db, key)

		if lmdb.IsNotFound(err) {
			return nil
		}

		if err != nil {
			return err
		}

		flag := storedValue[0]
		switch flag {
		case FullValueFlag:
			return txn.Del(l.db, key, nil)
		case ChunkedValueFlag:
			if len(storedValue) < 9 {
				return ErrInvalidChunkMetadata
			}
			chunkCount := binary.LittleEndian.Uint32(storedValue[1:5])
			for i := 0; i < int(chunkCount); i++ {
				chunkKey := fmt.Sprintf("%s_chunk_%d", key, i)
				if err := txn.Del(l.db, []byte(chunkKey), nil); err != nil {
					return err
				}
			}
			return txn.Del(l.db, key, nil)
		}
		return ErrInvalidDataFormat
	})
}

// DeleteMany deletes multiple values with corresponding keys within a namespace.
func (l *LmdbEmbed) DeleteMany(keys [][]byte) error {
	if len(keys) == 0 {
		return nil
	}

	metrics.IncrCounterWithLabels(lmdbDeleteManyMetricKeyTotal, 1, l.label)
	startTime := time.Now()
	defer func() {
		metrics.MeasureSinceWithLabels(lmdbDeleteManyMetricKeyLatency, startTime, l.label)
	}()

	return l.env.Update(func(txn *lmdb.Txn) error {
		for _, key := range keys {
			storedValue, err := txn.Get(l.db, key)
			if err != nil {
				if lmdb.IsNotFound(err) {
					continue // Skip non-existent keys
				}
				return err
			}

			flag := storedValue[0]
			switch flag {
			case FullValueFlag:
				if err := txn.Del(l.db, key, nil); err != nil {
					return err
				}

			case ChunkedValueFlag:
				if len(storedValue) < 9 {
					return fmt.Errorf("invalid chunk metadata for key %s: %w", string(key), ErrInvalidChunkMetadata)
				}

				if err := l.deleteChunk(key, storedValue, txn); err != nil {
					return err
				}
			default:
				return fmt.Errorf("invalid data format for key %s: %w", string(key), ErrInvalidDataFormat)
			}
		}
		return nil
	})
}

func (l *LmdbEmbed) deleteChunk(key []byte, storedValue []byte, txn *lmdb.Txn) error {
	chunkCount := binary.LittleEndian.Uint32(storedValue[1:5])

	// Delete all chunks
	for i := 0; i < int(chunkCount); i++ {
		chunkKey := fmt.Sprintf("%s_chunk_%d", key, i)
		if err := txn.Del(l.db, []byte(chunkKey), nil); err != nil && !lmdb.IsNotFound(err) {
			return err
		}
	}

	// Delete the main key
	if err := txn.Del(l.db, key, nil); err != nil {
		return err
	}

	return nil
}

// Get retrieves a value associated with a key within a specific namespace.
func (l *LmdbEmbed) Get(key []byte) ([]byte, error) {
	metrics.IncrCounterWithLabels(lmdbGetMetricKeyTotal, 1, l.label)
	startTime := time.Now()
	defer func() {
		metrics.MeasureSinceWithLabels(lmdbGetMetricKeyLatency, startTime, l.label)
	}()

	var value []byte
	err := l.env.View(func(txn *lmdb.Txn) error {
		storedValue, err := txn.Get(l.db, key)
		if err != nil {
			if lmdb.IsNotFound(err) {
				return ErrKeyNotFound
			}
			return fmt.Errorf("failed to get key %s: %w", string(key), err)
		}

		if len(storedValue) == 0 {
			return ErrRecordCorrupted
		}

		flag := storedValue[0]
		switch flag {
		case FullValueFlag:
			value = make([]byte, len(storedValue[1:]))
			copy(value, storedValue[1:])
			return nil

		case ChunkedValueFlag:
			if len(storedValue) < 9 {
				return fmt.Errorf("invalid chunk metadata for key %s: %w", string(key), ErrInvalidChunkMetadata)
			}

			// Parse chunk metadata
			chunkCount := binary.LittleEndian.Uint32(storedValue[1:5])
			storedChecksum := binary.LittleEndian.Uint32(storedValue[5:9])
			var calculatedChecksum uint32

			fullValue := bytes.NewBuffer(make([]byte, 0, chunkCount*1024))

			for i := uint32(0); i < chunkCount; i++ {
				chunkKey := fmt.Sprintf("%s_chunk_%d", key, i)
				chunkData, err := txn.Get(l.db, []byte(chunkKey))
				if err != nil {
					if lmdb.IsNotFound(err) {
						return fmt.Errorf("chunk %d missing for key %s", i, string(key))
					}
					return err
				}

				calculatedChecksum = crc32.Update(calculatedChecksum, crc32.IEEETable, chunkData)
				fullValue.Write(chunkData)
			}

			if calculatedChecksum != storedChecksum {
				return fmt.Errorf("checksum mismatch for key %s: %w", string(key), ErrRecordCorrupted)
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

func (l *LmdbEmbed) Snapshot(w io.Writer) error {
	startTime := time.Now()
	metrics.IncrCounterWithLabels(lmdbSnapshotMetricKeyTotal, 1, l.label)
	defer func() {
		metrics.MeasureSinceWithLabels(lmdbSnapshotMetricsKeyLatency, startTime, l.label)
	}()
	bw := bufio.NewWriter(w)
	defer bw.Flush()

	return l.env.View(func(txn *lmdb.Txn) error {
		stat, err := l.env.Stat()
		if err != nil {
			return fmt.Errorf("failed to get database stats: %w", err)
		}

		cursor, err := txn.OpenCursor(l.db)
		if err != nil {
			return fmt.Errorf("failed to open cursor: %w", err)
		}
		defer cursor.Close()

		buffer := make([]byte, stat.PSize)
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

			binary.LittleEndian.PutUint32(buffer[0:4], uint32(len(key)))
			bytesWritten = 4
			copy(buffer[bytesWritten:], key)
			bytesWritten += len(key)

			binary.LittleEndian.PutUint32(buffer[bytesWritten:bytesWritten+4], uint32(len(val)))
			bytesWritten += 4
			copy(buffer[bytesWritten:], val)
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

			if err := txn.Put(l.db, key, value, 0); err != nil {
				return fmt.Errorf("failed to insert key-value pair: %w", err)
			}
		}

		return nil
	})
}

func (l *LmdbEmbed) StoreMetadata(key []byte, value []byte) error {
	return l.env.Update(func(txn *lmdb.Txn) error {
		metaDB, err := txn.OpenDBI(sysBucketMetaData, lmdb.Create)
		if err != nil {
			if !lmdb.IsNotFound(err) {
				return err
			}
		}
		return txn.Put(metaDB, key, value, 0)
	})
}

func (l *LmdbEmbed) RetrieveMetadata(key []byte) ([]byte, error) {
	var value []byte
	err := l.env.View(func(txn *lmdb.Txn) error {
		metaDB, err := txn.OpenDBI(sysBucketMetaData, 0)
		if err != nil {
			return err
		}

		data, err := txn.Get(metaDB, key)
		if err != nil && !lmdb.IsNotFound(err) {
			return err
		}

		if lmdb.IsNotFound(err) {
			return ErrKeyNotFound
		}

		value = append([]byte(nil), data...)
		return nil
	})
	return value, err
}
