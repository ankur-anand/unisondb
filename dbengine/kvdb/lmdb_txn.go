package kvdb

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"runtime"
	"time"

	"github.com/PowerDNS/lmdb-go/lmdb"
	"github.com/hashicorp/go-metrics"
)

// LMDBTxnQueue encapsulates LMDB Transactions and provides an API that allow multiple
// operation to be chained in queue that is supposed to be Executed in the same orders.
// It flushes the batch automatically if configured max batch Size threshold is reached.
// Caller should call Commit in the end to finish any pending Txn not flushed via maxBatchSize.
// Single instance of Txn are not concurrent safe.
type LMDBTxnQueue struct {
	label           []metrics.Label
	db              lmdb.DBI
	env             *lmdb.Env
	err             error
	opsQueue        []func(*lmdb.Txn) error
	maxBatchSize    int
	entriesModified float32
	putOps          float32
	deleteOps       float32
}

// NewLMDBTxn returns a initialized LMDBTxnQueue for Batch API queuing and commit.
func (l *LmdbEmbed) NewLMDBTxn(maxBatchSize int) *LMDBTxnQueue {
	return &LMDBTxnQueue{
		env:          l.env,
		db:           l.db,
		maxBatchSize: maxBatchSize,
		opsQueue:     make([]func(*lmdb.Txn) error, 0, maxBatchSize),
	}
}

// BatchPut queue one or more key-value pairs inside a transaction that will be commited upon Commit or max batch size
// threshold breach. If the value exists, it replaces the existing value, else sets a new value associated with the key.
// Caller need to take care to not upsert the row column value, else there is no guarantee for consistency in storage.
func (lt *LMDBTxnQueue) BatchPut(keys, values [][]byte) *LMDBTxnQueue {
	if lt.err != nil {
		return lt
	}

	for i, key := range keys {
		// append the set function to queue for processing
		lt.opsQueue = append(lt.opsQueue, func(t *lmdb.Txn) error {

			storedValue := make([]byte, 1+len(values[i]))
			storedValue[0] = kvValue
			copy(storedValue[1:], values[i])
			err := t.Put(lt.db, key, storedValue, 0)
			if err != nil {
				return err
			}
			lt.putOps++
			lt.entriesModified++

			return nil
		})
	}

	return lt
}

// BatchDelete queue one or more key inside a transaction for deletion. It doesn't delete rows or columns.
// Deletion happens either when Commit is called or max batch size threshold is reached.
// Caller need to call BatchDeleteRows or BatchDeleteRowsColumns to work with rows and columns type value.
func (lt *LMDBTxnQueue) BatchDelete(keys [][]byte) *LMDBTxnQueue {
	if lt.err != nil {
		return lt
	}

	for _, key := range keys {
		lt.opsQueue = append(lt.opsQueue, func(txn *lmdb.Txn) error {

			storedValue, err := txn.Get(lt.db, key)
			// if not found continue to next key.
			if lmdb.IsNotFound(err) {
				return nil
			}

			if err != nil {
				return err
			}

			lt.deleteOps++
			lt.entriesModified++
			flag := storedValue[0]
			switch flag {
			case kvValue:
				return txn.Del(lt.db, key, nil)
			case chunkedValue:
				if len(storedValue) < 9 {
					return ErrInvalidChunkMetadata
				}
				chunkCount := binary.LittleEndian.Uint32(storedValue[1:5])
				for i := 0; i < int(chunkCount); i++ {
					lt.entriesModified++
					chunkKey := fmt.Sprintf("%s_chunk_%d", key, i)
					if err := txn.Del(lt.db, []byte(chunkKey), nil); err != nil {
						return err
					}
				}
				return txn.Del(lt.db, key, nil)
			}
			return ErrInvalidOpsForValueType
		})
	}
	return lt
}

// SetChunks stores a value that has been split into chunks, associating them with a single key.
// It queues the operation in Txn, which is flushed when either max size threshold is reached or during commit call.
func (lt *LMDBTxnQueue) SetChunks(key []byte, chunks [][]byte, checksum uint32) *LMDBTxnQueue {
	if lt.err != nil {
		return lt
	}

	metaData := make([]byte, 9)
	metaData[0] = chunkedValue
	binary.LittleEndian.PutUint32(metaData[1:], uint32(len(chunks)))
	binary.LittleEndian.PutUint32(metaData[5:], checksum)

	lt.opsQueue = append(lt.opsQueue, func(txn *lmdb.Txn) error {
		storedValue, err := txn.Get(lt.db, key)

		if err == nil && len(storedValue) > 0 && storedValue[0] == chunkedValue {
			if len(storedValue) < 9 {
				return fmt.Errorf("invalid chunk metadata for key %s: %w", string(key), ErrInvalidChunkMetadata)
			}

			oldChunkCount := binary.LittleEndian.Uint32(storedValue[1:5])
			for i := uint32(0); i < oldChunkCount; i++ {
				chunkKey := fmt.Sprintf("%s_chunk_%d", key, i)
				if err := txn.Del(lt.db, []byte(chunkKey), nil); err != nil && !lmdb.IsNotFound(err) {
					return err
				}
			}
		}

		lt.putOps++
		lt.entriesModified++
		// Store chunks
		for i, chunk := range chunks {
			lt.entriesModified++
			chunkKey := fmt.Sprintf("%s_chunk_%d", key, i)
			err := txn.Put(lt.db, []byte(chunkKey), chunk, 0)
			if err != nil {
				return err
			}
		}

		// Store metadata
		if err := txn.Put(lt.db, key, metaData, 0); err != nil {
			return err
		}

		return nil
	})

	return lt
}

// BatchPutRowColumns queues updates or inserts of multiple rows with the provided column entries.
// Each row in `rowKeys` maps to a set of columns in `columnEntriesPerRow`.
func (lt *LMDBTxnQueue) BatchPutRowColumns(rowKeys [][]byte, columnEntriesPerRow []map[string][]byte) *LMDBTxnQueue {
	if lt.err != nil {
		return lt
	}

	if len(rowKeys) != len(columnEntriesPerRow) {
		lt.err = ErrInvalidArguments
		return lt
	}

	for i, rowKey := range rowKeys {
		lt.opsQueue = append(lt.opsQueue, func(txn *lmdb.Txn) error {
			columnEntries := columnEntriesPerRow[i]
			if len(columnEntries) == 0 {
				return nil
			}

			lt.putOps++
			lt.entriesModified++
			pKey := append(append([]byte(nil), rowKey...), rowKeySeperator...)
			entries := appendRowKeyToColumnKey(pKey, columnEntries)

			for entryKey, entry := range entries {
				lt.entriesModified++
				if err := txn.Put(lt.db, []byte(entryKey), entry, 0); err != nil {
					return err
				}
			}

			if err := txn.Put(lt.db, pKey, []byte{rowColumnValue}, 0); err != nil {
				return err
			}

			return nil
		})
	}

	return lt
}

// BatchDeleteRowColumns queues deletes of multiple rows with the provided column entries.
// Each row in `rowKeys` maps to a set of columns in `columnEntriesPerRow`.
func (lt *LMDBTxnQueue) BatchDeleteRowColumns(rowKeys [][]byte, columnEntriesPerRow []map[string][]byte) *LMDBTxnQueue {
	if lt.err != nil {
		return lt
	}

	if len(rowKeys) != len(columnEntriesPerRow) {
		lt.err = ErrInvalidArguments
		return lt
	}

	for i, rowKey := range rowKeys {
		lt.opsQueue = append(lt.opsQueue, func(txn *lmdb.Txn) error {
			columnEntries := columnEntriesPerRow[i]
			if len(columnEntries) == 0 {
				return nil
			}

			lt.deleteOps++
			lt.entriesModified++
			pKey := append(append([]byte(nil), rowKey...), rowKeySeperator...)
			entries := appendRowKeyToColumnKey(pKey, columnEntries)

			for entryKey := range entries {
				if err := txn.Del(lt.db, []byte(entryKey), nil); err != nil {
					if !lmdb.IsNotFound(err) {
						return err
					}
					lt.entriesModified++
				}
			}

			if err := txn.Put(lt.db, pKey, []byte{rowColumnValue}, 0); err != nil {
				return err
			}
			return nil
		})
	}

	return lt
}

// BatchDeleteRows  queue deletes of the row and all it's associated Columns from the database.
func (lt *LMDBTxnQueue) BatchDeleteRows(rowKeys [][]byte) *LMDBTxnQueue {
	if lt.err != nil {
		return lt
	}

	for _, rowKey := range rowKeys {

		lt.opsQueue = append(lt.opsQueue, func(txn *lmdb.Txn) error {
			c, err := txn.OpenCursor(lt.db)
			if err != nil {
				return err
			}
			defer c.Close()
			lt.deleteOps++

			pKey := append(append([]byte(nil), rowKey...), rowKeySeperator...)
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
				lt.entriesModified++
				k, _, err = c.Get(nil, nil, lmdb.Next)
			}

			if err != nil {
				if errors.Is(err, lmdb.NotFound) {
					return nil
				}
				return err
			}

			return nil
		})
	}

	return lt
}

func (lt *LMDBTxnQueue) flushBatch() error {
	if lt.err != nil || len(lt.opsQueue) == 0 {
		return lt.err
	}

	runtime.LockOSThread()
	defer runtime.UnlockOSThread()

	startTime := time.Now()

	metrics.IncrCounterWithLabels(mTxnFlushTotal, 1, lt.label)
	defer func() {
		metrics.MeasureSinceWithLabels(mTxnFlushLatency, startTime, lt.label)
	}()

	txn, err := lt.env.BeginTxn(nil, 0)
	if err != nil {
		return err
	}

	for _, op := range lt.opsQueue {
		if err := op(txn); err != nil {
			lt.err = err
			break
		}
	}

	if lt.err != nil {
		txn.Abort()
		return lt.err
	}

	// no errors occurred commit txn
	err = txn.Commit()

	if lt.err == nil {
		metrics.IncrCounterWithLabels(mTxnFlushBatchSize, float32(len(lt.opsQueue)), lt.label)
		metrics.IncrCounterWithLabels(mSetTotal, lt.putOps, lt.label)
		metrics.IncrCounterWithLabels(mDelTotal, lt.deleteOps, lt.label)
		metrics.IncrCounterWithLabels(mTxnEntriesModifiedTotal, lt.entriesModified, lt.label)
		lt.opsQueue = nil
		lt.putOps = 0
		lt.deleteOps = 0
		lt.entriesModified = 0
	}
	return err
}

// Commit all the pending operation in queue.
func (lt *LMDBTxnQueue) Commit() error {
	if lt.err != nil {
		return lt.err
	}

	return lt.flushBatch()
}
