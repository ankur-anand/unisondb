package kvdrivers

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"runtime"
	"time"

	"github.com/PowerDNS/lmdb-go/lmdb"
	"github.com/hashicorp/go-metrics"
)

// LMDBTxnQueue encapsulates LMDB Transactions and provides an API that allow multiple
// operation to be put in queue that is supposed to be Executed in the same orders.
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
	stats           *TxnStats
}

// NewTxnQueue returns an initialized LMDBTxnQueue for Batch API queuing and commit.
func (l *LmdbEmbed) NewTxnQueue(maxBatchSize int) *LMDBTxnQueue {
	return &LMDBTxnQueue{
		env:          l.env,
		db:           l.db,
		maxBatchSize: maxBatchSize,
		label:        l.label,
		opsQueue:     make([]func(*lmdb.Txn) error, 0, maxBatchSize),
		stats:        &TxnStats{},
	}
}

// BatchPut queue one or more key-value pairs inside a transaction that will be commited upon Commit or max batch size
// threshold breach. If the value exists, it replaces the existing value, else sets a new value associated with the key.
// Caller need to take care to not upsert the row column value, else there is no guarantee for consistency in storage.
func (lq *LMDBTxnQueue) BatchPut(keys, values [][]byte) error {
	if lq.err != nil {
		return lq.err
	}

	for i, key := range keys {
		// append the set function to queue for processing
		lq.opsQueue = append(lq.opsQueue, func(t *lmdb.Txn) error {
			storedValue := make([]byte, 1+len(values[i]))
			storedValue[0] = kvValue
			copy(storedValue[1:], values[i])
			err := t.Put(lq.db, key, storedValue, 0)
			if err != nil {
				return err
			}
			lq.putOps++
			lq.entriesModified++

			return nil
		})
	}

	if len(lq.opsQueue) >= lq.maxBatchSize {
		lq.err = lq.flushBatch()
	}

	return lq.err
}

// BatchDelete queue one or more key inside a transaction for deletion. It doesn't delete rows or columns.
// Deletion happens either when Commit is called or max batch size threshold is reached.
// Caller need to call BatchDeleteRows or BatchDeleteRowsColumns to work with rows and columns type value.
func (lq *LMDBTxnQueue) BatchDelete(keys [][]byte) error {
	if lq.err != nil {
		return lq.err
	}

	for _, key := range keys {
		lq.opsQueue = append(lq.opsQueue, func(txn *lmdb.Txn) error {
			storedValue, err := txn.Get(lq.db, key)
			// if not found continue to next key.
			if lmdb.IsNotFound(err) {
				return nil
			}

			if err != nil {
				return err
			}

			lq.deleteOps++
			lq.entriesModified++
			flag := storedValue[0]
			switch flag {
			case kvValue:
				return txn.Del(lq.db, key, nil)
			case chunkedValue:
				if len(storedValue) < 9 {
					return ErrInvalidChunkMetadata
				}
				chunkCount := binary.LittleEndian.Uint32(storedValue[1:5])
				for i := 0; i < int(chunkCount); i++ {
					lq.entriesModified++
					chunkKey := fmt.Sprintf("%s_chunk_%d", key, i)
					if err := txn.Del(lq.db, []byte(chunkKey), nil); err != nil {
						return err
					}
				}
				return txn.Del(lq.db, key, nil)
			}
			return ErrInvalidOpsForValueType
		})
	}

	if len(lq.opsQueue) >= lq.maxBatchSize {
		lq.err = lq.flushBatch()
	}

	return lq.err
}

// SetChunks stores a value that has been split into chunks, associating them with a single key.
// It queues the operation in Txn, which is flushed when either max size threshold is reached or during commit call.
func (lq *LMDBTxnQueue) SetChunks(key []byte, chunks [][]byte, checksum uint32) error {
	if lq.err != nil {
		return lq.err
	}

	metaData := make([]byte, 9)
	metaData[0] = chunkedValue
	binary.LittleEndian.PutUint32(metaData[1:], uint32(len(chunks)))
	binary.LittleEndian.PutUint32(metaData[5:], checksum)

	lq.opsQueue = append(lq.opsQueue, func(txn *lmdb.Txn) error {
		storedValue, err := txn.Get(lq.db, key)

		if err == nil && len(storedValue) > 0 && storedValue[0] == chunkedValue {
			if len(storedValue) < 9 {
				return fmt.Errorf("invalid chunk metadata for key %s: %w", string(key), ErrInvalidChunkMetadata)
			}

			oldChunkCount := binary.LittleEndian.Uint32(storedValue[1:5])
			for i := uint32(0); i < oldChunkCount; i++ {
				chunkKey := fmt.Sprintf("%s_chunk_%d", key, i)
				if err := txn.Del(lq.db, []byte(chunkKey), nil); err != nil && !lmdb.IsNotFound(err) {
					return err
				}
			}
		}

		lq.putOps++
		lq.entriesModified++
		// Store chunks
		for i, chunk := range chunks {
			lq.entriesModified++
			chunkKey := fmt.Sprintf("%s_chunk_%d", key, i)
			err := txn.Put(lq.db, []byte(chunkKey), chunk, 0)
			if err != nil {
				return err
			}
		}

		// Store metadata
		if err := txn.Put(lq.db, key, metaData, 0); err != nil {
			return err
		}

		return nil
	})

	// We are saving chunk ASAP as this is already LOB, divided into chunks.
	lq.err = lq.flushBatch()

	return lq.err
}

// BatchPutRowColumns queues updates or inserts of multiple rows with the provided column entries.
// Each row in `rowKeys` maps to a set of columns in `columnEntriesPerRow`.
func (lq *LMDBTxnQueue) BatchPutRowColumns(rowKeys [][]byte, columnEntriesPerRow []map[string][]byte) error {
	if lq.err != nil {
		return lq.err
	}

	if len(rowKeys) != len(columnEntriesPerRow) {
		lq.err = ErrInvalidArguments
		return lq.err
	}

	for i, rowKey := range rowKeys {
		lq.opsQueue = append(lq.opsQueue, func(txn *lmdb.Txn) error {
			columnEntries := columnEntriesPerRow[i]
			if len(columnEntries) == 0 {
				return nil
			}

			lq.putOps++
			lq.entriesModified++
			pKey := append(append([]byte(nil), rowKey...), rowKeySeperator...)
			entries := appendRowKeyToColumnKey(pKey, columnEntries)

			for entryKey, entry := range entries {
				lq.entriesModified++
				if err := txn.Put(lq.db, []byte(entryKey), entry, 0); err != nil {
					return err
				}
			}

			if err := txn.Put(lq.db, pKey, []byte{rowColumnValue}, 0); err != nil {
				return err
			}

			return nil
		})
	}

	if len(lq.opsQueue) >= lq.maxBatchSize {
		lq.err = lq.flushBatch()
	}

	return lq.err
}

// BatchDeleteRowColumns queues deletes of multiple rows with the provided column entries.
// Each row in `rowKeys` maps to a set of columns in `columnEntriesPerRow`.
func (lq *LMDBTxnQueue) BatchDeleteRowColumns(rowKeys [][]byte, columnEntriesPerRow []map[string][]byte) error {
	if lq.err != nil {
		return lq.err
	}

	if len(rowKeys) != len(columnEntriesPerRow) {
		lq.err = ErrInvalidArguments
		return lq.err
	}

	for i, rowKey := range rowKeys {
		lq.opsQueue = append(lq.opsQueue, func(txn *lmdb.Txn) error {
			columnEntries := columnEntriesPerRow[i]
			if len(columnEntries) == 0 {
				return nil
			}

			lq.deleteOps++
			lq.entriesModified++
			pKey := append(append([]byte(nil), rowKey...), rowKeySeperator...)
			entries := appendRowKeyToColumnKey(pKey, columnEntries)

			for entryKey := range entries {
				if err := txn.Del(lq.db, []byte(entryKey), nil); err != nil {
					if !lmdb.IsNotFound(err) {
						return err
					}
					lq.entriesModified++
				}
			}

			if err := txn.Put(lq.db, pKey, []byte{rowColumnValue}, 0); err != nil {
				return err
			}
			return nil
		})
	}

	if len(lq.opsQueue) >= lq.maxBatchSize {
		lq.err = lq.flushBatch()
	}

	return lq.err
}

// BatchDeleteRows  queue deletes of the row and all it's associated Columns from the database.
func (lq *LMDBTxnQueue) BatchDeleteRows(rowKeys [][]byte) error {
	if lq.err != nil {
		return lq.err
	}

	for _, rowKey := range rowKeys {
		lq.opsQueue = append(lq.opsQueue, func(txn *lmdb.Txn) error {
			c, err := txn.OpenCursor(lq.db)
			if err != nil {
				return err
			}
			defer c.Close()
			lq.deleteOps++

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
				lq.entriesModified++
				k, _, err = c.Get(nil, nil, lmdb.Next)
			}

			if err != nil {
				if lmdb.IsNotFound(err) {
					return nil
				}
				return err
			}

			return nil
		})
	}

	if len(lq.opsQueue) >= lq.maxBatchSize {
		lq.err = lq.flushBatch()
	}

	return lq.err
}

func (lq *LMDBTxnQueue) flushBatch() error {
	if lq.err != nil || len(lq.opsQueue) == 0 {
		return lq.err
	}

	runtime.LockOSThread()
	defer runtime.UnlockOSThread()

	startTime := time.Now()

	metrics.IncrCounterWithLabels(mTxnFlushTotal, 1, lq.label)
	defer func() {
		metrics.MeasureSinceWithLabels(mTxnFlushLatency, startTime, lq.label)
	}()

	txn, err := lq.env.BeginTxn(nil, 0)
	if err != nil {
		return err
	}

	for _, op := range lq.opsQueue {
		if err := op(txn); err != nil {
			lq.err = err
			break
		}
	}

	if lq.err != nil {
		txn.Abort()
		return lq.err
	}

	// no errors occurred commit txn
	err = txn.Commit()

	if lq.err == nil {
		metrics.IncrCounterWithLabels(mTxnFlushBatchSize, float32(len(lq.opsQueue)), lq.label)
		metrics.IncrCounterWithLabels(mSetTotal, lq.putOps, lq.label)
		metrics.IncrCounterWithLabels(mDelTotal, lq.deleteOps, lq.label)
		metrics.IncrCounterWithLabels(mTxnEntriesModifiedTotal, lq.entriesModified, lq.label)
		lq.stats.EntriesModified += lq.entriesModified
		lq.stats.DeleteOps += lq.deleteOps
		lq.stats.PutOps += lq.putOps
		lq.opsQueue = nil
		lq.putOps = 0
		lq.deleteOps = 0
		lq.entriesModified = 0
	}
	return err
}

func (lq *LMDBTxnQueue) Stats() TxnStats {
	return *lq.stats
}

// Commit all the pending operation in queue.
func (lq *LMDBTxnQueue) Commit() error {
	if lq.err != nil {
		return lq.err
	}

	return lq.flushBatch()
}
