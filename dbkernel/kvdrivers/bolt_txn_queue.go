package kvdrivers

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"log/slog"
	"time"

	"github.com/PowerDNS/lmdb-go/lmdb"
	"github.com/hashicorp/go-metrics"
	"go.etcd.io/bbolt"
)

// BoltTxnQueue encapsulates Boltdb Transactions and provides an API that allow multiple
// operation to be put in queue that is supposed to be Executed in the same orders.
// It flushes the batch automatically if configured max batch Size threshold is reached.
// Caller should call Commit in the end to finish any pending Txn not flushed via maxBatchSize.
// Single instance of Txn are not concurrent safe.
type BoltTxnQueue struct {
	label           []metrics.Label
	err             error
	db              *bbolt.DB
	namespace       []byte
	opsQueue        []func(tx *bbolt.Bucket) error
	maxBatchSize    int
	entriesModified float32
	putOps          float32
	deleteOps       float32
	stats           *TxnStats
}

// NewTxnQueue returns an initialized BoltTxnQueue for Batch API queuing and commit.
func (b *BoltDBEmbed) NewTxnQueue(maxBatchSize int) *BoltTxnQueue {
	return &BoltTxnQueue{
		label:        b.label,
		err:          nil,
		maxBatchSize: maxBatchSize,
		opsQueue:     make([]func(bucket *bbolt.Bucket) error, 0, maxBatchSize),
		namespace:    b.namespace,
		db:           b.db,
		stats:        &TxnStats{},
	}
}

// BatchPut queue one or more key-value pairs inside a transaction that will be commited upon Commit or max batch size
// threshold breach. If the value exists, it replaces the existing value, else sets a new value associated with the key.
// Caller need to take care to not upsert the row column value, else there is no guarantee for consistency in storage.
func (bq *BoltTxnQueue) BatchPut(keys, values [][]byte) error {
	if bq.err != nil {
		return bq.err
	}

	for i, key := range keys {
		// append the set function to queue for processing
		bq.opsQueue = append(bq.opsQueue, func(txn *bbolt.Bucket) error {
			storedValue := make([]byte, 1+len(values[i]))
			storedValue[0] = kvValue
			copy(storedValue[1:], values[i])

			err := txn.Put(key, storedValue)
			if err != nil {
				return err
			}
			bq.putOps++
			bq.entriesModified++

			return nil
		})
	}

	if len(bq.opsQueue) >= bq.maxBatchSize {
		bq.err = bq.flushBatch()
	}

	return bq.err
}

// BatchDelete queue one or more key inside a transaction for deletion. It doesn't delete rows or columns.
// Deletion happens either when Commit is called or max batch size threshold is reached.
// Caller need to call BatchDeleteRows or BatchDeleteRowsColumns to work with rows and columns type value.
func (bq *BoltTxnQueue) BatchDelete(keys [][]byte) error {
	if bq.err != nil {
		return bq.err
	}

	for _, key := range keys {
		bq.opsQueue = append(bq.opsQueue, func(txn *bbolt.Bucket) error {
			storedValue := txn.Get(key)
			if storedValue == nil {
				return nil
			}

			bq.deleteOps++
			bq.entriesModified++
			flag := storedValue[0]
			switch flag {
			case kvValue:
				return txn.Delete(key)
			case chunkedValue:
				if len(storedValue) < 9 {
					return ErrInvalidChunkMetadata
				}
				chunkCount := binary.LittleEndian.Uint32(storedValue[1:5])
				for i := 0; i < int(chunkCount); i++ {
					bq.entriesModified++
					chunkKey := fmt.Sprintf("%s_chunk_%d", key, i)
					if err := txn.Delete([]byte(chunkKey)); err != nil {
						return err
					}
				}
				return txn.Delete(key)
			}
			return ErrInvalidOpsForValueType
		})
	}

	if len(bq.opsQueue) >= bq.maxBatchSize {
		bq.err = bq.flushBatch()
	}

	return bq.err
}

// SetChunks stores a value that has been split into chunks, associating them with a single key.
// It queues the operation in Txn, which is flushed when either max size threshold is reached or during commit call.
func (bq *BoltTxnQueue) SetChunks(key []byte, chunks [][]byte, checksum uint32) error {
	if bq.err != nil {
		return bq.err
	}

	bq.opsQueue = append(bq.opsQueue, func(txn *bbolt.Bucket) error {
		// get last stored for keys, if present.
		// older chunk needs to deleted for not leaking the space.
		storedValue := txn.Get(key)
		if storedValue != nil && storedValue[0] == chunkedValue {
			if len(storedValue) < 9 {
				return ErrInvalidChunkMetadata
			}
			chunkCount := binary.LittleEndian.Uint32(storedValue[1:5])

			for i := 0; i < int(chunkCount); i++ {
				chunkKey := fmt.Sprintf("%s_chunk_%d", key, i)
				if err := txn.Delete([]byte(chunkKey)); err != nil {
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
		if err := txn.Put(key, metaData); err != nil {
			return err
		}

		// individual chunk
		for i, chunk := range chunks {
			chunkKey := fmt.Sprintf("%s_chunk_%d", key, i)
			if err := txn.Put([]byte(chunkKey), chunk); err != nil {
				return err
			}
		}

		return nil
	})

	if len(bq.opsQueue) >= bq.maxBatchSize {
		bq.err = bq.flushBatch()
	}

	return bq.err
}

// BatchPutRowColumns queues updates or inserts of multiple rows with the provided column entries.
// Each row in `rowKeys` maps to a set of columns in `columnEntriesPerRow`.
func (bq *BoltTxnQueue) BatchPutRowColumns(rowKeys [][]byte, columnEntriesPerRow []map[string][]byte) error {
	if bq.err != nil {
		return bq.err
	}

	if len(rowKeys) != len(columnEntriesPerRow) {
		bq.err = ErrInvalidArguments
		return bq.err
	}

	for i, rowKey := range rowKeys {
		bq.opsQueue = append(bq.opsQueue, func(txn *bbolt.Bucket) error {
			columnEntries := columnEntriesPerRow[i]
			if len(columnEntries) == 0 {
				return nil
			}

			bq.putOps++
			bq.entriesModified++
			pKey := append(append([]byte(nil), rowKey...), rowKeySeperator...)
			entries := appendRowKeyToColumnKey(pKey, columnEntries)

			for entryKey, entry := range entries {
				bq.entriesModified++
				if err := txn.Put([]byte(entryKey), entry); err != nil {
					return err
				}
			}

			if err := txn.Put(pKey, []byte{rowColumnValue}); err != nil {
				return err
			}

			return nil
		})
	}

	if len(bq.opsQueue) >= bq.maxBatchSize {
		bq.err = bq.flushBatch()
	}

	return bq.err
}

// BatchDeleteRowColumns queues deletes of multiple rows with the provided column entries.
// Each row in `rowKeys` maps to a set of columns in `columnEntriesPerRow`.
func (bq *BoltTxnQueue) BatchDeleteRowColumns(rowKeys [][]byte, columnEntriesPerRow []map[string][]byte) error {
	if bq.err != nil {
		return bq.err
	}

	if len(rowKeys) != len(columnEntriesPerRow) {
		bq.err = ErrInvalidArguments
		return bq.err
	}

	for i, rowKey := range rowKeys {
		bq.opsQueue = append(bq.opsQueue, func(txn *bbolt.Bucket) error {
			columnEntries := columnEntriesPerRow[i]
			if len(columnEntries) == 0 {
				return nil
			}

			bq.deleteOps++
			bq.entriesModified++
			pKey := append(append([]byte(nil), rowKey...), rowKeySeperator...)
			entries := appendRowKeyToColumnKey(pKey, columnEntries)

			for entryKey := range entries {
				if err := txn.Delete([]byte(entryKey)); err != nil {
					if !lmdb.IsNotFound(err) {
						return err
					}
					bq.entriesModified++
				}
			}

			if err := txn.Put(pKey, []byte{rowColumnValue}); err != nil {
				return err
			}
			return nil
		})
	}

	if len(bq.opsQueue) >= bq.maxBatchSize {
		bq.err = bq.flushBatch()
	}

	return bq.err
}

// BatchDeleteRows  queue deletes of the row and all it's associated Columns from the database.
func (bq *BoltTxnQueue) BatchDeleteRows(rowKeys [][]byte) error {
	if bq.err != nil {
		return bq.err
	}

	for _, rowKey := range rowKeys {
		bq.opsQueue = append(bq.opsQueue, func(txn *bbolt.Bucket) error {
			c := txn.Cursor()
			pKey := append(append([]byte(nil), rowKey...), rowKeySeperator...)
			bq.deleteOps++

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
				bq.entriesModified++
				if err := txn.Delete(k); err != nil {
					return err
				}
			}

			return nil
		})
	}

	if len(bq.opsQueue) >= bq.maxBatchSize {
		bq.err = bq.flushBatch()
	}

	return bq.err
}

func (bq *BoltTxnQueue) flushBatch() error {
	if bq.err != nil || len(bq.opsQueue) == 0 {
		return bq.err
	}

	startTime := time.Now()

	metrics.IncrCounterWithLabels(mTxnFlushTotal, 1, bq.label)
	defer func() {
		metrics.MeasureSinceWithLabels(mTxnFlushLatency, startTime, bq.label)
	}()

	txn, err := bq.db.Begin(true)
	if err != nil {
		return err
	}

	// Make sure the transaction rolls back in the event of a panic.
	defer func() {
		if bq.err != nil {
			err := txn.Rollback()
			slog.Error("[kvdb.boltdb]: rollback transaction failed", "error", err)
		}
	}()

	bucket := txn.Bucket(bq.namespace)
	if bucket == nil {
		return ErrBucketNotFound
	}

	for _, op := range bq.opsQueue {
		if err := op(bucket); err != nil {
			bq.err = err
			break
		}
	}

	if bq.err != nil {
		_ = txn.Rollback()
		return bq.err
	}

	// no errors occurred commit txn
	err = txn.Commit()

	if bq.err == nil {
		metrics.IncrCounterWithLabels(mTxnFlushBatchSize, float32(len(bq.opsQueue)), bq.label)
		metrics.IncrCounterWithLabels(mSetTotal, bq.putOps, bq.label)
		metrics.IncrCounterWithLabels(mDelTotal, bq.deleteOps, bq.label)
		metrics.IncrCounterWithLabels(mTxnEntriesModifiedTotal, bq.entriesModified, bq.label)
		bq.stats.EntriesModified += bq.entriesModified
		bq.stats.DeleteOps += bq.deleteOps
		bq.stats.PutOps += bq.putOps

		bq.opsQueue = nil
		bq.putOps = 0
		bq.deleteOps = 0
		bq.entriesModified = 0
	}
	return err
}

func (bq *BoltTxnQueue) Stats() TxnStats {
	return *bq.stats
}

// Commit all the pending operation in queue.
func (bq *BoltTxnQueue) Commit() error {
	if bq.err != nil {
		return bq.err
	}

	return bq.flushBatch()
}
