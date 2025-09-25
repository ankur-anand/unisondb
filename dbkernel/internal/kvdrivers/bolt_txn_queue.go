package kvdrivers

import (
	"bytes"
	"encoding/binary"
	"log/slog"
	"time"

	"go.etcd.io/bbolt"
)

// compile time check.
var _ txnWriter = (*BoltTxnQueue)(nil)

// BoltTxnQueue encapsulates Boltdb Transactions and provides an API that allow multiple
// operation to be put in queue that is supposed to be Executed in the same orders.
// It flushes the batch automatically if configured max batch Size threshold is reached.
// Caller should call Commit in the end to finish any pending Txn not flushed via maxBatchSize.
// Single instance of Txn are not concurrent safe.
type BoltTxnQueue struct {
	err             error
	db              *bbolt.DB
	namespace       []byte
	opsQueue        []func(tx *bbolt.Bucket) error
	maxBatchSize    int
	entriesModified float32
	putOps          float32
	deleteOps       float32
	stats           *TxnStats
	mt              *MetricsTracker
}

// NewTxnQueue returns an initialized BoltTxnQueue for Batch API queuing and commit.
func (b *BoltDBEmbed) NewTxnQueue(maxBatchSize int) *BoltTxnQueue {
	return &BoltTxnQueue{
		mt:           b.mt,
		err:          nil,
		maxBatchSize: maxBatchSize,
		opsQueue:     make([]func(bucket *bbolt.Bucket) error, 0, maxBatchSize),
		namespace:    b.namespace,
		db:           b.db,
		stats:        &TxnStats{},
	}
}

// BatchPutKV queue one or more key-value pairs inside a transaction that will be commited upon Commit or max batch size
// threshold breach. If the value exists, it replaces the existing value, else sets a new value associated with the key.
func (bq *BoltTxnQueue) BatchPutKV(keys, values [][]byte) error {
	if bq.err != nil {
		return bq.err
	}

	for i, key := range keys {
		// append the set function to queue for processing
		bq.opsQueue = append(bq.opsQueue, func(txn *bbolt.Bucket) error {
			typedKey := KeyKV(key)

			err := txn.Put(typedKey, values[i])
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

// BatchDeleteKV queue one or more key inside a transaction for deletion. It doesn't delete rows or columns.
// Deletion happens either when Commit is called or max batch size threshold is reached.
func (bq *BoltTxnQueue) BatchDeleteKV(keys [][]byte) error {
	if bq.err != nil {
		return bq.err
	}

	for _, key := range keys {
		bq.opsQueue = append(bq.opsQueue, func(txn *bbolt.Bucket) error {
			typedKey := KeyKV(key)
			storedValue := txn.Get(typedKey)
			if storedValue == nil {
				return nil
			}
			return txn.Delete(typedKey)
		})
	}

	if len(bq.opsQueue) >= bq.maxBatchSize {
		bq.err = bq.flushBatch()
	}

	return bq.err
}

// SetLobChunks stores a value that has been split into chunks, associating them with a single key.
// It queues the operation in Txn, which is flushed when either max size threshold is reached or during commit call.
func (bq *BoltTxnQueue) SetLobChunks(key []byte, chunks [][]byte, checksum uint32) error {
	if bq.err != nil {
		return bq.err
	}

	typedKey := KeyBlobChunk(key, 0)
	bq.opsQueue = append(bq.opsQueue, func(txn *bbolt.Bucket) error {
		// get last stored for keys, if present.
		// older chunk needs to deleted for not leaking the space.
		storedValue := txn.Get(typedKey)
		if storedValue != nil && storedValue[0] == chunkedValue {
			if len(storedValue) < 9 {
				return ErrInvalidChunkMetadata
			}
			chunkCount := binary.LittleEndian.Uint32(storedValue[1:5])

			for i := 1; i <= int(chunkCount); i++ {
				chunkKey := KeyBlobChunk(key, i)
				if err := txn.Delete(chunkKey); err != nil {
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
		if err := txn.Put(typedKey, metaData); err != nil {
			return err
		}

		// individual chunk
		for i, chunk := range chunks {
			chunkKey := KeyBlobChunk(key, i+1)
			if err := txn.Put(chunkKey, chunk); err != nil {
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

// BatchDeleteLobChunks queue one or more LOB key inside a transaction for deletion. It doesn't delete rows or columns.
// Deletion happens either when Commit is called or max batch size threshold is reached.
func (bq *BoltTxnQueue) BatchDeleteLobChunks(keys [][]byte) error {
	if bq.err != nil {
		return bq.err
	}

	for _, key := range keys {
		bq.opsQueue = append(bq.opsQueue, func(txn *bbolt.Bucket) error {
			typedKey := KeyBlobChunk(key, 0)
			storedValue := txn.Get(typedKey)
			if storedValue == nil {
				return nil
			}

			if len(storedValue) != 9 {
				return ErrInvalidChunkMetadata
			}

			chunkCount := binary.LittleEndian.Uint32(storedValue[1:5])
			for i := 1; i <= int(chunkCount); i++ {
				chunkKey := KeyBlobChunk(key, i)
				if err := txn.Delete(chunkKey); err != nil {
					return err
				}
			}

			return txn.Delete(typedKey)
		})
	}

	if len(bq.opsQueue) >= bq.maxBatchSize {
		bq.err = bq.flushBatch()
	}

	return bq.err
}

// BatchSetCells queues updates or inserts of multiple rows with the provided column entries.
// Each row in `rowKeys` maps to a set of columns in `columnEntriesPerRow`.
func (bq *BoltTxnQueue) BatchSetCells(rowKeys [][]byte, columnEntriesPerRow []map[string][]byte) error {
	if bq.err != nil {
		return bq.err
	}

	if len(rowKeys) != len(columnEntriesPerRow) {
		bq.err = ErrInvalidArguments
		return bq.err
	}

	for i, rowKey := range rowKeys {
		i, rowKey := i, rowKey
		bq.opsQueue = append(bq.opsQueue, func(txn *bbolt.Bucket) error {
			columnEntries := columnEntriesPerRow[i]
			if len(columnEntries) == 0 {
				return nil
			}

			bq.putOps++
			for key, value := range columnEntries {
				bq.entriesModified++
				typedKey := KeyColumn(rowKey, unsafeStringToBytes(key))
				if err := txn.Put(typedKey, value); err != nil {
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

// BatchDeleteCells queues deletes of multiple rows with the provided column entries.
// Each row in `rowKeys` maps to a set of columns in `columnEntriesPerRow`.
func (bq *BoltTxnQueue) BatchDeleteCells(rowKeys [][]byte, columnEntriesPerRow []map[string][]byte) error {
	if bq.err != nil {
		return bq.err
	}

	if len(rowKeys) != len(columnEntriesPerRow) {
		bq.err = ErrInvalidArguments
		return bq.err
	}

	for i, rowKey := range rowKeys {
		i, rowKey := i, rowKey
		bq.opsQueue = append(bq.opsQueue, func(txn *bbolt.Bucket) error {
			columnEntries := columnEntriesPerRow[i]
			if len(columnEntries) == 0 {
				return nil
			}

			bq.deleteOps++
			for key := range columnEntries {
				bq.entriesModified++
				typedKey := KeyColumn(rowKey, unsafeStringToBytes(key))
				if err := txn.Delete(typedKey); err != nil {
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

// BatchDeleteRows  queue deletes of the row and all it's associated Columns from the database.
func (bq *BoltTxnQueue) BatchDeleteRows(rowKeys [][]byte) error {
	if bq.err != nil {
		return bq.err
	}

	for _, rowKey := range rowKeys {
		bq.opsQueue = append(bq.opsQueue, func(txn *bbolt.Bucket) error {
			c := txn.Cursor()
			typedKey := RowKey(rowKey)

			bq.deleteOps++
			keys := make([][]byte, 0)
			// IMP: Never delete in cursor, directly followed by Next
			// k, _ := c.Seek(rowKey); k != nil; k, _ = c.Next() {
			// 		c.DeleteKV()
			// }
			// The above pattern should be avoided at all for
			// https://github.com/boltdb/bolt/issues/620
			// https://github.com/etcd-io/bbolt/issues/146
			// Cursor.DeleteKV followed by Next skips the next k/v pair
			for k, _ := c.Seek(typedKey); k != nil; k, _ = c.Next() {
				if !bytes.HasPrefix(k, typedKey) {
					break // Stop if key is outside the prefix range
				}
				kk := append([]byte(nil), k...)
				keys = append(keys, kk)
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

	txn, err := bq.db.Begin(true)
	if err != nil {
		return err
	}

	// Make sure the transaction rolls back in the event of a panic.
	defer func() {
		if bq.err != nil {
			err := txn.Rollback()
			slog.Error("[kvdrivers]", "message", "Transaction rollback failed", "error", err)
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
		bq.mt.RecordFlush(len(bq.opsQueue), startTime)
		bq.mt.RecordBatchOps(OpSet, int(bq.putOps))
		bq.mt.RecordBatchOps(OpDelete, int(bq.deleteOps))
		bq.mt.RecordWriteUnits(int(bq.entriesModified))

		bq.stats.EntriesModified += bq.entriesModified
		bq.stats.DeleteOps += bq.deleteOps
		bq.stats.PutOps += bq.putOps

		bq.opsQueue = nil
		bq.putOps = 0
		bq.deleteOps = 0
		bq.entriesModified = 0
	} else {
		bq.mt.RecordError(TxnCommit)
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
