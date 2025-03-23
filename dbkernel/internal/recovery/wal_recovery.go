package recovery

import (
	"errors"
	"fmt"
	"io"

	"github.com/ankur-anand/unisondb/dbkernel/internal"
	"github.com/ankur-anand/unisondb/dbkernel/internal/wal"
	"github.com/ankur-anand/unisondb/internal/logcodec"
	"github.com/ankur-anand/unisondb/schemas/logrecord"
	"github.com/bits-and-blooms/bloom/v3"
)

// WalRecovery recovers WAL entries from WAL files and persists them into the B-tree store.
type WalRecovery struct {
	w *walRecovery
}

// NewWalRecovery creates and returns a new initialized instance of WalRecovery.
func NewWalRecovery(store internal.BTreeStore, walIO *wal.WalIO, bloom *bloom.BloomFilter) *WalRecovery {
	return &WalRecovery{
		w: &walRecovery{
			store:            store,
			walIO:            walIO,
			recoveredCount:   0,
			lastRecoveredPos: nil,
			bloom:            bloom,
		},
	}
}

func (w *WalRecovery) RecoveredCount() int {
	return w.w.recoveredCount
}

func (w *WalRecovery) LastRecoveredOffset() *wal.Offset {
	if w.w.lastRecoveredPos == nil {
		return nil
	}
	off := *w.w.lastRecoveredPos
	return &off
}

// Recover restores WAL entries starting from the provided checkpoint.
func (w *WalRecovery) Recover(checkPoint []byte) error {
	return w.w.recoverWAL(checkPoint)
}

type walRecovery struct {
	store            internal.BTreeStore
	walIO            *wal.WalIO
	recoveredCount   int
	lastRecoveredPos *wal.Offset
	bloom            *bloom.BloomFilter
}

// recoverWAL recover wal from last check point saved in btree store.
func (wr *walRecovery) recoverWAL(checkPoint []byte) error {
	var offset wal.Offset
	if len(checkPoint) != 0 {
		metadata := internal.UnmarshalMetadata(checkPoint)
		offset = *metadata.Pos
	}

	reader, err := wr.walIO.NewReaderWithStart(&offset)
	if err != nil {
		return fmt.Errorf("recover WAL failed %w", err)
	}

	if len(checkPoint) != 0 {
		// first value will be duplicate, so we can ignore it.
		_, _, err := reader.Next()
		if wr.isFatalError(err) {
			return fmt.Errorf("recover WAL failed %w", err)
		}
	}

	for {
		value, pos, err := reader.Next()
		if err != nil && errors.Is(err, io.EOF) {
			break
		}

		if err != nil {
			return fmt.Errorf("recover WAL failed %w", err)
		}
		record := logrecord.GetRootAsLogRecord(value, 0)
		err = wr.handleRecord(record)
		wr.lastRecoveredPos = pos
		if err != nil {
			return fmt.Errorf("recover WAL failed %w", err)
		}
	}
	return nil
}

func (wr *walRecovery) handleRecord(record *logrecord.LogRecord) error {
	// we only recover two cases.
	// Individual Insert/Delete
	// Txn Insert/Delete and Chunk. Uncommited Txn are ignored.
	switch record.TxnState() {
	case logrecord.TransactionStateNone:
		wr.recoveredCount++
		switch record.OperationType() {
		case logrecord.LogOperationTypeInsert:
			return wr.handleInsert(record)
		case logrecord.LogOperationTypeDelete:
			return wr.handleDelete(record)
		case logrecord.LogOperationTypeDeleteRowByKey:
			return wr.handleDeleteRowByKey(record)
		}
	case logrecord.TransactionStateCommit:
		return wr.handleTxnCommited(record)
	}

	return nil
}

func (wr *walRecovery) handleInsert(record *logrecord.LogRecord) error {
	logEntry := logcodec.DeserializeFBRootLogRecord(record)
	switch record.EntryType() {
	case logrecord.LogEntryTypeKV:
		var keys [][]byte
		var values [][]byte
		for _, entry := range logEntry.Entries {
			kvEntry := logcodec.DeserializeKVEntry(entry)
			wr.bloom.Add(kvEntry.Key)
			keys = append(keys, kvEntry.Key)
			values = append(values, kvEntry.Value)
		}
		return wr.store.SetMany(keys, values)
	case logrecord.LogEntryTypeRow:
		var keys [][]byte
		var values []map[string][]byte
		for _, entry := range logEntry.Entries {
			rowEntry := logcodec.DeserializeRowUpdateEntry(entry)
			wr.bloom.Add(rowEntry.Key)
			keys = append(keys, rowEntry.Key)
			values = append(values, rowEntry.Columns)
		}

		return wr.store.SetManyRowColumns(keys, values)
	}

	return nil
}

func (wr *walRecovery) handleDelete(record *logrecord.LogRecord) error {
	logEntry := logcodec.DeserializeFBRootLogRecord(record)
	switch record.EntryType() {
	case logrecord.LogEntryTypeKV:
		var keys [][]byte
		for _, entry := range logEntry.Entries {
			kvEntry := logcodec.DeserializeKVEntry(entry)
			keys = append(keys, kvEntry.Key)
		}
		return wr.store.DeleteMany(keys)
	case logrecord.LogEntryTypeRow:
		var keys [][]byte
		var values []map[string][]byte
		for _, entry := range logEntry.Entries {
			rowEntry := logcodec.DeserializeRowUpdateEntry(entry)
			keys = append(keys, rowEntry.Key)
			values = append(values, rowEntry.Columns)
		}

		return wr.store.DeleteManyRowColumns(keys, values)
	}

	return nil
}

func (wr *walRecovery) handleDeleteRowByKey(record *logrecord.LogRecord) error {
	logEntry := logcodec.DeserializeFBRootLogRecord(record)
	var keys [][]byte
	for _, entry := range logEntry.Entries {
		kvEntry := logcodec.DeserializeRowUpdateEntry(entry)
		keys = append(keys, kvEntry.Key)
	}
	_, err := wr.store.DeleteEntireRows(keys)
	return err
}

func (wr *walRecovery) isFatalError(err error) bool {
	return err != nil && !errors.Is(err, io.EOF)
}

// handleTxnCommited handles the current commited txn, for chunked, insert and delete ops.
func (wr *walRecovery) handleTxnCommited(record *logrecord.LogRecord) error {
	wr.recoveredCount++
	switch record.EntryType() {
	case logrecord.LogEntryTypeKV:
		return wr.handleKVValuesTxn(record)
	case logrecord.LogEntryTypeChunked:
		return wr.handleChunkedValuesTxn(record)
	case logrecord.LogEntryTypeRow:
		return wr.handleRowColumnTxn(record)
	}

	return nil
}

// handleKVValuesTxn Handles the insert and delete operation of Txn and updates
// the same to the underlying btree bases store.
func (wr *walRecovery) handleKVValuesTxn(record *logrecord.LogRecord) error {
	records, err := wr.walIO.GetTransactionRecords(wal.DecodeOffset(record.PrevTxnWalIndexBytes()))
	if err != nil {
		return err
	}

	// remove the begins part from the
	preparedRecords := records[1:]

	values := make([][]byte, 0, len(preparedRecords))
	keys := make([][]byte, 0, len(preparedRecords))
	for _, pRecord := range preparedRecords {
		logEntry := logcodec.DeserializeFBRootLogRecord(pRecord)
		for _, entry := range logEntry.Entries {
			kvEntry := logcodec.DeserializeKVEntry(entry)
			wr.bloom.Add(kvEntry.Key)
			keys = append(keys, kvEntry.Key)
			values = append(values, kvEntry.Value)
		}
	}

	wr.recoveredCount += len(records)
	switch record.OperationType() {
	case logrecord.LogOperationTypeInsert:
		return wr.store.SetMany(keys, values)
	case logrecord.LogOperationTypeDelete:
		return wr.store.DeleteMany(keys)
	}
	return nil
}

// handleRowColumnTxn Handles the insert and delete operation of Txn for RowUpdate and updates
// the same to the underlying btree bases store.
func (wr *walRecovery) handleRowColumnTxn(record *logrecord.LogRecord) error {
	records, err := wr.walIO.GetTransactionRecords(wal.DecodeOffset(record.PrevTxnWalIndexBytes()))
	if err != nil {
		return err
	}

	// remove the begins part from the
	preparedRecords := records[1:]

	values := make([]map[string][]byte, 0, len(preparedRecords))
	keys := make([][]byte, 0, len(preparedRecords))
	for _, pRecord := range preparedRecords {
		logEntry := logcodec.DeserializeFBRootLogRecord(pRecord)
		for _, entry := range logEntry.Entries {
			rowEntry := logcodec.DeserializeRowUpdateEntry(entry)
			wr.bloom.Add(rowEntry.Key)
			keys = append(keys, rowEntry.Key)
			values = append(values, rowEntry.Columns)
		}
	}

	wr.recoveredCount += len(records)
	switch record.OperationType() {
	case logrecord.LogOperationTypeInsert:
		return wr.store.SetManyRowColumns(keys, values)
	case logrecord.LogOperationTypeDelete:
		return wr.store.DeleteManyRowColumns(keys, values)
	case logrecord.LogOperationTypeDeleteRowByKey:
		_, err := wr.store.DeleteEntireRows(keys)
		return err
	}

	return nil
}

// handleChunkedValuesTxn saves all the chunked value that is part of the current commit txn.
// to the provided btree based store.
func (wr *walRecovery) handleChunkedValuesTxn(record *logrecord.LogRecord) error {
	checksum := record.Crc32Checksum()
	dr := logcodec.DeserializeFBRootLogRecord(record)
	kv := logcodec.DeserializeKVEntry(dr.Entries[0])
	key := kv.Key
	records, err := wr.walIO.GetTransactionRecords(wal.DecodeOffset(record.PrevTxnWalIndexBytes()))
	if err != nil {
		return fmt.Errorf("failed to reconstruct batch value: %w", err)
	}

	// remove the begins part from the
	preparedRecords := records[1:]

	values := make([][]byte, len(preparedRecords))
	for i, record := range preparedRecords {
		r := logcodec.DeserializeFBRootLogRecord(record)
		kv := logcodec.DeserializeKVEntry(r.Entries[0])
		values[i] = kv.Value
	}
	count := len(records)
	err = wr.store.SetChunks(key, values, checksum)
	wr.recoveredCount += count
	wr.bloom.Add(key)
	return err
}
