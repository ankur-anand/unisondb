package dbkernel

import (
	"github.com/ankur-anand/unisondb/dbkernel/internal"
	"github.com/ankur-anand/unisondb/dbkernel/internal/wal"
	"github.com/ankur-anand/unisondb/internal/logcodec"
	"github.com/ankur-anand/unisondb/schemas/logrecord"
)

// applyInsert applies an insert operation from a LogRecord to the MemTable.
func (e *Engine) applyInsert(record *logrecord.LogRecord, offset *Offset) error {
	logEntry := logcodec.DeserializeFBRootLogRecord(record)
	switch record.EntryType() {
	case logrecord.LogEntryTypeKV:
		for _, entry := range logEntry.Entries {
			kvEntry := logcodec.DeserializeKVEntry(entry)
			memValue := getValueStruct(internal.LogOperationInsert, internal.EntryTypeKV, kvEntry.Value)
			if err := e.memTableWrite(kvEntry.Key, memValue); err != nil {
				return err
			}
		}
		e.writeOffset(offset)
	case logrecord.LogEntryTypeRow:
		for _, entry := range logEntry.Entries {
			rowEntry := logrecord.GetRootAsRowUpdateEntry(entry, 0)
			rowKey := rowEntry.KeyBytes()
			memValue := getValueStruct(internal.LogOperationInsert, internal.EntryTypeRow, entry)
			if err := e.memTableWrite(rowKey, memValue); err != nil {
				return err
			}
		}
		e.writeOffset(offset)
	case logrecord.LogEntryTypeEvent:
		e.writeOffset(offset)
	}
	return nil
}

// applyDelete applies a delete operation from a LogRecord to the MemTable.
func (e *Engine) applyDelete(record *logrecord.LogRecord, offset *Offset) error {
	logEntry := logcodec.DeserializeFBRootLogRecord(record)
	switch record.EntryType() {
	case logrecord.LogEntryTypeKV:
		for _, entry := range logEntry.Entries {
			kvEntry := logcodec.DeserializeKVEntry(entry)
			memValue := getValueStruct(internal.LogOperationDelete, internal.EntryTypeKV, kvEntry.Value)
			if err := e.memTableWrite(kvEntry.Key, memValue); err != nil {
				return err
			}
		}
		e.writeOffset(offset)
	case logrecord.LogEntryTypeRow:
		for _, entry := range logEntry.Entries {
			rowEntry := logrecord.GetRootAsRowUpdateEntry(entry, 0)
			rowKey := rowEntry.KeyBytes()
			memValue := getValueStruct(internal.LogOperationDelete, internal.EntryTypeRow, entry)
			if err := e.memTableWrite(rowKey, memValue); err != nil {
				return err
			}
		}
		e.writeOffset(offset)
	}
	return nil
}

// applyDeleteRowByKey applies a delete-row-by-key operation from a LogRecord to the MemTable.
func (e *Engine) applyDeleteRowByKey(record *logrecord.LogRecord, offset *Offset) error {
	logEntry := logcodec.DeserializeFBRootLogRecord(record)
	for _, entry := range logEntry.Entries {
		rowEntry := logrecord.GetRootAsRowUpdateEntry(entry, 0)
		rowKey := rowEntry.KeyBytes()
		memValue := getValueStruct(internal.LogOperationDeleteRowByKey, internal.EntryTypeRow, entry)
		if err := e.memTableWrite(rowKey, memValue); err != nil {
			return err
		}
	}
	e.writeOffset(offset)
	return nil
}

// applyTxnCommit applies a transaction commit from a LogRecord to the MemTable.
func (e *Engine) applyTxnCommit(record *logrecord.LogRecord, offset *Offset) error {
	switch record.EntryType() {
	case logrecord.LogEntryTypeKV:
		_, err := e.applyKVValuesTxn(record, offset)
		return err
	case logrecord.LogEntryTypeChunked:
		return e.applyChunkedValuesTxn(record, offset)
	case logrecord.LogEntryTypeRow:
		_, err := e.applyRowColumnTxn(record, offset)
		return err
	}
	return nil
}

// applyKVValuesTxn applies KV transaction entries to the MemTable.
func (e *Engine) applyKVValuesTxn(record *logrecord.LogRecord, offset *Offset) (int64, error) {
	records, err := e.walIO.GetTransactionRecords(wal.DecodeOffset(record.PrevTxnWalIndexBytes()))
	if err != nil {
		return 0, err
	}

	// remove the begins part from the
	preparedRecords := records[1:]
	// Empty Increment the offset as this operation was carried but don't provide any offset value
	// newer offset is present.
	e.writeNilOffset()

	var applied int64
	for _, pRecord := range preparedRecords {
		logEntry := logcodec.DeserializeFBRootLogRecord(pRecord)
		for _, entry := range logEntry.Entries {
			kvEntry := logcodec.DeserializeKVEntry(entry)
			memValue := getValueStruct(byte(record.OperationType()), internal.EntryTypeKV, kvEntry.Value)
			if err := e.memTableWrite(kvEntry.Key, memValue); err != nil {
				return applied, err
			}
			applied++
			// empty offset just to increment the offset count that will be flushed.
			e.writeNilOffset()
		}
	}

	// finally write the current offset.
	e.writeOffset(offset)
	return applied, nil
}

// applyRowColumnTxn applies Row transaction entries to the MemTable.
func (e *Engine) applyRowColumnTxn(record *logrecord.LogRecord, offset *Offset) (int64, error) {
	records, err := e.walIO.GetTransactionRecords(wal.DecodeOffset(record.PrevTxnWalIndexBytes()))
	if err != nil {
		return 0, err
	}

	// remove the begins part from the
	preparedRecords := records[1:]
	// Empty Increment the offset as this operation was carried but don't provide any offset value
	e.writeNilOffset()

	var applied int64
	for _, pRecord := range preparedRecords {
		logEntry := logcodec.DeserializeFBRootLogRecord(pRecord)
		for _, entry := range logEntry.Entries {
			rowEntry := logcodec.DeserializeRowUpdateEntry(entry)
			memValue := getValueStruct(byte(record.OperationType()), internal.EntryTypeRow, entry)
			if err := e.memTableWrite(rowEntry.Key, memValue); err != nil {
				return applied, err
			}
			applied++
			e.writeNilOffset()
		}
	}

	e.writeOffset(offset)
	return applied, nil
}

// applyChunkedValuesTxn applies chunked value transaction to the MemTable.
func (e *Engine) applyChunkedValuesTxn(record *logrecord.LogRecord, offset *wal.Offset) error {
	logEntry := logcodec.DeserializeFBRootLogRecord(record)
	kvEntry := logcodec.DeserializeKVEntry(logEntry.Entries[0])
	chunkedKey := kvEntry.Key
	memValue := getValueStruct(byte(logrecord.LogOperationTypeInsert), byte(logrecord.LogEntryTypeChunked), offset.Encode())
	if err := e.memTableWrite(chunkedKey, memValue); err != nil {
		return err
	}
	e.writeOffset(offset)
	return nil
}

// applyLogRecord routes a LogRecord to the appropriate apply method based on TxnState and OperationType.
func (e *Engine) applyLogRecord(record *logrecord.LogRecord, offset *wal.Offset) error {
	switch record.TxnState() {
	case logrecord.TransactionStateNone:
		switch record.OperationType() {
		case logrecord.LogOperationTypeInsert:
			return e.applyInsert(record, offset)
		case logrecord.LogOperationTypeDelete:
			return e.applyDelete(record, offset)
		case logrecord.LogOperationTypeDeleteRowByKey:
			return e.applyDeleteRowByKey(record, offset)
		}
	case logrecord.TransactionStateCommit:
		return e.applyTxnCommit(record, offset)
	}
	return nil
}
