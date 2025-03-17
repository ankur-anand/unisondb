package logcodec

import (
	"github.com/ankur-anand/unisondb/schemas/logrecord"
	flatbuffers "github.com/google/flatbuffers/go"
)

// SerializeLogRecord encodes a LogRecord to binary format using FlatBuffers.
func serializeLogRecord(record *LogRecord, builder *flatbuffers.Builder) []byte {
	builder.Reset()

	// https://flatbuffers.dev/internals/
	// tables/unions/strings/vectors (these are never stored in-line)
	// they are always referred by uoffset_t, which is currently always a uint32_t
	// So In the record if any of these are present. it will be always be referred
	// by the offset.

	var txnIDOffset flatbuffers.UOffsetT
	if len(record.TxnID) > 0 {
		txnIDOffset = builder.CreateByteVector(record.TxnID)
	}

	var prevTxnWalIndexOffset flatbuffers.UOffsetT
	if len(record.PrevTxnWalIndex) > 0 {
		prevTxnWalIndexOffset = builder.CreateByteVector(record.PrevTxnWalIndex)
	}

	var payloadType byte
	var payloadOffset flatbuffers.UOffsetT

	if record.Payload.KeyValueBatchEntries != nil {
		payloadType = byte(logrecord.LogOperationDataKeyValueBatchEntries)

		payloadOffset = serializeKeyValueEntries(record, builder)
	} else if record.Payload.RowUpdateEntries != nil {
		payloadType = byte(logrecord.LogOperationDataRowUpdateEntries)
		payloadOffset = serializeRowEntries(record, builder)
	}

	logrecord.LogRecordStart(builder)
	logrecord.LogRecordAddLsn(builder, record.LSN)
	logrecord.LogRecordAddHlc(builder, record.HLC)
	logrecord.LogRecordAddCrc32Checksum(builder, record.CRC32Checksum)
	logrecord.LogRecordAddOperationType(builder, record.OperationType)
	logrecord.LogRecordAddTxnState(builder, record.TxnState)
	logrecord.LogRecordAddEntryType(builder, record.EntryType)

	if len(record.TxnID) > 0 {
		logrecord.LogRecordAddTxnId(builder, txnIDOffset)
	}

	if len(record.PrevTxnWalIndex) > 0 {
		logrecord.LogRecordAddPrevTxnWalIndex(builder, prevTxnWalIndexOffset)
	}

	if payloadOffset != 0 {
		logrecord.LogRecordAddPayloadType(builder, logrecord.LogOperationData(payloadType))
		logrecord.LogRecordAddPayload(builder, payloadOffset)
	}

	logRecordOffset := logrecord.LogRecordEnd(builder)
	builder.Finish(logRecordOffset)

	return builder.FinishedBytes()
}

func serializeKeyValueEntries(record *LogRecord, builder *flatbuffers.Builder) flatbuffers.UOffsetT {
	entries := record.Payload.KeyValueBatchEntries.Entries

	kvEntryOffsets := make([]flatbuffers.UOffsetT, len(entries))

	for i := 0; i < len(entries); i++ {
		keyOffset := builder.CreateByteVector(entries[i].Key)
		valueOffset := builder.CreateByteVector(entries[i].Value)

		logrecord.KeyValueEntryStart(builder)
		logrecord.KeyValueEntryAddKey(builder, keyOffset)
		logrecord.KeyValueEntryAddValue(builder, valueOffset)
		kvEntryOffsets[i] = logrecord.KeyValueEntryEnd(builder)
	}

	logrecord.KeyValueBatchEntriesStartEntriesVector(builder, len(entries))

	// vector are build in reverse way as builder starts writing data at the end of a buffer
	// stack-like structure.
	for i := len(entries) - 1; i >= 0; i-- {
		builder.PrependUOffsetT(kvEntryOffsets[i])
	}
	entriesVector := builder.EndVector(len(entries))

	logrecord.KeyValueBatchEntriesStart(builder)
	logrecord.KeyValueBatchEntriesAddEntries(builder, entriesVector)
	return logrecord.KeyValueBatchEntriesEnd(builder)
}

func serializeRowEntries(record *LogRecord, builder *flatbuffers.Builder) flatbuffers.UOffsetT {
	entries := record.Payload.RowUpdateEntries.Entries

	rowEntryOffsets := make([]flatbuffers.UOffsetT, len(entries))

	for i := 0; i < len(entries); i++ {
		keyOffset := builder.CreateByteVector(entries[i].Key)
		columnDataOffsets := make([]flatbuffers.UOffsetT, len(entries[i].Columns))

		for j := 0; j < len(entries[i].Columns); j++ {
			nameOffset := builder.CreateString(entries[i].Columns[j].Name)
			valueOffset := builder.CreateByteVector(entries[i].Columns[j].Value)

			logrecord.ColumnDataStart(builder)
			logrecord.ColumnDataAddName(builder, nameOffset)
			logrecord.ColumnDataAddValue(builder, valueOffset)
			columnDataOffsets[j] = logrecord.ColumnDataEnd(builder)
		}

		logrecord.RowUpdateEntryStartColumnsVector(builder, len(entries[i].Columns))
		for j := len(entries[i].Columns) - 1; j >= 0; j-- {
			builder.PrependUOffsetT(columnDataOffsets[j])
		}
		columnsVector := builder.EndVector(len(entries[i].Columns))

		logrecord.RowUpdateEntryStart(builder)
		logrecord.RowUpdateEntryAddKey(builder, keyOffset)
		logrecord.RowUpdateEntryAddColumns(builder, columnsVector)
		rowEntryOffsets[i] = logrecord.RowUpdateEntryEnd(builder)
	}

	logrecord.RowUpdateEntriesStartEntriesVector(builder, len(entries))
	for i := len(entries) - 1; i >= 0; i-- {
		builder.PrependUOffsetT(rowEntryOffsets[i])
	}
	entriesVector := builder.EndVector(len(entries))

	logrecord.RowUpdateEntriesStart(builder)
	logrecord.RowUpdateEntriesAddEntries(builder, entriesVector)
	return logrecord.RowUpdateEntriesEnd(builder)
}

// DeserializeLogRecord decodes a binary FlatBuffer into a LogRecord.
func DeserializeLogRecord(buf []byte) *LogRecord {
	record := &LogRecord{}

	fbRecord := logrecord.GetRootAsLogRecord(buf, 0)
	deserializeScalarIntoLogRecord(record, fbRecord)

	payloadType := fbRecord.PayloadType()

	// https://flatbuffers.dev/tutorial/#union-access
	// union only stores a FlatBuffer table
	switch payloadType {
	case logrecord.LogOperationDataKeyValueBatchEntries:

		deserializeKeyValueEntriesIntoLogRecord(record, fbRecord)

	case logrecord.LogOperationDataRowUpdateEntries:

		deserializeRowEntriesIntoRecords(record, fbRecord)
	}
	return record
}

func deserializeScalarIntoLogRecord(record *LogRecord, fbRecord *logrecord.LogRecord) {
	record.LSN = fbRecord.Lsn()
	record.HLC = fbRecord.Hlc()
	record.CRC32Checksum = fbRecord.Crc32Checksum()
	record.OperationType = fbRecord.OperationType()
	record.TxnState = fbRecord.TxnState()
	record.EntryType = fbRecord.EntryType()

	if txnID := fbRecord.TxnIdBytes(); txnID != nil {
		record.TxnID = make([]byte, len(txnID))
		copy(record.TxnID, txnID)
	}

	if prevTxnWalIndex := fbRecord.PrevTxnWalIndexBytes(); prevTxnWalIndex != nil {
		record.PrevTxnWalIndex = make([]byte, len(prevTxnWalIndex))
		copy(record.PrevTxnWalIndex, prevTxnWalIndex)
	}
}

func deserializeKeyValueEntriesIntoLogRecord(record *LogRecord, fbRecord *logrecord.LogRecord) {
	union := new(flatbuffers.Table)
	if fbRecord.Payload(union) {
		kvEntries := logrecord.KeyValueBatchEntries{}
		kvEntries.Init(union.Bytes, union.Pos)

		record.Payload.KeyValueBatchEntries = &KeyValueBatchEntries{}
		entriesLen := kvEntries.EntriesLength()
		record.Payload.KeyValueBatchEntries.Entries = make([]KeyValueEntry, entriesLen)

		for i := 0; i < entriesLen; i++ {
			entry := new(logrecord.KeyValueEntry)
			if kvEntries.Entries(entry, i) {
				key := entry.KeyBytes()
				value := entry.ValueBytes()
				// don't reference flat-buffer memory.
				keyCopy := make([]byte, len(key))
				copy(keyCopy, key)

				valueCopy := make([]byte, len(value))
				copy(valueCopy, value)

				record.Payload.KeyValueBatchEntries.Entries[i] = KeyValueEntry{
					Key:   keyCopy,
					Value: valueCopy,
				}
			}
		}
	}
}

func deserializeRowEntriesIntoRecords(record *LogRecord, fbRecord *logrecord.LogRecord) {
	union := new(flatbuffers.Table)
	if fbRecord.Payload(union) {
		rowEntries := &logrecord.RowUpdateEntries{}
		rowEntries.Init(union.Bytes, union.Pos)

		record.Payload.RowUpdateEntries = &RowUpdateEntries{}
		entriesLen := rowEntries.EntriesLength()
		record.Payload.RowUpdateEntries.Entries = make([]RowUpdateEntry, entriesLen)

		for i := 0; i < entriesLen; i++ {
			entry := new(logrecord.RowUpdateEntry)
			if rowEntries.Entries(entry, i) {
				rowKey := entry.KeyBytes()

				// Get columns
				columnsLen := entry.ColumnsLength()
				columns := make([]ColumnData, columnsLen)

				for j := 0; j < columnsLen; j++ {
					var col logrecord.ColumnData
					if entry.Columns(&col, j) {
						name := string(col.Name())
						value := col.ValueBytes()
						ValueCopy := make([]byte, len(value))
						copy(ValueCopy, value)

						columns[j] = ColumnData{
							Name:  name,
							Value: ValueCopy,
						}
					}
				}

				record.Payload.RowUpdateEntries.Entries[i] = RowUpdateEntry{
					Key:     rowKey,
					Columns: columns,
				}
			}
		}
	}
}
