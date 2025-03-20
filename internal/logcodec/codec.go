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

	payloadOffset := serializeEntries(record, builder)

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
		logrecord.LogRecordAddEntries(builder, payloadOffset)
	}

	logRecordOffset := logrecord.LogRecordEnd(builder)
	builder.Finish(logRecordOffset)

	return builder.FinishedBytes()
}

func serializeEntries(log *LogRecord, builder *flatbuffers.Builder) flatbuffers.UOffsetT {

	kvOffsets := make([]flatbuffers.UOffsetT, len(log.Entries))
	for i, kv := range log.Entries {
		dataOffset := builder.CreateByteVector(kv)
		logrecord.EncodedEntryStart(builder)
		logrecord.EncodedEntryAddEntry(builder, dataOffset)
		kvOffsets[i] = logrecord.EncodedEntryEnd(builder)
	}

	logrecord.LogRecordStartEntriesVector(builder, len(kvOffsets))
	for i := len(kvOffsets) - 1; i >= 0; i-- {
		builder.PrependUOffsetT(kvOffsets[i])
	}
	return builder.EndVector(len(kvOffsets))
}

func DeserializeLogRecord(data []byte) *LogRecord {
	return DeserializeFBRootLogRecord(logrecord.GetRootAsLogRecord(data, 0))
}

// DeserializeFBRootLogRecord converts the FlatBuffer WAL Log Record to Go Struct LogRecord.
func DeserializeFBRootLogRecord(fbRecord *logrecord.LogRecord) *LogRecord {

	entriesLen := fbRecord.EntriesLength()
	var entries [][]byte
	for i := 0; i < entriesLen; i++ {
		entry := new(logrecord.EncodedEntry)
		fbRecord.Entries(entry, i)
		entries = append(entries, entry.EntryBytes())
	}

	record := &LogRecord{
		LSN:             fbRecord.Lsn(),
		HLC:             fbRecord.Hlc(),
		CRC32Checksum:   fbRecord.Crc32Checksum(),
		OperationType:   fbRecord.OperationType(),
		TxnState:        fbRecord.TxnState(),
		EntryType:       fbRecord.EntryType(),
		TxnID:           fbRecord.TxnIdBytes(),
		PrevTxnWalIndex: fbRecord.PrevTxnWalIndexBytes(),
		Entries:         entries,
	}

	return record
}
