package walrecord

import (
	"hash/crc32"

	"github.com/ankur-anand/wal"
	flatbuffers "github.com/google/flatbuffers/go"
)

// Record is what gets encoded for flat-buffer schema.
type Record struct {
	Index uint64
	Hlc   uint64
	// Row Key
	Key []byte
	// Row Value if Not Columnar.
	Value        []byte
	LogOperation LogOperation

	ValueType     ValueType
	TxnStatus     TxnStatus
	TxnID         []byte
	PrevTxnOffset *wal.ChunkPosition
	// ColumnEntries is wide column.
	ColumnEntries map[string][]byte
}

// FBEncode encodes the provided record into flat-buffer format.
//
//nolint:funlen
func (wr *Record) FBEncode() ([]byte, error) {
	builder := flatbuffers.NewBuilder(2048)
	keyOffset := builder.CreateByteVector(wr.Key)

	// https://flatbuffers.dev/internals/
	// tables/unions/strings/vectors (these are never stored in-line)
	// they are always referred by uoffset_t, which is currently always a uint32_t
	// So In the record if any of these are present. it will be always be referred
	// by the offset.

	// build the column
	var columnOffsets []flatbuffers.UOffsetT
	if wr.ValueType == ValueTypeColumn {
		for k, v := range wr.ColumnEntries {
			colNameOffset := builder.CreateString(k)
			colValueOffset := builder.CreateByteVector(v)
			ColumnEntryStart(builder)
			ColumnEntryAddColumnName(builder, colNameOffset)
			ColumnEntryAddColumnValue(builder, colValueOffset)
			ColumnEntryAddCrc32Checksum(builder, crc32.ChecksumIEEE(v))
			columnOffsets = append(columnOffsets, ColumnEntryEnd(builder))
		}
	}

	WalRecordStartColumnsVector(builder, len(columnOffsets))

	// just making sure we are reading in the same order in which we insrted.
	// stack
	for i := len(columnOffsets) - 1; i >= 0; i-- {
		builder.PrependUOffsetT(columnOffsets[i])
	}
	columnsOffset := builder.EndVector(len(columnOffsets))

	var valueOffset flatbuffers.UOffsetT

	if len(wr.Value) > 0 {
		valueOffset = builder.CreateByteVector(wr.Value)
	} else {
		valueOffset = builder.CreateByteVector([]byte{})
	}

	var batchOffset flatbuffers.UOffsetT
	var lastBatchPosOffset flatbuffers.UOffsetT

	if wr.PrevTxnOffset != nil {
		lastBatchPosOffset = builder.CreateByteVector(wr.PrevTxnOffset.Encode())
	}

	if len(wr.TxnID) > 0 {
		batchOffset = builder.CreateByteVector(wr.TxnID)
	} else {
		batchOffset = builder.CreateByteVector([]byte{}) // Assign an empty vector
	}

	checksum := crc32.ChecksumIEEE(wr.Value)

	// Start building the WAL Record
	WalRecordStart(builder)
	WalRecordAddIndex(builder, wr.Index)
	WalRecordAddHlc(builder, wr.Hlc)
	WalRecordAddOperation(builder, wr.LogOperation)
	WalRecordAddKey(builder, keyOffset)
	WalRecordAddValue(builder, valueOffset)
	WalRecordAddTxnId(builder, batchOffset)
	WalRecordAddCrc32Checksum(builder, checksum)
	WalRecordAddPrevTxnWalIndex(builder, lastBatchPosOffset)
	WalRecordAddTxnStatus(builder, wr.TxnStatus)
	WalRecordAddValueType(builder, wr.ValueType)
	WalRecordAddColumns(builder, columnsOffset)
	walRecordOffset := WalRecordEnd(builder)

	// Finish FlatBuffer
	builder.Finish(walRecordOffset)

	return builder.FinishedBytes(), nil
}
