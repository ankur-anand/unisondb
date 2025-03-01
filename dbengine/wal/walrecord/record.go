package walrecord

import (
	"hash/crc32"

	flatbuffers "github.com/google/flatbuffers/go"
	"github.com/rosedblabs/wal"
)

// Record is what gets encoded for flat-buffer schema.
type Record struct {
	Index        uint64
	Hlc          uint64
	Key          []byte
	Value        []byte
	LogOperation LogOperation

	ValueType     ValueType
	TxnStatus     TxnStatus
	TxnID         []byte
	PrevTxnOffset *wal.ChunkPosition
}

// FBEncode encodes the provided record into flat-buffer format.
func (wr *Record) FBEncode() ([]byte, error) {
	initSize := 1 + len(wr.Key) + len(wr.Value)
	builder := flatbuffers.NewBuilder(initSize)

	keyOffset := builder.CreateByteVector(wr.Key)

	// Ensure valueOffset is always valid
	var valueOffset flatbuffers.UOffsetT

	if len(wr.Value) > 0 {
		valueOffset = builder.CreateByteVector(wr.Value)
	} else {
		valueOffset = builder.CreateByteVector([]byte{}) // Assign an empty vector
	}

	// Ensure batchOffset is always valid
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

	walRecordOffset := WalRecordEnd(builder)

	// Finish FlatBuffer
	builder.Finish(walRecordOffset)

	return builder.FinishedBytes(), nil
}
