package store

import (
	"fmt"
	"hash/crc32"

	"github.com/ankur-anand/kvalchemy/storage"
	"github.com/ankur-anand/kvalchemy/storage/wrecord"
	flatbuffers "github.com/google/flatbuffers/go"
	"github.com/rosedblabs/wal"
)

// walRecord.
type walRecord struct {
	hlc   uint64
	key   []byte
	value []byte
	op    wrecord.LogOperation

	batchID      []byte
	lastBatchPos *wal.ChunkPosition
}

func (w *walRecord) fbEncode() ([]byte, error) {
	initSize := 1 + len(w.key) + len(w.value)
	builder := flatbuffers.NewBuilder(initSize)

	keyOffset := builder.CreateByteVector(w.key)

	// Ensure valueOffset is always valid
	var valueOffset flatbuffers.UOffsetT

	if len(w.value) > 0 {
		compressValue, err := storage.CompressLZ4(w.value)
		if err != nil {
			return nil, fmt.Errorf("compress lz4: %w", err)
		}
		valueOffset = builder.CreateByteVector(compressValue)
	} else {
		valueOffset = builder.CreateByteVector([]byte{}) // Assign an empty vector
	}

	// Ensure batchOffset is always valid
	var batchOffset flatbuffers.UOffsetT
	var lastBatchPosOffset flatbuffers.UOffsetT

	if w.lastBatchPos != nil {
		lastBatchPosOffset = builder.CreateByteVector(w.lastBatchPos.Encode())
	}

	if len(w.batchID) > 0 {
		batchOffset = builder.CreateByteVector(w.batchID)
	} else {
		batchOffset = builder.CreateByteVector([]byte{}) // Assign an empty vector
	}

	checksum := crc32.ChecksumIEEE(w.value)

	// Start building the WAL Record
	wrecord.WalRecordStart(builder)
	wrecord.WalRecordAddHlc(builder, w.hlc)
	wrecord.WalRecordAddOperation(builder, w.op)
	wrecord.WalRecordAddKey(builder, keyOffset)
	wrecord.WalRecordAddValue(builder, valueOffset)
	wrecord.WalRecordAddBatchId(builder, batchOffset)
	wrecord.WalRecordAddRecordChecksum(builder, checksum)
	wrecord.WalRecordAddLastBatchPos(builder, lastBatchPosOffset)

	walRecordOffset := wrecord.WalRecordEnd(builder)

	// Finish FlatBuffer
	builder.Finish(walRecordOffset)

	return builder.FinishedBytes(), nil
}
