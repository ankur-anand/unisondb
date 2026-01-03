package wal

import (
	"github.com/ankur-anand/unisondb/internal/logcodec"
	"github.com/ankur-anand/unisondb/pkg/raftwalfs"
	"github.com/ankur-anand/unisondb/pkg/walfs"
	"github.com/ankur-anand/unisondb/schemas/logrecord"
	"github.com/hashicorp/raft"
)

var raftInternalTemplate []byte

func init() {
	record := &logcodec.LogRecord{
		LSN:           1,
		HLC:           1,
		OperationType: logrecord.LogOperationTypeRaftInternal,
		EntryType:     logrecord.LogEntryTypeKV,
	}
	raftInternalTemplate = record.FBEncode(64)
}

// RaftWALDecoder decodes Raft WAL entries and extracts the payload.
// It implements walfs.RecordDecoder for Raft-mode WAL reading.
//
// In Raft mode, WAL entries are BinaryCodecV1-encoded raft.Log structures.
// The actual payload (e.g., LogRecord flatbuffer) is stored in raft.Log.Data.
// This decoder unwraps the Raft envelope and returns the inner payload.
//
// For non-command entries (Noop, Barrier, Configuration), it returns a
// RaftInternal LogRecord to maintain LSN continuity.
type RaftWALDecoder struct {
	codec raftwalfs.Codec
}

// NewRaftWALDecoder creates a RaftWALDecoder with the given codec.
// If codec is nil, BinaryCodecV1{} is used as default.
func NewRaftWALDecoder(codec raftwalfs.Codec) *RaftWALDecoder {
	if codec == nil {
		codec = raftwalfs.BinaryCodecV1{}
	}
	return &RaftWALDecoder{codec: codec}
}

// Decode decodes a Raft WAL entry and returns the inner payload (raft.Log.Data).
// For non-command log types (Noop, Barrier, Configuration), returns a RaftInternal
// LogRecord to maintain LSN continuity across all Raft indexes.
func (d *RaftWALDecoder) Decode(data []byte) ([]byte, error) {
	if len(data) == 0 {
		return nil, nil
	}

	log, err := d.codec.Decode(data)
	if err != nil {
		return nil, err
	}

	// user data
	if log.Type == raft.LogCommand {
		return log.Data, nil
	}

	result := make([]byte, len(raftInternalTemplate))
	copy(result, raftInternalTemplate)

	fb := logrecord.GetRootAsLogRecord(result, 0)
	fb.MutateLsn(log.Index)
	if !log.AppendedAt.IsZero() {
		fb.MutateHlc(uint64(log.AppendedAt.UnixNano()))
	}

	return result, nil
}

var _ walfs.RecordDecoder = (*RaftWALDecoder)(nil)
