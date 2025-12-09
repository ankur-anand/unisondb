package raftwalfs

import (
	"errors"

	"github.com/ankur-anand/unisondb/schemas/logrecord"
)

// DataMutator provides in-place mutation of fields within encoded data.
// This is used to inject Raft index as LSN into engine's FlatBuffer records
// after the log is committed to Raft.
type DataMutator interface {
	// MutateLSN updates the LSN field in the encoded data buffer (in-place).
	MutateLSN(data []byte, lsn uint64) error

	// GetLSN reads the LSN from encoded data.
	// Returns 0 if the field is not present.
	GetLSN(data []byte) uint64
}

// LogRecordMutator implements DataMutator for engine's LogRecord FlatBuffer.
type LogRecordMutator struct{}

// MutateLSN updates the LSN field in a LogRecord FlatBuffer.
// The mutation is done in-place without allocating new memory.
//
// Note: FlatBuffers doesn't store fields with default values (0).
// If the original LogRecord was created with LSN=0 without ForceDefaults,
// the mutation will fail. The engine must ensure LSN is explicitly written
// by either using a non-zero placeholder or configuring the builder.
func (m LogRecordMutator) MutateLSN(data []byte, lsn uint64) error {
	if len(data) == 0 {
		return nil
	}

	record := logrecord.GetRootAsLogRecord(data, 0)
	if !record.MutateLsn(lsn) {
		// This happens when LSN field wasn't stored in the buffer
		return errors.New("failed to mutate LSN: field not present in buffer")
	}
	return nil
}

// GetLSN reads the LSN from a LogRecord FlatBuffer.
func (m LogRecordMutator) GetLSN(data []byte) uint64 {
	if len(data) == 0 {
		return 0
	}
	record := logrecord.GetRootAsLogRecord(data, 0)
	return record.Lsn()
}

var _ DataMutator = LogRecordMutator{}
