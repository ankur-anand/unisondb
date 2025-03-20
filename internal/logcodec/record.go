package logcodec

import (
	"github.com/ankur-anand/unisondb/schemas/logrecord"
	flatbuffers "github.com/google/flatbuffers/go"
)

type KeyValueEntry struct {
	Key   []byte
	Value []byte
}

type RowEntry struct {
	Key     []byte
	Columns map[string][]byte
}

type LogRecord struct {
	LSN             uint64
	HLC             uint64
	CRC32Checksum   uint32
	OperationType   logrecord.LogOperationType
	TxnState        logrecord.TransactionState
	EntryType       logrecord.LogEntryType
	TxnID           []byte
	PrevTxnWalIndex []byte
	Entries         [][]byte
}

// FBEncode encodes the provided record into flat-buffer format.
func (r *LogRecord) FBEncode(sizeHint int) []byte {
	builder := flatbuffers.NewBuilder(sizeHint)
	return serializeLogRecord(r, builder)
}
