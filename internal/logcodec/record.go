package logcodec

import (
	"github.com/ankur-anand/unisondb/schemas/logrecord"
	flatbuffers "github.com/google/flatbuffers/go"
)

type ColumnData struct {
	Name  string
	Value []byte
}

type KeyValueEntry struct {
	Key   []byte
	Value []byte
}

type RowUpdateEntry struct {
	Key     []byte
	Columns []ColumnData
}

type LogOperationData struct {
	KeyValueBatchEntries *KeyValueBatchEntries
	RowUpdateEntries     *RowUpdateEntries
}

type KeyValueBatchEntries struct {
	Entries []KeyValueEntry
}

type RowUpdateEntries struct {
	Entries []RowUpdateEntry
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
	Payload         LogOperationData
}

// FBEncode encodes the provided record into flat-buffer format.
func (r *LogRecord) FBEncode() []byte {
	builder := flatbuffers.NewBuilder(1024)
	return serializeLogRecord(r, builder)
}
