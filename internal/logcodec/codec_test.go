package logcodec

import (
	"testing"

	"github.com/ankur-anand/unisondb/schemas/logrecord"
	"github.com/brianvoe/gofakeit/v7"
	flatbuffers "github.com/google/flatbuffers/go"
	"github.com/stretchr/testify/assert"
)

func TestKeyValueSerialization(t *testing.T) {

	original := &LogRecord{
		LSN:             123456789,
		HLC:             987654321,
		OperationType:   logrecord.LogOperationTypeInsert,
		TxnState:        logrecord.TransactionStateNone,
		EntryType:       logrecord.LogEntryTypeKV,
		TxnID:           []byte("transaction-001"),
		PrevTxnWalIndex: nil,
		Payload: LogOperationData{
			KeyValueBatchEntries: &KeyValueBatchEntries{
				Entries: []KeyValueEntry{
					{
						Key:   []byte("key1"),
						Value: []byte("value1"),
					},
					{
						Key:   []byte("key2"),
						Value: []byte("value2"),
					},
				},
			},
		},
	}

	builder := flatbuffers.NewBuilder(1024)
	data := serializeLogRecord(original, builder)

	deserialized := DeserializeLogRecord(data)
	assert.Equal(t, original, deserialized, "failed to deserialize LogRecord")
}

func TestRowBatchSerialization(t *testing.T) {
	// Create a complex LogRecord with multiple row entries and multiple columns per row
	record := &LogRecord{
		LSN:             123456789,
		HLC:             987654321,
		OperationType:   logrecord.LogOperationTypeInsert,
		TxnState:        logrecord.TransactionStateNone,
		EntryType:       logrecord.LogEntryTypeRow,
		TxnID:           []byte("transaction-batch-001"),
		PrevTxnWalIndex: nil,
		Payload: LogOperationData{
			RowUpdateEntries: &RowUpdateEntries{
				Entries: []RowUpdateEntry{
					{
						Key: []byte(gofakeit.UUID()),
						Columns: []ColumnData{
							{Name: "name", Value: []byte(gofakeit.Name())},
							{Name: "email", Value: []byte(gofakeit.Email())},
							{Name: "address", Value: []byte(gofakeit.Address().Address)},
							{Name: "phone", Value: []byte(gofakeit.Phone())},
							{Name: "profile_picture", Value: []byte(gofakeit.LetterN(2048))},
						},
					},
					{
						Key: []byte(gofakeit.UUID()),
						Columns: []ColumnData{
							{Name: "name", Value: []byte(gofakeit.Name())},
							{Name: "email", Value: []byte(gofakeit.Email())},
							{Name: "is_verified", Value: []byte{0x01}},
							{Name: "signup_date", Value: []byte(gofakeit.Date().String())},
						},
					},
					{
						Key: []byte(gofakeit.UUID()),
						Columns: []ColumnData{
							{Name: "uid_id", Value: []byte(gofakeit.UUID())},
							{Name: "status", Value: []byte("active")},
						},
					},
				},
			},
		},
	}

	data := serializeLogRecord(record, flatbuffers.NewBuilder(1024))

	deserializedRecord := DeserializeLogRecord(data)
	assert.Equal(t, record, deserializedRecord, "failed to deserialize LogRecord")
}

func BenchmarkSerialization(b *testing.B) {
	original := &LogRecord{
		LSN:             123456789,
		HLC:             987654321,
		OperationType:   logrecord.LogOperationTypeInsert,
		TxnState:        logrecord.TransactionStateNone,
		EntryType:       logrecord.LogEntryTypeKV,
		TxnID:           []byte("transaction-001"),
		PrevTxnWalIndex: nil,
		Payload: LogOperationData{
			KeyValueBatchEntries: &KeyValueBatchEntries{
				Entries: []KeyValueEntry{
					{
						Key:   []byte("key1"),
						Value: []byte("value1"),
					},
					{
						Key:   []byte("key2"),
						Value: []byte("value2"),
					},
				},
			},
		},
	}

	builder := flatbuffers.NewBuilder(1024)
	b.ResetTimer()
	var serializedData []byte
	var err error

	for i := 0; i < b.N; i++ {
		serializedData = serializeLogRecord(original, builder)
		assert.NoError(b, err, "failed to serialize LogRecord")
	}

	if len(serializedData) == 0 {
		b.Fatal("Serialization produced empty data")
	}
}

func BenchmarkDeserialization(b *testing.B) {
	original := &LogRecord{
		LSN:             123456789,
		HLC:             987654321,
		OperationType:   logrecord.LogOperationTypeInsert,
		TxnState:        logrecord.TransactionStateNone,
		EntryType:       logrecord.LogEntryTypeKV,
		TxnID:           []byte("transaction-001"),
		PrevTxnWalIndex: nil,
		Payload: LogOperationData{
			KeyValueBatchEntries: &KeyValueBatchEntries{
				Entries: []KeyValueEntry{
					{
						Key:   []byte("key1"),
						Value: []byte("value1"),
					},
					{
						Key:   []byte("key2"),
						Value: []byte("value2"),
					},
				},
			},
		},
	}

	builder := flatbuffers.NewBuilder(1024)
	data := serializeLogRecord(original, builder)

	b.ResetTimer()

	var deserializedData *LogRecord

	for i := 0; i < b.N; i++ {
		deserializedData = DeserializeLogRecord(data)
	}

	if deserializedData == nil {
		b.Fatal("Deserialization produced empty data")
	}
}
