package logcodec

import (
	"testing"
	"time"

	"github.com/ankur-anand/unisondb/schemas/logrecord"
	"github.com/brianvoe/gofakeit/v7"
	flatbuffers "github.com/google/flatbuffers/go"
	"github.com/stretchr/testify/assert"
)

func TestKeyValueSerialization(t *testing.T) {

	entries := []KeyValueEntry{
		{
			Key:   []byte("key1"),
			Value: []byte("value1"),
		},
		{
			Key:   []byte("key2"),
			Value: []byte("value2"),
		},
	}

	kvEncoded := make([][]byte, 0, len(entries))
	sizeHint := 0
	for _, entry := range entries {
		encoded := SerializeKVEntry(entry.Key, entry.Value)
		sizeHint += len(encoded)
		kvEncoded = append(kvEncoded, encoded)

	}

	original := &LogRecord{
		LSN:           123456789,
		HLC:           987654321,
		OperationType: logrecord.LogOperationTypeInsert,
		TxnState:      logrecord.TransactionStateNone,
		EntryType:     logrecord.LogEntryTypeKV,
		TxnID:         []byte("transaction-001"),
		Entries:       kvEncoded,
	}

	data := original.FBEncode(sizeHint)
	deserialized := DeserializeLogRecord(data)
	assert.Equal(t, original, deserialized, "failed to deserialize LogRecord")

	for i, entry := range deserialized.Entries {
		expected := entries[i]
		got := DeserializeKVEntry(entry)
		assert.Equal(t, expected, got, "failed to deserialize KVEntry")
	}
}

func TestRowBatchSerialization(t *testing.T) {
	entries := []RowEntry{
		{
			Key: []byte(gofakeit.UUID()),
			Columns: map[string][]byte{
				"name":            []byte(gofakeit.Name()),
				"email":           []byte(gofakeit.Email()),
				"address":         []byte(gofakeit.Address().Address),
				"phone":           []byte(gofakeit.Phone()),
				"profile_picture": []byte(gofakeit.LetterN(2048)),
			},
		},
		{
			Key: []byte(gofakeit.UUID()),
			Columns: map[string][]byte{
				"name":        []byte(gofakeit.Name()),
				"email":       []byte(gofakeit.Email()),
				"is_verified": {0x01},
				"signup_date": []byte(gofakeit.Date().Format(time.RFC3339)),
			},
		},
		{
			Key: []byte(gofakeit.UUID()),
			Columns: map[string][]byte{
				"uid_id": []byte(gofakeit.UUID()),
				"status": []byte("active"),
			},
		},
	}

	serializedEntries := make([][]byte, 0, len(entries))
	for _, entry := range entries {
		serializedEntries = append(serializedEntries, SerializeRowUpdateEntry(entry.Key, entry.Columns))
	}

	record := &LogRecord{
		LSN:             123456789,
		HLC:             987654321,
		OperationType:   logrecord.LogOperationTypeInsert,
		TxnState:        logrecord.TransactionStateNone,
		EntryType:       logrecord.LogEntryTypeRow,
		TxnID:           []byte("transaction-batch-001"),
		PrevTxnWalIndex: nil,
		Entries:         serializedEntries,
	}

	data := serializeLogRecord(record, flatbuffers.NewBuilder(1024))
	deserializedRecord := DeserializeLogRecord(data)
	assert.Equal(t, serializedEntries, deserializedRecord.Entries, "failed to deserialize LogRecord")
	for i, entry := range deserializedRecord.Entries {
		expected := entries[i]
		rowData := DeserializeRowUpdateEntry(entry)
		assert.Equal(t, expected.Key, rowData.Key, "failed to deserialize RowUpdateEntry")
		assert.Equal(t, expected.Columns, rowData.Columns, "failed to deserialize RowUpdateEntry")
	}
}

func BenchmarkLogSerialization(b *testing.B) {
	entries := []RowEntry{
		{
			Key: []byte(gofakeit.UUID()),
			Columns: map[string][]byte{
				"name":            []byte(gofakeit.Name()),
				"email":           []byte(gofakeit.Email()),
				"address":         []byte(gofakeit.Address().Address),
				"phone":           []byte(gofakeit.Phone()),
				"profile_picture": []byte(gofakeit.LetterN(2048)),
			},
		},
		{
			Key: []byte(gofakeit.UUID()),
			Columns: map[string][]byte{
				"name":        []byte(gofakeit.Name()),
				"email":       []byte(gofakeit.Email()),
				"is_verified": {0x01},
				"signup_date": []byte(gofakeit.Date().Format(time.RFC3339)),
			},
		},
		{
			Key: []byte(gofakeit.UUID()),
			Columns: map[string][]byte{
				"uid_id": []byte(gofakeit.UUID()),
				"status": []byte("active"),
			},
		},
	}

	serializedEntries := make([][]byte, 0, len(entries))
	sizeHint := 0
	for _, entry := range entries {
		encoded := SerializeRowUpdateEntry(entry.Key, entry.Columns)
		sizeHint += len(encoded)
		serializedEntries = append(serializedEntries, encoded)
	}

	record := &LogRecord{
		LSN:             123456789,
		HLC:             987654321,
		OperationType:   logrecord.LogOperationTypeInsert,
		TxnState:        logrecord.TransactionStateNone,
		EntryType:       logrecord.LogEntryTypeRow,
		TxnID:           []byte("transaction-batch-001"),
		PrevTxnWalIndex: nil,
		Entries:         serializedEntries,
	}
	b.ResetTimer()
	var encoded []byte
	for i := 0; i < b.N; i++ {
		encoded = record.FBEncode(sizeHint)
	}

	if encoded == nil {
		b.FailNow()
	}

}

func BenchmarkLogDeserialization(b *testing.B) {
	entries := []RowEntry{
		{
			Key: []byte(gofakeit.UUID()),
			Columns: map[string][]byte{
				"name":            []byte(gofakeit.Name()),
				"email":           []byte(gofakeit.Email()),
				"address":         []byte(gofakeit.Address().Address),
				"phone":           []byte(gofakeit.Phone()),
				"profile_picture": []byte(gofakeit.LetterN(2048)),
			},
		},
		{
			Key: []byte(gofakeit.UUID()),
			Columns: map[string][]byte{
				"name":        []byte(gofakeit.Name()),
				"email":       []byte(gofakeit.Email()),
				"is_verified": {0x01},
				"signup_date": []byte(gofakeit.Date().Format(time.RFC3339)),
			},
		},
		{
			Key: []byte(gofakeit.UUID()),
			Columns: map[string][]byte{
				"uid_id": []byte(gofakeit.UUID()),
				"status": []byte("active"),
			},
		},
	}

	serializedEntries := make([][]byte, 0, len(entries))
	sizeHint := 0
	for _, entry := range entries {
		encoded := SerializeRowUpdateEntry(entry.Key, entry.Columns)
		sizeHint += len(encoded)
		serializedEntries = append(serializedEntries, encoded)
	}

	record := &LogRecord{
		LSN:             123456789,
		HLC:             987654321,
		OperationType:   logrecord.LogOperationTypeInsert,
		TxnState:        logrecord.TransactionStateNone,
		EntryType:       logrecord.LogEntryTypeRow,
		TxnID:           []byte("transaction-batch-001"),
		PrevTxnWalIndex: nil,
		Entries:         serializedEntries,
	}
	data := serializeLogRecord(record, flatbuffers.NewBuilder(sizeHint))
	b.ResetTimer()

	var deserializedData *LogRecord

	for i := 0; i < b.N; i++ {
		deserializedData = DeserializeLogRecord(data)
	}

	if deserializedData == nil {
		b.Fatal("Deserialization produced empty data")
	}
}
