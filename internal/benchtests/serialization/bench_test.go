package serialization

import (
	"fmt"
	"math/rand"
	"testing"

	"github.com/ankur-anand/kvalchemy/internal/benchtests/serialization/logrecord"
	flatbuffers "github.com/google/flatbuffers/go"
	"google.golang.org/protobuf/proto"
)

var (
	sizes = []int{1 << 10, 5 << 10, 10 << 10, 20 << 10, 50 << 10, 100 << 10, 500 << 10, 1 << 20}
)

func prepareFlatBufferRowUpdateEntry(builder *flatbuffers.Builder) []byte {
	builder.Reset()
	columns := []struct {
		name  string
		value []byte
	}{
		{"column1", []byte("value")},
		{"column2", []byte("value")},
		{"column3", []byte("value")},
		{"column4", []byte("value")},
		{"column5", []byte("value")},
		{"column6", []byte("value")},
		{"column7", []byte("value")},
		{"column8", []byte("value")},
		{"column9", []byte("value")},
		{"column10", []byte("value")},
		{name: "colums11", value: generateLargeRandomBytes(20 << 10)},
	}

	nameOffsets := make([]flatbuffers.UOffsetT, len(columns))
	valueOffsets := make([]flatbuffers.UOffsetT, len(columns))

	// Create strings and byte vectors first (reduces context switching)
	for i, col := range columns {
		nameOffsets[i] = builder.CreateString(col.name)
		valueOffsets[i] = builder.CreateByteVector(col.value)
	}

	// Create ColumnData objects
	columnOffsets := make([]flatbuffers.UOffsetT, len(columns))
	for i := len(columns) - 1; i >= 0; i-- { // Reverse order for FlatBuffers
		logrecord.ColumnDataStart(builder)
		logrecord.ColumnDataAddName(builder, nameOffsets[i])
		logrecord.ColumnDataAddValue(builder, valueOffsets[i])
		columnOffsets[i] = logrecord.ColumnDataEnd(builder)
	}

	// Store in vector in one pass
	logrecord.RowUpdateEntryStartColumnsVector(builder, len(columns))
	for i := len(columnOffsets) - 1; i >= 0; i-- {
		builder.PrependUOffsetT(columnOffsets[i])
	}
	columnsVec := builder.EndVector(len(columns))

	rowKey := builder.CreateByteVector([]byte("example-key"))

	logrecord.RowUpdateEntryStart(builder)
	logrecord.RowUpdateEntryAddKey(builder, rowKey)
	logrecord.RowUpdateEntryAddColumns(builder, columnsVec)
	entry := logrecord.RowUpdateEntryEnd(builder)

	builder.Finish(entry)
	return builder.FinishedBytes()
}

func BenchmarkFlatBuffersEncode(b *testing.B) {
	builder := flatbuffers.NewBuilder(64 * 1024)
	data := prepareFlatBufferRowUpdateEntry(builder)
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		data := prepareFlatBufferRowUpdateEntry(builder)
		b.SetBytes(int64(len(data)))
	}

	b.Log(len(data))

}

func BenchmarkFlatBuffersDecode(b *testing.B) {
	builder := flatbuffers.NewBuilder(64 * 1024)
	data := prepareFlatBufferRowUpdateEntry(builder)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		rowEntry := logrecord.GetRootAsRowUpdateEntry(data, 0)
		_ = rowEntry.KeyBytes()
		colsLen := rowEntry.ColumnsLength()
		for j := 0; j < colsLen; j++ {
			var col logrecord.ColumnData
			if rowEntry.Columns(&col, j) {
				_ = col.Name()
				_ = col.ValueBytes()
			}
		}
		b.SetBytes(int64(len(data)))
	}
}

// consistent with the new 'Pb' suffix definitions
func prepareProtoRowUpdateEntry() *RowUpdateEntryPb {
	return &RowUpdateEntryPb{
		Key: []byte("example-key"),
		Columns: []*ColumnDataPb{
			{Name: "column1", Value: []byte("value")},
			{Name: "column2", Value: []byte("value")},
			{Name: "column3", Value: []byte("value")},
			{Name: "column4", Value: []byte("value")},
			{Name: "column5", Value: []byte("value")},
			{Name: "column6", Value: []byte("value")},
			{Name: "column7", Value: []byte("value")},
			{Name: "column8", Value: []byte("value")},
			{Name: "column9", Value: []byte("value")},
			{Name: "column10", Value: []byte("value")},
			{Name: "colums11", Value: generateLargeRandomBytes(20 << 10)},
		},
	}
}

func BenchmarkProtoBufEncode(b *testing.B) {
	rowEntry := prepareProtoRowUpdateEntry()
	data, _ := proto.Marshal(rowEntry)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		data, err := proto.Marshal(rowEntry)
		if err != nil {
			b.Fatal(err)
		}
		b.SetBytes(int64(len(data)))
	}
	b.Log(len(data))
}

func BenchmarkProtoBufDecode(b *testing.B) {
	rowEntry := prepareProtoRowUpdateEntry()
	data, err := proto.Marshal(rowEntry)
	if err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var decoded RowUpdateEntryPb
		if err := proto.Unmarshal(data, &decoded); err != nil {
			b.Fatal(err)
		}

		// Access fields to mimic realistic decode operation
		_ = decoded.Key
		for _, col := range decoded.Columns {
			_ = col.Name
			_ = col.Value
		}

		b.SetBytes(int64(len(data)))
	}
}

// Prepare serialized individual row entry
func serializeRow(key string, columns map[string]string) []byte {
	builder := flatbuffers.NewBuilder(256)

	var colOffsets []flatbuffers.UOffsetT
	for name, val := range columns {
		nameOffset := builder.CreateString(name)
		valueOffset := builder.CreateByteVector([]byte(val))
		logrecord.ColumnDataStart(builder)
		logrecord.ColumnDataAddName(builder, nameOffset)
		logrecord.ColumnDataAddValue(builder, valueOffset)
		colOffsets = append(colOffsets, logrecord.ColumnDataEnd(builder))
	}

	logrecord.RowUpdateEntryStartColumnsVector(builder, len(colOffsets))
	for i := len(colOffsets) - 1; i >= 0; i-- {
		builder.PrependUOffsetT(colOffsets[i])
	}
	colsVec := builder.EndVector(len(colOffsets))

	keyOffset := builder.CreateByteVector([]byte(key))
	logrecord.RowUpdateEntryStart(builder)
	logrecord.RowUpdateEntryAddKey(builder, keyOffset)
	logrecord.RowUpdateEntryAddColumns(builder, colsVec)
	rowEntryOffset := logrecord.RowUpdateEntryEnd(builder)

	builder.Finish(rowEntryOffset)
	return builder.FinishedBytes()
}

// Prepare main LogRecord with encoded rows
func serializeLogRecord(lsn, hlc uint64, encodedRows [][]byte) []byte {
	builder := flatbuffers.NewBuilder(1024)

	encodedOffsets := make([]flatbuffers.UOffsetT, len(encodedRows))
	for i, row := range encodedRows {
		rowDataOffset := builder.CreateByteVector(row)
		logrecord.RowUpdateEncodedStart(builder)
		logrecord.RowUpdateEncodedAddValue(builder, rowDataOffset)
		encodedOffsets[i] = logrecord.RowUpdateEncodedEnd(builder)
	}

	logrecord.RowUpdateEntriesStartEntriesVector(builder, len(encodedOffsets))
	for i := len(encodedOffsets) - 1; i >= 0; i-- {
		builder.PrependUOffsetT(encodedOffsets[i])
	}
	entriesVec := builder.EndVector(len(encodedOffsets))

	logrecord.RowUpdateEntriesStart(builder)
	logrecord.RowUpdateEntriesAddEntries(builder, entriesVec)
	payloadOffset := logrecord.RowUpdateEntriesEnd(builder)

	txnID := builder.CreateByteVector([]byte("txn-123"))
	prevTxnID := builder.CreateByteVector([]byte("prev-456"))

	logrecord.LogRecordStart(builder)
	logrecord.LogRecordAddLsn(builder, lsn)
	logrecord.LogRecordAddHlc(builder, hlc)
	logrecord.LogRecordAddCrc32Checksum(builder, 0xDEADBEEF)
	logrecord.LogRecordAddOperationType(builder, logrecord.LogOperationTypeInsert)
	logrecord.LogRecordAddTxnState(builder, logrecord.TransactionStateCommit)
	logrecord.LogRecordAddEntryType(builder, logrecord.LogEntryTypeRow)
	logrecord.LogRecordAddTxnId(builder, txnID)
	logrecord.LogRecordAddPrevTxnWalIndex(builder, prevTxnID)
	logrecord.LogRecordAddPayloadType(builder, logrecord.LogOperationDataRowUpdateEntries)
	logrecord.LogRecordAddPayload(builder, payloadOffset)
	logRecordOffset := logrecord.LogRecordEnd(builder)

	builder.Finish(logRecordOffset)
	return builder.FinishedBytes()
}

// --- Benchmark Functions ---

func BenchmarkEncodedRowsLogRecordEncode(b *testing.B) {
	encodedRows := [][]byte{
		serializeRow("key1", map[string]string{"col1": "val1", "col2": "val2"}),
		serializeRow("key2", map[string]string{"col3": "val3", "col4": "val4"}),
	}
	data := serializeLogRecord(123, 456, encodedRows)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		data := serializeLogRecord(123, 456, encodedRows)
		b.SetBytes(int64(len(data)))
	}

	fmt.Printf("FlatBuffer LogRecord size: %d bytes\n", len(data))
}

func BenchmarkEncodedRowsLogRecordDecode(b *testing.B) {
	encodedRows := [][]byte{
		serializeRow("key1", map[string]string{"col1": "val1", "col2": "val2"}),
		serializeRow("key2", map[string]string{"col3": "val3", "col4": "val4"}),
	}
	data := serializeLogRecord(123, 456, encodedRows)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		record := logrecord.GetRootAsLogRecord(data, 0)

		var payloadTable flatbuffers.Table
		if record.Payload(&payloadTable) {
			var rowUpdateEntries logrecord.RowUpdateEntries
			rowUpdateEntries.Init(payloadTable.Bytes, payloadTable.Pos)

			var rowEncoded logrecord.RowUpdateEncoded
			for j := 0; j < rowUpdateEntries.EntriesLength(); j++ {
				if rowUpdateEntries.Entries(&rowEncoded, j) {
					rowEntryBytes := rowEncoded.ValueBytes()
					rowEntry := logrecord.GetRootAsRowUpdateEntry(rowEntryBytes, 0)
					_ = rowEntry.KeyBytes()

					var col logrecord.ColumnData
					for k := 0; k < rowEntry.ColumnsLength(); k++ {
						rowEntry.Columns(&col, k)
						_, _ = col.Name(), col.ValueBytes()
					}
				}
			}
		}
		b.SetBytes(int64(len(data)))
	}
}

// Serialize a single RowUpdateEntry separately.
func serializeProtoRow(key string, columns map[string]string) []byte {
	rowEntry := &RowUpdateEntryPb{
		Key: []byte(key),
	}
	for name, val := range columns {
		rowEntry.Columns = append(rowEntry.Columns, &ColumnDataPb{
			Name:  name,
			Value: []byte(val),
		})
	}

	data, _ := proto.Marshal(rowEntry)
	return data
}

// Serialize LogRecord embedding pre-encoded RowUpdateEntries.
func serializeProtoLogRecord(lsn, hlc uint64, encodedRows [][]byte) []byte {
	rowUpdateEntries := &RowUpdateEntriesPb{}
	for _, encodedRow := range encodedRows {
		rowUpdateEntries.Entries = append(rowUpdateEntries.Entries, &RowUpdateEncodedPb{
			Value: encodedRow,
		})
	}

	logRecord := &LogRecordPb{
		Lsn:             lsn,
		Hlc:             hlc,
		Crc32Checksum:   0xDEADBEEF,
		OperationType:   LogOperationTypePb_LOG_OPERATION_TYPE_PB_INSERT,
		TxnState:        TransactionStatePb_TRANSACTION_STATE_PB_COMMIT,
		EntryType:       LogEntryTypePb_LOG_ENTRY_TYPE_PB_ROW,
		TxnId:           []byte("txn-123"),
		PrevTxnWalIndex: []byte("prev-456"),
		Payload: &LogOperationDataPb{
			Payload: &LogOperationDataPb_RowUpdateEntries{
				RowUpdateEntries: rowUpdateEntries,
			},
		},
	}

	data, _ := proto.Marshal(logRecord)
	return data
}

// Benchmark Encoding
func BenchmarkProtoEncodedRowsLogRecordEncode(b *testing.B) {
	encodedRows := [][]byte{
		serializeProtoRow("key1", map[string]string{"col1": "val1", "col2": "val2"}),
		serializeProtoRow("key2", map[string]string{"col3": "val3", "col4": "val4"}),
	}
	data := serializeProtoLogRecord(123, 456, encodedRows)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		data := serializeProtoLogRecord(123, 456, encodedRows)
		b.SetBytes(int64(len(data)))
	}
	fmt.Printf("ProtoBuf LogRecord size: %d bytes\n", len(data))
}

// Benchmark Decoding
func BenchmarkProtoEncodedRowsLogRecordDecode(b *testing.B) {
	encodedRows := [][]byte{
		serializeProtoRow("key1", map[string]string{"col1": "val1", "col2": "val2"}),
		serializeProtoRow("key2", map[string]string{"col3": "val3", "col4": "val4"}),
	}
	data := serializeProtoLogRecord(123, 456, encodedRows)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var logRecord LogRecordPb
		_ = proto.Unmarshal(data, &logRecord)

		// Correct handling of oneof payload
		switch payload := logRecord.Payload.Payload.(type) {
		case *LogOperationDataPb_RowUpdateEntries:
			for _, encodedRow := range payload.RowUpdateEntries.Entries {
				var rowEntry RowUpdateEntryPb
				_ = proto.Unmarshal(encodedRow.Value, &rowEntry)
				_ = rowEntry.Key
				for _, col := range rowEntry.Columns {
					_, _ = col.Name, col.Value
				}
			}
		default:
			b.Fatal("unexpected payload type")
		}

		b.SetBytes(int64(len(data)))
	}
}

// generateLargeRandomBytes returns random bytes of given size.
func generateLargeRandomBytes(size int) []byte {
	data := make([]byte, size)
	rand.Read(data)
	return data
}

// --- FlatBuffers serialization ---

func serializeFlatBufferWithLargeValue(value []byte) []byte {
	// Step 1: serialize KeyValueEntry separately as a root FlatBuffer
	entryBuilder := flatbuffers.NewBuilder(len(value) + 128)
	keyOffset := entryBuilder.CreateByteVector([]byte("key"))
	valueOffset := entryBuilder.CreateByteVector(value)

	logrecord.KeyValueEntryStart(entryBuilder)
	logrecord.KeyValueEntryAddKey(entryBuilder, keyOffset)
	logrecord.KeyValueEntryAddValue(entryBuilder, valueOffset)
	entry := logrecord.KeyValueEntryEnd(entryBuilder)
	entryBuilder.Finish(entry)

	// Get full serialized bytes (with root offset!)
	encodedEntryBytes := entryBuilder.FinishedBytes()

	// Step 2: serialize the LogRecord embedding this entry
	builder := flatbuffers.NewBuilder(len(encodedEntryBytes) + 128)

	encodedValueOffset := builder.CreateByteVector(encodedEntryBytes)
	logrecord.KeyValueEncodedStart(builder)
	logrecord.KeyValueEncodedAddValue(builder, encodedValueOffset)
	encodedEntry := logrecord.KeyValueEncodedEnd(builder)

	logrecord.KeyValueBatchEntriesStartEntriesVector(builder, 1)
	builder.PrependUOffsetT(encodedEntry)
	entriesVec := builder.EndVector(1)

	logrecord.KeyValueBatchEntriesStart(builder)
	logrecord.KeyValueBatchEntriesAddEntries(builder, entriesVec)
	kvBatch := logrecord.KeyValueBatchEntriesEnd(builder)

	logrecord.LogRecordStart(builder)
	logrecord.LogRecordAddPayloadType(builder, logrecord.LogOperationDataKeyValueBatchEntries)
	logrecord.LogRecordAddPayload(builder, kvBatch)
	logrecord.LogRecordAddLsn(builder, 123)
	logrecord.LogRecordAddHlc(builder, 456)
	logrecord.LogRecordAddCrc32Checksum(builder, 0xDEADBEEF)
	logrecord.LogRecordAddOperationType(builder, logrecord.LogOperationTypeInsert)
	logrecord.LogRecordAddTxnState(builder, logrecord.TransactionStateCommit)
	logrecord.LogRecordAddEntryType(builder, logrecord.LogEntryTypeKV)

	logRecord := logrecord.LogRecordEnd(builder)
	builder.Finish(logRecord)

	return builder.FinishedBytes()
}

// --- Protobuf serialization ---

func serializeProtoWithLargeValue(value []byte) []byte {
	record := &LogRecordPb{
		Lsn:             1,
		Hlc:             1,
		Crc32Checksum:   0xDEADBEEF,
		OperationType:   LogOperationTypePb_LOG_OPERATION_TYPE_PB_INSERT,
		TxnState:        TransactionStatePb_TRANSACTION_STATE_PB_COMMIT,
		EntryType:       LogEntryTypePb_LOG_ENTRY_TYPE_PB_ROW,
		TxnId:           []byte("txn-large"),
		PrevTxnWalIndex: []byte("prev-large"),
		Payload: &LogOperationDataPb{
			Payload: &LogOperationDataPb_RowUpdateEntries{
				RowUpdateEntries: &RowUpdateEntriesPb{
					Entries: []*RowUpdateEncodedPb{
						{Value: value},
					},
				},
			},
		},
	}
	data, _ := proto.Marshal(record)
	return data
}

// BenchmarkFlatBuffersLargeValue tests FlatBuffers with large payloads.
func BenchmarkFlatBuffersLargeValue(b *testing.B) {

	for _, size := range sizes {
		b.Run(formatSize(size), func(b *testing.B) {
			data := generateLargeRandomBytes(size)
			var serialized []byte
			for i := 0; i < b.N; i++ {
				serialized = serializeProtoWithLargeValue(data)
				b.SetBytes(int64(len(serialized)))
			}
			b.Logf("FlatBuffers serialized size (%s): %d bytes", formatSize(size), len(serialized))
		})
	}
}

//func BenchmarkFlatBuffersDeserialization(b *testing.B) {
//	sizes := []int{10 << 10, 100 << 10, 500 << 10, 1 << 20}
//
//	for _, size := range sizes {
//		b.Run(formatSize(size), func(b *testing.B) {
//			data := serializeFlatBufferWithLargeValue(generateLargeRandomBytes(size))
//
//			b.ResetTimer()
//			for i := 0; i < b.N; i++ {
//				record := logrecord.GetRootAsLogRecord(data, 0)
//
//				payloadTable := new(flatbuffers.Table)
//				if record.Payload(payloadTable) {
//					var kvBatch logrecord.KeyValueBatchEntries
//					kvBatch.Init(payloadTable.Bytes, payloadTable.Pos)
//
//					var kvEncoded logrecord.KeyValueEncoded
//					if kvBatch.Entries(&kvEncoded, 0) {
//						encodedBytes := kvEncoded.ValueBytes()
//
//						// Proper way: decode embedded table (non-root object)
//						var kvEntry logrecord.KeyValueEntry
//						kvEntry.Init(encodedBytes, flatbuffers.GetUOffsetT(encodedBytes))
//
//						key := kvEntry.KeyBytes()
//						value := kvEntry.ValueBytes()
//
//						_, _ = key, value // use for benchmark
//					}
//				}
//
//				b.SetBytes(int64(len(data)))
//			}
//		})
//	}
//}

func BenchmarkFlatBuffersDeserialization(b *testing.B) {

	for _, size := range sizes {
		b.Run(formatSize(size), func(b *testing.B) {
			data := serializeFlatBufferWithLargeValue(generateLargeRandomBytes(size))

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				record := logrecord.GetRootAsLogRecord(data, 0)
				payloadTable := new(flatbuffers.Table)

				if record.Payload(payloadTable) {
					var kvBatch logrecord.KeyValueBatchEntries
					kvBatch.Init(payloadTable.Bytes, payloadTable.Pos)

					var kvEncoded logrecord.KeyValueEncoded
					if kvBatch.Entries(&kvEncoded, 0) {
						encodedBytes := kvEncoded.ValueBytes()

						// Now this is correct because bytes include root offset
						kvEntry := logrecord.GetRootAsKeyValueEntry(encodedBytes, 0)

						key := kvEntry.KeyBytes()
						value := kvEntry.ValueBytes()

						_, _ = key, value
					}
				}

				b.SetBytes(int64(len(data)))
			}
		})
	}
}

// BenchmarkProtoBufLargeValue tests Protobuf with large payloads.
func BenchmarkProtoBufLargeValue(b *testing.B) {

	for _, size := range sizes {
		b.Run(formatSize(size), func(b *testing.B) {
			largeValue := generateLargeRandomBytes(size) // 1MB payload

			var serialized []byte
			for i := 0; i < b.N; i++ {
				serialized = serializeProtoWithLargeValue(largeValue)
				b.SetBytes(int64(len(serialized)))
			}

			b.Logf("ProtoBuf serialized size (%s): %d bytes", formatSize(size), len(serialized))
		})
	}
}

func BenchmarkProtoBufDeserialization(b *testing.B) {

	for _, size := range sizes {
		b.Run(formatSize(size), func(b *testing.B) {
			data := serializeProtoWithLargeValue(generateLargeRandomBytes(size))
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				var record LogRecordPb
				proto.Unmarshal(data, &record)
				payload := record.Payload.GetRowUpdateEntries()
				_ = payload.Entries[0].Value
				b.SetBytes(int64(len(data)))
			}
		})
	}
}

func formatSize(size int) string {
	if size >= 1024*1024 {
		return fmt.Sprintf("%dMB", size/(1024*1024))
	}
	if size >= 1024 {
		return fmt.Sprintf("%dKB", size/1024)
	}
	return fmt.Sprintf("%dB", size)
}
