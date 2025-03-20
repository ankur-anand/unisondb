package logcodec

import (
	"github.com/ankur-anand/unisondb/schemas/logrecord"
	flatbuffers "github.com/google/flatbuffers/go"
)

// SerializeRowUpdateEntry encodes a single RowUpdateEntry into FlatBuffers.
func SerializeRowUpdateEntry(rowKey []byte, columns map[string][]byte) []byte {
	builder := flatbuffers.NewBuilder(1024)

	rowKeyOffset := builder.CreateByteVector(rowKey)

	columnOffsets := make([]flatbuffers.UOffsetT, 0, len(columns))
	for name, value := range columns {
		nameOffset := builder.CreateString(name)
		valueOffset := builder.CreateByteVector(value)

		logrecord.ColumnDataStart(builder)
		logrecord.ColumnDataAddName(builder, nameOffset)
		logrecord.ColumnDataAddValue(builder, valueOffset)
		columnOffsets = append(columnOffsets, logrecord.ColumnDataEnd(builder))
	}

	logrecord.RowUpdateEntryStartColumnsVector(builder, len(columns))
	for i := len(columnOffsets) - 1; i >= 0; i-- {
		builder.PrependUOffsetT(columnOffsets[i])
	}
	columnsVector := builder.EndVector(len(columns))

	logrecord.RowUpdateEntryStart(builder)
	logrecord.RowUpdateEntryAddKey(builder, rowKeyOffset)
	logrecord.RowUpdateEntryAddColumns(builder, columnsVector)
	rowEntryOffset := logrecord.RowUpdateEntryEnd(builder)

	builder.Finish(rowEntryOffset)

	return builder.FinishedBytes()
}

// DeserializeRowUpdateEntry decodes a FlatBuffer-encoded RowUpdateEntry.
func DeserializeRowUpdateEntry(buf []byte) RowEntry {
	columns := make(map[string][]byte)

	rowEntry := logrecord.GetRootAsRowUpdateEntry(buf, 0)
	rowKey := rowEntry.KeyBytes()

	columnsLen := rowEntry.ColumnsLength()
	for i := 0; i < columnsLen; i++ {
		col := new(logrecord.ColumnData)
		if rowEntry.Columns(col, i) {
			colName := string(col.Name())
			colValue := col.ValueBytes()
			columns[colName] = colValue
		}
	}

	return RowEntry{
		Key:     rowKey,
		Columns: columns,
	}
}
