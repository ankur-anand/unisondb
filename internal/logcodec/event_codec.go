package logcodec

import (
	"github.com/ankur-anand/unisondb/schemas/logrecord"
	flatbuffers "github.com/google/flatbuffers/go"
)

// SerializeEventEntry encodes an EventEntry into FlatBuffers.
func SerializeEventEntry(entry *EventEntry) []byte {
	if entry == nil {
		return nil
	}
	builder := flatbuffers.NewBuilder(256)

	pipelineOffset := builder.CreateString(entry.Pipeline)
	schemaOffset := builder.CreateString(entry.SchemaID)
	eventIDOffset := builder.CreateString(entry.EventID)

	partitionVector := buildColumnDataVector(builder, entry.PartitionValues, logrecord.EventEntryStartPartitionValuesVector)
	fieldsVector := buildColumnDataVector(builder, entry.Fields, logrecord.EventEntryStartFieldsVector)

	logrecord.EventEntryStart(builder)
	logrecord.EventEntryAddPipeline(builder, pipelineOffset)
	logrecord.EventEntryAddSchemaId(builder, schemaOffset)
	logrecord.EventEntryAddEventId(builder, eventIDOffset)
	logrecord.EventEntryAddOperation(builder, entry.Operation)
	logrecord.EventEntryAddOccurredAt(builder, entry.OccurredAt)
	if partitionVector != 0 {
		logrecord.EventEntryAddPartitionValues(builder, partitionVector)
	}
	if fieldsVector != 0 {
		logrecord.EventEntryAddFields(builder, fieldsVector)
	}
	builder.Finish(logrecord.EventEntryEnd(builder))

	return builder.FinishedBytes()
}

// DeserializeEventEntry decodes a FlatBuffers EventEntry.
func DeserializeEventEntry(buf []byte) EventEntry {
	fbEntry := logrecord.GetRootAsEventEntry(buf, 0)
	entry := EventEntry{
		Pipeline:        string(fbEntry.Pipeline()),
		SchemaID:        string(fbEntry.SchemaId()),
		EventID:         string(fbEntry.EventId()),
		Operation:       fbEntry.Operation(),
		OccurredAt:      fbEntry.OccurredAt(),
		PartitionValues: make(map[string][]byte, fbEntry.PartitionValuesLength()),
		Fields:          make(map[string][]byte, fbEntry.FieldsLength()),
	}

	col := new(logrecord.ColumnData)
	for i := 0; i < fbEntry.PartitionValuesLength(); i++ {
		if fbEntry.PartitionValues(col, i) {
			entry.PartitionValues[string(col.Name())] = col.ValueBytes()
		}
	}
	for i := 0; i < fbEntry.FieldsLength(); i++ {
		if fbEntry.Fields(col, i) {
			entry.Fields[string(col.Name())] = col.ValueBytes()
		}
	}

	return entry
}

func buildColumnDataVector(builder *flatbuffers.Builder, columns map[string][]byte, start func(*flatbuffers.Builder, int) flatbuffers.UOffsetT) flatbuffers.UOffsetT {
	if len(columns) == 0 {
		return 0
	}
	offsets := make([]flatbuffers.UOffsetT, 0, len(columns))
	for name, value := range columns {
		nameOffset := builder.CreateString(name)
		valueOffset := builder.CreateByteVector(value)
		logrecord.ColumnDataStart(builder)
		logrecord.ColumnDataAddName(builder, nameOffset)
		logrecord.ColumnDataAddValue(builder, valueOffset)
		offsets = append(offsets, logrecord.ColumnDataEnd(builder))
	}
	start(builder, len(columns))
	//  fb builds vectors backwards.
	for i := len(offsets) - 1; i >= 0; i-- {
		builder.PrependUOffsetT(offsets[i])
	}
	return builder.EndVector(len(columns))
}
