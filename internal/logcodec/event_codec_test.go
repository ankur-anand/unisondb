package logcodec

import (
	"testing"
	"time"

	"github.com/ankur-anand/unisondb/schemas/logrecord"
	"github.com/brianvoe/gofakeit/v7"
	flatbuffers "github.com/google/flatbuffers/go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestEventEntrySerialization(t *testing.T) {
	event := &EventEntry{
		Pipeline:   "orders-pipeline",
		SchemaID:   "schemas/orders/v1",
		EventID:    gofakeit.UUID(),
		Operation:  logrecord.EventOperationInsert,
		OccurredAt: uint64(time.Now().UnixNano()),
		PartitionValues: map[string][]byte{
			"event_date": []byte("2024-11-18"),
		},
		Fields: map[string][]byte{
			"order_id":    []byte("123"),
			"customer_id": []byte("42"),
		},
	}

	serialized := SerializeEventEntry(event)
	record := &LogRecord{
		LSN:           111,
		HLC:           222,
		EntryType:     logrecord.LogEntryTypeEvent,
		Entries:       [][]byte{serialized},
		OperationType: logrecord.LogOperationTypeInsert,
	}
	encoded := record.FBEncode(len(serialized))
	decoded := DeserializeLogRecord(encoded)
	require.Equal(t, logrecord.LogEntryTypeEvent, decoded.EntryType)
	require.Len(t, decoded.Entries, 1)

	decodedEvent := DeserializeEventEntry(decoded.Entries[0])
	assert.Equal(t, event.Pipeline, decodedEvent.Pipeline)
	assert.Equal(t, event.SchemaID, decodedEvent.SchemaID)
	assert.Equal(t, event.EventID, decodedEvent.EventID)
	assert.Equal(t, event.Operation, decodedEvent.Operation)
	assert.Equal(t, event.OccurredAt, decodedEvent.OccurredAt)
	assert.Equal(t, event.PartitionValues, decodedEvent.PartitionValues)
	assert.Equal(t, event.Fields, decodedEvent.Fields)
}

func TestDeserializeEventEntryWithoutOptionalVectors(t *testing.T) {
	builder := flatbuffers.NewBuilder(64)
	pipeline := builder.CreateString("minimal-pipeline")
	schema := builder.CreateString("schemas/test/v1")
	eventID := builder.CreateString("evt-1")

	logrecord.EventEntryStart(builder)
	logrecord.EventEntryAddPipeline(builder, pipeline)
	logrecord.EventEntryAddSchemaId(builder, schema)
	logrecord.EventEntryAddEventId(builder, eventID)
	logrecord.EventEntryAddOperation(builder, logrecord.EventOperationDelete)
	logrecord.EventEntryAddOccurredAt(builder, 42)
	builder.Finish(logrecord.EventEntryEnd(builder))

	decoded := DeserializeEventEntry(builder.FinishedBytes())
	assert.Equal(t, "minimal-pipeline", decoded.Pipeline)
	assert.Equal(t, "schemas/test/v1", decoded.SchemaID)
	assert.Equal(t, "evt-1", decoded.EventID)
	assert.Equal(t, logrecord.EventOperationDelete, decoded.Operation)
	assert.Equal(t, uint64(42), decoded.OccurredAt)
	assert.Empty(t, decoded.PartitionValues)
	assert.Empty(t, decoded.Fields)
}

func TestSerializeEventEntryNil(t *testing.T) {
	assert.Nil(t, SerializeEventEntry(nil))
}

func TestSerializeEventEntryWithEmptyMaps(t *testing.T) {
	event := &EventEntry{
		Pipeline:        "p",
		SchemaID:        "sid",
		EventID:         "eid",
		Operation:       logrecord.EventOperationUpdate,
		OccurredAt:      999,
		PartitionValues: nil,
		Fields:          map[string][]byte{},
	}

	serialized := SerializeEventEntry(event)
	decoded := DeserializeEventEntry(serialized)

	assert.Equal(t, event.Pipeline, decoded.Pipeline)
	assert.Equal(t, event.SchemaID, decoded.SchemaID)
	assert.Equal(t, event.EventID, decoded.EventID)
	assert.Equal(t, event.Operation, decoded.Operation)
	assert.Equal(t, event.OccurredAt, decoded.OccurredAt)
	assert.Empty(t, decoded.PartitionValues)
	assert.Empty(t, decoded.Fields)
}
