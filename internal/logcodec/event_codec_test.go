package logcodec

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/ankur-anand/unisondb/schemas/logrecord"
	"github.com/brianvoe/gofakeit/v7"
	flatbuffers "github.com/google/flatbuffers/go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestEventEntrySerialization(t *testing.T) {
	// Create a sample payload (JSON for this example)
	payload, _ := json.Marshal(map[string]any{
		"order_id":    "123",
		"customer_id": "42",
		"amount":      99.99,
	})

	event := &EventEntry{
		EventID:    gofakeit.UUID(),
		EventType:  "order.created",
		OccurredAt: uint64(time.Now().UnixNano()),
		Payload:    payload,
		Metadata: []KeyValueEntry{
			{Key: []byte("schema_id"), Value: []byte("schemas/orders/v1")},
			{Key: []byte("partition.date"), Value: []byte("2024-11-18")},
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
	assert.Equal(t, event.EventID, decodedEvent.EventID)
	assert.Equal(t, event.EventType, decodedEvent.EventType)
	assert.Equal(t, event.OccurredAt, decodedEvent.OccurredAt)
	assert.Equal(t, event.Payload, decodedEvent.Payload)
	require.Len(t, decodedEvent.Metadata, 2)
	assert.Equal(t, event.Metadata[0].Key, decodedEvent.Metadata[0].Key)
	assert.Equal(t, event.Metadata[0].Value, decodedEvent.Metadata[0].Value)
	assert.Equal(t, event.Metadata[1].Key, decodedEvent.Metadata[1].Key)
	assert.Equal(t, event.Metadata[1].Value, decodedEvent.Metadata[1].Value)
}

func TestDeserializeEventEntryWithoutOptionalFields(t *testing.T) {
	builder := flatbuffers.NewBuilder(64)
	eventID := builder.CreateString("evt-1")
	eventType := builder.CreateString("user.deleted")

	logrecord.EventEntryStart(builder)
	logrecord.EventEntryAddEventId(builder, eventID)
	logrecord.EventEntryAddEventType(builder, eventType)
	logrecord.EventEntryAddOccurredAt(builder, 42)
	builder.Finish(logrecord.EventEntryEnd(builder))

	decoded := DeserializeEventEntry(builder.FinishedBytes())
	assert.Equal(t, "evt-1", decoded.EventID)
	assert.Equal(t, "user.deleted", decoded.EventType)
	assert.Equal(t, uint64(42), decoded.OccurredAt)
	assert.Nil(t, decoded.Payload)
	assert.Empty(t, decoded.Metadata)
}

func TestSerializeEventEntryNil(t *testing.T) {
	assert.Nil(t, SerializeEventEntry(nil))
}

func TestSerializeEventEntryMinimal(t *testing.T) {
	event := &EventEntry{
		EventID:    "minimal-event",
		EventType:  "test.event",
		OccurredAt: 999,
		Payload:    nil,
		Metadata:   nil,
	}

	serialized := SerializeEventEntry(event)
	decoded := DeserializeEventEntry(serialized)

	assert.Equal(t, event.EventID, decoded.EventID)
	assert.Equal(t, event.EventType, decoded.EventType)
	assert.Equal(t, event.OccurredAt, decoded.OccurredAt)
	assert.Nil(t, decoded.Payload)
	assert.Empty(t, decoded.Metadata)
}

func TestSerializeEventEntryWithMetadataOnly(t *testing.T) {
	event := &EventEntry{
		EventID:    "meta-event",
		EventType:  "test.with.metadata",
		OccurredAt: 12345,
		Payload:    []byte("simple payload"),
		Metadata: []KeyValueEntry{
			{Key: []byte("tenant_id"), Value: []byte("acme-corp")},
			{Key: []byte("correlation_id"), Value: []byte("xyz-789")},
		},
	}

	serialized := SerializeEventEntry(event)
	decoded := DeserializeEventEntry(serialized)

	assert.Equal(t, event.EventID, decoded.EventID)
	assert.Equal(t, event.EventType, decoded.EventType)
	assert.Equal(t, event.OccurredAt, decoded.OccurredAt)
	assert.Equal(t, event.Payload, decoded.Payload)
	require.Len(t, decoded.Metadata, 2)
	assert.Equal(t, []byte("tenant_id"), decoded.Metadata[0].Key)
	assert.Equal(t, []byte("acme-corp"), decoded.Metadata[0].Value)
	assert.Equal(t, []byte("correlation_id"), decoded.Metadata[1].Key)
	assert.Equal(t, []byte("xyz-789"), decoded.Metadata[1].Value)
}
