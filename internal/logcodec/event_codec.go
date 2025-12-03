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

	eventIDOffset := builder.CreateString(entry.EventID)
	eventTypeOffset := builder.CreateString(entry.EventType)
	payloadOffset := builder.CreateByteVector(entry.Payload)
	metadataVector := buildKeyValueVector(builder, entry.Metadata)

	logrecord.EventEntryStart(builder)
	logrecord.EventEntryAddEventId(builder, eventIDOffset)
	logrecord.EventEntryAddEventType(builder, eventTypeOffset)
	logrecord.EventEntryAddOccurredAt(builder, entry.OccurredAt)
	if entry.Payload != nil {
		logrecord.EventEntryAddPayload(builder, payloadOffset)
	}
	if metadataVector != 0 {
		logrecord.EventEntryAddMetadata(builder, metadataVector)
	}
	builder.Finish(logrecord.EventEntryEnd(builder))

	return builder.FinishedBytes()
}

// DeserializeEventEntry decodes a FlatBuffers EventEntry.
func DeserializeEventEntry(buf []byte) EventEntry {
	fbEntry := logrecord.GetRootAsEventEntry(buf, 0)
	entry := EventEntry{
		EventID:    string(fbEntry.EventId()),
		EventType:  string(fbEntry.EventType()),
		OccurredAt: fbEntry.OccurredAt(),
		Payload:    fbEntry.PayloadBytes(),
		Metadata:   make([]KeyValueEntry, fbEntry.MetadataLength()),
	}

	kv := new(logrecord.KeyValueEntry)
	for i := 0; i < fbEntry.MetadataLength(); i++ {
		if fbEntry.Metadata(kv, i) {
			entry.Metadata[i] = KeyValueEntry{
				Key:   kv.KeyBytes(),
				Value: kv.ValueBytes(),
			}
		}
	}

	return entry
}

func buildKeyValueVector(builder *flatbuffers.Builder, entries []KeyValueEntry) flatbuffers.UOffsetT {
	if len(entries) == 0 {
		return 0
	}
	offsets := make([]flatbuffers.UOffsetT, 0, len(entries))
	for _, entry := range entries {
		keyOffset := builder.CreateByteVector(entry.Key)
		valueOffset := builder.CreateByteVector(entry.Value)
		logrecord.KeyValueEntryStart(builder)
		logrecord.KeyValueEntryAddKey(builder, keyOffset)
		logrecord.KeyValueEntryAddValue(builder, valueOffset)
		offsets = append(offsets, logrecord.KeyValueEntryEnd(builder))
	}
	logrecord.EventEntryStartMetadataVector(builder, len(entries))
	// fb builds vectors backwards.
	for i := len(offsets) - 1; i >= 0; i-- {
		builder.PrependUOffsetT(offsets[i])
	}
	return builder.EndVector(len(entries))
}
