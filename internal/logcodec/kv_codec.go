package logcodec

import (
	"github.com/ankur-anand/unisondb/schemas/logrecord"
	flatbuffers "github.com/google/flatbuffers/go"
)

// SerializeKVEntry encodes a single KeyValueEntry into FlatBuffers.
func SerializeKVEntry(key []byte, value []byte) []byte {
	builder := flatbuffers.NewBuilder(len(key) + len(value))

	keyOffset := builder.CreateByteVector(key)
	valueOffset := builder.CreateByteVector(value)

	logrecord.KeyValueEntryStart(builder)
	logrecord.KeyValueEntryAddKey(builder, keyOffset)
	logrecord.KeyValueEntryAddValue(builder, valueOffset)
	builder.Finish(logrecord.KeyValueEntryEnd(builder))

	return builder.FinishedBytes()
}

func DeserializeKVEntry(data []byte) KeyValueEntry {
	record := logrecord.GetRootAsKeyValueEntry(data, 0)
	return KeyValueEntry{Key: record.KeyBytes(), Value: record.ValueBytes()}
}
