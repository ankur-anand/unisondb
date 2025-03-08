package kvdb

import (
	"errors"
	"fmt"
)

const (
	valueTypeFull    byte = 254
	valueTypeChunked byte = 255
	valueTypeColumns byte = 253
)

const (
	rowKeySeperator = "::"
)

var (
	ErrInvalidChunkMetadata   = errors.New("invalid chunk metadata")
	ErrInvalidOpsForValueType = errors.New("unsupported operation for given value type")
	ErrKeyNotFound            = errors.New("key not found")
	ErrBucketNotFound         = errors.New("bucket not found")
	ErrRecordCorrupted        = errors.New("record corrupted")
	ErrUseGetColumnAPI        = errors.New("use get column api")
	ErrInvalidArguments       = errors.New("invalid arguments")
)

var (
	sysBucketMetaData = "sys.kv.alchemy.wal.metadata.bucket"
	packageKey        = []string{"kvalchemy", "kvdb"}
)

var (
	mSetTotal         = append(packageKey, []string{"set", "total"}...)
	mGetTotal         = append(packageKey, []string{"get", "total"}...)
	mDelTotal         = append(packageKey, []string{"delete", "total"}...)
	mSetLatency       = append(packageKey, []string{"set", "durations", "seconds"}...)
	mGetLatency       = append(packageKey, []string{"get", "durations", "seconds"}...)
	mDelLatency       = append(packageKey, []string{"delete", "durations", "seconds"}...)
	mChunkSetTotal    = append(packageKey, []string{"set", "chunks", "total"}...)
	mChunksSetLatency = append(packageKey, []string{"set", "chunks", "durations", "seconds"}...)
	mSetManyTotal     = append(packageKey, []string{"set", "many", "total"}...)
	mSetManyLatency   = append(packageKey, []string{"set", "many", "durations", "seconds"}...)
	mDelManyTotal     = append(packageKey, []string{"delete", "many", "total"}...)
	mDelManyLatency   = append(packageKey, []string{"delete", "many", "durations", "seconds"}...)
	mSnapshotTotal    = append(packageKey, []string{"snapshot", "total"}...)
	mSnapshotLatency  = append(packageKey, []string{"snapshot", "durations", "seconds"}...)
	mRowSetTotal      = append(packageKey, []string{"row", "set", "total"}...)
	mRowSetLatency    = append(packageKey, []string{"row", "set", "durations", "seconds"}...)
	mRowDeleteTotal   = append(packageKey, []string{"row", "delete", "total"}...)
	mRowDeleteLatency = append(packageKey, []string{"row", "delete", "durations", "seconds"}...)
	mRowGetTotal      = append(packageKey, []string{"row", "get", "total"}...)
	mRowGetLatency    = append(packageKey, []string{"row", "get", "durations", "seconds"}...)
)

type Config struct {
	Namespace string
	NoSync    bool
	MmapSize  int64
}

func appendRowKeyToColumnKey(rowKey []byte, entries map[string][]byte) map[string][]byte {
	mapEntries := make(map[string][]byte, len(entries))
	for key, entry := range entries {
		newKey := fmt.Sprintf("%s%s", rowKey, key)
		mapEntries[newKey] = entry
	}
	return mapEntries
}

// ColumnPredicate defines a function used to filter column keys.
type ColumnPredicate func(columnKey []byte) bool
