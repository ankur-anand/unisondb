package kvdrivers

import (
	"errors"
	"fmt"
)

const (
	kvValue        byte = 254
	chunkedValue   byte = 255
	rowColumnValue byte = 253
)

const (
	DefaultFlushSizeThreshold = 32
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
	ErrTxnAlreadyActive       = errors.New("an active transaction already exists; commit or abort it first")
	ErrTxnClosed              = errors.New("transaction has already been committed or aborted")
)

var (
	sysBucketMetaData = "sys.kv.unison.db.wal.metadata.bucket"
	packageKey        = []string{"unisondb", "kv", "drivers"}
)

type TxnStats struct {
	EntriesModified float32
	PutOps          float32
	DeleteOps       float32
}

var (
	mSetTotal      = append(packageKey, []string{"set", "total"}...)
	mGetTotal      = append(packageKey, []string{"get", "total"}...)
	mDelTotal      = append(packageKey, []string{"delete", "total"}...)
	mSetLatency    = append(packageKey, []string{"set", "durations", "seconds"}...)
	mGetLatency    = append(packageKey, []string{"get", "durations", "seconds"}...)
	mDelLatency    = append(packageKey, []string{"delete", "durations", "seconds"}...)
	mRowGetTotal   = append(packageKey, []string{"row", "get", "total"}...)
	mRowGetLatency = append(packageKey, []string{"row", "get", "durations", "seconds"}...)

	mSnapshotTotal   = append(packageKey, []string{"snapshot", "total"}...)
	mSnapshotLatency = append(packageKey, []string{"snapshot", "durations", "seconds"}...)

	mTxnFlushTotal           = append(packageKey, []string{"flush", "total"}...)
	mTxnFlushLatency         = append(packageKey, []string{"flush", "durations", "seconds"}...)
	mTxnFlushBatchSize       = append(packageKey, []string{"flush", "batch", "size"}...)
	mTxnEntriesModifiedTotal = append(packageKey, []string{"entries", "modified", "total"}...)
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
