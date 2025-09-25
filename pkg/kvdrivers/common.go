package kvdrivers

import (
	"errors"
)

const (
	kvValue        byte = 254
	chunkedValue   byte = 255
	rowColumnValue byte = 253
)

type ValueEntryType byte

const (
	UnknownValueEntry ValueEntryType = iota
	KeyValueValueEntry
	ChunkedValueEntry
	RowColumnValueEntry
)

var (
	ErrInvalidChunkMetadata = errors.New("invalid chunk metadata")
	ErrKeyNotFound          = errors.New("key not found")
	ErrBucketNotFound       = errors.New("bucket not found")
	ErrRecordCorrupted      = errors.New("record corrupted")
	ErrUseGetColumnAPI      = errors.New("use get column api")
	ErrInvalidArguments     = errors.New("invalid arguments")
	ErrTxnAlreadyActive     = errors.New("an active transaction already exists; commit or abort it first")
	ErrTxnClosed            = errors.New("transaction has already been committed or aborted")
)

var (
	sysBucketMetaData = "sys.kv.unison.db.wal.metadata.bucket"
)

type TxnStats struct {
	EntriesModified float32
	PutOps          float32
	DeleteOps       float32
}

var (
	OpSet     = "set"
	OpGet     = "get"
	OpDelete  = "delete"
	TxnCommit = "commit"
)

type Config struct {
	Namespace string
	NoSync    bool
	MmapSize  int64
}

// ColumnPredicate defines a function used to filter column keys.
type ColumnPredicate func(columnKey []byte) bool
