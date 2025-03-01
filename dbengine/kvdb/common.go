package kvdb

import "errors"

const (
	FullValueFlag    byte = 254
	ChunkedValueFlag byte = 255
)

var (
	ErrInvalidChunkMetadata = errors.New("invalid chunk metadata")
	ErrInvalidDataFormat    = errors.New("invalid data format")
	ErrKeyNotFound          = errors.New("key not found")
	ErrBucketNotFound       = errors.New("bucket not found")
	ErrRecordCorrupted      = errors.New("record corrupted")
)

var (
	sysBucketMetaData = "sys.kv.alchemy.wal.metadata.bucket"
	packageKey        = []string{"kvalchemy", "kvdb"}
)

type Config struct {
	Namespace string
	NoSync    bool
	MmapSize  int64
}
