package dbengine

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"

	"github.com/ankur-anand/kvalchemy/dbengine/kvdb"
	"github.com/ankur-anand/kvalchemy/dbengine/wal"
	"github.com/ankur-anand/kvalchemy/dbengine/wal/walrecord"
	"github.com/dgraph-io/badger/v4/y"
)

const (
	dbFileName  = "alchemy.db"
	walDirName  = "wal"
	pidLockName = "pid.lock"
)

var (
	// ErrKeyNotFound is a sentinel error for missing keys.
	ErrKeyNotFound     = kvdb.ErrKeyNotFound
	ErrBucketNotFound  = kvdb.ErrBucketNotFound
	ErrRecordCorrupted = kvdb.ErrRecordCorrupted

	ErrInCloseProcess   = errors.New("in-Close process")
	ErrDatabaseDirInUse = errors.New("pid.lock is held by another process")
	ErrInternalError    = errors.New("internal error")
)

var (
	// Marks values stored directly in memory.
	directValuePrefix  byte = 254
	walReferencePrefix byte = 255 // Marks values stored as a reference in WAL
	metaValueDelete         = byte(walrecord.LogOperationDelete)
	metaValueInsert         = byte(walrecord.LogOperationInsert)
)

var (
	sysKeyWalCheckPoint = []byte("sys.kv.alchemy.key.wal.checkpoint")
	sysKeyBloomFilter   = []byte("sys.kv.alchemy.key.bloom-filter")
)

// BtreeWriter defines the interface for interacting with a B-tree based storage
// for setting individual values, chunks and many value at once.
type BtreeWriter interface {
	// Set associates a value with a key.
	Set(key []byte, value []byte) error
	// SetMany associates multiple values with corresponding keys.
	SetMany(keys [][]byte, values [][]byte) error
	// SetChunks stores a value that has been split into chunks, associating them with a single key.
	SetChunks(key []byte, chunks [][]byte, checksum uint32) error
	// Delete deletes a value with a key.
	Delete(key []byte) error
	// DeleteMany delete multiple values with corresponding keys.
	DeleteMany(keys [][]byte) error

	StoreMetadata(key []byte, value []byte) error
	FSync() error
}

// BtreeReader defines the interface for interacting with a B-tree based storage
// for getting individual values, chunks and many value at once.
type BtreeReader interface {
	// Get retrieves a value associated with a key.
	Get(key []byte) ([]byte, error)
	// Snapshot writes the complete database to the provided io writer.
	Snapshot(w io.Writer) error
	RetrieveMetadata(key []byte) ([]byte, error)
}

// BTreeStore combines the BtreeWriter and BtreeReader interfaces.
type BTreeStore interface {
	BtreeWriter
	BtreeReader
	Close() error
}

// decodeChunkPositionWithValue decodes a MemTable entry into either a ChunkPosition (WAL lookup) or a direct value.
func decodeChunkPositionWithValue(data []byte) (*wal.Offset, []byte, error) {
	if len(data) == 0 {
		return nil, nil, ErrKeyNotFound
	}

	flag := data[0] // First byte determines type

	switch flag {
	case directValuePrefix:
		// Direct value stored
		return nil, data[1:], nil
	case walReferencePrefix:
		// Stored ChunkPosition (WAL lookup required)
		chunkPos := wal.DecodeOffset(data[1:])

		return chunkPos, nil, nil
	default:
		return nil, nil, fmt.Errorf("invalid MemTable entry flag: %d", flag)
	}
}

func getWalRecord(entry y.ValueStruct, wIO *wal.WalIO) (*walrecord.WalRecord, error) {
	chunkPos, value, err := decodeChunkPositionWithValue(entry.Value)
	if err != nil {
		return nil, fmt.Errorf("failed to decode chunk position: %w", err)
	}

	if chunkPos == nil {
		return walrecord.GetRootAsWalRecord(value, 0), nil
	}

	walValue, err := wIO.Read(chunkPos)
	if err != nil {
		return nil, fmt.Errorf("failed to read WAL for chunk position: %w", err)
	}

	return walrecord.GetRootAsWalRecord(walValue, 0), nil
}

func getValueStruct(ops byte, direct bool, value []byte) y.ValueStruct {
	storeValue := make([]byte, len(value)+1)

	switch direct {
	case true:
		storeValue[0] = directValuePrefix
	default:
		storeValue[0] = walReferencePrefix
	}

	copy(storeValue[1:], value)

	return y.ValueStruct{
		Meta:  ops,
		Value: storeValue,
	}
}

func marshalChecksum(checksum uint32) []byte {
	buf := make([]byte, 4) // uint32 takes 4 bytes
	binary.LittleEndian.PutUint32(buf, checksum)
	return buf
}

func unmarshalChecksum(data []byte) uint32 {
	if len(data) < 4 {
		return 0
	}
	return binary.LittleEndian.Uint32(data)
}

// EngineConfig embeds all the config needed for Engine.
type EngineConfig struct {
	ValueThreshold int64       `toml:"value_threshold"`
	ArenaSize      int64       `toml:"arena_size"`
	WalConfig      wal.Config  `toml:"wal_config"`
	BtreeConfig    kvdb.Config `toml:"btree_config"`
}
