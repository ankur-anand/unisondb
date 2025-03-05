package dbengine

import (
	"errors"
	"io"

	"github.com/ankur-anand/kvalchemy/dbengine/kvdb"
	"github.com/ankur-anand/kvalchemy/dbengine/wal"
	"github.com/ankur-anand/kvalchemy/dbengine/wal/walrecord"
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
	ErrInvalidOffset    = errors.New("invalid offset")
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

var (
	packageKey = []string{"kvalchemy", "dbengine"}
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

	SetRowColumns(rowKey []byte, columnEntries map[string][]byte) error
	DeleteRowColumns(rowKey []byte, columnEntries map[string][]byte) error
	DeleteEntireRow(rowKey []byte) (int, error)
	
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

// DBEngine : which bTreeStore engine to use for the underlying persistence storage.
type DBEngine string

const (
	BoltDBEngine DBEngine = "BOLT"
	LMDBEngine   DBEngine = "LMDB"
)

// EngineConfig embeds all the config needed for Engine.
type EngineConfig struct {
	ValueThreshold int64       `toml:"value_threshold"`
	ArenaSize      int64       `toml:"arena_size"`
	WalConfig      wal.Config  `toml:"wal_config"`
	BtreeConfig    kvdb.Config `toml:"btree_config"`
	DBEngine       DBEngine    `toml:"db_engine"`
}

// NewDefaultEngineConfig returns an initialized default config for engine.
func NewDefaultEngineConfig() *EngineConfig {
	return &EngineConfig{
		ValueThreshold: 2 * 1024,
		ArenaSize:      4 << 20,
		WalConfig:      *wal.NewDefaultConfig(),
		BtreeConfig: kvdb.Config{
			Namespace: "kv.alchemy.sys.default",
			NoSync:    true,
			MmapSize:  4 << 30,
		},
		DBEngine: LMDBEngine,
	}
}
