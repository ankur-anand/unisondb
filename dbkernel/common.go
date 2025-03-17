package dbkernel

import (
	"errors"
	"io"

	"github.com/ankur-anand/unisondb/dbkernel/internal/kvdrivers"
	"github.com/ankur-anand/unisondb/dbkernel/internal/wal"
	"github.com/ankur-anand/unisondb/schemas/logrecord"
)

const (
	dbFileName  = "unison.db"
	walDirName  = "wal"
	pidLockName = "pid.lock"
)

var (
	// ErrKeyNotFound is a sentinel error for missing keys.
	ErrKeyNotFound     = kvdrivers.ErrKeyNotFound
	ErrBucketNotFound  = kvdrivers.ErrBucketNotFound
	ErrRecordCorrupted = kvdrivers.ErrRecordCorrupted
	ErrUseGetColumnAPI = kvdrivers.ErrUseGetColumnAPI

	ErrInCloseProcess   = errors.New("in-Close process")
	ErrDatabaseDirInUse = errors.New("pid.lock is held by another process")
	ErrInternalError    = errors.New("internal error")
	ErrInvalidOffset    = errors.New("invalid offset")
)

var (
	sysKeyWalCheckPoint = []byte("sys.kv.unisondb.key.wal.checkpoint")
	sysKeyBloomFilter   = []byte("sys.kv.unisondb.key.bloom-filter")
)

var (
	packageKey = []string{"unisondb", "dbkernel"}
)

// DBEngine : which bTreeStore engine to use for the underlying persistence storage.
type DBEngine string

const (
	BoltDBEngine DBEngine = "BOLT"
	LMDBEngine   DBEngine = "LMDB"
)

// EngineConfig embeds all the config needed for Engine.
type EngineConfig struct {
	ValueThreshold int64            `toml:"value_threshold"`
	ArenaSize      int64            `toml:"arena_size"`
	WalConfig      wal.Config       `toml:"wal_config"`
	BtreeConfig    kvdrivers.Config `toml:"btree_config"`
	DBEngine       DBEngine         `toml:"db_engine"`
}

// NewDefaultEngineConfig returns an initialized default config for engine.
func NewDefaultEngineConfig() *EngineConfig {
	return &EngineConfig{
		ValueThreshold: 2 * 1024,
		ArenaSize:      4 << 20,
		WalConfig:      *wal.NewDefaultConfig(),
		BtreeConfig: kvdrivers.Config{
			Namespace: "kv.unisondb.sys.default",
			NoSync:    true,
			MmapSize:  4 << 30,
		},
		DBEngine: LMDBEngine,
	}
}
