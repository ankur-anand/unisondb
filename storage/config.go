package storage

import (
	"fmt"
	"log/slog"
	"os"

	"github.com/pelletier/go-toml/v2"
	"github.com/rosedblabs/wal"
)

// StorageConfig stores all tunable parameters.
type StorageConfig struct {
	BytesPerSync   int64 `toml:"bytes_per_sync"`
	SegmentSize    int64 `toml:"segment_size"`
	ValueThreshold int64 `toml:"value_threshold"`
	ArenaSize      int64 `toml:"arena_size"`
}

// DefaultConfig returns the default configuration values.
func DefaultConfig() *StorageConfig {
	return &StorageConfig{
		BytesPerSync:   1 * wal.MB,  // 1MB
		SegmentSize:    16 * wal.MB, // 16MB
		ValueThreshold: 2 * wal.KB,  // Store small values in MemTable
		ArenaSize:      1 * wal.MB,  // Default Arena Size
	}
}

// LoadConfig loads configuration from a TOML file, using defaults if file is missing.
func LoadConfig(filepath string) (*StorageConfig, error) {
	config := DefaultConfig() // Load default values

	file, err := os.ReadFile(filepath)
	if err != nil {
		// If file doesn't exist, return defaults without error
		slog.Warn("Config file not found, using default settings.", "path", filepath)
		return config, err
	}

	err = toml.Unmarshal(file, config)
	if err != nil {
		return nil, fmt.Errorf("failed to parse TOML config: %w", err)
	}

	return config, nil
}
