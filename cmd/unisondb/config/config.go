package config

// Config : top-level configuration.
type Config struct {
	HTTPPort    int           `toml:"http_port"`
	Grpc        GrpcConfig    `toml:"grpc"`
	Storage     StorageConfig `toml:"storage"`
	PprofEnable bool          `toml:"pprof_enable"`
	logLevel    string        `toml:"log_level"`
}

type GrpcConfig struct {
	Port     int    `toml:"port"`
	CertPath string `toml:"cert_path"`
	KeyPath  string `toml:"key_path"`
}

type StorageConfig struct {
	BaseDir      string   `toml:"base_dir"`
	Namespaces   []string `toml:"namespaces"`
	BytesPerSync string   `toml:"bytes_per_sync"`
	SegmentSize  string   `toml:"segment_size"`
	ArenaSize    string   `toml:"arena_size"`
}

type RelayConfig struct {
	CertPath        string `toml:"cert_path"`
	KeyPath         string `toml:"key_path"`
	upstreamAddress string `toml:"upstream_address"`
}
