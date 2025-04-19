package config

import (
	"crypto/sha256"
	"fmt"
	"log/slog"
	"strconv"
	"strings"
	"time"

	"github.com/ankur-anand/unisondb/pkg/svcutils"
	"golang.org/x/time/rate"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

// Config : top-level configuration.
type Config struct {
	HTTPPort     int                    `toml:"http_port"`
	Grpc         GrpcConfig             `toml:"grpc_config"`
	Storage      StorageConfig          `toml:"storage_config"`
	PprofEnable  bool                   `toml:"pprof_enable"`
	RelayConfigs map[string]RelayConfig `toml:"relayer_config"`
	LogConfig    LogConfig              `toml:"log_config"`
	Limiter      Limiter                `toml:"limiter"`
	FuzzConfig   FuzzConfig             `toml:"fuzz_config"`
}

type GrpcConfig struct {
	Port     int    `toml:"port"`
	CertPath string `toml:"cert_path"`
	KeyPath  string `toml:"key_path"`
	CAPath   string `toml:"ca_path"`
}

type StorageConfig struct {
	BaseDir      string   `toml:"base_dir"`
	Namespaces   []string `toml:"namespaces"`
	BytesPerSync string   `toml:"bytes_per_sync"`
	SegmentSize  string   `toml:"segment_size"`
	ArenaSize    string   `toml:"arena_size"`
}

// RelayConfig holds TLS and upstream gRPC config.
type RelayConfig struct {
	Namespaces          []string `toml:"namespaces"`
	CertPath            string   `toml:"cert_path"`
	KeyPath             string   `toml:"key_path"`
	CAPath              string   `toml:"ca_path"`
	UpstreamAddress     string   `toml:"upstream_address"`
	GrpcServiceConfig   string   `toml:"grpc_service_config"`
	SegmentLagThreshold int      `toml:"segment_lag_threshold"`
}

type LogConfig struct {
	MinLevelPercents map[string]float64 `toml:"min_level_percents"`
	LogLevel         string             `toml:"log_level"`
}

type Limiter struct {
	Interval string `toml:"interval"`
	Burst    int    `toml:"burst"`
}

type FuzzConfig struct {
	OpsPerNamespace     int `toml:"ops_per_namespace"`
	WorkersPerNamespace int `toml:"workers_per_namespace"`
}

func ParseLevelPercents(cfg LogConfig) (map[slog.Level]float64, error) {
	out := map[slog.Level]float64{
		slog.LevelDebug: 100.0,
		slog.LevelInfo:  25.0,
		slog.LevelWarn:  100.0,
		slog.LevelError: 100.0,
	}

	for k, v := range cfg.MinLevelPercents {
		switch strings.ToLower(k) {
		case "debug":
			out[slog.LevelDebug] = v
		case "info":
			out[slog.LevelInfo] = v
		case "warn":
			out[slog.LevelWarn] = v
		case "error":
			out[slog.LevelError] = v
		default:
			return nil, fmt.Errorf("unknown log level: %s", k)
		}
	}
	return out, nil
}

func NewRelayerGRPCConn(cfg *RelayConfig) (*grpc.ClientConn, error) {
	var creds credentials.TransportCredentials
	var err error

	if cfg.CertPath != "" && cfg.KeyPath != "" {
		creds, err = svcutils.NewMTLSCreds(cfg.CertPath, cfg.KeyPath, cfg.CAPath)
		if err != nil {
			return nil, err
		}
	} else {
		creds, err = svcutils.NewTLSCreds(cfg.CAPath)
		if err != nil {
			return nil, err
		}
	}

	if cfg.GrpcServiceConfig == "" {
		cfg.GrpcServiceConfig = svcutils.BuildDefaultRelayerServiceConfigJSON()
	}

	return grpc.NewClient(cfg.UpstreamAddress,
		grpc.WithTransportCredentials(creds),
		grpc.WithDefaultServiceConfig(cfg.GrpcServiceConfig))

}

func HashRelayConfig(relay RelayConfig) string {
	var b strings.Builder
	b.WriteString(relay.CertPath)
	b.WriteByte('|')
	b.WriteString(relay.KeyPath)
	b.WriteByte('|')
	b.WriteString(relay.CAPath)
	b.WriteByte('|')
	b.WriteString(relay.UpstreamAddress)
	b.WriteByte('|')
	b.WriteString(relay.GrpcServiceConfig)
	b.WriteByte('|')
	b.WriteString(strconv.Itoa(relay.SegmentLagThreshold))

	sum := sha256.Sum256([]byte(b.String()))
	return fmt.Sprintf("%x", sum)
}

func BuildLimiter(cfg Limiter) (*rate.Limiter, error) {
	interval := 1 * time.Second
	burst := 3

	if cfg.Interval != "" {
		parsed, err := time.ParseDuration(cfg.Interval)
		if err != nil {
			return nil, fmt.Errorf("invalid retry interval: %w", err)
		}
		interval = parsed
	}
	if cfg.Burst > 0 {
		burst = cfg.Burst
	}

	return rate.NewLimiter(rate.Every(interval), burst), nil
}
