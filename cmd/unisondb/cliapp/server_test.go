package cliapp

import (
	"context"
	"log/slog"
	"math"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/ankur-anand/unisondb/cmd/unisondb/config"
	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc"
)

func TestParseLogLevel(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		want    slog.Level
		wantErr bool
	}{
		{name: "debug", input: "debug", want: slog.LevelDebug},
		{name: "info mixed case", input: "InFo", want: slog.LevelInfo},
		{name: "warn alias", input: "warning", want: slog.LevelWarn},
		{name: "error", input: "error", want: slog.LevelError},
		{name: "empty defaults to info", input: "", want: slog.LevelInfo},
		{name: "unknown", input: "verbose", wantErr: true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := parseLogLevel(tc.input)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("expected error for input %q", tc.input)
				}
				return
			}

			if err != nil {
				t.Fatalf("parseLogLevel(%q) returned unexpected error: %v", tc.input, err)
			}
			if got != tc.want {
				t.Fatalf("parseLogLevel(%q) = %v, want %v", tc.input, got, tc.want)
			}
		})
	}
}

func TestClampInt64(t *testing.T) {
	const (
		min = int64(10)
		max = int64(100)
	)

	tests := []struct {
		name string
		in   int64
		want int64
	}{
		{name: "below min", in: 1, want: min},
		{name: "within range", in: 42, want: 42},
		{name: "above max", in: 150, want: max},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := clampInt64(tc.in, min, max); got != tc.want {
				t.Fatalf("clampInt64(%d, %d, %d) = %d, want %d", tc.in, min, max, got, tc.want)
			}
		})
	}
}

func TestClampToUint32(t *testing.T) {
	const min = uint32(512)

	tests := []struct {
		name string
		in   int64
		want uint32
	}{
		{name: "negative returns min", in: -10, want: min},
		{name: "less than min positive", in: 200, want: min},
		{name: "within range", in: 1024, want: 1024},
		{name: "above max uint32", in: int64(math.MaxUint32) + 100, want: math.MaxUint32},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := clampToUint32(tc.in, min); got != tc.want {
				t.Fatalf("clampToUint32(%d, %d) = %d, want %d", tc.in, min, got, tc.want)
			}
		})
	}
}

func TestBuildNamespaceSegmentLagMap(t *testing.T) {
	cfg := &config.Config{
		RelayConfigs: map[string]config.RelayConfig{
			"relay-a": {
				Namespaces:          []string{"alpha", "beta"},
				SegmentLagThreshold: 5,
			},
			"relay-b": {
				Namespaces:          []string{"gamma"},
				SegmentLagThreshold: 7,
			},
		},
	}

	got, err := buildNamespaceSegmentLagMap(cfg)
	if err != nil {
		t.Fatalf("buildNamespaceSegmentLagMap returned error: %v", err)
	}

	want := map[string]int{
		"alpha": 5,
		"beta":  5,
		"gamma": 7,
	}

	if len(got) != len(want) {
		t.Fatalf("unexpected map size: got %d, want %d", len(got), len(want))
	}

	for ns, lag := range want {
		if got[ns] != lag {
			t.Fatalf("namespace %q lag = %d, want %d", ns, got[ns], lag)
		}
	}
}

func TestBuildNamespaceSegmentLagMapDuplicateNamespace(t *testing.T) {
	cfg := &config.Config{
		RelayConfigs: map[string]config.RelayConfig{
			"relay-a": {
				Namespaces:          []string{"shared"},
				SegmentLagThreshold: 5,
			},
			"relay-b": {
				Namespaces:          []string{"shared"},
				SegmentLagThreshold: 7,
			},
		},
	}

	_, err := buildNamespaceSegmentLagMap(cfg)
	if err == nil {
		t.Fatalf("expected error due to duplicate namespace, got nil")
	}
	if !strings.Contains(err.Error(), "duplicate namespace") {
		t.Fatalf("unexpected error message: %v", err)
	}
}

func TestBuildNamespaceSegmentLagMapEmpty(t *testing.T) {
	cfg := &config.Config{
		RelayConfigs: map[string]config.RelayConfig{},
	}

	got, err := buildNamespaceSegmentLagMap(cfg)
	if err != nil {
		t.Fatalf("buildNamespaceSegmentLagMap returned error: %v", err)
	}
	if len(got) != 0 {
		t.Fatalf("expected empty map, got %v", got)
	}
}

func TestSetupStorageConfigClampsAndOptions(t *testing.T) {
	s := &Server{
		cfg: config.Config{
			Storage: config.StorageConfig{
				SegmentSize:           "1MB",
				BytesPerSync:          "128",
				ArenaSize:             "512KB",
				DisableEntryTypeCheck: true,
			},
		},
	}

	err := s.SetupStorageConfig(context.Background())
	if err != nil {
		t.Fatalf("SetupStorageConfig returned error: %v", err)
	}

	if s.storageConfig == nil {
		t.Fatalf("storageConfig not initialized")
	}

	if s.storageConfig.WalConfig.SegmentSize != 4<<20 {
		t.Fatalf("SegmentSize = %d, want %d", s.storageConfig.WalConfig.SegmentSize, 4<<20)
	}

	if s.storageConfig.WalConfig.BytesPerSync != 512 {
		t.Fatalf("BytesPerSync = %d, want %d", s.storageConfig.WalConfig.BytesPerSync, 512)
	}

	if s.storageConfig.ArenaSize != 1<<20 {
		t.Fatalf("ArenaSize = %d, want %d", s.storageConfig.ArenaSize, 1<<20)
	}

	if !s.storageConfig.DisableEntryTypeCheck {
		t.Fatalf("DisableEntryTypeCheck expected true")
	}
}

func TestSetupStorageConfigWriteNotifyInvalid(t *testing.T) {
	s := &Server{
		cfg: config.Config{
			Storage: config.StorageConfig{},
			WriteNotifyConfig: config.WriteNotifyConfig{
				Enabled:  true,
				MaxDelay: "1ns",
			},
		},
	}

	err := s.SetupStorageConfig(context.Background())
	if err == nil {
		t.Fatal("expected error for invalid write notify max delay, got nil")
	}
	if !strings.Contains(err.Error(), "write notify config") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestSetupStorageConfigWalCleanupInvalidInterval(t *testing.T) {
	s := &Server{
		cfg: config.Config{
			Storage: config.StorageConfig{
				WALCleanupConfig: config.WALCleanupConfig{
					Enabled:  true,
					Interval: "0s",
					MaxAge:   "2s",
				},
			},
		},
	}

	err := s.SetupStorageConfig(context.Background())
	if err == nil {
		t.Fatal("expected error for invalid wal cleanup interval, got nil")
	}
	if !strings.Contains(err.Error(), "wal cleanup config") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestSetupStorageConfigWalCleanupSuccess(t *testing.T) {
	s := &Server{
		cfg: config.Config{
			Storage: config.StorageConfig{
				WALCleanupConfig: config.WALCleanupConfig{
					Enabled:     true,
					Interval:    "2s",
					MaxAge:      "5s",
					MinSegments: 1,
					MaxSegments: 3,
				},
			},
		},
	}

	err := s.SetupStorageConfig(context.Background())
	if err != nil {
		t.Fatalf("SetupStorageConfig returned error: %v", err)
	}

	if s.storageConfig.WalConfig.CleanupInterval != 2*time.Second {
		t.Fatalf("CleanupInterval = %v, want %v", s.storageConfig.WalConfig.CleanupInterval, 2*time.Second)
	}

	if !s.storageConfig.WalConfig.AutoCleanup {
		t.Fatalf("AutoCleanup expected true")
	}

	if s.storageConfig.WalConfig.MaxAge != 5*time.Second {
		t.Fatalf("MaxAge = %v, want %v", s.storageConfig.WalConfig.MaxAge, 5*time.Second)
	}

	if s.storageConfig.WalConfig.MinSegment != 1 {
		t.Fatalf("MinSegment = %d, want 1", s.storageConfig.WalConfig.MinSegment)
	}

	if s.storageConfig.WalConfig.MaxSegment != 3 {
		t.Fatalf("MaxSegment = %d, want 3", s.storageConfig.WalConfig.MaxSegment)
	}
}

func TestSetupStorageConfigWriteNotifySuccess(t *testing.T) {
	s := &Server{
		cfg: config.Config{
			Storage: config.StorageConfig{},
			WriteNotifyConfig: config.WriteNotifyConfig{
				Enabled:  true,
				MaxDelay: "2s",
			},
		},
	}

	err := s.SetupStorageConfig(context.Background())
	if err != nil {
		t.Fatalf("SetupStorageConfig returned error: %v", err)
	}

	if !s.storageConfig.WriteNotifyCoalescing.Enabled {
		t.Fatalf("WriteNotifyCoalescing.Enabled expected true")
	}

	expected := 2 * time.Second
	if s.storageConfig.WriteNotifyCoalescing.Duration != expected {
		t.Fatalf("WriteNotifyCoalescing.Duration = %v, want %v", s.storageConfig.WriteNotifyCoalescing.Duration, expected)
	}
}

func TestSetupStorageConfigWalCleanupInvalidMaxAge(t *testing.T) {
	s := &Server{
		cfg: config.Config{
			Storage: config.StorageConfig{
				WALCleanupConfig: config.WALCleanupConfig{
					Enabled:  true,
					Interval: "2s",
					MaxAge:   "1ns",
				},
			},
		},
	}

	err := s.SetupStorageConfig(context.Background())
	if err == nil {
		t.Fatal("expected error due to invalid wal cleanup max age, got nil")
	}
	if !strings.Contains(err.Error(), "wal cleanup config") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestSetupStorageConfigWalFsyncIntervalInvalid(t *testing.T) {
	t.Run("parse error", func(t *testing.T) {
		s := &Server{
			cfg: config.Config{
				Storage: config.StorageConfig{
					WalFsyncInterval: "not-a-duration",
				},
			},
		}

		err := s.SetupStorageConfig(context.Background())
		if err == nil {
			t.Fatal("expected error due to invalid WalFsyncInterval, got nil")
		}
	})

	t.Run("too small", func(t *testing.T) {
		s := &Server{
			cfg: config.Config{
				Storage: config.StorageConfig{
					WalFsyncInterval: "500ms",
				},
			},
		}

		err := s.SetupStorageConfig(context.Background())
		if err != nil {
			t.Fatalf("SetupStorageConfig returned error: %v", err)
		}

		if s.storageConfig.WalConfig.SyncInterval != 500*time.Millisecond {
			t.Fatalf("SyncInterval = %v, want %v", s.storageConfig.WalConfig.SyncInterval, 500*time.Millisecond)
		}
	})
}

func TestSetupStorageConfigWalCleanupInvalidMaxAgeFormat(t *testing.T) {
	s := &Server{
		cfg: config.Config{
			Storage: config.StorageConfig{
				WALCleanupConfig: config.WALCleanupConfig{
					Enabled:  true,
					Interval: "2s",
					MaxAge:   "not-a-duration",
				},
			},
		},
	}

	err := s.SetupStorageConfig(context.Background())
	if err == nil {
		t.Fatal("expected error due to invalid MaxAge, got nil")
	}
}

func TestSetupStorageConfigWalFsyncIntervalApplied(t *testing.T) {
	s := &Server{
		cfg: config.Config{
			Storage: config.StorageConfig{
				WalFsyncInterval: "3s",
			},
		},
	}

	err := s.SetupStorageConfig(context.Background())
	if err != nil {
		t.Fatalf("SetupStorageConfig returned error: %v", err)
	}

	if s.storageConfig.WalConfig.SyncInterval != 3*time.Second {
		t.Fatalf("SyncInterval = %v, want %v", s.storageConfig.WalConfig.SyncInterval, 3*time.Second)
	}
}

func TestClampToUint32AtMaxBoundary(t *testing.T) {
	if got := clampToUint32(int64(math.MaxUint32), 1); got != math.MaxUint32 {
		t.Fatalf("clampToUint32 at boundary = %d, want %d", got, math.MaxUint32)
	}
}

func TestParseLogLevelWithWhitespace(t *testing.T) {
	level, err := parseLogLevel(" info ")
	assert.NoError(t, err, "failed to parse log level")
	assert.Equal(t, level, slog.LevelInfo)
}

func startTestGRPCServer(t *testing.T) (addr string, shutdown func()) {
	t.Helper()

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to listen: %v", err)
	}

	server := grpc.NewServer()
	go func() {
		if serveErr := server.Serve(lis); serveErr != nil {
			t.Logf("grpc test server stopped: %v", serveErr)
		}
	}()

	return lis.Addr().String(), func() {
		server.Stop()
		_ = lis.Close()
	}
}

func TestBuildNamespaceGrpcClientsSuccess(t *testing.T) {
	addr, shutdown := startTestGRPCServer(t)
	defer shutdown()

	cfg := config.Config{
		RelayConfigs: map[string]config.RelayConfig{
			"relay-a": {
				Namespaces:          []string{"alpha", "beta"},
				UpstreamAddress:     addr,
				AllowInsecure:       true,
				SegmentLagThreshold: 5,
			},
			"relay-b": {
				Namespaces:          []string{"gamma"},
				UpstreamAddress:     addr,
				AllowInsecure:       true,
				SegmentLagThreshold: 5,
			},
		},
	}

	clients, err := buildNamespaceGrpcClients(cfg)
	if err != nil {
		t.Fatalf("buildNamespaceGrpcClients returned error: %v", err)
	}

	if len(clients) != 3 {
		t.Fatalf("expected 3 namespace connections, got %d", len(clients))
	}

	alpha := clients["alpha"]
	beta := clients["beta"]
	gamma := clients["gamma"]

	if alpha == nil || beta == nil || gamma == nil {
		t.Fatalf("expected connections for all namespaces, got %#v", clients)
	}

	if alpha != beta || alpha != gamma {
		t.Fatalf("expected shared connection, got alpha=%p beta=%p gamma=%p", alpha, beta, gamma)
	}

	t.Cleanup(func() {
		for _, conn := range clients {
			_ = conn.Close()
		}
	})
}

func TestBuildNamespaceGrpcClientsError(t *testing.T) {
	cfg := config.Config{
		RelayConfigs: map[string]config.RelayConfig{
			"bad-relay": {
				Namespaces:      []string{"alpha"},
				UpstreamAddress: "127.0.0.1:0",
				CAPath:          "non-existent-ca.pem",
				AllowInsecure:   false,
			},
		},
	}

	_, err := buildNamespaceGrpcClients(cfg)
	if err == nil {
		t.Fatal("expected error when TLS credentials cannot be loaded")
	}
	if !strings.Contains(err.Error(), "failed to dial") && !strings.Contains(err.Error(), "no such file") {
		t.Fatalf("unexpected error: %v", err)
	}
}
