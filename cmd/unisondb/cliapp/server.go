package cliapp

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"os"
	"slices"
	"sync"
	"time"

	"github.com/ankur-anand/unisondb/cmd/unisondb/config"
	"github.com/ankur-anand/unisondb/dbkernel"
	"github.com/ankur-anand/unisondb/internal/etc"
	udbmetrics "github.com/ankur-anand/unisondb/internal/metrics"
	"github.com/ankur-anand/unisondb/internal/services/fuzzer"
	"github.com/ankur-anand/unisondb/internal/services/streamer"
	"github.com/ankur-anand/unisondb/pkg/logutil"
	"github.com/ankur-anand/unisondb/pkg/notifier"
	"github.com/ankur-anand/unisondb/pkg/umetrics"
	"github.com/hashicorp/go-metrics"
	hashiprom "github.com/hashicorp/go-metrics/prometheus"
	"github.com/pelletier/go-toml/v2"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	promreporter "github.com/uber-go/tally/v4/prometheus"
	"google.golang.org/grpc"
)

var (
	modeReplica = "replica"
	modeRelay   = "relay"
	modeFuzz    = "fuzz"
)

type Server struct {
	mode          string
	env           string
	cfg           config.Config
	engines       map[string]*dbkernel.Engine
	storageConfig *dbkernel.EngineConfig
	pl            *slog.Logger
	fuzzStats     *fuzzer.FuzzStats
	notifiers     map[string]notifier.Notifier
	services      []Service
	deps          *Dependencies
	PortsFile     string

	// callbacks when shutdown.
	DeferCallback []func(ctx context.Context)
}

// InitFromCLI initializes the server from CLI arguments.
func (ms *Server) InitFromCLI(cfgPath, env, mode string) func(ctx context.Context) error {
	return func(ctx context.Context) error {
		ms.mode = mode
		ms.env = env

		cfgBytes, err := os.ReadFile(cfgPath)
		if err != nil {
			return err
		}

		err = toml.Unmarshal(cfgBytes, &ms.cfg)
		if err != nil {
			return err
		}

		ms.engines = make(map[string]*dbkernel.Engine)
		logPercentage, err := config.ParseLevelPercents(ms.cfg.LogConfig)
		if err != nil {
			return err
		}

		logLevel, err := parseLogLevel(ms.cfg.LogConfig.LogLevel)
		if err != nil {
			return err
		}

		disableTimeStamp := ms.cfg.LogConfig.DisableTimestamp
		pl := logutil.NewPercentLogger(logPercentage, slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
			Level: logLevel,
			ReplaceAttr: func(groups []string, a slog.Attr) slog.Attr {
				if disableTimeStamp && a.Key == slog.TimeKey {
					return slog.Attr{}
				}
				if a.Key == slog.MessageKey {
					return slog.String("module", a.Value.String())
				}
				return a
			},
		}),
			logLevel)
		ms.pl = pl
		slog.SetDefault(slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
			Level: logLevel,
			ReplaceAttr: func(groups []string, a slog.Attr) slog.Attr {
				if disableTimeStamp && a.Key == slog.TimeKey {
					return slog.Attr{}
				}
				if a.Key == slog.MessageKey {
					return slog.String("module", a.Value.String())
				}
				return a
			},
		})))

		slog.Info("[unisondb.cliapp]",
			slog.String("event_type", "configuration.loaded"),
			slog.Group("cliapp",
				slog.String("mode", mode),
				slog.String("env", env),
				slog.String("log_level", logLevel.String()),
				slog.String("config_path", cfgPath)),
			slog.Group("storage",
				slog.String("path", ms.cfg.Storage.BaseDir),
				slog.Any("namespaces", ms.cfg.Storage.Namespaces)),
		)

		return nil
	}
}

// InitTelemetry initializes telemetry and metrics.
func (ms *Server) InitTelemetry(ctx context.Context) error {
	prometheus.Unregister(collectors.NewGoCollector())
	err := prometheus.Register(collectors.NewBuildInfoCollector())
	if err != nil {
		return err
	}

	ioCollector, err := udbmetrics.NewIOStatsCollector()
	if err != nil {
		slog.Warn("[unisondb.cliapp] Failed to create I/O stats collector", "error", err)
	} else {
		err = prometheus.Register(ioCollector)
		if err != nil {
			slog.Warn("[unisondb.cliapp] Failed to register I/O stats collector", "error", err)
		}
	}

	ms.fuzzStats = fuzzer.NewFuzzStats()

	sink, err := hashiprom.NewPrometheusSink()
	if err != nil {
		return err
	}

	defaultConfig := metrics.DefaultConfig("")
	defaultConfig.TimerGranularity = time.Second
	defaultConfig.EnableHostname = false
	defaultConfig.EnableRuntimeMetrics = false
	_, err = metrics.NewGlobal(defaultConfig, sink)

	if err != nil {
		return err
	}

	streamer.RegisterMetrics()
	reporter := promreporter.NewReporter(promreporter.Options{
		Registerer: prometheus.DefaultRegisterer,
		Gatherer:   prometheus.DefaultGatherer,
	})

	closer, err := umetrics.Initialize(umetrics.Options{
		Prefix:         "unisondb",
		Reporter:       reporter,
		ReportInterval: 10 * time.Second,
		CommonTags:     nil,
	})
	if err != nil {
		return err
	}

	ms.DeferCallback = append(ms.DeferCallback, func(ctx context.Context) {
		err := closer.Close()
		if err != nil {
			slog.Error("[unisondb.cliapp] tally.Closer: failed", "error", err)
		}
	})
	return nil
}

func (ms *Server) SetupStorageConfig(ctx context.Context) error {
	if ms.cfg.RaftConfig.Enabled && (ms.mode == modeReplica || ms.mode == modeRelay) {
		return fmt.Errorf("raft: cannot enable raft in %s mode", ms.mode)
	}

	storeConfig := dbkernel.NewDefaultEngineConfig()

	if ms.cfg.Storage.WalFsyncInterval != "" {
		duration, err := time.ParseDuration(ms.cfg.Storage.WalFsyncInterval)
		if err != nil {
			return err
		}
		storeConfig.WalConfig.SyncInterval = duration
	}

	if ms.cfg.Storage.SegmentSize != "" {
		original := etc.ParseSize(ms.cfg.Storage.SegmentSize)
		value := clampInt64(original, 4<<20, math.MaxInt64)
		logClamped("SegmentSize", original, value)
		storeConfig.WalConfig.SegmentSize = value
	}

	if ms.cfg.Storage.BytesPerSync != "" {
		original := etc.ParseSize(ms.cfg.Storage.BytesPerSync)
		value := clampToUint32(original, 512)
		logClamped("BytesPerSync", original, int64(value))
		storeConfig.WalConfig.BytesPerSync = value
	}

	if ms.cfg.Storage.ArenaSize != "" {
		original := etc.ParseSize(ms.cfg.Storage.ArenaSize)
		value := clampInt64(original, 1<<20, math.MaxInt64)
		logClamped("ArenaSize", original, value)
		storeConfig.ArenaSize = value
	}

	if ms.cfg.WriteNotifyConfig.Enabled {
		dur, err := time.ParseDuration(ms.cfg.WriteNotifyConfig.MaxDelay)
		if err != nil {
			return err
		}
		if dur <= 1 {
			return errors.New("invalid value for write notify config: max delay must be greater than 1")
		}
		storeConfig.WriteNotifyCoalescing.Enabled = true
		storeConfig.WriteNotifyCoalescing.Duration = dur
	}
	storeConfig.DisableEntryTypeCheck = ms.cfg.Storage.DisableEntryTypeCheck
	ms.storageConfig = storeConfig
	return ms.setupWalCleanup(ctx)
}

func (ms *Server) setupWalCleanup(_ context.Context) error {
	if ms.cfg.Storage.WALCleanupConfig.Enabled {
		cleanUpDur, err := time.ParseDuration(ms.cfg.Storage.WALCleanupConfig.Interval)
		if err != nil {
			return err
		}
		if cleanUpDur <= 1 {
			return errors.New("invalid value for wal cleanup config: interval must be greater than 1")
		}
		ms.storageConfig.WalConfig.CleanupInterval = cleanUpDur

		dur, err := time.ParseDuration(ms.cfg.Storage.WALCleanupConfig.MaxAge)
		if err != nil {
			return err
		}
		if dur <= 1 {
			return errors.New("invalid value for wal cleanup config: max age must be greater than 1")
		}
		ms.storageConfig.WalConfig.AutoCleanup = true
		ms.storageConfig.WalConfig.MaxAge = dur
		ms.storageConfig.WalConfig.MaxSegment = ms.cfg.Storage.WALCleanupConfig.MaxSegments
		ms.storageConfig.WalConfig.MinSegment = ms.cfg.Storage.WALCleanupConfig.MinSegments
	}

	return nil
}

func (ms *Server) SetupNotifier(ctx context.Context) error {
	if len(ms.cfg.NotifierConfigs) == 0 {
		return nil
	}

	ms.notifiers = make(map[string]notifier.Notifier)

	for namespace, notifierCfg := range ms.cfg.NotifierConfigs {
		if !slices.Contains(ms.cfg.Storage.Namespaces, namespace) {
			slog.Warn("[unisondb.cliapp] Notifier configured for non-existent namespace",
				slog.String("namespace", namespace))
			continue
		}

		bindPort := notifierCfg.BindPort

		if !isValidPort(bindPort) {
			return fmt.Errorf("invalid value for notifier binding port: %d", bindPort)
		}

		bindAddr := fmt.Sprintf("tcp://localhost:%d", bindPort)

		// notifier for this namespace
		zmqNotifierCfg := notifier.Config{
			BindAddress:   bindAddr,
			Namespace:     namespace,
			HighWaterMark: notifierCfg.HighWaterMark,
			LingerTime:    notifierCfg.LingerTime,
		}

		zmqNotifier, err := notifier.NewZeroMQNotifier(zmqNotifierCfg)
		if err != nil {
			return fmt.Errorf("failed to create ZeroMQ notifier for namespace %s: %w", namespace, err)
		}

		ms.notifiers[namespace] = zmqNotifier

		ns := namespace
		ms.DeferCallback = append(ms.DeferCallback, func(ctx context.Context) {
			err := zmqNotifier.Close()
			if err != nil {
				slog.Error("[unisondb.cliapp] SetupNotifier: close ZeroMQ notifier failed",
					"namespace", ns, "error", err)
			}
		})

		slog.Info("[unisondb.cliapp]",
			slog.String("event_type", "zeromq.notifier.created"),
			slog.String("namespace", namespace),
			slog.String("bind_address", bindAddr))
	}

	slog.Info("[unisondb.cliapp]",
		slog.String("event_type", "zeromq.notifiers.initialized"),
		slog.Int("namespace_count", len(ms.notifiers)))

	return nil
}

func (ms *Server) SetupStorage(ctx context.Context) error {
	for _, namespace := range ms.cfg.Storage.Namespaces {
		engineConfig := *ms.storageConfig

		if ms.mode == modeReplica || ms.mode == modeRelay {
			engineConfig.ReadOnly = true
		}

		// Enable Raft mode WAL options when Raft is enabled in config
		if ms.cfg.RaftConfig.Enabled {
			engineConfig.WalConfig.RaftMode = true
		}

		if ms.notifiers != nil {
			if zmqNotifier, exists := ms.notifiers[namespace]; exists {
				engineConfig.ChangeNotifier = zmqNotifier
			}
		}

		store, err := dbkernel.NewStorageEngine(ms.cfg.Storage.BaseDir, namespace, &engineConfig)
		if err != nil {
			return err
		}

		ms.engines[namespace] = store
		ms.DeferCallback = append(ms.DeferCallback, func(ctx context.Context) {
			err := store.Close(ctx)
			if err != nil {
				slog.Error("[unisondb.cliapp] SetupStorage: close storage engine failed", "error", err)
			}
		})
	}
	return nil
}

func buildNamespaceGrpcClients(cfg config.Config) (map[string]*grpc.ClientConn, error) {
	namespaceToConn := make(map[string]*grpc.ClientConn)
	relayHashToConn := make(map[string]*grpc.ClientConn)
	var mu sync.Mutex

	for _, relay := range cfg.RelayConfigs {
		hash := config.HashRelayConfig(relay)

		mu.Lock()
		conn, exists := relayHashToConn[hash]
		if !exists {
			var err error
			conn, err = config.NewRelayerGRPCConn(&relay)
			if err != nil {
				mu.Unlock()
				return nil, fmt.Errorf("failed to dial %s: %w", relay.UpstreamAddress, err)
			}

			relayHashToConn[hash] = conn
		}
		mu.Unlock()

		for _, ns := range relay.Namespaces {
			namespaceToConn[ns] = conn
		}
	}
	return namespaceToConn, nil
}

func buildNamespaceLSNLagMap(cfg *config.Config) (map[string]int, error) {
	nsLagMap := make(map[string]int)

	for name, relay := range cfg.RelayConfigs {
		for _, ns := range relay.Namespaces {
			if _, exists := nsLagMap[ns]; exists {
				return nil, fmt.Errorf("duplicate namespace '%s' found in relay '%s'", ns, name)
			}
			nsLagMap[ns] = relay.LSNLagThreshold
		}
	}
	return nsLagMap, nil
}
