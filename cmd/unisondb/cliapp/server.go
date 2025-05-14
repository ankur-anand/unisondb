package cliapp

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"net"
	"net/http"
	_ "net/http/pprof"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/ankur-anand/unisondb/cmd/unisondb/config"
	"github.com/ankur-anand/unisondb/dbkernel"
	"github.com/ankur-anand/unisondb/internal/etc"
	"github.com/ankur-anand/unisondb/internal/grpcutils"
	"github.com/ankur-anand/unisondb/internal/services/fuzzer"
	"github.com/ankur-anand/unisondb/internal/services/kvstore"
	"github.com/ankur-anand/unisondb/internal/services/relayer"
	"github.com/ankur-anand/unisondb/internal/services/streamer"
	"github.com/ankur-anand/unisondb/pkg/logutil"
	"github.com/ankur-anand/unisondb/pkg/svcutils"
	"github.com/ankur-anand/unisondb/pkg/umetrics"
	v1 "github.com/ankur-anand/unisondb/schemas/proto/gen/go/unisondb/replicator/v1"
	v1Streamer "github.com/ankur-anand/unisondb/schemas/proto/gen/go/unisondb/streamer/v1"
	"github.com/hashicorp/go-metrics"
	hashiprom "github.com/hashicorp/go-metrics/prometheus"
	"github.com/pelletier/go-toml/v2"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	promreporter "github.com/uber-go/tally/v4/prometheus"
	"golang.org/x/sync/errgroup"
	"golang.org/x/time/rate"
	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/reflection"
)

var (
	modeReplicator = "replicator"
	modeRelayer    = "relayer"
	modeFuzzer     = "fuzzer"
)

var kAlv = keepalive.EnforcementPolicy{
	// don't allow < 20 sec ping
	MinTime:             20 * time.Second,
	PermitWithoutStream: true,
}

type relayerWithInfo struct {
	namespace string
	relayer   *relayer.Relayer
}

type Server struct {
	mode                  string
	env                   string
	relayerGRPCEnabled    bool
	cfg                   config.Config
	engines               map[string]*dbkernel.Engine
	grpcServer            *grpc.Server
	httpServer            *http.Server
	storageConfig         *dbkernel.EngineConfig
	pl                    *slog.Logger
	clientGrpcConnections map[string]*grpc.ClientConn
	relayer               []relayerWithInfo
	fuzzStats             *fuzzer.FuzzStats
	statsHandler          *grpcutils.GRPCStatsHandler

	pprofServer *http.Server

	// callbacks when shutdown.
	DeferCallback []func(ctx context.Context)
}

func (ms *Server) InitFromCLI(cfgPath, env, mode string, relayerGRPCEnabled bool) func(ctx context.Context) error {
	return func(ctx context.Context) error {
		ms.mode = mode
		ms.env = env
		ms.relayerGRPCEnabled = relayerGRPCEnabled

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

func (ms *Server) InitTelemetry(ctx context.Context) error {
	prometheus.Unregister(collectors.NewGoCollector())
	err := prometheus.Register(collectors.NewBuildInfoCollector())
	if err != nil {
		return err
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

	ms.storageConfig = storeConfig
	return ms.setupWalCleanup(ctx)
}

func (ms *Server) setupWalCleanup(ctx context.Context) error {
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

func (ms *Server) SetupStorage(ctx context.Context) error {
	for _, namespace := range ms.cfg.Storage.Namespaces {
		store, err := dbkernel.NewStorageEngine(ms.cfg.Storage.BaseDir, namespace, ms.storageConfig)
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

func (ms *Server) SetupRelayer(ctx context.Context) error {
	if ms.mode != modeRelayer {
		return nil
	}
	conns, err := buildNamespaceGrpcClients(ms.cfg)
	if err != nil {
		return err
	}

	ms.clientGrpcConnections = conns

	ms.DeferCallback = append(ms.DeferCallback, func(ctx context.Context) {
		for _, conn := range ms.clientGrpcConnections {
			err := conn.Close()
			if err != nil {
				slog.Error("[unisondb.cliapp] SetupRelayer: close client grpc connection failed]",
					"err", err, "connection-state", conn.GetState())
			}
		}
	})

	lagConf, err := buildNamespaceSegmentLagMap(&ms.cfg)
	if err != nil {
		return err
	}

	var limiter *rate.Limiter
	if ms.cfg.WalIOGlobalLimiter.Enable {
		limiter = rate.NewLimiter(rate.Limit(ms.cfg.WalIOGlobalLimiter.RateLimit), ms.cfg.WalIOGlobalLimiter.Burst)
	}

	for _, engine := range ms.engines {
		conn, ok := conns[engine.Namespace()]
		if !ok || conn == nil {
			return fmt.Errorf("grpc client connection missing for namespace: %s", engine.Namespace())
		}

		rl := relayer.NewRelayer(engine, engine.Namespace(),
			conns[engine.Namespace()], lagConf[engine.Namespace()], ms.pl)

		if limiter != nil {
			limitIO := relayer.NewRateLimitedWalIO(ctx, rl.CurrentWalIO(), limiter)
			rl.EnableRateLimitedWalIO(limitIO)
		}

		ms.relayer = append(ms.relayer, relayerWithInfo{
			namespace: engine.Namespace(),
			relayer:   rl,
		})
	}

	return nil
}

func (ms *Server) SetupGrpcServer(ctx context.Context) error {
	if ms.mode == modeRelayer && !ms.relayerGRPCEnabled {
		slog.Info("[unisondb.cliapp] gRPC server disabled in relayer mode by flag/config")
		return nil
	}
	errGroup, _ := errgroup.WithContext(ctx)

	rep := streamer.NewGrpcStreamer(errGroup, ms.engines, 2*time.Minute)
	kvr := kvstore.NewKVReaderService(ms.engines)
	kvw := kvstore.NewKVWriterService(ms.engines)

	grpcMethods := grpcutils.RegisterGRPCSMethods(v1Streamer.WalStreamerService_ServiceDesc,
		v1.KVStoreWriteService_ServiceDesc,
		v1.KVStoreReadService_ServiceDesc)

	enabledMethodsLogs := make(map[string]bool)
	for method := range grpcMethods {
		enabledMethodsLogs[method] = true
	}

	statsHandler := grpcutils.NewGRPCStatsHandler(grpcMethods)
	ms.statsHandler = statsHandler

	ir := grpcutils.NewStatefulInterceptor(ms.pl, enabledMethodsLogs)
	var serverOpts []grpc.ServerOption

	if ms.cfg.Grpc.AllowInsecure {
		slog.Warn("[unisondb.cliapp]",
			slog.String("event_type", "grpc.INSECURE.Mode"),
			slog.Bool("allow_insecure", ms.cfg.Grpc.AllowInsecure),
			slog.Int("port", ms.cfg.Grpc.Port))
	} else {
		creds, err := svcutils.NewMTLSCreds(ms.cfg.Grpc.CertPath, ms.cfg.Grpc.KeyPath, ms.cfg.Grpc.CAPath)
		if err != nil {
			return fmt.Errorf("failed to load TLS credentials: %w", err)
		}
		serverOpts = append(serverOpts, grpc.Creds(creds))
	}

	serverOpts = append(serverOpts,
		grpc.StatsHandler(statsHandler),
		grpc.ChainStreamInterceptor(grpcutils.RequireNamespaceInterceptor,
			grpcutils.RequestIDStreamInterceptor,
			grpcutils.MethodInterceptor,
			ir.TelemetryStreamInterceptor),

		grpc.ChainUnaryInterceptor(grpcutils.RequireNamespaceUnaryInterceptor,
			grpcutils.RequestIDUnaryInterceptor,
			grpcutils.MethodUnaryInterceptor,
			ir.TelemetryUnaryInterceptor),

		grpc.KeepaliveEnforcementPolicy(kAlv),
		grpc.KeepaliveParams(keepalive.ServerParameters{
			// send ping frames when client is idle.
			Time: 5 * time.Minute,
		}),
		// https://github.com/grpc/grpc-go/pull/6922
		grpc.WaitForHandlers(true),
	)

	gS := grpc.NewServer(
		serverOpts...,
	)

	healthServer := health.NewServer()
	healthpb.RegisterHealthServer(gS, healthServer)

	v1Streamer.RegisterWalStreamerServiceServer(gS, rep)
	v1.RegisterKVStoreReadServiceServer(gS, kvr)
	// only register write server if allowed
	if ms.mode == modeReplicator {
		v1.RegisterKVStoreWriteServiceServer(gS, kvw)
	}

	if ms.env != "prod" {
		reflection.Register(gS)
	}

	ms.grpcServer = gS
	ms.DeferCallback = append(ms.DeferCallback, func(ctx context.Context) {
		slog.Info("[unisondb.cliapp]",
			slog.String("event_type", "stopping.gRPC.server"))
		rep.Close()
		gS.GracefulStop()
	})
	return nil
}

func (ms *Server) SetupHTTPServer(ctx context.Context) error {
	mux := http.NewServeMux()
	mux.Handle("GET /metrics", promhttp.Handler())
	mux.Handle("GET /fuzzstats", ms.fuzzStats)
	ms.httpServer = &http.Server{
		WriteTimeout: time.Second * 15,
		ReadTimeout:  time.Second * 15,
		IdleTimeout:  time.Second * 60,
		Handler:      mux,
	}
	ms.DeferCallback = append(ms.DeferCallback, func(ctx context.Context) {
		slog.Info("[unisondb.cliapp]",
			slog.String("event_type", "stopping.HTTP.server"))
		err := ms.httpServer.Shutdown(ctx)
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			slog.Error("[unisondb.cliapp] SetupHTTPServer: shutdown http server failed", "error", err)
		}
	})
	return nil
}

func (ms *Server) RunGrpc(ctx context.Context) error {
	if ms.grpcServer == nil {
		slog.Debug("[unisondb.cliapp] gRPC server not initialized, skipping start")
		return nil
	}

	ip := ms.cfg.Grpc.ListenIP
	if ip == "" {
		ip = "0.0.0.0"
	}
	addr := fmt.Sprintf("%s:%d", ip, ms.cfg.Grpc.Port)

	var lis net.ListenConfig
	l, err := lis.Listen(ctx, "tcp", addr)
	if err != nil {
		return err
	}
	slog.Info("[unisondb.cliapp]",
		slog.String("event_type", "gRPC.server.started"),
		slog.String("listen_ip", ip),
		slog.Int("port", ms.cfg.Grpc.Port),
	)
	errCh := make(chan error, 1)
	go func() {
		errCh <- ms.grpcServer.Serve(l)
	}()
	select {
	case err := <-errCh:
		return err
	case <-ctx.Done():
		return nil
	}
}

func (ms *Server) RunHTTP(ctx context.Context) error {
	var lis net.ListenConfig

	ip := ms.cfg.ListenIP
	if ip == "" {
		ip = "0.0.0.0"
	}
	addr := fmt.Sprintf("%s:%d", ip, ms.cfg.HTTPPort)

	l, err := lis.Listen(ctx, "tcp", addr)
	if err != nil {
		return err
	}
	slog.Info("[unisondb.cliapp]",
		slog.String("event_type", "HTTP.server.started"),
		slog.String("listen_ip", ip),
		slog.Int("port", ms.cfg.HTTPPort),
	)
	errCh := make(chan error, 1)
	go func() {
		errCh <- ms.httpServer.Serve(l)
	}()

	select {
	case <-ctx.Done():
		return nil
	case err := <-errCh:
		if !errors.Is(err, http.ErrServerClosed) {
			return err
		}
		return nil
	}
}

func (ms *Server) StartRelayer(ctx context.Context) error {
	if ms.mode != modeRelayer {
		return nil
	}

	limiter, err := config.BuildLimiter(ms.cfg.Limiter)
	if err != nil {
		return err
	}
	g, ctx := errgroup.WithContext(ctx)

	for _, r := range ms.relayer {
		g.Go(func() error {
			relayer.RunLoop(ctx, relayer.LoopOptions{
				Namespace:      r.namespace,
				Run:            r.relayer.StartRelay,
				Limiter:        limiter,
				Classify:       relayer.DefaultClassifier,
				OnPermanentErr: func(err error) {},
			})
			return nil
		})
	}

	return g.Wait()
}

func (ms *Server) RunFuzzer(ctx context.Context) error {
	if ms.mode != modeFuzzer {
		return nil
	}

	if ms.cfg.FuzzConfig.OpsPerNamespace == 0 || ms.cfg.FuzzConfig.WorkersPerNamespace == 0 {
		return fmt.Errorf("[unisondb.cliapp] invalid fuzz config: OpsPerNamespace=%d, WorkersPerNamespace=%d",
			ms.cfg.FuzzConfig.OpsPerNamespace,
			ms.cfg.FuzzConfig.WorkersPerNamespace,
		)
	}

	g, ctx := errgroup.WithContext(ctx)

	for _, engine := range ms.engines {
		engine := engine
		g.Go(func() error {
			fuzzer.FuzzEngineOps(ctx,
				engine,
				ms.cfg.FuzzConfig.OpsPerNamespace,
				ms.cfg.FuzzConfig.WorkersPerNamespace,
				ms.fuzzStats,
				engine.Namespace(),
			)
			return nil
		})
	}

	g.Go(func() error {
		ms.fuzzStats.StartStatsMonitor(ctx, 1*time.Minute)
		return nil
	})

	if ms.cfg.FuzzConfig.LocalRelayerCount > 0 {
		for _, engine := range ms.engines {
			eng := engine
			g.Go(func() error {
				return relayer.StartNLocalRelayer(ctx, eng, ms.cfg.FuzzConfig.LocalRelayerCount, 1*time.Minute)
			})
		}
	}
	return g.Wait()
}

func (ms *Server) SetupPprofServer(ctx context.Context) error {
	if !ms.cfg.PProfConfig.Enabled {
		return nil
	}
	pprofServer := &http.Server{
		Addr:         fmt.Sprintf("localhost:%d", ms.cfg.PProfConfig.Port),
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 10 * time.Second,
		IdleTimeout:  60 * time.Second,
		Handler:      http.DefaultServeMux,
	}

	ms.pprofServer = pprofServer

	ms.DeferCallback = append(ms.DeferCallback, func(ctx context.Context) {
		slog.Info("[unisondb.cliapp]",
			slog.String("event_type", "stopping.pprof.HTTP.server"))
		if err := pprofServer.Shutdown(ctx); err != nil {
			slog.Error("[unisondb.cliapp] failed to shutdown pprof HTTP server", "err", err)
		}
	})
	return nil
}

func (ms *Server) RunPprofServer(ctx context.Context) error {
	if !ms.cfg.PProfConfig.Enabled {
		return nil
	}

	slog.Info("[unisondb.cliapp]",
		slog.String("event_type", "pprof.HTTP.server.started"),
		slog.Int("port", ms.cfg.PProfConfig.Port),
	)
	errCh := make(chan error, 1)
	go func() {
		errCh <- ms.pprofServer.ListenAndServe()
	}()

	select {
	case <-ctx.Done():
		return nil
	case err := <-errCh:
		if !errors.Is(err, http.ErrServerClosed) {
			return err
		}
		return nil
	}
}

// PeriodicLogEngineOffset log's the offset of wal entry for all the initialized namespace
// every minute.
func (ms *Server) PeriodicLogEngineOffset(ctx context.Context) error {
	tick := time.NewTicker(1 * time.Minute)
	defer tick.Stop()
	for {
		select {
		case <-tick.C:
			for _, engine := range ms.engines {
				currentOffset := engine.CurrentOffset()
				var segmentID uint32
				if currentOffset != nil {
					segmentID = currentOffset.SegmentID
				}

				slog.Info("[unisondb.cliapp]",
					slog.String("event_type", "engines.offset.report"),
					slog.Group("engine",
						slog.String("namespace", engine.Namespace()),
						slog.Uint64("current_segment_id", uint64(segmentID)),
					),
				)
			}
		case <-ctx.Done():
			return nil
		}
	}
}

func (ms *Server) PeriodicGrpcUpdateStreamAgeBuckets(ctx context.Context) error {
	if ms.statsHandler == nil {
		return nil
	}
	tick := time.NewTicker(30 * time.Second)
	defer tick.Stop()
	for {
		select {
		case <-tick.C:
			ms.statsHandler.UpdateStreamAgeBuckets()
		case <-ctx.Done():
			return nil
		}
	}
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

func buildNamespaceSegmentLagMap(cfg *config.Config) (map[string]int, error) {
	nsLagMap := make(map[string]int)

	for name, relay := range cfg.RelayConfigs {
		for _, ns := range relay.Namespaces {
			if _, exists := nsLagMap[ns]; exists {
				return nil, fmt.Errorf("duplicate namespace '%s' found in relay '%s'", ns, name)
			}
			nsLagMap[ns] = relay.SegmentLagThreshold
		}
	}
	return nsLagMap, nil
}

func clampInt64(value, min, max int64) int64 {
	if value < min {
		return min
	}
	if value > max {
		return max
	}
	return value
}

func clampToUint32(value int64, min uint32) uint32 {
	if value < 0 {
		return min
	}
	if value > int64(math.MaxUint32) {
		return math.MaxUint32
	}
	if value < int64(min) {
		return min
	}
	return uint32(value)
}

func logClamped[T comparable](field string, original, clamped T) {
	if original != clamped {
		slog.Warn("[unisondb.cliapp] Storage configuration value clamped",
			"field", field,
			"original", original,
			"clamped", clamped,
		)
	}
}

func parseLogLevel(levelStr string) (slog.Level, error) {
	switch strings.ToLower(levelStr) {
	case "debug":
		return slog.LevelDebug, nil
	case "info":
		return slog.LevelInfo, nil
	case "warn", "warning":
		return slog.LevelWarn, nil
	case "error":
		return slog.LevelError, nil
	case "":
		return slog.LevelInfo, nil
	default:
		return 0, fmt.Errorf("unknown log level: %q", levelStr)
	}
}
