package cliapp

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"net"
	"net/http"
	"os"
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
	v1 "github.com/ankur-anand/unisondb/schemas/proto/gen/go/unisondb/replicator/v1"
	v1Streamer "github.com/ankur-anand/unisondb/schemas/proto/gen/go/unisondb/streamer/v1"
	"github.com/hashicorp/go-metrics"
	hashiprom "github.com/hashicorp/go-metrics/prometheus"
	"github.com/pelletier/go-toml/v2"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"golang.org/x/sync/errgroup"
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
	grpcEnabled           bool
	cfg                   config.Config
	engines               map[string]*dbkernel.Engine
	grpcServer            *grpc.Server
	httpServer            *http.Server
	storageConfig         *dbkernel.EngineConfig
	pl                    *slog.Logger
	clientGrpcConnections map[string]*grpc.ClientConn
	relayer               []relayerWithInfo
	fuzzStats             *fuzzer.FuzzStats

	// callbacks when shutdown.
	DeferCallback []func(ctx context.Context)
}

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

		pl := logutil.NewPercentLogger(logPercentage, slog.NewTextHandler(os.Stdout, nil),
			slog.LevelInfo)
		ms.pl = pl
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

	defaultConfig := metrics.DefaultConfig("unisondb")
	defaultConfig.EnableHostname = false
	_, err = metrics.New(defaultConfig, sink)
	if err != nil {
		return err
	}
	streamer.RegisterMetrics()
	return nil
}

func (ms *Server) SetupStorageConfig(ctx context.Context) error {
	storeConfig := dbkernel.NewDefaultEngineConfig()

	if ms.cfg.Storage.SegmentSize != "" {
		original := etc.ParseSize(ms.cfg.Storage.SegmentSize)
		value := clampInt64(original, 4<<20, math.MaxInt64)
		logClamped("SegmentSize", original, value)
		storeConfig.WalConfig.SegmentSize = value
	}

	if ms.cfg.Storage.BytesPerSync != "" {
		original := etc.ParseSize(ms.cfg.Storage.BytesPerSync)
		value := clampToUint32(original, 512)
		logClamped("BytesPerSync", original, value)
		storeConfig.WalConfig.BytesPerSync = value
	}

	if ms.cfg.Storage.ArenaSize != "" {
		original := etc.ParseSize(ms.cfg.Storage.ArenaSize)
		value := clampInt64(original, 1<<20, math.MaxInt64)
		logClamped("ArenaSize", original, value)
		storeConfig.ArenaSize = value
	}

	ms.storageConfig = storeConfig
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

	for _, engine := range ms.engines {
		rl := relayer.NewRelayer(engine, engine.Namespace(),
			conns[engine.Namespace()], lagConf[engine.Namespace()], ms.pl)
		ms.relayer = append(ms.relayer, relayerWithInfo{
			namespace: engine.Namespace(),
			relayer:   rl,
		})
	}

	return nil
}

func (ms *Server) SetupGrpcServer(ctx context.Context) error {
	if ms.mode == modeRelayer && !ms.grpcEnabled {
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
	for _, method := range grpcMethods {
		enabledMethodsLogs[method] = true
	}

	statsHandler := grpcutils.NewGRPCStatsHandler(grpcMethods)

	ir := grpcutils.NewStatefulInterceptor(ms.pl, enabledMethodsLogs)

	creds, err := svcutils.NewMTLSCreds(ms.cfg.Grpc.CertPath, ms.cfg.Grpc.KeyPath, ms.cfg.Grpc.CAPath)
	if err != nil {
		return err
	}

	gS := grpc.NewServer(grpc.Creds(creds),
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

	var lis net.ListenConfig
	l, err := lis.Listen(ctx, "tcp", fmt.Sprintf("localhost:%d", ms.cfg.Grpc.Port))
	if err != nil {
		return err
	}
	slog.Info("[unisondb.cliapp] gRPC server started", "port", ms.cfg.Grpc.Port)
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
	l, err := lis.Listen(ctx, "tcp", fmt.Sprintf("localhost:%d", ms.cfg.HTTPPort))
	if err != nil {
		return err
	}
	slog.Info("[unisondb.cliapp] HTTPServer started", "port", ms.cfg.HTTPPort)
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

	return g.Wait()
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

			conn, err := config.NewRelayerGRPCConn(&relay)
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

func logClamped(field string, original, clamped any) {
	if original != clamped {
		slog.Warn("[unisondb.cliapp] Storage configuration value clamped",
			"field", field,
			"original", original,
			"clamped", clamped,
		)
	}
}
