package cliapp

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"sync"
	"time"

	"github.com/ankur-anand/unisondb/cmd/unisondb/config"
	"github.com/ankur-anand/unisondb/dbkernel"
	"github.com/ankur-anand/unisondb/internal/etc"
	"github.com/ankur-anand/unisondb/internal/grpcutils"
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
		value := etc.ParseSize(ms.cfg.Storage.SegmentSize)
		storeConfig.WalConfig.SegmentSize = value
	}

	if ms.cfg.Storage.BytesPerSync != "" {
		value := etc.ParseSize(ms.cfg.Storage.BytesPerSync)
		storeConfig.WalConfig.BytesPerSync = uint32(value)
	}

	if ms.cfg.Storage.ArenaSize != "" {
		value := etc.ParseSize(ms.cfg.Storage.ArenaSize)
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

	for _, r := range ms.relayer {
		go func() {
			relayer.RunLoop(ctx, relayer.LoopOptions{
				Namespace:      r.namespace,
				Run:            r.relayer.StartRelay,
				Limiter:        limiter,
				Classify:       relayer.DefaultClassifier,
				OnPermanentErr: func(err error) {},
			})
		}()
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
