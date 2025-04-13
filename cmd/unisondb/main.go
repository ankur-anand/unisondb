package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/ankur-anand/unisondb/cmd/unisondb/config"
	"github.com/ankur-anand/unisondb/dbkernel"
	"github.com/ankur-anand/unisondb/internal/etc"
	"github.com/ankur-anand/unisondb/internal/grpcutils"
	"github.com/ankur-anand/unisondb/internal/services/kvstore"
	"github.com/ankur-anand/unisondb/internal/services/streamer"
	"github.com/ankur-anand/unisondb/pkg/logutil"
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
	"google.golang.org/grpc/keepalive"
)

var (
	modeReplicator = "replicator"
	modeRelayer    = "relayer"
)

var (
	cfgFile = flag.String("c", "./config.toml", "config file")
	env     = flag.String("e", "dev", "environment")
	// possible value are replicator and relay.
	mode = flag.String("m", "replicator", "mode")
)

var kAlv = keepalive.EnforcementPolicy{
	MinTime:             5 * time.Second, // don't allow < 5 sec ping
	PermitWithoutStream: false,           // No Ping without active stream.
}

func main() {
	flag.Parse()
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	server := &mainServer{}
	setupFunc := []func(context.Context) error{
		server.init,
		server.initTelemetry,
		server.setupStorageConfig,
		server.setupStorage,
		server.setupGrpcServer,
		server.setupHTTPServer,
	}

	for _, fn := range setupFunc {
		if err := fn(ctx); err != nil {
			fatalIfErr(err)
		}
	}

	slog.Info("[main] replicator started", "config-file", *cfgFile, "env", *env)

	runFunc := []func(context.Context) error{
		server.RunGrpc,
		server.RunHTTP,
	}

	for _, fn := range runFunc {
		if err := fn(ctx); err != nil {
			fatalIfErr(err)
		}
	}

	<-ctx.Done()
	cancel()
	slog.Info("[main] replicator server shutting down")
	ctx, cancel = context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	for _, fn := range server.deferCallback {
		fn(ctx)
	}
}

type mainServer struct {
	cfg           config.Config
	engines       map[string]*dbkernel.Engine
	grpcServer    *grpc.Server
	httpServer    *http.Server
	storageConfig *dbkernel.EngineConfig

	// callbacks when shutdown.
	deferCallback []func(ctx context.Context)
}

func fatalIfErr(err error) {
	if err != nil {
		log.Fatalln(fmt.Errorf("[main] mainServer.fatalIfErr: %v", err))
	}
}

func (ms *mainServer) init(ctx context.Context) error {
	cfgBytes, err := os.ReadFile(*cfgFile)
	fatalIfErr(err)
	err = toml.Unmarshal(cfgBytes, &ms.cfg)
	fatalIfErr(err)
	ms.engines = make(map[string]*dbkernel.Engine)
	return nil
}

func (ms *mainServer) initTelemetry(ctx context.Context) error {
	prometheus.Unregister(collectors.NewGoCollector())
	err := prometheus.Register(collectors.NewBuildInfoCollector())
	fatalIfErr(err)

	sink, err := hashiprom.NewPrometheusSink()
	fatalIfErr(err)
	defaultConfig := metrics.DefaultConfig("kvalchemy")
	defaultConfig.EnableHostname = false
	_, err = metrics.New(defaultConfig, sink)
	fatalIfErr(err)
	streamer.RegisterMetrics()
	return nil
}

func (ms *mainServer) setupStorageConfig(ctx context.Context) error {
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

func (ms *mainServer) setupStorage(ctx context.Context) error {
	for _, namespace := range ms.cfg.Storage.Namespaces {
		store, err := dbkernel.NewStorageEngine(ms.cfg.Storage.BaseDir, namespace, ms.storageConfig)
		fatalIfErr(err)
		ms.engines[namespace] = store
		ms.deferCallback = append(ms.deferCallback, func(ctx context.Context) {
			err := store.Close(ctx)
			if err != nil {
				slog.Error("[main] mainServer.setupStorage: close storage engine failed", "error", err)
			}
		})
	}
	return nil
}

func (ms *mainServer) setupGrpcServer(ctx context.Context) error {
	errGroup, _ := errgroup.WithContext(ctx)
	rep := streamer.NewGrpcStreamer(errGroup, ms.engines, 2*time.Minute)
	kvr := kvstore.NewKVReaderService(ms.engines)
	kvw := kvstore.NewKVWriterService(ms.engines)

	pl := logutil.NewPercentLogger(map[slog.Level]float64{
		slog.LevelInfo: 10,
	}, slog.NewTextHandler(os.Stdout, nil), slog.LevelInfo)
	ir := grpcutils.NewStatefulInterceptor(pl, make(map[string]bool))
	gS := grpc.NewServer(grpc.ChainStreamInterceptor(grpcutils.RequireNamespaceInterceptor,
		grpcutils.RequestIDStreamInterceptor,
		grpcutils.MethodInterceptor,
		ir.TelemetryStreamInterceptor),

		grpc.ChainUnaryInterceptor(grpcutils.RequireNamespaceUnaryInterceptor,
			grpcutils.RequestIDUnaryInterceptor,
			grpcutils.MethodUnaryInterceptor,
			ir.TelemetryUnaryInterceptor),

		grpc.KeepaliveEnforcementPolicy(kAlv))

	v1Streamer.RegisterWalStreamerServiceServer(gS, rep)
	v1.RegisterKVStoreReadServiceServer(gS, kvr)
	// only register write server if allowed
	if *mode == "replicator" {
		v1.RegisterKVStoreWriteServiceServer(gS, kvw)
	}

	ms.grpcServer = gS
	ms.deferCallback = append(ms.deferCallback, func(ctx context.Context) {
		gS.GracefulStop()
	})
	return nil
}

func (ms *mainServer) setupHTTPServer(ctx context.Context) error {
	mux := http.NewServeMux()
	mux.Handle("GET /metrics", promhttp.Handler())

	ms.httpServer = &http.Server{
		WriteTimeout: time.Second * 15,
		ReadTimeout:  time.Second * 15,
		IdleTimeout:  time.Second * 60,
		Handler:      mux,
	}
	ms.deferCallback = append(ms.deferCallback, func(ctx context.Context) {
		err := ms.httpServer.Shutdown(ctx)
		if err != nil {
			slog.Error("[main] mainServer.setupHTTPServer: shutdown http server failed", "error", err)
		}
	})
	return nil
}

func (ms *mainServer) RunGrpc(ctx context.Context) error {
	go func() {
		var lis net.ListenConfig
		l, err := lis.Listen(ctx, "tcp", fmt.Sprintf("localhost:%d", ms.cfg.Grpc.Port))
		fatalIfErr(err)
		slog.Info("[main] replicator grpc server started", "port", ms.cfg.Grpc.Port)
		err = ms.grpcServer.Serve(l)
		fatalIfErr(err)
	}()
	return nil
}

func (ms *mainServer) RunHTTP(ctx context.Context) error {
	go func() {
		var lis net.ListenConfig
		l, err := lis.Listen(ctx, "tcp", fmt.Sprintf("localhost:%d", ms.cfg.HTTPPort))
		fatalIfErr(err)
		slog.Info("[main] replicator http server started", "port", ms.cfg.HTTPPort)
		err = ms.httpServer.Serve(l)
		if !errors.Is(err, http.ErrServerClosed) {
			fatalIfErr(err)
		}
	}()
	return nil
}
