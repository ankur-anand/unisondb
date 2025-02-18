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

	"github.com/ankur-anand/kvalchemy/cmd/replicator/config"
	"github.com/ankur-anand/kvalchemy/internal/xy"
	v1 "github.com/ankur-anand/kvalchemy/proto/gen/go/kvalchemy/replicator/v1"
	"github.com/ankur-anand/kvalchemy/replicator"
	"github.com/ankur-anand/kvalchemy/storage"
	"github.com/hashicorp/go-metrics"
	hashiprom "github.com/hashicorp/go-metrics/prometheus"
	"github.com/pelletier/go-toml/v2"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"google.golang.org/grpc"
	"google.golang.org/grpc/keepalive"
)

var (
	cfgFile = flag.String("c", "./config.toml", "config file")
	env     = flag.String("e", "dev", "environment")
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
	engines       map[string]*storage.Engine
	grpcServer    *grpc.Server
	httpServer    *http.Server
	storageConfig *storage.StorageConfig

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
	ms.engines = make(map[string]*storage.Engine)
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
	replicator.RegisterMetrics()
	return nil
}

func (ms *mainServer) setupStorageConfig(ctx context.Context) error {
	storeConfig := storage.DefaultConfig()

	storageSettings := map[*int64]string{
		&storeConfig.SegmentSize:    ms.cfg.Storage.SegmentSize,
		&storeConfig.ValueThreshold: ms.cfg.Storage.ValueThreshold,
		&storeConfig.ArenaSize:      ms.cfg.Storage.ArenaSize,
		&storeConfig.BytesPerSync:   ms.cfg.Storage.BytesPerSync,
	}

	for field, value := range storageSettings {
		if value != "" {
			*field = xy.ParseSize(value)
		}
	}
	ms.storageConfig = storeConfig
	return nil
}

func (ms *mainServer) setupStorage(ctx context.Context) error {
	for _, namespace := range ms.cfg.Storage.Namespaces {
		store, err := storage.NewStorageEngine(ms.cfg.Storage.BaseDir, namespace, ms.storageConfig)
		fatalIfErr(err)
		ms.engines[namespace] = store
		ms.deferCallback = append(ms.deferCallback, func(ctx context.Context) {
			err := store.Close()
			if err != nil {
				slog.Error("[main] mainServer.setupStorage: close storage engine failed", "error", err)
			}
		})
	}
	return nil
}

func (ms *mainServer) setupGrpcServer(ctx context.Context) error {
	rep := replicator.NewWalReplicatorServer(ms.engines)

	gS := grpc.NewServer(grpc.ChainStreamInterceptor(replicator.RequireNamespaceInterceptor,
		replicator.RequestIDStreamInterceptor,
		replicator.CorrelationIDStreamInterceptor,
		replicator.TelemetryInterceptor),
		grpc.KeepaliveEnforcementPolicy(kAlv))

	v1.RegisterWALReplicationServiceServer(gS, rep)
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
