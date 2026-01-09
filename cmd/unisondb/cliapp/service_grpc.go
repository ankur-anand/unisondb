package cliapp

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"time"

	"github.com/ankur-anand/unisondb/internal/grpcutils"
	"github.com/ankur-anand/unisondb/internal/services/streamer"
	"github.com/ankur-anand/unisondb/pkg/svcutils"
	v1 "github.com/ankur-anand/unisondb/schemas/proto/gen/go/unisondb/replicator/v1"
	v1Streamer "github.com/ankur-anand/unisondb/schemas/proto/gen/go/unisondb/streamer/v1"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/reflection"
)

var kAlvPolicy = keepalive.EnforcementPolicy{
	MinTime:             20 * time.Second,
	PermitWithoutStream: true,
}

type GRPCService struct {
	server       *grpc.Server
	streamer     *streamer.GrpcStreamer
	statsHandler *grpcutils.GRPCStatsHandler
	addr         string
	boundAddr    string
	ready        chan struct{}
	enabled      bool
	deps         *Dependencies
}

func (g *GRPCService) Name() string {
	return "grpc"
}

func (g *GRPCService) Setup(ctx context.Context, deps *Dependencies) error {
	g.ready = make(chan struct{})

	// replica mode doesn't serve downstream, so no gRPC server needed
	if deps.Mode == modeReplica {
		slog.Info("[unisondb.cliapp] gRPC server disabled in replica mode (no downstream serving)")
		g.enabled = false
		close(g.ready)
		return nil
	}
	g.enabled = true
	g.deps = deps

	errGroup, _ := errgroup.WithContext(ctx)
	g.streamer = streamer.NewGrpcStreamer(errGroup, deps.Engines, 2*time.Minute)

	grpcMethods := grpcutils.RegisterGRPCSMethods(
		v1Streamer.WalStreamerService_ServiceDesc,
		v1.KVStoreWriteService_ServiceDesc,
		v1.KVStoreReadService_ServiceDesc,
	)

	enabledMethodsLogs := make(map[string]bool)
	for method := range grpcMethods {
		enabledMethodsLogs[method] = true
	}
	enabledMethodsLogs["/unisondb.streamer.v1.WalStreamerService/GetLatestLSN"] = false

	g.statsHandler = grpcutils.NewGRPCStatsHandler(grpcMethods)
	// StreamAgeService need this for stats collection
	deps.StatsHandler = g.statsHandler

	ir := grpcutils.NewStatefulInterceptor(deps.Logger, enabledMethodsLogs)
	var serverOpts []grpc.ServerOption

	if deps.Config.Grpc.AllowInsecure {
		slog.Warn("[unisondb.cliapp]",
			slog.String("event_type", "grpc.INSECURE.Mode"),
			slog.Bool("allow_insecure", deps.Config.Grpc.AllowInsecure),
			slog.Int("port", deps.Config.Grpc.Port))
	} else {
		creds, err := svcutils.NewMTLSCreds(
			deps.Config.Grpc.CertPath,
			deps.Config.Grpc.KeyPath,
			deps.Config.Grpc.CAPath,
		)
		if err != nil {
			return fmt.Errorf("failed to load TLS credentials: %w", err)
		}
		serverOpts = append(serverOpts, grpc.Creds(creds))
	}

	serverOpts = append(serverOpts,
		grpc.StatsHandler(g.statsHandler),
		grpc.ChainStreamInterceptor(
			grpcutils.RequireNamespaceInterceptor,
			grpcutils.RequestIDStreamInterceptor,
			grpcutils.MethodInterceptor,
			ir.TelemetryStreamInterceptor,
		),
		grpc.ChainUnaryInterceptor(
			grpcutils.RequireNamespaceUnaryInterceptor,
			grpcutils.RequestIDUnaryInterceptor,
			grpcutils.MethodUnaryInterceptor,
			ir.TelemetryUnaryInterceptor,
		),
		grpc.KeepaliveEnforcementPolicy(kAlvPolicy),
		grpc.KeepaliveParams(keepalive.ServerParameters{
			Time: 5 * time.Minute,
		}),
		grpc.InitialWindowSize(4<<20),
		grpc.InitialConnWindowSize(8<<20),
		grpc.WriteBufferSize(256*1024),
		grpc.ReadBufferSize(256*1024),
		grpc.WaitForHandlers(true),
	)

	g.server = grpc.NewServer(serverOpts...)

	healthServer := health.NewServer()
	healthpb.RegisterHealthServer(g.server, healthServer)

	v1Streamer.RegisterWalStreamerServiceServer(g.server, g.streamer)

	if deps.Env != "prod" {
		reflection.Register(g.server)
	}

	ip := deps.Config.Grpc.ListenIP
	if ip == "" {
		ip = "0.0.0.0"
	}
	g.addr = fmt.Sprintf("%s:%d", ip, deps.Config.Grpc.Port)

	return nil
}

func (g *GRPCService) Run(ctx context.Context) error {
	if !g.enabled {
		return nil
	}

	var lis net.ListenConfig
	l, err := lis.Listen(ctx, "tcp", g.addr)
	if err != nil {
		return fmt.Errorf("grpc listen error: %w", err)
	}

	g.boundAddr = l.Addr().String()
	close(g.ready)

	slog.Info("[unisondb.cliapp]",
		slog.String("event_type", "gRPC.server.started"),
		slog.String("addr", g.boundAddr),
	)

	errCh := make(chan error, 1)
	go func() {
		errCh <- g.server.Serve(l)
	}()

	select {
	case err := <-errCh:
		return err
	case <-ctx.Done():
		return nil
	}
}

func (g *GRPCService) Close(ctx context.Context) error {
	if g.server == nil {
		return nil
	}

	slog.Info("[unisondb.cliapp]",
		slog.String("event_type", "stopping.gRPC.server"))

	if g.streamer != nil {
		g.streamer.Close()
	}
	g.server.GracefulStop()

	return nil
}

func (g *GRPCService) Enabled() bool {
	return g.enabled
}

func (g *GRPCService) Addr() string {
	return g.addr
}

func (g *GRPCService) BoundAddr() string {
	return g.boundAddr
}

func (g *GRPCService) Ready() <-chan struct{} {
	return g.ready
}
