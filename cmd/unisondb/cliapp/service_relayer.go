package cliapp

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/ankur-anand/unisondb/cmd/unisondb/config"
	"github.com/ankur-anand/unisondb/internal/services/relayer"
	"golang.org/x/sync/errgroup"
	"golang.org/x/time/rate"
	"google.golang.org/grpc"
)

type relayerInfo struct {
	namespace string
	relayer   *relayer.Relayer
}

// RelayerService manages WAL replication from upstream servers.
type RelayerService struct {
	relayers    []relayerInfo
	connections map[string]*grpc.ClientConn
	enabled     bool
	deps        *Dependencies
}

func (r *RelayerService) Name() string {
	return "relayer"
}

func (r *RelayerService) Setup(ctx context.Context, deps *Dependencies) error {
	if deps.Mode != modeReplica && deps.Mode != modeRelay {
		r.enabled = false
		return nil
	}
	r.enabled = true
	r.deps = deps

	// gRPC client connections to upstream servers
	conns, err := buildNamespaceGrpcClients(deps.Config)
	if err != nil {
		return fmt.Errorf("failed to build grpc clients: %w", err)
	}
	r.connections = conns

	// namespace segment lag configuration
	lagConf, err := buildNamespaceSegmentLagMap(&deps.Config)
	if err != nil {
		return fmt.Errorf("failed to build segment lag map: %w", err)
	}

	// rate limiter if configured
	var limiter *rate.Limiter
	if deps.Config.WalIOGlobalLimiter.Enable {
		limiter = rate.NewLimiter(
			rate.Limit(deps.Config.WalIOGlobalLimiter.RateLimit),
			deps.Config.WalIOGlobalLimiter.Burst,
		)
	}

	// relayers for each engine
	for _, engine := range deps.Engines {
		conn, ok := conns[engine.Namespace()]
		if !ok || conn == nil {
			return fmt.Errorf("grpc client connection missing for namespace: %s", engine.Namespace())
		}

		rl := relayer.NewRelayer(
			engine,
			engine.Namespace(),
			conn,
			lagConf[engine.Namespace()],
			deps.Logger,
		)

		if limiter != nil {
			limitIO := relayer.NewRateLimitedWalIO(ctx, rl.CurrentWalIO(), limiter)
			rl.EnableRateLimitedWalIO(limitIO)
		}

		r.relayers = append(r.relayers, relayerInfo{
			namespace: engine.Namespace(),
			relayer:   rl,
		})
	}

	slog.Info("[unisondb.cliapp]",
		slog.String("event_type", "relayer.setup.completed"),
		slog.Int("namespace_count", len(r.relayers)),
	)

	return nil
}

func (r *RelayerService) Run(ctx context.Context) error {
	if !r.enabled {
		return nil
	}

	limiter, err := config.BuildLimiter(r.deps.Config.Limiter)
	if err != nil {
		return fmt.Errorf("failed to build limiter: %w", err)
	}

	g, ctx := errgroup.WithContext(ctx)

	for _, rel := range r.relayers {
		rel := rel
		g.Go(func() error {
			slog.Info("[unisondb.cliapp]",
				slog.String("event_type", "relayer.started"),
				slog.String("namespace", rel.namespace),
			)

			relayer.RunLoop(ctx, relayer.LoopOptions{
				Namespace:      rel.namespace,
				Run:            rel.relayer.StartRelay,
				Limiter:        limiter,
				Classify:       relayer.DefaultClassifier,
				OnPermanentErr: func(err error) {},
			})
			return nil
		})
	}

	return g.Wait()
}

func (r *RelayerService) Close(ctx context.Context) error {
	slog.Info("[unisondb.cliapp]",
		slog.String("event_type", "relayer.close.started"),
	)

	for ns, conn := range r.connections {
		if err := conn.Close(); err != nil {
			slog.Error("[unisondb.cliapp]",
				slog.String("event_type", "relayer.connection.close.error"),
				slog.String("namespace", ns),
				slog.Any("error", err),
				slog.String("connection_state", conn.GetState().String()),
			)
		}
	}

	return nil
}

func (r *RelayerService) Enabled() bool {
	return r.enabled
}
