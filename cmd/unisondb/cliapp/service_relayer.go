package cliapp

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/ankur-anand/unisondb/cmd/unisondb/config"
	"github.com/ankur-anand/unisondb/dbkernel"
	"github.com/ankur-anand/unisondb/internal/services/relayer"
	"github.com/ankur-anand/unisondb/internal/services/streamer"
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

	lagConf, err := buildNamespaceLSNLagMap(&deps.Config)
	if err != nil {
		return fmt.Errorf("failed to build LSN lag map: %w", err)
	}

	// rate limiter if configured
	var limiter *rate.Limiter
	if deps.Config.WalIOGlobalLimiter.Enable {
		limiter = rate.NewLimiter(
			rate.Limit(deps.Config.WalIOGlobalLimiter.RateLimit),
			deps.Config.WalIOGlobalLimiter.Burst,
		)
	}

	// Resolve per-namespace streamer type.
	nsStreamerType := buildNamespaceStreamerTypeMap(&deps.Config)

	// relayers for each engine
	for _, engine := range deps.Engines {
		ns := engine.Namespace()
		st := nsStreamerType[ns]

		var rl *relayer.Relayer
		switch st {
		case config.StreamerTypeBlobStore:
			var err error
			rl, err = r.setupBlobStoreRelayer(ctx, engine, ns, lagConf[ns], deps)
			if err != nil {
				return fmt.Errorf("blobstore relayer setup for %s: %w", ns, err)
			}
		default:
			conn, ok := conns[ns]
			if !ok || conn == nil {
				return fmt.Errorf("grpc client connection missing for namespace: %s", ns)
			}

			rl = relayer.NewRelayer(
				engine,
				ns,
				conn,
				lagConf[ns],
				deps.Logger,
			)

			if limiter != nil {
				limitIO := relayer.NewRateLimitedWalIO(ctx, rl.CurrentWalIO(), limiter)
				rl.EnableRateLimitedWalIO(limitIO)
			}
		}

		r.relayers = append(r.relayers, relayerInfo{
			namespace: ns,
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

// setupBlobStoreRelayer creates a Relayer backed by a BlobStoreStreamerClient.
func (r *RelayerService) setupBlobStoreRelayer(
	ctx context.Context,
	engine *dbkernel.Engine,
	ns string,
	lsnLagThreshold int,
	deps *Dependencies,
) (*relayer.Relayer, error) {
	bsCfg := findBlobStoreConfig(ns, &deps.Config)
	if bsCfg.BucketURL == "" {
		return nil, fmt.Errorf("blobstore bucket_url not configured for namespace %s", ns)
	}

	store, err := streamer.OpenNamespaceBlobStore(ctx, bsCfg.BucketURL, bsCfg.Prefix, ns)
	if err != nil {
		return nil, fmt.Errorf("open blobstore: %w", err)
	}

	handler := dbkernel.NewReplicaWALHandler(engine)
	walIO := relayer.NewWalIOHandler(handler)

	currentLSN := engine.OpsReceivedCount()
	var refreshInterval time.Duration
	if bsCfg.RefreshInterval != "" {
		if d, err := time.ParseDuration(bsCfg.RefreshInterval); err == nil {
			refreshInterval = d
		}
	}
	client := streamer.NewBlobStoreStreamerClient(store, ns, walIO, currentLSN, refreshInterval)

	if bsCfg.CacheDir != "" {
		client.CacheDir = bsCfg.CacheDir
	}

	rl := relayer.NewRelayerWithStreamer(engine, ns, client, walIO, lsnLagThreshold, deps.Logger)
	return rl, nil
}

func findBlobStoreConfig(ns string, cfg *config.Config) config.BlobStoreRelayConfig {
	for _, relay := range cfg.RelayConfigs {
		for _, rns := range relay.Namespaces {
			if rns == ns {
				return relay.BlobStore
			}
		}
	}
	return config.BlobStoreRelayConfig{}
}

func buildNamespaceStreamerTypeMap(cfg *config.Config) map[string]config.StreamerType {
	m := make(map[string]config.StreamerType)
	for _, relay := range cfg.RelayConfigs {
		st := relay.StreamerType
		if st == "" {
			st = config.StreamerTypeGRPC
		}
		for _, ns := range relay.Namespaces {
			m[ns] = st
		}
	}
	return m
}
