package cliapp

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"time"

	"github.com/ankur-anand/unisondb/cmd/unisondb/config"
	"github.com/ankur-anand/unisondb/dbkernel"
	"github.com/ankur-anand/unisondb/pkg/raftcluster"
	"github.com/ankur-anand/unisondb/pkg/raftmux"
	"github.com/ankur-anand/unisondb/pkg/raftwalfs"
	"github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb/v2"
)

const modeRaft = "raft"

// raftApplier implements dbkernel.RaftApplier.
type raftApplier struct {
	cluster *raftcluster.Cluster
	timeout time.Duration
}

func (r *raftApplier) Apply(data []byte) (uint64, error) {
	future := r.cluster.Apply(data, r.timeout)
	if err := future.Error(); err != nil {
		return 0, err
	}
	if resp := future.Response(); resp != nil {
		if err, ok := resp.(error); ok {
			return 0, err
		}
	}
	return future.Index(), nil
}

// namespaceRaft holds all Raft resources for a single namespace.
type namespaceRaft struct {
	cluster     *raftcluster.Cluster
	logStore    *raftwalfs.LogStore
	stableStore *raftboltdb.BoltStore
	transport   raft.Transport
}

// RaftService manages Raft clusters for all namespaces.
type RaftService struct {
	clusters         map[string]*namespaceRaft
	transportManager *raftmux.RaftTransportManager
	deps             *Dependencies
	enabled          bool
}

func (r *RaftService) Name() string {
	return "raft"
}

func (r *RaftService) Setup(ctx context.Context, deps *Dependencies) error {
	r.deps = deps
	r.clusters = make(map[string]*namespaceRaft)

	cfg := deps.Config.RaftConfig
	if !cfg.Enabled {
		slog.Info("[unisondb.cliapp] Raft service disabled")
		r.enabled = false
		return nil
	}

	if cfg.NodeID == "" {
		return errors.New("raft: node_id is required")
	}
	if cfg.BindAddr == "" {
		return errors.New("raft: bind_addr is required")
	}

	r.enabled = true

	// Create shared transport manager for all namespaces
	r.transportManager = raftmux.NewRaftTransportManager(cfg.BindAddr)
	if err := r.transportManager.Start(); err != nil {
		return fmt.Errorf("start transport manager: %w", err)
	}

	// Set up Raft for each namespace
	for namespace, engine := range deps.Engines {
		nr, err := r.setupNamespaceRaft(ctx, namespace, engine, cfg)
		if err != nil {
			// Clean up any already-created clusters
			r.closeAll()
			return fmt.Errorf("raft setup for namespace %s: %w", namespace, err)
		}
		r.clusters[namespace] = nr

		slog.Info("[unisondb.cliapp]",
			slog.String("event_type", "raft.namespace.initialized"),
			slog.String("namespace", namespace),
			slog.String("node_id", cfg.NodeID),
			slog.String("bind_addr", cfg.BindAddr),
		)
	}

	return nil
}

func (r *RaftService) createTransport(namespace string) (raft.Transport, error) {
	streamLayer, err := r.transportManager.RegisterNamespace(namespace)
	if err != nil {
		return nil, fmt.Errorf("register namespace %s: %w", namespace, err)
	}
	return raft.NewNetworkTransport(streamLayer, 3, 10*time.Second, io.Discard), nil
}

func (r *RaftService) createSnapshotStore(namespace string, cfg config.RaftConfig) (raft.SnapshotStore, error) {
	snapshotDir := filepath.Join(r.deps.Config.Storage.BaseDir, namespace, "raft-snapshots")
	if err := os.MkdirAll(snapshotDir, 0755); err != nil {
		return nil, fmt.Errorf("create snapshot dir: %w", err)
	}

	snapshotRetain := cfg.SnapshotRetain
	if snapshotRetain <= 0 {
		snapshotRetain = 2
	}
	return raft.NewFileSnapshotStore(snapshotDir, snapshotRetain, io.Discard)
}

func (r *RaftService) createStableStore(namespace string) (*raftboltdb.BoltStore, error) {
	stableStorePath := filepath.Join(r.deps.Config.Storage.BaseDir, namespace, "raft-stable.db")
	return raftboltdb.NewBoltStore(stableStorePath)
}

func (r *RaftService) setupNamespaceRaft(
	_ context.Context,
	namespace string,
	engine *dbkernel.Engine,
	cfg config.RaftConfig,
) (*namespaceRaft, error) {
	wal := engine.WAL()
	if wal == nil {
		return nil, errors.New("engine WAL is nil - ensure engine is created with raft mode enabled")
	}

	codec := raftwalfs.BinaryCodecV1{DataMutator: raftwalfs.LogRecordMutator{}}
	logStore, err := raftwalfs.NewLogStore(wal, 0, raftwalfs.WithCodec(codec))
	if err != nil {
		return nil, fmt.Errorf("create log store: %w", err)
	}

	transport, err := r.createTransport(namespace)
	if err != nil {
		logStore.Close()
		return nil, fmt.Errorf("create transport: %w", err)
	}

	fileSnapshotStore, err := r.createSnapshotStore(namespace, cfg)
	if err != nil {
		logStore.Close()
		return nil, fmt.Errorf("create snapshot store: %w", err)
	}

	holder := engine.SnapshotIndexHolder()
	snapshotStore := dbkernel.NewFSMSnapshotStore(fileSnapshotStore, holder)
	safeLogStore := dbkernel.NewSafeGCLogStore(logStore, holder)

	stableStore, err := r.createStableStore(namespace)
	if err != nil {
		logStore.Close()
		return nil, fmt.Errorf("create stable store: %w", err)
	}

	raftConfig := r.buildRaftConfig(cfg)
	raftInstance, err := raft.NewRaft(raftConfig, engine, safeLogStore, stableStore, snapshotStore, transport)
	if err != nil {
		stableStore.Close()
		logStore.Close()
		return nil, fmt.Errorf("create raft: %w", err)
	}

	cluster := raftcluster.NewCluster(raftInstance)
	r.wireEngine(engine, cluster, logStore, cfg)

	if cfg.Bootstrap {
		if err := r.bootstrapCluster(raftInstance, cfg); err != nil {
			slog.Warn("[unisondb.cliapp] Bootstrap failed (may already be bootstrapped)",
				slog.String("namespace", namespace),
				slog.Any("error", err),
			)
		}
	}

	return &namespaceRaft{
		cluster:     cluster,
		logStore:    logStore,
		stableStore: stableStore,
		transport:   transport,
	}, nil
}

func (r *RaftService) wireEngine(
	engine *dbkernel.Engine,
	cluster *raftcluster.Cluster,
	logStore *raftwalfs.LogStore,
	cfg config.RaftConfig,
) {
	engine.SetRaftMode(true)
	engine.SetRaftApplier(&raftApplier{cluster: cluster, timeout: r.getApplyTimeout(cfg)})
	engine.SetPositionLookup(logStore.GetPosition)
	engine.SetWALCommitCallback(logStore.CommitPosition)
}

func (r *RaftService) buildRaftConfig(cfg config.RaftConfig) *raft.Config {
	rc := raft.DefaultConfig()
	rc.LocalID = raft.ServerID(cfg.NodeID)
	rc.LogOutput = io.Discard

	rc.BatchApplyCh = true
	rc.MaxAppendEntries = 256
	rc.TrailingLogs = 20480

	if cfg.HeartbeatTimeout != "" {
		if d, err := time.ParseDuration(cfg.HeartbeatTimeout); err == nil {
			rc.HeartbeatTimeout = d
		}
	}
	if cfg.ElectionTimeout != "" {
		if d, err := time.ParseDuration(cfg.ElectionTimeout); err == nil {
			rc.ElectionTimeout = d
		}
	}
	if cfg.CommitTimeout != "" {
		if d, err := time.ParseDuration(cfg.CommitTimeout); err == nil {
			rc.CommitTimeout = d
		}
	}

	if cfg.SnapshotThreshold > 0 {
		rc.SnapshotThreshold = cfg.SnapshotThreshold
	}
	if cfg.SnapshotInterval != "" {
		if d, err := time.ParseDuration(cfg.SnapshotInterval); err == nil {
			rc.SnapshotInterval = d
		}
	}

	return rc
}

func (r *RaftService) getApplyTimeout(cfg config.RaftConfig) time.Duration {
	if cfg.ApplyTimeout != "" {
		if d, err := time.ParseDuration(cfg.ApplyTimeout); err == nil {
			return d
		}
	}
	return 10 * time.Second
}

func (r *RaftService) bootstrapCluster(raftInstance *raft.Raft, cfg config.RaftConfig) error {
	servers := make([]raft.Server, 0, len(cfg.Peers)+1)

	servers = append(servers, raft.Server{
		ID:      raft.ServerID(cfg.NodeID),
		Address: raft.ServerAddress(cfg.BindAddr),
	})

	for _, peer := range cfg.Peers {
		servers = append(servers, raft.Server{
			ID:      raft.ServerID(peer.ID),
			Address: raft.ServerAddress(peer.Address),
		})
	}

	configuration := raft.Configuration{Servers: servers}
	return raftInstance.BootstrapCluster(configuration).Error()
}

func (r *RaftService) Run(ctx context.Context) error {
	if !r.enabled {
		return nil
	}

	slog.Info("[unisondb.cliapp]",
		slog.String("event_type", "raft.service.running"),
		slog.Int("cluster_count", len(r.clusters)),
	)

	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			r.logClusterStatus()
		}
	}
}

func (r *RaftService) logClusterStatus() {
	for namespace, nr := range r.clusters {
		stats := nr.cluster.Stats()
		slog.Info("[unisondb.cliapp]",
			slog.String("event_type", "raft.cluster.status"),
			slog.String("namespace", namespace),
			slog.String("state", stats.State.String()),
			slog.Bool("is_leader", stats.IsLeader),
			slog.String("leader", string(stats.Leader)),
			slog.Uint64("last_index", stats.LastIdx),
		)
	}
}

func (r *RaftService) Close(ctx context.Context) error {
	if !r.enabled {
		return nil
	}

	slog.Info("[unisondb.cliapp]",
		slog.String("event_type", "raft.service.stopping"),
	)

	r.closeAll()
	return nil
}

func (r *RaftService) closeAll() {
	for namespace, nr := range r.clusters {
		slog.Info("[unisondb.cliapp]",
			slog.String("event_type", "raft.cluster.closing"),
			slog.String("namespace", namespace),
		)

		if nr.cluster != nil {
			if err := nr.cluster.Close(); err != nil {
				slog.Error("[unisondb.cliapp] Failed to close Raft cluster",
					slog.String("namespace", namespace),
					slog.Any("error", err),
				)
			}
		}

		if nr.stableStore != nil {
			if err := nr.stableStore.Close(); err != nil {
				slog.Error("[unisondb.cliapp] Failed to close stable store",
					slog.String("namespace", namespace),
					slog.Any("error", err),
				)
			}
		}

		if nr.logStore != nil {
			if err := nr.logStore.Close(); err != nil {
				slog.Error("[unisondb.cliapp] Failed to close log store",
					slog.String("namespace", namespace),
					slog.Any("error", err),
				)
			}
		}
	}

	if r.transportManager != nil {
		if err := r.transportManager.Close(); err != nil {
			slog.Error("[unisondb.cliapp] Failed to close transport manager",
				slog.Any("error", err),
			)
		}
	}
}

func (r *RaftService) GetCluster(namespace string) *raftcluster.Cluster {
	if nr, ok := r.clusters[namespace]; ok {
		return nr.cluster
	}
	return nil
}

func (r *RaftService) IsLeader(namespace string) bool {
	if nr, ok := r.clusters[namespace]; ok {
		return nr.cluster.IsLeader()
	}
	return false
}

var _ Service = (*RaftService)(nil)
