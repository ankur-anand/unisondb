package cliapp

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/ankur-anand/unisondb/cmd/unisondb/config"
	"github.com/ankur-anand/unisondb/dbkernel"
	"github.com/ankur-anand/unisondb/pkg/raftcluster"
	"github.com/ankur-anand/unisondb/pkg/raftmux"
	"github.com/ankur-anand/unisondb/pkg/raftwalfs"
	"github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb/v2"
)

//  Startup Sequence
//
// 1. Create transport manager (yamux listener)
// 2. Setup Serf membership with tags (raft-addr, namespaces)
// 3. For each namespace:
//    a. Create log store (using engine's WAL)
//    b. Create transport (register namespace with yamux)
//    c. Create snapshot store (file-based)
//    d. Create stable store (BoltDB)
//    e. Create Raft instance with engine as FSM
//    f. Wire engine: SetRaftMode, SetRaftApplier, SetPositionLookup, SetWALCommitCallback
//    g. Bootstrap if configured
//    h. Register cluster with membership
// 4. Run: Start event handler, join Serf peers

// raftApplier implements dbkernel.RaftApplier.
type raftApplier struct {
	cluster *raftcluster.Cluster
	timeout time.Duration
}

func (r *raftApplier) Apply(data []byte) (uint64, error) {
	if !r.cluster.IsLeader() {
		leaderAddr, leaderID := r.cluster.LeaderWithID()
		return 0, &raftcluster.NotLeaderError{LeaderAddr: leaderAddr, LeaderID: leaderID}
	}
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

type raftStores struct {
	logStore      *raftwalfs.LogStore
	safeLogStore  *dbkernel.SafeGCLogStore
	stableStore   *raftboltdb.BoltStore
	snapshotStore raft.SnapshotStore
	transport     raft.Transport
}

// RaftService manages Raft clusters for all namespaces.
type RaftService struct {
	clusters         map[string]*namespaceRaft
	transportManager *raftmux.RaftTransportManager
	membership       *raftcluster.Membership
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
	if cfg.SerfBindAddr != "" && cfg.SerfBindPort != 0 && len(cfg.Peers) > 0 {
		return errors.New("raft: peers cannot be set when serf membership is enabled")
	}

	r.enabled = true

	// Create shared transport manager for all namespaces
	r.transportManager = raftmux.NewRaftTransportManager(cfg.BindAddr)
	if err := r.transportManager.Start(); err != nil {
		return fmt.Errorf("start transport manager: %w", err)
	}

	// Collect namespace names for Serf tags
	namespaces := make([]string, 0, len(deps.Engines))
	for namespace := range deps.Engines {
		namespaces = append(namespaces, namespace)
	}

	// Create Serf membership for node discovery
	if err := r.setupMembership(cfg, namespaces); err != nil {
		r.transportManager.Close()
		return fmt.Errorf("setup membership: %w", err)
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

		// Register cluster with membership for automatic node discovery
		if r.membership != nil {
			r.membership.RegisterCluster(nr.cluster)
		}

		slog.Info("[unisondb.cliapp]",
			slog.String("event_type", "raft.namespace.initialized"),
			slog.String("namespace", namespace),
			slog.String("node_id", cfg.NodeID),
			slog.String("bind_addr", cfg.BindAddr),
		)
	}

	return nil
}

func (r *RaftService) setupMembership(cfg config.RaftConfig, namespaces []string) error {
	if cfg.SerfBindAddr == "" || cfg.SerfBindPort == 0 {
		slog.Info("[unisondb.cliapp] Serf membership disabled (no serf_bind_addr/serf_bind_port)")
		return nil
	}

	// Tags for Serf membership:
	// - raft-addr: yamux transport manager address for all namespaces
	// - namespaces: comma-separated list of namespaces this node hosts
	tags := map[string]string{
		"raft-addr":  r.transportManager.Addr().String(),
		"namespaces": strings.Join(namespaces, ","),
	}

	membershipCfg := raftcluster.MembershipConfiguration{
		// this should be always Same as Raft ServerID
		NodeName:        cfg.NodeID,
		Tags:            tags,
		AdvertiseAddr:   cfg.SerfBindAddr,
		AdvertisePort:   cfg.SerfBindPort,
		SecretKeyBase64: cfg.SerfSecretKey,
	}

	membership, err := raftcluster.NewMembership(membershipCfg, slog.Default())
	if err != nil {
		return fmt.Errorf("create membership: %w", err)
	}

	r.membership = &membership

	slog.Info("[unisondb.cliapp]",
		slog.String("event_type", "raft.membership.initialized"),
		slog.String("node_id", cfg.NodeID),
		slog.String("serf_addr", fmt.Sprintf("%s:%d", cfg.SerfBindAddr, cfg.SerfBindPort)),
		slog.String("raft_addr", r.transportManager.Addr().String()),
	)

	return nil
}

func (r *RaftService) createTransport(namespace string) (raft.Transport, error) {
	streamLayer, err := r.transportManager.RegisterNamespace(namespace)
	if err != nil {
		return nil, fmt.Errorf("register namespace %s: %w", namespace, err)
	}
	return raft.NewNetworkTransport(streamLayer, 3, 10*time.Second, raftcluster.NewHashicorpLogWriter(slog.Default(), "[raft-transport]")), nil
}

// mostly using it in the error path.
func closeRaftTransport(transport raft.Transport) {
	if transport == nil {
		return
	}
	if closer, ok := transport.(raft.WithClose); ok {
		if err := closer.Close(); err != nil {
			slog.Warn("[unisondb.cliapp] failed to close raft transport", slog.Any("error", err))
		}
	}
}

func (r *RaftService) createRaftStores(namespace string, engine *dbkernel.Engine, cfg config.RaftConfig) (*raftStores, error) {
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
		closeRaftTransport(transport)
		return nil, fmt.Errorf("create snapshot store: %w", err)
	}

	holder := engine.SnapshotIndexHolder()
	snapshotStore := dbkernel.NewFSMSnapshotStore(fileSnapshotStore, holder)
	safeLogStore := dbkernel.NewSafeGCLogStore(logStore, holder)

	stableStore, err := r.createStableStore(namespace)
	if err != nil {
		logStore.Close()
		closeRaftTransport(transport)
		return nil, fmt.Errorf("create stable store: %w", err)
	}

	return &raftStores{
		logStore:      logStore,
		safeLogStore:  safeLogStore,
		stableStore:   stableStore,
		snapshotStore: snapshotStore,
		transport:     transport,
	}, nil
}

func closeRaftStores(stores *raftStores) {
	if stores == nil {
		return
	}
	if stores.stableStore != nil {
		_ = stores.stableStore.Close()
	}
	if stores.logStore != nil {
		_ = stores.logStore.Close()
	}
	closeRaftTransport(stores.transport)
}

func (r *RaftService) shouldBootstrap(cfg config.RaftConfig, stores *raftStores) (bool, error) {
	if !cfg.Bootstrap {
		return false, nil
	}

	hasState, err := raft.HasExistingState(stores.safeLogStore, stores.stableStore, stores.snapshotStore)
	if err != nil {
		return false, fmt.Errorf("check raft state: %w", err)
	}
	return !hasState, nil
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
	return raft.NewFileSnapshotStore(snapshotDir, snapshotRetain, raftcluster.NewHashicorpLogWriter(slog.Default(), "[raft-snapshot]"))
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
	stores, err := r.createRaftStores(namespace, engine, cfg)
	if err != nil {
		return nil, err
	}

	shouldBootstrap, err := r.shouldBootstrap(cfg, stores)
	if err != nil {
		closeRaftStores(stores)
		return nil, err
	}

	raftConfig := r.buildRaftConfig(cfg)
	raftInstance, err := raft.NewRaft(raftConfig, engine, stores.safeLogStore, stores.stableStore, stores.snapshotStore, stores.transport)
	if err != nil {
		closeRaftStores(stores)
		return nil, fmt.Errorf("create raft: %w", err)
	}

	cluster := raftcluster.NewCluster(raftInstance, raft.ServerID(cfg.NodeID), namespace)
	r.wireEngine(engine, cluster, stores.logStore, cfg)

	if shouldBootstrap {
		if err := r.bootstrapCluster(raftInstance, cfg); err != nil {
			slog.Warn("[unisondb.cliapp] Bootstrap failed (may already be bootstrapped)",
				slog.String("namespace", namespace),
				slog.Any("error", err),
			)
		}
	}

	return &namespaceRaft{
		cluster:     cluster,
		logStore:    stores.logStore,
		stableStore: stores.stableStore,
		transport:   stores.transport,
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
	rc.LogOutput = raftcluster.NewHashicorpLogWriter(slog.Default(), "[raft]")

	rc.BatchApplyCh = true
	rc.MaxAppendEntries = 256
	rc.TrailingLogs = 20480

	if cfg.SnapshotInterval == "" {
		rc.SnapshotInterval = 30 * time.Second
	}

	if cfg.SnapshotThreshold == 0 {
		rc.SnapshotThreshold = 16384
	}

	if cfg.HeartbeatTimeout != "" {
		if d, err := time.ParseDuration(cfg.HeartbeatTimeout); err == nil {
			rc.HeartbeatTimeout = d
		} else {
			slog.Warn("[unisondb.cliapp] invalid heartbeat_timeout",
				slog.String("value", cfg.HeartbeatTimeout),
				slog.Any("error", err))
		}
	}
	if cfg.ElectionTimeout != "" {
		if d, err := time.ParseDuration(cfg.ElectionTimeout); err == nil {
			rc.ElectionTimeout = d
		} else {
			slog.Warn("[unisondb.cliapp] invalid election_timeout",
				slog.String("value", cfg.ElectionTimeout),
				slog.Any("error", err))
		}
	}
	if cfg.CommitTimeout != "" {
		if d, err := time.ParseDuration(cfg.CommitTimeout); err == nil {
			rc.CommitTimeout = d
		} else {
			slog.Warn("[unisondb.cliapp] invalid commit_timeout",
				slog.String("value", cfg.CommitTimeout),
				slog.Any("error", err))
		}
	}

	if cfg.SnapshotThreshold > 0 {
		rc.SnapshotThreshold = cfg.SnapshotThreshold
	}

	if cfg.SnapshotInterval != "" {
		if d, err := time.ParseDuration(cfg.SnapshotInterval); err == nil {
			rc.SnapshotInterval = d
		} else {
			slog.Warn("[unisondb.cliapp] invalid snapshot_interval",
				slog.String("value", cfg.SnapshotInterval),
				slog.Any("error", err))
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

	if r.membership != nil {
		go func() {
			if err := r.membership.EventHandler(ctx.Done()); err != nil {
				slog.Error("[unisondb.cliapp] membership event handler error",
					slog.Any("error", err))
			}
		}()

		cfg := r.deps.Config.RaftConfig
		if len(cfg.SerfPeers) > 0 {
			n, err := r.membership.Join(cfg.SerfPeers)
			if err != nil {
				slog.Warn("[unisondb.cliapp] failed to join some serf peers",
					slog.Any("error", err),
					slog.Int("joined", n),
					slog.Int("total", len(cfg.SerfPeers)))
			} else {
				slog.Info("[unisondb.cliapp]",
					slog.String("event_type", "raft.membership.joined"),
					slog.Int("peers_joined", n))
			}
		}
	}

	slog.Info("[unisondb.cliapp]",
		slog.String("event_type", "raft.service.running"),
		slog.Int("cluster_count", len(r.clusters)),
	)

	<-ctx.Done()
	return nil
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
	waitForRemoval := make(map[string]bool, len(r.clusters))

	// We will follow this for raft shutdown and it's important we have this step.
	// 1. Prepare Raft clusters for shutdown (leadership transfer/self-removal).
	// 2. Close membership after initiating Raft leave, so peers observe the leave.
	// 3. Shut down Raft clusters (wait for removal if needed).
	// 4. Close transport manager last

	for namespace, nr := range r.clusters {
		slog.Info("[unisondb.cliapp]",
			slog.String("event_type", "raft.cluster.leaving"),
			slog.String("namespace", namespace),
		)

		if nr.cluster != nil {
			waitForRemoval[namespace] = nr.cluster.Leave()
		}
	}

	if r.membership != nil {
		slog.Info("[unisondb.cliapp]",
			slog.String("event_type", "raft.membership.closing"))
		if err := r.membership.Close(); err != nil {
			slog.Error("[unisondb.cliapp] Failed to close membership",
				slog.Any("error", err))
		}
	}

	for namespace, nr := range r.clusters {
		if nr.cluster != nil {
			if err := nr.cluster.Shutdown(waitForRemoval[namespace]); err != nil {
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
