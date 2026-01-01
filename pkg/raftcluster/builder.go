package raftcluster

import (
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/ankur-anand/unisondb/pkg/raftwalfs"
	"github.com/ankur-anand/unisondb/pkg/walfs"
	"github.com/hashicorp/raft"
)

// NodeConfig represents a node in the Raft cluster.
type NodeConfig struct {
	ID      string
	Address string
}

// ClusterBuilder builds a Raft cluster.
type ClusterBuilder struct {
	nodeID  string
	dataDir string
	nodes   []NodeConfig

	raftConfig         *raft.Config
	walSegmentSize     int64
	msyncEveryWrite    bool
	codec              raftwalfs.Codec
	transportTimeout   time.Duration
	snapshotRetain     int
	snapshotStoreFunc  func() (raft.SnapshotStore, error)
	fsmFactory         func() raft.FSM
	stableStoreFactory func() (raft.StableStore, error)
}

// NewClusterBuilder creates a new builder.
func NewClusterBuilder(nodeID, dataDir string, nodes []NodeConfig) *ClusterBuilder {
	return &ClusterBuilder{
		nodeID:           nodeID,
		dataDir:          dataDir,
		nodes:            nodes,
		walSegmentSize:   64 * 1024 * 1024,
		transportTimeout: 10 * time.Second,
		snapshotRetain:   2,
	}
}

// WithRaftConfig sets custom Raft configuration.
func (b *ClusterBuilder) WithRaftConfig(cfg *raft.Config) *ClusterBuilder {
	b.raftConfig = cfg
	return b
}

// WithWALSegmentSize sets the WAL segment size.
func (b *ClusterBuilder) WithWALSegmentSize(size int64) *ClusterBuilder {
	b.walSegmentSize = size
	return b
}

// WithMSyncEveryWrite enables msync on every write for durability.
func (b *ClusterBuilder) WithMSyncEveryWrite(msync bool) *ClusterBuilder {
	b.msyncEveryWrite = msync
	return b
}

// WithCodec sets the WAL codec.
func (b *ClusterBuilder) WithCodec(codec raftwalfs.Codec) *ClusterBuilder {
	b.codec = codec
	return b
}

// WithTransportTimeout sets the transport timeout.
func (b *ClusterBuilder) WithTransportTimeout(timeout time.Duration) *ClusterBuilder {
	b.transportTimeout = timeout
	return b
}

// WithFSMFactory sets the FSM factory function.
func (b *ClusterBuilder) WithFSMFactory(factory func() raft.FSM) *ClusterBuilder {
	b.fsmFactory = factory
	return b
}

// WithSnapshotStore sets a custom snapshot store factory.
func (b *ClusterBuilder) WithSnapshotStore(factory func() (raft.SnapshotStore, error)) *ClusterBuilder {
	b.snapshotStoreFunc = factory
	return b
}

// WithStableStore sets a custom stable store factory.
func (b *ClusterBuilder) WithStableStore(factory func() (raft.StableStore, error)) *ClusterBuilder {
	b.stableStoreFactory = factory
	return b
}

// BuiltCluster contains all resources created during cluster building.
type BuiltCluster struct {
	Cluster   *Cluster
	LogStore  *raftwalfs.LogStore
	WAL       *walfs.WALog
	Transport *raft.NetworkTransport

	cleanupOnce sync.Once
}

// Close cleans up all cluster resources.
func (bc *BuiltCluster) Close() error {
	var errs []error

	bc.cleanupOnce.Do(func() {
		if bc.Cluster != nil {
			if err := bc.Cluster.Close(); err != nil {
				errs = append(errs, err)
			}
		}

		// Close transport
		if bc.Transport != nil {
			if err := bc.Transport.Close(); err != nil {
				errs = append(errs, err)
			}
		}

		// Close log store
		if bc.LogStore != nil {
			if err := bc.LogStore.Close(); err != nil {
				errs = append(errs, err)
			}
		}

		// Close WAL
		if bc.WAL != nil {
			if err := bc.WAL.Close(); err != nil {
				errs = append(errs, err)
			}
		}
	})

	if len(errs) > 0 {
		return fmt.Errorf("cleanup errors: %v", errs)
	}
	return nil
}

// Build creates the cluster. Returns BuiltCluster with all resources.
func (b *ClusterBuilder) Build() (*BuiltCluster, error) {
	result := &BuiltCluster{}

	var ourNode NodeConfig
	for _, n := range b.nodes {
		if n.ID == b.nodeID {
			ourNode = n
			break
		}
	}
	if ourNode.ID == "" {
		return nil, fmt.Errorf("node %s not found in nodes list", b.nodeID)
	}

	// Build the Raft instance
	logStore, wal, transport, r, err := b.buildRaft(ourNode.Address)
	if err != nil {
		return nil, fmt.Errorf("build raft: %w", err)
	}

	result.LogStore = logStore
	result.WAL = wal
	result.Transport = transport
	result.Cluster = NewCluster(r)

	return result, nil
}

func (b *ClusterBuilder) buildRaft(bindAddr string) (*raftwalfs.LogStore, *walfs.WALog, *raft.NetworkTransport, *raft.Raft, error) {
	wal, logStore, err := b.createLogStore()
	if err != nil {
		return nil, nil, nil, nil, err
	}

	transport, err := b.createTransport(bindAddr)
	if err != nil {
		logStore.Close()
		wal.Close()
		return nil, nil, nil, nil, err
	}

	stableStore, snapshotStore, err := b.createStores()
	if err != nil {
		transport.Close()
		logStore.Close()
		wal.Close()
		return nil, nil, nil, nil, err
	}

	r, err := raft.NewRaft(b.getRaftConfig(), b.getFSM(), logStore, stableStore, snapshotStore, transport)
	if err != nil {
		transport.Close()
		logStore.Close()
		wal.Close()
		return nil, nil, nil, nil, fmt.Errorf("create raft: %w", err)
	}

	return logStore, wal, transport, r, nil
}

func (b *ClusterBuilder) createLogStore() (*walfs.WALog, *raftwalfs.LogStore, error) {
	walDir := filepath.Join(b.dataDir, "wal")
	if err := os.MkdirAll(walDir, 0755); err != nil {
		return nil, nil, fmt.Errorf("create wal dir: %w", err)
	}

	wal, err := walfs.NewWALog(walDir, ".wal",
		walfs.WithMaxSegmentSize(b.walSegmentSize),
		walfs.WithMSyncEveryWrite(b.msyncEveryWrite),
		walfs.WithReaderCommitCheck(),
	)
	if err != nil {
		return nil, nil, fmt.Errorf("create wal: %w", err)
	}

	codec := b.codec
	if codec == nil {
		codec = raftwalfs.BinaryCodecV1{DataMutator: raftwalfs.LogRecordMutator{}}
	}

	logStore, err := raftwalfs.NewLogStore(wal, 0, raftwalfs.WithCodec(codec))
	if err != nil {
		wal.Close()
		return nil, nil, fmt.Errorf("create log store: %w", err)
	}

	return wal, logStore, nil
}

func (b *ClusterBuilder) createTransport(bindAddr string) (*raft.NetworkTransport, error) {
	addr, err := net.ResolveTCPAddr("tcp", bindAddr)
	if err != nil {
		return nil, fmt.Errorf("resolve bind address: %w", err)
	}

	transport, err := raft.NewTCPTransport(bindAddr, addr, 3, b.transportTimeout, io.Discard)
	if err != nil {
		return nil, fmt.Errorf("create transport: %w", err)
	}

	return transport, nil
}

func (b *ClusterBuilder) createStores() (raft.StableStore, raft.SnapshotStore, error) {
	var stableStore raft.StableStore
	if b.stableStoreFactory != nil {
		var err error
		stableStore, err = b.stableStoreFactory()
		if err != nil {
			return nil, nil, fmt.Errorf("create stable store: %w", err)
		}
	} else {
		stableStore = raft.NewInmemStore()
	}

	snapshotStore, err := b.createSnapshotStore()
	if err != nil {
		return nil, nil, err
	}

	return stableStore, snapshotStore, nil
}

func (b *ClusterBuilder) createSnapshotStore() (raft.SnapshotStore, error) {
	if b.snapshotStoreFunc != nil {
		store, err := b.snapshotStoreFunc()
		if err != nil {
			return nil, fmt.Errorf("create snapshot store: %w", err)
		}
		return store, nil
	}

	snapshotDir := filepath.Join(b.dataDir, "snapshots")
	if err := os.MkdirAll(snapshotDir, 0755); err != nil {
		return nil, fmt.Errorf("create snapshot dir: %w", err)
	}

	store, err := raft.NewFileSnapshotStore(snapshotDir, b.snapshotRetain, io.Discard)
	if err != nil {
		return nil, fmt.Errorf("create snapshot store: %w", err)
	}

	return store, nil
}

func (b *ClusterBuilder) getRaftConfig() *raft.Config {
	cfg := b.raftConfig
	if cfg == nil {
		cfg = raft.DefaultConfig()
		cfg.HeartbeatTimeout = 150 * time.Millisecond
		cfg.ElectionTimeout = 150 * time.Millisecond
		cfg.LeaderLeaseTimeout = 100 * time.Millisecond
		cfg.CommitTimeout = 10 * time.Millisecond
		cfg.LogOutput = io.Discard
	}
	cfg.LocalID = raft.ServerID(b.nodeID)
	return cfg
}

func (b *ClusterBuilder) getFSM() raft.FSM {
	if b.fsmFactory != nil {
		return b.fsmFactory()
	}
	return &noopFSM{}
}

// Bootstrap bootstraps the cluster. Should only be called on one node.
func (bc *BuiltCluster) Bootstrap(nodes []NodeConfig) error {
	servers := make([]raft.Server, len(nodes))
	for i, n := range nodes {
		servers[i] = raft.Server{
			ID:      raft.ServerID(n.ID),
			Address: raft.ServerAddress(n.Address),
		}
	}

	configuration := raft.Configuration{Servers: servers}
	return bc.Cluster.raft.BootstrapCluster(configuration).Error()
}

// WaitForLeader waits until the cluster has a leader.
func (bc *BuiltCluster) WaitForLeader(timeout time.Duration) error {
	deadline := time.Now().Add(timeout)

	for time.Now().Before(deadline) {
		if bc.Cluster.raft.Leader() != "" {
			return nil
		}
		time.Sleep(50 * time.Millisecond)
	}

	return fmt.Errorf("no leader after %v", timeout)
}

type noopFSM struct{}

func (f *noopFSM) Apply(log *raft.Log) interface{} { return nil }
func (f *noopFSM) Snapshot() (raft.FSMSnapshot, error) {
	return &noopSnapshot{}, nil
}
func (f *noopFSM) Restore(rc io.ReadCloser) error { return rc.Close() }

type noopSnapshot struct{}

func (s *noopSnapshot) Persist(sink raft.SnapshotSink) error { return sink.Close() }
func (s *noopSnapshot) Release()                             {}
