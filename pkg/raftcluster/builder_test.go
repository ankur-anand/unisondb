package raftcluster

import (
	"fmt"
	"io"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/ankur-anand/unisondb/pkg/raftwalfs"
	"github.com/ankur-anand/unisondb/pkg/walfs"
	"github.com/hashicorp/raft"
)

type nodeConfig struct {
	ID      string
	Address string
}

type clusterBuilder struct {
	nodeID    string
	namespace string
	dataDir   string
	nodes     []nodeConfig

	raftConfig         *raft.Config
	wal                *walfs.WALog
	codec              raftwalfs.Codec
	transportTimeout   time.Duration
	snapshotRetain     int
	snapshotStoreFunc  func() (raft.SnapshotStore, error)
	fsmFactory         func() raft.FSM
	stableStoreFactory func() (raft.StableStore, error)
}

func newClusterBuilder(nodeID, namespace, dataDir string, nodes []nodeConfig) *clusterBuilder {
	return &clusterBuilder{
		nodeID:           nodeID,
		namespace:        namespace,
		dataDir:          dataDir,
		nodes:            nodes,
		transportTimeout: 10 * time.Second,
		snapshotRetain:   2,
	}
}

func (b *clusterBuilder) WithWAL(wal *walfs.WALog) *clusterBuilder {
	b.wal = wal
	return b
}

func (b *clusterBuilder) WithFSMFactory(factory func() raft.FSM) *clusterBuilder {
	b.fsmFactory = factory
	return b
}

type builtCluster struct {
	Cluster   *Cluster
	LogStore  *raftwalfs.LogStore
	WAL       *walfs.WALog
	Transport *raft.NetworkTransport

	cleanupOnce sync.Once
}

func (bc *builtCluster) Close() error {
	var errs []error

	bc.cleanupOnce.Do(func() {
		if bc.Cluster != nil {
			if err := bc.Cluster.Close(); err != nil {
				errs = append(errs, err)
			}
		}

		if bc.Transport != nil {
			if err := bc.Transport.Close(); err != nil {
				errs = append(errs, err)
			}
		}

		if bc.LogStore != nil {
			if err := bc.LogStore.Close(); err != nil {
				errs = append(errs, err)
			}
		}
	})

	if len(errs) > 0 {
		return fmt.Errorf("cleanup errors: %v", errs)
	}
	return nil
}

func (b *clusterBuilder) build() (*builtCluster, error) {
	result := &builtCluster{}

	var ourNode nodeConfig
	for _, n := range b.nodes {
		if n.ID == b.nodeID {
			ourNode = n
			break
		}
	}
	if ourNode.ID == "" {
		return nil, fmt.Errorf("node %s not found in nodes list", b.nodeID)
	}

	// build the Raft instance
	logStore, wal, transport, r, err := b.buildRaft(ourNode.Address)
	if err != nil {
		return nil, fmt.Errorf("build raft: %w", err)
	}

	result.LogStore = logStore
	result.WAL = wal
	result.Transport = transport
	result.Cluster = NewCluster(r, raft.ServerID(b.nodeID), b.namespace)

	return result, nil
}

func (b *clusterBuilder) buildRaft(bindAddr string) (*raftwalfs.LogStore, *walfs.WALog, *raft.NetworkTransport, *raft.Raft, error) {
	wal, logStore, err := b.createLogStore()
	if err != nil {
		return nil, nil, nil, nil, err
	}

	transport, err := b.createTransport(bindAddr)
	if err != nil {
		logStore.Close()
		return nil, nil, nil, nil, err
	}

	stableStore, snapshotStore, err := b.createStores()
	if err != nil {
		transport.Close()
		logStore.Close()
		return nil, nil, nil, nil, err
	}

	r, err := raft.NewRaft(b.getRaftConfig(), b.getFSM(), logStore, stableStore, snapshotStore, transport)
	if err != nil {
		transport.Close()
		logStore.Close()
		return nil, nil, nil, nil, fmt.Errorf("create raft: %w", err)
	}

	return logStore, wal, transport, r, nil
}

func (b *clusterBuilder) createLogStore() (*walfs.WALog, *raftwalfs.LogStore, error) {
	if b.wal == nil {
		return nil, nil, fmt.Errorf("WAL is required - use WithWAL() to provide engine-initialized WAL")
	}

	codec := b.codec
	if codec == nil {
		codec = raftwalfs.BinaryCodecV1{DataMutator: raftwalfs.LogRecordMutator{}}
	}

	logStore, err := raftwalfs.NewLogStore(b.wal, 0, raftwalfs.WithCodec(codec))
	if err != nil {
		return nil, nil, fmt.Errorf("create log store: %w", err)
	}

	return b.wal, logStore, nil
}

func (b *clusterBuilder) createTransport(bindAddr string) (*raft.NetworkTransport, error) {
	addr, err := net.ResolveTCPAddr("tcp", bindAddr)
	if err != nil {
		return nil, fmt.Errorf("resolve bind address: %w", err)
	}

	transport, err := raft.NewTCPTransport(bindAddr, addr, 3, b.transportTimeout, &lwr{logger: slog.Default().With(slog.String("source", "raft-transport"))})
	if err != nil {
		return nil, fmt.Errorf("create transport: %w", err)
	}

	return transport, nil
}

func (b *clusterBuilder) createStores() (raft.StableStore, raft.SnapshotStore, error) {
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

func (b *clusterBuilder) createSnapshotStore() (raft.SnapshotStore, error) {
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

	store, err := raft.NewFileSnapshotStore(snapshotDir, b.snapshotRetain, &lwr{logger: slog.Default().With(slog.String("source", "raft-snapshot"))})
	if err != nil {
		return nil, fmt.Errorf("create snapshot store: %w", err)
	}

	return store, nil
}

func (b *clusterBuilder) getRaftConfig() *raft.Config {
	cfg := b.raftConfig
	if cfg == nil {
		cfg = raft.DefaultConfig()
		cfg.LogOutput = &lwr{logger: slog.Default().With(slog.String("source", "raft"))}
	}
	cfg.LocalID = raft.ServerID(b.nodeID)
	return cfg
}

func (b *clusterBuilder) getFSM() raft.FSM {
	if b.fsmFactory != nil {
		return b.fsmFactory()
	}
	return &noopFSM{}
}

// Bootstrap bootstraps the cluster. Should only be called on one node.
func (bc *builtCluster) Bootstrap(nodes []nodeConfig) error {
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
func (bc *builtCluster) WaitForLeader(timeout time.Duration) error {
	deadline := time.Now().Add(timeout)

	for time.Now().Before(deadline) {
		leaderAddr, _ := bc.Cluster.raft.LeaderWithID()
		if leaderAddr != "" {
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
