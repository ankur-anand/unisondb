package cliapp

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"sync"
	"testing"
	"time"

	"github.com/ankur-anand/unisondb/cmd/unisondb/config"
	"github.com/ankur-anand/unisondb/dbkernel"
	"github.com/ankur-anand/unisondb/pkg/raftcluster"
	"github.com/hashicorp/go-hclog"
	"github.com/hashicorp/raft"
	"github.com/stretchr/testify/require"
)

func TestBlobStoreStreamerService_StandaloneStreamsAndResumes(t *testing.T) {
	engineDir := t.TempDir()
	namespace := "orders"
	engine, err := dbkernel.NewStorageEngine(engineDir, namespace, dbkernel.NewDefaultEngineConfig())
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, engine.Close(context.Background()))
	})

	for i := 0; i < 8; i++ {
		err = engine.PutKV([]byte(fmt.Sprintf("key-%02d", i)), []byte(fmt.Sprintf("value-%02d", i)))
		require.NoError(t, err)
	}
	initialLSN := engine.OpsReceivedCount()

	bucketDir := t.TempDir()
	bucketURL := (&url.URL{Scheme: "file", Path: bucketDir}).String()

	svc := &BlobStoreStreamerService{}
	deps := &Dependencies{
		Mode: "server",
		Config: config.Config{
			BlobStoreStreaming: config.BlobStoreStreamingConfig{
				Enabled:       true,
				FlushInterval: "50ms",
				Namespaces: map[string]config.BlobStoreWriteNSConfig{
					namespace: {
						BucketURL:  bucketURL,
						BasePrefix: "unisondb",
					},
				},
			},
		},
		Engines: map[string]*dbkernel.Engine{
			namespace: engine,
		},
	}

	require.NoError(t, svc.Setup(context.Background(), deps))
	t.Cleanup(func() {
		require.NoError(t, svc.Close(context.Background()))
	})

	runOnce := func(ctx context.Context) error {
		var (
			runErr error
			wg     sync.WaitGroup
		)
		wg.Add(1)
		go func() {
			defer wg.Done()
			runErr = svc.Run(ctx)
		}()

		<-ctx.Done()
		wg.Wait()
		return runErr
	}

	runCtx1, cancel1 := context.WithCancel(context.Background())
	done1 := make(chan error, 1)
	go func() {
		done1 <- runOnce(runCtx1)
	}()

	require.Eventually(t, func() bool {
		lsn, err := svc.lastWrittenLSN(context.Background(), namespace)
		return err == nil && lsn == initialLSN
	}, 5*time.Second, 50*time.Millisecond)

	cancel1()
	require.NoError(t, <-done1)

	for i := 8; i < 12; i++ {
		err = engine.PutKV([]byte(fmt.Sprintf("key-%02d", i)), []byte(fmt.Sprintf("value-%02d", i)))
		require.NoError(t, err)
	}
	finalLSN := engine.OpsReceivedCount()

	lsnBeforeRestart, err := svc.lastWrittenLSN(context.Background(), namespace)
	require.NoError(t, err)
	require.Equal(t, initialLSN, lsnBeforeRestart)

	runCtx2, cancel2 := context.WithCancel(context.Background())
	done2 := make(chan error, 1)
	go func() {
		done2 <- runOnce(runCtx2)
	}()

	require.Eventually(t, func() bool {
		lsn, err := svc.lastWrittenLSN(context.Background(), namespace)
		return err == nil && lsn == finalLSN
	}, 5*time.Second, 50*time.Millisecond)

	cancel2()
	require.NoError(t, <-done2)
}

func TestBlobStoreStreamerService_RaftLeadershipChangeResumesFromCurrent(t *testing.T) {
	const namespace = "orders"

	engineDir := t.TempDir()
	engine, err := dbkernel.NewStorageEngine(engineDir, namespace, dbkernel.NewDefaultEngineConfig())
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, engine.Close(context.Background()))
	})

	for i := 0; i < 6; i++ {
		err = engine.PutKV([]byte(fmt.Sprintf("key-%02d", i)), []byte(fmt.Sprintf("value-%02d", i)))
		require.NoError(t, err)
	}
	initialLSN := engine.OpsReceivedCount()

	cluster1, cluster2, cleanupRaft := newLeadershipTestClusters(t, namespace)
	t.Cleanup(cleanupRaft)
	require.Eventually(t, func() bool { return cluster1.IsLeader() }, 5*time.Second, 20*time.Millisecond)

	bucketDir := t.TempDir()
	bucketURL := (&url.URL{Scheme: "file", Path: bucketDir}).String()

	svc := &BlobStoreStreamerService{}
	deps := &Dependencies{
		Mode: "server",
		Config: config.Config{
			RaftConfig: config.RaftConfig{Enabled: true},
			BlobStoreStreaming: config.BlobStoreStreamingConfig{
				Enabled:       true,
				FlushInterval: "50ms",
				Namespaces: map[string]config.BlobStoreWriteNSConfig{
					namespace: {
						BucketURL:  bucketURL,
						BasePrefix: "unisondb",
					},
				},
			},
		},
		Engines: map[string]*dbkernel.Engine{
			namespace: engine,
		},
		RaftService: &RaftService{
			enabled: true,
			clusters: map[string]*namespaceRaft{
				namespace: {cluster: cluster1},
			},
		},
	}

	require.NoError(t, svc.Setup(context.Background(), deps))
	t.Cleanup(func() {
		require.NoError(t, svc.Close(context.Background()))
	})

	runCtx, cancelRun := context.WithCancel(context.Background())
	runDone := make(chan error, 1)
	go func() {
		runDone <- svc.Run(runCtx)
	}()

	require.Eventually(t, func() bool {
		lsn, err := svc.lastWrittenLSN(context.Background(), namespace)
		return err == nil && lsn == initialLSN
	}, 5*time.Second, 50*time.Millisecond)

	require.NoError(t, cluster1.Raft().LeadershipTransfer().Error())
	require.Eventually(t, func() bool {
		return !cluster1.IsLeader() && cluster2.IsLeader()
	}, 5*time.Second, 20*time.Millisecond)

	for i := 6; i < 10; i++ {
		err = engine.PutKV([]byte(fmt.Sprintf("key-%02d", i)), []byte(fmt.Sprintf("value-%02d", i)))
		require.NoError(t, err)
	}
	finalLSN := engine.OpsReceivedCount()

	time.Sleep(300 * time.Millisecond)
	lsnWhileFollower, err := svc.lastWrittenLSN(context.Background(), namespace)
	require.NoError(t, err)
	require.Equal(t, initialLSN, lsnWhileFollower)

	require.NoError(t, cluster2.Raft().LeadershipTransfer().Error())
	require.Eventually(t, func() bool {
		return cluster1.IsLeader() && !cluster2.IsLeader()
	}, 5*time.Second, 20*time.Millisecond)

	require.Eventually(t, func() bool {
		lsn, err := svc.lastWrittenLSN(context.Background(), namespace)
		return err == nil && lsn == finalLSN
	}, 5*time.Second, 50*time.Millisecond)

	cancelRun()
	runErr := <-runDone
	require.True(t, runErr == nil || errors.Is(runErr, context.Canceled), "unexpected run error: %v", runErr)
}

func newLeadershipTestClusters(t *testing.T, namespace string) (*raftcluster.Cluster, *raftcluster.Cluster, func()) {
	t.Helper()

	node1ID := raft.ServerID("node1-" + namespace)
	node2ID := raft.ServerID("node2-" + namespace)

	cfg1 := raft.DefaultConfig()
	cfg1.LocalID = node1ID
	cfg1.Logger = hclog.NewNullLogger()
	cfg1.HeartbeatTimeout = 100 * time.Millisecond
	cfg1.ElectionTimeout = 100 * time.Millisecond
	cfg1.CommitTimeout = 20 * time.Millisecond
	cfg1.LeaderLeaseTimeout = 100 * time.Millisecond

	cfg2 := raft.DefaultConfig()
	cfg2.LocalID = node2ID
	cfg2.Logger = hclog.NewNullLogger()
	cfg2.HeartbeatTimeout = 100 * time.Millisecond
	cfg2.ElectionTimeout = 100 * time.Millisecond
	cfg2.CommitTimeout = 20 * time.Millisecond
	cfg2.LeaderLeaseTimeout = 100 * time.Millisecond

	logStore1 := raft.NewInmemStore()
	stableStore1 := raft.NewInmemStore()
	snapStore1 := raft.NewInmemSnapshotStore()
	addr1, transport1 := raft.NewInmemTransport(raft.ServerAddress(node1ID))

	logStore2 := raft.NewInmemStore()
	stableStore2 := raft.NewInmemStore()
	snapStore2 := raft.NewInmemSnapshotStore()
	addr2, transport2 := raft.NewInmemTransport(raft.ServerAddress(node2ID))

	transport1.Connect(addr2, transport2)
	transport2.Connect(addr1, transport1)

	raft1, err := raft.NewRaft(cfg1, &raft.MockFSM{}, logStore1, stableStore1, snapStore1, transport1)
	require.NoError(t, err)
	raft2, err := raft.NewRaft(cfg2, &raft.MockFSM{}, logStore2, stableStore2, snapStore2, transport2)
	require.NoError(t, err)

	bootstrap := raft1.BootstrapCluster(raft.Configuration{
		Servers: []raft.Server{
			{ID: node1ID, Address: addr1, Suffrage: raft.Voter},
			{ID: node2ID, Address: addr2, Suffrage: raft.Voter},
		},
	})
	require.NoError(t, bootstrap.Error())

	cluster1 := raftcluster.NewCluster(raft1, node1ID, namespace)
	cluster2 := raftcluster.NewCluster(raft2, node2ID, namespace)

	cleanup := func() {
		_ = cluster1.Shutdown(false)
		_ = cluster2.Shutdown(false)
		_ = transport1.Close()
		_ = transport2.Close()
	}

	return cluster1, cluster2, cleanup
}
