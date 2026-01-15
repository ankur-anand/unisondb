package raftcluster

import (
	"fmt"
	"io"
	"net"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ankur-anand/unisondb/internal/logcodec"
	"github.com/ankur-anand/unisondb/pkg/walfs"
	"github.com/ankur-anand/unisondb/schemas/logrecord"
	"github.com/hashicorp/raft"
	"github.com/stretchr/testify/require"
)

// countingFSM counts applied logs for verification.
type countingFSM struct {
	count atomic.Uint64
}

func (f *countingFSM) Apply(log *raft.Log) interface{} {
	f.count.Add(1)
	return nil
}

func (f *countingFSM) Snapshot() (raft.FSMSnapshot, error) {
	return &noopSnapshot{}, nil
}

func (f *countingFSM) Restore(rc io.ReadCloser) error {
	return rc.Close()
}

func (f *countingFSM) Count() uint64 {
	return f.count.Load()
}

func createTestPayload(key, value []byte) []byte {
	kvEntry := logcodec.SerializeKVEntry(key, value)
	record := &logcodec.LogRecord{
		LSN:           1,
		HLC:           uint64(time.Now().UnixNano()),
		OperationType: logrecord.LogOperationTypeInsert,
		TxnState:      logrecord.TransactionStateNone,
		EntryType:     logrecord.LogEntryTypeKV,
		Entries:       [][]byte{kvEntry},
	}
	return record.FBEncode(256 + len(key) + len(value))
}

func allocatePorts(tb testing.TB, count int) []int {
	tb.Helper()

	listeners := make([]net.Listener, 0, count)
	ports := make([]int, 0, count)
	for i := 0; i < count; i++ {
		listener, err := net.Listen("tcp", "127.0.0.1:0")
		require.NoError(tb, err)
		listeners = append(listeners, listener)
		ports = append(ports, listener.Addr().(*net.TCPAddr).Port)
	}

	for _, listener := range listeners {
		_ = listener.Close()
	}

	return ports
}

func isAddrInUse(err error) bool {
	if err == nil {
		return false
	}
	return strings.Contains(err.Error(), "address already in use")
}

func setupTestCluster(t *testing.T) ([]*builtCluster, []*countingFSM) {
	t.Helper()

	const maxAttempts = 5
	var lastErr error

	for attempt := 0; attempt < maxAttempts; attempt++ {
		ports := allocatePorts(t, 3)
		nodes := []nodeConfig{
			{ID: "node0", Address: fmt.Sprintf("127.0.0.1:%d", ports[0])},
			{ID: "node1", Address: fmt.Sprintf("127.0.0.1:%d", ports[1])},
			{ID: "node2", Address: fmt.Sprintf("127.0.0.1:%d", ports[2])},
		}

		clusters := make([]*builtCluster, 3)
		fsms := make([]*countingFSM, 3)
		builtCount := 0

		for i := 0; i < 3; i++ {
			fsm := &countingFSM{}
			fsms[i] = fsm

			dir := t.TempDir()
			walDir := filepath.Join(dir, "wal")
			wal, err := walfs.NewWALog(walDir, ".wal", walfs.WithReaderCommitCheck())
			if err != nil {
				lastErr = err
				break
			}
			builder := newClusterBuilder(nodes[i].ID, "test", dir, nodes).
				WithWAL(wal).
				WithFSMFactory(func() raft.FSM { return fsm })

			built, err := builder.build()
			if err != nil {
				wal.Close()
				lastErr = err
				break
			}
			clusters[i] = built
			builtCount++
		}

		if builtCount < 3 {
			for j := 0; j < builtCount; j++ {
				clusters[j].Close()
			}
			if isAddrInUse(lastErr) && attempt < maxAttempts-1 {
				continue
			}
			t.Fatalf("failed to build cluster: %v", lastErr)
		}

		if err := clusters[0].Bootstrap(nodes); err != nil {
			for _, c := range clusters {
				c.Close()
			}
			t.Fatalf("failed to bootstrap: %v", err)
		}

		if err := clusters[0].WaitForLeader(10 * time.Second); err != nil {
			for _, c := range clusters {
				c.Close()
			}
			t.Fatalf("failed to elect leader: %v", err)
		}

		time.Sleep(500 * time.Millisecond)
		return clusters, fsms
	}

	t.Fatalf("failed to build cluster after %d attempts: %v", maxAttempts, lastErr)
	return nil, nil
}

func findLeader(clusters []*builtCluster) *Cluster {
	for _, c := range clusters {
		if c.Cluster.IsLeader() {
			return c.Cluster
		}
	}
	return nil
}

func membersFromConfig(cfg *raft.Configuration, namespace string) []MemberInformation {
	members := make([]MemberInformation, 0, len(cfg.Servers))
	for _, server := range cfg.Servers {
		members = append(members, MemberInformation{
			NodeName: string(server.ID),
			Tags: map[string]string{
				"raft-addr":  string(server.Address),
				"namespaces": namespace,
			},
			Status: MemberStatusAlive,
		})
	}
	return members
}

func TestCluster_SingleApply(t *testing.T) {
	clusters, fsms := setupTestCluster(t)
	defer func() {
		for _, c := range clusters {
			c.Close()
		}
	}()

	leader := findLeader(clusters)
	if leader == nil {
		t.Fatal("no leader found")
	}

	payload := createTestPayload([]byte("key1"), []byte("value1"))
	future := leader.Apply(payload, 5*time.Second)
	if err := future.Error(); err != nil {
		t.Fatalf("apply failed: %v", err)
	}

	time.Sleep(100 * time.Millisecond)

	for i, fsm := range fsms {
		if fsm.Count() < 1 {
			t.Errorf("node %d: expected count >= 1, got %d", i, fsm.Count())
		}
	}
}

func TestCluster_BatchApply(t *testing.T) {
	clusters, fsms := setupTestCluster(t)
	defer func() {
		for _, c := range clusters {
			c.Close()
		}
	}()

	leader := findLeader(clusters)
	if leader == nil {
		t.Fatal("no leader found")
	}

	payloads := make([][]byte, 100)
	for i := range payloads {
		payloads[i] = createTestPayload(
			[]byte(fmt.Sprintf("key-%d", i)),
			[]byte(fmt.Sprintf("value-%d", i)),
		)
	}

	errs := leader.ApplyBatch(payloads, 5*time.Second)
	for i, err := range errs {
		if err != nil {
			t.Errorf("batch item %d failed: %v", i, err)
		}
	}

	time.Sleep(200 * time.Millisecond)

	for i, fsm := range fsms {
		if fsm.Count() < 100 {
			t.Errorf("node %d: expected count >= 100, got %d", i, fsm.Count())
		}
	}
}

func TestCluster_NumVoters(t *testing.T) {
	clusters, _ := setupTestCluster(t)
	defer func() {
		for _, c := range clusters {
			c.Close()
		}
	}()

	for i, c := range clusters {
		count, err := c.Cluster.NumVoters()
		if err != nil {
			t.Fatalf("node %d NumVoters error: %v", i, err)
		}
		if count != len(clusters) {
			t.Fatalf("node %d expected %d voters, got %d", i, len(clusters), count)
		}
	}
}

func TestCluster_RaftConfiguration(t *testing.T) {
	clusters, _ := setupTestCluster(t)
	defer func() {
		for _, c := range clusters {
			c.Close()
		}
	}()

	for i, c := range clusters {
		cfg, err := c.Cluster.RaftConfiguration()
		require.NoError(t, err, "node %d", i)
		if len(cfg.Servers) != len(clusters) {
			t.Fatalf("node %d expected %d servers, got %d", i, len(clusters), len(cfg.Servers))
		}
		for _, server := range cfg.Servers {
			if server.Suffrage != raft.Voter {
				t.Fatalf("node %d expected voter, got %v", i, server.Suffrage)
			}
		}
	}
}

func TestCluster_RemoveServer(t *testing.T) {
	clusters, _ := setupTestCluster(t)
	defer func() {
		for _, c := range clusters {
			c.Close()
		}
	}()

	leader := findLeader(clusters)
	if leader == nil {
		t.Fatal("no leader found")
	}

	cfg, err := leader.RaftConfiguration()
	if err != nil {
		t.Fatalf("raft configuration: %v", err)
	}

	var removeID raft.ServerID
	leaderAddr, _ := leader.LeaderWithID()
	for _, server := range cfg.Servers {
		if server.Address != leaderAddr {
			removeID = server.ID
			break
		}
	}
	if removeID == "" {
		t.Fatal("no follower to remove")
	}

	if err := leader.RemoveServer(removeID); err != nil {
		t.Fatalf("RemoveServer: %v", err)
	}

	deadline := time.Now().Add(5 * time.Second)
	for {
		cfg, err := leader.RaftConfiguration()
		if err != nil {
			t.Fatalf("raft configuration after remove: %v", err)
		}
		found := false
		for _, server := range cfg.Servers {
			if server.ID == removeID {
				found = true
				break
			}
		}
		if !found {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("server %s still in config", removeID)
		}
		time.Sleep(50 * time.Millisecond)
	}

	count, err := leader.NumVoters()
	if err != nil {
		t.Fatalf("NumVoters: %v", err)
	}
	if count != len(clusters)-1 {
		t.Fatalf("expected %d voters, got %d", len(clusters)-1, count)
	}
}

func setupBenchCluster(b *testing.B, msync bool) []*builtCluster {
	b.Helper()

	ports := allocatePorts(b, 3)
	nodes := []nodeConfig{
		{ID: "node0", Address: fmt.Sprintf("127.0.0.1:%d", ports[0])},
		{ID: "node1", Address: fmt.Sprintf("127.0.0.1:%d", ports[1])},
		{ID: "node2", Address: fmt.Sprintf("127.0.0.1:%d", ports[2])},
	}

	clusters := make([]*builtCluster, 3)
	for i := 0; i < 3; i++ {
		dir := b.TempDir()
		walDir := filepath.Join(dir, "wal")
		wal, err := walfs.NewWALog(walDir, ".wal",
			walfs.WithMSyncEveryWrite(msync),
			walfs.WithReaderCommitCheck(),
		)
		if err != nil {
			b.Fatalf("failed to create WAL for node %d: %v", i, err)
		}
		builder := newClusterBuilder(nodes[i].ID, "test", dir, nodes).
			WithWAL(wal)
		built, err := builder.build()
		if err != nil {
			b.Fatalf("failed to build node %d: %v", i, err)
		}
		clusters[i] = built
	}

	if err := clusters[0].Bootstrap(nodes); err != nil {
		b.Fatalf("bootstrap failed: %v", err)
	}
	if err := clusters[0].WaitForLeader(10 * time.Second); err != nil {
		b.Fatalf("no leader: %v", err)
	}
	time.Sleep(500 * time.Millisecond)

	return clusters
}

func findBenchLeader(b *testing.B, clusters []*builtCluster) *Cluster {
	for _, c := range clusters {
		if c.Cluster.IsLeader() {
			return c.Cluster
		}
	}
	b.Fatal("no leader")
	return nil
}

func closeClusters(clusters []*builtCluster) {
	for _, c := range clusters {
		c.Close()
	}
}

func BenchmarkCluster_Throughput(b *testing.B) {
	clusters := setupBenchCluster(b, false)
	defer closeClusters(clusters)
	leader := findBenchLeader(b, clusters)

	payload := createTestPayload([]byte("bench-key"), make([]byte, 256))

	numWorkers := 8
	var nextOp atomic.Int64
	var successOps atomic.Int64
	startCh := make(chan struct{})

	var wg sync.WaitGroup

	for w := 0; w < numWorkers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-startCh
			for {
				if nextOp.Add(1) > int64(b.N) {
					return
				}
				future := leader.Apply(payload, 5*time.Second)
				if err := future.Error(); err == nil {
					successOps.Add(1)
				}
			}
		}()
	}

	b.ResetTimer()
	b.ReportAllocs()

	start := time.Now()
	close(startCh)
	wg.Wait()
	b.StopTimer()
	elapsed := time.Since(start)
	if elapsed > 0 {
		b.ReportMetric(float64(successOps.Load())/elapsed.Seconds(), "ops/s")
	}
}

func BenchmarkCluster_Throughput_MSync(b *testing.B) {
	clusters := setupBenchCluster(b, true)
	defer closeClusters(clusters)
	leader := findBenchLeader(b, clusters)

	payload := createTestPayload([]byte("bench-key"), make([]byte, 256))

	numWorkers := 8
	var nextOp atomic.Int64
	var successOps atomic.Int64
	startCh := make(chan struct{})

	var wg sync.WaitGroup

	for w := 0; w < numWorkers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-startCh
			for {
				if nextOp.Add(1) > int64(b.N) {
					return
				}
				future := leader.Apply(payload, 5*time.Second)
				if err := future.Error(); err == nil {
					successOps.Add(1)
				}
			}
		}()
	}

	b.ResetTimer()
	b.ReportAllocs()

	start := time.Now()
	close(startCh)
	wg.Wait()
	b.StopTimer()
	elapsed := time.Since(start)
	if elapsed > 0 {
		b.ReportMetric(float64(successOps.Load())/elapsed.Seconds(), "ops/s")
	}
}

func TestCluster_ReconcileMembers_OnlyLeaderProcesses(t *testing.T) {
	clusters, _ := setupTestCluster(t)
	defer closeClusters(clusters)

	leader := findLeader(clusters)
	require.NotNil(t, leader, "no leader found")

	var follower *Cluster
	for _, c := range clusters {
		if !c.Cluster.IsLeader() {
			follower = c.Cluster
			break
		}
	}
	require.NotNil(t, follower, "no follower found")

	initialVoters, err := leader.NumVoters()
	require.NoError(t, err)

	cfg, err := leader.RaftConfiguration()
	require.NoError(t, err)
	members := membersFromConfig(cfg, "test")
	members = append(members, MemberInformation{
		NodeName: "new-node",
		Tags:     map[string]string{"raft-addr": "127.0.0.1:29999", "namespaces": "test"},
		Status:   MemberStatusAlive,
	})
	follower.ReconcileMembers(members)

	time.Sleep(200 * time.Millisecond)
	currentVoters, err := leader.NumVoters()
	require.NoError(t, err)
	require.Equal(t, initialVoters, currentVoters, "follower should not modify raft config")
}

func TestCluster_ReconcileMembers_IgnoresMissingRaftAddr(t *testing.T) {
	clusters, _ := setupTestCluster(t)
	defer closeClusters(clusters)

	leader := findLeader(clusters)
	require.NotNil(t, leader, "no leader found")

	initialVoters, err := leader.NumVoters()
	require.NoError(t, err)

	cfg, err := leader.RaftConfiguration()
	require.NoError(t, err)
	members := membersFromConfig(cfg, "test")
	members = append(members, MemberInformation{
		NodeName: "new-node",
		Tags:     map[string]string{"other-tag": "value"},
		Status:   MemberStatusAlive,
	})
	leader.ReconcileMembers(members)

	time.Sleep(200 * time.Millisecond)
	currentVoters, err := leader.NumVoters()
	require.NoError(t, err)
	require.Equal(t, initialVoters, currentVoters, "missing raft-addr should be ignored")
}

func TestCluster_ReconcileMembers_AddsServerOnAlive(t *testing.T) {
	clusters, _ := setupTestCluster(t)
	defer closeClusters(clusters)

	leader := findLeader(clusters)
	require.NotNil(t, leader, "no leader found")

	initialVoters, err := leader.NumVoters()
	require.NoError(t, err)
	require.Equal(t, 3, initialVoters)

	newNodeID := "new-node-4"
	newNodeAddr := "127.0.0.1:27299"

	cfg, err := leader.RaftConfiguration()
	require.NoError(t, err)
	members := membersFromConfig(cfg, "test")
	members = append(members, MemberInformation{
		NodeName: newNodeID,
		Tags:     map[string]string{"raft-addr": newNodeAddr, "namespaces": "test"},
		Status:   MemberStatusAlive,
	})
	leader.ReconcileMembers(members)

	deadline := time.Now().Add(5 * time.Second)
	var found bool
	for time.Now().Before(deadline) {
		cfg, err := leader.RaftConfiguration()
		require.NoError(t, err)
		for _, server := range cfg.Servers {
			if string(server.ID) == newNodeID {
				found = true
				require.Equal(t, newNodeAddr, string(server.Address))
				break
			}
		}
		if found {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	require.True(t, found, "new node should be added to raft configuration")
}

func TestCluster_ReconcileMembers_UpdatesAddress(t *testing.T) {
	clusters, _ := setupTestCluster(t)
	defer closeClusters(clusters)

	leader := findLeader(clusters)
	require.NotNil(t, leader, "no leader found")

	cfg, err := leader.RaftConfiguration()
	require.NoError(t, err)

	members := membersFromConfig(cfg, "test")

	targetID := clusters[1].Cluster.LocalID()
	newPort := allocatePorts(t, 1)[0]
	newAddr := fmt.Sprintf("127.0.0.1:%d", newPort)

	var originalAddr string
	for i, member := range members {
		if member.NodeName == string(targetID) {
			originalAddr = member.Tags["raft-addr"]
			members[i].Tags["raft-addr"] = newAddr
			break
		}
	}
	require.NotEmpty(t, originalAddr)

	leader.ReconcileMembers(members)

	cfg, err = leader.RaftConfiguration()
	require.NoError(t, err)

	var updatedAddr raft.ServerAddress
	for _, server := range cfg.Servers {
		if server.ID == targetID {
			updatedAddr = server.Address
			break
		}
	}
	require.Equal(t, raft.ServerAddress(newAddr), updatedAddr)

	for i, member := range members {
		if member.NodeName == string(targetID) {
			members[i].Tags["raft-addr"] = originalAddr
			break
		}
	}

	leader.ReconcileMembers(members)

	cfg, err = leader.RaftConfiguration()
	require.NoError(t, err)

	var restoredAddr raft.ServerAddress
	for _, server := range cfg.Servers {
		if server.ID == targetID {
			restoredAddr = server.Address
			break
		}
	}
	require.Equal(t, raft.ServerAddress(originalAddr), restoredAddr)
}

func TestCluster_ReconcileMembers_RemovesServerOnLeave(t *testing.T) {
	clusters, _ := setupTestCluster(t)
	defer closeClusters(clusters)

	leader := findLeader(clusters)
	require.NotNil(t, leader, "no leader found")

	var followerID string
	cfg, err := leader.RaftConfiguration()
	require.NoError(t, err)
	for _, server := range cfg.Servers {
		if server.ID != leader.LocalID() {
			followerID = string(server.ID)
			break
		}
	}
	require.NotEmpty(t, followerID, "no follower to remove")

	initialVoters, err := leader.NumVoters()
	require.NoError(t, err)
	require.Equal(t, 3, initialVoters)

	cfg, err = leader.RaftConfiguration()
	require.NoError(t, err)
	members := membersFromConfig(cfg, "test")
	for i := range members {
		if members[i].NodeName == followerID {
			members[i].Status = MemberStatusLeft
			break
		}
	}
	leader.ReconcileMembers(members)

	deadline := time.Now().Add(5 * time.Second)
	var removed bool
	for time.Now().Before(deadline) {
		cfg, err := leader.RaftConfiguration()
		require.NoError(t, err)
		found := false
		for _, server := range cfg.Servers {
			if string(server.ID) == followerID {
				found = true
				break
			}
		}
		if !found {
			removed = true
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	require.True(t, removed, "follower should be removed from raft configuration")

	finalVoters, err := leader.NumVoters()
	require.NoError(t, err)
	require.Equal(t, initialVoters-1, finalVoters)
}

func TestCluster_ReconcileMembers_DoesNotRemoveFailed(t *testing.T) {
	clusters, _ := setupTestCluster(t)
	defer closeClusters(clusters)

	leader := findLeader(clusters)
	require.NotNil(t, leader)

	cfg, err := leader.RaftConfiguration()
	require.NoError(t, err)

	var followerID raft.ServerID
	for _, server := range cfg.Servers {
		if server.ID != leader.LocalID() {
			followerID = server.ID
			break
		}
	}
	require.NotEmpty(t, followerID)

	members := membersFromConfig(cfg, "test")
	for i := range members {
		if members[i].NodeName == string(followerID) {
			members[i].Status = MemberStatusFailed
			break
		}
	}
	leader.ReconcileMembers(members)

	cfg, err = leader.RaftConfiguration()
	require.NoError(t, err)
	found := false
	for _, server := range cfg.Servers {
		if server.ID == followerID {
			found = true
			break
		}
	}
	require.True(t, found)
}

func TestCluster_ReconcileMembers_RemovesReaped(t *testing.T) {
	clusters, _ := setupTestCluster(t)
	defer closeClusters(clusters)

	leader := findLeader(clusters)
	require.NotNil(t, leader)

	cfg, err := leader.RaftConfiguration()
	require.NoError(t, err)

	var reapedID raft.ServerID
	for _, server := range cfg.Servers {
		if server.ID != leader.LocalID() {
			reapedID = server.ID
			break
		}
	}
	require.NotEmpty(t, reapedID)

	members := membersFromConfig(cfg, "test")
	for i := range members {
		if members[i].NodeName == string(reapedID) {
			members[i].Status = MemberStatusReaped
			break
		}
	}

	leader.ReconcileMembers(members)

	deadline := time.Now().Add(5 * time.Second)
	for {
		cfg, err := leader.RaftConfiguration()
		require.NoError(t, err)
		stillFound := false
		for _, server := range cfg.Servers {
			if server.ID == reapedID {
				stillFound = true
				break
			}
		}
		if !stillFound {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("server %s still in config", reapedID)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

func TestCluster_OnChangeEvent_IgnoresUnknownEvent(t *testing.T) {
	clusters, _ := setupTestCluster(t)
	defer closeClusters(clusters)

	leader := findLeader(clusters)
	require.NotNil(t, leader, "no leader found")

	initialVoters, err := leader.NumVoters()
	require.NoError(t, err)

	leader.OnChangeEvent(MemberEventLeaderChange, MemberInformation{
		NodeName: "some-node",
		Tags:     map[string]string{"raft-addr": "127.0.0.1:27499", "namespaces": "test"},
	})

	time.Sleep(200 * time.Millisecond)
	currentVoters, err := leader.NumVoters()
	require.NoError(t, err)
	require.Equal(t, initialVoters, currentVoters, "unknown event should not change config")
}

func TestCluster_Close_LeaderTransfersLeadership(t *testing.T) {
	clusters, _ := setupTestCluster(t)

	leader := findLeader(clusters)
	require.NotNil(t, leader, "no leader found")

	leaderID := leader.LocalID()
	require.NotEmpty(t, leaderID)

	err := leader.Close()
	require.NoError(t, err)

	deadline := time.Now().Add(10 * time.Second)
	var newLeader *Cluster
	for time.Now().Before(deadline) {
		for _, c := range clusters {
			if c.Cluster.LocalID() != leaderID && !c.Cluster.closed.Load() {
				if c.Cluster.IsLeader() {
					newLeader = c.Cluster
					break
				}
			}
		}
		if newLeader != nil {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}

	require.NotNil(t, newLeader, "new leader not elected")

	payload := createTestPayload([]byte("leader-close"), []byte("ok"))
	future := newLeader.Apply(payload, 5*time.Second)
	require.NoError(t, future.Error())

	for _, c := range clusters {
		if c.Cluster.LocalID() != leaderID {
			c.Close()
		}
	}
}

func TestCluster_Close_NonLeaderWaitsForRemoval(t *testing.T) {
	clusters, _ := setupTestCluster(t)
	defer func() {
		for _, c := range clusters {
			c.Close()
		}
	}()

	leader := findLeader(clusters)
	require.NotNil(t, leader, "no leader found")

	var follower *builtCluster
	var followerID raft.ServerID
	for _, c := range clusters {
		if !c.Cluster.IsLeader() {
			follower = c
			followerID = c.Cluster.LocalID()
			break
		}
	}
	require.NotNil(t, follower, "no follower found")

	err := leader.RemoveServer(followerID)
	require.NoError(t, err)

	start := time.Now()
	err = follower.Cluster.Close()
	elapsed := time.Since(start)
	require.NoError(t, err)

	require.Less(t, elapsed, 2*time.Second, "follower should detect removal quickly")
}

func TestCluster_Close_SingleNodeCluster(t *testing.T) {
	port := allocatePorts(t, 1)[0]
	nodes := []nodeConfig{
		{ID: "solo", Address: fmt.Sprintf("127.0.0.1:%d", port)},
	}

	fsm := &countingFSM{}
	dir := t.TempDir()
	walDir := filepath.Join(dir, "wal")
	wal, err := walfs.NewWALog(walDir, ".wal", walfs.WithReaderCommitCheck())
	require.NoError(t, err)
	builder := newClusterBuilder(nodes[0].ID, "test", dir, nodes).
		WithWAL(wal).
		WithFSMFactory(func() raft.FSM { return fsm })

	built, err := builder.build()
	require.NoError(t, err)

	err = built.Bootstrap(nodes)
	require.NoError(t, err)

	err = built.WaitForLeader(5 * time.Second)
	require.NoError(t, err)

	err = built.Cluster.Close()
	require.NoError(t, err)
}

func TestCluster_Close_Idempotent(t *testing.T) {
	clusters, _ := setupTestCluster(t)
	defer closeClusters(clusters)

	cluster := clusters[0].Cluster

	err := cluster.Close()
	require.NoError(t, err)

	err = cluster.Close()
	require.NoError(t, err)

	err = cluster.Close()
	require.NoError(t, err)
}

func TestCluster_LocalID(t *testing.T) {
	clusters, _ := setupTestCluster(t)
	defer closeClusters(clusters)

	expectedIDs := []string{"node0", "node1", "node2"}
	for i, c := range clusters {
		require.Equal(t, raft.ServerID(expectedIDs[i]), c.Cluster.LocalID())
	}
}

func TestCluster_ImplementsClusterDiscover(t *testing.T) {
	var _ ClusterDiscover = (*Cluster)(nil)
}

func TestIntegration_MembershipNotifiesMultipleClusters(t *testing.T) {
	membership := &Membership{}
	l1 := &testClusterListener{}
	l2 := &testClusterListener{}
	l3 := &testClusterListener{}

	membership.RegisterCluster(l1)
	membership.RegisterCluster(l2)
	membership.RegisterCluster(l3)

	info := MemberInformation{
		NodeName: "external-node",
		Tags:     map[string]string{"raft-addr": "127.0.0.1:28099", "namespaces": "test"},
	}

	membership.notifyClusters(MemberEventJoin, info)

	require.True(t, l1.hasEvent(MemberEventJoin, "external-node"))
	require.True(t, l2.hasEvent(MemberEventJoin, "external-node"))
	require.True(t, l3.hasEvent(MemberEventJoin, "external-node"))
}

func TestIntegration_RegisterClusterWithMembership(t *testing.T) {
	membership := &Membership{
		clusters: make([]ClusterDiscover, 0),
	}

	listener := &testClusterListener{}
	membership.RegisterCluster(listener)

	require.Len(t, membership.clusters, 1)

	info := MemberInformation{
		NodeName: "test-node",
		Tags:     map[string]string{"raft-addr": "127.0.0.1:28199", "namespaces": "test"},
	}

	membership.notifyClusters(MemberEventJoin, info)

	require.True(t, listener.hasEvent(MemberEventJoin, "test-node"))
}

func TestCluster_AddServer_AlreadyExists(t *testing.T) {
	clusters, _ := setupTestCluster(t)
	defer closeClusters(clusters)

	leader := findLeader(clusters)
	require.NotNil(t, leader)

	existingID := clusters[1].Cluster.LocalID()
	cfg, err := leader.RaftConfiguration()
	require.NoError(t, err)

	var existingAddr raft.ServerAddress
	for _, server := range cfg.Servers {
		if server.ID == existingID {
			existingAddr = server.Address
			break
		}
	}
	require.NotEmpty(t, existingAddr)

	err = leader.AddServer(existingID, existingAddr)

	require.NoError(t, err)
}

func TestCluster_AddServer_UpdatesAddress(t *testing.T) {
	clusters, _ := setupTestCluster(t)
	defer closeClusters(clusters)

	leader := findLeader(clusters)
	require.NotNil(t, leader)

	targetID := clusters[1].Cluster.LocalID()
	cfg, err := leader.RaftConfiguration()
	require.NoError(t, err)

	var originalAddr raft.ServerAddress
	for _, server := range cfg.Servers {
		if server.ID == targetID {
			originalAddr = server.Address
			break
		}
	}
	require.NotEmpty(t, originalAddr)

	newPort := allocatePorts(t, 1)[0]
	newAddr := raft.ServerAddress(fmt.Sprintf("127.0.0.1:%d", newPort))

	err = leader.AddServer(targetID, newAddr)
	require.NoError(t, err)

	cfg, err = leader.RaftConfiguration()
	require.NoError(t, err)

	var updatedAddr raft.ServerAddress
	for _, server := range cfg.Servers {
		if server.ID == targetID {
			updatedAddr = server.Address
			break
		}
	}
	require.Equal(t, newAddr, updatedAddr)

	err = leader.AddServer(targetID, originalAddr)
	require.NoError(t, err)

	cfg, err = leader.RaftConfiguration()
	require.NoError(t, err)

	var restoredAddr raft.ServerAddress
	for _, server := range cfg.Servers {
		if server.ID == targetID {
			restoredAddr = server.Address
			break
		}
	}
	require.Equal(t, originalAddr, restoredAddr)
}

func TestCluster_RemoveServer_NotExists(t *testing.T) {
	clusters, _ := setupTestCluster(t)
	defer closeClusters(clusters)

	leader := findLeader(clusters)
	require.NotNil(t, leader)

	err := leader.RemoveServer("non-existent-node")

	require.NoError(t, err)
}

func TestCluster_Stats(t *testing.T) {
	clusters, _ := setupTestCluster(t)
	defer closeClusters(clusters)

	leader := findLeader(clusters)
	require.NotNil(t, leader)

	stats := leader.Stats()
	require.True(t, stats.IsLeader)
	require.Equal(t, raft.Leader, stats.State)
	require.NotEmpty(t, stats.LeaderAddr)
	require.Greater(t, stats.LastIdx, uint64(0))

	for _, c := range clusters {
		if !c.Cluster.IsLeader() {
			stats := c.Cluster.Stats()
			require.False(t, stats.IsLeader)
			require.Equal(t, raft.Follower, stats.State)
			require.NotEmpty(t, stats.LeaderAddr)
			break
		}
	}
}

func TestCluster_ReconcileMembers_SequentialJoins(t *testing.T) {
	clusters, _ := setupTestCluster(t)
	defer closeClusters(clusters)

	leader := findLeader(clusters)
	require.NotNil(t, leader)

	numJoins := 3

	cfg, err := leader.RaftConfiguration()
	require.NoError(t, err)
	members := membersFromConfig(cfg, "test")

	for i := 0; i < numJoins; i++ {
		info := MemberInformation{
			NodeName: fmt.Sprintf("sequential-node-%d", i),
			Tags:     map[string]string{"raft-addr": fmt.Sprintf("127.0.0.1:%d", 28550+i), "namespaces": "test"},
			Status:   MemberStatusAlive,
		}
		members = append(members, info)
		leader.ReconcileMembers(members)
		time.Sleep(500 * time.Millisecond)
	}

	cfg, err = leader.RaftConfiguration()
	require.NoError(t, err)

	addedCount := 0
	for _, server := range cfg.Servers {
		for i := 0; i < numJoins; i++ {
			if string(server.ID) == fmt.Sprintf("sequential-node-%d", i) {
				addedCount++
				break
			}
		}
	}
	require.Equal(t, numJoins, addedCount, "all sequential nodes should be added")
}

func TestSplitAndTrim(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected []string
	}{
		{
			name:     "empty string",
			input:    "",
			expected: nil,
		},
		{
			name:     "single value",
			input:    "foo",
			expected: []string{"foo"},
		},
		{
			name:     "multiple values",
			input:    "foo,bar,baz",
			expected: []string{"foo", "bar", "baz"},
		},
		{
			name:     "values with spaces",
			input:    " foo , bar , baz ",
			expected: []string{"foo", "bar", "baz"},
		},
		{
			name:     "trailing comma",
			input:    "foo,bar,",
			expected: []string{"foo", "bar"},
		},
		{
			name:     "leading comma",
			input:    ",foo,bar",
			expected: []string{"foo", "bar"},
		},
		{
			name:     "only commas",
			input:    ",,,",
			expected: []string{},
		},
		{
			name:     "only spaces and commas",
			input:    "  ,  ,  ",
			expected: []string{},
		},
		{
			name:     "mixed empty and non-empty",
			input:    "foo,,bar,  ,baz",
			expected: []string{"foo", "bar", "baz"},
		},
		{
			name:     "single space",
			input:    " ",
			expected: []string{},
		},
		{
			name:     "whitespace only value",
			input:    "foo,   ,bar",
			expected: []string{"foo", "bar"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := SplitAndTrim(tt.input)
			require.Equal(t, tt.expected, result)
		})
	}
}
