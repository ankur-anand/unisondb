package raftcluster

import (
	"fmt"
	"io"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ankur-anand/unisondb/internal/logcodec"
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

// setupTestCluster creates a 3-node test cluster.
func setupTestCluster(t *testing.T, basePort int) ([]*BuiltCluster, []*countingFSM) {
	t.Helper()

	nodes := []NodeConfig{
		{ID: "node0", Address: fmt.Sprintf("127.0.0.1:%d", basePort)},
		{ID: "node1", Address: fmt.Sprintf("127.0.0.1:%d", basePort+1)},
		{ID: "node2", Address: fmt.Sprintf("127.0.0.1:%d", basePort+2)},
	}

	clusters := make([]*BuiltCluster, 3)
	fsms := make([]*countingFSM, 3)

	for i := 0; i < 3; i++ {
		fsm := &countingFSM{}
		fsms[i] = fsm

		dir := t.TempDir()
		builder := NewClusterBuilder(nodes[i].ID, dir, nodes).
			WithFSMFactory(func() raft.FSM { return fsm })

		built, err := builder.Build()
		if err != nil {
			for j := 0; j < i; j++ {
				clusters[j].Close()
			}
			t.Fatalf("failed to build cluster node %d: %v", i, err)
		}
		clusters[i] = built
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

func findLeader(clusters []*BuiltCluster) *Cluster {
	for _, c := range clusters {
		if c.Cluster.IsLeader() {
			return c.Cluster
		}
	}
	return nil
}

func TestCluster_SingleApply(t *testing.T) {
	clusters, fsms := setupTestCluster(t, 17000)
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
	clusters, fsms := setupTestCluster(t, 17100)
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
	clusters, _ := setupTestCluster(t, 17200)
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
	clusters, _ := setupTestCluster(t, 17300)
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

func setupBenchCluster(b *testing.B, basePort int, msync bool) []*BuiltCluster {
	b.Helper()

	nodes := []NodeConfig{
		{ID: "node0", Address: fmt.Sprintf("127.0.0.1:%d", basePort)},
		{ID: "node1", Address: fmt.Sprintf("127.0.0.1:%d", basePort+1)},
		{ID: "node2", Address: fmt.Sprintf("127.0.0.1:%d", basePort+2)},
	}

	clusters := make([]*BuiltCluster, 3)
	for i := 0; i < 3; i++ {
		dir := b.TempDir()
		builder := NewClusterBuilder(nodes[i].ID, dir, nodes).
			WithMSyncEveryWrite(msync)
		built, err := builder.Build()
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

func findBenchLeader(b *testing.B, clusters []*BuiltCluster) *Cluster {
	for _, c := range clusters {
		if c.Cluster.IsLeader() {
			return c.Cluster
		}
	}
	b.Fatal("no leader")
	return nil
}

func closeClusters(clusters []*BuiltCluster) {
	for _, c := range clusters {
		c.Close()
	}
}

func BenchmarkCluster_Apply_Serial(b *testing.B) {
	clusters := setupBenchCluster(b, 18000, false)
	defer closeClusters(clusters)
	leader := findBenchLeader(b, clusters)

	payload := createTestPayload([]byte("bench-key"), make([]byte, 256))

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		future := leader.Apply(payload, 5*time.Second)
		if err := future.Error(); err != nil {
			b.Fatalf("apply failed: %v", err)
		}
	}
}

func BenchmarkCluster_Apply_Parallel(b *testing.B) {
	clusters := setupBenchCluster(b, 19000, false)
	defer closeClusters(clusters)
	leader := findBenchLeader(b, clusters)

	payload := createTestPayload([]byte("bench-key"), make([]byte, 256))

	b.ResetTimer()
	b.ReportAllocs()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			future := leader.Apply(payload, 5*time.Second)
			if err := future.Error(); err != nil {
				b.Errorf("apply failed: %v", err)
			}
		}
	})
}

func BenchmarkCluster_Apply_Pipeline(b *testing.B) {
	clusters := setupBenchCluster(b, 20000, false)
	defer closeClusters(clusters)
	leader := findBenchLeader(b, clusters)

	payload := createTestPayload([]byte("bench-key"), make([]byte, 256))
	batchSize := 100

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i += batchSize {
		count := batchSize
		if i+batchSize > b.N {
			count = b.N - i
		}

		futures := make([]raft.ApplyFuture, count)
		for j := 0; j < count; j++ {
			futures[j] = leader.Apply(payload, 5*time.Second)
		}

		for _, f := range futures {
			if err := f.Error(); err != nil {
				b.Errorf("apply failed: %v", err)
			}
		}
	}
}

func BenchmarkCluster_Apply_Serial_MSync(b *testing.B) {
	clusters := setupBenchCluster(b, 23000, true)
	defer closeClusters(clusters)
	leader := findBenchLeader(b, clusters)

	payload := createTestPayload([]byte("bench-key"), make([]byte, 256))

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		future := leader.Apply(payload, 5*time.Second)
		if err := future.Error(); err != nil {
			b.Fatalf("apply failed: %v", err)
		}
	}
}

func BenchmarkCluster_Apply_Parallel_MSync(b *testing.B) {
	clusters := setupBenchCluster(b, 24000, true)
	defer closeClusters(clusters)
	leader := findBenchLeader(b, clusters)

	payload := createTestPayload([]byte("bench-key"), make([]byte, 256))

	b.ResetTimer()
	b.ReportAllocs()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			future := leader.Apply(payload, 5*time.Second)
			if err := future.Error(); err != nil {
				b.Errorf("apply failed: %v", err)
			}
		}
	})
}

func BenchmarkCluster_Apply_Pipeline_MSync(b *testing.B) {
	clusters := setupBenchCluster(b, 25000, true)
	defer closeClusters(clusters)
	leader := findBenchLeader(b, clusters)

	payload := createTestPayload([]byte("bench-key"), make([]byte, 256))
	batchSize := 100

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i += batchSize {
		count := batchSize
		if i+batchSize > b.N {
			count = b.N - i
		}

		futures := make([]raft.ApplyFuture, count)
		for j := 0; j < count; j++ {
			futures[j] = leader.Apply(payload, 5*time.Second)
		}

		for _, f := range futures {
			if err := f.Error(); err != nil {
				b.Errorf("apply failed: %v", err)
			}
		}
	}
}

func BenchmarkCluster_Throughput(b *testing.B) {
	clusters := setupBenchCluster(b, 22000, false)
	defer closeClusters(clusters)
	leader := findBenchLeader(b, clusters)

	payload := createTestPayload([]byte("bench-key"), make([]byte, 256))

	numWorkers := 8
	opsPerWorker := b.N / numWorkers
	if opsPerWorker < 1 {
		opsPerWorker = 1
	}

	b.ResetTimer()
	b.ReportAllocs()

	var wg sync.WaitGroup
	var totalOps atomic.Int64

	for w := 0; w < numWorkers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < opsPerWorker; i++ {
				future := leader.Apply(payload, 5*time.Second)
				if err := future.Error(); err == nil {
					totalOps.Add(1)
				}
			}
		}()
	}

	wg.Wait()
}

func BenchmarkCluster_Throughput_MSync(b *testing.B) {
	clusters := setupBenchCluster(b, 26000, true)
	defer closeClusters(clusters)
	leader := findBenchLeader(b, clusters)

	payload := createTestPayload([]byte("bench-key"), make([]byte, 256))

	numWorkers := 8
	opsPerWorker := b.N / numWorkers
	if opsPerWorker < 1 {
		opsPerWorker = 1
	}

	b.ResetTimer()
	b.ReportAllocs()

	var wg sync.WaitGroup
	var totalOps atomic.Int64

	for w := 0; w < numWorkers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < opsPerWorker; i++ {
				future := leader.Apply(payload, 5*time.Second)
				if err := future.Error(); err == nil {
					totalOps.Add(1)
				}
			}
		}()
	}

	wg.Wait()
}
