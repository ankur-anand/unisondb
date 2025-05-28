package benchmark

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/ankur-anand/unisondb/dbkernel"
	"github.com/ankur-anand/unisondb/internal/services/fuzzer"
	"github.com/ankur-anand/unisondb/internal/services/relayer"
	llhist "github.com/openhistogram/circonusllhist"
)

func BenchmarkUnisonDBFuzzAndReplication(b *testing.B) {
	replicatorCounts := []int{0, 10, 50, 100, 500, 1000, 1500, 2000}

	file, err := os.Create("replicator_fuzzer_metrics.csv")
	if err != nil {
		b.Fatalf("failed to create output file: %v", err)
	}
	defer file.Close()

	const (
		namespace  = "bench-namespace"
		opsPerSec  = 1000
		numWorkers = 20
		// modify this for run duration.
		duration = 1 * time.Minute
	)

	_, err = fmt.Fprintln(file, "type,replicators,ops_per_sec,p50_ms,p99_ms,throughput")
	if err != nil {
		b.Fatalf("failed to write to file: %v", err)
	}

	for _, numReplicators := range replicatorCounts {
		b.Run(fmt.Sprintf("replicators_%d", numReplicators), func(b *testing.B) {
			ctx, cancel := context.WithCancel(context.Background())

			dir := b.TempDir() + fmt.Sprintf("%d-%d", time.Now().UnixNano(), numReplicators)
			e, err := dbkernel.NewStorageEngine(dir, namespace, dbkernel.NewDefaultEngineConfig())
			if err != nil {
				b.Fatal(err)
			}

			defer e.Close(context.Background())

			stats := fuzzer.NewFuzzStats()

			go fuzzer.FuzzEngineOps(ctx, e, opsPerSec, numWorkers, stats, namespace, true)

			hists, err := relayer.StartNLocalRelayer(ctx, e, numReplicators, 1*time.Second)
			if err != nil {
				b.Fatalf("failed to start relayers: %v", err)
			}

			b.ResetTimer()
			time.Sleep(duration)
			b.StopTimer()
			cancel()

			snapshot := stats.Snapshot()
			nsStats, ok := snapshot[namespace]
			if !ok {
				b.Fatalf("no stats found for namespace: %s", namespace)
			}

			relayHist := llhist.New()
			for _, h := range hists {
				relayHist.Merge(h)
			}

			relayP99 := relayHist.ValueAtQuantile(0.99) * 1000
			relayThroughput := float64(relayHist.Count()) / duration.Seconds()

			// Write fuzzer metrics
			_, err = fmt.Fprintf(file, "fuzzer,%d,%.2f,%.2f,%.2f,%.2f\n",
				numReplicators, nsStats.OpsRate, nsStats.Latency["p50"], nsStats.Latency["p99"], nsStats.Throughput)
			if err != nil {
				b.Fatalf("failed to write to file: %v", err)
			}

			// Write replication metrics
			_, err = fmt.Fprintf(file, "replication,%d,NA,%.2f,%.2f,\n",
				numReplicators, relayP99, relayThroughput)
			if err != nil {
				b.Fatalf("failed to write to file: %v", err)
			}

			b.Run("Fuzzer/Throughput", func(b *testing.B) {
				b.ReportMetric(nsStats.Throughput, "ops/sec")
			})
			b.Run("Fuzzer/P99Latency", func(b *testing.B) {
				b.ReportMetric(nsStats.Latency["p99"], "ms")
			})
			b.Run("Replication/Throughput", func(b *testing.B) {
				b.ReportMetric(relayThroughput, "ops/sec")
			})
			b.Run("Replication/P99Latency", func(b *testing.B) {
				b.ReportMetric(relayP99, "ms")
			})
		})
	}
}
