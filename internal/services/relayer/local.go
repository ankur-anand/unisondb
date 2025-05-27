package relayer

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"strconv"
	"time"

	"github.com/ankur-anand/unisondb/dbkernel"
	"github.com/ankur-anand/unisondb/pkg/replicator"
	"github.com/ankur-anand/unisondb/schemas/logrecord"
	v1 "github.com/ankur-anand/unisondb/schemas/proto/gen/go/unisondb/streamer/v1"
	llhist "github.com/openhistogram/circonusllhist"
)

const segmentLagEmitThreshold = 3

// LocalWalRelayer encodes all the parameter needed to start local relayer and used for testing purpose only.
type LocalWalRelayer struct {
	id              string
	lastOffset      *dbkernel.Offset
	replicatedCount int
	lsn             uint64
	startTime       time.Time
}

func NewLocalWalRelayer(id int) *LocalWalRelayer {
	return &LocalWalRelayer{
		id:        strconv.Itoa(id),
		startTime: time.Now(),
	}
}

// Run starts the relayer which continuously pulls WAL records and lag emits metrics.
// nolint:gocognit
func (n *LocalWalRelayer) Run(ctx context.Context, engine *dbkernel.Engine, metricsTickInterval time.Duration,
	hist *llhist.Histogram) error {
	rpInstance := replicator.NewReplicator(engine,
		50,
		1*time.Second, n.lastOffset, "local")

	walReceiver := make(chan []*v1.WALRecord, 1000)
	replicatorErrors := make(chan error, 2)
	go func() {
		err := rpInstance.Replicate(ctx, walReceiver)
		replicatorErrors <- err
	}()

	ticker := time.NewTicker(metricsTickInterval)
	defer ticker.Stop()
	namespace := engine.Namespace()
	for {
		select {
		case <-ctx.Done():
			var segmentLag int
			segment := -1
			if n.lastOffset != nil {
				segmentLag = int(engine.CurrentOffset().SegmentID) - int(n.lastOffset.SegmentID)
				segment = int(n.lastOffset.SegmentID)
			}

			slog.Debug("[unisondb.relayer]",
				slog.String("event_type", "local.relayer.sync.stats"),
				slog.String("namespace", namespace),
				slog.Int("segment", segment),
				slog.Int("segment_lag", segmentLag),
				slog.Int("replicated", n.replicatedCount),
			)
			return nil
		case records := <-walReceiver:
			if len(records) == 0 {
				continue
			}
			for _, record := range records {
				fbRecord := logrecord.GetRootAsLogRecord(record.Record, 0)
				receivedLSN := fbRecord.Lsn()
				remoteHLC := fbRecord.Hlc()
				nowMs := dbkernel.HLCNow()
				physicalLatencyMs := nowMs - remoteHLC
				// Add latency to histogram in seconds
				err := hist.RecordValue(float64(physicalLatencyMs) / 1000.0)
				if err != nil {
					return err
				}
				if receivedLSN != n.lsn+1 {
					panic(fmt.Sprintf("received wrong LSN %d, want %d", receivedLSN, n.lsn+1))
				}
				n.lsn++
			}
			n.replicatedCount += len(records)
			n.lastOffset = dbkernel.DecodeOffset(records[len(records)-1].Offset)
		case err := <-replicatorErrors:
			if errors.Is(err, context.Canceled) || errors.Is(err, io.EOF) {
				return nil
			}
			panic(err)
		case <-ticker.C:
			if n.lastOffset != nil {
				segmentLag := int(engine.CurrentOffset().SegmentID) - int(n.lastOffset.SegmentID)
				if segmentLag >= segmentLagEmitThreshold {
					slog.Info("[unisondb.relayer]",
						slog.String("event_type", "local.relayer.sync.stats"),
						slog.String("namespace", namespace),
						slog.Int("segment", int(n.lastOffset.SegmentID)),
						slog.Int("segment_lag", segmentLag),
						slog.Int("replicated", n.replicatedCount),
					)
				}
			}
		}
	}
}

// StartNLocalRelayer launches multiple local relayers for a given engine.
func StartNLocalRelayer(ctx context.Context, engine *dbkernel.Engine, num int, metricsTickInterval time.Duration) ([]*llhist.Histogram, error) {
	if num <= 0 {
		return nil, nil
	}
	slog.Info("[unisondb.relayer]",
		slog.String("event_type", "starting.local.relayer"),
		slog.String("namespace", engine.Namespace()),
		slog.Int("num", num),
	)

	hists := make([]*llhist.Histogram, num)

	for i := 0; i < num; i++ {
		rep := NewLocalWalRelayer(i)
		hist := llhist.New()
		hists[i] = hist

		go func(r *LocalWalRelayer, h *llhist.Histogram) {
			if err := r.Run(ctx, engine, metricsTickInterval, h); err != nil && !errors.Is(err, context.Canceled) {
				panic(err)
			}
		}(rep, hist)
	}

	return hists, nil
}

func ReportReplicationStats(hists []*llhist.Histogram, namespace string, start time.Time) {
	merged := llhist.New()
	for _, h := range hists {
		merged.Merge(h)
	}

	p50 := merged.ValueAtQuantile(0.50)
	p90 := merged.ValueAtQuantile(0.90)
	p99 := merged.ValueAtQuantile(0.99)
	maxP := merged.Max()

	total := merged.Count()
	rate := float64(total) / time.Since(start).Seconds()
	fmt.Printf("\n==== Replication Stats for namespace \"%s\" ====\n", namespace)
	fmt.Printf("Total Records: %d\n", total)
	fmt.Printf("Throughput   : %.2f records/sec\n", rate)
	fmt.Printf("Latency (ms) : p50=%.2f  p90=%.2f  p99=%.2f  max=%.2f\n",
		p50*1000, p90*1000, p99*1000, maxP*1000)
	fmt.Println("==== Replication Stats ====")
}
