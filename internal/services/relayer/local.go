package relayer

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"strconv"
	"sync"
	"time"

	"github.com/ankur-anand/unisondb/dbkernel"
	"github.com/ankur-anand/unisondb/pkg/replicator"
	"github.com/ankur-anand/unisondb/schemas/logrecord"
	v1 "github.com/ankur-anand/unisondb/schemas/proto/gen/go/unisondb/streamer/v1"
	llhist "github.com/openhistogram/circonusllhist"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

const segmentLagEmitThreshold = 3

var (
	localSegmentLagGauge = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "unisondb",
		Subsystem: "local_relayer",
		Name:      "wal_segment_lag",
		Help:      "Difference in segment IDs between upstream and noop replica",
		// id can really increment cardinality of the metrics, should only be used in test or controlled
		// env.
	}, []string{"namespace", "id"})

	localWALReplicatedTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "unisondb",
		Subsystem: "local_relayer",
		Name:      "wal_records_replicated_total",
		Help:      "Cumulative count of WAL records successfully replicated locally.",
	}, []string{"namespace"})
)

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
	once *sync.Once, hist *llhist.Histogram) error {
	rpInstance := replicator.NewReplicator(engine,
		20,
		1*time.Second, n.lastOffset, "local")

	walReceiver := make(chan []*v1.WALRecord, 100)
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
			once.Do(func() {
				if n.replicatedCount > 0 && hist != nil {
					p99 := hist.ValueAtQuantile(0.99)
					throughput := float64(n.replicatedCount) / time.Since(n.startTime).Seconds() // or track your own `startTime`
					fmt.Println("==== Replication Stats ====")
					fmt.Printf("Namespace \"%s\": %.2f replication per second, p99=%.2f msec\n",
						namespace, throughput, p99*1000)
					fmt.Println("==== Replication Stats ====")
				}
			})
			return nil
		case records := <-walReceiver:
			if len(records) == 0 {
				continue
			}
			for _, record := range records {
				localWALReplicatedTotal.WithLabelValues(namespace).Inc()
				fbRecord := logrecord.GetRootAsLogRecord(record.Record, 0)
				receivedLSN := fbRecord.Lsn()
				remoteHLC := fbRecord.Hlc()
				eventRemoteTimeMs, _ := dbkernel.HLCDecode(remoteHLC)
				nowMs := uint64(time.Now().UnixMilli()) - dbkernel.CustomEpochMs
				physicalLatencyMs := nowMs - eventRemoteTimeMs
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
					localSegmentLagGauge.WithLabelValues(namespace, n.id).Set(float64(segmentLag))
					slog.Info("[unisondb.relayer]",
						slog.String("event_type", "local.relayer.sync.stats"),
						slog.String("namespace", namespace),
						slog.Int("segment", int(n.lastOffset.SegmentID)),
						slog.Int("segment_lag", segmentLag),
						slog.Int("replicated", n.replicatedCount),
					)
				} else {
					localSegmentLagGauge.DeleteLabelValues(namespace, n.id)
				}
			}
		}
	}
}

// StartNLocalRelayer launches multiple local relayers for a given engine.
func StartNLocalRelayer(ctx context.Context, engine *dbkernel.Engine, num int, metricsTickInterval time.Duration) error {
	if num <= 0 {
		return nil
	}
	slog.Info("[unisondb.relayer]",
		slog.String("event_type", "starting.local.relayer"),
		slog.String("namespace", engine.Namespace()),
		slog.Int("num", num),
	)

	once := sync.Once{}
	hist := llhist.New()
	for i := 0; i < num; i++ {
		rep := NewLocalWalRelayer(i)

		go func(r *LocalWalRelayer) {
			if err := r.Run(ctx, engine, metricsTickInterval, &once, hist); err != nil && !errors.Is(err, context.Canceled) {
				panic(err)
			}
		}(rep)
	}

	return nil
}
