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
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	localSegmentLagGauge = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "unisondb",
		Subsystem: "local_relayer",
		Name:      "wal_segment_lag",
		Help:      "Difference in segment IDs between upstream and noop replica",
		// id can really increment cardinality of the metrics, should only be used in test or controlled
		// env.
	}, []string{"namespace", "id"})
)

// LocalWalRelayer encodes all the parameter needed to start local relayer and used for testing purpose only.
type LocalWalRelayer struct {
	id              string
	lastOffset      *dbkernel.Offset
	replicatedCount int
	lsn             uint64
}

func NewLocalWalRelayer(id int) *LocalWalRelayer {
	return &LocalWalRelayer{
		id: strconv.Itoa(id),
	}
}

// Run starts the relayer which continuously pulls WAL records and lag emits metrics.
func (n *LocalWalRelayer) Run(ctx context.Context, engine *dbkernel.Engine, metricsTickInterval time.Duration) error {
	rpInstance := replicator.NewReplicator(engine,
		20,
		100*time.Millisecond, n.lastOffset, "local")

	walReceiver := make(chan []*v1.WALRecord, 2)
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
			return nil
		case records := <-walReceiver:
			if len(records) == 0 {
				continue
			}
			for _, record := range records {
				fbRecord := logrecord.GetRootAsLogRecord(record.Record, 0)
				receivedLSN := fbRecord.Lsn()
				if receivedLSN != n.lsn+1 {
					panic(fmt.Sprintf("received wrong LSN %d, want %d", receivedLSN, n.lsn+1))
				}
				n.lsn++
			}
			n.lastOffset = dbkernel.DecodeOffset(records[len(records)-1].Offset)
		case err := <-replicatorErrors:
			if !errors.Is(err, context.Canceled) || !errors.Is(err, io.EOF) {
				panic(err)
			}
		case <-ticker.C:
			localSegmentLagGauge.WithLabelValues(namespace, n.id)
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
	for i := 0; i < num; i++ {
		rep := NewLocalWalRelayer(i)

		go func(r *LocalWalRelayer) {
			if err := r.Run(ctx, engine, metricsTickInterval); err != nil && !errors.Is(err, context.Canceled) {
				panic(err)
			}
		}(rep)
	}

	return nil
}
